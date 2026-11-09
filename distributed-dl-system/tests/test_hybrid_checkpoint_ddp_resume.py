"""8.4 real three-rank CPU Gloo crash/resume through hybrid checkpoint storage.

Requires localhost Gloo sockets, with private FileStore rendezvous. Three disposable Flask agents and real cache/private
copies use a mocked outer launcher/isolation; only native child PIDs are killed.
No Pi, NFS mount, live DB, external interface or privileged system mutation.
"""
import io
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time
import unittest
from unittest.mock import patch
import zipfile

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
import test_hybrid_checkpoint_recovery as recovery
from test_hybrid_checkpoint_recovery import fixtures,leases,worker_lease
from hybrid_storage.staging_protocol import parse_plan
from checkpoint_protocol import group_identity,validate_group
from test_group_recovery import WORKER

# Stop at a stable complete cut, then SIGKILL one rank from the owning test.
# Remaining ranks are also reaped before any replacement group is launched.
NATIVE_WORKER=WORKER.replace(
    "dist.init_process_group('gloo',timeout=timedelta(seconds=15))",
    "dist.init_process_group('gloo',init_method='file://'+os.environ['DL_TEST_RENDEZVOUS'],rank=rank,world_size=int(os.environ['WORLD_SIZE']),timeout=timedelta(seconds=15))").replace('import json,os,random,sys','import json,os,random,sys,time').replace(
    "if case=='cut' and step==2: sys.exit(77)",
    "if case=='cut' and step==2:\n            (root/'awaiting-crash').write_text('ready')\n            while True: time.sleep(0.05)")


class RealHybridDDPTests(unittest.TestCase):
    world_size=3  # Test-fixture cardinality, never an elastic recovery policy.

    def setUp(self):
        self.r=recovery.RecoveryTests(methodName='runTest');self.addCleanup(self.r.doCleanups);self.r.setUp()
        self.f=self.r.f
        self.nodes=self.r.ddp(workers=self.world_size)
        # Map the three disposable agents to distinct localhost identities.
        clients=dict(self.f.http_by_ip);self.f.http_by_ip.clear()
        for number,node in enumerate(self.nodes,1):
            old=node['ip'];node['ip']=f'127.0.0.{number}'
            self.f.http_by_ip[node['ip']]=self.f.http if number==1 else clients[old]
        self.services=[self.f.service,*self.f.extra_services]
        self.program=self.f.fixture.base/f'native-{self.world_size}-rank.py';self.program.write_text(NATIVE_WORKER)
        self.processes=[];self.logs=[]
        self.addCleanup(self.reap)

    def reap(self):
        for process in self.processes:
            if process.poll() is None:process.kill()
        for process in self.processes:
            process.wait(timeout=10)
        for log in self.logs:log.close()
        self.processes=[];self.logs=[]

    def context(self):
        job=self.r.job()
        return json.loads(self.r.db.execute('SELECT context_json FROM distributed_attempts WHERE job_id=? AND retry_count=?',
                                         (job['job_id'],job['retry_count'])).fetchone()[0])

    def run_group(self,case,*,baseline=False,source=None,pause_marker=None,pause_ranks=None):
        job=self.r.job();token=f"{job['job_id']}:{job['retry_count']}"
        if baseline:
            with socket.socket() as temporary:
                temporary.bind(('127.0.0.1',0));port=temporary.getsockname()[1]
            context=dict(world_size=self.world_size,master_addr='127.0.0.1',master_port=port)
        else:context=self.context()
        directories=[]
        for rank,service in enumerate(self.services):
            directory=(self.f.fixture.base/case/str(rank) if baseline else
                       service.root/'worker-storage/jobs'/job['job_id']/'checkpoints')
            directory.mkdir(parents=True,exist_ok=True);directories.append(directory)
            env={**os.environ,'PYTHONPATH':str(ROOT/'agent'),'OMP_NUM_THREADS':'1','MKL_NUM_THREADS':'1',
                'DL_TEST_RENDEZVOUS':str(self.f.fixture.base/(case+'-rendezvous')),'GLOO_SOCKET_IFNAME':'lo','RANK':str(rank),'WORLD_SIZE':str(self.world_size),'MASTER_ADDR':context['master_addr'],
                'MASTER_PORT':str(context['master_port']),'DL_JOB_ID':job['job_id'],
                'DL_MANIFEST_SHA256':job['storage_manifest_sha256'],'DL_COMPATIBILITY_KEY':'c'*64,
                'DL_ATTEMPT_TOKEN':token,'DL_DDP_GROUP_ID':group_identity(job['job_id'],token,job['storage_manifest_sha256'],'c'*64,context),
                'DL_CHECKPOINT_DIR':str(directory),'DL_CHECKPOINT_INTERVAL_SECONDS':'1',
                'DL_RESUME_STATUS_PATH':str(directory/'resume-status.json'),'DL_LAUNCHER_PID':'99999999'}
            if not baseline:
                lease=leases.get(job,service.credential.worker)
                env.update(DL_EXECUTION_LEASE_PATH=str(worker_lease.lease_path(service.root,lease)),DL_WORKER_ROOT=str(service.root))
            if source:
                record=next(r for r in service.journal.records() if r['lease']['attempt_token']==token)
                plan=parse_plan(record['plan'])
                private=service.workspaces.root/'attempts'/plan.identity.key/'checkpoints/resume.dlckpt'
                self.assertEqual(private.read_bytes(),(self.r.root/'commits'/(source.publication_id+'.dlckpt')).read_bytes())
                env.update(DL_RESUME_CHECKPOINT=str(private),DL_RESUME_SHA256=source.sha256)
            log=(directory/(case+'.log')).open('w');self.logs.append(log)
            self.processes.append(self.f.real_popen([sys.executable,str(self.program),case],env=env,stdout=log,stderr=subprocess.STDOUT))
        if case=='cut' or pause_marker is not None:
            marker=pause_marker or 'awaiting-crash'
            paused=range(self.world_size) if pause_ranks is None else pause_ranks
            deadline=time.monotonic()+40
            while not all((directories[rank]/marker).exists() for rank in paused):
                if any(p.poll() is not None for p in self.processes) or time.monotonic()>deadline:
                    self.fail('\n'.join((directory/(case+'.log')).read_text() for directory in directories))
                time.sleep(0.05)
        else:
            for rank,process in enumerate(self.processes):
                self.assertEqual(process.wait(timeout=45),0,(directories[rank]/(case+'.log')).read_text())
            self.reap()
        return directories

    def states(self,path):
        with patch.object(subprocess,'Popen',self.f.real_popen):
            import torch
        with zipfile.ZipFile(path) as archive:
            return [torch.load(io.BytesIO(archive.read(f'rank-{rank}.pt')),weights_only=True,map_location='cpu') for rank in range(self.world_size)]

    def equal(self,a,b):
        import torch
        if isinstance(a,torch.Tensor):return torch.equal(a,b)
        if isinstance(a,dict):return a.keys()==b.keys() and all(self.equal(a[key],b[key]) for key in a)
        if isinstance(a,(list,tuple)):return len(a)==len(b) and all(self.equal(x,y) for x,y in zip(a,b))
        return a==b

    def assert_state(self,a,b,path='state'):
        import torch
        if isinstance(a,torch.Tensor):
            self.assertEqual(a.dtype,b.dtype,path);self.assertEqual(a.shape,b.shape,path)
            if a.is_floating_point() and path.startswith(('state.model.','state.optimizer.')):
                torch.testing.assert_close(a,b,rtol=1e-7,atol=1e-8,msg=path)
            else:self.assertTrue(torch.equal(a,b),path)
        elif isinstance(a,dict):
            self.assertEqual(a.keys(),b.keys(),path)
            for key in a:self.assert_state(a[key],b[key],path+'.'+str(key))
        elif isinstance(a,(list,tuple)):
            self.assertEqual(len(a),len(b),path)
            for index,(x,y) in enumerate(zip(a,b)):self.assert_state(x,y,path+'.'+str(index))
        else:self.assertEqual(a,b,path)

    def differences(self,a,b,path='state'):
        import torch
        if isinstance(a,torch.Tensor):
            if torch.equal(a,b):return []
            return [dict(path=path,dtype=str(a.dtype),shape=list(a.shape),
                         max_abs=float((a.to(torch.float64)-b.to(torch.float64)).abs().max()))]
        if isinstance(a,dict):
            if a.keys()!=b.keys():return [dict(path=path,keys_differ=True)]
            return [item for key in a for item in self.differences(a[key],b[key],path+'.'+str(key))]
        if isinstance(a,(list,tuple)):
            if len(a)!=len(b):return [dict(path=path,length_differs=True)]
            return [item for index,(x,y) in enumerate(zip(a,b)) for item in self.differences(x,y,path+'.'+str(index))]
        return [] if a==b else [dict(path=path,expected=repr(a),actual=repr(b))]

    def test_sigkill_rank_then_committed_three_rank_resume_matches_baseline_with_stated_fp32_tolerance(self):
        baseline=self.run_group('baseline',baseline=True)[0]/'latest.dlckpt'
        control=self.run_group('baseline-control',baseline=True)[0]/'latest.dlckpt'
        settings=leases.settings()
        with patch.object(leases,'settings',return_value={**settings,'duration_seconds':60}):self.r.cold()
        original=self.run_group('cut')[0]/'latest.dlckpt'
        source=self.r.save(raw=original.read_bytes())
        self.assertEqual(source.global_step,2);self.assertFalse(source.completed)
        self.processes[1].kill();self.assertEqual(self.processes[1].wait(timeout=10),-9)
        self.reap()  # No old rank survives the replacement boundary.
        self.f.process.poll.return_value=1
        for service in self.services:
            service.quiescent=lambda *_:True
            self.assertFalse(service.note_exit(service.module.current_job['lease'],1));service.tick()
        self.assertEqual(self.r.runtime.reconcile(self.r.job()),'QUEUED')
        self.assertEqual(self.r.job()['retry_count'],1);self.assertEqual(fixtures.store.get_reservations(),[])
        self.f.process.poll.return_value=None
        for service in self.services:service.module.current_job=None;service.module.job_process=None
        # Reopen controller state, not just a new coordinator over an old handle.
        old=self.r.db;db=self.f.fixture.connect();self.addCleanup(db.close)
        registry=type(self.r.registry)(db,self.r.registry.publisher)
        self.r.db=self.f.db=self.f.fixture.db=db;self.r.registry=self.f.registry=registry
        self.r.publisher=type(self.r.publisher)(registry,self.r.root,reserve_free_bytes=0);self.r.publisher.initialize()
        self.r.runtime=self.f.runtime=self.r.coordinator();old.close()
        before=[service.fetcher.cache.metrics.snapshot().staged_bytes for service in self.services]
        with patch.object(leases,'settings',return_value={**settings,'duration_seconds':60}):self.assertTrue(self.f.dispatch())
        self.assertEqual(self.r.job()['status'],'SCHEDULED')
        self.assertEqual(before,[service.fetcher.cache.metrics.snapshot().staged_bytes for service in self.services])
        resumed=self.run_group('resume',source=source)[0]/'latest.dlckpt'
        self.assertEqual(self.r.runtime.reconcile(self.r.job()),'RUNNING')
        expected=self.states(baseline);actual=self.states(resumed)
        label='THREE' if self.world_size==3 else 'TWO'
        differences={str(rank):self.differences(expected[rank],actual[rank]) for rank in range(self.world_size)}
        print(label+'_RANK_STATE_DIFFERENCES '+json.dumps(differences,sort_keys=True),flush=True)
        comparison=self.states(control)
        print('UNINTERRUPTED_'+label+'_RANK_DIFFERENCES '+json.dumps({str(rank):self.differences(expected[rank],comparison[rank]) for rank in range(self.world_size)},sort_keys=True),flush=True)
        saved=self.states(self.r.root/'commits'/(source.publication_id+'.dlckpt'))
        import torch
        for rank in range(self.world_size):
            loaded=torch.load(self.services[rank].root/'worker-storage/jobs'/self.f.job_id/'checkpoints/restored.pt',weights_only=True)
            for key in loaded:self.assertTrue(self.equal(loaded[key],saved[rank][key]),f'Rank {rank} restore {key} is not exact')
            self.assert_state(expected[rank],actual[rank])
            self.assert_state(expected[rank],comparison[rank])
        final=self.r.save(raw=resumed.read_bytes())
        self.assertEqual(final.global_step,6);self.assertTrue(final.completed);self.assertEqual(final.retry_count,1)
        self.assertEqual(validate_group(resumed,self.f.job_id,self.r.job()['storage_manifest_sha256'])['world_size'],self.world_size)
        self.f.process.poll.return_value=0
        for service in self.services:
            service.quiescent=lambda *_:True
            self.assertTrue(service.note_exit(service.module.current_job['lease'],0));service.tick()
        with patch('job_artifacts.collect') as collect:
            self.assertEqual(self.r.runtime.reconcile(self.r.job()),'COMPLETED');self.assertEqual(collect.call_count,self.world_size)
        self.assertEqual(self.r.db.execute("SELECT COUNT(*) FROM recovery_events WHERE event_type='DDP_RESUMED'").fetchone()[0],1)
        self.assertEqual(self.r.publisher.head('alice',self.f.job_id),final)


if __name__=='__main__':unittest.main(verbosity=2)
