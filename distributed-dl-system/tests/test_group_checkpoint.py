"""Exercise real DDP checkpoint cuts, failed acknowledgments and central transfer guards."""
import hashlib
import importlib.util
import io
import json
import os
import shutil
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import Mock, patch
import zipfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'agent'))
sys.path.insert(0,str(ROOT/'controller'))
from checkpoint_protocol import validate_group, group_identity
import group_checkpoints as central
import job_store as store

WORKER = '''
import hashlib,json,os,random,sys,time
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch
import torch
import torch.distributed as dist
from torch.nn.parallel import DistributedDataParallel as DDP
from dl_checkpoint import DistributedCheckpointManager
rank=int(os.environ['RANK']);directory=Path(os.environ['DL_CHECKPOINT_DIR'])
case=sys.argv[1]
torch.set_num_threads(1);torch.manual_seed(123)
dist.init_process_group('gloo',timeout=timedelta(seconds=6))
try:
    model=DDP(torch.nn.Sequential(torch.nn.Linear(2,4),torch.nn.BatchNorm1d(4),
                                 torch.nn.Dropout(0.2),torch.nn.Linear(4,1)))
    optimizer=torch.optim.Adam(model.parameters(),lr=0.003)
    scheduler=torch.optim.lr_scheduler.StepLR(optimizer,step_size=2,gamma=0.7)
    torch.manual_seed(123+rank);random.seed(100+rank)
    try:
        import numpy as np
        np.random.seed(200+rank)
    except ImportError: pass
    if case=='missing-numpy':
        sys.modules['numpy']=None
        try: DistributedCheckpointManager()
        except ValueError as error:
            (directory/'error.json').write_text(json.dumps({'error':str(error)}))
            sys.exit(0)
        raise AssertionError('Missing NumPy was not rejected before checkpoint collectives')
    manager=DistributedCheckpointManager()
    for step in (1,2):
        inputs=torch.randn(8,2)+rank*2
        random.random()
        optimizer.zero_grad();model(inputs).square().mean().backward()
        optimizer.step();scheduler.step()
        if step==2 and case=='mismatch' and rank==1:
            optimizer.param_groups[0]['lr']+=0.01
        if step==2 and case in ('reject','crash') and rank==1:
            original=manager._acknowledge
            def acknowledge(proposal,state_sha):
                if case=='crash':
                    (directory/'awaiting-ack').write_text('ready')
                    while True: time.sleep(0.1)
                value=original(proposal,state_sha);value['accepted']=False;return value
            manager._acknowledge=acknowledge
        kwargs=dict(global_step=step,progress={'next_batch':step},scheduler=scheduler,
                    extra_state={'sampler':{'epoch':0,'rank':rank}},completed=step==2,force=True)
        try:
            if step==2 and case=='write-failure' and rank==0:
                with patch('dl_checkpoint.os.replace',side_effect=OSError('simulated disk failure')):
                    manager.save(model,optimizer,**kwargs)
            else: manager.save(model,optimizer,**kwargs)
        except Exception as error:
            (directory/'error.json').write_text(json.dumps({'error':str(error)}))
            if step!=2 or case=='success': raise
            break
        if step==1 and rank==0:
            (directory/'previous.sha').write_text(hashlib.sha256(manager.path.read_bytes()).hexdigest())
        if step==2:
            torch.save({'model':model.module.state_dict(),'optimizer':optimizer.state_dict(),
                        'scheduler':scheduler.state_dict(),'torch_rng':torch.get_rng_state(),
                        'python_rng':random.getstate()},directory/'local.pt')
finally: dist.destroy_process_group()
'''


def same(a,b):
    import torch
    if isinstance(a,torch.Tensor): return isinstance(b,torch.Tensor) and torch.equal(a,b)
    if isinstance(a,dict): return a.keys()==b.keys() and all(same(a[k],b[k]) for k in a)
    if isinstance(a,(list,tuple)): return len(a)==len(b) and all(same(x,y) for x,y in zip(a,b))
    return a==b


class GroupCheckpointTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.root=Path(self.temp.name)
        self.original=store.DB_FILE
        store.DB_FILE=str(self.root/'jobs.db');store.initialize_database()
        self.context={'world_size':2,'master_addr':'127.0.0.1','master_port':29613}

    def tearDown(self):
        store.DB_FILE=self.original
        self.temp.cleanup()

    def run_group(self,case):
        with socket.socket() as listener:
            listener.bind(('127.0.0.1',0));self.context['master_port']=listener.getsockname()[1]
        path=self.root/'worker.py';path.write_text(WORKER)
        group_id=group_identity('group','group:0','snapshot','compat',self.context)
        processes=[];logs=[]
        try:
            for rank in range(2):
                directory=self.root/f'rank-{rank}';directory.mkdir()
                log=(directory/'training.log').open('w');logs.append(log)
                env=dict(os.environ,PYTHONPATH=str(ROOT/'agent'),GLOO_SOCKET_IFNAME='lo',
                         RANK=str(rank),WORLD_SIZE='2',MASTER_ADDR='127.0.0.1',
                         MASTER_PORT=str(self.context['master_port']),DL_JOB_ID='group',
                         DL_MANIFEST_SHA256='snapshot',DL_COMPATIBILITY_KEY='compat',
                         DL_ATTEMPT_TOKEN='group:0',DL_DDP_GROUP_ID=group_id,
                         DL_CHECKPOINT_DIR=str(directory),DL_CHECKPOINT_INTERVAL_SECONDS='1')
                processes.append(subprocess.Popen([sys.executable,str(path),case],env=env,
                                                  stdout=log,stderr=subprocess.STDOUT))
            if case=='crash':
                deadline=time.monotonic()+35
                while not (self.root/'rank-1/awaiting-ack').exists():
                    if time.monotonic()>deadline: self.fail('Rank never reached acknowledgment boundary')
                    time.sleep(0.05)
                root=self.root/'rank-0'
                self.assertTrue(list(root.glob('.ddp-stage-*')))
                self.assertEqual(validate_group(root/'latest.dlckpt','group','snapshot')['global_step'],1)
                processes[1].kill();processes[1].wait()
            for rank,process in enumerate(processes):
                if case=='crash' and rank==1: continue
                code=process.wait(timeout=40)
                if code:
                    self.fail((self.root/f'rank-{rank}'/'training.log').read_text())
        finally:
            for process in processes:
                if process.poll() is None: process.kill();process.wait()
            for log in logs: log.close()
        archive=self.root/'rank-0/latest.dlckpt'
        self.assertFalse((self.root/'rank-1/latest.dlckpt').exists())
        self.assertFalse(list((self.root/'rank-0').glob('.ddp-stage-*')))
        return archive

    def test_complete_cut_preserves_rank_buffers_rng_and_optimizer(self):
        import torch
        archive=self.run_group('success')
        meta=validate_group(archive,'group','snapshot','group:0')
        self.assertEqual(meta['global_step'],2);self.assertTrue(meta['completed'])
        self.assertEqual(len(meta['rank_acknowledgments']),2)
        states=[]
        with zipfile.ZipFile(archive) as z:
            for rank in range(2):
                with z.open(f'rank-{rank}.pt') as stream:
                    state=torch.load(stream,map_location='cpu',weights_only=True)
                local=torch.load(self.root/f'rank-{rank}/local.pt',weights_only=True)
                for key in local: self.assertTrue(same(local[key],state[key]),key)
                self.assertEqual(state['progress'],{'next_batch':2})
                self.assertEqual(state['extra_state']['sampler']['rank'],rank)
                states.append(state)
        self.assertTrue(same(states[0]['optimizer'],states[1]['optimizer']))
        self.assertFalse(torch.equal(states[0]['model']['1.running_mean'],states[1]['model']['1.running_mean']))
        self.assertFalse(torch.equal(states[0]['torch_rng'],states[1]['torch_rng']))

    def preserved_after(self,case):
        archive=self.run_group(case)
        previous=(self.root/'rank-0/previous.sha').read_text()
        self.assertEqual(hashlib.sha256(archive.read_bytes()).hexdigest(),previous)
        meta=validate_group(archive,'group','snapshot')
        self.assertEqual(meta['global_step'],1);self.assertFalse(meta['completed'])
        self.assertTrue((self.root/'rank-0/error.json').exists())

    def test_missing_numpy_fails_before_checkpoint_collectives(self):
        archive=self.run_group('missing-numpy')
        self.assertFalse(archive.exists())
        for rank in range(2):
            message=json.loads((self.root/f'rank-{rank}/error.json').read_text())['error']
            self.assertIn('requires numpy',message)

    def test_disagreeing_optimizer_does_not_publish(self): self.preserved_after('mismatch')
    def test_rejected_ack_does_not_publish(self): self.preserved_after('reject')
    def test_disk_failure_preserves_previous_bundle(self): self.preserved_after('write-failure')
    def test_rank_dies_during_ack_previous_bundle_remains(self): self.preserved_after('crash')

    def setup_job(self):
        spec={'execution':{'mode':'distributed','workers':2},'recovery':{'mode':'checkpoint','max_retries':0},
              'resources_per_worker':{'cpu_cores':1,'memory_gb':1,'gpus':0}}
        store.add_job('group',1,max_retries=0,job_type='custom',status='RECEIVED',owner='student',job_config=spec)
        workspace=self.root/'central';(workspace/'checkpoints').mkdir(parents=True)
        with store.get_connection() as db:
            db.execute("UPDATE jobs SET status='QUEUED',storage_path=?,storage_manifest_sha256='snapshot' WHERE job_id='group'",(str(workspace),))
            for rank in range(2):
                db.execute('INSERT INTO job_environments VALUES(?,?,?,?,?,?,?)',
                    ('group',f'pi0{rank+1}','pytorch-cpu','env',json.dumps({'compatibility_key':'compat'}),1,1))
        nodes=[{'id':f'pi0{r+1}','ip':f'192.0.2.{r+1}','port':5000} for r in range(2)]
        store.mark_scheduled('group',nodes[0]['id'],nodes[0]['ip'],spec['resources_per_worker'],nodes,self.context)
        return store.get_job('group')

    def response(self,payload):
        response=Mock(status_code=200)
        response.__enter__=Mock(return_value=response);response.__exit__=Mock(return_value=False)
        response.iter_content.return_value=[payload]
        return response

    def test_corrupt_partial_stale_and_wrong_group_cannot_replace_central(self):
        archive=self.run_group('success');job=self.setup_job()
        spec=importlib.util.spec_from_file_location('step8_agent',ROOT/'agent/agent.py')
        agent=importlib.util.module_from_spec(spec);spec.loader.exec_module(agent)
        base=self.root/'agent-root';directory=base/'worker-storage/jobs/group'
        (directory/'checkpoints').mkdir(parents=True)
        (directory/'prepared.json').write_text(json.dumps({'manifest_sha256':'snapshot','compatibility_key':'compat'}))
        descriptor={'manifest':{'execution_spec':{'execution':{'mode':'distributed','workers':2}}}}
        (directory/'request.json').write_text(json.dumps(descriptor))
        shutil.copyfile(archive,directory/'checkpoints/latest.dlckpt')
        with patch.object(agent,'BASE_DIR',base):
            client=agent.app.test_client()
            response=client.get('/jobs/group/custom-checkpoint?attempt_token=group:0')
            self.assertEqual(response.status_code,200);self.assertEqual(response.data,archive.read_bytes());response.close()
            self.assertEqual(client.get('/jobs/group/custom-checkpoint?attempt_token=group:1').status_code,422)
            descriptor['manifest']['execution_spec']['execution']['workers']=3
            (directory/'request.json').write_text(json.dumps(descriptor))
            self.assertEqual(client.get('/jobs/group/custom-checkpoint').status_code,422)
        with patch.object(central.requests,'get',return_value=self.response(archive.read_bytes())):
            self.assertTrue(central.sync(job,force=True))
        previous=central.path_for(job).read_bytes()
        with zipfile.ZipFile(archive) as z: entries={name:z.read(name) for name in z.namelist()}
        for kind in ('corrupt','partial','missing-rank','stale','wrong-group','regressing'):
            modified=dict(entries);meta=json.loads(modified['metadata.json'])
            if kind=='corrupt': modified['rank-1.pt']=b'corrupt'
            if kind=='partial': meta['rank_acknowledgments']=meta['rank_acknowledgments'][:1]
            if kind=='missing-rank': modified.pop('rank-1.pt')
            if kind=='stale': meta['attempt_token']='group:1'
            if kind=='wrong-group': meta['group_id']='0'*64
            if kind=='regressing': meta['global_step']=1;meta['completed']=False
            modified['metadata.json']=json.dumps(meta).encode()
            output=io.BytesIO()
            with zipfile.ZipFile(output,'w') as z:
                for name,payload in modified.items(): z.writestr(name,payload)
            with patch.object(central.requests,'get',return_value=self.response(output.getvalue())):
                self.assertFalse(central.sync(job,force=True),kind)
            self.assertEqual(central.path_for(job).read_bytes(),previous)

    def test_missing_final_group_checkpoint_cannot_complete_job(self):
        import distributed_dispatch as dispatch
        job=self.setup_job();store.mark_running('group',1)
        job=store.get_job('group')
        def completed(job,rank):
            return {'attempt_token':'group:0','ddp':{**self.context,'rank':rank['rank']},
                    'status':'COMPLETED','started_at':1,'return_code':0}
        with patch.object(central,'sync',return_value=False),patch.object(dispatch,'worker',side_effect=completed):
            for _ in range(3): dispatch.progress(job)
        self.assertEqual(dispatch.group(job)['phase'],'STOPPING')
        self.assertEqual(store.get_job('group')['status'],'RUNNING')
        self.assertEqual(len(store.get_reservations()),2)
        with patch.object(central,'sync',return_value=False),patch.object(dispatch.requests,'post'), \
             patch.object(dispatch,'worker',return_value=None),patch.object(dispatch,'collect_ranks',return_value=True):
            dispatch.progress(store.get_job('group'))
        self.assertEqual(store.get_job('group')['status'],'FAILED')
        self.assertEqual(store.get_reservations(),[])


if __name__=='__main__': unittest.main()
