"""8.3 disposable DB, signed Flask transports, real cache/copies, mocked launcher.

No listener, live mount, Pi or privileged isolation. CPU SDK resume tested separately.
"""
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import shutil
import sqlite3
import sys
import time
import unittest
from unittest.mock import Mock,patch
import zipfile

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
import test_staging_dispatch as dispatch_fixture
from test_staging_dispatch import fixtures,leases,worker_lease
from hybrid_storage import checkpoint_schema,restore_schema
from hybrid_storage.checkpoint_publication import LocalCheckpointPublisher
from hybrid_storage.checkpoint_transfer import CheckpointCollector
from hybrid_storage.checkpoint_restore import CheckpointRestorer
from hybrid_storage.config import StorageConfig
from hybrid_storage.staging_protocol import START_PATH,STAGE_PATH
from checkpoint_service import CheckpointService,install_checkpoint_service
from checkpoint_recovery import CheckpointDispatch
from checkpoint_protocol import group_identity


def checkpoint_job(f,*,owner='alice',backend='nfs',generated=False):
    f.serial+=1;identity=f'checkpoint-{f.serial}'
    document=json.loads((ROOT/'examples/job.checkpoint.json').read_text())
    document.update(project=str(f.project),dataset=None if generated else str(f.data))
    spec=fixtures.validate_spec(document,f.base)
    fixtures.store.add_job(identity,1,job_type='custom',owner=owner,status='RECEIVED',job_config=spec,storage_backend=backend)
    receipt=fixtures.job_storage.stage_job(identity);workspace=Path(receipt['storage_path'])
    return identity,workspace,(workspace/'manifest.json').read_bytes()


class RecoveryTests(unittest.TestCase):
    def setUp(self):
        self.f=dispatch_fixture.DispatchTests(methodName='runTest');self.addCleanup(self.f.doCleanups)
        with patch.object(fixtures.RegistryTests,'job',checkpoint_job),patch.object(dispatch_fixture.DispatchTests,'prepare_environment'):
            self.f.setUp()
        self.db,self.registry=self.f.db,self.f.registry
        import recovery_metrics
        with patch.object(recovery_metrics,'DB_PATH',self.f.fixture.db_path):recovery_metrics.init_table()
        checkpoint_schema.migrate(self.db);restore_schema.migrate(self.db)
        self.root=self.f.fixture.base/'checkpoints';self.root.mkdir(mode=0o700)
        self.publisher=LocalCheckpointPublisher(self.registry,self.root,reserve_free_bytes=0);self.publisher.initialize()
        original_reset=self.f.popen.reset_mock
        def reset(*args,**kwargs):
            for call in self.f.popen.call_args_list:call.kwargs['stdout'].close()
            return original_reset(*args,**kwargs)
        self.f.popen.reset_mock=reset
        self.install_worker(self.f.service)
        self.runtime=self.coordinator();self.f.runtime=self.runtime;self.f.prepare_environment(enqueue=False)

    def coordinator(self):
        keys=self.f.client.credentials
        return CheckpointDispatch(self.registry,self.f.client,publisher=self.publisher,
            collector=CheckpointCollector(self.publisher,keys,get=self.get),
            restorer=CheckpointRestorer(self.publisher,keys,post=self.post),settings=StorageConfig('nfs',True,True,'resource'))

    def install_worker(self,service):
        spool=service.root.parent/(service.root.name+'-checkpoint-spool');spool.mkdir(mode=0o700)
        trusted=CheckpointService(service.credential,worker_root=service.root,spool_root=spool,reserve_free_bytes=0)
        install_checkpoint_service(service.module,trusted)
        return trusted

    def response(self,result):
        import requests
        value=requests.Response();value.status_code=result.status_code
        value._content=result.data;value._content_consumed=True;value.headers.update(result.headers)
        result.close();return value

    def http(self,url):
        address=url.split('/')[2].split(':')[0]
        return self.f.http_by_ip.get(address,self.f.http),'/'+url.split('/',3)[3]

    def get(self,url,*,headers,**kwargs):
        http,path=self.http(url);return self.response(http.get(path,headers=headers))

    def post(self,url,*,data,headers,**kwargs):
        http,path=self.http(url);raw=data.read()
        self.f.calls.append((path,'checkpoint-restore'))
        return self.response(http.post(path,data=raw,headers=headers))

    def job(self):return fixtures.store.get_job(self.f.job_id)

    def cold(self):
        if self.job()['status']=='READY':
            fixtures.store.enqueue_custom_job(self.f.job_id,[r['node_id'] for r in fixtures.store.get_job_environments(self.f.job_id)],storage_runtime=self.runtime)
        self.assertTrue(self.f.dispatch());self.assertEqual(self.job()['status'],'RUNNING')
        return self.job()

    def raw(self,job=None,step=5,completed=False):
        job=job or self.job();world=json.loads(job['job_config'])['execution']['workers']
        meta=dict(format_version=1 if world==1 else 2,job_id=job['job_id'],manifest_sha256=job['storage_manifest_sha256'],
                  attempt_token=f"{job['job_id']}:{job['retry_count']}",compatibility_key='c'*64,global_step=step,completed=completed,created_at=1)
        if world==1:
            state={'state.pt':b'opaque-test-state'}
            meta.update(state_sha256=hashlib.sha256(state['state.pt']).hexdigest(),lease_fence=worker_lease.fence(leases.get(job,'pi01')))
        else:
            context=json.loads(self.db.execute('SELECT context_json FROM distributed_attempts WHERE job_id=? AND retry_count=?',(job['job_id'],job['retry_count'])).fetchone()[0])
            state={f'rank-{rank}.pt':str(rank).encode() for rank in range(world)}
            hashes={str(rank):hashlib.sha256(state[f'rank-{rank}.pt']).hexdigest() for rank in range(world)}
            meta.update(kind='ddp',world_size=world,checkpoint_id='b'*32,group_id=group_identity(job['job_id'],meta['attempt_token'],job['storage_manifest_sha256'],'c'*64,context),
                shared_state_sha256='d'*64,rank_state_sha256=hashes,
                rank_acknowledgments=[dict(rank=rank,checkpoint_id='b'*32,state_sha256=hashes[str(rank)],accepted=True) for rank in range(world)],
                rank_fences={str(rank):worker_lease.fence(leases.get(job,f'pi{rank+1:02}')) for rank in range(world)})
        stream=io.BytesIO()
        with zipfile.ZipFile(stream,'w',compression=zipfile.ZIP_STORED) as archive:
            archive.writestr('metadata.json',json.dumps(meta))
            for name,value in state.items():archive.writestr(name,value)
        return stream.getvalue()

    def save(self,raw=None,step=5,completed=False):
        raw=self.raw(step=step,completed=completed) if raw is None else raw
        path=self.f.worker/'worker-storage/jobs'/self.f.job_id/'checkpoints/latest.dlckpt';path.write_bytes(raw)
        job,stage,members=self.runtime.snapshot(self.f.job_id)
        return self.runtime.sync(job,members)

    def fail(self):
        job,stage,members=self.runtime.snapshot(self.f.job_id)
        self.runtime.abort_intent(job,stage,'Disposable simulated worker failure')
        self.f.process.poll.return_value=1
        for member in members:
            service=self.f.service if member[0]==0 else self.f.extra_services[member[0]-1]
            service.quiescent=lambda *_:True
        self.assertEqual(self.runtime.cleanup(job,stage,members),'QUEUED')
        self.f.process.poll.return_value=None
        self.f.agent.current_job=None;self.f.agent.job_process=None
        for service in getattr(self.f,'extra_services',[]):service.module.current_job=None;service.module.job_process=None
        return self.job()

    def ack(self,service=None,*,bad=False):
        service=service or self.f.service;agent=service.module;observed=agent.current_job
        expected=observed['resume_checkpoint'];ddp=observed.get('ddp')
        info=dict(job_id=observed['job_id'],attempt_token=observed['attempt_token'],pid=observed['pid'],confirmed_at=time.time(),
            checkpoint_sha256=expected['sha256'],restored_global_step=expected['global_step'],model_restored=True,optimizer_restored=not bad,rng_restored=True)
        if ddp:
            info.update(rank=ddp['rank'],world_size=ddp['world_size'],group_id=group_identity(observed['job_id'],observed['attempt_token'],observed['manifest_sha256'],expected['compatibility_key'],ddp),
                        checkpoint_id=expected['checkpoint_id'],source_attempt=expected['attempt_token'],application_state_restored=True,all_ranks_restored=True)
        (service.root/'worker-storage/jobs'/self.f.job_id/'checkpoints/resume-status.json').write_text(json.dumps(info))

    def test_cold_commit_recover_warm_private_copy_sdk_barrier_and_final_commit(self):
        self.cold();first=self.save();self.fail();before=self.f.cache.metrics.snapshot().staged_bytes
        self.f.calls.clear();self.assertTrue(self.f.dispatch())
        self.assertEqual(self.job()['status'],'SCHEDULED')
        self.assertEqual(self.f.cache.metrics.snapshot().staged_bytes,before)
        paths=[path for path,_ in self.f.calls]
        self.assertLess(paths.index(STAGE_PATH),paths.index('/jobs/'+self.f.job_id+'/custom-checkpoint'))
        self.assertLess(paths.index('/jobs/'+self.f.job_id+'/custom-checkpoint'),paths.index(START_PATH))
        argv=self.f.popen.call_args.args[0];self.assertIn('--staged-resume',argv)
        records=list(self.f.service.journal.records());current=next(r for r in records if r['lease']['attempt_token'].endswith(':1'))
        from hybrid_storage.staging_protocol import parse_plan
        path=self.f.manager.root/'attempts'/parse_plan(current['plan']).identity.key/'checkpoints/resume.dlckpt'
        self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(),first.sha256)
        self.assertEqual(path.stat().st_nlink,1);self.assertFalse(path.stat().st_mode&0o222)
        self.assertEqual(self.runtime.reconcile(self.job()),'RESTORE_PENDING')
        self.ack();self.assertEqual(self.runtime.reconcile(self.job()),'RUNNING')
        self.assertEqual(self.job()['retry_count'],1)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM recovery_events WHERE event_type='RESUMED'").fetchone()[0],1)
        details=json.loads(self.db.execute("SELECT details_json FROM recovery_events WHERE event_type='RESUMED'").fetchone()[0])
        self.assertEqual(details['restored_global_step'],5);self.assertEqual(details['checkpoint_sha256'],first.sha256)
        prepared=json.loads(self.db.execute("SELECT details_json FROM recovery_events WHERE event_type='RESTORE_PREPARED'").fetchone()[0])
        self.assertEqual(prepared['global_step'],details['restored_global_step'])
        second=self.save(step=9,completed=True);self.assertNotEqual(first.publication_id,second.publication_id)
        self.assertEqual(self.save(raw=(self.f.worker/'worker-storage/jobs'/self.f.job_id/'checkpoints/latest.dlckpt').read_bytes()).publication_id,second.publication_id)
        lease=self.f.agent.current_job['lease'];self.f.process.poll.return_value=0
        self.f.service.quiescent=lambda *_:True
        self.assertTrue(self.f.service.note_exit(lease,0));self.f.service.tick()
        with patch('job_artifacts.collect'):
            self.assertEqual(self.runtime.reconcile(self.job()),'COMPLETED')
        self.assertEqual(self.job()['status'],'COMPLETED')
        self.assertEqual(len(list((self.root/'commits').iterdir())),2)

    def test_missing_or_corrupt_committed_head_refuses_cold_restart(self):
        self.cold();head=self.save();self.fail()
        path=self.root/'commits'/(head.publication_id+'.dlckpt');path.chmod(0o600);path.write_bytes(b'corrupt');path.chmod(0o400)
        self.f.popen.reset_mock()
        with self.assertRaises((ValueError,OSError)):self.f.dispatch()
        self.f.popen.assert_not_called();self.assertEqual(self.job()['status'],'QUEUED')

    def test_restore_failure_never_launches_and_retains_reservation(self):
        self.cold();self.save();self.fail();self.f.popen.reset_mock()
        with patch.object(self.runtime.restorer,'stage',side_effect=OSError('offline')):
            with self.assertRaises(OSError):self.f.dispatch()
        self.f.popen.assert_not_called();self.assertEqual(len(fixtures.store.get_reservations()),1)
        self.assertEqual(leases.get(self.job(),'pi01')['renewal_blocked'],1)

    def test_final_metrics_not_completed_from_old_or_nonfinal_checkpoint(self):
        self.cold();self.save();job,stage,members=self.runtime.snapshot(self.f.job_id)
        self.assertFalse(self.runtime.completion_ready(job,stage,members,[]))
        with self.registry._tx(write=False),self.assertRaises(PermissionError):self.runtime.require_final(self.db,self.f.job_id)
        self.assertEqual(self.job()['status'],'RUNNING')
        self.save(step=8,completed=True)
        with self.registry._tx(write=False):self.runtime.require_final(self.db,self.f.job_id)

    def test_restart_reconciles_acknowledged_launch_without_relaunch(self):
        self.cold();self.save();self.fail();self.assertTrue(self.f.dispatch());self.ack()
        self.runtime=self.coordinator();self.f.runtime=self.runtime;self.f.popen.reset_mock()
        self.assertEqual(self.runtime.reconcile(self.job()),'RUNNING');self.f.popen.assert_not_called()

    def test_wrong_sdk_ack_stays_pending_and_expired_lease_cannot_start(self):
        self.cold();self.save();self.fail();self.assertTrue(self.f.dispatch());self.ack(bad=True)
        self.assertEqual(self.runtime.reconcile(self.job()),'RESTORE_PENDING')
        with self.db:self.db.execute('UPDATE execution_leases SET expires_at=0,expires_clock=0 WHERE retry_count=1')
        self.assertNotEqual(self.runtime.reconcile(self.job()),'RUNNING')
        self.assertNotEqual(self.job()['status'],'COMPLETED')

    def test_explicit_migration_idempotent_and_frozen_source_immutable(self):
        version=self.db.execute('PRAGMA user_version').fetchone()[0]
        self.assertTrue(restore_schema.installed(self.db));restore_schema.migrate(self.db)
        self.assertEqual(self.db.execute('PRAGMA user_version').fetchone()[0],version)
        self.cold()
        with self.assertRaises(sqlite3.IntegrityError),self.db:
            self.db.execute("UPDATE hybrid_restore_attempts SET source_json='{}'")
        with self.assertRaises(sqlite3.IntegrityError),self.db:self.db.execute('DELETE FROM hybrid_restore_attempts')

    def test_restore_authentication_tamper_and_corrupt_private_copy_never_launch(self):
        self.cold();self.save();self.fail();self.f.popen.reset_mock()
        from hybrid_storage.checkpoint_transfer import MAC
        post=self.runtime.restorer.post
        def tamper(url,**kwargs):
            kwargs['headers']={**kwargs['headers'],MAC:'f'*64}
            return post(url,**kwargs)
        with patch.object(self.runtime.restorer,'post',side_effect=tamper):
            with self.assertRaises(__import__('requests').HTTPError):self.f.dispatch()
        self.f.popen.assert_not_called()
        self.assertEqual(self.job()['status'],'SCHEDULED')
        job,stage,members=self.runtime.snapshot(self.f.job_id)
        self.runtime.abort_intent(job,stage,'Disposable bad restore')
        self.f.service.quiescent=lambda *_:True
        self.assertEqual(self.runtime.cleanup(job,stage,members),'QUEUED')
        restore=self.runtime.restorer.stage
        def corrupt(*args,**kwargs):
            result=restore(*args,**kwargs)
            from hybrid_storage.staging_protocol import parse_plan
            record=next(r for r in self.f.service.journal.records() if r['lease']['attempt_token'].endswith(':2'))
            plan=parse_plan(record['plan']);path=self.f.manager.root/'attempts'/plan.identity.key/'checkpoints/resume.dlckpt'
            path.chmod(0o600);path.write_bytes(b'bad bytes');path.chmod(0o400)
            return result
        with patch.object(self.runtime.restorer,'stage',side_effect=corrupt):
            with self.assertRaises(__import__('requests').HTTPError):self.f.dispatch()
        self.f.popen.assert_not_called()

    def test_cancellation_after_restore_before_start_never_launches(self):
        self.cold();self.save();self.fail();self.f.popen.reset_mock()
        stage=self.runtime.restorer.stage
        def cancel(*args,**kwargs):
            value=stage(*args,**kwargs)
            fixtures.store.control_job(self.f.job_id,'alice','cancel')
            return value
        with patch.object(self.runtime.restorer,'stage',side_effect=cancel):
            with self.assertRaises((ValueError,PermissionError)):self.f.dispatch()
        self.f.popen.assert_not_called();self.assertEqual(self.job()['status'],'CANCELLING')

    def test_known_checkpoint_missing_head_never_cold_starts(self):
        self.cold();self.save();self.fail();self.f.popen.reset_mock()
        with patch.object(self.publisher,'head',return_value=None):
            with self.assertRaises(PermissionError):self.f.dispatch()
        self.f.popen.assert_not_called();self.assertEqual(self.job()['status'],'QUEUED')

    def test_student_checkpoint_projection_allows_numeric_progress_only(self):
        from staging_view import event_details
        value=event_details(dict(event_type='RESUMED',details_json=json.dumps(dict(global_step=5,retry_count=1,
            sha256='a'*64,checkpoint_path='/private/other-owner',credential='secret'))))
        self.assertEqual(value,dict(global_step=5,retry_count=1))
        self.assertEqual(event_details(dict(event_type='CHECKPOINT_SYNC',details_json='{"global_step":true}')),{})

    def replacement(self):
        node=dict(id='pi02',ip='192.0.2.2',port=5000)
        key=dispatch_fixture.Credential('pi02','2'*32,b'2'*32)
        self.f.client.credentials['pi02']=key
        base=self.f.fixture.base/'replacement';base.mkdir(mode=0o700)
        (base/'agent').mkdir();(base/'agent/custom_runner.py').write_text('# isolated test marker')
        (base/'logs').mkdir()
        shutil.copytree(self.f.worker/'worker-storage/jobs'/self.f.job_id,base/'worker-storage/jobs'/self.f.job_id)
        for name in ('cache','private'):(base/name).mkdir(mode=0o700)
        cache=dispatch_fixture.WorkerCache(base/'cache',input_mount=self.f.fixture.objects)
        fetch=dispatch_fixture.CacheFetcher(cache,source=dispatch_fixture.copy_fixture.SOURCE,quota_bytes=16*1024*1024,reserve_free_bytes=0,io_timeout=3)
        fetch.initialize();self.addCleanup(fetch.close)
        self.f.stack.enter_context(patch.object(fetch,'_spawn',side_effect=self.f.fake_spawn))
        manager=dispatch_fixture.WorkspaceManager(cache,base/'private',max_input_bytes=16*1024*1024,reserve_free_bytes=0);manager.initialize()
        spec=importlib.util.spec_from_file_location('replacement_worker',ROOT/'agent/agent.py')
        agent=importlib.util.module_from_spec(spec)
        with patch.object(dispatch_fixture.subprocess,'run',return_value=Mock(returncode=1)):spec.loader.exec_module(agent)
        agent.BASE_DIR=base;agent.LOG_DIR=base/'logs';agent.node_id=lambda:'pi02'
        service=dispatch_fixture.StagingService(fetch,manager,key,worker_root=base,ownership=worker_lease,
            cancelled=agent.attempt_cancelled,isolate=self.f.isolate,quiescent=lambda *_:False,terminate=Mock())
        dispatch_fixture.install_staging_service(agent,service,watchdog=False);self.addCleanup(service.close)
        self.install_worker(service);self.f.http_by_ip[node['ip']]=agent.app.test_client()
        self.f.nodes.append(node)
        record=json.loads((base/'worker-storage/jobs'/self.f.job_id/'prepared.json').read_text())
        fixtures.store.record_environment(self.f.job_id,'pi02',record,storage_runtime=self.runtime)
        self.runtime=self.coordinator();self.f.runtime=self.runtime
        return node,service,cache

    def test_replacement_worker_selected_by_existing_resources_restages_only_missing_inputs(self):
        node,service,cache=self.replacement();self.cold();head=self.save();self.fail()
        with patch.object(dispatch_fixture.dispatcher,'select_best_node',return_value={'node':node,'resources':{}}) as select:
            self.assertTrue(self.f.dispatch())
        self.assertIn('pi02',select.call_args.kwargs['allowed_nodes'])
        self.assertEqual(self.job()['node_id'],'pi02');self.assertGreater(cache.metrics.snapshot().staged_bytes,0)
        self.ack(service);self.assertEqual(self.runtime.reconcile(self.job()),'RUNNING')
        self.assertEqual(self.publisher.head('alice',self.f.job_id),head)
        self.assertEqual(leases.get(self.job(),'pi02')['attempt_token'],self.f.job_id+':1')

    def test_original_job_version_drift_refuses_dispatch_before_reservation(self):
        self.cold();self.save();self.fail();spec=json.loads(self.job()['job_config'])
        spec['entrypoint']=['python','different.py']
        with self.db:self.db.execute('UPDATE jobs SET job_config=? WHERE job_id=?',(json.dumps(spec),self.f.job_id))
        self.f.popen.reset_mock()
        with self.assertRaises((PermissionError,ValueError)):self.f.dispatch()
        self.f.popen.assert_not_called();self.assertEqual(len(fixtures.store.get_reservations()),0)

    def ddp(self):
        original=fixtures.validate_spec
        def checkpoint_spec(document,base):
            document=dict(document,recovery=dict(mode='checkpoint',interval_seconds=5))
            return original(document,base)
        with patch.object(fixtures,'validate_spec',side_effect=checkpoint_spec):nodes=self.f.ddp_job()
        for service in self.f.extra_services:self.install_worker(service)
        self.runtime=self.coordinator();self.f.runtime=self.runtime
        self.f.stack.enter_context(patch('distributed_dispatch.select_group',return_value=nodes))
        return nodes

    def test_ddp_all_three_ranks_stage_restore_then_confirm_as_one_group(self):
        nodes=self.ddp();self.cold();head=self.save();self.fail();self.f.calls.clear()
        self.assertTrue(self.f.dispatch());self.assertEqual(self.job()['status'],'SCHEDULED')
        paths=[path for path,_ in self.f.calls]
        self.assertEqual(paths.count(STAGE_PATH),3);self.assertEqual(paths.count(START_PATH),3)
        restore='/jobs/'+self.f.job_id+'/custom-checkpoint'
        self.assertEqual(paths.count(restore),3)
        self.assertLess(max(i for i,path in enumerate(paths) if path==restore),paths.index(START_PATH))
        self.ack();self.ack(self.f.extra_services[0])
        self.assertEqual(self.runtime.reconcile(self.job()),'RESTORE_PENDING')
        self.ack(self.f.extra_services[1])
        self.assertEqual(self.runtime.reconcile(self.job()),'RUNNING')
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM recovery_events WHERE event_type='DDP_RESUMED'").fetchone()[0],1)
        self.assertEqual(self.db.execute('SELECT SUM(restored) FROM hybrid_restore_ranks WHERE stage_id=(SELECT stage_id FROM hybrid_restore_attempts WHERE job_id=? AND retry_count=1)',(self.f.job_id,)).fetchone()[0],3)
        self.assertEqual(self.publisher.head('alice',self.f.job_id),head)
        details=json.loads(self.db.execute("SELECT details_json FROM recovery_events WHERE event_type='DDP_RESUMED'").fetchone()[0])
        self.assertEqual(details['attempt_token'],self.f.job_id+':0');self.assertEqual(details['world_size'],3)
        self.assertEqual(details['acknowledged_ranks'],[0,1,2])
        final=self.save(step=9,completed=True);self.assertEqual(final.retry_count,1)
        self.f.process.poll.return_value=0
        for service in [self.f.service,*self.f.extra_services]:
            service.quiescent=lambda *_:True
            self.assertTrue(service.note_exit(service.module.current_job['lease'],0));service.tick()
        with patch('job_artifacts.collect'):
            self.assertEqual(self.runtime.reconcile(self.job()),'COMPLETED')
        self.assertEqual(self.job()['status'],'COMPLETED')

    def test_ddp_last_restore_failure_launches_zero_ranks(self):
        self.ddp();self.cold();self.save();self.fail();self.f.popen.reset_mock()
        real=self.runtime.restorer.stage
        def fail_last(actor,job_id,node,*args):
            if node['id']=='pi03':raise OSError('disposable disconnected rank')
            return real(actor,job_id,node,*args)
        with patch.object(self.runtime.restorer,'stage',side_effect=fail_last):
            with self.assertRaises(OSError):self.f.dispatch()
        self.f.popen.assert_not_called();self.assertEqual(len(fixtures.store.get_reservations()),3)

    def test_real_cpu_committed_bytes_resume_model_optimizer_scheduler_rng_exactly(self):
        # Native CPU subprocesses, no DDP socket and no mocked training claim.
        settings=leases.settings()
        with patch.object(leases,'settings',return_value={**settings,'duration_seconds':60}):self.cold()
        program=self.f.fixture.base/'real-train.py'
        program.write_text("""import os,random,sys
from pathlib import Path
import torch,numpy as np
from dl_checkpoint import CheckpointManager
torch.set_num_threads(1);torch.manual_seed(44);random.seed(44);np.random.seed(44)
m=torch.nn.Sequential(torch.nn.Linear(3,8),torch.nn.Dropout(0.3),torch.nn.Linear(8,1))
o=torch.optim.Adam(m.parameters(),lr=0.01);s=torch.optim.lr_scheduler.StepLR(o,step_size=3,gamma=0.9)
c=CheckpointManager();r=c.load(m,o,scheduler=s);start=r['progress']['next_step'] if r else 0
for step in range(start,12):
 x=torch.randn(8,3)*(0.5+random.random())*float(np.random.uniform(0.8,1.2));y=x.sum(dim=1,keepdim=True)
 o.zero_grad();loss=(m(x)-y).square().mean();loss.backward();o.step();s.step()
 if step==3:c.save(m,o,scheduler=s,global_step=4,progress={'next_step':4},force=True)
 if step==5 and sys.argv[1]=='interrupt':os._exit(77)
c.save(m,o,scheduler=s,global_step=12,progress={'next_step':12},completed=True,force=True)
torch.save(dict(model=m.state_dict(),optimizer=o.state_dict(),scheduler=s.state_dict(),rng=torch.get_rng_state(),python=random.getstate(),numpy=np.random.get_state()[1].tolist()),Path(os.environ['DL_OUTPUT_DIR'])/'final.pt')
""")
        job=self.job();old=leases.get(job,'pi01')
        def environment(folder,token):
            folder.mkdir(exist_ok=True)
            return {**os.environ,'PYTHONPATH':str(ROOT/'agent'),'DL_JOB_ID':self.f.job_id,
                'DL_MANIFEST_SHA256':job['storage_manifest_sha256'],'DL_COMPATIBILITY_KEY':'c'*64,
                'DL_ATTEMPT_TOKEN':token,'DL_LAUNCHER_PID':'99999999','DL_CHECKPOINT_DIR':str(folder),
                'DL_OUTPUT_DIR':str(folder),'DL_CHECKPOINT_INTERVAL_SECONDS':'1','OMP_NUM_THREADS':'1',
                'DL_RESUME_STATUS_PATH':str(folder/'resume-status.json')}
        def execute(mode,env,code=0):
            import subprocess
            process=self.f.real_popen([sys.executable,str(program),mode],env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            stdout,stderr=process.communicate(timeout=40)
            self.assertEqual(process.returncode,code,stderr.decode())
        base=self.f.fixture.base/'baseline';execute('normal',environment(base,self.f.job_id+':0'))
        first=self.f.worker/'worker-storage/jobs'/self.f.job_id/'checkpoints'
        env=environment(first,self.f.job_id+':0')
        env.update(DL_EXECUTION_LEASE_PATH=str(worker_lease.lease_path(self.f.worker,old)),DL_WORKER_ROOT=str(self.f.worker))
        execute('interrupt',env,77);raw=(first/'latest.dlckpt').read_bytes();head=self.save(raw=raw)
        self.assertEqual(head.global_step,4);self.fail()
        with patch.object(leases,'settings',return_value={**settings,'duration_seconds':60}):self.assertTrue(self.f.dispatch())
        from hybrid_storage.staging_protocol import parse_plan
        record=next(r for r in self.f.service.journal.records() if r['lease']['attempt_token'].endswith(':1'))
        plan=parse_plan(record['plan']);private=self.f.manager.root/'attempts'/plan.identity.key/'checkpoints/resume.dlckpt'
        destination=self.f.fixture.base/'resumed';env=environment(destination,self.f.job_id+':1')
        env.update(DL_RESUME_CHECKPOINT=str(private),DL_RESUME_SHA256=head.sha256,
                   DL_RESUME_STATUS_PATH=str(first/'resume-status.json'),
                   DL_EXECUTION_LEASE_PATH=str(worker_lease.lease_path(self.f.worker,record['lease'])),DL_WORKER_ROOT=str(self.f.worker))
        execute('normal',env)
        self.assertEqual(self.runtime.reconcile(self.job()),'RUNNING')
        with patch.object(dispatch_fixture.subprocess,'Popen',self.f.real_popen):
            import torch
        def equal(a,b):
            if isinstance(a,torch.Tensor):return torch.equal(a,b)
            if isinstance(a,dict):return a.keys()==b.keys() and all(equal(a[k],b[k]) for k in a)
            if isinstance(a,(list,tuple)):return len(a)==len(b) and all(equal(x,y) for x,y in zip(a,b))
            return a==b
        self.assertTrue(equal(torch.load(base/'final.pt',weights_only=True),torch.load(destination/'final.pt',weights_only=True)))

    def test_checkpoint_cold_launch_without_trusted_worker_service_fails_closed(self):
        del self.f.agent.app.extensions['checkpoint_publication']
        fixtures.store.enqueue_custom_job(self.f.job_id,['pi01'],storage_runtime=self.runtime)
        import requests
        with self.assertRaises(requests.HTTPError):self.f.dispatch()
        self.f.popen.assert_not_called();self.assertEqual(self.job()['status'],'SCHEDULED')

    def test_unsigned_restore_rejected_and_no_staging_receipt_cannot_restore(self):
        response=self.f.http.post('/jobs/'+self.f.job_id+'/custom-checkpoint',data=b'untrusted')
        self.assertEqual(response.status_code,403);self.f.popen.assert_not_called()


if __name__=='__main__':unittest.main(verbosity=2)
