"""9.4 local DDP matrix: owned native CPU/Gloo children plus signed fixture agents.

Only disposable jobs/files/PIDs are faulted. Outer launcher/isolation and NFS
mount facts remain mocked. These tests do not satisfy physical Pi acceptance.
"""
from contextlib import closing
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
import test_hybrid_checkpoint_ddp_resume as native
import test_hybrid_checkpoint_recovery as recovery
from test_hybrid_checkpoint_recovery import fixtures,leases
from checkpoint_protocol import group_identity,validate_group


FAULT_WORKER=native.NATIVE_WORKER.replace(
    "dist.init_process_group('gloo',init_method=",
    "if case=='before-training' and rank==1:\n    (root/'before-training').write_text('ready')\n    while True:time.sleep(0.05)\ndist.init_process_group('gloo',init_method=").replace(
    "        manager.save(model,optimizer,",
    "        if case=='checkpoint-crash' and step==2 and rank==1:\n            def pause(proposal,state_sha):\n                (root/'awaiting-checkpoint-ack').write_text('ready')\n                while True:time.sleep(0.05)\n            manager._acknowledge=pause\n        manager.save(model,optimizer,")


class NativeMatrixTests(unittest.TestCase):
    def setUp(self):
        self.n=native.RealHybridDDPTests(methodName='runTest')
        self.n.world_size=2 if self._testMethodName.startswith('test_two_rank_native_committed') else 3
        self.addCleanup(self.n.doCleanups);self.n.setUp()
        self.r,self.f=self.n.r,self.n.f
        # A different student's HELD job and files must survive every fault.
        other,folder,_=self.f.fixture.job(owner='bob')
        marker=folder/'results/foreign-result.txt';marker.parent.mkdir(exist_ok=True)
        marker.write_bytes(b'keep another student result')
        self.other,self.other_folder=other,folder
        self.other_state=self.foreign_state()
        self.addCleanup(lambda:self.assertEqual(self.foreign_state(),self.other_state))

    def foreign_state(self):
        job=fixtures.store.get_job(self.other)
        files={str(p.relative_to(self.other_folder)):hashlib.sha256(p.read_bytes()).hexdigest()
               for p in self.other_folder.rglob('*') if p.is_file()}
        events=self.r.db.execute('SELECT COUNT(*) FROM recovery_events WHERE job_id=?',(self.other,)).fetchone()[0]
        return job,files,events

    def cold(self):
        settings=leases.settings()
        with patch.object(leases,'settings',return_value={**settings,'duration_seconds':60}):self.r.cold()

    def fail_owned_group(self):
        self.n.reap()  # Never claim quiescence while a native child is alive.
        self.f.process.poll.return_value=1
        for service in self.n.services:
            service.quiescent=lambda *_:True
            self.assertFalse(service.note_exit(service.module.current_job['lease'],1));service.tick()
        with patch('job_artifacts.collect') as results:
            self.assertEqual(self.r.runtime.reconcile(self.r.job()),'QUEUED');results.assert_not_called()
        self.assertEqual(fixtures.store.get_reservations(),[])
        self.assertEqual(self.r.job()['retry_count'],1)
        for service in self.n.services:
            with service.fetcher.cache._session() as (db,_,__):
                self.assertEqual(db.execute('SELECT COUNT(*) FROM cache_pins').fetchone()[0],0)
            service.module.current_job=None;service.module.job_process=None
        self.f.process.poll.return_value=None

    def complete(self,raw):
        final=self.r.save(raw=raw);self.assertTrue(final.completed)
        self.f.process.poll.return_value=0
        for service in self.n.services:
            service.quiescent=lambda *_:True
            self.assertTrue(service.note_exit(service.module.current_job['lease'],0));service.tick()
        with patch('job_artifacts.collect') as results:
            self.assertEqual(self.r.runtime.reconcile(self.r.job()),'COMPLETED')
            self.assertEqual(results.call_count,self.n.world_size)
            self.assertTrue(all(call.args[0]['job_id']==self.f.job_id for call in results.call_args_list))
        self.assertEqual(fixtures.store.get_reservations(),[])
        self.assertEqual([row['status'] for row in fixtures.store.get_ranks(self.f.job_id,1)],['COMPLETED']*self.n.world_size)
        self.assertEqual([row['status'] for row in fixtures.store.get_ranks(self.f.job_id,0)],['STOPPED']*self.n.world_size)
        return final

    def test_two_rank_native_committed_full_group_recovery_and_state_match(self):
        self.n.test_sigkill_rank_then_committed_three_rank_resume_matches_baseline_with_stated_fp32_tolerance()
        self.assertEqual([row['status'] for row in fixtures.store.get_ranks(self.f.job_id,1)],['COMPLETED']*2)
        self.assertEqual([row['status'] for row in fixtures.store.get_ranks(self.f.job_id,0)],['STOPPED']*2)

    def test_three_rank_process_crash_before_training_retries_full_group_without_foreign_damage(self):
        self.n.program.write_text(FAULT_WORKER);self.cold()
        paths=self.n.run_group('before-training',pause_marker='before-training',pause_ranks=[1])
        self.assertFalse(any((path/'last-step').exists() for path in paths))
        self.assertEqual(len(fixtures.store.get_reservations()),3)
        self.n.processes[1].kill();self.assertEqual(self.n.processes[1].wait(timeout=10),-9)
        self.fail_owned_group()
        self.assertIsNone(self.r.publisher.head('alice',self.f.job_id))
        self.cold()
        completed=self.n.run_group('cold-retry')[0]/'latest.dlckpt'
        final=self.complete(completed.read_bytes());self.assertEqual(final.retry_count,1)
        self.assertEqual([row['status'] for row in fixtures.store.get_ranks(self.f.job_id,1)],['COMPLETED']*3)

    def test_three_rank_crash_during_checkpoint_ack_keeps_old_cut_then_recovers_entire_group(self):
        self.n.program.write_text(FAULT_WORKER)
        baseline=self.n.run_group('baseline',baseline=True)[0]/'latest.dlckpt'
        self.cold()
        paths=self.n.run_group('checkpoint-crash',pause_marker='awaiting-checkpoint-ack',pause_ranks=[1])
        candidate=paths[0]/'latest.dlckpt'
        old=validate_group(candidate,self.f.job_id,self.r.job()['storage_manifest_sha256'])
        self.assertEqual(old['global_step'],1)
        staged=list(paths[0].glob('.ddp-stage-*'));self.assertEqual(len(staged),1)
        proposal=validate_group(staged[0],self.f.job_id,self.r.job()['storage_manifest_sha256'])
        self.assertEqual(proposal['global_step'],2)  # Proposed ACKs are not a committed SDK save.
        head=self.r.save(raw=candidate.read_bytes());self.assertEqual(head.global_step,1)
        old_job=self.r.job();old_lease=leases.get(old_job,'pi01')
        self.n.processes[1].kill();self.assertEqual(self.n.processes[1].wait(timeout=10),-9)
        self.assertEqual(hashlib.sha256(candidate.read_bytes()).hexdigest(),head.sha256)
        self.fail_owned_group()
        self.assertEqual(self.r.publisher.head('alice',self.f.job_id),head)
        self.assertEqual(self.r.db.execute("SELECT COUNT(*) FROM hybrid_checkpoint_publications WHERE phase='COMMITTED'").fetchone()[0],1)
        self.assertFalse(self.n.services[0].note_exit(old_lease,0))
        with self.assertRaises(ValueError):
            with leases.publication(old_job,'pi01'):self.fail('Stale rank result accepted')
        before=[service.fetcher.cache.metrics.snapshot().staged_bytes for service in self.n.services]
        settings=leases.settings()
        with patch.object(leases,'settings',return_value={**settings,'duration_seconds':60}):self.assertTrue(self.f.dispatch())
        self.assertEqual(self.r.job()['status'],'SCHEDULED')
        self.assertEqual(before,[service.fetcher.cache.metrics.snapshot().staged_bytes for service in self.n.services])
        resumed=self.n.run_group('resume',source=head)[0]/'latest.dlckpt'
        self.assertEqual(self.r.runtime.reconcile(self.r.job()),'RUNNING')
        expected=self.n.states(baseline);actual=self.n.states(resumed)
        for a,b in zip(expected,actual):self.n.assert_state(a,b)
        print('ACK_CRASH_THREE_RANK_STATE_DIFFERENCES '+json.dumps({str(rank):self.n.differences(expected[rank],actual[rank]) for rank in range(3)},sort_keys=True),flush=True)
        final=self.complete(resumed.read_bytes());self.assertEqual(final.global_step,6)
        self.assertEqual(self.r.db.execute("SELECT COUNT(*) FROM recovery_events WHERE event_type='DDP_RESUMED'").fetchone()[0],1)

    def test_native_two_rank_load_rejects_real_three_rank_checkpoint_without_training(self):
        source=self.n.run_group('source-three',baseline=True)[0]/'latest.dlckpt'
        job=self.r.job();data=source.read_bytes();processes=[];logs=[]
        script=self.f.fixture.base/'world-mismatch.py'
        script.write_text(native.NATIVE_WORKER.replace("('corrupt','different-cut','callback-error')",
            "('corrupt','different-cut','callback-error','world-mismatch')"))
        context=dict(world_size=2,master_addr='127.0.0.1',master_port=29622)
        try:
            for rank in range(2):
                folder=self.f.fixture.base/'world-mismatch'/str(rank);folder.mkdir(parents=True)
                resume=folder/'resume.dlckpt';resume.write_bytes(data)
                env={**os.environ,'PYTHONPATH':str(ROOT/'agent'),'OMP_NUM_THREADS':'1','MKL_NUM_THREADS':'1',
                    'GLOO_SOCKET_IFNAME':'lo','RANK':str(rank),'WORLD_SIZE':'2','MASTER_ADDR':context['master_addr'],
                    'MASTER_PORT':str(context['master_port']),'DL_TEST_RENDEZVOUS':str(self.f.fixture.base/'world-mismatch-rendezvous'),
                    'DL_JOB_ID':job['job_id'],'DL_MANIFEST_SHA256':job['storage_manifest_sha256'],'DL_COMPATIBILITY_KEY':'c'*64,
                    'DL_ATTEMPT_TOKEN':f'{job["job_id"]}:1','DL_DDP_GROUP_ID':group_identity(job['job_id'],f'{job["job_id"]}:1',job['storage_manifest_sha256'],'c'*64,context),
                    'DL_CHECKPOINT_DIR':str(folder),'DL_CHECKPOINT_INTERVAL_SECONDS':'1','DL_LAUNCHER_PID':'99999999',
                    'DL_RESUME_STATUS_PATH':str(folder/'resume-status.json'),'DL_RESUME_CHECKPOINT':str(resume),
                    'DL_RESUME_SHA256':hashlib.sha256(data).hexdigest()}
                log=(folder/'process.log').open('w');logs.append(log)
                processes.append(self.f.real_popen([sys.executable,str(script),'world-mismatch'],env=env,stdout=log,stderr=subprocess.STDOUT))
            for rank,process in enumerate(processes):
                folder=self.f.fixture.base/'world-mismatch'/str(rank)
                self.assertEqual(process.wait(timeout=40),0,(folder/'process.log').read_text())
                self.assertIn('worker count',json.loads((folder/'error.json').read_text())['error'])
                self.assertFalse((folder/'resume-status.json').exists());self.assertFalse((folder/'last-step').exists())
        finally:
            for process in processes:
                if process.poll() is None:process.kill()
            for process in processes:process.wait(timeout=10)
            for log in logs:log.close()


class ControllerMatrixTests(unittest.TestCase):
    def setUp(self):
        self.r=recovery.RecoveryTests(methodName='runTest');self.addCleanup(self.r.doCleanups);self.r.setUp()
        self.f=self.r.f;self.r.ddp()

    def test_controller_rejects_changed_world_size_before_any_new_reservation_or_launch(self):
        self.r.cold();head=self.r.save();self.r.fail();self.f.popen.reset_mock()
        spec=json.loads(self.r.job()['job_config']);spec['execution']['workers']=2
        with self.r.db:self.r.db.execute('UPDATE jobs SET job_config=? WHERE job_id=?',(json.dumps(spec),self.f.job_id))
        with self.assertRaises((ValueError,PermissionError)):self.f.dispatch()
        self.f.popen.assert_not_called();self.assertEqual(fixtures.store.get_reservations(),[])
        self.assertEqual(self.r.db.execute('SELECT publication_id FROM hybrid_checkpoint_heads WHERE job_id=?',(self.f.job_id,)).fetchone()[0],head.publication_id)

    def test_competing_full_group_recoveries_reserve_launch_and_confirm_once(self):
        from concurrent.futures import ThreadPoolExecutor
        from hybrid_storage.checkpoint_publication import LocalCheckpointPublisher
        from hybrid_storage.checkpoint_transfer import CheckpointCollector
        from hybrid_storage.checkpoint_restore import CheckpointRestorer
        from hybrid_storage.config import StorageConfig
        from checkpoint_recovery import CheckpointDispatch
        self.r.cold();self.r.save();self.r.fail();self.f.popen.reset_mock()
        barrier=threading.Barrier(2);nodes=self.f.nodes
        def selected(*args,**kwargs):barrier.wait(timeout=15);return nodes
        def run(_):
            with closing(self.f.fixture.connect()) as db:
                registry=type(self.r.registry)(db,self.r.registry.publisher)
                publisher=LocalCheckpointPublisher(registry,self.r.root,reserve_free_bytes=0)
                keys=self.f.client.credentials
                runtime=CheckpointDispatch(registry,self.f.client,publisher=publisher,
                    collector=CheckpointCollector(publisher,keys,get=self.r.get),
                    restorer=CheckpointRestorer(publisher,keys,post=self.r.post),settings=StorageConfig('nfs',True,True,'resource'))
                try:return runtime.dispatch(self.r.job(),set())
                except ValueError as error:return type(error).__name__
        with patch('distributed_dispatch.select_group',side_effect=selected),ThreadPoolExecutor(max_workers=2) as pool:
            outcomes=list(pool.map(run,range(2)))
        self.assertEqual(outcomes.count(True),1,outcomes)
        self.assertTrue(any(isinstance(result,str) for result in outcomes),outcomes)
        self.assertEqual(self.f.popen.call_count,3);self.assertEqual(len(fixtures.store.get_reservations()),3)
        self.assertEqual(self.r.db.execute('SELECT COUNT(*) FROM hybrid_staging_attempts WHERE retry_count=1').fetchone()[0],1)
        for service in [self.f.service,*self.f.extra_services]:self.r.ack(service)
        self.assertEqual(self.r.runtime.reconcile(self.r.job()),'RUNNING')
        self.assertEqual(self.r.db.execute("SELECT COUNT(*) FROM recovery_events WHERE event_type='DDP_RESUMED'").fetchone()[0],1)


if __name__=='__main__':unittest.main(verbosity=2)
