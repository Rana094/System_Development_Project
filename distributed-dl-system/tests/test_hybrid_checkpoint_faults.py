"""8.4 fault matrix: disposable DB/files, signed Flask control, mocked launcher.

Only owned disposable files/processes are faulted. No network listener or Pi.
"""
from contextlib import closing
import errno
import hashlib
import io
import json
from pathlib import Path
import sys
import time
import threading
from threading import Thread as NativeThread
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
import test_hybrid_checkpoint_recovery as recovery
from test_hybrid_checkpoint_recovery import fixtures,leases,worker_lease
import requests
from hybrid_storage import checkpoint_publication as publication
from hybrid_storage.checkpoint_publication import LocalCheckpointPublisher
from hybrid_storage.checkpoint_transfer import CheckpointCollector
from hybrid_storage.checkpoint_restore import CheckpointRestorer
from hybrid_storage.config import StorageConfig
from hybrid_storage.staging_protocol import STAGE_PATH,START_PATH,parse_plan
from checkpoint_recovery import CheckpointDispatch
import checkpoint_restore_service as restore_service


class CheckpointFaultTests(unittest.TestCase):
    def setUp(self):
        self.r=recovery.RecoveryTests(methodName='runTest')
        self.addCleanup(self.r.doCleanups);self.r.setUp()
        self.f=self.r.f

    def reopen(self):
        old=self.r.db;db=self.f.fixture.connect();self.addCleanup(db.close)
        registry=type(self.r.registry)(db,self.r.registry.publisher)
        self.r.db=self.f.db=self.f.fixture.db=db
        self.r.registry=self.f.registry=registry
        self.r.publisher=LocalCheckpointPublisher(registry,self.r.root,reserve_free_bytes=0)
        self.r.publisher.initialize()
        self.r.runtime=self.f.runtime=self.r.coordinator();old.close()
        return db

    def expire(self,retry):
        with self.r.db:
            self.r.db.execute('UPDATE execution_leases SET expires_at=0,expires_clock=0 WHERE retry_count=?',(retry,))
        # Model elapsed time on both independent clocks, not a controller-only
        # deadline rewrite that a disconnected worker cannot observe.
        for service in [self.f.service,*getattr(self.f,'extra_services',[])]:
            path=worker_lease.authority_path(service.root)
            if not path.exists():continue
            record=worker_lease.read(path)
            if record['lease']['attempt_token'].rsplit(':',1)[1]!=str(retry):continue
            record['lease'].update(issued_at=time.time()-40,expires_at=time.time()-10)
            record['deadline']=worker_lease.clock()-10
            worker_lease.atomic(path,record);worker_lease.atomic(worker_lease.lease_path(service.root,record['lease']),record)

    def quiet(self,code=1):
        self.f.process.poll.return_value=code
        for service in [self.f.service,*getattr(self.f,'extra_services',[])]:
            service.quiescent=lambda *_:True

    def test_worker_connection_loss_retains_reservation_until_fenced_then_late_result_rejected(self):
        r=self.r;r.cold();head=r.save();old=r.job();old_lease=leases.get(old,'pi01')
        with patch.object(self.f.client,'post',side_effect=requests.Timeout('disposable worker offline')):
            self.assertEqual(r.runtime.reconcile(old),'PENDING')
            self.assertEqual(len(fixtures.store.get_reservations()),1)
            self.expire(0)
            self.assertEqual(r.runtime.reconcile(old),'QUEUED')
        self.assertEqual(fixtures.store.get_reservations(),[])
        self.assertFalse(self.f.service.note_exit(old_lease,0))
        with self.assertRaises(ValueError):
            with leases.publication(old,'pi01'):self.fail('Stale result publication entered')
        self.assertEqual(r.publisher.head('alice',self.f.job_id),head)
        self.quiet();r.runtime.tick()
        self.assertEqual(r.db.execute('SELECT phase FROM hybrid_cleanup_ranks').fetchone()[0],'QUIESCENT')
        self.assertNotEqual(r.job()['status'],'COMPLETED')

    def test_interrupted_upload_and_real_connection_reopen_keep_only_previous_commit(self):
        r=self.r;r.cold();first=r.save();raw=r.raw(step=6)
        (self.f.worker/'worker-storage/jobs'/self.f.job_id/'checkpoints/latest.dlckpt').write_bytes(raw)
        original=r.runtime.collector.get
        def interrupted(url,**kwargs):
            response=original(url,**kwargs);body=response.content
            def chunks(size):
                yield body[:80]
                raise requests.ConnectionError('disposable mid-body loss')
            response.iter_content=chunks;return response
        with patch.object(r.runtime.collector,'get',side_effect=interrupted):
            with self.assertRaises(requests.ConnectionError):r.runtime.sync(r.job(),r.runtime.snapshot(self.f.job_id)[2])
        self.assertEqual(r.publisher.head('alice',self.f.job_id),first)
        rejected=r.db.execute("SELECT publication_id FROM hybrid_checkpoint_publications WHERE phase='REJECTED'").fetchone()[0]
        self.assertEqual((r.root/'pending'/rejected/'candidate').stat().st_size,80)
        self.reopen()
        self.assertEqual(r.publisher.head('alice',self.f.job_id),first)
        r.fail();self.assertTrue(self.f.dispatch())
        frozen=r.db.execute('SELECT source_id FROM hybrid_restore_attempts WHERE retry_count=1').fetchone()[0]
        self.assertEqual(frozen,first.publication_id)
        self.assertEqual(r.job()['status'],'SCHEDULED')

    def test_process_death_after_checkpoint_rename_does_not_promote_orphan_on_restart(self):
        r=self.r;r.cold();head=r.save();raw=r.raw(step=6)
        (self.f.worker/'worker-storage/jobs'/self.f.job_id/'checkpoints/latest.dlckpt').write_bytes(raw)
        class PowerLoss(BaseException):pass
        rename=publication._rename_noreplace
        def crash(*args):rename(*args);raise PowerLoss('disposable controller crash boundary')
        with patch.object(publication,'_rename_noreplace',side_effect=crash):
            with self.assertRaises(PowerLoss):r.runtime.sync(r.job(),r.runtime.snapshot(self.f.job_id)[2])
        orphan=r.db.execute("SELECT publication_id FROM hybrid_checkpoint_publications WHERE phase='PUBLISHING'").fetchone()[0]
        self.assertTrue((r.root/'commits'/(orphan+'.dlckpt')).exists())
        self.reopen();r.fail();self.assertTrue(self.f.dispatch())
        self.assertEqual(r.db.execute('SELECT source_id FROM hybrid_restore_attempts WHERE retry_count=1').fetchone()[0],head.publication_id)
        with self.assertRaises(PermissionError):r.publisher.committed('alice',orphan)

    def test_sigkill_controller_publisher_process_after_rename_preserves_head_on_reopen(self):
        r=self.r;r.cold();head=r.save();raw=r.raw(step=6)
        base=self.f.fixture.base;candidate=base/'native-publication.dlckpt';candidate.write_bytes(raw)
        marker=base/'native-publisher-renamed';log=base/'native-publisher.log'
        program="""import hashlib,json,sqlite3,sys,time
from pathlib import Path
sys.path.insert(0,sys.argv[1])
from hybrid_storage.artifacts import ArtifactRegistrar
from hybrid_storage.registry import StorageRegistry
from hybrid_storage.checkpoint_publication import LocalCheckpointPublisher
from hybrid_storage import checkpoint_publication as publication
args=json.loads(sys.argv[2]);db=sqlite3.connect(args['db'],timeout=10)
db.execute('PRAGMA foreign_keys=ON');db.execute('PRAGMA synchronous=FULL')
registry=StorageRegistry(db,ArtifactRegistrar(Path(args['objects']),staging_root=Path(args['scratch'])))
publisher=LocalCheckpointPublisher(registry,Path(args['root']),reserve_free_bytes=0)
original=publication._rename_noreplace
def pause(*items):
 original(*items);Path(args['marker']).write_text('renamed')
 while True:time.sleep(0.05)
publication._rename_noreplace=pause
raw=Path(args['candidate']).read_bytes()
with open(args['candidate'],'rb') as source:
 publisher.publish('alice',args['job'],'pi01',source,publication_id='7'*32,expected_head=args['head'],expected_sha256=hashlib.sha256(raw).hexdigest(),expected_size=len(raw))
"""
        args=dict(db=str(self.f.fixture.db_path),objects=str(r.registry.publisher.root),scratch=str(r.registry.publisher.staging_root),
                  root=str(r.root),marker=str(marker),candidate=str(candidate),job=self.f.job_id,head=head.publication_id)
        import subprocess
        with log.open('w') as output:
            process=self.f.real_popen([sys.executable,'-I','-B','-c',program,str(ROOT),json.dumps(args)],stdout=output,stderr=subprocess.STDOUT)
            try:
                deadline=time.monotonic()+15
                while not marker.exists():
                    if process.poll() is not None or time.monotonic()>deadline:self.fail(log.read_text() or 'Native publisher never reached rename')
                    time.sleep(0.02)
                process.kill();self.assertEqual(process.wait(timeout=5),-9)
            finally:
                if process.poll() is None:process.kill();process.wait(timeout=5)
        self.reopen()
        self.assertEqual(r.publisher.head('alice',self.f.job_id),head)
        self.assertEqual(r.db.execute("SELECT phase FROM hybrid_checkpoint_publications WHERE publication_id=?",('7'*32,)).fetchone()[0],'PUBLISHING')
        with self.assertRaises(PermissionError):r.publisher.committed('alice','7'*32)
        r.fail();self.assertTrue(self.f.dispatch())
        self.assertEqual(r.db.execute('SELECT source_id FROM hybrid_restore_attempts WHERE retry_count=1').fetchone()[0],head.publication_id)

    def test_missing_old_checkpoint_is_not_hidden_by_newer_incomplete_or_legacy_file(self):
        r=self.r;r.cold();head=r.save();r.fail()
        path=r.root/'commits'/(head.publication_id+'.dlckpt');path.unlink()  # disposable fault only
        legacy=Path(r.job()['storage_path'])/'checkpoints/latest.dlckpt';legacy.write_bytes(b'looks like a legacy checkpoint')
        self.f.popen.reset_mock();self.reopen()
        with self.assertRaises(FileNotFoundError):self.f.dispatch()
        self.assertEqual(r.job()['status'],'QUEUED');self.assertEqual(fixtures.store.get_reservations(),[])
        self.f.popen.assert_not_called();self.assertEqual(legacy.read_bytes(),b'looks like a legacy checkpoint')

    def test_corrupt_committed_final_never_publishes_results_or_completes(self):
        r=self.r;r.cold();head=r.save(completed=True)
        path=r.root/'commits'/(head.publication_id+'.dlckpt');path.chmod(0o600);path.write_bytes(b'bit rot');path.chmod(0o400)
        self.quiet(0)
        self.assertTrue(self.f.service.note_exit(self.f.agent.current_job['lease'],0));self.f.service.tick()
        with patch('job_artifacts.collect') as results:
            self.assertEqual(r.runtime.reconcile(r.job()),'FINAL_CHECKPOINT_PENDING')
            self.assertEqual(r.runtime.reconcile(r.job()),'FINAL_CHECKPOINT_PENDING')
            self.assertEqual(r.runtime.reconcile(r.job()),'QUEUED')
            results.assert_not_called()
        self.assertEqual(r.db.execute('SELECT final_failures FROM hybrid_restore_attempts WHERE retry_count=0').fetchone()[0],3)
        self.assertEqual(r.job()['retry_count'],1)

    def test_partial_restore_body_and_no_space_never_publish_private_resume_or_launch(self):
        r=self.r;r.cold();head=r.save();r.fail();self.f.popen.reset_mock()
        original=r.runtime.restorer.post
        def truncated(url,**kwargs):
            body=kwargs['data'].read();kwargs['data']=io.BytesIO(body[:-20])
            return original(url,**kwargs)
        with patch.object(r.runtime.restorer,'post',side_effect=truncated):
            with self.assertRaises(requests.HTTPError):self.f.dispatch()
        self.f.popen.assert_not_called()
        self.assertEqual(list(self.f.workspace_root.glob('attempts/*/checkpoints/resume.dlckpt')),[])
        self.quiet();job,stage,members=r.runtime.snapshot(self.f.job_id)
        r.runtime.abort_intent(job,stage,'Disposable truncated restore');self.assertEqual(r.runtime.cleanup(job,stage,members),'QUEUED')
        self.f.process.poll.return_value=None
        with patch.object(restore_service,'space',side_effect=OSError(errno.ENOSPC,'disposable full disk')):
            with self.assertRaises(requests.HTTPError):self.f.dispatch()
        self.f.popen.assert_not_called();self.assertEqual(r.publisher.head('alice',self.f.job_id),head)
        self.assertEqual(list(self.f.workspace_root.glob('attempts/*/checkpoints/resume.dlckpt')),[])
        self.assertFalse(list(self.f.workspace_root.glob('attempts/*/checkpoints/*.part')))

    def test_lost_restore_reply_and_controller_restart_never_blindly_launches(self):
        r=self.r;r.cold();r.save();r.fail();self.f.popen.reset_mock();post=r.runtime.restorer.post
        def lost(url,**kwargs):
            response=post(url,**kwargs);self.assertEqual(response.status_code,200)
            raise requests.Timeout('restore persisted but ACK lost')
        with patch.object(r.runtime.restorer,'post',side_effect=lost):
            with self.assertRaises(requests.Timeout):self.f.dispatch()
        self.assertEqual(len(list(self.f.workspace_root.glob('attempts/*/checkpoints/resume.dlckpt'))),1)
        self.reopen();self.quiet()
        self.assertEqual(r.runtime.reconcile(r.job()),'QUEUED');self.f.popen.assert_not_called()
        self.assertEqual(r.job()['retry_count'],2)

    def test_lost_start_reply_on_recovery_fences_instead_of_starting_twice(self):
        r=self.r;r.cold();head=r.save();r.fail();self.f.popen.reset_mock();post=self.f.client.post
        def lost(url,**kwargs):
            response=post(url,**kwargs)
            if url.endswith(START_PATH):raise requests.Timeout('start accepted but ACK lost')
            return response
        with patch.object(self.f.client,'post',side_effect=lost):
            with self.assertRaises(requests.Timeout):self.f.dispatch()
        self.assertEqual(self.f.popen.call_count,1)
        self.assertEqual(r.db.execute('SELECT launch_sent,started_at FROM hybrid_restore_ranks WHERE stage_id=(SELECT stage_id FROM hybrid_restore_attempts WHERE retry_count=1)').fetchone(),(1,None))
        self.reopen();self.quiet()
        self.assertEqual(r.runtime.reconcile(r.job()),'QUEUED')
        self.assertEqual(self.f.popen.call_count,1);self.assertEqual(r.publisher.head('alice',self.f.job_id),head)

    def test_restart_after_sdk_proof_event_before_running_is_idempotent(self):
        r=self.r;r.cold();r.save();r.fail();self.assertTrue(self.f.dispatch());r.ack()
        class PowerLoss(BaseException):pass
        from staging_dispatch import StagingDispatch
        with patch.object(StagingDispatch,'group_started',side_effect=PowerLoss('after durable restore confirmation')):
            with self.assertRaises(PowerLoss):r.runtime.reconcile(r.job())
        self.assertEqual(r.job()['status'],'SCHEDULED')
        self.assertEqual(r.db.execute("SELECT COUNT(*) FROM recovery_events WHERE event_type='RESUMED'").fetchone()[0],1)
        self.reopen();self.f.popen.reset_mock()
        self.assertEqual(r.runtime.reconcile(r.job()),'RUNNING');self.f.popen.assert_not_called()
        self.assertEqual(r.db.execute("SELECT COUNT(*) FROM recovery_events WHERE event_type='RESUMED'").fetchone()[0],1)

    def test_nfs_outage_warm_recovery_uses_zero_missing_bytes_and_cold_replacement_fails_closed(self):
        r=self.r;node,service,cache=r.replacement();r.cold();head=r.save();r.fail()
        with patch.object(self.f.fetcher,'_spawn',side_effect=AssertionError('Warm recovery must not read NFS')):
            self.assertTrue(self.f.dispatch())
        r.ack();self.assertEqual(r.runtime.reconcile(r.job()),'RUNNING')
        r.fail();self.f.popen.reset_mock()
        with patch.object(recovery.dispatch_fixture.dispatcher,'select_best_node',return_value={'node':node,'resources':{}}), \
             patch.object(service.fetcher,'_spawn',side_effect=OSError(errno.ENETUNREACH,'disposable NFS outage')):
            with self.assertRaises(requests.HTTPError):self.f.dispatch()
        self.f.popen.assert_not_called();self.assertEqual(r.job()['status'],'SCHEDULED')
        self.assertEqual(cache.metrics.snapshot().staged_bytes,0)
        self.assertEqual(r.publisher.head('alice',self.f.job_id),head)

    def test_ddp_stale_rank_checkpoint_result_rejected_for_entire_group(self):
        r=self.r;r.ddp();r.cold();head=r.save();old=r.job();old_bytes=r.raw(step=8)
        r.fail();self.assertTrue(self.f.dispatch())
        for service in [self.f.service,*self.f.extra_services]:r.ack(service)
        self.assertEqual(r.runtime.reconcile(r.job()),'RUNNING')
        path=self.f.worker/'worker-storage/jobs'/self.f.job_id/'checkpoints/latest.dlckpt';path.write_bytes(old_bytes)
        with self.assertRaises(requests.HTTPError):r.runtime.sync(r.job(),r.runtime.snapshot(self.f.job_id)[2])
        with self.assertRaises(ValueError):
            with leases.publication(old,'pi01'):self.fail('Old DDP result accepted')
        self.assertEqual(r.publisher.head('alice',self.f.job_id),head)
        self.assertNotEqual(r.job()['status'],'COMPLETED')

    def test_restore_connection_dies_after_partial_read_no_receipt_or_private_file_accepted(self):
        r=self.r;r.cold();head=r.save();r.fail();self.f.popen.reset_mock()
        original=restore_service.stream_copy;copied=[]
        def interrupted(source,destination,**kwargs):
            class Broken:
                started=False
                def read(self,size):
                    if self.started:
                        copied.append(destination.tell())
                        raise OSError(errno.ECONNRESET,'disposable restore disconnected')
                    self.started=True;return source.read(min(size,64))
            return original(Broken(),destination,**kwargs)
        with patch.object(restore_service,'stream_copy',side_effect=interrupted):
            with self.assertRaises(requests.HTTPError):self.f.dispatch()
        self.assertEqual(copied,[64]);self.f.popen.assert_not_called()
        self.assertEqual(list(self.f.workspace_root.glob('attempts/*/checkpoints/resume.dlckpt')),[])
        service=self.f.agent.app.extensions['checkpoint_publication']
        self.assertEqual(list(service.spool.glob('restores/*/receipt.json')),[])
        self.assertEqual(r.publisher.head('alice',self.f.job_id),head)

    def test_controller_host_boot_mismatch_never_renews_old_owner_or_reuses_live_slot(self):
        r=self.r;r.cold();head=r.save();old=r.job()
        with r.db:r.db.execute("UPDATE execution_leases SET controller_boot_id='disposable-old-host-boot'")
        self.reopen()
        with patch.object(self.f.client,'post',side_effect=requests.Timeout('disposable disconnected worker')), \
             patch.object(leases,'START_CLOCK',leases.clock()):
            self.assertEqual(r.runtime.reconcile(old),'PENDING')
            self.assertEqual(len(fixtures.store.get_reservations()),1)
            self.assertEqual(leases.get(r.job(),'pi01')['renewal_blocked'],1)
            self.expire(0)
            self.assertEqual(r.runtime.reconcile(r.job()),'PENDING')  # New host has not waited its guard interval.
            with patch.object(leases,'START_CLOCK',leases.clock()-100):
                self.assertEqual(r.runtime.reconcile(r.job()),'QUEUED')
        self.assertEqual(r.publisher.head('alice',self.f.job_id),head)

    def test_incomplete_new_ddp_bundle_never_displaces_last_recoverable_commit(self):
        import zipfile
        r=self.r;r.ddp();r.cold();head=r.save()
        with zipfile.ZipFile(io.BytesIO(r.raw(step=6))) as archive:
            entries={name:archive.read(name) for name in archive.namelist()}
        meta=json.loads(entries['metadata.json']);meta['rank_acknowledgments']=meta['rank_acknowledgments'][:2]
        entries['metadata.json']=json.dumps(meta).encode();output=io.BytesIO()
        with zipfile.ZipFile(output,'w') as archive:
            for name,data in entries.items():archive.writestr(name,data)
        (self.f.worker/'worker-storage/jobs'/self.f.job_id/'checkpoints/latest.dlckpt').write_bytes(output.getvalue())
        with self.assertRaises(requests.HTTPError):r.runtime.sync(r.job(),r.runtime.snapshot(self.f.job_id)[2])
        self.assertEqual(r.publisher.head('alice',self.f.job_id),head)
        r.fail();self.assertTrue(self.f.dispatch())
        self.assertEqual(r.db.execute('SELECT source_id FROM hybrid_restore_attempts WHERE retry_count=1').fetchone()[0],head.publication_id)

    def runtime_on(self,db):
        r=self.r;registry=type(r.registry)(db,r.registry.publisher)
        publisher=LocalCheckpointPublisher(registry,r.root,reserve_free_bytes=0)
        keys=self.f.client.credentials
        return CheckpointDispatch(registry,self.f.client,publisher=publisher,
            collector=CheckpointCollector(publisher,keys,get=r.get),
            restorer=CheckpointRestorer(publisher,keys,post=r.post),settings=StorageConfig('nfs',True,True,'resource'))

    def test_two_sqlite_connections_competing_recovery_reserve_and_launch_once(self):
        r=self.r;r.cold();r.save();r.fail();self.f.popen.reset_mock()
        barrier=threading.Barrier(2);outcomes=[]
        def selected(**kwargs):
            barrier.wait(timeout=15);return {'node':self.f.node,'resources':{}}
        def run():
            try:
                with closing(self.f.fixture.connect()) as db:
                    runtime=self.runtime_on(db);outcomes.append(runtime.dispatch(r.job(),set()))
            except ValueError as error:outcomes.append(type(error).__name__)
            except BaseException as error:outcomes.append(error)
        with patch.object(recovery.dispatch_fixture.dispatcher,'select_best_node',side_effect=selected):
            workers=[NativeThread(target=run) for _ in range(2)]
            for worker in workers:worker.start()
            for worker in workers:worker.join(timeout=25)
            self.assertFalse(any(worker.is_alive() for worker in workers),'Recovery race did not finish')
        self.assertEqual(outcomes.count(True),1,repr(outcomes));self.assertEqual(len(outcomes),2)
        self.assertTrue(any(isinstance(value,str) for value in outcomes),repr(outcomes))
        self.assertEqual(self.f.popen.call_count,1);self.assertEqual(len(fixtures.store.get_reservations()),1)
        self.assertEqual(r.db.execute('SELECT COUNT(*) FROM hybrid_staging_attempts WHERE retry_count=1').fetchone()[0],1)
        self.reopen();r.ack();self.assertEqual(r.runtime.reconcile(r.job()),'RUNNING')
        self.assertEqual(r.db.execute("SELECT COUNT(*) FROM recovery_events WHERE event_type='RESUMED'").fetchone()[0],1)


if __name__=='__main__':unittest.main(verbosity=2)
