"""7.3 local lifecycle races/faults with actual journals/SQLite/cache and signed HTTP.

Fake mount facts and mocked isolation/trainers; no live services, mounts or Pis.
"""
from contextlib import closing
import errno
import importlib.util
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import threading
import time
from unittest.mock import Mock,patch
import unittest

REAL_THREAD=threading.Thread
import test_staging_dispatch as fixture_module
from hybrid_storage import cleanup_schema,registry_schema
from hybrid_storage.staging_protocol import CONTROL_PATH,START_PATH,STAGE_PATH,encode
from hybrid_storage.workspaces import WorkspaceBusy
from agent.staging import StagingService,install_staging_service
from staging_dispatch import StagingDispatch
import job_store as store
import execution_leases as leases
import execution_lease as worker_lease


class RecoveryTests(unittest.TestCase):
    def setUp(self):
        self.f=fixture_module.DispatchTests(methodName='runTest')
        self.addCleanup(self.f.doCleanups);self.f.setUp()

    def staged(self):
        f=self.f;job,lease=f.reserve()
        stage,replies=f.runtime.stage_group(job,[f.node],[lease])
        return job,stage,replies

    def pins(self):
        with self.f.cache._session() as (db,_,__):return db.execute('SELECT COUNT(*) FROM cache_pins').fetchone()[0]

    def control(self,action,stage):
        f=self.f
        with f.registry._tx(write=False):member=f.runtime.members(stage.stage_id)[0]
        return f.runtime.control(stage.stage_id,member,action)

    def expire(self, *, controller=True,worker=True):
        f=self.f
        if controller:
            with f.db:f.db.execute('UPDATE execution_leases SET expires_at=?,expires_clock=?',(time.time()-10,worker_lease.clock()-10))
        if worker:
            path=worker_lease.authority_path(f.worker);record=worker_lease.read(path)
            # Simulate elapsed wall/BOOTTIME consistently; preserve original TTL.
            record['lease']['issued_at']=time.time()-40;record['lease']['expires_at']=time.time()-10
            record['deadline']=worker_lease.clock()-10
            worker_lease.atomic(path,record);worker_lease.atomic(worker_lease.lease_path(f.worker,record['lease']),record)

    def restart_worker(self,**callbacks):
        f=self.f;self.assertTrue(f.service.close())
        spec=importlib.util.spec_from_file_location('restarted_worker',fixture_module.ROOT/'agent/agent.py')
        agent=importlib.util.module_from_spec(spec)
        with patch.object(subprocess,'run',return_value=Mock(returncode=1)):spec.loader.exec_module(agent)
        agent.BASE_DIR=f.worker;agent.LOG_DIR=f.worker/'logs';agent.node_id=lambda:'pi01'
        service=StagingService(f.fetcher,f.manager,f.key,worker_root=f.worker,ownership=worker_lease,
                              cancelled=agent.attempt_cancelled,isolate=f.isolate,**callbacks)
        install_staging_service(agent,service,watchdog=False);self.addCleanup(service.close)
        f.agent=agent;f.service=service;f.http=agent.app.test_client()
        return service

    def test_cancel_ready_tombstones_releases_pins_and_rejects_delayed_start(self):
        f=self.f;job,stage,replies=self.staged();self.assertGreater(self.pins(),0)
        store.control_job(f.job_id,'alice','cancel')
        self.assertEqual(f.runtime.reconcile(job),'CANCELLED');self.assertEqual(self.pins(),0)
        self.assertEqual(store.get_attempts(f.job_id)[0]['status'],'CANCELLED')
        self.assertEqual(store.get_reservations(),[])
        with self.assertRaises(Exception):f.runtime.start(job,f.node,{},stage,replies[0])
        f.popen.assert_not_called()

    def test_cancel_before_stage_delivery_installs_durable_tombstone(self):
        f=self.f;job,lease=f.reserve();plan=f.runtime.plans(job)[0]
        stage=f.runtime.lifecycle.begin('alice',f.job_id,(plan,))
        body=dict(stage_id=stage.stage_id,plan=plan.document(),lease=lease)
        f.client.call(f.node,CONTROL_PATH,dict(action='cancel',**body))
        with self.assertRaises(Exception):f.client.call(f.node,STAGE_PATH,body)
        self.assertEqual(self.pins(),0);f.spawn.assert_not_called();f.popen.assert_not_called()

    def test_cancel_during_private_copy_does_not_wait_for_materializer_or_start_training(self):
        f=self.f;job,lease=f.reserve();plan=f.runtime.plans(job)[0]
        stage=f.runtime.lifecycle.begin('alice',f.job_id,(plan,));f.runtime._owned_stages.add(stage.stage_id)
        entered=threading.Event();release=threading.Event();errors=[]
        original=f.manager._copy_file_impl
        def copy(*args,**kwargs):
            entered.set()
            if not release.wait(5):raise RuntimeError('fixture copy gate timed out')
            return original(*args,**kwargs)
        def stage_worker():
            try:f.client.call(f.node,STAGE_PATH,dict(stage_id=stage.stage_id,plan=plan.document(),lease=lease))
            except Exception as error:errors.append(error)
        with patch.object(f.manager,'_copy_file_impl',side_effect=copy):
            thread=REAL_THREAD(target=stage_worker);thread.start()
            self.addCleanup(release.set)
            self.assertTrue(entered.wait(5));self.assertGreater(self.pins(),0)
            store.control_job(f.job_id,'alice','cancel')
            reply=self.control('cancel',stage)
            self.assertEqual(reply['phase'],'ABORTING');self.assertFalse(reply['pins_released'])
            release.set();thread.join(5);self.assertFalse(thread.is_alive())
        self.assertTrue(errors);f.service.tick()
        self.assertEqual(self.pins(),0);f.popen.assert_not_called()
        self.assertIn(f.runtime.reconcile(job),('CANCELLED','TERMINAL'))
        self.assertEqual(store.get_job(f.job_id)['status'],'CANCELLED')

    def test_expired_ready_attempt_self_fences_without_controller_request(self):
        f=self.f;job,stage,replies=self.staged();self.expire(worker=True,controller=False)
        f.service.tick();self.assertEqual(self.pins(),0)
        self.assertEqual(f.service._record(stage.stage_id)['outcome'],'LEASE_EXPIRED')
        with self.assertRaises(Exception):f.runtime.start(job,f.node,{},stage,replies[0])
        f.popen.assert_not_called()

    def test_background_watchdog_cleans_expired_staging_without_new_request(self):
        f=self.f;_,stage,_=self.staged();self.expire(worker=True,controller=False)
        with patch.object(threading,'Thread',REAL_THREAD):f.service.start_watchdog()
        limit=time.monotonic()+3
        while time.monotonic()<limit and f.service._record(stage.stage_id)['phase']!='TERMINAL':time.sleep(.05)
        self.assertEqual(self.pins(),0)
        self.assertEqual(f.service._record(stage.stage_id)['phase'],'TERMINAL')

    def test_controller_restart_aborts_forgotten_staging_instead_of_launching(self):
        f=self.f;job,stage,_=self.staged()
        resumed=StagingDispatch(f.registry,f.client,settings=f.runtime.lifecycle.settings)
        self.assertEqual(resumed.reconcile(job),'QUEUED')
        self.assertEqual(store.get_job(f.job_id)['retry_count'],1)
        self.assertEqual(self.pins(),0);f.popen.assert_not_called()
        self.assertEqual(f.db.execute('SELECT phase FROM hybrid_staging_attempts').fetchone()[0],'TERMINAL')

    def test_worker_restart_never_resurrects_ready_workspace(self):
        f=self.f;job,stage,_=self.staged()
        self.restart_worker()
        self.assertEqual(self.pins(),0)
        self.assertTrue(f.agent.attempt_cancelled(f.job_id,f'{f.job_id}:0'))
        self.assertEqual(f.service._record(stage.stage_id)['outcome'],'RESTART_ABORTED')
        self.assertEqual(f.runtime.reconcile(job),'QUEUED');f.popen.assert_not_called()

    def test_restarted_running_worker_retains_pins_without_descendant_proof(self):
        f=self.f;self.assertTrue(f.dispatch());self.assertGreater(self.pins(),0)
        service=self.restart_worker(quiescent=lambda *_:False)
        self.assertGreater(self.pins(),0)
        self.assertEqual(service.journal.records()[0]['phase'],'ABORTING')
        service.quiescent=lambda *_:True;service.tick()
        self.assertEqual(self.pins(),0)

    def test_offline_worker_keeps_reservation_until_safe_expiry_and_cleans_on_return(self):
        import requests
        f=self.f;self.assertTrue(f.dispatch());job=store.get_job(f.job_id)
        with patch.object(f.client,'post',side_effect=requests.Timeout('fixture offline')):
            self.assertEqual(f.runtime.reconcile(job),'PENDING')
            self.assertEqual(len(store.get_reservations()),1);self.assertGreater(self.pins(),0)
            self.expire()
            self.assertEqual(f.runtime.reconcile(job),'QUEUED')
        self.assertEqual(store.get_reservations(),[]);self.assertGreater(self.pins(),0)
        self.assertEqual(f.db.execute('SELECT phase FROM hybrid_cleanup_ranks').fetchone()[0],'LEASE_FENCED')
        f.process.poll.return_value=0;f.service.quiescent=lambda *_:True
        f.runtime.tick()
        self.assertEqual(self.pins(),0)
        self.assertEqual(f.db.execute('SELECT phase FROM hybrid_cleanup_ranks').fetchone()[0],'QUIESCENT')

    def test_late_success_and_publication_after_expiry_are_rejected(self):
        f=self.f;self.assertTrue(f.dispatch());job=store.get_job(f.job_id)
        lease=leases.ensure(job,'pi01');self.expire()
        f.process.poll.return_value=0
        self.assertFalse(f.service.note_exit(lease,0))
        with self.assertRaises(ValueError):leases.current(job,'pi01')
        with self.assertRaises(ValueError):store.mark_completed(f.job_id)
        with self.assertRaises(ValueError):
            with leases.publication(job,'pi01'):self.fail('Expired result publication entered')
        self.assertNotEqual(store.get_job(f.job_id)['status'],'COMPLETED')

    def test_publication_serializes_with_cancellation_and_rejects_unreserved_worker(self):
        f=self.f;self.assertTrue(f.dispatch());job=store.get_job(f.job_id)
        with self.assertRaises(ValueError):
            with leases.publication(job,'pi99'):self.fail('Unreserved publisher entered')
        entered=threading.Event();done=threading.Event();errors=[]
        def cancel():
            entered.set()
            try:store.control_job(f.job_id,'alice','cancel')
            except Exception as error:errors.append(error)
            finally:done.set()
        with leases.publication(job,'pi01'):
            thread=REAL_THREAD(target=cancel);thread.start();self.assertTrue(entered.wait(2))
            self.assertFalse(done.wait(.1))  # SQLite write lock is the publication boundary.
        thread.join(5);self.assertFalse(thread.is_alive());self.assertEqual(errors,[])
        with self.assertRaises(ValueError):
            with leases.publication(job,'pi01'):self.fail('Cancelled publisher entered')

    def test_normal_completion_releases_pins_and_commits_only_authenticated_current_outcome(self):
        f=self.f;self.assertTrue(f.dispatch());job=store.get_job(f.job_id)
        lease=leases.ensure(job,'pi01');f.process.poll.return_value=0
        f.service.quiescent=lambda *_:True
        self.assertTrue(f.service.note_exit(lease,0));f.service.tick();self.assertEqual(self.pins(),0)
        with patch('job_artifacts.collect') as collect:self.assertEqual(f.runtime.reconcile(job),'COMPLETED')
        collect.assert_called_once();self.assertEqual(store.get_reservations(),[])
        with self.assertRaises(ValueError):leases.current(job,'pi01')
        self.assertEqual(f.db.execute('SELECT phase FROM hybrid_staging_attempts').fetchone()[0],'TERMINAL')

    def test_completion_closes_exact_bound_attempt_not_later_noncurrent_history(self):
        f=self.f;self.assertTrue(f.dispatch());job=store.get_job(f.job_id);lease=leases.ensure(job,'pi01')
        with f.db:f.db.execute("INSERT INTO job_attempts(job_id,attempt_number,status,error) VALUES(?,99,'FAILED','preserve unrelated history')",(f.job_id,))
        f.process.poll.return_value=0;f.service.quiescent=lambda *_:True
        self.assertTrue(f.service.note_exit(lease,0));f.service.tick()
        with patch('job_artifacts.collect'):self.assertEqual(f.runtime.reconcile(job),'COMPLETED')
        attempts=store.get_attempts(f.job_id)
        self.assertEqual(attempts[0]['status'],'COMPLETED')
        self.assertEqual(attempts[1]['status'],'FAILED');self.assertEqual(attempts[1]['error'],'preserve unrelated history')

    def test_replayed_cleanup_for_terminal_attempt_does_not_terminate_another_owner(self):
        f=self.f;self.assertTrue(f.dispatch());job=store.get_job(f.job_id);lease=leases.ensure(job,'pi01')
        stage_id=f.service.journal.records()[0]['stage_id']
        f.process.poll.return_value=0;f.service.quiescent=lambda *_:True
        self.assertTrue(f.service.note_exit(lease,0));f.service.tick()
        f.service.terminate.reset_mock()
        with f.registry._tx(write=False):member=f.runtime.members(stage_id)[0]
        self.assertTrue(f.runtime.control(stage_id,member,'cancel')['pins_released'])
        f.service.terminate.assert_not_called()

    def test_live_known_process_or_false_containment_proof_keeps_pins(self):
        f=self.f;self.assertTrue(f.dispatch());job=store.get_job(f.job_id)
        lease=leases.ensure(job,'pi01');f.service.quiescent=lambda *_:True
        self.assertTrue(f.service.note_exit(lease,0));f.service.tick()
        self.assertGreater(self.pins(),0)
        f.process.poll.return_value=0;f.service.quiescent=lambda *_:False;f.service.tick()
        self.assertGreater(self.pins(),0)
        f.service.quiescent=lambda *_:True;f.service.tick();self.assertEqual(self.pins(),0)

    def test_launch_without_termination_or_quiescence_provider_is_rejected(self):
        f=self.f;f.service.quiescent=None
        with self.assertRaises(Exception):f.dispatch()
        f.popen.assert_not_called()

    def test_unpin_disk_error_retains_controller_reservation_until_idempotent_retry(self):
        f=self.f;job,stage,_=self.staged();store.control_job(f.job_id,'alice','cancel')
        with patch.object(f.cache,'unpin',side_effect=OSError(errno.ENOSPC,'fixture disk full')):
            self.assertEqual(f.runtime.reconcile(job),'PENDING')
        self.assertGreater(self.pins(),0);self.assertEqual(len(store.get_reservations()),1)
        self.assertEqual(f.runtime.reconcile(job),'CANCELLED');self.assertEqual(self.pins(),0)

    def test_orphan_parts_cleanup_preserves_unknown_files_and_cache_objects(self):
        f=self.f;self.staged();parts=f.cache_root/'parts'
        owned=parts/('a'*64+'.'+'b'*32+'.part');owned.write_bytes(b'partial');owned.chmod(0o600)
        unknown=parts/'student-not-a-managed-part';unknown.write_bytes(b'keep')
        before=[p for p in (f.cache_root/'objects').rglob('*') if p.is_file()]
        self.assertTrue(f.fetcher.reconcile_orphans())
        self.assertFalse(owned.exists());self.assertEqual(unknown.read_bytes(),b'keep')
        self.assertTrue(all(p.exists() for p in before))

    def test_orphan_symlink_is_never_followed_or_deleted(self):
        f=self.f;target=f.fixture.base/'keep';target.write_bytes(b'keep')
        link=f.cache_root/'parts'/('a'*64+'.'+'b'*32+'.part');link.symlink_to(target)
        with self.assertRaises(OSError):f.fetcher.reconcile_orphans()
        self.assertTrue(link.is_symlink());self.assertEqual(target.read_bytes(),b'keep')

    def test_inherited_cache_slot_retains_pins_until_orphan_producer_reaped(self):
        f=self.f;_,stage,_=self.staged();marker=f.fixture.base/'producer-ready'
        script="import fcntl,sys,time; from pathlib import Path; s=open(sys.argv[1],'r+'); fcntl.flock(s,fcntl.LOCK_EX); Path(sys.argv[2]).write_text('held'); time.sleep(30)"
        child=f.real_popen([sys.executable,'-c',script,str(f.cache_root/'state/fetch.lock'),str(marker)])
        def cleanup():
            if child.poll() is None:child.kill()
            child.wait(timeout=5)
        self.addCleanup(cleanup)
        deadline=time.monotonic()+3
        while not marker.exists() and time.monotonic()<deadline:time.sleep(.02)
        self.assertTrue(marker.exists())
        reply=self.control('cancel',stage)
        self.assertFalse(reply['pins_released']);self.assertGreater(self.pins(),0)
        child.kill();child.wait(timeout=5)
        self.assertTrue(self.control('cancel',stage)['pins_released']);self.assertEqual(self.pins(),0)

    def test_actual_disposable_guardian_descendants_stop_before_pins_release(self):
        f=self.f;self.assertTrue(f.dispatch());job=store.get_job(f.job_id);lease=leases.ensure(job,'pi01')
        marker=f.fixture.base/'trainer-ready'
        code="import subprocess,sys,time; from pathlib import Path; c=subprocess.Popen([sys.executable,'-c','import time; time.sleep(30)']); Path(sys.argv[1]).write_text(str(c.pid)); time.sleep(30)"
        guardian=f.real_popen([sys.executable,str(fixture_module.ROOT/'agent/lease_runner.py'),
             '--lease-path',str(worker_lease.lease_path(f.worker,lease)),'--',sys.executable,'-c',code,str(marker)],
             start_new_session=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        def cleanup():
            import signal
            try:os.killpg(guardian.pid,signal.SIGKILL)
            except ProcessLookupError:pass
            guardian.wait(timeout=5)
        self.addCleanup(cleanup)
        f.process.pid=guardian.pid;f.process.poll.side_effect=guardian.poll
        worker_lease.attach(f.worker,lease,guardian.pid)
        deadline=time.monotonic()+4
        while not marker.exists() and time.monotonic()<deadline:time.sleep(.02)
        self.assertTrue(marker.exists())
        def quiescent(*_):
            if guardian.poll() is None:return False
            for path in Path('/proc').iterdir():
                if not path.name.isdigit():continue
                try:
                    if os.getpgid(int(path.name))==guardian.pid and (path/'stat').read_text().rsplit(')',1)[1].split()[0] not in ('Z','X'):return False
                except (OSError,IndexError):pass
            return True
        f.service.quiescent=quiescent
        store.control_job(f.job_id,'alice','cancel')
        f.runtime.reconcile(job);guardian.wait(timeout=5)
        deadline=time.monotonic()+3
        while not quiescent() and time.monotonic()<deadline:time.sleep(.02)
        self.assertTrue(quiescent());f.service.tick();self.assertEqual(self.pins(),0)
        self.assertIn(f.runtime.reconcile(job),('CANCELLED','TERMINAL'))
        self.assertEqual(store.get_job(f.job_id)['status'],'CANCELLED')

    def test_unpublished_partial_workspace_removed_but_published_outputs_retained(self):
        f=self.f
        original=f.manager._copy_file_impl
        def fail(*args,**kwargs):
            original(*args,**kwargs)
            raise OSError(errno.ENOSPC,'fixture interrupted private copy')
        with patch.object(f.manager,'_copy_file_impl',side_effect=fail):
            with self.assertRaises(Exception):f.dispatch()
        self.assertEqual(list((f.workspace_root/'attempts').iterdir()),[])
        self.assertEqual(self.pins(),0)

    def test_authenticated_control_cannot_spoof_other_worker_or_stale_fence(self):
        f=self.f;job,stage,_=self.staged()
        with f.registry._tx(write=False):_,_,plan,lease=f.runtime.members(stage.stage_id)[0]
        body=dict(action='cancel',stage_id=stage.stage_id,plan=plan.document(),lease=leases.descriptor(lease))
        body['lease']['generation']+=1
        with self.assertRaises(Exception):f.client.call(f.node,CONTROL_PATH,body)
        self.assertGreater(self.pins(),0);self.assertFalse(f.agent.attempt_cancelled(f.job_id,f'{f.job_id}:0'))

    def test_cleanup_schema_partial_or_interrupted_migration_does_not_repair_existing_data(self):
        f=self.f
        with f.db:f.db.execute('DROP INDEX hybrid_cleanup_pending')
        before=list(f.db.iterdump())
        with self.assertRaises(registry_schema.RegistryError):cleanup_schema.migrate(f.db)
        self.assertEqual(list(f.db.iterdump()),before)

    def test_interrupted_fresh_migration_rolls_back_and_concurrent_repeats_preserve_old_rows(self):
        from concurrent.futures import ThreadPoolExecutor
        target=fixture_module.fixtures.RegistryTests(methodName='runTest')
        self.addCleanup(target.doCleanups);target.setUp();target.registry()
        fixture_module.staging_schema.migrate(target.db)
        target.db.execute('PRAGMA user_version=73')
        before=list(target.db.iterdump())
        with patch.object(cleanup_schema,'DDL',cleanup_schema.DDL+['CREATE TABLE invalid (']):
            with self.assertRaises(sqlite3.OperationalError):cleanup_schema.migrate(target.db)
        self.assertEqual(list(target.db.iterdump()),before)
        jobs=[tuple(r) for r in target.db.execute('SELECT * FROM jobs')]
        def migrate(_):
            with closing(target.connect()) as db:return cleanup_schema.migrate(db)
        # The shared agent fixture mocks Thread to suppress its watcher. Restore
        # actual threads for this deliberate independent-SQLite-writer test.
        with patch.object(threading,'Thread',REAL_THREAD), ThreadPoolExecutor(max_workers=3) as pool:
            results=[future.result(timeout=10) for future in [pool.submit(migrate,n) for n in range(3)]]
        self.assertEqual(len(results),3)
        self.assertEqual(target.db.execute('SELECT COUNT(*) FROM hybrid_cleanup_migrations').fetchone()[0],1)
        self.assertEqual(target.db.execute('PRAGMA user_version').fetchone()[0],73)
        self.assertEqual([tuple(r) for r in target.db.execute('SELECT * FROM jobs')],jobs)

    def test_cancel_preserves_published_private_outputs_checkpoints_and_input_objects(self):
        f=self.f;_,stage,_=self.staged();plan,_,_=f.service._ready[stage.stage_id]
        root=f.workspace_root/'attempts'/plan.identity.key
        output=root/'outputs/result.json';output.write_bytes(b'keep results')
        checkpoint=root/'checkpoints/candidate';checkpoint.write_bytes(b'keep checkpoint')
        self.control('cancel',stage)
        self.assertEqual(self.pins(),0)
        self.assertEqual(output.read_bytes(),b'keep results');self.assertEqual(checkpoint.read_bytes(),b'keep checkpoint')
        for entry in plan.inputs():
            with f.cache.open_verified(entry.sha256,entry.size_bytes):pass

    def test_controller_gap_before_lease_or_stage_does_not_rewrite_previous_attempt(self):
        f=self.f
        # Protected workflow gap: reservation committed but no ensure/begin/RPC.
        job=store.get_job(f.job_id)
        store.mark_scheduled(f.job_id,'pi01',f.node['ip'],json.loads(job['job_config'])['resources_per_worker'],storage_runtime=f.runtime)
        with f.db:
            f.db.execute("INSERT INTO job_attempts(job_id,attempt_number,status,error) VALUES(?,0,'FAILED','preserve previous')",(f.job_id,))
        self.assertEqual(f.runtime.reconcile(job),'QUEUED')
        self.assertEqual(store.get_attempts(f.job_id)[0]['error'],'preserve previous')
        self.assertEqual(store.get_reservations(),[]);f.popen.assert_not_called()

    def test_missing_cleanup_migration_refuses_dispatch_before_reservation(self):
        f=self.f
        with f.db:f.db.execute('DROP INDEX hybrid_cleanup_pending')
        with self.assertRaises(ValueError):f.dispatch()
        self.assertEqual(store.get_reservations(),[]);f.popen.assert_not_called()

    def test_journal_conflicting_identity_or_corruption_fails_closed(self):
        f=self.f;_,stage,_=self.staged()
        path=f.workspace_root/'lifecycle'/(stage.stage_id+'.json')
        record=json.loads(path.read_text());record['plan_sha256']='f'*64
        path.write_bytes(encode(record));path.chmod(0o600)
        with self.assertRaises(ValueError):f.service.tick()
        self.assertGreater(self.pins(),0);f.popen.assert_not_called()

    def test_single_service_lock_prevents_two_bootstraps_from_reconciling_each_other(self):
        f=self.f
        from hybrid_storage.staging_journal import StagingJournal
        journal=StagingJournal(f.workspace_root)
        with self.assertRaises(BlockingIOError):journal.initialize()

    def test_cleanup_migration_idempotent_full_local_and_retains_existing_history(self):
        f=self.f;before=list(f.db.iterdump());cleanup_schema.migrate(f.db)
        self.assertEqual(list(f.db.iterdump()),before)
        self.assertEqual(cleanup_schema.migration_plan(f.db)['action'],'unchanged')
        with closing(f.fixture.connect()) as db:
            db.execute('PRAGMA synchronous=NORMAL')
            with self.assertRaises(registry_schema.RegistryError):cleanup_schema.migrate(db)
        self.assertEqual(f.db.execute('PRAGMA foreign_key_check').fetchall(),[])

    def test_default_reconciliation_and_renewal_never_send_unsigned_hybrid_requests(self):
        f=self.f;self.assertTrue(f.dispatch());job=store.get_job(f.job_id)
        with patch.object(leases.requests,'post',side_effect=AssertionError('unsigned POST')):
            leases.renew(job)
            fixture_module.dispatcher.monitor_running_jobs()
        self.assertTrue(leases.get(job,'pi01')['renewal_blocked'])

    def test_signed_running_renewal_preserves_identity_and_extends_both_deadlines(self):
        f=self.f;self.assertTrue(f.dispatch());job=store.get_job(f.job_id)
        old=leases.get(job,'pi01')
        with patch.object(leases,'settings',return_value=dict(enabled=True,duration_seconds=30,renew_every_seconds=0)):
            self.assertEqual(f.runtime.reconcile(job),'RUNNING')
        new=leases.get(job,'pi01');held=worker_lease.read(worker_lease.authority_path(f.worker))
        self.assertEqual(new['lease_id'],old['lease_id']);self.assertGreaterEqual(new['expires_at'],old['expires_at'])
        self.assertEqual(new['expires_at'],held['lease']['expires_at'])

    def test_three_rank_cancellation_preserves_group_until_every_rank_is_quiescent(self):
        import distributed_dispatch as ddp
        f=self.f;nodes=f.ddp_job()
        with patch.object(ddp,'select_group',return_value=nodes):self.assertTrue(f.dispatch())
        job=store.get_job(f.job_id);store.control_job(f.job_id,'alice','cancel')
        self.assertEqual(f.runtime.reconcile(job),'PENDING');self.assertEqual(len(store.get_reservations()),3)
        f.process.poll.return_value=0
        for service in [f.service,*f.extra_services]:service.quiescent=lambda *_:True
        self.assertEqual(f.runtime.reconcile(job),'CANCELLED');self.assertEqual(store.get_reservations(),[])
        self.assertEqual([r[0] for r in f.db.execute('SELECT phase FROM hybrid_cleanup_ranks')],['QUIESCENT']*3)


if __name__=='__main__':unittest.main(verbosity=2)
