"""12.1 real isolated lifecycle fixtures + operator log contract; no live cluster.

Run through tools/run_tests.py --isolated: fixture imports initialize copied DBs.
"""
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import errno
import json
import logging
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from hybrid_storage import events
# Establish the existing agent package before checkpoint fixtures prepend the
# agent script directory to sys.path (the repository's scripts share that name).
import test_staging_dispatch as dispatch_support


class Capture(logging.Handler):
    def __init__(self):super().__init__();self.rows=[]
    def emit(self,record):self.rows.append(json.loads(record.getMessage()))


@contextmanager
def capture():
    logger=events.LOGGER;state=(logger.level,logger.propagate,logger.disabled,logger.handlers[:])
    handler=Capture();logger.handlers=[handler];logger.disabled=False;logger.propagate=False;logger.setLevel(logging.INFO)
    try:yield handler.rows
    finally:logger.setLevel(state[0]);logger.propagate=state[1];logger.disabled=state[2];logger.handlers=state[3]


class ContractTests(unittest.TestCase):
    context=dict(job_id='job-one',attempt_id='job-one:2',worker_id='pi02',rank=1,
                 manifest_version='a'*64,manifest_schema_version=1,dataset_version=3,stage_id='b'*32)

    def test_schema_event_id_and_unix_utc_nanoseconds(self):
        # Unix epoch is independent of the machine's local timezone/DST.
        with capture() as rows,patch.object(events.time,'time_ns',return_value=1794268800123456789):
            a=events.emit('storage.staging.started',**self.context)
            b=events.emit('storage.staging.started',**self.context)
        self.assertRegex(a,r'^[0-9a-f]{32}$');self.assertNotEqual(a,b)
        self.assertEqual(rows[0],dict(self.context,schema_version=1,event_type='storage.staging.started',
                                     event_id=a,occurred_at_ns=1794268800123456789))

    def test_unknown_fields_and_free_text_reject_entire_record(self):
        with capture() as rows:
            for key in ('path','password','lease','lease_id','owner','filename','message','credentials','extra'):
                self.assertIsNone(events.emit('storage.staging.started',**self.context,**{key:'/srv/secret\nTOKEN'}))
            self.assertIsNone(events.emit('storage.scheduler.choice',**self.context,requested_policy='resource',
                scheduler_policy='resource',reason='private-student-name',missing_bytes=None))
            self.assertIsNone(events.emit('unknown.event',**self.context))
        self.assertEqual(rows,[])

    def test_numeric_bounds_booleans_missing_fields_and_bad_identifiers(self):
        with capture() as rows:
            for value in (-1,True,1.5,float('nan'),float('inf'),2**63,'secret'):
                self.assertIsNone(events.emit('storage.checkpoint.committed',**self.context,
                    checkpoint_id='c'*32,world_size=1,verified_bytes=value))
            for key,value in (('job_id','../alice'),('attempt_id','other:1'),('worker_id','pi02\nsecret'),
                              ('manifest_version','bad'),('rank',65536),('dataset_version',0),('manifest_schema_version',2)):
                self.assertIsNone(events.emit('storage.staging.started',**dict(self.context,**{key:value})))
            self.assertIsNone(events.emit('storage.checkpoint.committed',**self.context))
        self.assertEqual(rows,[])

    def test_disabled_logging_reads_no_clock_and_writes_nothing(self):
        with patch.object(events.LOGGER,'isEnabledFor',return_value=False), \
             patch.object(events.time,'time_ns',side_effect=AssertionError('disabled clock')), \
             patch.object(events.time,'monotonic_ns',side_effect=AssertionError('disabled clock')):
            self.assertIsNone(events.emit('storage.staging.started',**self.context))
            with events.observe_staging(None,'pi01','b'*32):pass
            clock=events.RecoveryClock();clock.begin(dict(job_id='j',retry_count=0))
            self.assertIsNone(clock.finish(dict(job_id='j',retry_count=0)))

    def test_logger_and_clock_failures_are_advisory(self):
        with capture(),patch.object(events.LOGGER,'info',side_effect=RuntimeError('handler unavailable')):
            self.assertIsNone(events.emit('storage.staging.started',**self.context))
        with capture(),patch.object(events.time,'time_ns',side_effect=RuntimeError('clock unavailable')):
            self.assertIsNone(events.emit('storage.staging.started',**self.context))

    def test_error_messages_paths_and_secret_arguments_never_serialized(self):
        error=OSError(errno.EIO,'credential=secret','/private/student/secret')
        with capture() as rows,events.scope(self.context):events.copy_error(error,1,retry=True)
        self.assertEqual(rows[0]['errno'],errno.EIO);self.assertEqual(rows[0]['error_class'],'os_error')
        self.assertNotIn('secret',json.dumps(rows));self.assertNotIn('private',json.dumps(rows))

    def test_mount_corruption_timeout_and_capacity_have_closed_error_classes(self):
        from hybrid_storage.nfs_mount import MountNotReady
        from hybrid_storage.worker_cache import CacheCorrupt
        for error,kind in ((MountNotReady('secret'),'mount_unavailable'),(CacheCorrupt('secret'),'integrity'),
                           (TimeoutError('secret'),'timeout'),(OSError(errno.ENOSPC,'secret'),'capacity'),
                           (PermissionError('secret'),'denied_or_fenced'),(RuntimeError('secret'),'other')):
            self.assertEqual(events.error_fields(error)['error_class'],kind)

    def test_scopes_do_not_mix_threads_and_reset_on_exception(self):
        def copy(worker):
            with events.scope(dict(self.context,worker_id=worker)):
                events.copy_error(OSError(errno.ESTALE,'private'),1)
        with capture() as rows:
            with ThreadPoolExecutor(max_workers=3) as pool:list(pool.map(copy,('pi01','pi02','pi03')))
            with self.assertRaises(ValueError):
                with events.scope(self.context):raise ValueError('abort')
            events.copy_error(OSError(errno.EIO,'not attributed'),1)
        self.assertEqual(sorted(r['worker_id'] for r in rows),['pi01','pi02','pi03'])

    def test_recovery_clock_bounded_monotonic_and_restart_unknown(self):
        with capture(),patch.object(events.time,'monotonic_ns',side_effect=[100,160]):
            clock=events.RecoveryClock();job=dict(job_id='j',retry_count=1)
            clock.begin(job);clock.begin(job);self.assertEqual(clock.finish(job),60)
            self.assertIsNone(clock.finish(job));self.assertIsNone(events.RecoveryClock().finish(job))
        with capture():
            for index in range(200):clock.begin(dict(job_id=f'j{index}',retry_count=0))
        self.assertEqual(len(clock._starts),128)
        self.assertIsNone(clock.finish(dict(job_id='j0',retry_count=0)))


class LifecycleTests(unittest.TestCase):
    def fixture(self,cls,method='runTest'):
        f=cls(methodName=method);self.addCleanup(f.doCleanups);f.setUp();return f

    def test_real_signed_staging_cold_warm_ready_attribution_and_no_wire_change(self):
        import test_staging_dispatch as support
        f=self.fixture(support.DispatchTests);job,lease=f.reserve()
        with capture() as rows:
            stage,replies=f.runtime.stage_group(job,[f.node],[lease])
            again,warm=f.runtime.stage_group(job,[f.node],[support.leases.ensure(job,'pi01')])
        ready=[r for r in rows if r['event_type']=='storage.staging.ready']
        self.assertEqual(len(ready),2);cold,hot=ready
        required=sum({i.sha256:i.size_bytes for i in f.runtime.plans(job)[0].inputs()}.values())
        self.assertEqual((cold['cache_hit_bytes'],cold['transferred_bytes'],cold['verified_bytes']),(0,required,required))
        self.assertEqual((hot['cache_hit_bytes'],hot['transferred_bytes'],hot['verified_bytes']),(required,0,required))
        self.assertEqual(cold['manifest_version'],f.binding.sha256)
        self.assertEqual(cold['dataset_version'],f.data.version)
        self.assertEqual((cold['attempt_id'],cold['worker_id'],cold['rank'],cold['stage_id']),
                         (job['job_id']+':0','pi01',0,stage.stage_id))
        self.assertGreater(cold['staging_duration_ns'],0);self.assertGreater(cold['verification_duration_ns'],0)
        self.assertEqual(stage,again);self.assertEqual(replies[0]['receipt'],warm[0]['receipt'])
        self.assertNotIn('events',replies[0]);f.popen.assert_not_called()

    def test_transient_copy_error_retries_and_releases_context(self):
        import test_staging_dispatch as support
        f=self.fixture(support.DispatchTests);job,lease=f.reserve();original=f.fetcher._copy;count=0
        def copy(*args):
            nonlocal count
            count+=1
            if count==1:raise OSError(errno.ESTALE,'secret mount','/srv/private')
            return original(*args)
        with capture() as rows,patch.object(f.fetcher,'_copy',side_effect=copy):
            f.runtime.stage_group(job,[f.node],[lease])
            events.copy_error(OSError(errno.EIO,'unbound'),1)
        names=[r['event_type'] for r in rows]
        self.assertEqual(names.count('storage.nfs.error'),1);self.assertEqual(names.count('storage.nfs.retry'),1)
        self.assertEqual(names[-1],'storage.staging.ready');self.assertNotIn('secret',json.dumps(rows))

    def test_missing_source_failure_has_no_ready_event_or_launch(self):
        import test_staging_dispatch as support
        f=self.fixture(support.DispatchTests)
        for path in f.fixture.objects.rglob('*'):
            if path.is_file():path.unlink()  # Only disposable fixture sources.
        import requests
        with capture() as rows,self.assertRaises(requests.HTTPError):f.dispatch()
        types=[r['event_type'] for r in rows]
        self.assertIn('storage.nfs.error',types);self.assertIn('storage.staging.failed',types)
        self.assertNotIn('storage.staging.ready',types);f.popen.assert_not_called()

    def test_checkpoint_emitted_after_commit_never_for_invalid_or_idempotent_replay(self):
        import test_checkpoint_publication as support
        f=self.fixture(support.PublicationTests)
        with capture() as rows:
            checkpoint=f.publish();self.assertEqual(f.publish(),checkpoint)
            with self.assertRaises(ValueError):f.publish(f.archive(step=2),identity='2'*32,head=checkpoint.publication_id)
        committed=[r for r in rows if r['event_type']=='storage.checkpoint.committed']
        self.assertEqual(len(committed),1);row=committed[0]
        self.assertEqual(row['checkpoint_id'],checkpoint.publication_id)
        self.assertEqual(row['verified_bytes'],checkpoint.size_bytes)
        self.assertEqual(row['manifest_version'],f.binding.sha256)
        self.assertEqual(f.publisher.head('alice',f.job_id),checkpoint)

    def test_logging_failure_cannot_prevent_real_staging_and_checkpoint_commit(self):
        import test_staging_dispatch as staging
        f=self.fixture(staging.DispatchTests)
        with capture(),patch.object(events.LOGGER,'info',side_effect=RuntimeError('broken log')):
            self.assertTrue(f.dispatch())
        import test_checkpoint_publication as checkpoints
        p=self.fixture(checkpoints.PublicationTests)
        with capture(),patch.object(events.LOGGER,'info',side_effect=RuntimeError('broken log')):
            checkpoint=p.publish()
        self.assertEqual(p.publisher.head('alice',p.job_id),checkpoint)

    def test_scheduler_resource_and_locality_report_facts_without_changing_choice(self):
        import test_scheduler_policy as support
        f=self.fixture(support.PolicyTests)
        with capture() as rows:
            self.assertEqual(f.single(f.policy(support.RESOURCE))['node']['id'],'pi03')
            self.assertEqual(f.single(f.policy())['node']['id'],'pi01')
        self.assertEqual([r['scheduler_policy'] for r in rows],['resource','locality'])
        self.assertIsNone(rows[0]['missing_bytes']);self.assertEqual(rows[1]['missing_bytes'],0)
        self.assertEqual(rows[1]['reason'],'verified_missing_bytes')

    def test_three_rank_parallel_staging_has_exact_worker_rank_and_manifest_context(self):
        import distributed_dispatch as ddp
        f=self.fixture(dispatch_support.DispatchTests);nodes=f.ddp_job()
        with capture() as rows,patch.object(ddp,'select_group',return_value=nodes):
            self.assertTrue(f.dispatch())
        ready=[r for r in rows if r['event_type']=='storage.staging.ready']
        self.assertEqual(sorted((r['rank'],r['worker_id']) for r in ready),[(0,'pi01'),(1,'pi02'),(2,'pi03')])
        self.assertEqual(len({r['stage_id'] for r in ready}),1)
        self.assertEqual(len({r['manifest_version'] for r in ready}),1)
        self.assertEqual(f.popen.call_count,3)

    def test_fenced_workspace_has_failed_event_without_ready_or_training(self):
        f=self.fixture(dispatch_support.DispatchTests)
        import requests
        with capture() as rows,patch.object(f.manager,'prepare',side_effect=PermissionError('secret revoked lease')), \
             self.assertRaises(requests.HTTPError):f.dispatch()
        failed=[r for r in rows if r['event_type']=='storage.staging.failed']
        self.assertEqual(len(failed),1);self.assertEqual(failed[0]['error_class'],'denied_or_fenced')
        self.assertGreater(failed[0]['transferred_bytes'],0)
        self.assertEqual(failed[0]['verified_bytes'],failed[0]['transferred_bytes'])
        self.assertFalse(any(r['event_type']=='storage.staging.ready' for r in rows))
        self.assertNotIn('secret',json.dumps(rows));f.popen.assert_not_called()

    def test_sdk_ack_required_for_recovery_latency_and_restart_reports_unknown(self):
        import test_hybrid_checkpoint_recovery as support
        f=self.fixture(support.RecoveryTests)
        with capture() as rows:
            f.cold();saved=f.save();f.fail();self.assertTrue(f.f.dispatch())
            self.assertEqual(f.runtime.reconcile(f.job()),'RESTORE_PENDING')
            self.assertFalse(any(r['event_type']=='storage.recovery.confirmed' for r in rows))
            f.ack();self.assertEqual(f.runtime.reconcile(f.job()),'RUNNING')
            f.runtime.reconcile(f.job())
        confirmed=[r for r in rows if r['event_type']=='storage.recovery.confirmed']
        self.assertEqual(len(confirmed),1);self.assertGreater(confirmed[0]['recovery_latency_ns'],0)
        self.assertEqual(confirmed[0]['checkpoint_id'],saved.publication_id)
        self.assertEqual(confirmed[0]['attempt_id'],f.f.job_id+':1')
        with capture() as restarted:
            f.fail();self.assertTrue(f.f.dispatch());f.runtime=f.coordinator();f.f.runtime=f.runtime
            f.ack();self.assertEqual(f.runtime.reconcile(f.job()),'RUNNING')
        self.assertIsNone(next(r for r in restarted if r['event_type']=='storage.recovery.confirmed')['recovery_latency_ns'])

    def test_ddp_commit_and_recovery_wait_for_all_sdk_ranks_before_emitting(self):
        import test_hybrid_checkpoint_recovery as support
        f=self.fixture(support.RecoveryTests);f.ddp()
        with capture() as rows:
            f.cold();saved=f.save();f.fail();self.assertTrue(f.f.dispatch())
            f.ack();f.ack(f.f.extra_services[0])
            self.assertEqual(f.runtime.reconcile(f.job()),'RESTORE_PENDING')
            self.assertFalse(any(r['event_type']=='storage.recovery.confirmed' for r in rows))
            f.ack(f.f.extra_services[1]);self.assertEqual(f.runtime.reconcile(f.job()),'RUNNING')
        commits=[r for r in rows if r['event_type']=='storage.checkpoint.committed']
        self.assertEqual(len(commits),1);self.assertEqual(commits[0]['world_size'],3)
        confirmed=[r for r in rows if r['event_type']=='storage.recovery.confirmed']
        self.assertEqual(sorted((r['rank'],r['worker_id']) for r in confirmed),[(0,'pi01'),(1,'pi02'),(2,'pi03')])
        self.assertEqual({r['checkpoint_id'] for r in confirmed},{saved.publication_id})
        self.assertEqual(len({r['recovery_latency_ns'] for r in confirmed}),1)
        self.assertGreater(confirmed[0]['recovery_latency_ns'],0)


if __name__=='__main__':unittest.main(verbosity=2)
