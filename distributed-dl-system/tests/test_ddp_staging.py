"""9.2 bounded group staging with signed Flask RPCs and real private copies.

Fake NFS facts/helpers and mocked trainer/isolation; no listener or physical Pis.
Events/barriers prove concurrency without throughput or timing claims. Controller
SQLite keeps its default same-thread guard; only HTTP calls run in the pool.
"""
from pathlib import Path
import sys
import threading
import time
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
import test_staging_dispatch as fixture
from hybrid_storage.staging_protocol import CONTROL_PATH, STAGE_PATH, START_PATH, decode
from staging_dispatch import StagingDispatch


class DDPStagingTests(unittest.TestCase):
    def setUp(self):
        self.f = fixture.DispatchTests(methodName='runTest')
        self.addCleanup(self.f.doCleanups);self.f.setUp()
        self.owner_thread = threading.get_ident()

    def group(self, *, workers=3, parallelism=3, **dataset_acl):
        f = self.f
        nodes = f.ddp_job(workers=workers,**dataset_acl)
        f.runtime = StagingDispatch(f.registry,f.client,settings=f.runtime.lifecycle.settings,
                                    stage_parallelism=parallelism)
        f.stack.enter_context(patch('distributed_dispatch.select_group',return_value=nodes))
        return nodes,[f.service,*f.extra_services]

    def pins(self, service):
        with service.fetcher.cache._session() as (db,_,__):
            return db.execute('SELECT COUNT(*) FROM cache_pins').fetchone()[0]

    def assert_aborted(self, services, *, state='QUEUED'):
        f = self.f
        self.assertFalse(any(path==START_PATH for path,_ in f.calls))
        f.popen.assert_not_called()
        self.assertEqual(fixture.fixtures.store.get_job(f.job_id)['status'],state)
        self.assertEqual(fixture.fixtures.store.get_reservations(),[])
        self.assertEqual([r[0] for r in f.db.execute('SELECT phase FROM hybrid_cleanup_ranks')],
                         ['QUIESCENT']*len(services))
        for service in services:
            self.assertEqual(self.pins(service),0)
            self.assertEqual(service.journal.records()[0]['phase'],'TERMINAL')

    def test_every_rank_requires_full_source_and_data_not_sampler_indices(self):
        self.group();f = self.f;plans = f.runtime.plans(fixture.fixtures.store.get_job(f.job_id))
        self.assertEqual([p.identity.rank for p in plans],[0,1,2])
        self.assertEqual(len({p.sha256 for p in plans}),3)
        self.assertTrue(any(item.path.startswith('data/') for item in plans[0].inputs()))
        self.assertTrue(any(item.path=='source/requirements.txt' for item in plans[0].inputs()))
        self.assertTrue(all(p.inputs()==plans[0].inputs() for p in plans))
        self.assertTrue(all(p.source==plans[0].source and p.dataset==plans[0].dataset for p in plans))

    def test_three_rank_overlap_reverse_replies_preserve_rank_binding_and_db_thread(self):
        nodes,services = self.group();f = self.f
        barrier = threading.Barrier(3);finished = [threading.Event() for _ in nodes]
        original = f.client.post;lock = threading.Lock();active = 0;peak = 0;threads = set()
        db_threads = [];actual_record = f.runtime.lifecycle.record_ready
        def record(*args,**kwargs):
            db_threads.append(threading.get_ident())
            return actual_record(*args,**kwargs)
        def post(url,**kwargs):
            nonlocal active,peak
            if not url.endswith(STAGE_PATH):return original(url,**kwargs)
            rank = decode(kwargs['data'])['body']['plan']['identity']['rank']
            with lock:
                active += 1;peak = max(peak,active);threads.add(threading.get_ident())
            try:
                barrier.wait(timeout=10)
                result = original(url,**kwargs)
                if rank<2:self.assertTrue(finished[rank+1].wait(10))
                finished[rank].set()
                return result
            finally:
                with lock:active -= 1
        with patch.object(f.client,'post',side_effect=post), \
             patch.object(f.runtime.lifecycle,'record_ready',side_effect=record):
            self.assertTrue(f.dispatch())
        self.assertEqual(peak,3);self.assertEqual(len(threads),3)
        self.assertNotIn(self.owner_thread,threads);self.assertEqual(db_threads,[self.owner_thread]*3)
        self.assertEqual([path for path,_ in f.calls],[STAGE_PATH]*3+[START_PATH]*3)
        starts = [body for path,body in f.calls if path==START_PATH]
        self.assertEqual([body['ddp']['rank'] for body in starts],[0,1,2])
        self.assertEqual(len(fixture.fixtures.store.get_reservations()),3)
        for rank,service in enumerate(services):
            record = service.journal.records()[0]
            self.assertEqual(record['plan']['identity']['rank'],rank)
            plan = f.runtime.plans(fixture.fixtures.store.get_job(f.job_id))[rank]
            data = service.workspaces.root/'attempts'/plan.identity.key/'inputs/data'
            self.assertEqual((data/'a/rows.csv').read_bytes(),b'x,y\n1,2\n')
            self.assertEqual((data/'a.txt').read_bytes(),b'root rows\n')
            self.assertGreater(self.pins(service),0)

    def test_two_rank_group_uses_same_full_plan_barrier_and_launcher(self):
        _,services = self.group(workers=2);f = self.f
        barrier = threading.Barrier(2);original = f.client.post
        def post(url,**kwargs):
            if url.endswith(STAGE_PATH):barrier.wait(timeout=10)
            return original(url,**kwargs)
        with patch.object(f.client,'post',side_effect=post):self.assertTrue(f.dispatch())
        self.assertEqual([path for path,_ in f.calls],[STAGE_PATH]*2+[START_PATH]*2)
        self.assertEqual([r['status'] for r in fixture.fixtures.store.get_ranks(f.job_id,0)],['RUNNING']*2)
        self.assertTrue(all(self.pins(service)>0 for service in services))

    def test_parallelism_two_bounds_outstanding_calls_for_three_ranks(self):
        self.group(parallelism=2);f = self.f
        original = f.client.post;barrier = threading.Barrier(2);lock = threading.Lock()
        active = 0;peak = 0;entered = []
        def post(url,**kwargs):
            nonlocal active,peak
            if not url.endswith(STAGE_PATH):return original(url,**kwargs)
            rank = decode(kwargs['data'])['body']['plan']['identity']['rank']
            with lock:active += 1;peak = max(peak,active);entered.append(rank)
            try:
                if rank<2:barrier.wait(timeout=10)
                return original(url,**kwargs)
            finally:
                with lock:active -= 1
        with patch.object(f.client,'post',side_effect=post):self.assertTrue(f.dispatch())
        self.assertEqual(peak,2);self.assertEqual(sorted(entered),[0,1,2])

    def test_serial_operator_limit_is_supported_and_invalid_limits_rejected(self):
        self.group(workers=2,parallelism=1);f = self.f
        self.assertTrue(f.dispatch())
        self.assertEqual([path for path,_ in f.calls],[STAGE_PATH]*2+[START_PATH]*2)
        for invalid in (0,-1,33,True,1.5,'3'):
            with self.subTest(invalid=invalid),self.assertRaises(ValueError):
                StagingDispatch(f.registry,f.client,settings=f.runtime.lifecycle.settings,stage_parallelism=invalid)

    def test_partial_failure_cancels_inflight_private_copies_then_releases_group(self):
        _,services = self.group();f = self.f
        entered = [threading.Event(),threading.Event()];release = threading.Event()
        self.addCleanup(release.set)
        for index,service in enumerate(services[1:]):
            original = service.workspaces._copy_file_impl
            def copy(*args,_index=index,_original=original,**kwargs):
                entered[_index].set()
                if not release.wait(10):raise RuntimeError('fixture copy gate timeout')
                return _original(*args,**kwargs)
            f.stack.enter_context(patch.object(service.workspaces,'_copy_file_impl',side_effect=copy))
        def fail(body):
            self.assertTrue(all(event.wait(10) for event in entered))
            raise OSError('fixture failed rank before READY')
        original_post = f.client.post
        def post(url,**kwargs):
            result = original_post(url,**kwargs)
            if url.endswith(CONTROL_PATH) and decode(kwargs['data'])['worker_id']=='pi03':
                release.set()
            return result
        with patch.object(services[0],'stage',side_effect=fail),patch.object(f.client,'post',side_effect=post):
            with self.assertRaises(Exception):f.dispatch()
        self.assert_aborted(services)

    def test_offline_member_retains_all_reservations_until_lease_fenced(self):
        import requests
        _,services = self.group();f = self.f;original = f.client.post
        def offline(url,**kwargs):
            if '192.0.2.3:' in url:raise requests.Timeout('fixture disconnected node')
            return original(url,**kwargs)
        with patch.object(f.client,'post',side_effect=offline):
            with self.assertRaises(requests.Timeout):f.dispatch()
            self.assertEqual(len(fixture.fixtures.store.get_reservations()),3)
            self.assertEqual(fixture.fixtures.store.get_job(f.job_id)['status'],'SCHEDULED')
            self.assertEqual(f.db.execute('SELECT phase FROM hybrid_staging_attempts').fetchone()[0],'ABORTING')
            self.assertEqual(f.db.execute('SELECT MIN(renewal_blocked) FROM execution_leases').fetchone()[0],1)
            f.popen.assert_not_called()
            with f.db:f.db.execute('UPDATE execution_leases SET issued_at=?,expires_at=?,expires_clock=?',
                                  (time.time()-40,time.time()-10,fixture.worker_lease.clock()-10))
            self.assertEqual(f.runtime.reconcile(fixture.fixtures.store.get_job(f.job_id)),'QUEUED')
        self.assertEqual(fixture.fixtures.store.get_reservations(),[])
        self.assertEqual(f.db.execute('SELECT phase FROM hybrid_cleanup_ranks WHERE rank=2').fetchone()[0],'LEASE_FENCED')
        f.runtime.tick()
        self.assertEqual(f.db.execute('SELECT phase FROM hybrid_cleanup_ranks WHERE rank=2').fetchone()[0],'QUIESCENT')
        self.assertEqual(services[2].journal.records()[0]['phase'],'TERMINAL')

    def test_owner_cancellation_during_parallel_stage_never_launches(self):
        _,services = self.group();f = self.f
        barrier = threading.Barrier(3);cancelled = threading.Event();original = f.client.post
        def post(url,**kwargs):
            if url.endswith(STAGE_PATH):
                rank = decode(kwargs['data'])['body']['plan']['identity']['rank']
                barrier.wait(timeout=10)
                if rank==0:
                    fixture.fixtures.store.control_job(f.job_id,'alice','cancel');cancelled.set()
                self.assertTrue(cancelled.wait(10))
            return original(url,**kwargs)
        with patch.object(f.client,'post',side_effect=post):
            with self.assertRaises(Exception):f.dispatch()
        self.assert_aborted(services,state='CANCELLED')

    def test_peer_lease_expiry_after_last_ready_still_prevents_any_launch(self):
        _,services = self.group();f = self.f;original = f.runtime.lifecycle.record_ready;count = 0
        def record(*args,**kwargs):
            nonlocal count
            result = original(*args,**kwargs);count += 1
            if count==3:
                with f.db:f.db.execute("UPDATE execution_leases SET issued_at=?,expires_at=?,expires_clock=? WHERE node_id='pi01'",
                                      (time.time()-40,time.time()-10,fixture.worker_lease.clock()-10))
            return result
        with patch.object(f.runtime.lifecycle,'record_ready',side_effect=record):
            with self.assertRaises(ValueError):f.dispatch()
        self.assertEqual(count,3);self.assert_aborted(services)

    def test_wrong_signed_ready_aborts_every_rank(self):
        _,services = self.group();f = self.f;original = services[2].stage
        def wrong(body):
            result = original(body);result['receipt']['generation'] += 1;return result
        with patch.object(services[2],'stage',side_effect=wrong):
            with self.assertRaises(PermissionError):f.dispatch()
        self.assert_aborted(services)

    def test_authorization_revoked_after_ready_cannot_launch(self):
        _,services = self.group(dataset_owner='bob',dataset_readers=('alice',))
        f = self.f;original = f.runtime.lifecycle.record_ready;count = 0
        binding = f.registry.get_job_inputs('alice',f.job_id)
        def record(*args,**kwargs):
            nonlocal count
            result = original(*args,**kwargs);count += 1
            if count==3:
                f.registry.set_readers('bob',binding.dataset_id,[])
            return result
        with patch.object(f.runtime.lifecycle,'record_ready',side_effect=record):
            with self.assertRaises(PermissionError):f.dispatch()
        self.assert_aborted(services)

    def test_changed_reservation_after_ready_is_retained_for_review(self):
        self.group();f = self.f;original = f.runtime.lifecycle.record_ready;count = 0
        def record(*args,**kwargs):
            nonlocal count
            result = original(*args,**kwargs);count += 1
            if count==3:
                with f.db:f.db.execute("UPDATE resource_reservations SET created_at=created_at+1 WHERE node_id='pi03'")
            return result
        with patch.object(f.runtime.lifecycle,'record_ready',side_effect=record):
            with self.assertRaises(ValueError):f.dispatch()
        f.popen.assert_not_called()
        self.assertEqual(len(fixture.fixtures.store.get_reservations()),3)
        self.assertEqual(fixture.fixtures.store.get_job(f.job_id)['status'],'SCHEDULED')
        self.assertEqual(f.db.execute('SELECT phase FROM hybrid_staging_attempts').fetchone()[0],'ABORTING')

    def test_lost_ready_reply_tombstones_attempt_and_rejects_delayed_replay(self):
        import requests
        _,services = self.group();f = self.f;original = f.client.post;saved = []
        def lose(url,**kwargs):
            result = original(url,**kwargs)
            if url.endswith(STAGE_PATH) and decode(kwargs['data'])['worker_id']=='pi03':
                saved.append(decode(kwargs['data'])['body'])
                raise requests.Timeout('fixture lost READY response after successful materialization')
            return result
        with patch.object(f.client,'post',side_effect=lose):
            with self.assertRaises(requests.Timeout):f.dispatch()
        self.assert_aborted(services);self.assertEqual(len(saved),1)
        with self.assertRaises(requests.HTTPError):f.client.call(f.nodes[2],STAGE_PATH,saved[0])
        self.assert_aborted(services)

    def test_one_missing_object_aborts_whole_group_without_running(self):
        _,services = self.group();f = self.f
        plan = f.runtime.plans(fixture.fixtures.store.get_job(f.job_id))[0]
        missing = next(item for item in plan.inputs() if item.path=='data/a/rows.csv')
        (f.fixture.objects/fixture.object_key(missing.sha256)).unlink()
        with self.assertRaises(Exception):f.dispatch()
        self.assert_aborted(services)

    def test_failure_stops_admission_of_unsubmitted_ranks(self):
        _,services = self.group(parallelism=1);f = self.f
        with patch.object(services[0].fetcher,'ensure',side_effect=OSError('fixture missing object')):
            with self.assertRaises(Exception):f.dispatch()
        staged = [body['plan']['identity']['rank'] for path,body in f.calls if path==STAGE_PATH]
        self.assertEqual(staged,[0]);self.assert_aborted(services)

    def test_partial_launch_failure_aborts_all_and_retains_live_rank_reservations(self):
        import requests
        _,services = self.group();f = self.f;original = f.client.post
        def fail(url,**kwargs):
            if url.endswith(START_PATH) and decode(kwargs['data'])['worker_id']=='pi02':
                raise requests.Timeout('fixture uncertain rank launch')
            return original(url,**kwargs)
        with patch.object(f.client,'post',side_effect=fail):
            with self.assertRaises(requests.Timeout):f.dispatch()
        self.assertEqual(f.popen.call_count,1)
        self.assertEqual(len(fixture.fixtures.store.get_reservations()),3)
        self.assertEqual(f.db.execute('SELECT phase FROM hybrid_staging_attempts').fetchone()[0],'ABORTING')
        self.assertEqual(f.db.execute('SELECT MIN(renewal_blocked) FROM execution_leases').fetchone()[0],1)
        self.assertGreater(self.pins(services[0]),0)
        f.process.poll.return_value=1;services[0].quiescent=lambda *_:True
        self.assertEqual(f.runtime.reconcile(fixture.fixtures.store.get_job(f.job_id)),'QUEUED')
        self.assertEqual(fixture.fixtures.store.get_reservations(),[])
        self.assertTrue(all(self.pins(service)==0 for service in services))


if __name__=='__main__':unittest.main(verbosity=2)
