"""11.4 combined efficiency/fault acceptance, entirely disposable and Pi-less.

Rates/disk failures are controlled fixtures, not network benchmarks. Actual SHA
objects, immutable bindings, signed inventory, SQLite, selectors and child-process
crash boundaries run locally. Queue launch and training/isolation remain mocked.
"""
from contextlib import redirect_stdout
from dataclasses import replace
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import threading
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import test_bandwidth_policy as bandwidth
import test_cache_inventory as inventory
import test_cache_fetch as fetch_fixture
import test_scheduler_locality_acceptance as queue
import scheduler
import job_store as store
from agent.cache_storage import cache_maintenance
from hybrid_storage.bandwidth import JobBandwidth, TransferObservations
from hybrid_storage.cache_coverage import CoverageAPI
from hybrid_storage.cache_fetch import CacheFetcher, CacheCapacityError
from hybrid_storage.cache_inventory import CacheInventory
from hybrid_storage.dataset_manifests import DatasetEntry
from hybrid_storage.inputs import InputFile, object_key
from hybrid_storage.inventory_reporter import InventoryReporter
from hybrid_storage.scheduling import SchedulingPolicy
from hybrid_storage.staging_protocol import STAGE_PATH, START_PATH
from hybrid_storage.transfer_metrics import TransferMetrics
from hybrid_storage.worker_cache import WorkerCache, CacheActivityBusy
from hybrid_storage.workspaces import AttemptIdentity, WorkspacePlan
from hybrid_storage import inventory_protocol as protocol


class SignedRateWorkloadTests(unittest.TestCase):
    """Three real inventories and exact coverage, with synthetic changing rates."""
    def setUp(self):
        self.fixture = queue.ExactMBTests(methodName='runTest')
        self.addCleanup(self.fixture.doCleanups); self.fixture.setUp()
        f = self.fixture
        self.clock, self.registry = f.clock, f.registry
        source = f.storage.source(f.registry)
        artifact = f.registry.register_artifact('alice', io.BytesIO(b'x'*100), expected_size=100)
        dataset = f.registry.create_dataset('alice', [
            DatasetEntry('data.bin', 'file', artifact.sha256, 100),
            DatasetEntry('alias.bin', 'file', artifact.sha256, 100)])
        self.job, binding = f.job(1, source, dataset)
        self.plans = (WorkspacePlan(AttemptIdentity(self.job['job_id'], self.job['job_id']+':0'),
                                    binding.sha256, source, dataset),)
        f.fixture.owner.close()
        keys = (f.fixture.fixture.key, f.fixture.fixture.key2,
                protocol.Credential('pi03', 'c'*32, b'3'*32))
        self.owner = CacheInventory(f.storage.connect, keys, configured_workers=[k.worker for k in keys],
            state_root=f.storage.base, clock=self.clock,
            transfer_observations=TransferObservations(clock=self.clock.monotonic))
        self.owner.start(); self.addCleanup(self.owner.close)
        self.metrics, self.reporters = {}, {}
        for key in keys:
            self.owner.enroll(key.worker)
            root = f.storage.base/(key.worker+'-efficiency'); root.mkdir(mode=0o700)
            cache = WorkerCache(root, input_mount=f.storage.base/'unused-nfs'); cache.initialize()
            if key.worker == 'pi01':
                for entry in source.files:
                    target = root/'objects'/object_key(entry.sha256)
                    target.parent.parent.mkdir(mode=0o700, exist_ok=True)
                    target.parent.mkdir(mode=0o700, exist_ok=True)
                    shutil.copyfile(f.storage.objects/object_key(entry.sha256), target)
                    target.chmod(0o400); cache.verify(entry.sha256, entry.size_bytes, force=True)
            metrics = TransferMetrics(key.worker, max_samples=3, max_age_seconds=60,
                clock=self.clock.monotonic, wall_clock=lambda: self.clock()/1e9)
            reporter = InventoryReporter(cache, key, transfer_metrics=metrics, clock=self.clock,
                monotonic=self.clock.monotonic, full_interval_seconds=1)
            reporter.initialize(boot_id=queue.BOOT); self.addCleanup(reporter.close)
            self.metrics[key.worker], self.reporters[key.worker] = metrics, reporter
        self.api = CoverageAPI(self.registry, self.owner)
        self.policy = SchedulingPolicy(self.registry, settings=bandwidth.BANDWIDTH, coverage=self.api)
        self.nodes = [{'id': worker} for worker in ('pi03', 'pi02', 'pi01')]
        for worker, rate in (('pi01', 1), ('pi02', 1000), ('pi03', 10)):
            self.samples(worker, rate, 3)
        self.publish()

    def samples(self, worker, rate, count):
        for _ in range(count):
            ns = int(1000/rate*1e9)
            self.metrics[worker].record(1000, ns+200_000_000, transfer_ns=ns)

    def publish(self):
        for worker, reporter in self.reporters.items():
            self.clock.advance(.001)
            request, headers = self.owner.challenge(worker)
            body, mac = reporter.reply(request, headers['X-DL-Inventory-MAC'], headers['X-DL-Worker-Key'])
            self.owner.accept(worker, request, body, mac)

    def select(self, **kwargs):
        with patch.object(scheduler, 'load_nodes', return_value=self.nodes), \
                patch.object(scheduler, 'get_resources', return_value=queue.support.resource()), \
                redirect_stdout(io.StringIO()):
            return self.policy.select(self.job, self.plans, scheduler.select_best_node, **kwargs)['node']['id']

    def test_signed_rate_drop_changes_choice_after_rolling_majority_with_transparent_cost(self):
        self.assertEqual(self.select(), 'pi02')
        self.samples('pi02', 1, 1); self.publish()
        self.assertEqual(self.select(), 'pi02')  # One slow sample is not a new median.
        self.samples('pi02', 1, 1); self.publish()
        self.assertEqual(self.select(), 'pi03')
        record = self.policy.history()[-1]
        for row in record['bandwidth']['workers']:
            expected = row['missing_bytes']/row['rate']['median_bytes_per_second'] + row['missing_objects']*.2
            self.assertAlmostEqual(row['seconds'], expected)
            self.assertEqual(row['rate']['sample_count'], 3)
            self.assertGreaterEqual(row['upper_seconds'], row['seconds'])
            self.assertLessEqual(row['lower_seconds'], row['seconds'])
        self.assertIsNone(record['bandwidth']['cluster_active_transfers'])
        self.samples('pi02', 1000, 3); self.publish()
        self.assertEqual(self.select(), 'pi02')

    def test_fresh_inventory_cannot_refresh_old_rates_and_locality_resource_rollback(self):
        for _ in range(7):
            self.clock.advance(10); self.publish()
        self.assertTrue(all(self.owner.eligible(w) for w in self.reporters))
        self.assertEqual(self.select(), 'pi03')
        self.assertEqual(self.policy.history()[-1]['policy'], 'resource')
        self.assertIsNone(self.policy.history()[-1]['bandwidth']['selected_estimate'])
        before = list(self.fixture.fixture.db.iterdump())
        self.policy.set_policy('locality'); self.assertEqual(self.select(), 'pi01')
        self.policy.set_policy('resource')
        with patch.object(self.api, 'for_job_workers', side_effect=AssertionError('resource must be independent')):
            self.assertEqual(self.select(), 'pi03')
        self.assertEqual(list(self.fixture.fixture.db.iterdump()), before)

    def test_no_samples_offline_and_hard_resource_exclusion_do_not_claim_fast_eligibility(self):
        self.reporters['pi02'].transfer_metrics = TransferMetrics('pi02', clock=self.clock.monotonic)
        self.publish(); self.assertEqual(self.select(), 'pi03')
        self.assertEqual(self.policy.history()[-1]['policy'], 'resource')
        self.reporters['pi02'].transfer_metrics = self.metrics['pi02']; self.publish()
        self.assertEqual(self.select(excluded_nodes={'pi02'}), 'pi03')
        self.owner.offline('pi02'); self.assertEqual(self.select(), 'pi03')
        self.assertEqual(self.policy.history()[-1]['policy'], 'resource')
        self.assertIsNone(self.policy.history()[-1]['total_missing_bytes'])


class BandwidthQueueTests(unittest.TestCase):
    """Real queue/quotas/reservations; synthetic rates/coverage and mocked launch."""
    setUp = queue.QueueAcceptanceTests.setUp
    database = queue.QueueAcceptanceTests.database
    add = queue.QueueAcceptanceTests.add
    complete = queue.QueueAcceptanceTests.complete

    def run_pass(self, locality):
        preferences = []
        def preference(api, *args):
            original = api.for_job_workers.side_effect
            api.for_job_workers.side_effect = lambda *a, **k: tuple(replace(row,
                missing_hashes=('d'*64,) if row.missing_bytes else ()) for row in original(*a, **k))
            api.inventory = Mock(spec=CacheInventory)
            api.inventory.bandwidth_facts.return_value = dict(disk_available_bytes=10**12, transfer=bandwidth.usable(100))
            result = JobBandwidth(api, *args, stage_parallelism=2)
            preferences.append(result)
            return result
        with patch.object(queue, 'JobLocality', side_effect=preference):
            result = queue.QueueAcceptanceTests.run_pass(self, locality)
        for pref in preferences:
            decision = pref.snapshot()
            if decision.selected_nodes:
                self.assertEqual(decision.policy, 'bandwidth')
        return result

    test_bandwidth_continuous_warm_arrivals_preserve_resource_FIFO_and_owner_turns = \
        queue.QueueAcceptanceTests.test_continuous_warm_arrivals_cannot_preempt_aged_cold_FIFO
    test_bandwidth_infeasible_head_does_not_block_other_owner = \
        queue.QueueAcceptanceTests.test_infeasible_cold_head_does_not_block_other_owners
    test_bandwidth_uncertain_cancellation_retains_quota = \
        queue.QueueAcceptanceTests.test_uncertain_and_cancelling_jobs_keep_quota_until_safe_release
    test_bandwidth_DDP_exclusive_reservations_and_no_partial_group = \
        queue.QueueAcceptanceTests.test_DDP_one_owner_slot_multiple_exclusive_workers_never_partial


class CrashInventoryRestageTests(unittest.TestCase):
    def setUp(self):
        self.f = inventory.InventoryTests(methodName='runTest')
        self.addCleanup(self.f.doCleanups); self.f.setUp()
        f = self.f
        f.cache.input_mount.mkdir()
        target = f.cache.input_mount/object_key(f.artifact.sha256)
        target.parent.mkdir(parents=True); target.write_bytes(f.data); target.chmod(0o400)
        self.authority = target
        self.fetcher = CacheFetcher(f.cache, source=fetch_fixture.SOURCE, quota_bytes=1024*1024,
                                   reserve_free_bytes=0, retry_delay=0)
        self.fetcher.initialize(); self.addCleanup(self.fetcher.close)
        self.input = InputFile('data.bin', f.artifact.sha256, len(f.data))
        self.assertTrue(f.poll())

    def spawn(self, request, output, lock):
        return subprocess.Popen([sys.executable, '-I', '-B', '-c', fetch_fixture.FAKE_HELPER,
            str(ROOT), json.dumps(request)], stdin=subprocess.DEVNULL, stdout=output,
            stderr=subprocess.DEVNULL, pass_fds=(request['output_fd'], lock))

    def test_eviction_crash_restart_signed_convergence_and_authorized_restaging(self):
        f = self.f
        program = """
import os,sys
from pathlib import Path
sys.path.insert(0,sys.argv[1])
from hybrid_storage.worker_cache import WorkerCache
from hybrid_storage.cache_fetch import CacheFetcher
from hybrid_storage.cache_eviction import CacheEvictor
cache=WorkerCache(Path(sys.argv[2]),input_mount=Path(sys.argv[3]))
gc=CacheEvictor(CacheFetcher(cache,source=sys.argv[4],quota_bytes=1,reserve_free_bytes=0))
def crash(prefix,digest):
    os.unlink(digest,dir_fd=prefix)
    os._exit(42)
gc._unlink=crash
gc.apply(gc.plan(),dry_run=False)
"""
        result = subprocess.run([sys.executable, '-B', '-c', program, str(ROOT), str(f.cache_root),
            str(f.cache.input_mount), fetch_fixture.SOURCE], capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 42, result.stderr)
        self.assertFalse(f.target.exists())
        self.assertEqual(f.entries(), [(f.artifact.sha256, 'VERIFIED', len(f.data))])  # Advisory lag.
        restarted = WorkerCache(f.cache_root, input_mount=f.cache.input_mount); restarted.initialize()
        restarted_fetch = CacheFetcher(restarted, source=fetch_fixture.SOURCE, quota_bytes=1024*1024,
                                       reserve_free_bytes=0)
        self.addCleanup(restarted_fetch.close)
        gc = cache_maintenance(restarted_fetch, notify=f.reporter.request_full,
                               protected_roots=(f.fixture.objects,))
        reconciled = gc.reconcile()
        self.assertEqual(reconciled['missing'], 1)
        self.assertTrue(f.poll()); self.assertEqual(f.entries(), [(f.artifact.sha256, 'REMOVED', 0)])
        with patch.object(restarted_fetch, '_spawn', side_effect=self.spawn) as spawn:
            with self.assertRaises(PermissionError):
                restarted_fetch.ensure([self.input], authorize=lambda _: False)
            spawn.assert_not_called()
            self.assertFalse(restarted_fetch.ensure([self.input], authorize=lambda _: True)[0].cache_hit)
            self.assertTrue(restarted_fetch.ensure([self.input], authorize=lambda _: True)[0].cache_hit)
            self.assertEqual(spawn.call_count, 1)
        self.assertEqual(list((f.cache_root/'parts').iterdir()), [])
        self.assertTrue(f.poll()); self.assertEqual(f.entries(), [(f.artifact.sha256, 'VERIFIED', len(f.data))])
        self.assertEqual(self.authority.read_bytes(), f.data)
        self.assertEqual((f.fixture.objects/f.artifact.key).read_bytes(), f.data)

    def test_full_disk_with_pinned_input_neither_eviction_nor_failed_copy_damages_authority(self):
        f = self.f
        f.cache.pin(f.artifact.sha256, len(f.data), job_id='active', attempt_id='attempt')
        self.fetcher.quota_bytes = len(f.data)
        gc = cache_maintenance(self.fetcher, protected_roots=(f.fixture.objects,))
        self.assertEqual(gc.plan(incoming_bytes=10).status, 'blocked_pins')
        new = f.register(b'new input')
        with patch.object(self.fetcher, '_spawn', side_effect=AssertionError('quota rejects before transfer')):
            with self.assertRaises(CacheCapacityError):
                self.fetcher.ensure([InputFile('new.bin', new.sha256, new.size_bytes)], authorize=lambda _: True)
        self.assertEqual(f.target.read_bytes(), f.data)
        report = gc.reconcile()
        self.assertEqual((report['pins_before'], report['pins_after']), (1, 1))
        self.assertEqual(gc.plan(incoming_bytes=10).status, 'blocked_pins')
        self.assertEqual(list((f.cache_root/'parts').iterdir()), [])
        self.assertEqual(self.authority.read_bytes(), f.data)


class ConcurrentBandwidthStagingTests(unittest.TestCase):
    def test_bounded_three_rank_staging_blocks_GC_and_policy_rollback_preserves_running_group(self):
        f = bandwidth.ConfiguredDispatchTests(methodName='runTest')
        self.addCleanup(f.doCleanups); f.setUp(); f.configure(3)
        actual = f.f; services = [actual.service, *actual.extra_services]
        barrier = threading.Barrier(2); lock = threading.Lock()
        active = 0; peak = 0; gaps = []
        original = actual.client.post
        def post(url, **kwargs):
            nonlocal active, peak
            if not url.endswith(STAGE_PATH): return original(url, **kwargs)
            rank = protocol.decode(kwargs['data'])['body']['plan']['identity']['rank']
            with lock: active += 1; peak = max(peak, active)
            try:
                if rank < 2: barrier.wait(timeout=10)
                return original(url, **kwargs)
            finally:
                with lock: active -= 1
        from contextlib import ExitStack
        with ExitStack() as stack:
            for service in services:
                ensure = service.fetcher.ensure
                def checked(*args, _ensure=ensure, _service=service, **kwargs):
                    result = _ensure(*args, **kwargs)
                    with self.assertRaises(CacheActivityBusy): cache_maintenance(_service.fetcher).plan()
                    with lock: gaps.append(_service.credential.worker)
                    return result
                stack.enter_context(patch.object(service.fetcher, 'ensure', side_effect=checked))
            stack.enter_context(patch.object(actual.client, 'post', side_effect=post))
            stack.enter_context(patch.object(bandwidth.ddp, 'get_resources', return_value=queue.support.resource()))
            stack.enter_context(patch.object(bandwidth.ddp, 'load_nodes', return_value=actual.nodes))
            self.assertTrue(f.dispatch())
        self.assertEqual(peak, 2); self.assertEqual(set(gaps), {'pi01', 'pi02', 'pi03'})
        first = next(i for i, (path, _) in enumerate(actual.calls) if path == START_PATH)
        self.assertEqual([path for path, _ in actual.calls[:first]], [STAGE_PATH]*3)
        before = list(actual.db.iterdump())
        actual.runtime.set_scheduler_policy('resource')
        self.assertEqual(list(actual.db.iterdump()), before)
        self.assertEqual(len(store.get_reservations()), 3)
        for service in services:
            fetch = CacheFetcher(service.fetcher.cache, source=fetch_fixture.SOURCE, quota_bytes=1, reserve_free_bytes=0)
            self.assertEqual(cache_maintenance(fetch).plan().status, 'blocked_pins')


if __name__ == '__main__':
    unittest.main(verbosity=2)
