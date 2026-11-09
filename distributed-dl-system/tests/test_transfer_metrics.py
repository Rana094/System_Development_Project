"""11.1 deterministic rates and real local copies with fake NFS mount facts.

No hosts contacted; local copy timings are deliberately not benchmark claims.
"""

from concurrent.futures import ThreadPoolExecutor
import errno
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import test_cache_fetch as fixtures
from hybrid_storage.cache_fetch import CacheCapacityError, CacheFetchTimeout
from hybrid_storage.inputs import InputFile
from hybrid_storage.nfs_mount import MountNotReady
from hybrid_storage.transfer_metrics import MIB, TransferMetrics
from hybrid_storage.worker_cache import CacheCorrupt


class TransferStatisticsTests(unittest.TestCase):
    def setUp(self):
        self.now, self.wall = 100.0, 1000.0

    def collector(self, **kw):
        return TransferMetrics('pi02', clock=lambda: self.now, wall_clock=lambda: self.wall, **kw)

    def test_actual_units_median_and_mad_resist_single_peak(self):
        metrics = self.collector()
        for rate in (8, 10, 12, 10, 1000):
            metrics.record(rate*MIB, 1_000_000_000)
        snap = metrics.snapshot()
        self.assertEqual(snap['median_mib_per_second'], 10)
        self.assertEqual(snap['mad_bytes_per_second'], 2*MIB)
        self.assertEqual(snap['sample_count'], 5)
        self.assertEqual(snap['payload_bytes'], 1040*MIB)
        self.assertEqual(snap['duration_ns'], 5_000_000_000)
        self.assertEqual(snap['worker_id'], 'pi02')
        self.assertIsNone(snap['cluster_concurrent_transfers'])
        self.assertIsNone(snap['fixed_overhead_seconds'])

    def test_no_samples_stale_boundary_wall_clock_and_backward_monotonic(self):
        metrics = self.collector(max_age_seconds=10)
        self.assertEqual(metrics.snapshot()['status'], 'no_samples')
        self.assertIsNone(metrics.snapshot()['median_bytes_per_second'])
        metrics.record(MIB, 1_000_000_000)
        self.now += 10; self.wall = -1000  # Wall adjustments do not expire data.
        self.assertEqual(metrics.snapshot()['sample_count'], 1)
        self.assertEqual(metrics.snapshot()['newest_age_seconds'], 10)
        self.now += .001
        snap = metrics.snapshot()
        self.assertEqual(snap['status'], 'stale')
        self.assertEqual(snap['sample_count'], 0)
        self.assertIsNone(snap['median_bytes_per_second'])
        self.assertGreater(snap['last_observation_age_seconds'], 10)
        self.now = 99
        self.assertEqual(metrics.snapshot()['sample_count'], 0)

    def test_rolling_count_and_age_do_not_mix_workers_or_mutable_documents(self):
        metrics = self.collector(max_samples=2, max_age_seconds=5)
        for rate in (1000, 2, 4):
            metrics.record(rate*MIB, 1_000_000_000)
            self.now += 2
        snap = metrics.snapshot()
        self.assertEqual(snap['median_mib_per_second'], 3)
        self.assertEqual(snap['recorded_sample_count'], 3)
        self.assertEqual(snap['retained_sample_count'], 2)
        self.assertEqual(snap['oldest_age_seconds'], 4)
        self.assertEqual(snap['newest_age_seconds'], 2)
        snap['median_mib_per_second'] = 0
        self.assertEqual(metrics.snapshot()['median_mib_per_second'], 3)
        self.assertEqual(TransferMetrics('pi03').snapshot()['sample_count'], 0)
        self.now += 2
        self.assertEqual(metrics.snapshot()['median_mib_per_second'], 4)

    def test_reject_bad_options_bytes_durations_and_clocks(self):
        for kwargs in ({'max_samples': True}, {'max_samples':4097}, {'max_age_seconds':0},
                       {'max_age_seconds':float('nan')}, {'max_age_seconds':86401}, {'clock':3}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                TransferMetrics('pi02', **kwargs)
        for identity in ('local', '../pi02', 'pi02:/path', None):
            with self.subTest(identity=identity), self.assertRaises(ValueError):
                TransferMetrics(identity)
        metrics = self.collector()
        for size, ns in ((0,1), (1,0), (-1,1), (True,1), (1,True), (1,2**63), (2**63,1), (1,1.1)):
            with self.subTest(size=size, ns=ns), self.assertRaises(ValueError):
                metrics.record(size, ns)
        self.now = float('nan')
        with self.assertRaises(ValueError): metrics.record(1,1)
        with self.assertRaises(ValueError): metrics.snapshot()

    def test_threaded_records_remain_bounded_and_consistent(self):
        metrics = self.collector(max_samples=32)
        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(lambda _:metrics.record(MIB, 1_000_000_000), range(200)))
        snap = metrics.snapshot()
        self.assertEqual(snap['recorded_sample_count'], 200)
        self.assertEqual(snap['sample_count'], 32)
        self.assertEqual(snap['median_mib_per_second'], 1)


class TransferCopyTests(unittest.TestCase):
    # Reuse only disposable fixture helpers, not the older test methods.
    setUp = fixtures.FetchTests.setUp
    fetcher_handle = fixtures.FetchTests.fetcher_handle
    source_file = fixtures.FetchTests.source_file
    fake_spawn = fixtures.FetchTests.fake_spawn
    ensure = fixtures.FetchTests.ensure
    fake_nfs = fixtures.FetchTests.fake_nfs

    def measured_fetcher(self):
        self.metrics = TransferMetrics('pi02')
        self.fetcher = self.fetcher_handle(transfer_metrics=self.metrics)
        return self.fetcher

    def test_real_copy_records_actual_payload_duration_warm_aliases_add_nothing(self):
        self.measured_fetcher()
        files = [self.input, InputFile('alias', self.sha, len(self.data))]
        with self.fake_nfs() as copies:
            self.ensure(files)
            self.ensure(files)
        snap = self.metrics.snapshot()
        self.assertEqual(copies.call_count, 1)
        self.assertEqual(snap['sample_count'], 1)
        self.assertEqual(snap['payload_bytes'], len(self.data))
        self.assertGreater(snap['duration_ns'], 0)
        expected = len(self.data)*1_000_000_000/snap['duration_ns']
        self.assertEqual(snap['median_bytes_per_second'], expected)
        self.assertEqual(snap['median_mib_per_second'], expected/MIB)
        self.assertEqual(self.target.read_bytes(), self.data)
        self.assertNotIn(str(self.nfs), json.dumps(snap))
        self.assertNotIn(self.sha, json.dumps(snap))
        json.dumps(snap, allow_nan=False)

    def test_empty_objects_are_verified_without_division_or_rate_sample(self):
        self.measured_fetcher()
        _, sha = self.source_file(b'')
        with self.fake_nfs(): self.ensure([InputFile('empty', sha, 0)])
        self.assertEqual(self.metrics.snapshot()['sample_count'], 0)

    def test_outage_quota_and_corruption_do_not_create_success_samples(self):
        self.measured_fetcher()
        with self.assertRaises(MountNotReady): self.ensure()
        small = self.fetcher_handle(quota_bytes=1, transfer_metrics=self.metrics)
        with self.fake_nfs(small), self.assertRaises(CacheCapacityError): self.ensure(handle=small)
        path, _ = self.source_file(self.data)
        path.write_bytes(b'X'*len(self.data))
        with self.fake_nfs(), self.assertRaises(CacheCorrupt): self.ensure()
        self.assertEqual(self.metrics.snapshot()['sample_count'], 0)
        self.assertFalse(self.target.exists())

    def test_failed_attempt_not_treated_as_fast_transfer_then_retry_is_measured(self):
        self.measured_fetcher()
        calls = []
        def retry(request, output, lock):
            calls.append(request)
            if len(calls) == 1:
                os.write(request['output_fd'], b'partial')
                return subprocess.Popen([sys.executable, '-c',
                    "print('{\"status\":\"io_error\",\"errno\":%d}')" % errno.ESTALE], stdout=output)
            return self.fake_spawn(request, output, lock)
        with patch.object(self.fetcher, '_spawn', side_effect=retry):
            self.assertEqual(self.ensure()[0].attempts, 2)
        self.assertEqual(self.metrics.snapshot()['sample_count'], 1)
        self.assertEqual(self.metrics.snapshot()['payload_bytes'], len(self.data))
        self.assertEqual(self.cache.metrics.snapshot().copy_failures, 1)

    def test_revoked_after_verified_copy_has_no_publication_but_copy_remains_observed(self):
        self.measured_fetcher()
        revoked = False
        def revoke(request, output, lock):
            nonlocal revoked
            revoked = True
            return self.fake_spawn(request, output, lock)
        with patch.object(self.fetcher, '_spawn', side_effect=revoke), self.assertRaises(PermissionError):
            self.ensure(authorize=lambda _:not revoked)
        self.assertFalse(self.target.exists())
        self.assertEqual(self.metrics.snapshot()['sample_count'], 1)

    def test_observer_failure_does_not_block_verified_publication(self):
        self.measured_fetcher()
        with patch.object(self.metrics, 'record', side_effect=OSError('telemetry unavailable')), self.fake_nfs():
            self.ensure()
        self.assertEqual(self.target.read_bytes(), self.data)
        self.assertEqual(self.metrics.snapshot()['sample_count'], 0)

    def test_optional_clock_failure_does_not_change_copy_or_existing_counters(self):
        self.measured_fetcher()
        clock = SimpleNamespace(monotonic=time.monotonic, sleep=time.sleep,
                                perf_counter_ns=lambda:(_ for _ in ()).throw(OSError('clock')))
        with patch('hybrid_storage.cache_fetch.time', clock), self.fake_nfs(): self.ensure()
        self.assertEqual(self.target.read_bytes(), self.data)
        self.assertEqual(self.metrics.snapshot()['sample_count'], 0)
        self.assertGreater(self.cache.metrics.snapshot().copy_ns, 0)

    def test_helper_claim_cannot_create_sample_before_parent_hash_verification(self):
        self.measured_fetcher()
        def liar(request, output, lock):
            os.write(request['output_fd'], b'X'*len(self.data))
            os.fchmod(request['output_fd'], 0o400)
            result = dict(status='ready', sha256=self.sha, size_bytes=len(self.data), transferred_bytes=len(self.data))
            return subprocess.Popen([sys.executable, '-c', f'print({json.dumps(result)!r})'], stdout=output)
        with patch.object(self.fetcher, '_spawn', side_effect=liar), self.assertRaises(ValueError):
            self.ensure()
        self.assertFalse(self.target.exists())
        self.assertEqual(self.metrics.snapshot()['sample_count'], 0)

    def test_timed_out_helper_is_reaped_without_throughput_sample(self):
        self.measured_fetcher()
        fast = self.fetcher_handle(transfer_metrics=self.metrics, io_timeout=.03)
        def sleeper(request, output, lock):
            return subprocess.Popen([sys.executable, '-c', 'import time;time.sleep(30)'],
                stdout=output, pass_fds=(request['output_fd'],lock))
        with patch.object(fast, '_spawn', side_effect=sleeper), self.assertRaises(CacheFetchTimeout):
            self.ensure(handle=fast)
        deadline = time.monotonic()+2
        while fast.pending() and time.monotonic() < deadline:
            fast.reconcile(); time.sleep(.01)
        self.assertEqual(fast.pending(), [])
        self.assertEqual(self.metrics.snapshot()['sample_count'], 0)
        self.assertFalse(self.target.exists())

    def test_old_or_malformed_measurement_does_not_change_staging_contract(self):
        self.measured_fetcher()
        for value in ('absent', None, True, -1, len(self.data)+1):
            # New digest each time so every iteration copies rather than hits.
            data = self.data + str(value).encode()
            _, sha = self.source_file(data)
            program = fixtures.FAKE_HELPER.replace(
                'print(json.dumps(copy_request(**json.loads(sys.argv[2]))), flush=True)',
                'result = copy_request(**json.loads(sys.argv[2])); '
                + ("result.pop('transferred_bytes', None)" if value == 'absent'
                   else "result['transferred_bytes'] = " + repr(value))
                + '; print(json.dumps(result), flush=True)')
            def spawn(request, output, lock):
                return subprocess.Popen([sys.executable, '-I', '-B', '-c', program, str(ROOT), json.dumps(request)],
                    stdout=output, stderr=subprocess.DEVNULL, pass_fds=(request['output_fd'],lock))
            with patch.object(self.fetcher, '_spawn', side_effect=spawn):
                self.assertFalse(self.ensure([InputFile('input',sha,len(data))])[0].cache_hit)
        self.assertEqual(self.metrics.snapshot()['sample_count'], 0)

    def test_simultaneous_handles_root_lock_yields_one_copy_and_one_sample(self):
        self.measured_fetcher()
        other = self.fetcher_handle(transfer_metrics=self.metrics)
        with self.fake_nfs(), self.fake_nfs(other), ThreadPoolExecutor(max_workers=2) as pool:
            results = [pool.submit(self.ensure, handle=h) for h in (self.fetcher, other)]
            self.assertEqual(sorted(f.result(timeout=10)[0].cache_hit for f in results), [False,True])
        self.assertEqual(self.metrics.snapshot()['sample_count'], 1)

    def test_default_no_collector_no_rate_record_and_invalid_collector_rejected(self):
        self.assertIsNone(self.fetcher.transfer_metrics)
        with self.fake_nfs(): self.ensure()
        with self.assertRaises(ValueError): self.fetcher_handle(transfer_metrics=object())


if __name__ == '__main__':
    unittest.main(verbosity=2)
