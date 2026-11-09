"""10.2 selector/registry integration; mocked resources, disposable signed cache inventory."""
from contextlib import redirect_stdout
from dataclasses import replace, FrozenInstanceError
import io
import json
from pathlib import Path
import sqlite3
import sys
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import test_cache_coverage as fixtures
sys.path.insert(0, str(ROOT / 'agent'))
import scheduler
import distributed_dispatch as ddp
import scheduler_metrics
from hybrid_storage.cache_coverage import CacheCoverage, CoverageAPI
from hybrid_storage.inventory_reporter import InventoryReporter
from hybrid_storage.locality import JobLocality
from hybrid_storage.worker_cache import WorkerCache

SHA = 'a' * 64


def resource(cpu=5, ram=4, cores=4):
    return {'node_state': 'IDLE', 'cpu': {'usage_percent': cpu, 'available_cores': cores},
            'memory': {'available_gb': ram}, 'gpu': {'available': 0}, 'temperature_c': 40}


def coverage(worker, missing, *, rank=None, world=None):
    return CacheCoverage(worker, 'job', 'placement', None, SHA, rank, world, 3, 100,
                         100-missing, missing, (100-missing)/100, (), True, 'current',
                         1, 'fixture-session', 1, 100)


class SelectorTests(unittest.TestCase):
    def setUp(self):
        self.nodes = [{'id': 'pi03'}, {'id': 'pi02'}, {'id': 'pi01'}]
        self.resources = {n['id']: resource() for n in self.nodes}
        self.missing = {'pi03': 80, 'pi02': 23, 'pi01': 0}
        self.api = Mock(spec=CoverageAPI)
        self.api.for_job_workers.side_effect = self.batch
        self.preference = JobLocality(self.api, 'alice', 'placement', SHA)

    def batch(self, actor, workers, job_id, *, rank=None):
        return tuple(coverage(w, self.missing[w], rank=rank, world=2 if rank is not None else None)
                     for w in workers)

    def single(self, preference=True, **kwargs):
        with patch.object(scheduler, 'load_nodes', return_value=self.nodes), \
                patch.object(scheduler, 'get_resources', side_effect=lambda n: self.resources[n['id']]), \
                redirect_stdout(io.StringIO()):
            return scheduler.select_best_node(locality=self.preference if preference else None, **kwargs)

    def group(self, *, groups=None, workers=2, excluded=(), require_ownership=False):
        job = {'job_id': 'placement', 'job_config': json.dumps({
            'execution': {'workers': workers}, 'resources_per_worker': {'cpu_cores': 1}})}
        prepared = [{'node_id': n['id'], 'record': {'compatibility_key': (groups or {}).get(n['id'], 'same')}}
                    for n in self.nodes]
        with patch.object(ddp, 'load_nodes', return_value=self.nodes), \
                patch.object(ddp, 'get_resources', side_effect=lambda n: self.resources[n['id']]), \
                patch.object(ddp, 'get_job_environments', return_value=prepared), \
                patch.object(ddp, 'get_reservations', return_value=[]), \
                patch.object(ddp.ownership, 'supported', side_effect=lambda s: s.get('lease', True)):
            return ddp.select_group(job, set(excluded), require_ownership=require_ownership,
                                    locality=self.preference)

    def test_single_tie_and_freshness_are_explicit(self):
        self.assertEqual(self.single()['node']['id'], 'pi01')
        view = self.preference.snapshot()
        self.assertEqual((view.policy, view.per_rank_missing_bytes), ('locality', (0,)))
        self.assertEqual({r.worker_id for r in view.workers}, {'pi01', 'pi02', 'pi03'})
        self.assertEqual(view.workers[0].inventory_age_ns, 1)
        self.assertEqual(view.document()['total_missing_bytes'], 0)
        json.dumps(view.document())
        with self.assertRaises(FrozenInstanceError):view.policy = 'resource'

    def test_resource_off_does_not_query_inventory(self):
        self.assertEqual(self.single(preference=False)['node']['id'], 'pi03')
        self.api.for_job_workers.assert_not_called()

    def test_hard_filters_and_primary_cpu_ram_remain_authoritative(self):
        self.resources['pi01'] = resource(cores=0)
        self.assertEqual(self.single(requirements={'cpu_cores': 1})['node']['id'], 'pi02')
        self.assertNotIn('pi01', self.api.for_job_workers.call_args.args[1])
        self.resources['pi03'] = resource(cpu=1, ram=1)
        self.assertEqual(self.single()['node']['id'], 'pi03')
        self.resources['pi02'] = resource(cpu=1, ram=8)
        self.assertEqual(self.single()['node']['id'], 'pi02')
        self.assertEqual(self.single(reserved_nodes={'pi02'})['node']['id'], 'pi03')

    def test_partial_stale_knowledge_restores_exact_resource_choice(self):
        def stale(*args, **kwargs):
            rows = self.batch(*args, **kwargs)
            return (replace(rows[0], inventory_eligible=False, inventory_status='untrusted_or_stale',
                            inventory_age_ns=90_000_000_000), *rows[1:])
        self.api.for_job_workers.side_effect = stale
        self.assertEqual(self.single()['node']['id'], 'pi03')
        view = self.preference.snapshot()
        self.assertEqual(view.policy, 'resource')
        self.assertIsNone(view.per_rank_missing_bytes)
        self.assertIsNone(view.workers[0].missing_bytes)
        self.assertEqual(view.workers[0].inventory_age_ns, 90_000_000_000)

    def test_db_acl_binding_or_bad_coverage_falls_back(self):
        for failure in (sqlite3.OperationalError('locked'), PermissionError('revoked'),
                        ValueError('no immutable input binding')):
            with self.subTest(failure=type(failure).__name__):
                self.api.for_job_workers.side_effect = failure
                self.assertEqual(self.single()['node']['id'], 'pi03')
                self.assertEqual(self.preference.snapshot().reason, 'coverage_unavailable')
        for changes in ({'input_manifest_sha256': 'b'*64}, {'worker_id': 'other'},
                        {'missing_bytes': -1}, {'world_size': 99}):
            self.api.for_job_workers.side_effect = lambda *a, **k: (replace(self.batch(*a, **k)[0], **changes), *self.batch(*a, **k)[1:])
            self.assertEqual(self.single()['node']['id'], 'pi03')
            self.assertEqual(self.preference.snapshot().policy, 'resource')

    def test_equal_missing_retains_inventory_order_and_empty_resets_report(self):
        self.missing = dict.fromkeys(self.missing, 10)
        self.assertEqual(self.single()['node']['id'], 'pi03')
        self.assertIsNone(self.single(allowed_nodes=[]))
        self.assertEqual(self.preference.snapshot().selected_nodes, ())
        self.assertEqual(self.preference.snapshot().reason, 'no_eligible_workers')

    def test_ddp_boundary_ties_and_per_rank_sum(self):
        # Warm pi03 must replace cold pi02, even though ID tie rules formerly
        # selected pi01/pi02. No sample-level sharding assumptions are used.
        self.missing = {'pi03': 0, 'pi02': 80, 'pi01': 23}
        selected = self.group()
        self.assertEqual([n['id'] for n in selected], ['pi03', 'pi01'])
        view = self.preference.snapshot()
        self.assertEqual(view.per_rank_missing_bytes, (0, 23))
        self.assertEqual(view.document()['total_missing_bytes'], 23)
        self.resources['pi03'] = resource(cpu=50)
        self.assertEqual([n['id'] for n in self.group()], ['pi01', 'pi02'])

    def test_ddp_compatibility_group_ties_preserve_resource_profile(self):
        self.nodes.append({'id': 'pi04'})
        self.resources['pi04'] = resource()
        self.missing = {'pi03': 80, 'pi04': 80, 'pi02': 23, 'pi01': 0}
        groups = {'pi03': 'first', 'pi04': 'first', 'pi01': 'second', 'pi02': 'second'}
        self.assertEqual([n['id'] for n in self.group(groups=groups)], ['pi01', 'pi02'])
        # CPU sums tie but profiles differ: retain the original first group's
        # profile rather than trade resource shape for cache coverage.
        self.resources['pi03'] = resource(cpu=4)
        self.resources['pi04'] = resource(cpu=6)
        self.assertEqual([n['id'] for n in self.group(groups=groups)], ['pi03', 'pi04'])

    def test_ddp_partial_group_or_unsupported_lease_never_becomes_feasible(self):
        self.resources['pi01']['lease'] = False
        self.assertIsNone(self.group(workers=3, require_ownership=True))
        self.api.for_job_workers.assert_not_called()
        self.assertEqual(self.preference.snapshot().reason, 'no_complete_group')
        self.assertIsNone(self.group(groups={'pi01': 'a', 'pi02': 'b', 'pi03': 'c'}))
        self.api.for_job_workers.side_effect = OSError('unavailable')
        self.assertEqual([n['id'] for n in self.group()], ['pi01', 'pi02'])
        self.assertEqual(self.preference.snapshot().policy, 'resource')
        self.assertIsNone(self.preference.snapshot().per_rank_missing_bytes)
        self.api.for_job_workers.side_effect = lambda *a, **k: tuple(
            replace(r, world_size=3) for r in self.batch(*a, **k))
        self.assertEqual([n['id'] for n in self.group()], ['pi01', 'pi02'])
        self.assertEqual(self.preference.snapshot().policy, 'resource')
        self.preference = JobLocality(self.api, 'alice', 'different-job', SHA)
        self.api.for_job_workers.reset_mock()
        self.assertEqual([n['id'] for n in self.group()], ['pi01', 'pi02'])
        self.assertEqual(self.preference.snapshot().reason, 'job_context_mismatch')
        self.api.for_job_workers.assert_not_called()

    def test_metrics_labels_effective_policy(self):
        collector = scheduler_metrics.SchedulerMetrics()
        with scheduler_metrics.observe_scheduling(collector):
            self.single()
            self.api.for_job_workers.side_effect = OSError('unavailable')
            self.single()
        self.assertEqual([d.document()['policy'] for d in collector.snapshot().decisions], ['locality', 'resource'])


class RegistryIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.CoverageTests(methodName='runTest')
        self.addCleanup(self.fixture.doCleanups)
        self.fixture.setUp()
        self.job, self.binding, source, data = self.fixture.bind()
        self.fixture.warm(source, data)
        self.inventory = self.fixture.fixture
        # Second worker uses a genuine signed FULL inventory from an empty
        # protected local cache, with no listener/SSH or fabricated DB row.
        root = self.fixture.storage.base / 'cache2'
        root.mkdir(mode=0o700)
        cache = WorkerCache(root, input_mount=self.fixture.storage.base/'not-mounted2')
        cache.initialize()
        reporter = InventoryReporter(cache, self.inventory.key2, clock=self.inventory.clock,
                                     monotonic=self.inventory.clock.monotonic)
        reporter.initialize(boot_id='21234567-0123-4567-8901-012345678901')
        self.addCleanup(reporter.close)
        raw, headers = self.inventory.owner.challenge('pi02')
        body, signature = reporter.reply(raw, headers['X-DL-Inventory-MAC'], headers['X-DL-Worker-Key'])
        self.inventory.owner.accept('pi02', raw, body, signature)

    def select(self, actor='alice', *, expected_sha=None):
        preference = JobLocality(self.fixture.api, actor, self.job, expected_sha or self.binding.sha256)
        nodes = [{'id': 'pi02'}, {'id': 'pi01'}]
        with patch.object(scheduler, 'load_nodes', return_value=nodes), \
                patch.object(scheduler, 'get_resources', return_value=resource()), redirect_stdout(io.StringIO()):
            result = scheduler.select_best_node(locality=preference)
        return result['node']['id'], preference.snapshot()

    def test_signed_inventory_and_exact_job_binding_drive_actual_selector(self):
        selected, view = self.select()
        self.assertEqual(selected, 'pi01')
        rows = {r.worker_id: r for r in view.workers}
        self.assertEqual(rows['pi01'].missing_bytes, 0)
        self.assertEqual(rows['pi02'].missing_bytes, rows['pi02'].required_bytes)
        self.assertGreater(rows['pi02'].required_bytes, 0)
        self.assertEqual(view.policy, 'locality')
        before = list(self.fixture.db.iterdump())
        self.select()
        self.assertEqual(list(self.fixture.db.iterdump()), before)

    def test_stale_inventory_and_wrong_owner_binding_have_no_locality_certainty(self):
        self.inventory.clock.advance(16)
        selected, view = self.select()
        self.assertEqual((selected, view.policy), ('pi02', 'resource'))
        self.assertIsNone(view.per_rank_missing_bytes)
        for actor, digest in [('bob', None), ('alice', 'b'*64)]:
            selected, view = self.select(actor, expected_sha=digest)
            self.assertEqual((selected, view.policy), ('pi02', 'resource'))

    def test_batch_uses_one_read_transaction_and_preserves_single_query_contract(self):
        statements = []
        self.fixture.db.set_trace_callback(statements.append)
        self.addCleanup(lambda: self.fixture.db.set_trace_callback(None))
        batch = self.fixture.api.for_job_workers('alice', ('pi01', 'pi02'), self.job)
        self.assertEqual(sum(s.strip().upper().startswith('BEGIN') for s in statements), 1)
        self.assertEqual(batch[0], self.fixture.api.for_job('alice', 'pi01', self.job))
        self.assertTrue(all(r.input_manifest_sha256 == self.binding.sha256 for r in batch))
        for bad in ((), ('pi01', 'pi01'), ('../pi01',), ['pi01']):
            with self.assertRaises(ValueError):self.fixture.api.for_job_workers('alice', bad, self.job)
        job, binding, _, _ = self.fixture.bind(distributed=True)
        full = self.fixture.api.for_job_workers('alice', ('pi01', 'pi02'), job, rank=0)
        for worker, row in zip(('pi01', 'pi02'), full):
            other_rank = self.fixture.api.for_job('alice', worker, job, rank=2)
            self.assertEqual(row.input_manifest_sha256, binding.sha256)
            self.assertEqual((row.rank, row.world_size), (0, 3))
            self.assertEqual((row.required_bytes, row.missing_bytes, row.missing_hashes),
                             (other_rank.required_bytes, other_rank.missing_bytes, other_rank.missing_hashes))


if __name__ == '__main__':
    unittest.main(verbosity=2)
