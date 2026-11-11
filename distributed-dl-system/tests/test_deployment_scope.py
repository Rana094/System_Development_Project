"""Explicit two-worker scope never implies all-three acceptance or weaker auth."""
from copy import deepcopy
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from hybrid_storage import deployment_toolkit as kit
from hybrid_storage import deployment_executor as host
from test_deployment_toolkit import ToolkitTests
from test_deployment_executor import ExecutorTests


class ScopeTests(unittest.TestCase):
    def setUp(self):
        fixture = ToolkitTests(methodName='runTest')
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        self.f = fixture
        self.selected = ['pi02', 'pi03']
        self.observed = {n: v for n, v in fixture.observations.items() if n != 'pi01'}

    def build(self, observed=None):
        return kit.prepare(self.f.output, host.encoded(self.observed if observed is None else observed),
                           root=self.f.root, workers=self.selected)

    def test_two_node_kit_has_no_pi01_payload_or_operations_and_preserves_other_gates(self):
        result = self.build()
        self.assertEqual(result['selected_workers'], ['pi02', 'pi03'])
        self.assertEqual(result['deferred_workers'], ['pi01'])
        self.assertFalse((self.f.output / 'pi01').exists())
        ops = host.decode((self.f.output / 'OPERATIONS.json').read_bytes())
        self.assertEqual(ops['stop_order'], ['controller', 'pi02', 'pi03'])
        self.assertEqual({h['host'] for h in ops['hosts']}, {'controller', 'pi02', 'pi03'})
        for name in ('controller', *self.selected):
            plan = host.decode((self.f.output / name / 'host-plan.json').read_bytes())
            self.assertEqual(plan['selected_workers'], self.selected)
            self.assertNotIn('B3', plan['required_gate_ids'])
            self.assertIn('B1', plan['required_gate_ids'])
            self.assertIn('B2', plan['required_gate_ids'])
            self.assertIn('B4', plan['required_gate_ids'])
            self.assertEqual(plan['deferred_gate_ids'], ['B3'])

    def test_no_implicit_deferral_when_two_node_inventory_given_without_selection(self):
        with self.assertRaises(ValueError):
            kit.prepare(self.f.output, host.encoded(self.observed), root=self.f.root)
        self.assertFalse(self.f.output.exists())

    def test_extra_pi01_or_missing_selected_worker_is_not_silently_ignored(self):
        for observed in (self.f.observations, {n: v for n, v in self.observed.items() if n != 'pi03'}):
            with self.assertRaises(ValueError):
                self.build(observed)
        self.assertFalse(self.f.output.exists())

    def test_retry_exact_scope_is_idempotent_and_changing_scope_requires_new_review(self):
        first = self.build()
        before = self.f.tree()
        self.assertEqual(self.build(), first)
        self.assertEqual(self.f.tree(), before)
        with self.assertRaises(ValueError):
            kit.prepare(self.f.output, host.encoded(self.f.observations), root=self.f.root)
        self.assertEqual(self.f.tree(), before)

    def test_cross_scope_and_ambiguous_approval_cannot_mutate_source(self):
        f = ExecutorTests(methodName='runTest')
        f.setUp()
        self.addCleanup(f.doCleanups)
        f.plan['selected_workers'] = self.selected
        receipt = f.receipt()
        # A default/all-three receipt must not authorize a two-node plan.
        with self.assertRaises(PermissionError):
            host.install(f.plan, f.directory, receipt, services=f.stopped)
        self.assertFalse((f.app / '.hybrid-deploy').exists())
        receipt.pop('all_three_workers')
        receipt['selected_workers'] = self.selected
        ambiguous = deepcopy(receipt)
        ambiguous['all_three_workers'] = ['pi01', 'pi02', 'pi03']
        with self.assertRaises(PermissionError):
            host.install(f.plan, f.directory, ambiguous, services=f.stopped)
        legacy_two = deepcopy(receipt)
        legacy_two.pop('selected_workers')
        legacy_two['all_three_workers'] = self.selected
        with self.assertRaises(PermissionError):
            host.install(f.plan, f.directory, legacy_two, services=f.stopped)
        self.assertEqual(host.install(f.plan, f.directory, receipt, services=f.stopped)['status'], 'INSTALLED_SERVICES_STOPPED')

    def test_invalid_scope_is_rejected_before_any_output(self):
        for workers in ([], ['pi02'], ['pi02', 'pi02'], ['pi02', 'pi99'], ['controller', 'pi02']):
            with self.subTest(workers=workers), self.assertRaises(ValueError):
                kit.prepare(self.f.output, host.encoded(self.observed), root=self.f.root, workers=workers)
        self.assertFalse(self.f.output.exists())


if __name__ == '__main__':
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(ScopeTests)
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
