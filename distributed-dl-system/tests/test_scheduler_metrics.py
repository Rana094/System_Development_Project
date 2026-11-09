"""Resource decisions match frozen pre-instrumentation outputs; no live cluster."""
from contextlib import redirect_stdout
from copy import deepcopy
from dataclasses import FrozenInstanceError
import io
import json
from pathlib import Path
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'controller'))
import scheduler
import distributed_dispatch as ddp
import scheduler_metrics as metrics


def snapshot(cpu=10, ram=4, cores=4, gpu=0, state='IDLE', lease=True):
    return {'node_state': state, 'cpu': {'usage_percent': cpu, 'available_cores': cores,
            'logical_cores': 4}, 'memory': {'available_gb': ram},
            'gpu': {'available': gpu}, 'temperature_c': 40, 'lease_test': lease}


def cases():
    """Inputs only; expected outputs live in the pre-edit golden fixture."""
    base = {'selector': 'single', 'order': ['pi03', 'pi02', 'pi01'],
            'snapshots': {key: snapshot() for key in ('pi01', 'pi02', 'pi03')},
            'args': {}}
    result = []

    def add(name, **changes):
        case = deepcopy(base)
        case.update(changes, name=name)
        result.append(case)
        return case

    add('single_equal_keeps_inventory')
    add('single_equal_reversed', order=['pi01', 'pi02', 'pi03'])
    c = add('single_cpu_before_ram')
    c['snapshots']['pi01'] = snapshot(cpu=1, ram=1)
    c['snapshots']['pi02'] = snapshot(cpu=2, ram=100)
    c = add('single_ram_breaks_cpu_tie')
    c['snapshots']['pi02'] = snapshot(ram=8)
    add('single_allowed_reserved_excluded', args={'allowed_nodes': ['pi02', 'pi01'],
        'reserved_nodes': ['pi02'], 'excluded_nodes': ['pi01']})
    c = add('single_offline_busy_capacity', args={'requirements': {'memory_gb': 5}})
    c['snapshots']['pi03'] = None
    c['snapshots']['pi02'] = snapshot(state='RUNNING')
    for field, value in [('cpu_cores', 5), ('memory_gb', 5), ('gpus', 1)]:
        add('single_insufficient_' + field, args={'requirements': {field: value}})
    c = add('single_logical_cores_fallback', args={'requirements': {'cpu_cores': 4}})
    for value in c['snapshots'].values():
        del value['cpu']['available_cores']
    c = add('single_available_cores_precedes_logical', args={'requirements': {'cpu_cores': 2}})
    c['snapshots']['pi03']['cpu']['available_cores'] = 0
    c = add('single_temperature_is_not_a_filter')
    c['snapshots']['pi03']['temperature_c'] = 100
    add('single_empty_inventory', order=[])

    base.update(selector='ddp', prepared={key: 'same' for key in base['order']},
                args={'excluded': [], 'workers': 2}, reserved=[])
    add('ddp_equal_uses_node_id')
    c = add('ddp_cpu_before_ram')
    c['snapshots']['pi03'] = snapshot(cpu=0, ram=1)
    c['snapshots']['pi02'] = snapshot(cpu=1, ram=1)
    c['snapshots']['pi01'] = snapshot(cpu=2, ram=100)
    c = add('ddp_ram_before_node_id')
    c['snapshots']['pi03'] = snapshot(ram=8)
    add('ddp_never_mix_incomplete_groups', prepared={'pi03': 'a', 'pi02': 'b', 'pi01': 'c'})
    add('ddp_full_three', args={'excluded': [], 'workers': 3})
    add('ddp_reserved_before_excluded_unprepared', reserved=['pi03'],
        prepared={'pi01': 'same'}, args={'excluded': ['pi03', 'pi02'], 'workers': 2})
    add('ddp_unprepared_not_probed', prepared={'pi01': 'same', 'pi02': 'same'})
    c = add('ddp_requires_lease', args={'excluded': [], 'workers': 2, 'require_ownership': True})
    c['snapshots']['pi03']['lease_test'] = False
    add('ddp_resume_compatibility', prepared={'pi03': 'other', 'pi02': 'same', 'pi01': 'same'},
        args={'excluded': [], 'workers': 2, 'resume': {'compatibility_key': 'same'}})
    c = add('ddp_resource_filters')
    c['snapshots']['pi03'] = None
    c['snapshots']['pi02'] = snapshot(state='RUNNING')
    c['snapshots']['pi01'] = snapshot(cores=0)
    # Equal total CPU across compatible groups retains first encountered group,
    # even if another group has more RAM or a lexically smaller compatibility key.
    for lower_cpu in (False, True):
        add('ddp_group_' + ('lower_total_cpu' if lower_cpu else 'tie_inventory'),
            order=['pi04', 'pi03', 'pi02', 'pi01'],
            prepared={'pi04': 'z', 'pi03': 'z', 'pi02': 'a', 'pi01': 'a'},
            snapshots={'pi04': snapshot(cpu=10, ram=1), 'pi03': snapshot(cpu=10, ram=1),
                       'pi02': snapshot(cpu=9 if lower_cpu else 10, ram=8),
                       'pi01': snapshot(cpu=10, ram=8)})
    return result


def invoke(case):
    """Mock network, inventory and DB reads; retain exact outputs and probe order."""
    nodes = [{'id': key, 'ip': '192.0.2.' + str(i + 1), 'port': 5000}
             for i, key in enumerate(case['order'])]
    probes = []
    resources = deepcopy(case['snapshots'])

    def probe(node):
        probes.append(node['id'])
        return resources[node['id']]

    stdout = io.StringIO()
    module = scheduler if case['selector'] == 'single' else ddp
    with redirect_stdout(stdout), patch.object(module, 'load_nodes', return_value=nodes), \
            patch.object(module, 'get_resources', side_effect=probe):
        args = deepcopy(case['args'])
        if module is scheduler:
            selected = module.select_best_node(**args)
            if selected:
                # Identity is also part of the existing selector's caller contract.
                assert selected['node'] is nodes[case['order'].index(selected['node']['id'])]
                assert selected['resources'] is resources[selected['node']['id']]
        else:
            count = args.pop('workers')
            job = {'job_id': 'fixture', 'job_config': json.dumps({
                'execution': {'workers': count},
                'resources_per_worker': {'cpu_cores': 1, 'memory_gb': 1, 'gpus': 0}})}
            prepared = [{'node_id': key, 'record': {'compatibility_key': value}}
                        for key, value in case['prepared'].items()]
            with patch.object(ddp, 'get_job_environments', return_value=prepared), \
                    patch.object(ddp, 'get_reservations', return_value=[
                        {'node_id': key} for key in case['reserved']]), \
                    patch.object(ddp.ownership, 'supported', side_effect=lambda s: s['lease_test']):
                selected = ddp.select_group(job, **args)
                for node in selected or []:
                    assert node is nodes[case['order'].index(node['id'])]
    return {'result': selected, 'probes': probes, 'stdout': stdout.getvalue()}


class MetricsTests(unittest.TestCase):
    def test_frozen_pre_edit_decisions_off_disabled_and_on(self):
        fixture = json.loads((ROOT / 'tests/fixtures/resource-scheduler-v1.json').read_text())
        self.assertEqual(set(fixture['expected']), {case['name'] for case in cases()})
        with patch.object(metrics.time, 'perf_counter_ns', side_effect=AssertionError('OFF clock')) as clock:
            for case in cases():
                with self.subTest(case=case['name'], mode='default-OFF'):
                    self.assertEqual(invoke(case), fixture['expected'][case['name']])
            clock.assert_not_called()
        for case in cases():
            collector = metrics.SchedulerMetrics()
            before = deepcopy(case)
            with self.subTest(case=case['name']), metrics.observe_scheduling(collector):
                with metrics.observe_scheduling(None), \
                        patch.object(metrics.time, 'perf_counter_ns') as clock:
                    self.assertEqual(invoke(case), fixture['expected'][case['name']])
                    clock.assert_not_called()
                self.assertEqual(collector.snapshot().recorded, 0)
                self.assertEqual(invoke(case), fixture['expected'][case['name']])
                self.assertEqual(case, before)
                decision = collector.snapshot().decisions[0]
                self.assertEqual(decision.resource_queries, len(fixture['expected'][case['name']]['probes']))
                self.assertEqual(decision.examined_nodes, len(case['order']))
                self.assertEqual(sum(dict(decision.filtered).values()) + decision.eligible_nodes,
                                 decision.examined_nodes)
                self.assertEqual(decision.selector, case['selector'])
                self.assertGreaterEqual(decision.duration_ns, 0)

    def test_single_counters_and_duration(self):
        case = next(c for c in cases() if c['name'] == 'single_offline_busy_capacity')
        collector = metrics.SchedulerMetrics()
        with metrics.observe_scheduling(collector), \
                patch.object(metrics.time, 'perf_counter_ns', side_effect=[100, 125]):
            self.assertIsNone(invoke(case)['result'])
        decision = collector.snapshot().decisions[0]
        self.assertEqual(decision.duration_ns, 25)
        self.assertEqual((decision.eligible_nodes, decision.selected_count), (0, 0))
        self.assertEqual({k: v for k, v in decision.filtered if v},
                         {'unavailable': 1, 'busy': 1, 'resources': 1})

    def test_ddp_groups_and_filters(self):
        collector = metrics.SchedulerMetrics()
        with metrics.observe_scheduling(collector):
            for name in ('ddp_group_tie_inventory', 'ddp_reserved_before_excluded_unprepared',
                         'ddp_never_mix_incomplete_groups', 'ddp_requires_lease',
                         'ddp_resume_compatibility', 'ddp_unprepared_not_probed'):
                invoke(next(c for c in cases() if c['name'] == name))
        a, b, c, d, e, f = collector.snapshot().decisions
        self.assertEqual((a.compatibility_groups, a.complete_groups, a.selected_nodes),
                         (2, 2, ('pi03', 'pi04')))
        self.assertEqual({k: v for k, v in b.filtered if v}, {'reserved': 1, 'excluded': 1})
        self.assertEqual((c.compatibility_groups, c.complete_groups, c.selected_count), (3, 0, 0))
        self.assertEqual(dict(d.filtered)['lease'], 1)
        self.assertEqual(dict(e.filtered)['compatibility'], 1)
        self.assertEqual(dict(f.filtered)['unprepared'], 1)

    def test_bounded_immutable_snapshot_and_json(self):
        collector = metrics.SchedulerMetrics(max_decisions=2)
        with metrics.observe_scheduling(collector):
            for case in cases()[:3]:
                invoke(case)
        view = collector.snapshot()
        self.assertEqual((view.recorded, view.dropped, len(view.decisions)), (3, 1, 2))
        with self.assertRaises(FrozenInstanceError):
            view.decisions[0].selected_count = 100
        document = json.loads(json.dumps(view.document()))
        self.assertEqual(document['decisions'][0]['policy'], 'resource')
        document['decisions'][0]['selected_nodes'].clear()
        self.assertEqual(collector.snapshot(), view)

    def test_nested_context_restores_after_exception(self):
        outer, inner = metrics.SchedulerMetrics(), metrics.SchedulerMetrics()
        with metrics.observe_scheduling(outer):
            with self.assertRaisesRegex(ValueError, 'fixture'):
                with metrics.observe_scheduling(inner):
                    invoke(cases()[0])
                    raise ValueError('fixture')
            invoke(cases()[0])
        invoke(cases()[0])
        self.assertEqual((outer.snapshot().recorded, inner.snapshot().recorded), (1, 1))

    def test_new_thread_does_not_inherit_observer(self):
        collector = metrics.SchedulerMetrics()
        observed = []
        with metrics.observe_scheduling(collector):
            thread = threading.Thread(target=lambda: observed.append(metrics.begin_selection('single')))
            thread.start()
            thread.join(timeout=5)
            self.assertFalse(thread.is_alive())
        self.assertEqual(observed, [None])
        self.assertEqual(collector.snapshot().recorded, 0)

    def test_clock_and_record_failure_do_not_change_choice(self):
        collector = metrics.SchedulerMetrics()
        case = cases()[0]
        expected = invoke(case)
        with metrics.observe_scheduling(collector):
            with patch.object(metrics.time, 'perf_counter_ns', side_effect=OSError('clock')):
                self.assertEqual(invoke(case), expected)
            with patch.object(collector, '_record', side_effect=OSError('observer')):
                self.assertEqual(invoke(case), expected)
        self.assertEqual(collector.snapshot().recorded, 0)

    def test_original_probe_exception_is_not_suppressed(self):
        collector = metrics.SchedulerMetrics()
        with metrics.observe_scheduling(collector), \
                patch.object(scheduler, 'load_nodes', return_value=[{'id': 'pi01'}]), \
                patch.object(scheduler, 'get_resources', side_effect=ValueError('original')):
            with self.assertRaisesRegex(ValueError, 'original'):
                scheduler.select_best_node()
        self.assertEqual(collector.snapshot().recorded, 0)

    def test_bounds_validation_and_diagnostic_truncation(self):
        for bad in (0, -1, 4097, True, 2.5, '2'):
            with self.assertRaises(ValueError):
                metrics.SchedulerMetrics(max_decisions=bad)
        with self.assertRaises(TypeError), metrics.observe_scheduling(object()):
            pass
        collector = metrics.SchedulerMetrics()
        with metrics.observe_scheduling(collector):
            measurement = metrics.begin_selection('ddp')
            measurement.finish([{'id': 'node-' + str(i)} for i in range(300)])
        decision = collector.snapshot().decisions[0]
        self.assertEqual((decision.selected_count, len(decision.selected_nodes)), (300, 256))
        self.assertTrue(decision.document()['selected_nodes_truncated'])

    def test_existing_dispatch_keeps_uncertain_reservation_with_metrics(self):
        import dispatcher
        import job_store as store
        import requests
        with tempfile.TemporaryDirectory() as temporary, \
                patch.object(store, 'DB_FILE', str(Path(temporary) / 'jobs.db')):
            store.initialize_database()
            store.add_job('uncertain-metrics', 1)
            collector = metrics.SchedulerMetrics()
            nodes = [{'id': 'pi01', 'ip': '192.0.2.1', 'port': 5000}]
            with metrics.observe_scheduling(collector), redirect_stdout(io.StringIO()), \
                    patch.object(scheduler, 'load_nodes', return_value=nodes), \
                    patch.object(scheduler, 'get_resources', return_value=snapshot()), \
                    patch.object(dispatcher.ownership, 'settings', return_value={'enabled': False}), \
                    patch.object(dispatcher, 'get_excluded_nodes', return_value=set()), \
                    patch.object(dispatcher.requests, 'post', side_effect=requests.Timeout('fixture')):
                self.assertFalse(dispatcher.dispatch_one_job(store.get_job('uncertain-metrics')))
            self.assertEqual(store.get_job('uncertain-metrics')['status'], 'SCHEDULED')
            self.assertEqual([r['node_id'] for r in store.get_reservations()], ['pi01'])
            self.assertEqual(collector.snapshot().decisions[0].selected_nodes, ('pi01',))
            dispatcher.unhealthy_nodes.clear()


if __name__ == '__main__':
    unittest.main(verbosity=2)
