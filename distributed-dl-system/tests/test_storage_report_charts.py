"""Optional controller plotting integration; no worker or training dependencies."""
import importlib.util
import json
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tools/benchmarks'))
from evaluation import reporting
import test_storage_evaluation_report as fixtures


@unittest.skipUnless(importlib.util.find_spec('matplotlib'), 'Optional controller reporting environment required')
class ChartTests(unittest.TestCase):
    def setUp(self):
        self.f = fixtures.ReportTests(methodName='runTest'); self.f.setUp(); self.addCleanup(self.f.doCleanups)

    def test_chart_points_are_exact_recorded_values_and_unknown_bytes_are_not_zero(self):
        a = self.f.fixture('direct', changes=dict(config='A', cache_hit_bytes=None, transferred_bytes=None, staging_ns=0))
        c = self.f.fixture('cached', count=3)
        data = reporting.collect([a, c]); out = reporting.write(data, self.f.base/'output', 'chart-report', charts=True)
        chart = json.loads((out/'chart-data.json').read_text()); metrics = json.loads((out/'metrics.json').read_text())['records']
        known = {(r['experiment_id'], r['record_id']):r for r in metrics}
        self.assertEqual(len(chart['figures']), 5)
        for point in chart['points']:
            row = known[point['experiment_id'], point['record_id']]
            self.assertIsNotNone(row['metric'][point['metric']]); self.assertEqual(point['value'], row['metric'][point['metric']])
        self.assertFalse(any(p['experiment_id']=='direct' and p['metric'] in ('copied_payload_bytes','cache_hit_ratio') for p in chart['points']))
        from PIL import Image
        for name in chart['figures']:
            self.assertTrue((out/(name+'.svg')).read_text().lstrip().startswith('<?xml'))
            with Image.open(out/(name+'.png')) as picture:
                self.assertGreater(picture.width, 800); self.assertGreater(picture.height, 200); picture.verify()

    def test_fault_chart_omits_missing_recovery_and_never_plots_whole_harness_as_restart(self):
        from evaluation import report_charts
        target=self.f.base/'fault-chart';target.mkdir()
        data=[dict(kind='fault',metadata=dict(experiment_id='fault',schema_version=1),records=[{}])]
        rows=[dict(experiment_id='fault',record_id='agent_process_down',status='PASS',metric=dict(detection_seconds=0.001,staging_seconds=None,agent_restart_seconds=0.2,harness_seconds=5,recovery_seconds=None))]
        report_charts.render(target,data,rows,[])
        chart=json.loads((target/'chart-data.json').read_text())
        self.assertEqual({p['metric'] for p in chart['points']},{'detection_seconds','agent_restart_seconds'})
        self.assertEqual(len(chart['points']),2)


if __name__=='__main__':unittest.main(verbosity=2)
