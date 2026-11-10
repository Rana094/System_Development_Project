"""13.4 provenance, measured units, missing data and offline report boundaries."""
import csv
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tools/benchmarks'))
from evaluation import report_records, reporting, runner


class ReportTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)

    def fixture(self, name='input', changes=None, count=1):
        root = self.base / name; root.mkdir()
        def save(path, value):
            path.parent.mkdir(parents=True, exist_ok=True); path.write_text(json.dumps(value))
        source = b'print("known source")\n'; (root / 'source').mkdir(); (root / 'source/train.py').write_bytes(source)
        meta = dict(schema_version=2, experiment_id=name, scope='local_component_rehearsal', sources={'train.py': hashlib.sha256(source).hexdigest()}, local_host={'hostname': 'fixture'})
        save(root / 'experiment.json', meta); save(root / 'completion.json', dict(meta, status='COMPLETED'))
        rows = []
        for i in range(count):
            runtime = dict(status='COMPLETED'); summary = dict(training_steps=2, timing=dict(training_loop_seconds=0.25))
            row = dict(schema_version=2, experiment_id=name, scope=meta['scope'], trial_id=f'trial-{i+1:04}', status='COMPLETED', exit_code=0,
                workload='regression', world_size=1, rows=12, dataset_id='dataset-one', dataset_version=1, dataset_sha256='a'*64, workload_sha256='b'*64,
                config='C', repeat_phase='repeat', application_cache_state='warm_repeat', total_ns=1000000000+i, staging_ns=100000000,
                cache_hit_bytes=100, transferred_bytes=0, ranks=[dict(rank=0, runtime=runtime, summary=summary)])
            if changes: row.update(changes)
            save(root / row['trial_id'] / 'result.json', row)
            save(root / row['trial_id'] / 'ranks/rank-0/runtime.json', runtime)
            save(root / row['trial_id'] / 'ranks/rank-0/output/summary.json', summary)
            rows.append(row)
        self.rewrite(root, rows)
        return root

    def rewrite(self, root, rows):
        (root / 'trials.jsonl').write_text(''.join(json.dumps(r)+'\n' for r in rows))
        fields = ['experiment_id','trial_id','scope','status','exit_code','world_size','config','repeat_phase','total_ns','staging_ns','cache_hit_bytes','transferred_bytes']
        with (root / 'trials.csv').open('w') as stream:
            writer=csv.DictWriter(stream,fieldnames=fields);writer.writeheader()
            for row in rows: writer.writerow({key:row[key] for key in fields})

    def test_measured_units_max_training_time_and_cache_ratio_are_exact(self):
        d = report_records.load(self.fixture()); row = reporting.normalize(d, d['records'][0])
        self.assertEqual(row['metric']['completion_seconds'], 1)
        self.assertEqual(row['metric']['staging_seconds'], 0.1)
        self.assertEqual(row['metric']['training_seconds'], 0.25)
        self.assertEqual(row['metric']['cache_hit_ratio'], 1)
        self.assertEqual(row['metric']['copied_payload_bytes'], 0)
        for key in reporting.MISSING: self.assertIsNone(row['metric'][key])

    def test_direct_input_unknown_bytes_stay_null_and_zero_staging_is_explicit(self):
        d = report_records.load(self.fixture(changes=dict(config='A',cache_hit_bytes=None,transferred_bytes=None,staging_ns=0)))
        row = reporting.normalize(d,d['records'][0])
        self.assertIsNone(row['metric']['cache_hit_ratio']);self.assertIsNone(row['metric']['nfs_wire_bytes'])
        self.assertIsNone(row['metric']['copied_payload_bytes']);self.assertEqual(row['metric']['staging_seconds'],0)

    def test_duplicate_ids_csv_or_leaf_mismatch_never_produces_report(self):
        for case in ('duplicate','csv','leaf'):
            root=self.fixture(name=case);row=json.loads((root/'trials.jsonl').read_text())
            if case=='duplicate':self.rewrite(root,[row,row])
            elif case=='csv':(root/'trials.csv').write_text((root/'trials.csv').read_text().replace('1000000000','1000000001'))
            else:(root/'trial-0001/result.json').write_text('{}')
            with self.subTest(case=case),self.assertRaises(ValueError):reporting.collect([root])

    def test_changed_rank_evidence_and_frozen_source_are_rejected(self):
        for case in ('rank','source'):
            root=self.fixture(name=case)
            if case=='rank':(root/'trial-0001/ranks/rank-0/output/summary.json').write_text('{}')
            else:(root/'source/train.py').write_bytes(b'changed')
            with self.subTest(case=case),self.assertRaises(ValueError):reporting.collect([root])

    def test_traversal_symlink_fifo_and_overlapping_inputs_fail_closed(self):
        root=self.fixture(); original=(root/'experiment.json').read_text()
        meta=json.loads(original);meta['sources']={'../outside.py':'a'*64};(root/'experiment.json').write_text(json.dumps(meta))
        with self.assertRaises(ValueError):reporting.collect([root])
        (root/'experiment.json').write_text(original)
        source=root/'source/train.py';source.unlink();source.symlink_to('/etc/passwd')
        with self.assertRaises(OSError):reporting.collect([root])
        source.unlink();os.mkfifo(source)
        with self.assertRaises(ValueError):reporting.collect([root])
        with self.assertRaises(ValueError):reporting.collect([root,root])

    def test_strict_json_and_numbers_reject_boolean_negative_nonfinite_and_huge(self):
        for raw in ('{"a":1,"a":2}','{"a":NaN}','{"a":1e999}'):
            with self.assertRaises(ValueError):report_records.strict_json(raw)
        for value in (True,-1,float('inf'),2**10000):
            with self.assertRaises(ValueError):report_records.number(value)
        self.assertEqual(report_records.number(0),0)

    def test_row_schema_must_match_metadata_before_normalizing_timing_units(self):
        root=self.fixture(changes=dict(schema_version=1))
        with self.assertRaises(ValueError):reporting.collect([root])

    def test_cohort_keys_do_not_pool_versions_scopes_sizes_experiments_or_failed_trials(self):
        d=report_records.load(self.fixture(count=3));rows=[reporting.normalize(d,r) for r in d['records']]
        groups=reporting.summary(rows);self.assertEqual(groups[0]['successful'],3)
        self.assertEqual(groups[0]['metrics']['completion_seconds']['observed'],3)
        changed=[]
        for key in ('experiment_id','dataset_sha256','source_set_sha256','scope','world_size','rows','repeat_phase'):
            row=dict(rows[0]);row[key]=str(row[key])+'-different';changed.append(row)
        self.assertEqual(len(reporting.summary(rows+changed)),8)
        failed=dict(rows[0],status='FAILED')
        group=reporting.summary(rows+[failed])[0];self.assertEqual((group['attempted'],group['successful'],group['failed']),(4,3,1))
        self.assertEqual(group['metrics']['completion_seconds']['observed'],3)
        self.assertIsNone(reporting.summary([rows[0]])[0]['metrics']['completion_seconds']['stdev'])

    def test_disk_snapshot_counts_hardlinks_once_and_skips_external_links(self):
        root=self.base/'disk';root.mkdir();(root/'data').write_bytes(b'1234567')
        os.link(root/'data',root/'alias');(root/'outside').symlink_to('/etc')
        observed=report_records.disk_snapshot(root)
        self.assertEqual(observed['logical_unique_file_bytes'],7);self.assertEqual(observed['path_entry_bytes'],14)
        self.assertEqual(observed['unique_regular_files'],1);self.assertEqual(observed['skipped_links_or_special'],1)

    def test_cli_readonly_and_writer_preserve_input_and_existing_report(self):
        root=self.fixture();before=(root/'trials.jsonl').read_bytes();out=self.base/'output'
        command=[sys.executable,'-B',str(ROOT/'tools/benchmarks/storage_evaluation_report.py'),'--inputs',str(root),'--output-root',str(out),'--report-id','report-test']
        p=subprocess.run(command,capture_output=True,text=True,timeout=15);self.assertEqual(p.returncode,0,p.stderr)
        self.assertFalse(out.exists());self.assertEqual(json.loads(p.stdout)['status'],'VALIDATED_READONLY')
        data=reporting.collect([root]);target=reporting.write(data,out,'report-test')
        self.assertTrue((target/'metrics.csv').exists());self.assertTrue((target/'files.json').exists())
        self.assertIn('No timestamped first training-step event',(target/'report.md').read_text())
        self.assertEqual((root/'trials.jsonl').read_bytes(),before)
        with self.assertRaises(FileExistsError):reporting.write(data,out,'report-test')
        with self.assertRaises(ValueError):reporting.write(data,root,'nested-report')

    def test_missing_plotting_dependency_refuses_before_creating_report(self):
        from evaluation import report_charts
        data=reporting.collect([self.fixture()]);out=self.base/'output'
        with patch.object(report_charts,'require_matplotlib',side_effect=ImportError('not installed')):
            with self.assertRaises(ImportError):reporting.write(data,out,'report-test',charts=True)
        self.assertFalse(out.exists())

    def test_fault_harness_time_is_not_imputed_as_recovery_time(self):
        experiment=dict(kind='fault',metadata=dict(experiment_id='fault',scope='local_disposable_failure_rehearsal',sources={'a':'a'*64},local_host={'hostname':'fixture'}))
        row=reporting.normalize(experiment,dict(case='agent_process_down',status='PASS',harness_elapsed_ns=1000000000,detection_ns=1000000,restart_ns=2000000,recovery_ns=None))
        self.assertEqual(row['metric']['harness_seconds'],1);self.assertEqual(row['metric']['detection_seconds'],0.001)
        self.assertEqual(row['metric']['agent_restart_seconds'],0.002);self.assertIsNone(row['metric']['recovery_seconds'])


if __name__=='__main__':unittest.main(verbosity=2)
