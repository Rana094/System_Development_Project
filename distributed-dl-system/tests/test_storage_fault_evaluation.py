"""13.3 runner boundaries, honest receipts and actual disposable cache repair."""
from dataclasses import replace
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tools/benchmarks'))
from evaluation import faults


class FaultRunnerTests(unittest.TestCase):
    def settings(self):
        return faults.Settings('fault-test', ('cache_corruption',), 30)

    def test_plan_allowlists_injections_and_defers_every_physical_disruption(self):
        document = faults.plan(self.settings())
        self.assertEqual(document['status'], 'PLAN_ONLY')
        self.assertEqual(document['scope'], 'local_disposable_failure_rehearsal')
        self.assertEqual(len(document['deferred']), 3)
        self.assertTrue(all(item['status'] == 'NOT_RUN' for item in document['deferred']))
        self.assertFalse(any('credential' in name or name.endswith('.db') for name in document['sources']))
        self.assertNotIn('physical_network_disconnect', faults.CASES)

    def test_invalid_case_commands_duplicate_ids_or_bounds_are_rejected(self):
        for changes in (dict(cases=()), dict(cases=('cache_corruption',) * 2), dict(cases=('arbitrary command',)),
                        dict(cases=([],)), dict(experiment_id='../escape'), dict(timeout_seconds=True), dict(timeout_seconds=301)):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                replace(self.settings(), **changes).validate()

    def test_cli_default_is_readonly_and_live_faults_cannot_be_selected(self):
        with tempfile.TemporaryDirectory() as temporary:
            command = [sys.executable, '-B', str(ROOT / 'tools/benchmarks/storage_fault_evaluation.py'), '--output-root', temporary,
                       '--experiment-id', 'readonly', '--cases', 'cache_corruption']
            result = subprocess.run(command, capture_output=True, text=True, timeout=15)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(json.loads(result.stdout)['status'], 'PLAN_ONLY')
            self.assertEqual(list(Path(temporary).iterdir()), [])
            result = subprocess.run(command + ['--cases', 'physical_network_disconnect'], capture_output=True, text=True, timeout=15)
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(list(Path(temporary).iterdir()), [])

    def test_child_environment_drops_control_credentials_and_runtime_roots(self):
        with patch.dict(os.environ, DL_WORKER_ROOT='/live', DL_STORAGE_ROOT='/live', DL_EXECUTION_LEASE_PATH='/live',
                        DL_RESUME_CHECKPOINT='/live', PYTHONPATH='/live', WORKER_SECRET='secret'):
            env = faults.child_environment()
        self.assertFalse(any(key in env for key in ('DL_WORKER_ROOT', 'DL_STORAGE_ROOT', 'DL_EXECUTION_LEASE_PATH', 'DL_RESUME_CHECKPOINT', 'PYTHONPATH', 'WORKER_SECRET')))
        self.assertEqual(env['GLOO_SOCKET_IFNAME'], 'lo')

    def test_zero_exit_without_valid_receipt_cannot_pass(self):
        with tempfile.TemporaryDirectory() as temporary, patch.object(faults, 'execute', return_value=(0, False)):
            folder, result = faults.run(self.settings(), Path(temporary))
            self.assertEqual(result['status'], 'FAILED')
            row = json.loads((folder / 'faults.jsonl').read_text())
            self.assertEqual(row['status'], 'FAILED')
            self.assertTrue(all(row[k] is None for k in faults.METRICS))
            self.assertGreater(row['harness_elapsed_ns'], 0)

    def test_wrong_case_or_noninteger_phase_receipt_cannot_pass(self):
        for wrong in ('case', 'duration'):
            def execute(command, folder, *args):
                receipt = dict(case='wrong' if wrong == 'case' else 'cache_corruption', status='PASS', metric_scope='fixture')
                if wrong == 'duration': receipt['detection_ns'] = True
                (folder / 'measurements.json').write_text(json.dumps(receipt))
                return 0, False
            with self.subTest(wrong=wrong), tempfile.TemporaryDirectory() as temporary, patch.object(faults, 'execute', side_effect=execute):
                _, result = faults.run(self.settings(), Path(temporary))
                self.assertEqual(result['status'], 'FAILED')

    def test_timeout_kills_owned_descendant_group_and_preserves_unrelated_child(self):
        import psutil
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            foreign = subprocess.Popen([sys.executable, '-c', 'import time;time.sleep(30)'], start_new_session=True)
            try:
                program = "import subprocess,sys,time;from pathlib import Path;p=subprocess.Popen([sys.executable,'-c','import time;time.sleep(30)']);Path('child.pid').write_text(str(p.pid));time.sleep(30)"
                code, timeout = faults.execute([sys.executable, '-c', program], root, faults.child_environment(), 1)
                self.assertTrue(timeout); self.assertIsNone(code)
                pid = int((root / 'child.pid').read_text())
                deadline = time.monotonic() + 3
                while psutil.pid_exists(pid) and psutil.Process(pid).status() != psutil.STATUS_ZOMBIE and time.monotonic() < deadline:
                    time.sleep(0.01)
                self.assertTrue(not psutil.pid_exists(pid) or psutil.Process(pid).status() == psutil.STATUS_ZOMBIE)
                self.assertIsNone(foreign.poll())
            finally:
                foreign.kill(); foreign.wait(timeout=5)

    def test_actual_cache_corruption_repair_retains_raw_records_and_old_experiment(self):
        with tempfile.TemporaryDirectory() as temporary:
            folder, result = faults.run(self.settings(), Path(temporary))
            self.assertEqual(result['status'], 'LOCAL_PASS_LIVE_DEFERRED', (folder / 'cache_corruption/case.log').read_text())
            row = json.loads((folder / 'faults.jsonl').read_text())
            self.assertEqual(row['status'], 'PASS', (folder / 'cache_corruption/case.log').read_text())
            self.assertGreater(row['detection_ns'], 0); self.assertGreater(row['staging_ns'], 0)
            self.assertIsNone(row['restart_ns']); self.assertIsNone(row['recovery_ns'])
            self.assertTrue(row['details']['quarantine_preserved'])
            previous = (folder / 'faults.jsonl').read_bytes()
            with self.assertRaises(FileExistsError): faults.run(self.settings(), Path(temporary))
            self.assertEqual((folder / 'faults.jsonl').read_bytes(), previous)

    def test_existing_checkpoint_fixture_runs_from_private_copy_without_invented_phase_times(self):
        with tempfile.TemporaryDirectory() as temporary:
            settings = replace(self.settings(), cases=('partial_checkpoint',))
            folder, result = faults.run(settings, Path(temporary))
            self.assertEqual(result['status'], 'LOCAL_PASS_LIVE_DEFERRED', (folder / 'partial_checkpoint/case.log').read_text())
            row = json.loads((folder / 'faults.jsonl').read_text())
            self.assertEqual(row['details']['tests_run'], 1)
            self.assertTrue(all(row[key] is None for key in faults.METRICS))


if __name__ == '__main__':
    unittest.main(verbosity=2)
