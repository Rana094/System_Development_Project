"""All-three preparation from synthetic inventories; CLI safety and source drift."""
from copy import deepcopy
import hashlib
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
sys.path.insert(0, str(ROOT))
from hybrid_storage import deployment_toolkit as kit
from hybrid_storage import deployment_executor as host
from hybrid_storage import deployment_review as source
from hybrid_storage import delivery_plan


class ToolkitTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name)
        self.root = self.base / 'source'
        self.root.mkdir()
        names = set().union(*(source.catalog(role) for role in source.ROLES))
        names.add(delivery_plan.AUDIT)
        names.add('config/nodes.json')
        for name in names:
            target = self.root / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes((ROOT / name).read_bytes())
        audit_path = self.root / delivery_plan.AUDIT
        audit = json.loads(audit_path.read_text())
        audit['repository']['app_root'] = str(self.root)
        audit_path.write_text(json.dumps(audit))
        self.requests = kit.requests(root=self.root)
        self.observations = {}
        for name, request in self.requests.items():
            role = 'controller' if name == 'controller' else 'worker'
            units = ['dl-student-portal.service', 'dl-dispatcher.service'] if role == 'controller' else ['dl-node-agent.service']
            self.observations[name] = dict(schema_version=1, host=name, role=role,
                application_root=request['application_root'], hostname=request['hostname'],
                uid=request['uid'], gid=request['gid'], machine_id_sha256='a'*64,
                inspected_at_ns=time.time_ns(), python=[3, 11, 16], free_bytes=10*1024**3,
                services={u: {'ActiveState': 'active', 'ExecStart': '{ path=/usr/bin/python3 ; argv[]=/usr/bin/python3 fake.py ; ignore_errors=no ; start_time=[n/a] ; pid=1 ; status=0/0 }'} for u in units}, unit_files={},
                files={f: None for f in request['files']}, storage_config=None,
                database_opened=False, changes=False)
        self.output = self.base / 'review'

    def prepare(self):
        return kit.prepare(self.output, host.encoded(self.observations), root=self.root)

    def tree(self):
        return {str(p.relative_to(self.output)): (p.read_bytes(), p.stat().st_mode, p.stat().st_mtime_ns)
                for p in self.output.rglob('*') if p.is_file()}

    def test_all_three_concrete_source_unit_config_operations_and_no_live_effects(self):
        with patch('sqlite3.connect', side_effect=AssertionError('DB forbidden')), \
                patch('subprocess.run', side_effect=AssertionError('Command forbidden')), \
                patch('socket.socket', side_effect=AssertionError('Network forbidden')):
            result = self.prepare()
            self.assertEqual(kit.verify(self.output, root=self.root), result)
        self.assertTrue(result['executor_implemented'])
        self.assertFalse(result['apply_ready'])
        self.assertEqual(result['selected_workers'], ['pi01', 'pi02', 'pi03'])
        for name in ('controller', 'pi01', 'pi02', 'pi03'):
            plan = host.decode((self.output / name / 'host-plan.json').read_bytes())
            self.assertEqual(plan['unit_changes'], [])
            self.assertEqual(plan['config_changes'], [])
            self.assertFalse(plan['hybrid_activation'])
            self.assertEqual(plan['runtime_profile'], 'legacy-off-resource')
            self.assertIn('hybrid_storage/deployment_executor.py', plan['files'])
            self.assertNotIn('controller/jobs.db', plan['files'])
            self.assertFalse(json.loads((self.output / name / 'approval.example.json').read_text())['operator_approval'])
        self.assertEqual(json.loads((self.output/'pi03/host-plan.json').read_text())['baseline']['uid'], 1002)

    def test_repeat_is_byte_mode_mtime_idempotent(self):
        first = self.prepare()
        before = self.tree()
        self.assertEqual(self.prepare(), first)
        self.assertEqual(self.tree(), before)

    def test_missing_pi01_stale_inventory_wrong_identity_or_filename_set_refused(self):
        original = deepcopy(self.observations)
        changes = [lambda d: d.pop('pi01'), lambda d: d['pi01'].update(inspected_at_ns=1),
                   lambda d: d['pi03'].update(uid=1000), lambda d: d['pi02'].update(hostname='pi01'),
                   lambda d: d['pi01']['files'].update({'../../escape': None})]
        for change in changes:
            self.observations = deepcopy(original)
            change(self.observations)
            with self.assertRaises(ValueError): self.prepare()
            self.assertFalse(self.output.exists())

    def test_drift_extra_symlink_payload_and_partial_output_are_not_repaired(self):
        self.prepare()
        path = self.output/'pi01/payload/agent/agent.py'
        old = path.read_bytes()
        path.write_bytes(old + b'\n')
        with self.assertRaises(ValueError): kit.verify(self.output, root=self.root)
        path.write_bytes(old)
        path.unlink(); path.symlink_to(self.root/'agent/agent.py')
        with self.assertRaisesRegex(ValueError, 'Symlink'): kit.verify(self.output, root=self.root)
        path.unlink(); path.write_bytes(old)
        (self.output/'extra').write_text('retain')
        with self.assertRaises(ValueError): self.prepare()
        self.assertEqual((self.output/'extra').read_text(), 'retain')

    def test_source_drift_and_new_inventory_require_new_review(self):
        self.prepare()
        before = self.tree()
        path = self.root/'agent/agent.py'
        path.write_bytes(path.read_bytes() + b'\n')
        with self.assertRaisesRegex(ValueError, 'source changed'): self.prepare()
        self.assertEqual(self.tree(), before)

    def test_protected_host_launcher_help_is_portable_and_does_not_open_db(self):
        self.prepare()
        path = self.output/'pi03/run.py'
        done = subprocess.run([sys.executable, '-I', '-B', str(path), '--help'],
                              capture_output=True, text=True, timeout=10)
        self.assertEqual(done.returncode, 0, done.stderr)
        self.assertIn('rollback', done.stdout)
        self.assertIn('migrate', done.stdout)

    def test_inspect_program_is_stdlib_standalone_with_no_import_side_effects(self):
        code = kit.inspect_program(self.requests['pi01'], root=self.root).decode()
        compile(code, '<portable-inspection>', 'exec')
        self.assertNotIn('sudo ', code.splitlines()[-1])
        self.assertIn('m.inspect(', code.splitlines()[-1])
        # Loading the portable definitions alone must not import a DB/job runtime.
        definitions = '\n'.join(code.splitlines()[:-1]) + '\n'
        definitions += "assert 'sqlite3' not in sys.modules and 'job_store' not in sys.modules\n"
        done = subprocess.run([sys.executable, '-I', '-B', '-c', definitions],
                              capture_output=True, text=True, timeout=10)
        self.assertEqual(done.returncode, 0, done.stderr)


if __name__ == '__main__': unittest.main(verbosity=2)
