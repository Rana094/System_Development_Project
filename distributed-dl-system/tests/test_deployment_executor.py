"""Disposable host installs only: no live DB, SSH, sudo or systemd mutation."""
from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from hybrid_storage import deployment_executor as deploy


class ExecutorTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name)
        self.app = self.base / 'app'
        self.app.mkdir()
        (self.app / 'controller').mkdir()
        (self.app / 'config').mkdir()
        (self.app / 'controller/dispatcher.py').write_bytes(b'# old source\n')
        os.chmod(self.app / 'controller/dispatcher.py', 0o640)
        (self.app / 'config/storage.json').write_text('{}\n')
        with sqlite3.connect(self.app / 'controller/jobs.db') as db:
            db.execute('CREATE TABLE sentinel (value TEXT)')
            db.execute("INSERT INTO sentinel VALUES ('keep this job data')")
        (self.app / 'checkpoint.bin').write_bytes(b'authoritative checkpoint')
        self.unit = self.base / 'unit.service'
        self.unit.write_text('[Service]\nExecStart=/usr/bin/python3 original.py\n')
        self.props = dict(Id='dl-dispatcher.service', ActiveState='active', SubState='running',
                          MainPID='999', ControlGroup='', FragmentPath=str(self.unit), DropInPaths='',
                          UnitFileState='enabled', WorkingDirectory=str(self.app),
                          ExecStart='original-exact-argv', User='', Group='')
        self.directory = self.base / 'payload'
        self.directory.mkdir(mode=0o700)
        self.raw = {'controller/dispatcher.py': b'# new source\n',
                    'hybrid_storage/new_module.py': b'VALUE = 7\n'}
        self.build()

    def services(self, unit, role):
        return dict(self.props, Id=unit)

    def stopped(self, unit, role):
        return dict(self.services(unit, role), ActiveState='inactive', SubState='dead', MainPID='0')

    def build(self):
        for name, raw in self.raw.items():
            target = self.directory / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(raw)
        request = dict(host='controller', application_root=str(self.app), uid=os.getuid(), gid=os.getgid(), files=list(self.raw))
        baseline = deploy.inspect(request, services=self.services)
        self.plan = dict(schema_version=1, kind='reviewed-host-source-install', host='controller', role='controller',
                         runtime_profile='legacy-off-resource', hybrid_activation=False,
                         application_root=str(self.app), snapshot_id='snap-20261110T000000Z-fixture',
                         files={n: dict(bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest()) for n, raw in self.raw.items()},
                         baseline=baseline, unit_changes=[], config_changes=[], database_restore=False,
                         required_gate_ids=['FIXTURE_OFFLINE'])

    def receipt(self, action='install'):
        now = time.time_ns()
        return dict(schema_version=1, host='controller', action=action,
                    plan_sha256=deploy.digest(deploy.encoded(self.plan)), operator_approval=True,
                    all_three_workers=['pi01', 'pi02', 'pi03'], local_private_data_only=True,
                    issued_at_ns=now, expires_at_ns=now + 120 * 10**9,
                    maintenance_and_descendants_quiescent=True,
                    resolved_gate_evidence={'FIXTURE_OFFLINE': 'a' * 64})

    def install(self):
        return deploy.install(self.plan, self.directory, self.receipt(), services=self.stopped)

    def rollback(self):
        return deploy.rollback(self.plan, self.receipt('rollback'), services=self.stopped)

    def tree(self, path):
        return {str(p.relative_to(path)): (p.read_bytes(), p.stat().st_mode, p.stat().st_mtime_ns)
                for p in path.rglob('*') if p.is_file()}

    def test_dry_run_has_no_db_open_commands_network_or_mutation(self):
        before = self.tree(self.base)
        with patch('sqlite3.connect', side_effect=AssertionError('No DB open')), \
                patch('subprocess.run', side_effect=AssertionError('No commands')), \
                patch('socket.socket', side_effect=AssertionError('No network')):
            result = deploy.dry_run(self.plan, self.directory)
        self.assertFalse(result['mutations'])
        self.assertEqual(self.tree(self.base), before)

    def test_install_retry_rollback_retry_preserve_all_data_and_metadata(self):
        original = deploy.record(self.app / 'controller/dispatcher.py')
        data = (self.app / 'controller/jobs.db').read_bytes()
        with patch('subprocess.run', side_effect=AssertionError('No service commands')):
            result = self.install()
            before = self.tree(Path(result['snapshot']))
            self.assertEqual(self.install()['status'], 'ALREADY_INSTALLED')
            self.assertEqual(before, self.tree(Path(result['snapshot'])))
            self.assertEqual(self.rollback()['status'], 'ROLLED_BACK_SERVICES_STOPPED')
            self.assertEqual(self.rollback()['status'], 'ALREADY_ROLLED_BACK')
        self.assertEqual(deploy.record(self.app / 'controller/dispatcher.py'), original)
        self.assertEqual((self.app / 'controller/jobs.db').read_bytes(), data)
        self.assertEqual((self.app / 'checkpoint.bin').read_bytes(), b'authoritative checkpoint')
        self.assertFalse((self.app / 'hybrid_storage/new_module.py').exists())
        self.assertEqual((Path(result['snapshot']) / 'quarantine/hybrid_storage/new_module.py').read_bytes(), self.raw['hybrid_storage/new_module.py'])
        self.assertEqual(self.unit.read_text(), '[Service]\nExecStart=/usr/bin/python3 original.py\n')
        with self.assertRaisesRegex(ValueError, 'cannot be reapplied'):
            self.install()

    def test_new_source_directories_are_readable_to_service_and_cache_is_untouched(self):
        self.install()
        folder = self.app / 'hybrid_storage'
        self.assertEqual(folder.stat().st_mode & 0o777, 0o755)
        self.assertEqual(folder.stat().st_uid, os.getuid())
        self.assertEqual((folder / 'new_module.py').stat().st_mode & 0o777, 0o644)

    def test_every_approval_field_is_required_before_any_writes(self):
        for key in self.receipt():
            receipt = self.receipt()
            del receipt[key]
            with self.subTest(key=key), self.assertRaises((PermissionError, KeyError)):
                deploy.install(self.plan, self.directory, receipt, services=self.stopped)
            self.assertFalse((self.app / '.hybrid-deploy').exists())

    def test_expired_wrong_action_host_plan_subset_workers_and_internet_refused(self):
        changes = [dict(issued_at_ns=1), dict(expires_at_ns=1), dict(action='rollback'),
                   dict(host='pi02'), dict(plan_sha256='0' * 64),
                   dict(all_three_workers=['pi02', 'pi03']), dict(local_private_data_only=False),
                   dict(resolved_gate_evidence={}), dict(operator_approval=False)]
        for change in changes:
            receipt = self.receipt(); receipt.update(change)
            with self.subTest(change=change), self.assertRaises(PermissionError):
                deploy.install(self.plan, self.directory, receipt, services=self.stopped)
        self.assertFalse((self.app / '.hybrid-deploy').exists())

    def test_running_service_or_cgroup_descendants_refused_before_source_change(self):
        with self.assertRaisesRegex(ValueError, 'already be stopped'):
            deploy.install(self.plan, self.directory, self.receipt(), services=self.services)
        def populated(unit, role):
            return dict(self.stopped(unit, role), ControlGroup='/')
        # Current root cgroup contains processes; no processes/signals are changed.
        with self.assertRaisesRegex(ValueError, 'not quiescent'):
            deploy.install(self.plan, self.directory, self.receipt(), services=populated)
        self.assertFalse((self.app / '.hybrid-deploy').exists())

    def test_hostname_unit_execstart_storage_and_owner_drift_fail_closed(self):
        original = self.unit.read_bytes()
        self.unit.write_bytes(original + b'# changed\n')
        with self.assertRaisesRegex(ValueError, 'Unit bytes'):
            self.install()
        self.unit.write_bytes(original)
        (self.app / 'config/storage.json').write_text('{"storage_backend":"nfs"}')
        with self.assertRaisesRegex(ValueError, 'configuration drift'):
            self.install()
        (self.app / 'config/storage.json').write_text('{}\n')
        with patch('socket.gethostname', return_value='wrong-host'), self.assertRaisesRegex(ValueError, 'identity changed'):
            self.install()
        def drift(unit, role):
            return dict(self.stopped(unit, role), ExecStart='changed')
        with self.assertRaisesRegex(ValueError, 'ExecStart'):
            deploy.install(self.plan, self.directory, self.receipt(), services=drift)

    def test_path_traversal_suffix_role_escape_data_and_config_paths_rejected(self):
        for name in ('../escape.py', '/absolute.py', 'nested/controller/evil.py',
                     'storage/jobs/a.py', 'config/storage.json', 'agent/../../jobs.db'):
            with self.subTest(name=name):
                plan = deepcopy(self.plan)
                plan['files'] = {name: next(iter(plan['files'].values()))}
                plan['baseline']['files'] = {name: None}
                with self.assertRaises(ValueError):
                    deploy.validate_plan(plan)

    def test_symlink_and_hardlink_payload_and_destination_rejected(self):
        target = self.directory / 'controller/dispatcher.py'
        raw = target.read_bytes(); target.unlink(); target.symlink_to(self.app / 'controller/dispatcher.py')
        with self.assertRaises(OSError): self.install()
        target.unlink(); target.write_bytes(raw)
        duplicate = self.base / 'hardlink'
        os.link(target, duplicate)
        with self.assertRaises(ValueError): self.install()
        duplicate.unlink()  # disposable fixture link only
        installed = self.app / 'controller/dispatcher.py'
        installed.unlink(); installed.symlink_to(self.unit)
        with self.assertRaises(OSError): self.install()

    def test_disk_full_refusal_occurs_before_snapshot_or_source_write(self):
        class Empty:
            f_bavail = 0
            f_frsize = 4096
        with patch('os.fstatvfs', return_value=Empty()), self.assertRaisesRegex(OSError, 'budget'):
            self.install()
        self.assertEqual((self.app / 'controller/dispatcher.py').read_bytes(), b'# old source\n')
        self.assertFalse(any((self.app / '.hybrid-deploy').rglob('SNAPSHOT_COMPLETE.json')))

    def test_interrupted_install_resumes_without_refreshing_original_backups(self):
        publish = deploy.publish
        count = 0
        def interrupted(*args):
            nonlocal count
            count += 1
            if count == 2: raise OSError('simulated power loss')
            return publish(*args)
        with patch.object(deploy, 'publish', side_effect=interrupted), self.assertRaises(OSError):
            self.install()
        tx = next((self.app / '.hybrid-deploy/snapshots').iterdir())
        before = self.tree(tx / 'before')
        self.assertEqual(self.install()['status'], 'INSTALLED_SERVICES_STOPPED')
        self.assertEqual(self.tree(tx / 'before'), before)
        self.rollback()
        self.assertEqual((self.app / 'controller/dispatcher.py').read_bytes(), b'# old source\n')

    def test_partial_snapshot_is_retained_and_never_used_for_install(self):
        write = deploy.write_new
        def interrupted(path, raw, *args):
            if path.name == 'SNAPSHOT_COMPLETE.json': raise OSError('snapshot interrupted')
            return write(path, raw, *args)
        with patch.object(deploy, 'write_new', side_effect=interrupted), self.assertRaises(OSError):
            self.install()
        with self.assertRaises(FileNotFoundError): self.install()
        self.assertEqual((self.app / 'controller/dispatcher.py').read_bytes(), b'# old source\n')

    def test_external_edit_prevents_all_rollback_writes(self):
        result = self.install()
        target = self.app / 'hybrid_storage/new_module.py'
        target.write_bytes(b'# external edit\n')
        current = (self.app / 'controller/dispatcher.py').read_bytes()
        with self.assertRaisesRegex(ValueError, 'External edit'):
            self.rollback()
        self.assertEqual((self.app / 'controller/dispatcher.py').read_bytes(), current)
        self.assertFalse((Path(result['snapshot']) / 'ROLLED_BACK.json').exists())

    def test_changed_backup_never_replaces_current_source(self):
        result = self.install()
        (Path(result['snapshot']) / 'before/controller/dispatcher.py').write_bytes(b'bad backup')
        with self.assertRaisesRegex(ValueError, 'Backup changed'):
            self.rollback()
        self.assertEqual((self.app / 'controller/dispatcher.py').read_bytes(), b'# new source\n')

    def test_duplicate_json_nonfinite_and_profile_activation_are_rejected(self):
        for raw in (b'{"a":1,"a":2}', b'{"a":NaN}'):
            with self.assertRaises(ValueError): deploy.decode(raw)
        for change in (dict(hybrid_activation=True), dict(unit_changes=['new']),
                       dict(config_changes=['nfs']), dict(database_restore=True)):
            plan = deepcopy(self.plan); plan.update(change)
            with self.assertRaises(ValueError): deploy.validate_plan(plan)

    def test_exclusive_host_lock_prevents_parallel_installer(self):
        with deploy.lock(self.plan), self.assertRaises(BlockingIOError):
            self.install()
        self.assertEqual((self.app / 'controller/dispatcher.py').read_bytes(), b'# old source\n')

    def test_payload_corruption_never_changes_target(self):
        (self.directory / 'controller/dispatcher.py').write_bytes(b'corrupt')
        with self.assertRaisesRegex(ValueError, 'Payload content'):
            self.install()
        self.assertFalse((self.app / '.hybrid-deploy').exists())

    def test_sudo_controller_is_refused_before_creating_snapshot(self):
        if os.geteuid() == 0:
            self.skipTest('Non-root fixture required')
        with patch('os.geteuid', return_value=0), self.assertRaisesRegex(PermissionError, 'never sudo'):
            self.install()
        self.assertFalse((self.app / '.hybrid-deploy').exists())

    def test_rollback_can_restore_older_snapshot_with_fresh_approval(self):
        self.install()
        later = time.time_ns() + 25 * 3600 * 10**9
        with patch('time.time_ns', return_value=later):
            self.assertEqual(self.rollback()['status'], 'ROLLED_BACK_SERVICES_STOPPED')

    def test_systemd_exec_identity_ignores_runtime_timestamps_but_checks_argv(self):
        start = '{ path=/usr/bin/python3 ; argv[]=/usr/bin/python3 app.py ; ignore_errors=no ; start_time=[yesterday] ; pid=42 ; status=0/0 }'
        stopped = start.replace('[yesterday]', '[n/a]').replace('pid=42', 'pid=0')
        self.props['ExecStart'] = start
        self.build()
        def after_stop(unit, role):
            return dict(self.stopped(unit, role), ExecStart=stopped)
        self.assertEqual(deploy.install(self.plan, self.directory, self.receipt(), services=after_stop)['status'], 'INSTALLED_SERVICES_STOPPED')
        def changed_argv(unit, role):
            return dict(after_stop(unit, role), ExecStart=stopped.replace('app.py', 'evil.py'))
        with self.assertRaisesRegex(ValueError, 'ExecStart'):
            deploy.rollback(self.plan, self.receipt('rollback'), services=changed_argv)


if __name__ == '__main__': unittest.main(verbosity=2)
