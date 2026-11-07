"""Explicit single-snapshot enrollment; only copied controllers/disposable data."""

from concurrent.futures import ThreadPoolExecutor
import hashlib
import io
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import threading
import unittest
from unittest.mock import patch

import test_storage_registry as fixtures
from hybrid_storage.artifacts import ArtifactRegistrar, ArtifactMismatch
from hybrid_storage.dataset_manifests import DatasetEntry
from hybrid_storage.registry import StorageRegistry
from hybrid_storage.registry_schema import RegistryError
import input_storage


class LegacyTests(unittest.TestCase):
    # Reuse the reviewed real-controller fixture without inheriting/repeating its
    # 38 test methods. Its imports/initializers target a separate temporary copy.
    setUp = fixtures.RegistryTests.setUp
    connect = fixtures.RegistryTests.connect
    job = fixtures.RegistryTests.job
    registry = fixtures.RegistryTests.registry
    artifact = fixtures.RegistryTests.artifact
    source = fixtures.RegistryTests.source
    dataset = fixtures.RegistryTests.dataset

    def enroll(self, registry, identity=None, actor='alice', **changes):
        options = dict(managed_root=self.base / 'jobs', verify_workspace=fixtures.job_storage.verify_workspace)
        options.update(changes)
        return input_storage.register_legacy_inputs(registry, actor, identity or self.job_id, **options)

    def files(self, root):
        return {str(path.relative_to(root)): (path.stat().st_ino, path.stat().st_mode,
                                             hashlib.sha256(path.read_bytes()).hexdigest())
                for path in root.rglob('*') if path.is_file()}

    def test_active_legacy_job_is_reference_only_and_original_files_remain_loadable(self):
        registry = self.registry()
        identity, workspace, raw = self.job(backend='legacy')
        checkpoint = workspace / 'checkpoints' / 'fixture.bin'
        checkpoint.write_bytes(b'preserved checkpoint fixture')
        self.db.execute("UPDATE jobs SET status='RUNNING',retry_count=2,checkpoint_path=? WHERE job_id=?", (str(checkpoint), identity))
        self.db.commit()
        job = fixtures.store.get_job(identity)
        selection = fixtures.store.get_storage_binding(identity)
        before = self.files(workspace)
        binding = self.enroll(registry, identity)
        self.assertEqual(fixtures.store.get_job(identity), job)
        self.assertEqual(fixtures.store.get_storage_binding(identity), selection)
        self.assertEqual(self.files(workspace), before)
        self.assertEqual(binding.legacy_manifest_sha256, hashlib.sha256(raw).hexdigest())
        self.assertEqual(registry.get_job_inputs('alice', identity), binding)
        fixtures.job_storage.verify_workspace(workspace, fixtures.store.get_job(identity))
        fixtures.store.require_storage_execution(identity)

    def test_implicit_legacy_selection_is_not_backfilled_or_changed(self):
        registry = self.registry()
        identity, workspace, raw = self.job(backend='legacy')
        self.db.execute('DELETE FROM job_storage_bindings WHERE job_id=?', (identity,))
        self.db.commit()
        self.assertEqual(fixtures.store.get_storage_binding(identity)['origin'], 'implicit_legacy')
        self.enroll(registry, identity)
        self.assertIsNone(self.db.execute('SELECT 1 FROM job_storage_bindings WHERE job_id=?', (identity,)).fetchone())
        self.assertEqual(fixtures.store.get_storage_binding(identity)['origin'], 'implicit_legacy')

    def test_known_local_legacy_workload_runs_before_and_after_enrollment(self):
        registry = self.registry()
        (self.project / 'train.py').write_text(
            "import csv,json,os\nfrom pathlib import Path\n"
            "rows=list(csv.DictReader((Path(os.environ['DL_DATASET_PATH'])/'a/rows.csv').open()))\n"
            "weight=0.0\nfor epoch in range(2):\n"
            " for row in rows:\n"
            "  x,y=float(row['x']),float(row['y'])\n"
            "  weight-=0.1*(weight*x-y)*x\n"
            "print(json.dumps({'rows':len(rows),'weight':round(weight,6)},sort_keys=True))\n")
        identity, workspace, raw = self.job(backend='legacy')
        spec = json.loads(fixtures.store.get_job(identity)['job_config'])
        def run():
            return subprocess.run([sys.executable, *spec['entrypoint'][1:]], cwd=spec['project'],
                                  env=dict(os.environ, DL_DATASET_PATH=spec['dataset'], PYTHONDONTWRITEBYTECODE='1'),
                                  capture_output=True, text=True, timeout=10)
        before = run()
        self.enroll(registry, identity)
        after = run()
        self.assertEqual((before.returncode, after.returncode), (0, 0), (before.stderr, after.stderr))
        self.assertEqual(before.stdout, after.stdout)
        self.assertEqual(json.loads(after.stdout), {'rows': 1, 'weight': 0.38})
        fixtures.job_storage.verify_workspace(workspace, fixtures.store.get_job(identity))

    def test_held_nfs_snapshot_can_register_without_becoming_executable(self):
        registry = self.registry()
        before = fixtures.store.get_job(self.job_id)
        self.enroll(registry)
        self.assertEqual(fixtures.store.get_job(self.job_id), before)
        with self.assertRaises(ValueError): fixtures.store.require_storage_execution(self.job_id)
        self.assertEqual(before['status'], 'HELD')

    def test_exact_retry_reuses_one_dataset_sidecar_and_object_inodes(self):
        registry = self.registry()
        first = self.enroll(registry)
        dump = list(self.db.iterdump())
        objects = self.files(self.objects)
        second = self.enroll(registry)
        self.assertEqual(second, first)
        self.assertEqual(list(self.db.iterdump()), dump)
        self.assertEqual(self.files(self.objects), objects)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM storage_datasets').fetchone()[0], 1)

    def test_two_authorized_legacy_submissions_share_bytes_not_owner_access(self):
        registry = self.registry()
        alice, _, _ = self.job(backend='legacy')
        first = self.enroll(registry, alice)
        objects = self.files(self.objects)
        bob, _, _ = self.job(owner='bob', backend='legacy')
        second = self.enroll(registry, bob, actor='bob')
        self.assertEqual(first.source_sha256, second.source_sha256)
        self.assertNotEqual(first.dataset_id, second.dataset_id)
        self.assertEqual(self.files(self.objects), objects)
        with self.assertRaises(PermissionError): registry.get_job_inputs('bob', alice)
        with self.assertRaises(PermissionError): registry.get_dataset('alice', second.dataset_id, 1)

    def test_changed_bytes_create_new_committed_dataset_version_without_rerouting_job(self):
        registry = self.registry()
        binding = self.enroll(registry)
        first = registry.get_dataset('alice', binding.dataset_id, 1)
        changed = registry.register_artifact('alice', io.BytesIO(b'new rows\n'), expected_size=9)
        files = [entry for entry in first.entries if entry.type == 'file']
        entries = [DatasetEntry(entry.path, 'file', changed.sha256, changed.size_bytes) if entry == files[0] else entry
                   for entry in first.entries]
        second = registry.update_dataset('alice', binding.dataset_id, expected_head_sha256=first.sha256, entries=entries)
        self.assertEqual(second.version, 2)
        self.assertNotEqual(second.sha256, first.sha256)
        self.assertNotEqual(next(e.sha256 for e in second.entries if e.type == 'file'), files[0].sha256)
        self.assertEqual(registry.get_job_inputs('alice', self.job_id), binding)
        self.assertEqual(self.enroll(registry), binding)
        self.assertEqual(registry.get_dataset('alice', binding.dataset_id, 1), first)

    def test_concurrent_requests_commit_one_private_dataset_and_binding(self):
        self.registry()
        barrier = threading.Barrier(2, timeout=10)
        def register(_):
            db = self.connect()
            try:
                registry = StorageRegistry(db, self.publisher)
                original = registry.register_snapshot
                def wait(*args, **kwargs):
                    barrier.wait()
                    return original(*args, **kwargs)
                registry.register_snapshot = wait
                return self.enroll(registry)
            finally:
                db.close()
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(register, range(2)))
        self.assertEqual(results[0], results[1])
        for table in ('storage_datasets', 'storage_dataset_versions', 'storage_job_inputs'):
            self.assertEqual(self.db.execute('SELECT COUNT(*) FROM ' + table).fetchone()[0], 1)
        self.assertEqual(self.db.execute('PRAGMA foreign_key_check').fetchall(), [])

    def test_failure_during_final_transaction_rolls_back_manifests_dataset_and_sidecar(self):
        registry = self.registry()
        before = fixtures.store.get_job(self.job_id)
        def deny(action, table, *args):
            return sqlite3.SQLITE_DENY if action == sqlite3.SQLITE_INSERT and table == 'storage_job_inputs' else sqlite3.SQLITE_OK
        self.db.set_authorizer(deny)
        with self.assertRaises(sqlite3.DatabaseError): self.enroll(registry)
        self.db.set_authorizer(None)
        for table in ('storage_manifests', 'storage_datasets', 'storage_dataset_versions', 'storage_job_inputs'):
            self.assertEqual(self.db.execute('SELECT COUNT(*) FROM ' + table).fetchone()[0], 0)
        objects = self.files(self.objects)
        self.assertTrue(objects)
        self.enroll(registry)
        self.assertEqual(self.files(self.objects), objects)
        self.assertEqual(fixtures.store.get_job(self.job_id), before)

    def test_interrupted_file_publication_retries_without_partial_job_registration(self):
        registry = self.registry()
        original = registry.register_artifact
        calls = 0
        def interrupted(*args, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 2: raise OSError('injected publication failure')
            return original(*args, **kwargs)
        before = self.files(self.workspace)
        with patch.object(registry, 'register_artifact', side_effect=interrupted):
            with self.assertRaises(OSError): self.enroll(registry)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM storage_job_inputs').fetchone()[0], 0)
        self.assertEqual(self.files(self.workspace), before)
        self.assertEqual(list(self.scratch.iterdir()), [])
        self.enroll(registry)
        self.assertEqual(self.files(self.workspace), before)

    def test_unauthorized_owner_is_denied_before_workspace_verifier_or_publication(self):
        registry = self.registry()
        with patch.object(registry, 'register_artifact', side_effect=AssertionError('No publication')):
            with self.assertRaises(PermissionError): self.enroll(registry, actor='bob', verify_workspace=lambda *args: self.fail('No workspace reads'))
        self.assertEqual(list(self.objects.iterdir()), [])

    def test_received_or_unmanaged_jobs_are_not_implicitly_staged_or_assigned_owners(self):
        registry = self.registry()
        config = json.loads(fixtures.store.get_job(self.job_id)['job_config'])
        fixtures.store.add_job('not-staged', 1, job_type='custom', status='RECEIVED', owner='alice', job_config=config)
        with self.assertRaises(RegistryError): self.enroll(registry, 'not-staged')
        self.db.execute('UPDATE jobs SET owner=NULL WHERE job_id=?', (self.job_id,))
        self.db.commit()
        with self.assertRaises(PermissionError): self.enroll(registry)
        self.assertEqual(list(self.objects.iterdir()), [])

    def test_older_database_without_snapshot_columns_is_preserved_not_auto_upgraded(self):
        from hybrid_storage.registry_schema import migrate
        with sqlite3.connect(self.base / 'old-schema.db') as db:
            db.execute('PRAGMA foreign_keys=ON')
            db.execute("CREATE TABLE jobs(job_id TEXT PRIMARY KEY,status TEXT NOT NULL)")
            db.execute("INSERT INTO jobs VALUES('old','COMPLETED')")
            db.commit()
            migrate(db)
            registry = StorageRegistry(db, self.publisher)
            before = list(db.iterdump())
            with self.assertRaises(RegistryError): self.enroll(registry, 'old')
            self.assertEqual(list(db.iterdump()), before)

    def test_missing_legacy_file_fails_without_falling_back_to_original_upload(self):
        registry = self.registry()
        (self.workspace / 'source/train.py').unlink()
        self.assertTrue((self.project / 'train.py').exists())
        with self.assertRaises(ValueError): self.enroll(registry)
        self.assertEqual(list(self.objects.iterdir()), [])
        self.assertFalse((self.workspace / 'source/train.py').exists())

    def test_corrupt_existing_cas_object_is_not_overwritten_or_repaired(self):
        registry = self.registry()
        binding = self.enroll(registry)
        target = self.objects / ('sha256/' + binding.requirements_sha256[:2] + '/' + binding.requirements_sha256)
        target.chmod(0o600)
        target.write_bytes(b'corrupt')
        target.chmod(0o444)
        with self.assertRaises(ArtifactMismatch): self.enroll(registry)
        self.assertEqual(target.read_bytes(), b'corrupt')
        self.assertEqual(registry.get_job_inputs('alice', self.job_id), binding)

    def test_source_mutation_during_copy_fails_digest_and_never_commits_sidecar(self):
        registry = self.registry()
        original = registry.register_artifact
        first = json.loads(self.raw)['source']['inventory']['files'][0]['path']
        file = self.workspace / 'source' / first
        changed = False
        def mutate(*args, **kwargs):
            nonlocal changed
            if not changed:
                changed = True
                file.write_bytes(b'x' * file.stat().st_size)
            return original(*args, **kwargs)
        with patch.object(registry, 'register_artifact', side_effect=mutate):
            with self.assertRaises(ArtifactMismatch): self.enroll(registry)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM storage_job_inputs').fetchone()[0], 0)

    def test_authority_change_during_copy_prevents_final_registration(self):
        registry = self.registry()
        original = registry.register_artifact
        changed = False
        def mutate(*args, **kwargs):
            nonlocal changed
            result = original(*args, **kwargs)
            if not changed:
                changed = True
                self.db.execute("UPDATE jobs SET owner='bob' WHERE job_id=?", (self.job_id,))
                self.db.commit()
            return result
        with patch.object(registry, 'register_artifact', side_effect=mutate):
            with self.assertRaises(PermissionError): self.enroll(registry)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM storage_job_inputs').fetchone()[0], 0)

    def test_snapshot_change_after_copy_is_detected_by_existing_verifier(self):
        registry = self.registry()
        original = fixtures.job_storage.verify_workspace
        calls = 0
        def verify(workspace, job):
            nonlocal calls
            calls += 1
            if calls == 2: (workspace / 'manifest.json').write_bytes(self.raw + b' ')
            return original(workspace, job)
        with self.assertRaises(ValueError): self.enroll(registry, verify_workspace=verify)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM storage_job_inputs').fetchone()[0], 0)

    def test_legacy_case_conflict_remains_loadable_but_cannot_enter_stricter_manifest(self):
        registry = self.registry()
        (self.project / 'DATA').write_bytes(b'upper')
        (self.project / 'data').write_bytes(b'lower')
        identity, workspace, raw = self.job(backend='legacy')
        with self.assertRaises(ValueError): self.enroll(registry, identity)
        fixtures.job_storage.verify_workspace(workspace, fixtures.store.get_job(identity))
        self.assertEqual((workspace / 'manifest.json').read_bytes(), raw)
        self.assertEqual(list(self.objects.iterdir()), [])

    def test_wrong_managed_root_overlap_and_symlink_workspace_fail_closed(self):
        registry = self.registry()
        with self.assertRaises(RegistryError): self.enroll(registry, managed_root=self.base / 'wrong')
        unsafe = StorageRegistry(self.db, ArtifactRegistrar(self.workspace / 'objects', staging_root=self.base / 'other-scratch'))
        with self.assertRaises(RegistryError): self.enroll(unsafe)
        saved = self.base / 'saved-workspace'
        self.workspace.rename(saved)
        self.workspace.symlink_to(saved, target_is_directory=True)
        with self.assertRaises(OSError): self.enroll(registry)
        self.assertEqual((saved / 'manifest.json').read_bytes(), self.raw)
        self.assertEqual(list(self.objects.iterdir()), [])

    def test_generated_data_is_pinned_as_absent_without_fabricating_a_dataset(self):
        registry = self.registry()
        identity, workspace, raw = self.job(backend='legacy', generated=True)
        binding = self.enroll(registry, identity)
        self.assertIsNone(binding.dataset_id)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM storage_datasets').fetchone()[0], 0)
        fixtures.job_storage.verify_workspace(workspace, fixtures.store.get_job(identity))

    def test_revoked_preexisting_dataset_share_is_not_bypassed_by_registration(self):
        registry = self.registry()
        source = self.source(registry)
        dataset = self.dataset(registry, owner='bob', readers=['alice'])
        registry.bind_job('alice', self.job_id, legacy_manifest_bytes=self.raw, source_sha256=source.sha256,
                          dataset_id=dataset.dataset_id, dataset_version=1)
        registry.set_readers('bob', dataset.dataset_id, [])
        with self.assertRaises(PermissionError): self.enroll(registry, verify_workspace=lambda *args: self.fail('Denied before path read'))

    def test_lazy_registration_does_not_inspect_unrequested_jobs(self):
        registry = self.registry()
        identity, workspace, raw = self.job(backend='legacy')
        (self.workspace / 'source/train.py').unlink()  # Unrelated corrupted snapshot.
        binding = self.enroll(registry, identity)
        self.assertEqual(binding.job_id, identity)
        self.assertIsNone(registry.get_job_inputs('alice', self.job_id))

    def test_nonlocal_input_and_missing_verifier_are_rejected_before_publication(self):
        registry = self.registry()
        with self.assertRaises(TypeError): self.enroll(registry, verify_workspace=None)
        with patch('hybrid_storage.legacy_registration.require_local_fd', side_effect=OSError('not local')):
            with self.assertRaises(OSError): self.enroll(registry)
        self.assertEqual(list(self.objects.iterdir()), [])

    def test_importing_registration_seam_never_initializes_a_database(self):
        script = (f"import sys,sqlite3; sys.path.insert(0,{str(fixtures.ROOT)!r}); "
                  "sqlite3.connect=lambda *a,**k: (_ for _ in ()).throw(AssertionError('No database open')); "
                  "import controller.input_storage,hybrid_storage.legacy_registration; "
                  "assert 'job_store' not in sys.modules; assert 'job_storage' not in sys.modules")
        result = subprocess.run([sys.executable, '-B', '-c', script], capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == '__main__':
    unittest.main(verbosity=2)
