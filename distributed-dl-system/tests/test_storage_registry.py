"""Registry/migration acceptance in copied controllers and disposable local DBs."""

import atexit
from concurrent.futures import ThreadPoolExecutor
from dataclasses import FrozenInstanceError
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import sqlite3
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
COPY = tempfile.TemporaryDirectory(prefix='registry-controller-')
atexit.register(COPY.cleanup)
APP = Path(COPY.name)
for name in ('controller', 'hybrid_storage', 'config', 'examples'):
    shutil.copytree(ROOT / name, APP / name, ignore=shutil.ignore_patterns('jobs.db*', '*.lock', '__pycache__'))
sys.path.insert(0, str(APP))
sys.path.insert(0, str(APP / 'controller'))
import job_store as store
import job_storage
from job_spec import validate_spec
from hybrid_storage.artifacts import ArtifactMismatch, ArtifactRegistrar
from hybrid_storage.dataset_manifests import DatasetEntry
from hybrid_storage.input_manifests import SourceFile, SourceManifest, encoded, parse_source
from hybrid_storage.registry import StorageRegistry
from hybrid_storage import registry_schema as schema
from hybrid_storage.registry_schema import RegistryConflict, RegistryError, migrate, migration_plan


class RegistryTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='storage-registry-')
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name)
        self.db_path = self.base / 'jobs.db'
        for mocked in (patch.object(store, 'DB_FILE', str(self.db_path)),
                       patch.dict(os.environ, DL_STORAGE_ROOT=str(self.base / 'jobs'))):
            mocked.start()
            self.addCleanup(mocked.stop)
        store.initialize_database()
        self.db = self.connect()
        self.addCleanup(self.db.close)
        self.objects, self.scratch = self.base / 'objects', self.base / 'scratch'
        self.objects.mkdir(mode=0o755)
        self.scratch.mkdir(mode=0o700)
        self.publisher = ArtifactRegistrar(self.objects, staging_root=self.scratch)
        self.project = self.base / 'project'
        self.project.mkdir()
        (self.project / 'requirements.txt').write_bytes(b'')
        (self.project / 'train.py').write_bytes(b'print("train")\n')
        (self.project / 'train.py').chmod(0o755)
        (self.project / 'a').mkdir()
        (self.project / 'a/nested.py').write_bytes(b'nested = True\n')
        (self.project / 'a.txt').write_bytes(b'root file sorts differently from traversal\n')
        self.data = self.base / 'data'
        self.data.mkdir()
        (self.data / 'a').mkdir()
        (self.data / 'a/rows.csv').write_bytes(b'x,y\n1,2\n')
        (self.data / 'a.txt').write_bytes(b'root rows\n')
        self.serial = 0
        self.job_id, self.workspace, self.raw = self.job()

    def connect(self):
        db = sqlite3.connect(self.db_path, timeout=10)
        db.execute('PRAGMA foreign_keys=ON')
        db.execute('PRAGMA synchronous=FULL')
        return db

    def job(self, *, owner='alice', backend='nfs', generated=False):
        self.serial += 1
        identity = f'registry-{self.serial}'
        document = json.loads((ROOT / 'examples/job.single.json').read_text())
        document.update(project=str(self.project), dataset=None if generated else str(self.data))
        spec = validate_spec(document, self.base)
        store.add_job(identity, 1, job_type='custom', owner=owner, status='RECEIVED',
                      job_config=spec, storage_backend=backend)
        receipt = job_storage.stage_job(identity)
        workspace = Path(receipt['storage_path'])
        return identity, workspace, (workspace / 'manifest.json').read_bytes()

    def registry(self, db=None):
        db = self.db if db is None else db
        migrate(db)
        return StorageRegistry(db, self.publisher)

    def artifact(self, registry, data=b'data', owner='alice'):
        return registry.register_artifact(owner, io.BytesIO(data), expected_size=len(data))

    def source(self, registry, owner='alice', workspace=None):
        workspace = workspace or self.workspace
        listing = json.loads((workspace / 'manifest.json').read_bytes())['source']['inventory']
        files = []
        for entry in listing['files']:
            with (workspace / 'source' / entry['path']).open('rb') as stream:
                registry.register_artifact(owner, stream, expected_size=entry['size'], expected_sha256=entry['sha256'])
            files.append(SourceFile(entry['path'], entry['sha256'], entry['size'], entry['executable']))
        return registry.register_source(owner, SourceManifest(files, listing['directories']))

    def dataset(self, registry, owner='alice', readers=(), workspace=None):
        workspace = workspace or self.workspace
        listing = json.loads((workspace / 'manifest.json').read_bytes())['data']['inventory']
        entries = [DatasetEntry(path, 'directory') for path in listing['directories']]
        for entry in listing['files']:
            with (workspace / 'data' / entry['path']).open('rb') as stream:
                registry.register_artifact(owner, stream, expected_size=entry['size'], expected_sha256=entry['sha256'])
            entries.append(DatasetEntry(entry['path'], 'file', entry['sha256'], entry['size']))
        return registry.create_dataset(owner, entries, readers=readers)

    def ready(self):
        registry = self.registry()
        return registry, self.source(registry), self.dataset(registry)

    def bind(self, registry, source, data, **changes):
        kwargs = dict(legacy_manifest_bytes=self.raw, source_sha256=source.sha256,
                      dataset_id=data.dataset_id, dataset_version=data.version)
        kwargs.update(changes)
        return registry.bind_job('alice', self.job_id, **kwargs)

    def test_plan_is_readonly_and_migration_is_idempotent_with_old_rows_preserved(self):
        before = list(self.db.iterdump())
        plan = migration_plan(self.db)
        self.assertEqual(plan['action'], 'create')
        self.assertEqual(list(self.db.iterdump()), before)
        job = store.get_job(self.job_id)
        migrate(self.db)
        dump = list(self.db.iterdump())
        migrate(self.db)
        self.assertEqual(list(self.db.iterdump()), dump)
        self.assertEqual(store.get_job(self.job_id), job)
        self.assertEqual(migration_plan(self.db)['action'], 'unchanged')
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM storage_job_inputs').fetchone()[0], 0)
        self.assertEqual(self.db.execute('PRAGMA foreign_key_check').fetchall(), [])

    def test_old_minimal_fixture_migrates_without_job_backfill_or_rewrite(self):
        with sqlite3.connect(self.base / 'old.db') as db:
            db.execute('PRAGMA foreign_keys=ON')
            db.execute('CREATE TABLE jobs(job_id TEXT PRIMARY KEY,status TEXT NOT NULL)')
            db.execute("INSERT INTO jobs VALUES('old-running','RUNNING')")
            db.commit()
            migrate(db)
            migrate(db)
            self.assertEqual(db.execute('SELECT * FROM jobs').fetchall(), [('old-running', 'RUNNING')])
            self.assertEqual([row[1] for row in db.execute('PRAGMA table_info(jobs)')], ['job_id', 'status'])

    def test_interrupted_migration_rolls_back_all_new_schema(self):
        before = list(self.db.iterdump())
        with patch.object(schema, 'DDL', [*schema.DDL, 'CREATE TABLE broken(']):
            with self.assertRaises(sqlite3.OperationalError): migrate(self.db)
        self.assertEqual(list(self.db.iterdump()), before)
        migrate(self.db)

    def test_incompatible_or_partial_schema_is_not_silently_repaired(self):
        self.db.execute('CREATE TABLE storage_artifacts(unrelated TEXT)')
        self.db.commit()
        before = list(self.db.iterdump())
        with self.assertRaises(RegistryError): migrate(self.db)
        self.assertEqual(list(self.db.iterdump()), before)

    def test_missing_index_is_detected(self):
        migrate(self.db)
        self.db.execute('DROP INDEX storage_jobs_dataset')
        self.db.commit()
        with self.assertRaises(RegistryError): migration_plan(self.db)

    def test_unexpected_column_is_detected_without_repair(self):
        migrate(self.db)
        self.db.execute('ALTER TABLE storage_artifacts ADD COLUMN unexpected TEXT')
        self.db.commit()
        before = list(self.db.iterdump())
        with self.assertRaises(RegistryError): migrate(self.db)
        self.assertEqual(list(self.db.iterdump()), before)

    def test_future_marker_version_is_rejected_without_rewriting_it(self):
        migrate(self.db)
        trigger = next(sql for sql in schema.DDL if sql.startswith('CREATE TRIGGER hybrid_schema_migrations_update '))
        self.db.execute('DROP TRIGGER hybrid_schema_migrations_update')
        self.db.execute('PRAGMA ignore_check_constraints=ON')
        self.db.execute('UPDATE hybrid_schema_migrations SET version=2')
        self.db.execute('PRAGMA ignore_check_constraints=OFF')
        self.db.execute(trigger)
        self.db.commit()
        with self.assertRaises(RegistryError): migrate(self.db)
        self.assertEqual(self.db.execute('SELECT version FROM hybrid_schema_migrations').fetchone()[0], 2)

    def test_concurrent_migrators_serialize_and_leave_one_marker(self):
        barrier = threading.Barrier(2, timeout=10)
        def apply(_):
            db = self.connect()
            try:
                barrier.wait()
                return migrate(db)
            finally:
                db.close()
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(apply, range(2)))
        self.assertEqual(results[0], results[1])
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM hybrid_schema_migrations').fetchone()[0], 1)

    def test_active_legacy_attempt_reservation_lease_and_checkpoint_survive_migration(self):
        identity, workspace, raw = self.job(backend='legacy')
        checkpoint = self.base / 'checkpoint.bin'
        checkpoint.write_bytes(b'unchanged fixture checkpoint')
        self.db.execute("UPDATE jobs SET status='RUNNING',retry_count=2,node_id='pi02',checkpoint_path=? WHERE job_id=?",
                        (str(checkpoint), identity))
        self.db.execute("INSERT INTO resource_reservations VALUES('pi02',?,'{}',123)", (identity,))
        self.db.execute("INSERT INTO job_attempts(job_id,attempt_number,status,node_id) VALUES(?,3,'RUNNING','pi02')", (identity,))
        self.db.execute("INSERT INTO node_generations VALUES('pi02',4)")
        self.db.execute('INSERT INTO execution_leases VALUES(?,?,?,?,?,?,?,?,?,?,?,?)',
                        (identity, 2, 'pi02', 4, 'fixture-lease', identity + ':2', 1, 2, 'fixture-boot', 2, 0, 0))
        self.db.commit()
        tables = [row[0] for row in self.db.execute("SELECT name FROM sqlite_master WHERE type='table'")]
        before = {table: self.db.execute('SELECT * FROM ' + table).fetchall() for table in tables}
        migrate(self.db)
        migrate(self.db)
        self.assertEqual({table: self.db.execute('SELECT * FROM ' + table).fetchall() for table in tables}, before)
        self.assertEqual(checkpoint.read_bytes(), b'unchanged fixture checkpoint')
        job_storage.verify_workspace(workspace, store.get_job(identity))

    def test_unknown_checksum_fails_even_with_expected_schema_shape(self):
        migrate(self.db)
        trigger = next(sql for sql in schema.DDL if sql.startswith('CREATE TRIGGER hybrid_schema_migrations_update '))
        self.db.execute('DROP TRIGGER hybrid_schema_migrations_update')
        self.db.execute("UPDATE hybrid_schema_migrations SET checksum='wrong'")
        self.db.execute(trigger)
        self.db.commit()
        with self.assertRaises(RegistryError): migrate(self.db)

    def test_broken_registry_foreign_key_is_detected_without_repair(self):
        migrate(self.db)
        self.db.execute('PRAGMA foreign_keys=OFF')
        self.db.execute('INSERT INTO storage_artifact_owners VALUES(?,?)', ('alice', 'f' * 64))
        self.db.commit()
        self.db.execute('PRAGMA foreign_keys=ON')
        with self.assertRaises(RegistryError): migrate(self.db)

    def test_connection_guards_preserve_callers_uncommitted_work(self):
        self.db.execute("INSERT INTO owner_service VALUES('caller',123)")
        with self.assertRaises(RegistryError): migrate(self.db)
        self.assertTrue(self.db.in_transaction)
        self.assertEqual(self.db.execute("SELECT last_dispatch FROM owner_service WHERE owner='caller'").fetchone()[0], 123)
        self.db.rollback()
        for pragma in ('foreign_keys=OFF', 'synchronous=NORMAL'):
            self.db.execute('PRAGMA ' + pragma)
            with self.subTest(pragma=pragma), self.assertRaises(RegistryError): migrate(self.db)
            self.db.execute('PRAGMA foreign_keys=ON')
            self.db.execute('PRAGMA synchronous=FULL')

    def test_connections_with_remote_metadata_temp_shadow_or_attachments_fail(self):
        with patch('hybrid_storage.registry_schema.require_local_fd', side_effect=OSError('not local')):
            with self.assertRaises(OSError): migrate(self.db)
        self.db.execute('CREATE TEMP TABLE storage_manifests(fake TEXT)')
        with self.assertRaises(RegistryError): migrate(self.db)
        self.db.execute('DROP TABLE temp.storage_manifests')
        self.db.execute('ATTACH DATABASE ? AS other', (str(self.base / 'attached.db'),))
        with self.assertRaises(RegistryError): migrate(self.db)
        self.db.execute('DETACH DATABASE other')

    def test_in_memory_or_missing_jobs_schema_requires_explicit_valid_database(self):
        with sqlite3.connect(':memory:') as db:
            db.execute('PRAGMA foreign_keys=ON')
            with self.assertRaises(RegistryError): migrate(db)
        with sqlite3.connect(self.base / 'empty.db') as db:
            db.execute('PRAGMA foreign_keys=ON')
            with self.assertRaises(RegistryError): migrate(db)

    def test_duplicate_bytes_need_verified_streams_for_each_owners_grant(self):
        registry = self.registry()
        one = self.artifact(registry, owner='alice')
        two = self.artifact(registry, owner='bob')
        self.assertTrue(one.created)
        self.assertFalse(two.created)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM storage_artifacts').fetchone()[0], 1)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM storage_artifact_owners').fetchone()[0], 2)
        self.assertEqual(len(list(self.objects.glob('sha256/*/*'))), 1)

    def test_known_digest_alone_cannot_register_source_or_dataset_for_another_owner(self):
        registry = self.registry()
        artifact = self.artifact(registry)
        file = SourceFile('input', artifact.sha256, artifact.size_bytes)
        with self.assertRaises(PermissionError): registry.register_source('bob', SourceManifest([file]))
        with self.assertRaises(PermissionError): registry.create_dataset('bob', [DatasetEntry('input', 'file', file.sha256, file.size_bytes)])
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM storage_manifests').fetchone()[0], 0)

    def test_missing_or_corrupt_object_cannot_be_committed_as_manifest(self):
        registry = self.registry()
        artifact = self.artifact(registry)
        target = self.objects / artifact.key
        target.chmod(0o600)
        target.write_bytes(b'xxxx')
        target.chmod(0o444)
        with self.assertRaises(ArtifactMismatch): registry.register_source('alice', SourceManifest([SourceFile('input', artifact.sha256, 4)]))
        target.unlink()
        with self.assertRaises(FileNotFoundError): registry.create_dataset('alice', [DatasetEntry('input', 'file', artifact.sha256, 4)])
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM storage_manifests').fetchone()[0], 0)

    def test_failed_metadata_transaction_leaves_only_unreferenced_complete_object(self):
        registry = self.registry()
        # Inject a SQL failure without changing the validated schema.
        def authorizer(action, table, *args):
            return sqlite3.SQLITE_DENY if action == sqlite3.SQLITE_INSERT and table == 'storage_artifact_owners' else sqlite3.SQLITE_OK
        self.db.set_authorizer(authorizer)
        with self.assertRaises(sqlite3.DatabaseError): self.artifact(registry)
        self.db.set_authorizer(None)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM storage_artifacts').fetchone()[0], 0)
        self.assertEqual(len(list(self.objects.glob('sha256/*/*'))), 1)
        self.assertFalse(self.artifact(registry).created)

    def test_source_is_canonical_immutable_and_reuses_draft_source_tree_shape(self):
        registry = self.registry()
        source = self.source(registry)
        self.assertEqual(parse_source(source.canonical_bytes()), source)
        self.assertEqual(source.document()['kind'], 'source-tree')
        self.assertTrue(next(file for file in source.files if file.path == 'train.py').executable)
        self.assertEqual(registry.get_source('alice', source.sha256), source)
        with self.assertRaises(PermissionError): registry.get_source('bob', source.sha256)
        with self.assertRaises(FrozenInstanceError): source.files = ()
        with self.assertRaises(sqlite3.IntegrityError):
            self.db.execute("UPDATE storage_manifests SET canonical=x'00'")
        self.db.rollback()

    def test_source_codec_rejects_unknown_fields_unsafe_paths_and_nonboolean_modes(self):
        registry = self.registry()
        source = self.source(registry)
        for change in ({'kind': 'other'}, {'schema_version': True}, {'extra': 1}):
            with self.assertRaises(ValueError): parse_source(encoded(dict(source.document(), **change)))
        with self.assertRaises(ValueError): SourceFile('../escape', source.files[0].sha256, 1)
        with self.assertRaises(ValueError): SourceFile('file', source.files[0].sha256, 1, 1)
        with self.assertRaises(ValueError): parse_source(json.dumps(source.document()).encode())

    def test_dataset_reads_current_persisted_acl_and_old_versions_keep_bytes(self):
        registry = self.registry()
        data = self.dataset(registry)
        before = data.canonical_bytes()
        registry.set_readers('alice', data.dataset_id, ['bob'])
        self.assertEqual(registry.get_dataset('bob', data.dataset_id, 1), data)
        second = registry.update_dataset('alice', data.dataset_id, expected_head_sha256=data.sha256, entries=[])
        self.assertEqual(second.version, 2)
        registry.set_readers('alice', data.dataset_id, [])
        for version in (1, 2):
            with self.assertRaises(PermissionError): registry.get_dataset('bob', data.dataset_id, version)
        self.assertEqual(registry.get_dataset('alice', data.dataset_id, 1).canonical_bytes(), before)
        self.assertEqual(registry.get_dataset('alice', data.dataset_id, 2).previous_manifest_sha256, data.sha256)

    def test_dataset_reader_cannot_write_share_or_claim_foreign_artifacts(self):
        registry = self.registry()
        data = self.dataset(registry, readers=['bob'])
        with self.assertRaises(PermissionError): registry.set_readers('bob', data.dataset_id, ['eve'])
        with self.assertRaises(PermissionError): registry.update_dataset('bob', data.dataset_id, expected_head_sha256=data.sha256, entries=[])
        with self.assertRaises(PermissionError): registry.create_dataset('bob', data.entries)

    def test_dataset_compare_and_swap_rejects_stale_head_without_partial_rows(self):
        registry = self.registry()
        data = self.dataset(registry)
        registry.update_dataset('alice', data.dataset_id, expected_head_sha256=data.sha256, entries=[])
        before = self.db.execute('SELECT COUNT(*) FROM storage_manifests').fetchone()[0]
        with self.assertRaises(RegistryConflict): registry.update_dataset('alice', data.dataset_id, expected_head_sha256=data.sha256, entries=data.entries)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM storage_manifests').fetchone()[0], before)

    def test_two_connections_racing_dataset_head_commit_exactly_one_version(self):
        registry = self.registry()
        data = self.dataset(registry)
        barrier = threading.Barrier(2, timeout=10)
        def write(_):
            db = self.connect()
            try:
                local = StorageRegistry(db, self.publisher)
                original = local._verify_owned
                def pause(*args):
                    original(*args)
                    barrier.wait()
                local._verify_owned = pause
                try:
                    return local.update_dataset('alice', data.dataset_id, expected_head_sha256=data.sha256, entries=[]).version
                except RegistryConflict:
                    return 'conflict'
            finally:
                db.close()
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(write, range(2)))
        self.assertCountEqual(results, [2, 'conflict'])
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM storage_dataset_versions').fetchone()[0], 2)

    def test_job_pins_exact_source_dataset_requirements_and_legacy_hash(self):
        registry, source, data = self.ready()
        old_job = store.get_job(self.job_id)
        binding = self.bind(registry, source, data)
        self.assertEqual(binding.source_sha256, source.sha256)
        self.assertEqual(binding.dataset_sha256, data.sha256)
        self.assertEqual(binding.legacy_manifest_sha256, hashlib.sha256(self.raw).hexdigest())
        requirements = next(file for file in source.files if file.path == 'requirements.txt')
        self.assertEqual(binding.requirements_sha256, requirements.sha256)
        self.assertEqual(binding.requirements_size_bytes, 0)
        self.assertEqual(registry.get_job_inputs('alice', self.job_id), binding)
        self.assertEqual(self.bind(registry, source, data), binding)
        self.assertEqual(store.get_job(self.job_id), old_job)
        job_storage.verify_workspace(self.workspace, old_job)
        self.assertEqual(self.db.execute('PRAGMA foreign_key_check').fetchall(), [])

    def test_two_owners_jobs_reuse_physical_inputs_with_separate_bindings(self):
        registry, alice_source, alice_data = self.ready()
        alice_binding = self.bind(registry, alice_source, alice_data)
        count = self.db.execute('SELECT COUNT(*) FROM storage_artifacts').fetchone()[0]
        identity, workspace, raw = self.job(owner='bob')
        bob_source = self.source(registry, owner='bob', workspace=workspace)
        bob_data = self.dataset(registry, owner='bob', workspace=workspace)
        bob_binding = registry.bind_job('bob', identity, legacy_manifest_bytes=raw, source_sha256=bob_source.sha256,
                                        dataset_id=bob_data.dataset_id, dataset_version=1)
        self.assertEqual(alice_source.sha256, bob_source.sha256)
        self.assertNotEqual(alice_binding.dataset_id, bob_binding.dataset_id)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM storage_artifacts').fetchone()[0], count)
        self.assertEqual(len(list(self.objects.glob('sha256/*/*'))), count)
        with self.assertRaises(PermissionError): registry.get_job_inputs('bob', self.job_id)
        with self.assertRaises(PermissionError): registry.get_dataset('alice', bob_data.dataset_id, 1)

    def test_composite_foreign_key_rejects_inconsistent_artifact_size(self):
        registry = self.registry()
        artifact = self.artifact(registry)
        source = registry.register_source('alice', SourceManifest([]))
        with self.assertRaises(sqlite3.IntegrityError):
            self.db.execute('INSERT INTO storage_manifest_entries VALUES(?,?,?,?,?,?)',
                            (source.sha256, 'bad', 'file', artifact.sha256, artifact.size_bytes + 1, 0))
        self.db.rollback()

    def test_dataset_head_update_does_not_reinterpret_existing_job_binding(self):
        registry, source, data = self.ready()
        binding = self.bind(registry, source, data)
        registry.update_dataset('alice', data.dataset_id, expected_head_sha256=data.sha256, entries=[])
        self.assertEqual(registry.get_job_inputs('alice', self.job_id), binding)
        with self.assertRaises(sqlite3.IntegrityError): self.db.execute('UPDATE storage_job_inputs SET dataset_version=2')
        self.db.rollback()

    def test_another_valid_dataset_with_same_bytes_cannot_rebind_a_job(self):
        registry, source, data = self.ready()
        self.bind(registry, source, data)
        another = registry.create_dataset('alice', data.entries)
        with self.assertRaises(RegistryConflict): self.bind(registry, source, another)
        self.assertEqual(registry.get_job_inputs('alice', self.job_id).dataset_id, data.dataset_id)

    def test_changed_source_requirements_or_dataset_inventory_cannot_bind(self):
        registry, source, data = self.ready()
        artifact = self.artifact(registry, b'different dependencies')
        files = [file if file.path != 'requirements.txt' else SourceFile(file.path, artifact.sha256, artifact.size_bytes)
                 for file in source.files]
        altered = registry.register_source('alice', SourceManifest(files, source.directories))
        with self.assertRaises(ArtifactMismatch): self.bind(registry, altered, data)
        wrong_data = registry.create_dataset('alice', [])
        with self.assertRaises(ArtifactMismatch): self.bind(registry, source, wrong_data)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM storage_job_inputs').fetchone()[0], 0)

    def test_forged_legacy_bytes_and_other_owner_job_are_denied(self):
        registry, source, data = self.ready()
        with self.assertRaises(ArtifactMismatch): self.bind(registry, source, data, legacy_manifest_bytes=self.raw + b' ')
        with self.assertRaises(PermissionError): registry.bind_job('bob', self.job_id, legacy_manifest_bytes=self.raw, source_sha256=source.sha256)
        with self.assertRaises(PermissionError): registry.get_job_inputs('bob', self.job_id)

    def test_only_held_explicit_nfs_jobs_can_acquire_new_bindings(self):
        registry, source, data = self.ready()
        self.db.execute("UPDATE jobs SET status='QUEUED' WHERE job_id=?", (self.job_id,))
        self.db.commit()
        with self.assertRaises(RegistryError): self.bind(registry, source, data)
        old_id, workspace, raw = self.job(backend='legacy')
        with self.assertRaises(RegistryError): registry.bind_job('alice', old_id, legacy_manifest_bytes=raw, source_sha256=source.sha256,
                                                               dataset_id=data.dataset_id, dataset_version=1)
        self.assertIsNone(registry.get_job_inputs('alice', old_id))
        job_storage.verify_workspace(workspace, store.get_job(old_id))

    def test_generated_data_is_explicit_null_not_a_substitute_for_uploaded_data(self):
        registry, source, data = self.ready()
        with self.assertRaises(ArtifactMismatch): registry.bind_job('alice', self.job_id, legacy_manifest_bytes=self.raw, source_sha256=source.sha256)
        identity, workspace, raw = self.job(generated=True)
        binding = registry.bind_job('alice', identity, legacy_manifest_bytes=raw, source_sha256=source.sha256)
        self.assertIsNone(binding.dataset_id)
        self.assertEqual(registry.get_job_inputs('alice', identity), binding)

    def test_shared_dataset_binding_requires_current_acl_and_does_not_grant_ownership(self):
        registry = self.registry()
        source = self.source(registry)
        foreign = self.dataset(registry, owner='bob')
        with self.assertRaises(PermissionError): self.bind(registry, source, foreign)
        registry.set_readers('bob', foreign.dataset_id, ['alice'])
        binding = self.bind(registry, source, foreign)
        self.assertEqual(binding.dataset_sha256, foreign.sha256)
        registry.set_readers('bob', foreign.dataset_id, [])
        with self.assertRaises(PermissionError): registry.get_job_inputs('alice', self.job_id)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM storage_job_inputs').fetchone()[0], 1)

    def test_revocation_during_byte_verification_prevents_binding_commit(self):
        registry = self.registry()
        source = self.source(registry)
        foreign = self.dataset(registry, owner='bob', readers=['alice'])
        other_db = self.connect()
        self.addCleanup(other_db.close)
        other = StorageRegistry(other_db, self.publisher)
        original = registry._verify_bytes
        def revoke(files):
            original(files)
            other.set_readers('bob', foreign.dataset_id, [])
        with patch.object(registry, '_verify_bytes', side_effect=revoke):
            with self.assertRaises(PermissionError): self.bind(registry, source, foreign)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM storage_job_inputs').fetchone()[0], 0)

    def test_job_config_drift_is_detected_and_cannot_change_pinned_requirements(self):
        registry, source, data = self.ready()
        self.bind(registry, source, data)
        document = json.loads(store.get_job(self.job_id)['job_config'])
        document['entrypoint'] = ['python', 'different.py']
        self.db.execute('UPDATE jobs SET job_config=? WHERE job_id=?', (json.dumps(document), self.job_id))
        self.db.commit()
        with self.assertRaises(ArtifactMismatch): registry.get_job_inputs('alice', self.job_id)

    def test_referenced_artifacts_manifests_bindings_and_jobs_cannot_be_deleted(self):
        registry, source, data = self.ready()
        self.bind(registry, source, data)
        for table in ('storage_artifacts', 'storage_manifests', 'storage_dataset_versions', 'storage_job_inputs'):
            with self.subTest(table=table), self.assertRaises(sqlite3.IntegrityError): self.db.execute('DELETE FROM ' + table)
            self.db.rollback()
        with self.assertRaises(sqlite3.IntegrityError): self.db.execute('DELETE FROM jobs WHERE job_id=?', (self.job_id,))
        self.db.rollback()

    def test_manifest_content_and_normalized_rows_are_verified_on_load(self):
        registry, source, data = self.ready()
        update = next(sql for sql in schema.DDL if sql.startswith('CREATE TRIGGER storage_manifests_update '))
        self.db.execute('DROP TRIGGER storage_manifests_update')
        raw = source.canonical_bytes().replace(b'"executable":true', b'"executable":false')
        self.db.execute('UPDATE storage_manifests SET canonical=? WHERE sha256=?', (raw, source.sha256))
        self.db.execute(update)
        self.db.commit()
        with self.assertRaises(ArtifactMismatch): registry.get_source('alice', source.sha256)


if __name__ == '__main__':
    unittest.main(verbosity=2)
