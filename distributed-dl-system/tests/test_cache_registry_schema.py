"""6.1 schema acceptance in copied controllers/disposable local SQLite only."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
import importlib
import io
from pathlib import Path
import sqlite3
import subprocess
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import test_storage_registry as fixtures
from hybrid_storage import cache_registry_schema as schema
from hybrid_storage import registry_schema as base


class CacheSchemaTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.RegistryTests(methodName='runTest')
        self.addCleanup(self.fixture.doCleanups)
        self.fixture.setUp()
        self.db = self.fixture.db
        self.registry = self.fixture.registry()
        self.artifact = self.registry.register_artifact('alice', io.BytesIO(b'input'), expected_size=5)

    def install(self):
        schema.migrate(self.db)

    def connect(self, factory=sqlite3.Connection):
        db = sqlite3.connect(self.fixture.db_path, timeout=10, factory=factory)
        db.execute('PRAGMA foreign_keys=ON')
        db.execute('PRAGMA synchronous=FULL')
        self.addCleanup(db.close)
        return db

    def insert(self, table, **values):
        columns = ','.join(values)
        with self.db:
            self.db.execute(f'INSERT INTO {table} ({columns}) VALUES ({",".join("?" for _ in values)})', tuple(values.values()))

    def worker(self, worker_id='pi01', *, enrolled=False):
        values = dict(worker_id=worker_id, created_at_ns=10, updated_at_ns=10)
        if enrolled:
            values.update(enabled=1,auth_state='ENROLLED',auth_principal='fixture:'+worker_id,last_seen_ns=10)
        self.insert('cache_registry_workers', **values)

    def identity(self, cache_id='c'*32, worker_id='pi01', epoch=1):
        self.insert('cache_registry_identities',cache_id=cache_id,worker_id=worker_id,
                    epoch_high_water=epoch,created_at_ns=10,updated_at_ns=10)

    def session(self, **overrides):
        values = dict(session_id='d'*32,worker_id='pi01',cache_id='c'*32,cache_epoch=1,
                      worker_boot_id='01234567-0123-4567-8901-012345678901',controller_epoch='e'*32,
                      created_at_ns=10,updated_at_ns=10)
        values.update(overrides)
        self.insert('cache_registry_sessions',**values)

    def advance(self, *, active=False, sequence=1, body='f'*64):
        with self.db:
            self.db.execute('''UPDATE cache_registry_sessions SET last_sequence=?,last_body_sha256=?,
                has_full_snapshot=1,inventory_updated_at_ns=10,fresh_until_ns=30,last_seen_ns=10,
                disk_total_bytes=1000,disk_free_bytes=800,disk_available_bytes=700,
                auth_principal='fixture:pi01',authenticated_at_ns=10,state=? WHERE session_id=?''',
                (sequence,body,'ACTIVE' if active else 'UNVERIFIED','d'*32))

    def entry(self, **overrides):
        values = dict(session_id='d'*32,worker_id='pi01',cache_id='c'*32,cache_epoch=1,
                      sha256=self.artifact.sha256,size_bytes=5,inventory_sequence=1,updated_at_ns=10)
        values.update(overrides)
        self.insert('cache_registry_entries',**values)

    def active_fixture(self):
        self.install(); self.worker(enrolled=True); self.identity(); self.session(); self.advance(active=True)

    def test_import_and_plan_are_readonly_no_connection_or_controller_import(self):
        with patch.object(sqlite3,'connect',side_effect=AssertionError('no connection')):
            importlib.reload(schema)
        before = self.fixture.db_path.read_bytes()
        plan = schema.migration_plan(self.db)
        self.assertEqual(plan['action'],'create')
        self.assertEqual(self.fixture.db_path.read_bytes(),before)
        self.assertFalse(self.db.execute("SELECT 1 FROM sqlite_master WHERE name='cache_registry_workers'").fetchone())

    def test_explicit_idempotent_install_preserves_all_existing_tables_rows_and_version(self):
        tables = [row[0] for row in self.db.execute("SELECT name FROM sqlite_master WHERE type='table'")]
        original = {table:[tuple(row) for row in self.db.execute(f'SELECT * FROM {table}')] for table in tables}
        with self.db: self.db.execute('PRAGMA user_version=73')
        self.install()
        for table in tables:
            self.assertEqual([tuple(row) for row in self.db.execute(f'SELECT * FROM {table}')],original[table])
        marker = tuple(self.db.execute('SELECT * FROM cache_registry_migrations').fetchone())
        self.install()
        self.assertEqual(tuple(self.db.execute('SELECT * FROM cache_registry_migrations').fetchone()),marker)
        self.assertEqual(self.db.execute('PRAGMA user_version').fetchone()[0],73)
        self.assertEqual(schema.migration_plan(self.db)['statements'],[])
        base.require_schema(self.db,check_foreign_keys=True)

    def test_no_workers_or_inventories_backfilled_from_config_or_jobs(self):
        self.install()
        for table in schema.TABLES-{'cache_registry_migrations'}:
            self.assertEqual(self.db.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0],0)

    def test_prerequisite_and_explicit_full_fk_local_connection_are_required(self):
        with closing(sqlite3.connect(':memory:')) as memory:
            with self.assertRaises(base.RegistryError): schema.migrate(memory)
        for pragma in ('PRAGMA foreign_keys=OFF','PRAGMA synchronous=NORMAL'):
            db=self.connect();db.execute(pragma)
            with self.subTest(pragma=pragma),self.assertRaises(base.RegistryError): schema.migrate(db)
        with self.db:
            self.db.execute("INSERT INTO storage_artifacts VALUES (?,?,?)",('a'*64,0,'sha256/aa/'+'a'*64))
            with self.assertRaises(base.RegistryError): schema.migrate(self.db)
        with patch.object(base,'require_local_fd',side_effect=OSError('NFS database refused')):
            with self.assertRaises(OSError): schema.migrate(self.db)
        # Separate legacy-only disposable file: no automatic artifact migration.
        path=self.fixture.base/'legacy-only.db'
        with sqlite3.connect(path) as db:
            db.execute('PRAGMA foreign_keys=ON');db.execute('PRAGMA synchronous=FULL')
            db.execute('CREATE TABLE jobs(job_id TEXT PRIMARY KEY)')
        with sqlite3.connect(path) as db:
            db.execute('PRAGMA foreign_keys=ON');db.execute('PRAGMA synchronous=FULL')
            with self.assertRaises(base.RegistryError): schema.migrate(db)

    def test_unknown_partial_shape_or_checksum_is_never_repaired(self):
        with self.db: self.db.execute('CREATE TABLE cache_registry_unrecognized (value TEXT)')
        with self.assertRaises(base.RegistryError): self.install()
        with self.db: self.db.execute('DROP TABLE cache_registry_unrecognized') # Disposable conflicting fixture only.
        self.install()
        with self.db: self.db.execute('DROP INDEX cache_registry_entry_digest')
        with self.assertRaises(base.RegistryError): schema.migration_plan(self.db)

    def test_future_marker_is_rejected_without_rollback_or_deletion(self):
        self.install()
        with self.db:
            self.db.execute("INSERT INTO cache_registry_migrations VALUES (2,'future-checksum',20)")
        before=self.fixture.db_path.read_bytes()
        with self.assertRaises(base.RegistryError): self.install()
        self.assertEqual(self.fixture.db_path.read_bytes(),before)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM cache_registry_migrations').fetchone()[0],2)

    def test_registry_marker_and_event_history_are_immutable(self):
        self.active_fixture()
        self.insert('cache_registry_events',session_id='d'*32,worker_id='pi01',sequence=1,
                    kind='FULL',body_sha256='f'*64,received_at_ns=10)
        for sql in ("UPDATE cache_registry_migrations SET checksum='bad'",'DELETE FROM cache_registry_migrations',
                    'DELETE FROM cache_registry_events',"UPDATE cache_registry_events SET body_sha256='bad'"):
            with self.subTest(sql=sql),self.assertRaises(sqlite3.IntegrityError),self.db: self.db.execute(sql)

    def test_workers_and_sessions_default_untrusted_ineligible(self):
        self.install();self.worker();self.identity();self.session()
        row=self.db.execute('SELECT enabled,auth_state,auth_principal,last_seen_ns FROM cache_registry_workers').fetchone()
        self.assertEqual(tuple(row),(0,'UNENROLLED',None,None))
        self.assertEqual(tuple(self.db.execute('SELECT state,last_sequence,has_full_snapshot FROM cache_registry_sessions').fetchone()),
                         ('UNVERIFIED',0,0))
        with self.assertRaises(sqlite3.IntegrityError): self.advance(active=True)

    def test_worker_id_and_auth_principal_constraints_match_operator_conventions(self):
        self.install()
        for identity in ('local','../pi01','_pi01','pi.01','a'*64,''):
            with self.subTest(identity=identity),self.assertRaises(sqlite3.IntegrityError): self.worker(identity)
        self.worker(enrolled=True)
        with self.assertRaises(sqlite3.IntegrityError):
            self.insert('cache_registry_workers',worker_id='pi02',auth_principal='fixture:pi01',created_at_ns=10,updated_at_ns=10)

    def test_worker_cache_spoof_or_epoch_mismatch_has_no_valid_session(self):
        self.install();self.worker();self.worker('pi02');self.identity()
        for changes in ({'worker_id':'pi02'},{'cache_epoch':2},{'cache_id':'b'*32}):
            with self.subTest(changes=changes),self.assertRaises(sqlite3.IntegrityError): self.session(**changes)

    def test_boot_identity_requires_canonical_kernel_uuid_shape(self):
        self.install();self.worker();self.identity()
        for value in ('-'*36,'a'*36,'01234567-0123-4567-8901-01234567890Z'):
            with self.subTest(value=value),self.assertRaises(sqlite3.IntegrityError): self.session(worker_boot_id=value)

    def test_retirement_and_epoch_advance_invalidate_old_session_without_deleting_rows(self):
        self.active_fixture();self.entry(state='VERIFIED',verified_bytes=5,last_verified_at_ns=9)
        with self.db: self.db.execute('UPDATE cache_registry_identities SET epoch_high_water=2')
        self.assertEqual(self.db.execute('SELECT state FROM cache_registry_sessions').fetchone()[0],'STALE')
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM cache_registry_entries').fetchone()[0],1)
        for sql in ('UPDATE cache_registry_identities SET epoch_high_water=1',
                    'DELETE FROM cache_registry_identities'):
            with self.subTest(sql=sql),self.assertRaises(sqlite3.IntegrityError),self.db: self.db.execute(sql)
        with self.db: self.db.execute('UPDATE cache_registry_identities SET retired=1')
        with self.assertRaises(sqlite3.IntegrityError),self.db: self.db.execute('UPDATE cache_registry_identities SET retired=0')

    def test_auth_revocation_or_disable_invalidates_observations_without_erasing_history(self):
        self.active_fixture();self.entry()
        with self.db: self.db.execute("UPDATE cache_registry_workers SET auth_state='REVOKED'")
        self.assertEqual(self.db.execute('SELECT state FROM cache_registry_sessions').fetchone()[0],'STALE')
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM cache_registry_entries').fetchone()[0],1)
        with self.assertRaises(sqlite3.IntegrityError),self.db: self.db.execute("UPDATE cache_registry_sessions SET state='ACTIVE'")

    def test_stale_or_closed_session_cannot_reactivate_without_new_handshake(self):
        self.active_fixture()
        with self.db: self.db.execute("UPDATE cache_registry_sessions SET state='STALE'")
        with self.assertRaises(sqlite3.IntegrityError),self.db:
            self.db.execute("UPDATE cache_registry_sessions SET state='ACTIVE'")
        with self.db: self.db.execute("UPDATE cache_registry_sessions SET state='CLOSED'")
        with self.assertRaises(sqlite3.IntegrityError),self.db:
            self.db.execute("UPDATE cache_registry_sessions SET state='UNVERIFIED'")

    def test_only_one_active_session_per_worker_and_session_identity_cannot_change(self):
        self.active_fixture()
        with self.assertRaises(sqlite3.IntegrityError):
            self.session(session_id='a'*32,state='ACTIVE',auth_principal='fixture:pi01',authenticated_at_ns=10,
                last_sequence=1,last_body_sha256='f'*64,has_full_snapshot=1,inventory_updated_at_ns=10,
                fresh_until_ns=30,last_seen_ns=10,disk_total_bytes=1000,disk_free_bytes=800,disk_available_bytes=700)
        for field,value in (('worker_id','pi02'),('controller_epoch','a'*32),('worker_boot_id','a'*36)):
            with self.subTest(field=field),self.assertRaises(sqlite3.IntegrityError),self.db:
                self.db.execute(f'UPDATE cache_registry_sessions SET {field}=?',(value,))

    def test_sequence_replay_cannot_change_body_or_extend_inventory_freshness(self):
        self.active_fixture()
        for changes in ("last_sequence=0","last_body_sha256='"+'a'*64+"'",'fresh_until_ns=40','inventory_updated_at_ns=11'):
            with self.subTest(changes=changes),self.assertRaises(sqlite3.IntegrityError),self.db:
                self.db.execute('UPDATE cache_registry_sessions SET '+changes)
        with self.db: self.db.execute('UPDATE cache_registry_sessions SET last_seen_ns=20')
        self.assertEqual(self.db.execute('SELECT fresh_until_ns FROM cache_registry_sessions').fetchone()[0],30)

    def test_verified_entry_binds_exact_artifact_size_worker_cache_epoch_and_sequence(self):
        self.active_fixture()
        for changes in ({'sha256':'a'*64},{'size_bytes':6},{'worker_id':'pi02'},{'cache_epoch':2},
                        {'inventory_sequence':2},{'state':'VERIFIED','verified_bytes':4,'last_verified_at_ns':9},
                        {'state':'VERIFIED','verified_bytes':5}):
            with self.subTest(changes=changes),self.assertRaises(sqlite3.IntegrityError): self.entry(**changes)
        self.entry(state='VERIFIED',verified_bytes=5,last_verified_at_ns=9)
        with self.assertRaises(sqlite3.IntegrityError): self.entry()
        with self.db: self.db.execute("UPDATE cache_registry_entries SET state='CORRUPT',verified_bytes=0")
        self.assertEqual(tuple(self.db.execute('SELECT state,verified_bytes,last_verified_at_ns FROM cache_registry_entries').fetchone()),
                         ('CORRUPT',0,9))

    def test_disk_capacity_inconsistent_negative_and_noninteger_values_are_rejected(self):
        self.active_fixture()
        for changes in ('disk_available_bytes=900','disk_total_bytes=-1','disk_free_bytes=NULL',"disk_free_bytes='bad'"):
            with self.subTest(changes=changes),self.assertRaises(sqlite3.IntegrityError),self.db:
                self.db.execute('UPDATE cache_registry_sessions SET '+changes)

    def test_event_ids_bind_worker_and_sequence_and_delta_base(self):
        self.active_fixture()
        valid=dict(session_id='d'*32,worker_id='pi01',sequence=1,kind='FULL',body_sha256='f'*64,received_at_ns=10)
        for changes in ({'worker_id':'pi02'},{'sequence':2},{'kind':'CACHE_ADDED','base_sequence':None},
                        {'kind':'CACHE_ADDED','base_sequence':2}):
            with self.subTest(changes=changes),self.assertRaises(sqlite3.IntegrityError):
                self.insert('cache_registry_events',**dict(valid,**changes))
        self.insert('cache_registry_events',**valid)
        with self.assertRaises(sqlite3.IntegrityError): self.insert('cache_registry_events',**valid)

    def test_interrupted_migration_rolls_back_all_new_ddl_without_touching_old_rows(self):
        class Interrupted(sqlite3.Connection):
            def execute(inner,sql,*args,**kwargs):
                if sql==schema.DDL[3]: raise OSError('injected migration interruption')
                return super().execute(sql,*args,**kwargs)
        db=self.connect(Interrupted)
        before=tuple(self.db.execute('SELECT * FROM jobs').fetchone())
        with self.assertRaises(OSError): schema.migrate(db)
        self.assertFalse(self.db.execute("SELECT 1 FROM sqlite_master WHERE name LIKE 'cache_registry_%'").fetchone())
        self.assertEqual(tuple(self.db.execute('SELECT * FROM jobs').fetchone()),before)
        self.install()

    def test_concurrent_idempotent_migrations_serialize_with_one_marker(self):
        def migrate(_):
            with closing(sqlite3.connect(self.fixture.db_path,timeout=10)) as db:
                db.execute('PRAGMA foreign_keys=ON');db.execute('PRAGMA synchronous=FULL')
                return schema.migrate(db)
        with ThreadPoolExecutor(max_workers=3) as executor:
            results=list(executor.map(migrate,range(3)))
        self.assertTrue(all(result['checksum']==schema.CHECKSUM for result in results))
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM cache_registry_migrations').fetchone()[0],1)
        schema.require_schema(self.db)

    def test_indexes_foreign_keys_and_base_registry_compatibility(self):
        self.active_fixture();self.entry(state='VERIFIED',verified_bytes=5,last_verified_at_ns=9)
        indexes={row[1] for row in self.db.execute('PRAGMA index_list(cache_registry_entries)')}
        self.assertIn('cache_registry_entry_digest',indexes)
        self.assertIn('cache_registry_entry_worker',indexes)
        self.assertEqual(self.db.execute('PRAGMA foreign_key_check').fetchall(),[])
        base.require_schema(self.db,check_foreign_keys=True)
        schema.require_schema(self.db)
        self.assertEqual(self.registry.register_artifact('alice',io.BytesIO(b'input'),expected_size=5),
                         self.registry.register_artifact('alice',io.BytesIO(b'input'),expected_size=5))


if __name__=='__main__':
    unittest.main(verbosity=2)
