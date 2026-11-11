"""Actual legacy fixture + new deploy transaction; no production DB opens."""
from contextlib import closing
import importlib
from pathlib import Path
import sqlite3
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from test_deployment_executor import ExecutorTests
from hybrid_storage import deployment_migrations as migrations
from hybrid_storage import deployment_executor as deploy


class MigrationTests(unittest.TestCase):
    def setUp(self):
        f = ExecutorTests(methodName='runTest')
        self.addCleanup(f.doCleanups)
        f.setUp()
        self.fixture = f
        import test_storage_registry as actual
        legacy = actual.RegistryTests(methodName='runTest')
        self.addCleanup(legacy.doCleanups)
        legacy.setUp()
        legacy.db.execute("INSERT INTO jobs (job_id,status,duration,created_at) VALUES ('keep-job','QUEUED',5,1)")
        legacy.db.execute('PRAGMA user_version=97')
        legacy.db.commit()
        self.original_jobs = list(legacy.db.execute('SELECT job_id,status FROM jobs ORDER BY job_id'))
        self.dbpath = f.app / 'controller/jobs.db'
        with closing(sqlite3.connect(self.dbpath)) as db:
            legacy.db.backup(db)
        for name in migrations.MIGRATIONS:
            filename = 'hybrid_storage/' + name + '.py'
            f.raw[filename] = (ROOT / filename).read_bytes()
        f.build()
        f.install()

    def run_migrate(self):
        f = self.fixture
        return migrations.migrate(f.plan, f.receipt('migrate'), services=f.stopped)

    def test_backup_restored_rehearsal_twice_live_fixture_once_retry_and_source_rollback(self):
        result = self.run_migrate()
        self.assertTrue(result['rehearsed_twice'])
        self.assertTrue(result['legacy_preserved'])
        self.assertFalse(result['database_restored'])
        recovery = Path(result['recovery_directory'])
        before = self.fixture.tree(recovery)
        self.assertEqual(self.run_migrate()['status'], 'ALREADY_MIGRATED')
        self.assertEqual(self.fixture.tree(recovery), before)
        self.fixture.rollback()
        with closing(sqlite3.connect(self.dbpath)) as db:
            self.assertEqual(db.execute('SELECT job_id,status FROM jobs ORDER BY job_id').fetchall(), self.original_jobs)
            self.assertEqual(db.execute('PRAGMA user_version').fetchone()[0], 97)
            self.assertEqual(db.execute('PRAGMA quick_check').fetchone()[0], 'ok')
            self.assertEqual(db.execute('SELECT COUNT(*) FROM hybrid_schema_migrations').fetchone()[0], 1)
        self.assertEqual(self.fixture.tree(recovery), before)

    def test_active_job_blocks_before_backup(self):
        f = self.fixture
        with closing(sqlite3.connect(self.dbpath)) as db:
            db.execute("UPDATE jobs SET status='RUNNING'")
            db.commit()
        with self.assertRaisesRegex(ValueError, 'Active controller job'):
            self.run_migrate()
        tx = next((f.app / '.hybrid-deploy/snapshots').iterdir())
        self.assertFalse((tx / 'database-recovery').exists())

    def test_outstanding_reservation_blocks_migration(self):
        with closing(sqlite3.connect(self.dbpath)) as db:
            db.execute("INSERT INTO resource_reservations VALUES ('pi02','keep-job','{}',1)")
            db.commit()
        with self.assertRaisesRegex(ValueError, 'reservation'):
            self.run_migrate()

    def test_unexpired_execution_lease_blocks_migration(self):
        import time
        with closing(sqlite3.connect(self.dbpath)) as db:
            db.execute("INSERT INTO execution_leases VALUES ('keep-job',0,'pi02',1,'fixture-lease','fixture-token',?,?, 'fixture-boot',1,0,0)", (time.time(), time.time()+60))
            db.commit()
        with self.assertRaisesRegex(ValueError, 'lease'):
            self.run_migrate()

    def test_wrong_approval_never_opens_database(self):
        f = self.fixture
        with patch('sqlite3.connect', side_effect=AssertionError('DB must not open')), self.assertRaises(PermissionError):
            migrations.migrate(f.plan, f.receipt('install'), services=f.stopped)

    def test_partial_migration_retry_retains_original_recovery_and_legacy_rows(self):
        module = importlib.import_module('hybrid_storage.staging_schema')
        original = module.migrate
        count = 0
        def fail_in_production(db):
            nonlocal count
            count += 1
            if count == 3:
                raise OSError('simulated interruption after first live additive migration')
            return original(db)
        with patch.object(module, 'migrate', side_effect=fail_in_production), self.assertRaises(OSError):
            self.run_migrate()
        with closing(sqlite3.connect(self.dbpath)) as db:
            self.assertIsNotNone(db.execute("SELECT name FROM sqlite_master WHERE name='cache_registry_workers'").fetchone())
            self.assertIsNone(db.execute("SELECT name FROM sqlite_master WHERE name='hybrid_staging_attempts'").fetchone())
        tx = next((self.fixture.app / '.hybrid-deploy/snapshots').iterdir())
        before = self.fixture.tree(tx / 'database-recovery')
        self.assertEqual(self.run_migrate()['status'], 'MIGRATED_SERVICES_STOPPED')
        self.assertEqual(self.fixture.tree(tx / 'database-recovery'), before)

    def test_post_migration_db_drift_prevents_blind_retry(self):
        self.run_migrate()
        with closing(sqlite3.connect(self.dbpath)) as db:
            db.execute("UPDATE jobs SET status='HELD'")
            db.commit()
        with self.assertRaisesRegex(ValueError, 'DB changed'):
            self.run_migrate()


if __name__ == '__main__':
    # Executor helper is imported for setup, not counted/executed a second time.
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(MigrationTests)
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
