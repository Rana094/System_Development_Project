"""7.1 side state/attempt/reservation/lease barrier; no hybrid dispatch or RPC."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import replace
import importlib
import json
from pathlib import Path
import sqlite3
import sys
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT));sys.path.insert(0,str(ROOT/'agent'))
import test_storage_registry as fixtures
import execution_leases as leases
from hybrid_storage import staging_schema as schema
from hybrid_storage import registry_schema as base
from hybrid_storage.config import StorageConfig,StorageConfigError
from hybrid_storage.staging_lifecycle import StagingLifecycle,require_ready
from hybrid_storage.workspaces import AttemptIdentity,WorkspacePlan


class StagingTests(unittest.TestCase):
    def setUp(self):
        self.fixture=fixtures.RegistryTests(methodName='runTest')
        self.addCleanup(self.fixture.doCleanups);self.fixture.setUp()
        self.db=self.fixture.db;self.registry=self.fixture.registry()
        self.source=self.fixture.source(self.registry);self.data=self.fixture.dataset(self.registry)
        self.binding=self.fixture.bind(self.registry,self.source,self.data)
        self.job_id=self.fixture.job_id
        self.settings=StorageConfig('nfs',True,True,'resource')
        self.lifecycle=StagingLifecycle(self.registry,settings=self.settings)

    def install(self):schema.migrate(self.db)

    def reserve(self,world=1,*,begin=True):
        if world>1:
            document=json.loads((ROOT/'examples/job.single.json').read_text())
            document.update(project=str(self.fixture.project),dataset=str(self.fixture.data),execution=dict(mode='distributed',workers=world))
            spec=fixtures.validate_spec(document,self.fixture.base)
            self.job_id='stage-ddp'
            fixtures.store.add_job(self.job_id,1,job_type='custom',owner='alice',status='RECEIVED',job_config=spec,storage_backend='nfs')
            receipt=fixtures.job_storage.stage_job(self.job_id);workspace=Path(receipt['storage_path'])
            self.source=self.fixture.source(self.registry,workspace=workspace)
            self.data=self.fixture.dataset(self.registry,workspace=workspace)
            self.binding=self.registry.bind_job('alice',self.job_id,legacy_manifest_bytes=(workspace/'manifest.json').read_bytes(),
                source_sha256=self.source.sha256,dataset_id=self.data.dataset_id,dataset_version=1)
        # Explicit synthetic prerequisite: production NFS enqueue/dispatch stays
        # gated until 7.2. Do not pretend these rows were a live hybrid dispatch.
        with self.db:
            self.db.execute("UPDATE jobs SET status='SCHEDULED',node_id='pi01',node_ip='192.0.2.1',scheduled_at=1 WHERE job_id=?",(self.job_id,))
            self.db.execute("INSERT INTO job_attempts(job_id,attempt_number,node_id,node_ip,status) VALUES(?,1,'pi01','192.0.2.1','SCHEDULED')",(self.job_id,))
            for rank in range(world):
                self.db.execute('INSERT INTO resource_reservations VALUES(?,?,?,?)',(f'pi{rank+1:02}',self.job_id,'{}',1))
                if world>1:self.db.execute('INSERT INTO job_ranks VALUES(?,?,?,?,?,?,?,?,?)',
                    (self.job_id,0,rank,f'pi{rank+1:02}',f'192.0.2.{rank+1}',5000,'RESERVED',None,None))
            if world>1:self.db.execute('INSERT INTO distributed_attempts VALUES(?,?,?,?,?,?)',
                (self.job_id,0,'STARTING','{}',None,1))
        job=fixtures.store.get_job(self.job_id)
        self.leases=[leases.ensure(job,f'pi{rank+1:02}') for rank in range(world)]
        self.plans=tuple(WorkspacePlan(AttemptIdentity(self.job_id,f'{self.job_id}:0',rank),self.binding.sha256,self.source,self.data)
                         for rank in range(world))
        return self.lifecycle.begin('alice',self.job_id,self.plans) if begin else None

    def ready(self,attempt,rank=0,receipt='a'*64):
        return self.lifecycle.record_ready('alice',attempt,rank,plan_sha256=self.plans[rank].sha256,receipt_sha256=receipt)

    def barrier(self):
        with base.transaction(self.db,write=False):return require_ready(self.db,self.job_id)

    def test_import_constructor_and_plan_do_not_open_or_write_database(self):
        with patch.object(sqlite3,'connect',side_effect=AssertionError('no connection')):
            importlib.reload(schema);StagingLifecycle(self.registry)
        before=list(self.db.iterdump());self.assertEqual(schema.migration_plan(self.db)['action'],'create')
        self.assertEqual(list(self.db.iterdump()),before)

    def test_explicit_idempotent_migration_preserves_all_existing_rows_and_user_version(self):
        with self.db:self.db.execute('PRAGMA user_version=71')
        tables=[row[0] for row in self.db.execute("SELECT name FROM sqlite_master WHERE type='table'")]
        rows={table:[tuple(r) for r in self.db.execute(f'SELECT * FROM {table}')] for table in tables}
        self.install();self.install()
        for table in tables:self.assertEqual([tuple(r) for r in self.db.execute(f'SELECT * FROM {table}')],rows[table])
        self.assertEqual(self.db.execute('PRAGMA user_version').fetchone()[0],71)
        self.assertEqual(schema.migration_plan(self.db)['action'],'unchanged')
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM hybrid_staging_attempts').fetchone()[0],0)

    def test_partial_future_schema_and_nonfull_connection_refused_without_repair(self):
        with self.db:self.db.execute('CREATE TABLE hybrid_staging_future (value TEXT)')
        with self.assertRaises(base.RegistryError):self.install()
        self.assertIsNotNone(self.db.execute("SELECT 1 FROM sqlite_master WHERE name='hybrid_staging_future'").fetchone())
        with closing(self.fixture.connect()) as db:
            db.execute('PRAGMA synchronous=NORMAL')
            with self.assertRaises(base.RegistryError):schema.migrate(db)

    def test_interrupted_ddl_rolls_back_only_new_namespace(self):
        class Interrupted(sqlite3.Connection):
            def execute(inner,sql,*args,**kwargs):
                if sql==schema.DDL[2]:raise OSError('injected migration crash')
                return super().execute(sql,*args,**kwargs)
        with closing(sqlite3.connect(self.fixture.db_path,factory=Interrupted)) as db:
            db.execute('PRAGMA foreign_keys=ON');db.execute('PRAGMA synchronous=FULL')
            before=list(self.db.iterdump())
            with self.assertRaises(OSError):schema.migrate(db)
        self.assertEqual(list(self.db.iterdump()),before)

    def test_concurrent_migrations_install_exactly_one_marker(self):
        def migrate(_):
            with closing(self.fixture.connect()) as db:return schema.migrate(db)
        with ThreadPoolExecutor(max_workers=3) as pool:results=list(pool.map(migrate,range(3)))
        self.assertTrue(all(r['checksum']==schema.CHECKSUM for r in results))
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM hybrid_staging_migrations').fetchone()[0],1)

    def test_off_default_and_unreserved_held_job_cannot_enter_staging(self):
        self.install()
        with self.assertRaises(StorageConfigError):StagingLifecycle(self.registry).begin('alice',self.job_id,())
        plans=(WorkspacePlan(AttemptIdentity(self.job_id,f'{self.job_id}:0'),self.binding.sha256,self.source,self.data),)
        with self.assertRaises(base.RegistryError):self.lifecycle.begin('alice',self.job_id,plans)
        self.assertEqual(fixtures.store.get_job(self.job_id)['status'],'HELD')

    def test_single_attempt_tracks_history_reservation_lease_without_changing_jobs_state(self):
        self.install();attempt=self.reserve()
        job=fixtures.store.get_job(self.job_id);history=fixtures.store.get_attempts(self.job_id)
        self.assertEqual(job['status'],'SCHEDULED');self.assertIsNone(job['started_at'])
        self.assertEqual(attempt.attempt_id,history[0]['attempt_id']);self.assertEqual(attempt.retry_count,0)
        row=self.db.execute('SELECT lease_id,generation,phase FROM hybrid_staging_ranks').fetchone()
        self.assertEqual(tuple(row),(self.leases[0]['lease_id'],self.leases[0]['generation'],'STAGING'))
        with self.assertRaises(base.RegistryError):self.barrier()
        self.assertEqual(self.lifecycle.begin('alice',self.job_id,self.plans),attempt)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM hybrid_staging_attempts').fetchone()[0],1)

    def test_ready_requires_exact_receipt_and_is_idempotent_not_launch_permission(self):
        self.install();attempt=self.reserve()
        with self.assertRaises(base.RegistryError):
            self.lifecycle.record_ready('alice',attempt,0,plan_sha256='f'*64,receipt_sha256='a'*64)
        self.assertEqual(self.ready(attempt),'READY');before=list(self.db.iterdump())
        self.assertEqual(self.ready(attempt),'READY');self.assertEqual(list(self.db.iterdump()),before)
        self.assertEqual(self.barrier(),attempt)
        with self.assertRaises(base.RegistryConflict):self.ready(attempt,receipt='b'*64)
        with self.assertRaises(ValueError):fixtures.store.mark_running(self.job_id)
        self.assertEqual(fixtures.store.get_job(self.job_id)['status'],'SCHEDULED')

    def test_actual_mark_running_blocks_nfs_before_schema_and_while_staging(self):
        # Establish rows before installing the side schema; direct job API must fail closed.
        self.reserve(begin=False)
        with self.assertRaises(ValueError):fixtures.store.mark_running(self.job_id)
        self.assertIsNone(fixtures.store.get_job(self.job_id)['started_at'])
        self.install();self.lifecycle.begin('alice',self.job_id,self.plans)
        with self.assertRaises(ValueError):fixtures.store.mark_running(self.job_id)
        self.assertIsNone(fixtures.store.get_job(self.job_id)['started_at'])
        self.assertEqual(fixtures.store.get_attempts(self.job_id)[0]['status'],'SCHEDULED')
        with self.assertRaises(sqlite3.IntegrityError),self.db:
            self.db.execute("UPDATE jobs SET status='RUNNING' WHERE job_id=?",(self.job_id,))

    def test_lease_expiry_revocation_or_generation_change_prevents_ready(self):
        self.install();attempt=self.reserve()
        for column,value in (('expires_at',0),('revoked',1),('renewal_blocked',1),('generation',self.leases[0]['generation']+1)):
            before=self.db.execute(f'SELECT {column} FROM execution_leases').fetchone()[0]
            with self.db:self.db.execute(f'UPDATE execution_leases SET {column}=?',(value,))
            with self.subTest(column=column),self.assertRaises(base.RegistryError):self.ready(attempt)
            with self.db:self.db.execute(f'UPDATE execution_leases SET {column}=?',(before,))
        self.assertEqual(self.ready(attempt),'READY')

    def test_reservation_mutation_and_monotonic_or_boot_expiry_block_launch_barrier(self):
        self.install();attempt=self.reserve();self.ready(attempt)
        with self.db:self.db.execute('UPDATE resource_reservations SET created_at=2')
        with self.assertRaises(base.RegistryError):self.barrier()
        with self.db:self.db.execute('UPDATE resource_reservations SET created_at=1')
        with base.transaction(self.db,write=False):
            with self.assertRaises(base.RegistryError):require_ready(self.db,self.job_id,boot='other-boot')
            with self.assertRaises(base.RegistryError):require_ready(self.db,self.job_id,clock=10**30)

    def test_cancellation_rejects_late_ready_without_releasing_reservation(self):
        self.install();attempt=self.reserve()
        self.assertEqual(fixtures.store.control_job(self.job_id,'alice','cancel'),'CANCELLING')
        with self.assertRaises(base.RegistryError):self.ready(attempt)
        with self.assertRaises(ValueError):fixtures.store.mark_running(self.job_id)
        self.assertEqual(len(fixtures.store.get_reservations()),1)

    def test_requeued_attempt_keeps_history_and_cannot_accept_previous_readiness(self):
        self.install();attempt=self.reserve();self.ready(attempt)
        self.assertEqual(fixtures.store.requeue_after_failure(self.job_id,'isolated retry fixture'),'QUEUED')
        self.assertEqual(fixtures.store.get_job(self.job_id)['retry_count'],1)
        with self.assertRaises(base.RegistryError):self.ready(attempt)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM hybrid_staging_attempts').fetchone()[0],1)
        self.assertEqual(fixtures.store.get_reservations(),[])
        with self.assertRaises(ValueError):fixtures.store.mark_scheduled(self.job_id,'pi01','192.0.2.1')

    def test_current_dataset_acl_revocation_invalidates_a_ready_barrier(self):
        self.install()
        identity,workspace,raw=self.fixture.job()
        self.source=self.fixture.source(self.registry,workspace=workspace)
        self.data=self.fixture.dataset(self.registry,owner='bob',readers=['alice'],workspace=workspace)
        self.binding=self.registry.bind_job('alice',identity,legacy_manifest_bytes=raw,source_sha256=self.source.sha256,
            dataset_id=self.data.dataset_id,dataset_version=1)
        self.job_id=identity;attempt=self.reserve();self.ready(attempt)
        self.registry.set_readers('bob',self.data.dataset_id,[])
        with self.assertRaises(PermissionError):self.barrier()
        with self.assertRaises(PermissionError):self.ready(attempt)

    def test_same_lease_renewal_preserves_staging_identity_and_ready_receipt(self):
        self.install();attempt=self.reserve();self.ready(attempt)
        with self.db:
            self.db.execute('UPDATE execution_leases SET expires_at=expires_at+1,expires_clock=expires_clock+1')
        self.assertEqual(self.barrier(),attempt)

    def test_ready_barrier_rejects_remote_database_even_on_existing_job_store_connection(self):
        self.install();attempt=self.reserve();self.ready(attempt)
        with patch.object(schema,'require_local_fd',side_effect=OSError('remote DB refused')):
            with self.assertRaises(OSError):self.barrier()

    def test_ddp_all_ranks_and_all_current_leases_required(self):
        self.install();attempt=self.reserve(3)
        self.assertEqual(self.ready(attempt,0),'STAGING');self.assertEqual(self.ready(attempt,1),'STAGING')
        with self.assertRaises(base.RegistryError):self.barrier()
        with self.db:self.db.execute("UPDATE execution_leases SET revoked=1 WHERE node_id='pi01'")
        with self.assertRaises(base.RegistryError):self.ready(attempt,2)
        self.assertEqual(self.db.execute('SELECT phase FROM hybrid_staging_ranks WHERE rank=2').fetchone()[0],'STAGING')
        with self.db:self.db.execute("UPDATE execution_leases SET revoked=0 WHERE node_id='pi01'")
        self.assertEqual(self.ready(attempt,2),'READY');self.assertEqual(self.barrier(),attempt)
        self.assertEqual(len(fixtures.store.get_reservations()),3)

    def test_unauthorized_owner_plan_and_stale_handle_rejected(self):
        self.install();attempt=self.reserve()
        with self.assertRaises(PermissionError):self.lifecycle.begin('bob',self.job_id,self.plans)
        with self.assertRaises(base.RegistryError):
            self.lifecycle.begin('alice',self.job_id,(replace(self.plans[0],input_manifest_sha256='f'*64),))
        with self.assertRaises(base.RegistryError):self.lifecycle.record_ready('alice',replace(attempt,retry_count=1),0,
            plan_sha256=self.plans[0].sha256,receipt_sha256='a'*64)

    def test_staging_history_identity_and_backward_transition_are_immutable(self):
        self.install();attempt=self.reserve();self.ready(attempt)
        for sql in ("UPDATE hybrid_staging_attempts SET retry_count=1",'DELETE FROM hybrid_staging_attempts',
                    "UPDATE hybrid_staging_attempts SET phase='STAGING'","UPDATE hybrid_staging_ranks SET receipt_sha256='"+'b'*64+"'"):
            with self.subTest(sql=sql),self.assertRaises(sqlite3.IntegrityError),self.db:self.db.execute(sql)
        self.assertEqual(self.db.execute('PRAGMA foreign_key_check').fetchall(),[])

    def test_legacy_jobs_still_reserve_run_complete_without_staging_records(self):
        self.install();fixtures.store.add_job('legacy',1)
        fixtures.store.mark_scheduled('legacy','pi02','192.0.2.2');fixtures.store.mark_running('legacy')
        self.assertEqual(fixtures.store.get_job('legacy')['status'],'RUNNING')
        with patch('job_timing.write_report'):self.assertTrue(fixtures.store.mark_completed('legacy'))
        self.assertEqual(fixtures.store.get_job('legacy')['status'],'COMPLETED')
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM hybrid_staging_attempts').fetchone()[0],0)


if __name__=='__main__':unittest.main(verbosity=2)
