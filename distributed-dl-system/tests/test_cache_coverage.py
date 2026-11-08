"""6.3 exact authorized metadata coverage; disposable local caches/SQLite only."""

from dataclasses import FrozenInstanceError
from contextlib import closing
import hashlib
import io
import json
from pathlib import Path
import sqlite3
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
import test_cache_inventory as fixtures
from hybrid_storage.cache_coverage import CacheCoverage, CoverageAPI, CoverageUnavailable, RankRequirements
from hybrid_storage.cache_inventory import CacheInventory
from hybrid_storage.dataset_manifests import DatasetEntry, MAX_INTEGER
from hybrid_storage.input_manifests import SourceFile, SourceManifest
from hybrid_storage.inputs import object_key
from hybrid_storage.inventory_reporter import InventoryReporter
from hybrid_storage.registry import StorageRegistry
from hybrid_storage.registry_schema import RegistryError
from hybrid_storage.worker_cache import WorkerCache
from hybrid_storage import cache_coverage as coverage_module
from hybrid_storage import inventory_protocol as p


class CoverageTests(unittest.TestCase):
    def setUp(self):
        self.fixture=fixtures.InventoryTests(methodName='runTest')
        self.addCleanup(self.fixture.doCleanups);self.fixture.setUp()
        self.storage=self.fixture.fixture
        self.registry=self.fixture.artifacts
        self.db=self.fixture.db
        self.owner=self.fixture.owner
        self.api=CoverageAPI(self.registry,self.owner)
        self.data=self.dataset([('first.bin',self.fixture.data),('second.bin',b'five!'),('alias.bin',self.fixture.data)])

    def dataset(self, entries, *, owner='alice', readers=()):
        result=[]
        for path,data in entries:
            artifact=self.registry.register_artifact(owner,io.BytesIO(data),expected_size=len(data))
            result.append(DatasetEntry(path,'file',artifact.sha256,artifact.size_bytes))
        return self.registry.create_dataset(owner,result,readers=readers)

    def query(self, worker='pi01', actor='alice', manifest=None):
        manifest=manifest or self.data
        return self.api.for_dataset(actor,worker,manifest.dataset_id,manifest.version)

    def bind(self, *, distributed=False, generated=False, owner='alice', new=False):
        identity,workspace,raw=self.storage.job_id,self.storage.workspace,self.storage.raw
        if distributed or generated or owner!='alice' or new:
            identity,workspace,raw=self.storage.job(owner=owner,generated=generated)
        if distributed:
            # New committed managed fixture, using the existing upload/spec path.
            document=json.loads((ROOT/'examples/job.single.json').read_text())
            document.update(project=str(self.storage.project),dataset=None if generated else str(self.storage.data))
            document['execution']={'mode':'distributed','workers':3}
            spec=fixtures.fixtures.validate_spec(document,self.storage.base)
            identity='coverage-ddp'
            fixtures.fixtures.store.add_job(identity,1,job_type='custom',owner=owner,status='RECEIVED',job_config=spec,storage_backend='nfs')
            receipt=fixtures.fixtures.job_storage.stage_job(identity);workspace=Path(receipt['storage_path']);raw=(workspace/'manifest.json').read_bytes()
        source=self.storage.source(self.registry,owner=owner,workspace=workspace)
        data=None if generated else self.storage.dataset(self.registry,owner=owner,workspace=workspace)
        binding=self.registry.bind_job(owner,identity,legacy_manifest_bytes=raw,source_sha256=source.sha256,
            dataset_id=None if data is None else data.dataset_id,dataset_version=None if data is None else data.version)
        return identity,binding,source,data

    def warm(self, source, dataset):
        shas={f.sha256 for f in source.files}
        if dataset:shas.update(e.sha256 for e in dataset.entries if e.type=='file')
        for sha in shas:
            raw=(self.storage.objects/object_key(sha)).read_bytes()
            self.fixture.install(raw)
        self.assertTrue(self.fixture.poll())

    def test_constructor_has_no_connections_migrations_or_filesystem_access(self):
        with patch.object(self.owner,'connect',side_effect=AssertionError('no DB')):
            self.assertIsInstance(CoverageAPI(self.registry,self.owner),CoverageAPI)
        with self.assertRaises(TypeError):CoverageAPI(None,self.owner)

    def test_duplicate_manifest_paths_count_digest_once_and_only_required_objects(self):
        self.assertTrue(self.fixture.poll())
        result=self.query()
        self.assertEqual((result.required_objects,result.required_bytes),(2,len(self.fixture.data)+5))
        self.assertEqual((result.verified_cached_bytes,result.missing_bytes),(len(self.fixture.data),5))
        self.assertEqual(result.missing_hashes,(hashlib.sha256(b'five!').hexdigest(),))
        self.assertAlmostEqual(result.locality_ratio,len(self.fixture.data)/(len(self.fixture.data)+5))
        self.assertEqual(result.inventory_age_ns,0)

    def test_warm_dataset_has_zero_missing_and_deterministic_serializable_result(self):
        self.fixture.install(b'five!');self.assertTrue(self.fixture.poll())
        result=self.query()
        self.assertEqual((result.missing_bytes,result.missing_hashes,result.locality_ratio),(0,(),1.0))
        self.assertEqual(result.document()['missing_hashes'],[]);json.dumps(result.document())
        with self.assertRaises(FrozenInstanceError):result.missing_bytes=1

    def test_unknown_configured_and_unconfigured_workers_are_all_misses(self):
        for worker in ('pi02','pi03','worker-unknown'):
            with self.subTest(worker=worker):
                result=self.query(worker)
                self.assertFalse(result.inventory_eligible);self.assertEqual(result.verified_cached_bytes,0)
                self.assertEqual(result.missing_bytes,result.required_bytes);self.assertIsNone(result.inventory_age_ns)

    def test_bad_worker_actor_version_and_path_inputs_are_rejected(self):
        for worker in ('../pi01','local',''):
            with self.subTest(worker=worker),self.assertRaises(ValueError):self.query(worker)
        for version in (None,True,0,'latest',MAX_INTEGER+1):
            with self.subTest(version=version),self.assertRaises(ValueError):
                self.api.for_dataset('alice','pi01',self.data.dataset_id,version)
        with self.assertRaises(ValueError):self.query(actor='../alice')

    def test_dataset_acl_denies_unknown_and_revoked_readers_before_inventory_lookup(self):
        with patch.object(self.api,'_view',side_effect=AssertionError('no inventory disclosure')):
            with self.assertRaises(PermissionError):self.query(actor='bob')
            with self.assertRaises(PermissionError):self.api.for_dataset('alice','pi01','ds_'+'a'*32,1)
        self.registry.set_readers('alice',self.data.dataset_id,['bob'])
        self.assertEqual(self.query(actor='bob').required_bytes,self.query().required_bytes)
        self.registry.set_readers('alice',self.data.dataset_id,[])
        with self.assertRaises(PermissionError):self.query(actor='bob')

    def test_head_update_does_not_change_exact_version_coverage(self):
        old=self.query()
        new=self.registry.update_dataset('alice',self.data.dataset_id,expected_head_sha256=self.data.sha256,entries=[])
        self.assertEqual(self.query().input_manifest_sha256,old.input_manifest_sha256)
        self.assertEqual(self.query().required_bytes,old.required_bytes)
        self.assertEqual(self.query(manifest=new).required_bytes,0)
        with self.assertRaises(RegistryError):self.api.for_dataset('alice','pi01',self.data.dataset_id,3)

    def test_stale_and_offline_inventory_cannot_contribute_verified_bytes(self):
        self.assertTrue(self.fixture.poll());self.fixture.clock.advance(16)
        result=self.query();self.assertFalse(result.inventory_eligible)
        self.assertEqual(result.inventory_age_ns,16*10**9);self.assertEqual(result.verified_cached_bytes,0)
        self.assertEqual(result.missing_bytes,result.required_bytes)
        self.assertTrue(self.fixture.poll());self.owner.offline('pi01')
        self.assertFalse(self.query().inventory_eligible);self.assertEqual(self.query().verified_cached_bytes,0)

    def test_inventory_age_tracks_full_delta_receipt_not_unchanged_heartbeat(self):
        self.assertTrue(self.fixture.poll());self.fixture.clock.advance(3)
        self.assertTrue(self.fixture.poll())
        self.assertEqual(self.query().inventory_age_ns,3_001_000_000)
        self.fixture.install(b'five!');self.assertTrue(self.fixture.poll())
        self.assertEqual(self.query().inventory_age_ns,0)

    def test_inventory_ttl_expires_even_if_worker_heartbeat_is_fresh(self):
        self.fixture.reporter.full_interval=300;self.owner.reconcile_ns=300*10**9
        self.assertTrue(self.fixture.poll())
        for _ in range(8):self.fixture.clock.advance(10);self.assertTrue(self.fixture.poll())
        self.fixture.clock.advance(11)
        # Direct clock advance expires TTL without asking heartbeat to reconcile.
        result=self.query();self.assertFalse(result.inventory_eligible);self.assertEqual(result.verified_cached_bytes,0)

    def test_corruption_removal_and_missing_inventory_entry_are_misses(self):
        self.assertTrue(self.fixture.poll())
        self.fixture.target.chmod(0o600);self.fixture.target.write_bytes(b'x'*len(self.fixture.data));self.fixture.target.chmod(0o400)
        self.assertTrue(self.fixture.poll());self.assertEqual(self.query().verified_cached_bytes,0)
        # Disposable registry fault; a verified filename alone never restores coverage.
        with self.db:self.db.execute('DELETE FROM cache_registry_entries')
        self.assertEqual(self.query().verified_cached_bytes,0)

    def test_closed_owner_and_failed_offline_write_ignore_historical_verified_rows(self):
        self.assertTrue(self.fixture.poll())
        with patch.object(self.owner,'connect',side_effect=sqlite3.OperationalError('disk full')):
            with self.assertRaises(sqlite3.OperationalError):self.owner.offline('pi01')
        self.assertEqual(self.query().verified_cached_bytes,0)
        self.assertTrue(self.fixture.poll());self.owner.close()
        self.assertEqual(self.query().verified_cached_bytes,0);self.assertEqual(self.query().inventory_status,'owner_unavailable')

    def test_future_clock_and_epoch_or_principal_revocation_fail_closed(self):
        self.assertTrue(self.fixture.poll());self.fixture.clock.now-=10**9
        result=self.query();self.assertFalse(result.inventory_eligible);self.assertIsNone(result.inventory_age_ns)
        self.fixture.clock.advance(2)
        with self.db:self.db.execute("UPDATE cache_registry_workers SET auth_state='REVOKED' WHERE worker_id='pi01'")
        self.assertEqual(self.query().verified_cached_bytes,0)

    def test_job_coverage_uses_source_requirements_and_exact_pinned_dataset(self):
        identity,binding,source,data=self.bind();self.warm(source,data)
        result=self.api.for_job('alice','pi01',identity)
        sizes={f.sha256:f.size_bytes for f in source.files}
        sizes.update({e.sha256:e.size_bytes for e in data.entries if e.type=='file'})
        self.assertEqual(result.required_bytes,sum(sizes.values()))
        self.assertEqual(result.required_objects,len(sizes));self.assertEqual(result.missing_bytes,0)
        self.assertEqual(result.input_manifest_sha256,binding.sha256)
        self.registry.update_dataset('alice',data.dataset_id,expected_head_sha256=data.sha256,entries=[])
        self.assertEqual(self.api.for_job('alice','pi01',identity).required_bytes,result.required_bytes)

    def test_legacy_unregistered_job_returns_explicit_unavailable_without_mutation(self):
        before=list(self.db.iterdump())
        self.assertIsNone(self.registry.get_job_inputs('alice',self.storage.job_id))
        with self.assertRaises(CoverageUnavailable):self.api.for_job('alice','pi01',self.storage.job_id)
        self.assertEqual(list(self.db.iterdump()),before)
        with self.assertRaises(PermissionError):self.api.for_job('bob','pi01',self.storage.job_id)

    def test_generated_job_uses_source_only_and_empty_rank_dataset_selection(self):
        identity,binding,source,data=self.bind(generated=True)
        result=self.api.for_job('alice','pi01',identity,rank=0,requirements=RankRequirements(0,1,binding.sha256,()))
        self.assertEqual(result.required_bytes,sum({f.sha256:f.size_bytes for f in source.files}.values()))
        self.assertIsNone(data)

    def test_ddp_defaults_each_rank_to_full_dataset_and_common_source(self):
        identity,binding,source,data=self.bind(distributed=True)
        whole=self.api.for_job('alice','pi01',identity)
        for rank in range(3):
            result=self.api.for_job('alice','pi01',identity,rank=rank)
            self.assertEqual(result.required_bytes,whole.required_bytes)
            self.assertEqual((result.rank,result.world_size),(rank,3))

    def test_explicit_rank_whole_file_sets_include_all_common_source_and_dedup(self):
        identity,binding,source,data=self.bind(distributed=True)
        data_files=[e for e in data.entries if e.type=='file'];selected=data_files[0]
        plan=RankRequirements(1,3,binding.sha256,(selected.sha256,selected.sha256))
        result=self.api.for_job('alice','pi01',identity,rank=1,requirements=plan)
        sizes={f.sha256:f.size_bytes for f in source.files};sizes[selected.sha256]=selected.size_bytes
        self.assertEqual(result.required_bytes,sum(sizes.values()));self.assertEqual(result.required_objects,len(sizes))
        self.assertEqual(plan.dataset_hashes,(selected.sha256,))
        empty=self.api.for_job('alice','pi01',identity,rank=2,requirements=RankRequirements(2,3,binding.sha256,()))
        self.assertEqual(empty.required_bytes,sum({f.sha256:f.size_bytes for f in source.files}.values()))

    def test_rank_declarations_reject_foreign_digest_wrong_binding_world_size_and_rank(self):
        identity,binding,source,data=self.bind(distributed=True)
        for plan in (RankRequirements(0,3,'f'*64,()),RankRequirements(0,2,binding.sha256,()),RankRequirements(1,3,binding.sha256,())):
            with self.subTest(plan=plan),self.assertRaises(ValueError):self.api.for_job('alice','pi01',identity,rank=0,requirements=plan)
        foreign=self.registry.register_artifact('bob',io.BytesIO(b'foreign'),expected_size=7)
        with self.assertRaises(PermissionError):
            self.api.for_job('alice','pi01',identity,rank=0,requirements=RankRequirements(0,3,binding.sha256,(foreign.sha256,)))
        for rank in (-1,3,True,'0'):
            with self.subTest(rank=rank),self.assertRaises(ValueError):self.api.for_job('alice','pi01',identity,rank=rank)
        with self.assertRaises(ValueError):self.api.for_job('alice','pi01',identity,requirements=RankRequirements(0,3,binding.sha256,()))

    def test_shared_job_dataset_acl_revocation_is_rechecked_even_with_cached_bytes(self):
        source=self.storage.source(self.registry)
        data=self.storage.dataset(self.registry,owner='bob',readers=['alice'])
        binding=self.registry.bind_job('alice',self.storage.job_id,legacy_manifest_bytes=self.storage.raw,
            source_sha256=source.sha256,dataset_id=data.dataset_id,dataset_version=data.version)
        self.warm(source,data);self.assertEqual(self.api.for_job('alice','pi01',binding.job_id).missing_bytes,0)
        self.registry.set_readers('bob',data.dataset_id,[])
        with self.assertRaises(PermissionError):self.api.for_job('alice','pi01',binding.job_id)

    def test_empty_and_zero_byte_artifacts_have_explicit_byte_ratio_semantics(self):
        empty=self.dataset([]);zero=self.dataset([('empty.bin',b'')])
        result=self.query(manifest=empty);self.assertEqual((result.required_bytes,result.locality_ratio),(0,0.0))
        self.assertTrue(self.fixture.poll())
        self.assertEqual(self.query(manifest=empty).locality_ratio,1.0)
        result=self.query(manifest=zero)
        self.assertEqual((result.required_bytes,result.verified_cached_bytes,result.missing_bytes,result.locality_ratio),(0,0,0,1.0))
        self.assertEqual(result.missing_hashes,(hashlib.sha256(b'').hexdigest(),))

    def test_queries_are_read_only_and_do_not_open_objects_or_trigger_reconciliation(self):
        self.assertTrue(self.fixture.poll());before=list(self.db.iterdump())
        with (patch.object(self.fixture.cache,'verify',side_effect=AssertionError('no worker I/O')),
             patch.object(self.registry.objects,'open_artifact',side_effect=AssertionError('no object I/O')),
             patch.object(self.owner,'connect',side_effect=AssertionError('reuse same read snapshot'))):
            self.query()
        self.assertEqual(list(self.db.iterdump()),before)

    def test_invalid_cache_namespace_is_all_misses_not_implicit_repair(self):
        self.assertTrue(self.fixture.poll())
        with self.db:self.db.execute('CREATE TABLE cache_registry_unrecognized (value TEXT)')
        result=self.query();self.assertEqual(result.inventory_status,'invalid_inventory')
        self.assertEqual(result.verified_cached_bytes,0)
        self.assertIsNotNone(self.db.execute("SELECT 1 FROM sqlite_master WHERE name='cache_registry_unrecognized'").fetchone())

    def test_manifest_authority_errors_are_not_hidden_as_empty_or_full_coverage(self):
        with patch.object(self.registry,'_load_manifest',side_effect=RegistryError('manifest mismatch')):
            with self.assertRaises(RegistryError):self.query()
        with patch.object(coverage_module,'_unique',return_value={'f'*64:1}):
            with self.assertRaises(RegistryError):self.query()

    def test_large_integer_byte_sum_uses_python_integer_and_overflow_is_rejected(self):
        # Pure arithmetic fixture only, not claimed as real published/cache bytes.
        files=(SourceFile('a','a'*64,MAX_INTEGER-1),SourceFile('b','b'*64,1))
        self.assertEqual(sum(coverage_module._unique(files).values()),MAX_INTEGER)
        with self.assertRaises(RegistryError):coverage_module._unique((*files,SourceFile('c','c'*64,1)))
        with self.assertRaises(RegistryError):coverage_module._unique((files[0],SourceFile('alias','a'*64,1)))

    def test_cross_database_registry_and_inventory_are_refused(self):
        other=fixtures.fixtures.RegistryTests(methodName='runTest')
        self.addCleanup(other.doCleanups);other.setUp()
        registry=other.registry();data=registry.create_dataset('alice',[])
        with self.assertRaises(RegistryError):CoverageAPI(registry,self.owner).for_dataset('alice','pi01',data.dataset_id,1)

    def test_only_exact_live_context_session_can_supply_cached_bytes(self):
        self.assertTrue(self.fixture.poll())
        columns=[row[1] for row in self.db.execute('PRAGMA table_info(cache_registry_sessions)')]
        values=list(self.db.execute("SELECT * FROM cache_registry_sessions WHERE state='ACTIVE'").fetchone())
        values[columns.index('session_id')]='f'*32
        with self.db:
            self.db.execute("UPDATE cache_registry_sessions SET state='STALE'")
            # Disposable inconsistent-controller-context fixture, not authenticated ingestion.
            self.db.execute('INSERT INTO cache_registry_sessions VALUES ('+','.join('?' for _ in values)+')',values)
        self.assertFalse(self.query().inventory_eligible);self.assertEqual(self.query().verified_cached_bytes,0)

    def test_source_and_dataset_same_digest_is_counted_once_in_job(self):
        content=(self.storage.project/'train.py').read_bytes()
        (self.storage.data/'a.txt').write_bytes(content)
        identity,binding,source,data=self.bind(new=True)
        result=self.api.for_job('alice','pi01',identity)
        sizes={f.sha256:f.size_bytes for f in source.files}
        sizes.update({e.sha256:e.size_bytes for e in data.entries if e.type=='file'})
        non_unique=sum(f.size_bytes for f in source.files)+sum(e.size_bytes for e in data.entries if e.type=='file')
        self.assertEqual(result.required_bytes,sum(sizes.values()));self.assertEqual(non_unique-result.required_bytes,len(content))

    def test_unrelated_verified_cached_objects_do_not_inflate_target_coverage(self):
        self.fixture.register(b'unrelated'*50);self.fixture.install(b'unrelated'*50)
        self.assertTrue(self.fixture.poll())
        self.assertEqual(self.query().verified_cached_bytes,len(self.fixture.data))

    def test_authorization_and_inventory_use_one_read_snapshot_during_acl_change(self):
        # WAL only on this disposable DB, to allow a second local writer while
        # the current authorized read snapshot finishes. Next query must deny.
        self.db.execute('PRAGMA journal_mode=WAL')
        self.registry.set_readers('alice',self.data.dataset_id,['bob'])
        original=self.registry._dataset
        def revoke_after_authorization(*args):
            result=original(*args)
            with closing(self.storage.connect()) as db:
                StorageRegistry(db,self.storage.publisher).set_readers('alice',self.data.dataset_id,[])
            return result
        with patch.object(self.registry,'_dataset',side_effect=revoke_after_authorization):
            result=self.query(actor='bob')
        self.assertEqual(result.required_bytes,len(self.fixture.data)+5)
        with self.assertRaises(PermissionError):self.query(actor='bob')

    def test_three_authenticated_workers_have_exact_0_2_502_byte_misses(self):
        # Small real local objects, intentionally BYTES, not the deferred MB acceptance fixture.
        data=self.dataset([('large.bin',b'A'*500),('small.bin',b'BB')])
        self.owner.close()
        keys=[self.fixture.key,self.fixture.key2,p.Credential('pi03','c'*32,b'3'*32)]
        owner=CacheInventory(self.storage.connect,keys,configured_workers=['pi01','pi02','pi03'],
            state_root=self.storage.base,clock=self.fixture.clock)
        owner.start();self.addCleanup(owner.close)
        reporters={}
        for index,key in enumerate(keys):
            owner.enroll(key.worker)
            if index==0:
                self.fixture.install(b'A'*500);self.fixture.install(b'BB');reporter=self.fixture.reporter
            else:
                root=self.storage.base/key.worker;root.mkdir(mode=0o700)
                cache=WorkerCache(root,input_mount=self.storage.base/'not-mounted');cache.initialize()
                if index==1:
                    sha=hashlib.sha256(b'A'*500).hexdigest();target=root/'objects'/object_key(sha)
                    target.parent.parent.mkdir(mode=0o700);target.parent.mkdir(mode=0o700)
                    target.write_bytes(b'A'*500);target.chmod(0o400);cache.verify(sha,500)
                reporter=InventoryReporter(cache,key,clock=self.fixture.clock,monotonic=self.fixture.clock.monotonic)
                reporter.initialize(boot_id='01234567-0123-4567-8901-012345678901');self.addCleanup(reporter.close)
            self.fixture.clock.advance();raw,headers=owner.challenge(key.worker)
            body,signature=reporter.reply(raw,headers['X-DL-Inventory-MAC'],headers['X-DL-Worker-Key'])
            owner.accept(key.worker,raw,body,signature);reporters[key.worker]=reporter
        api=CoverageAPI(self.registry,owner)
        results=[api.for_dataset('alice',key.worker,data.dataset_id,1) for key in keys]
        self.assertEqual([r.required_bytes for r in results],[502]*3)
        self.assertEqual([r.verified_cached_bytes for r in results],[502,500,0])
        self.assertEqual([r.missing_bytes for r in results],[0,2,502])
        owner.offline('pi01')
        result=api.for_dataset('alice','pi01',data.dataset_id,1)
        self.assertFalse(result.inventory_eligible);self.assertEqual(result.missing_bytes,502)


if __name__=='__main__':unittest.main(verbosity=2)
