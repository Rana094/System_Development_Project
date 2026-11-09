"""11.3 disposable dry-run/LRU/pin/crash/inventory checks. No live data or hosts."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import replace
import errno
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
import test_cache_fetch as fixtures
import test_cache_inventory as inventory_fixture
import test_private_workspaces as workspace_fixture
import test_staging_dispatch as staging_fixture
from agent.cache_storage import cache_maintenance
from hybrid_storage.cache_eviction import CacheEvictor, EvictionPlanStale
from hybrid_storage.cache_fetch import CacheFetcher, CacheFetchTimeout
from hybrid_storage.inputs import object_key
from hybrid_storage.worker_cache import CacheActivityBusy
from hybrid_storage import inventory_protocol as protocol


class EvictionTests(unittest.TestCase):
    setUp=fixtures.FetchTests.setUp
    fetcher_handle=fixtures.FetchTests.fetcher_handle
    source_file=fixtures.FetchTests.source_file
    fake_spawn=fixtures.FetchTests.fake_spawn
    ensure=fixtures.FetchTests.ensure
    fake_nfs=fixtures.FetchTests.fake_nfs

    def install(self,data,*,used=None):
        digest=hashlib.sha256(data).hexdigest();target=self.root/'objects'/object_key(digest)
        target.parent.parent.mkdir(mode=0o700,exist_ok=True);target.parent.mkdir(mode=0o700,exist_ok=True)
        target.write_bytes(data);target.chmod(0o400)
        self.cache.verify(digest,len(data))
        with self.cache._session() as (db,_,__):db.execute('UPDATE cache_objects SET last_used_ns=? WHERE sha256=?',(used,digest))
        return digest,target

    def collector(self,quota=1,**kw):
        return cache_maintenance(self.fetcher_handle(quota_bytes=quota),**kw)

    def hashes(self):
        return {str(p.relative_to(self.root)):hashlib.sha256(p.read_bytes()).hexdigest()
                for p in self.root.rglob('*') if p.is_file()}

    def test_constructor_no_io_and_rejects_bad_limits_and_protected_overlap(self):
        with patch('hybrid_storage.cache_eviction.require_local_fd',side_effect=AssertionError('no I/O')):
            gc=self.collector(protected_roots=(self.base/'workspaces',self.base/'checkpoints'))
        self.assertIsInstance(gc,CacheEvictor)
        for kwargs in ({'max_candidates':0},{'max_candidates':True},{'max_candidates':4097},
                       {'notify':object()},{'protected_roots':(self.root/'jobs',)},
                       {'protected_roots':(self.base,)},{'protected_roots':(Path('relative'),)}):
            with self.subTest(kwargs=kwargs),self.assertRaises((ValueError,TypeError)):self.collector(**kwargs)

    def test_plan_and_default_apply_are_readonly_and_LRU_is_deterministic(self):
        a,pa=self.install(b'a'*100,used=1);b,pb=self.install(b'b'*200,used=2);c,pc=self.install(b'c'*300,used=3)
        gc=self.collector(400);before=self.hashes()
        plan=gc.plan();self.assertEqual(plan.status,'ready')
        self.assertEqual([v.sha256 for v in plan.candidates],[a,b])
        self.assertEqual(plan.document()['selected_bytes'],300)
        self.assertTrue(gc.apply(plan)['dry_run']);self.assertEqual(self.hashes(),before)
        with self.cache._session(read_only=True) as (db,_,__):
            with self.assertRaises(sqlite3.OperationalError):db.execute('DELETE FROM cache_objects')
        result=gc.apply(plan,dry_run=False)
        self.assertEqual(result['status'],'satisfied');self.assertEqual(result['removed'],[a,b])
        self.assertFalse(pa.exists());self.assertFalse(pb.exists());self.assertTrue(pc.exists())
        states={r.sha256:r.state for r in self.cache.inventory()}
        self.assertEqual(states,{a:'missing',b:'missing',c:'verified'})
        with self.assertRaises(EvictionPlanStale):gc.apply(plan,dry_run=False)

    def test_no_pressure_no_deletion_and_ties_use_digest(self):
        first,_=self.install(b'first',used=1);second,_=self.install(b'second',used=1)
        gc=self.collector(1000);plan=gc.plan();self.assertEqual(plan.status,'not_needed')
        self.assertEqual(gc.apply(plan,dry_run=False)['removed'],[])
        gc=self.collector(1);plan=gc.plan()
        self.assertEqual([c.sha256 for c in plan.candidates],sorted((first,second)))

    def test_evicted_replica_restages_through_existing_authorized_NFS_flow(self):
        with self.fake_nfs():self.ensure()
        self.fetcher.quota_bytes=len(self.data)
        gc=cache_maintenance(self.fetcher)
        result=gc.apply(gc.plan(incoming_bytes=len(self.data)),dry_run=False)
        self.assertEqual(result['removed'],[self.sha]);self.assertFalse(self.target.exists())
        with self.fake_nfs() as spawned:
            self.assertFalse(self.ensure()[0].cache_hit)
        self.assertEqual(spawned.call_count,1)
        self.assertEqual(self.target.read_bytes(),self.data)
        self.assertEqual((self.nfs/object_key(self.sha)).read_bytes(),self.data)

    def test_every_pin_blocks_all_eviction_and_reference_counts_survive(self):
        digest,target=self.install(b'important',used=1);self.install(b'other',used=0)
        for attempt in ('first','second'):
            self.cache.pin(digest,9,job_id='job',attempt_id=attempt)
        gc=self.collector();plan=gc.plan()
        self.assertEqual((plan.status,plan.pinned_references,plan.pinned_objects),('blocked_pins',2,1))
        self.assertEqual(plan.candidates,())
        with self.assertRaises(EvictionPlanStale):gc.apply(plan,dry_run=False)
        self.cache.unpin(job_id='job',attempt_id='first')
        self.assertEqual(gc.plan().pinned_references,1)
        self.cache.unpin(job_id='job',attempt_id='second')
        gc.apply(gc.plan(),dry_run=False);self.assertFalse(target.exists())

    def test_pins_LRU_content_and_budget_changes_invalidate_reviewed_plan(self):
        for change in ('pin','use','inode','budget'):
            with self.subTest(change=change):
                data=('replica-'+change).encode();digest,target=self.install(data,used=1)
                gc=self.collector();plan=gc.plan()
                if change=='pin':self.cache.pin(digest,len(data),job_id='job',attempt_id='attempt')
                elif change=='use':
                    with self.cache.open_verified(digest,len(data)):pass
                elif change=='inode':
                    target.unlink();target.write_bytes(data);target.chmod(0o400)
                else:gc.fetcher.quota_bytes=2
                with self.assertRaises(EvictionPlanStale):gc.apply(plan,dry_run=False)
                self.assertTrue(target.exists())
                if change=='pin':self.cache.unpin(job_id='job',attempt_id='attempt')

    def test_forged_foreign_and_expired_in_memory_plans_are_never_applied(self):
        _,target=self.install(b'input');gc=self.collector();plan=gc.plan()
        for invalid in (replace(plan,used_bytes=0),plan.document()):
            with self.subTest(invalid=type(invalid)),self.assertRaises(EvictionPlanStale):gc.apply(invalid,dry_run=False)
        with self.assertRaises(EvictionPlanStale):self.collector().apply(plan,dry_run=False)
        for _ in range(8):gc.plan()
        with self.assertRaises(EvictionPlanStale):gc.apply(plan,dry_run=False)
        self.assertTrue(target.exists())

    def test_open_readers_and_staging_activity_block_cross_handle_eviction(self):
        digest,_=self.install(b'input');gc=self.collector()
        with self.cache.open_verified(digest,5),self.assertRaises(CacheActivityBusy):gc.plan()
        with self.cache.activity(),ThreadPoolExecutor(max_workers=1) as pool:
            future=pool.submit(gc.plan)
            with self.assertRaises(CacheActivityBusy):future.result(timeout=5)
        self.assertEqual(gc.plan().status,'ready')

    def test_same_filesystem_object_replacement_and_unsafe_entries_fail_closed(self):
        digest,target=self.install(b'retain');gc=self.collector()
        plan=gc.plan();target.chmod(0o600)
        with self.assertRaises(PermissionError):gc.apply(plan,dry_run=False)
        target.chmod(0o400)
        # inode metadata changed, so unverified content is not an LRU candidate.
        self.assertEqual(gc.plan().status,'insufficient_candidates')
        self.cache.verify(digest,6)
        target.unlink();target.symlink_to(self.nfs/object_key(self.sha))
        with self.assertRaises(PermissionError):gc.plan()
        self.assertTrue(target.is_symlink())

    def test_unknown_files_parts_quarantine_outputs_and_authoritative_files_retained(self):
        _,target=self.install(b'cache'*10)
        retained=[self.root/'objects'/'unregistered.bin',self.root/'parts'/'previous.part',
                  self.root/'quarantine'/'evidence',self.base/'checkpoints'/'latest.pt']
        for path in retained:
            path.parent.mkdir(mode=0o700,exist_ok=True);path.write_bytes(b'retain');path.chmod(0o600)
        authority=(self.nfs/object_key(self.sha)).read_bytes()
        gc=self.collector(30);result=gc.apply(gc.plan(),dry_run=False)
        self.assertFalse(target.exists());self.assertEqual(result['status'],'satisfied')
        self.assertTrue(all(path.read_bytes()==b'retain' for path in retained))
        self.assertEqual((self.nfs/object_key(self.sha)).read_bytes(),authority)
        blocked=self.collector(1);plan=blocked.plan()
        self.assertEqual(plan.status,'insufficient_candidates')
        with self.assertRaises(EvictionPlanStale):blocked.apply(plan,dry_run=False)

    def test_disk_reserve_and_incoming_bytes_are_explicit_and_actual_space_rechecked(self):
        _,target=self.install(b'x'*100)
        gc=self.collector(1000);gc.fetcher.reserve_free_bytes=100
        with patch.object(gc,'_available',return_value=50):plan=gc.plan(incoming_bytes=10)
        self.assertEqual(plan.status,'ready')
        with patch.object(gc,'_available',return_value=50):result=gc.apply(plan,dry_run=False)
        self.assertFalse(target.exists());self.assertEqual(result['status'],'insufficient_actual_space')
        self.assertEqual(result['available_bytes'],50)
        for incoming in (-1,True,2**63):
            with self.subTest(incoming=incoming),self.assertRaises(ValueError):gc.plan(incoming_bytes=incoming)

    def test_candidate_limit_refuses_partial_unreviewed_or_insufficient_eviction(self):
        _,first=self.install(b'one',used=1);_,second=self.install(b'two',used=2)
        gc=self.collector(max_candidates=1);plan=gc.plan()
        self.assertEqual(plan.status,'insufficient_candidates')
        with self.assertRaises(EvictionPlanStale):gc.apply(plan,dry_run=False)
        self.assertTrue(first.exists() and second.exists())

    def test_metadata_disk_full_prevents_unlink_and_unlink_failure_preserves_replica(self):
        digest,target=self.install(b'input');gc=self.collector();plan=gc.plan()
        original=self.cache._session
        @contextmanager
        def full(**kw):
            if not kw.get('read_only'):raise sqlite3.OperationalError('database or disk is full')
            with original(**kw) as value:yield value
        with patch.object(self.cache,'_session',full),self.assertRaises(sqlite3.OperationalError):gc.apply(plan,dry_run=False)
        self.assertTrue(target.exists());self.assertEqual(self.cache.inventory()[0].state,'verified')
        plan=gc.plan()
        with patch.object(gc,'_unlink',side_effect=PermissionError(errno.EACCES,'fixture')),self.assertRaises(PermissionError):gc.apply(plan,dry_run=False)
        self.assertEqual(self.cache.inventory()[0].state,'missing');self.assertTrue(target.exists())
        result=gc.reconcile();self.assertEqual(result['verified'],1)
        self.assertEqual(self.cache.inventory()[0].state,'verified')

    def test_real_process_crashes_before_and_after_unlink_reconcile_without_replay(self):
        program="""
import os,sys
from pathlib import Path
sys.path.insert(0,sys.argv[1])
from hybrid_storage.worker_cache import WorkerCache
from hybrid_storage.cache_fetch import CacheFetcher
from hybrid_storage.cache_eviction import CacheEvictor
cache=WorkerCache(Path(sys.argv[2]),input_mount=Path(sys.argv[3]))
fetch=CacheFetcher(cache,source=sys.argv[4],quota_bytes=1,reserve_free_bytes=0)
gc=CacheEvictor(fetch)
plan=gc.plan()
def crash(prefix,digest):
    if sys.argv[5]=='after_unlink':os.unlink(digest,dir_fd=prefix)
    os._exit(17)
gc._unlink=crash
gc.apply(plan,dry_run=False)
"""
        for when in ('before_unlink','after_unlink'):
            with self.subTest(when=when):
                digest,target=self.install(b'crash-'+when.encode(),used=0)
                result=subprocess.run([sys.executable,'-B','-c',program,str(ROOT),str(self.root),str(self.nfs),fixtures.SOURCE,when],
                                      capture_output=True,text=True,timeout=10)
                self.assertEqual(result.returncode,17,result.stderr)
                gc=self.collector();result=gc.reconcile()
                row={r.sha256:r for r in self.cache.inventory()}[digest]
                self.assertEqual(row.state,'verified' if when=='before_unlink' else 'missing')
                self.assertEqual(target.exists(),when=='before_unlink')
                # Keep the retained first sample out of the next crash victim set.
                if target.exists():
                    with self.cache._session() as (db,_,__):db.execute('UPDATE cache_objects SET last_used_ns=? WHERE sha256=?',(2**62,digest))
                self.assertEqual(result['pins_before'],result['pins_after'])

    def test_notification_failure_cannot_undo_or_hide_completed_eviction(self):
        _,target=self.install(b'input')
        def broken():raise OSError('notify offline')
        gc=self.collector(notify=broken);result=gc.apply(gc.plan(),dry_run=False)
        self.assertFalse(target.exists());self.assertEqual(result['status'],'satisfied')
        self.assertEqual(result['inventory_refresh'],'notification_failed_periodic_reconciliation_required')

    def test_nested_mount_admission_is_closed_before_unlink(self):
        _,target=self.install(b'input');gc=self.collector()
        with patch.object(gc,'_same_mount',side_effect=PermissionError('nested mount')),self.assertRaises(PermissionError):gc.plan()
        self.assertTrue(target.exists())

    def test_linked_object_is_not_deleted_and_unreaped_producer_retains_slot(self):
        _,target=self.install(b'input');linked=self.base/'retained-link';os.link(target,linked)
        gc=self.collector()
        with self.assertRaises(PermissionError):gc.plan()
        self.assertEqual(linked.read_bytes(),b'input');linked.unlink()
        self.cache.verify(hashlib.sha256(b'input').hexdigest(),5)
        class Blocked:
            pid=999999
            returncode=None
            def wait(self,timeout):raise subprocess.TimeoutExpired('fixture',timeout)
            def poll(self):return self.returncode
            def kill(self):pass
        blocked=Blocked()
        with patch.object(self.fetcher,'_spawn',return_value=blocked),self.assertRaises(CacheFetchTimeout):self.ensure()
        quick=cache_maintenance(self.fetcher_handle(quota_bytes=1,lock_timeout=.02))
        with self.assertRaises(CacheActivityBusy):quick.plan()
        self.assertTrue(list((self.root/'parts').glob('*.part')))
        blocked.returncode=0;self.fetcher.reconcile()
        self.assertEqual(quick.plan().status,'ready')

    def test_root_replacement_invalidates_plan_without_touching_relocated_bytes(self):
        _,target=self.install(b'input');gc=self.collector();plan=gc.plan()
        original=self.root;relocated=self.base/'retained-cache';original.rename(relocated)
        original.mkdir(mode=0o700);self.fetcher.initialize()
        with self.assertRaises(EvictionPlanStale):gc.apply(plan,dry_run=False)
        self.assertEqual((relocated/target.relative_to(original)).read_bytes(),b'input')


class WorkspacePinTests(unittest.TestCase):
    def setUp(self):
        self.f=workspace_fixture.PrivateWorkspaceTests(methodName='runTest');self.addCleanup(self.f.doCleanups);self.f.setUp()
        fetch=CacheFetcher(self.f.cache,source=fixtures.SOURCE,quota_bytes=1,reserve_free_bytes=0)
        fetch.initialize();self.gc=cache_maintenance(fetch,protected_roots=(self.f.root,))

    def test_ready_running_and_unproven_terminal_attempt_keep_all_objects(self):
        self.f.prepare();self.assertEqual(self.gc.plan().status,'blocked_pins')
        self.f.manager.mark_started(self.f.plan,authorize=lambda _:True)
        self.assertEqual(self.gc.plan().status,'blocked_pins')
        self.f.finish(outcome='LEASE_EXPIRED',proof=False)
        self.assertEqual(self.gc.plan().status,'blocked_pins')
        self.f.finish(outcome='LEASE_EXPIRED',proof=True)
        self.assertEqual(self.gc.plan().pinned_references,0)
        result=self.gc.apply(self.gc.plan(),dry_run=False)
        self.assertTrue(result['removed'])
        self.assertTrue((self.f.root/'attempts'/self.f.plan.identity.key/'inputs/source/train.py').exists())

    def test_reconciliation_retains_pins_even_if_pinned_object_is_missing(self):
        self.f.prepare();target=self.f.cache_root/'objects'/object_key(self.f.rows_sha)
        target.unlink()  # Disposable external-loss fixture, never GC of a pinned input.
        result=self.gc.reconcile()
        self.assertGreater(result['pins_before'],0);self.assertEqual(result['pins_before'],result['pins_after'])
        self.assertEqual(result['missing'],1);self.assertEqual(self.gc.plan().status,'blocked_pins')


class RegistryAndStagingTests(unittest.TestCase):
    def test_unnotified_eviction_uses_existing_incremental_removed_event(self):
        f=inventory_fixture.InventoryTests(methodName='runTest');self.addCleanup(f.doCleanups);f.setUp()
        self.assertTrue(f.poll())
        fetch=CacheFetcher(f.cache,source=fixtures.SOURCE,quota_bytes=1,reserve_free_bytes=0);fetch.initialize()
        gc=cache_maintenance(fetch);result=gc.apply(gc.plan(),dry_run=False)
        self.assertEqual(result['inventory_refresh'],'periodic_inventory_reconciliation')
        self.assertTrue(f.poll());self.assertEqual(f.entries(),[(f.artifact.sha256,'REMOVED',0)])
        kind=f.db.execute('SELECT kind FROM cache_registry_events ORDER BY sequence DESC LIMIT 1').fetchone()[0]
        self.assertEqual(kind,'CACHE_REMOVED')

    def test_eviction_and_restart_reconciliation_update_real_signed_registry(self):
        f=inventory_fixture.InventoryTests(methodName='runTest');self.addCleanup(f.doCleanups);f.setUp()
        self.assertTrue(f.poll());authority=(f.fixture.objects/f.artifact.key).read_bytes()
        fetch=CacheFetcher(f.cache,source=fixtures.SOURCE,quota_bytes=1,reserve_free_bytes=0);fetch.initialize()
        gc=cache_maintenance(fetch,notify=f.reporter.request_full)
        result=gc.apply(gc.plan(),dry_run=False)
        self.assertEqual(result['inventory_refresh'],'full_inventory_requested')
        self.assertTrue(f.poll());self.assertEqual(f.entries(),[(f.artifact.sha256,'REMOVED',0)])
        self.assertEqual((f.fixture.objects/f.artifact.key).read_bytes(),authority)
        gc.reconcile();self.assertTrue(f.poll());self.assertEqual(f.entries(),[(f.artifact.sha256,'REMOVED',0)])

    def test_actual_signed_stage_protects_fetch_to_pin_gap_and_running_state(self):
        f=staging_fixture.DispatchTests(methodName='runTest');self.addCleanup(f.doCleanups);f.setUp()
        maintenance_fetch=CacheFetcher(f.cache,source=fixtures.SOURCE,quota_bytes=1,reserve_free_bytes=0)
        gc=cache_maintenance(maintenance_fetch);original=f.fetcher.ensure;observed=[]
        def ensure(*args,**kw):
            result=original(*args,**kw)
            with self.assertRaises(CacheActivityBusy):gc.plan()
            observed.append('gap_protected');return result
        with patch.object(f.fetcher,'ensure',side_effect=ensure):self.assertTrue(f.dispatch())
        self.assertEqual(observed,['gap_protected']);self.assertEqual(gc.plan().status,'blocked_pins')


if __name__=='__main__':unittest.main(verbosity=2)
