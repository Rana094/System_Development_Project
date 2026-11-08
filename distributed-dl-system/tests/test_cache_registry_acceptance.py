"""6.4 registry acceptance: real local files/processes, no live DB/Pi/network.

The exact MB case writes about 1.504 GB under a TemporaryDirectory. MB means
1,000,000 bytes; no sparse files, synthetic presence or measured bandwidth claim.
"""

from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import threading
import time
import unittest
from unittest.mock import Mock, patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
import test_cache_coverage as fixtures
from hybrid_storage.cache_coverage import CoverageAPI
from hybrid_storage.cache_inventory import CacheInventory
from hybrid_storage.dataset_manifests import DatasetEntry
from hybrid_storage.inputs import object_key
from hybrid_storage.inventory_reporter import InventoryReporter
from hybrid_storage.worker_cache import WorkerCache
from hybrid_storage import inventory_protocol as p

MB=1_000_000
BOOT='01234567-0123-4567-8901-012345678901'


class RepeatedStream:
    """Bounded source, consumed by the real streaming publication API."""
    def __init__(self, count, value):
        self.remaining=count;self.chunk=value*(1024*1024)
    def read(self, size=-1):
        size=min(self.remaining,len(self.chunk),size if size>=0 else len(self.chunk))
        self.remaining-=size
        return self.chunk[:size]


class RegistryAcceptanceTests(unittest.TestCase):
    def setUp(self):
        self.fixture=fixtures.CoverageTests(methodName='runTest')
        self.addCleanup(self.fixture.doCleanups);self.fixture.setUp()
        self.worker=self.fixture.fixture;self.storage=self.fixture.storage
        self.db,self.owner,self.api=self.fixture.db,self.fixture.owner,self.fixture.api

    def coverage(self):return self.fixture.query()

    def poll(self):
        self.assertTrue(self.worker.poll())
        return self.coverage()

    def connect(self, factory=sqlite3.Connection, timeout=3):
        db=sqlite3.connect(self.storage.db_path,factory=factory,timeout=timeout)
        db.execute('PRAGMA foreign_keys=ON');db.execute('PRAGMA synchronous=FULL')
        return db

    def child(self, code, *args, expected=0):
        result=subprocess.run([sys.executable,'-I','-B','-c',code,str(ROOT),*map(str,args)],
                              capture_output=True,text=True,timeout=20)
        self.assertEqual(result.returncode,expected,result.stderr)
        return result

    def test_initial_empty_full_then_incremental_add_remove_and_reconcile(self):
        self.worker.target.unlink()  # Only this disposable fixture's replica.
        cold=self.poll();self.assertTrue(cold.inventory_eligible)
        self.assertEqual(cold.verified_cached_bytes,0)
        self.worker.install(self.worker.data);warm=self.poll()
        self.assertEqual(warm.verified_cached_bytes,len(self.worker.data))
        self.worker.target.unlink();removed=self.poll()
        self.assertEqual(removed.verified_cached_bytes,0)
        kinds=[r[0] for r in self.db.execute('SELECT kind FROM cache_registry_events ORDER BY sequence')]
        self.assertEqual(kinds,['FULL','CACHE_ADDED','CACHE_REMOVED'])
        self.worker.clock.advance(11);self.poll()
        self.assertEqual(self.db.execute('SELECT kind FROM cache_registry_events ORDER BY sequence DESC LIMIT 1').fetchone()[0],'FULL')

    def test_corrupt_replica_loses_coverage_and_repair_readds_it(self):
        self.poll();self.worker.target.chmod(0o600)
        self.worker.target.write_bytes(b'X'*len(self.worker.data));self.worker.target.chmod(0o400)
        result=self.poll();self.assertEqual(result.verified_cached_bytes,0)
        self.assertEqual(self.worker.cache.inventory()[0].state,'corrupt')
        # Disposable controlled repair; production repair remains CacheFetcher.
        self.worker.target.chmod(0o600);self.worker.target.write_bytes(self.worker.data);self.worker.target.chmod(0o400)
        result=self.poll();self.assertEqual(result.verified_cached_bytes,len(self.worker.data))

    def test_stale_heartbeat_and_inventory_deadline_never_make_old_hits_eligible(self):
        self.poll();self.worker.clock.advance(16)
        self.assertFalse(self.coverage().inventory_eligible)
        self.assertEqual(self.coverage().missing_bytes,self.coverage().required_bytes)
        self.assertTrue(self.worker.heartbeat.reconcile())
        self.assertEqual(self.db.execute('SELECT state FROM cache_registry_sessions').fetchone()[0],'STALE')
        self.poll();self.owner.reconcile_ns=300*10**9;self.worker.reporter.full_interval=300
        for _ in range(8):self.worker.clock.advance(10);self.poll()
        self.worker.clock.advance(11)
        self.assertFalse(self.coverage().inventory_eligible)
        self.assertEqual(self.coverage().verified_cached_bytes,0)

    def test_timeout_and_known_offline_transition_require_new_full_snapshot(self):
        before=self.poll()
        self.worker.heartbeat.transport=Mock(side_effect=OSError('disposable network interruption'))
        self.assertFalse(self.worker.poll());self.assertFalse(self.coverage().inventory_eligible)
        self.worker.heartbeat.transport=self.worker.transport
        after=self.poll();self.assertNotEqual(after.inventory_session_id,before.inventory_session_id)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM cache_registry_events WHERE kind='FULL'").fetchone()[0],2)

    def test_removed_or_missing_controller_observation_is_a_miss_and_full_repairs(self):
        before=self.poll()
        with self.db:self.db.execute('DELETE FROM cache_registry_entries')
        result=self.coverage();self.assertTrue(result.inventory_eligible)
        self.assertEqual(result.verified_cached_bytes,0)
        self.worker.clock.advance(11);after=self.poll()
        self.assertEqual(after.verified_cached_bytes,before.verified_cached_bytes)
        self.assertEqual(after.inventory_session_id,before.inventory_session_id)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM cache_registry_events').fetchone()[0],2)

    def test_duplicate_paths_and_unchanged_reports_never_double_count_or_extend_inventory_ttl(self):
        self.worker.install(b'five!');before=self.poll()
        self.assertEqual(before.required_objects,2)
        self.assertEqual(before.required_bytes,len(self.worker.data)+5)
        self.worker.clock.advance(2);self.poll()
        request,body,signature=self.worker.exchange()
        self.owner.accept('pi01',request,body,signature)
        row=tuple(self.db.execute("SELECT last_sequence,last_seen_ns,fresh_until_ns FROM cache_registry_sessions WHERE state='ACTIVE'").fetchone())
        for _ in range(3):self.assertEqual(self.owner.accept('pi01',request,body,signature),'duplicate')
        self.assertEqual(tuple(self.db.execute("SELECT last_sequence,last_seen_ns,fresh_until_ns FROM cache_registry_sessions WHERE state='ACTIVE'").fetchone()),row)
        self.assertEqual(self.coverage().verified_cached_bytes,before.required_bytes)

    def test_late_full_and_signed_sequence_jump_cannot_revive_replaced_session(self):
        old_request,old_body,old_mac=self.worker.exchange()
        self.owner.offline('pi01');fresh=self.poll()
        with self.assertRaises(p.AuthenticationError):self.owner.accept('pi01',old_request,old_body,old_mac)
        request,body,_=self.worker.exchange();changed=p.decode(body);changed['sequence']+=5
        raw=p.encode(changed)
        with self.assertRaises(p.InventoryError):
            self.owner.accept('pi01',request,raw,p.mac(self.worker.key,'report',raw,request=request))
        self.assertEqual(self.coverage().inventory_session_id,fresh.inventory_session_id)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM cache_registry_events').fetchone()[0],1)

    def test_inflight_response_after_offline_notification_cannot_restore_hits(self):
        self.poll();request,body,signature=self.worker.exchange()
        self.owner.offline('pi01')
        with self.assertRaises(p.AuthenticationError):self.owner.accept('pi01',request,body,signature)
        self.assertFalse(self.coverage().inventory_eligible)
        self.poll()

    def test_expired_execution_lease_cannot_be_replaced_by_good_cache_coverage(self):
        self.poll();ownership=fixtures.fixtures.agent.ownership
        now=time.time();root=self.storage.base/'lease-worker'
        lease=dict(node_id='pi01',generation=1,lease_id='e'*32,job_id='expired-job',
                   attempt_token='expired-job:0',issued_at=now,expires_at=now+1)
        ownership.accept(root,lease,'expired-job','expired-job:0','pi01')
        with patch.object(ownership.time,'time',return_value=now+2):
            with self.assertRaisesRegex(ValueError,'expired'):
                ownership.assert_owned(root,'expired-job','expired-job:0',1,'e'*32)
            # Persistent input presence is separate from permission to execute.
            self.assertTrue(self.coverage().inventory_eligible)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM resource_reservations').fetchone()[0],0)

    def test_worker_process_abort_preserves_cache_epoch_and_requires_new_full(self):
        before=self.poll();old_epoch=self.worker.reporter.epoch;cache_id=self.worker.reporter.cache_id
        self.worker.reporter.close();self.owner.offline('pi01')
        self.worker.clock.advance();request,headers=self.owner.challenge('pi01')
        request_path=self.storage.base/'challenge.bin';request_path.write_bytes(request)
        output=self.storage.base/'child-report.json'
        code='''import sys,os,json
from pathlib import Path
sys.path.insert(0,sys.argv[1])
from hybrid_storage.worker_cache import WorkerCache
from hybrid_storage.inventory_reporter import InventoryReporter
from hybrid_storage.inventory_protocol import Credential,challenge,mac
raw=Path(sys.argv[4]).read_bytes();value=challenge(raw)
cache=WorkerCache(Path(sys.argv[2]),input_mount=Path(sys.argv[3]))
key=Credential('pi01','a'*32,b'1'*32)
reporter=InventoryReporter(cache,key,clock=lambda:value['sent_at_ns'])
reporter.initialize(boot_id='11234567-0123-4567-8901-012345678901')
body,signature=reporter.reply(raw,mac(key,'challenge',raw),key.key_id)
with Path(sys.argv[5]).open('w') as stream:
 json.dump({'body':body.decode(),'signature':signature},stream);stream.flush();os.fsync(stream.fileno())
os._exit(17)
'''
        self.child(code,self.worker.cache_root,self.worker.cache.input_mount,request_path,output,expected=17)
        result=json.loads(output.read_text());body=result['body'].encode();report=p.report(body)
        self.assertEqual((report['cache_id'],report['cache_epoch']),(cache_id,old_epoch+1))
        self.assertEqual(self.owner.accept('pi01',request,body,result['signature']),'FULL')
        after=self.coverage();self.assertNotEqual(after.inventory_session_id,before.inventory_session_id)
        self.assertEqual(after.verified_cached_bytes,before.verified_cached_bytes)
        self.assertEqual(self.worker.target.read_bytes(),self.worker.data)
        # Abrupt exit released the OS lock; the next process epoch advances.
        handle=self.worker.reporter_handle();handle.initialize(boot_id=BOOT);self.addCleanup(handle.close)
        self.assertEqual(handle.epoch,old_epoch+2)

    def test_crashed_sqlite_writer_cannot_publish_partial_removed_inventory(self):
        before=self.poll();dump=list(self.db.iterdump())
        code='''import sys,sqlite3,os
db=sqlite3.connect(sys.argv[2]);db.execute('PRAGMA foreign_keys=ON');db.execute('PRAGMA synchronous=FULL')
db.execute('BEGIN IMMEDIATE')
db.execute("UPDATE cache_registry_entries SET state='REMOVED',verified_bytes=0")
db.execute("UPDATE cache_registry_sessions SET state='STALE' WHERE state='ACTIVE'")
os._exit(23)
'''
        self.child(code,self.storage.db_path,expected=23)
        self.assertEqual(list(self.db.iterdump()),dump)
        self.assertEqual(self.coverage().verified_cached_bytes,before.verified_cached_bytes)
        self.assertEqual(self.db.execute('PRAGMA integrity_check').fetchall(),[('ok',)])
        self.assertEqual(self.db.execute('PRAGMA foreign_key_check').fetchall(),[])

    def test_disk_full_during_second_entry_rolls_back_cursor_tombstones_and_history(self):
        self.poll();self.worker.install(b'five!');self.owner.reconcile_ns=10**9;self.worker.clock.advance(2)
        request,body,signature=self.worker.exchange();self.assertEqual(p.report(body)['kind'],'FULL')
        before=list(self.db.iterdump())
        class Interrupted(sqlite3.Connection):
            writes=0
            def execute(inner,sql,*args,**kwargs):
                if sql.lstrip().startswith('INSERT INTO cache_registry_entries'):
                    inner.writes+=1
                    if inner.writes==2:raise sqlite3.OperationalError('database or disk is full')
                return super().execute(sql,*args,**kwargs)
        with patch.object(self.owner,'connect',side_effect=lambda:self.connect(Interrupted)):
            with self.assertRaises(sqlite3.OperationalError):self.owner.accept('pi01',request,body,signature)
        self.assertEqual(list(self.db.iterdump()),before)
        self.assertEqual(self.owner.accept('pi01',request,body,signature),'FULL')
        self.assertEqual(self.coverage().missing_bytes,0)

    def test_actual_sqlite_busy_failure_invalidates_live_admission_and_reconnect_recovers(self):
        self.poll();blocked=self.connect();blocked.execute('BEGIN IMMEDIATE')
        try:
            with patch.object(self.owner,'connect',side_effect=lambda:self.connect(timeout=0.02)):
                self.assertFalse(self.worker.poll())
            self.assertFalse(self.coverage().inventory_eligible)
        finally:blocked.rollback();blocked.close()
        self.poll()
        self.assertEqual(self.db.execute('PRAGMA foreign_key_check').fetchall(),[])

    def test_independent_process_sqlite_writer_and_inventory_commit_serialize(self):
        with self.db:self.db.execute('CREATE TABLE acceptance_writes (value INTEGER PRIMARY KEY)')
        request,body,signature=self.worker.exchange();entered=threading.Event()
        code='''import sqlite3,sys
db=sqlite3.connect(sys.argv[1]);db.execute('PRAGMA synchronous=FULL');db.execute('BEGIN IMMEDIATE')
db.execute('INSERT INTO acceptance_writes VALUES(7)');print('LOCKED',flush=True)
assert sys.stdin.readline().strip()=='release'
db.commit();db.close()
'''
        process=subprocess.Popen([sys.executable,'-I','-B','-c',code,str(self.storage.db_path)],
            stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
        try:
            # Child announces its reserved SQLite write lock, not just process start.
            import select
            self.assertTrue(select.select([process.stdout],[],[],5)[0])
            self.assertEqual(process.stdout.readline().strip(),'LOCKED')
            class Contended(sqlite3.Connection):
                def execute(inner,sql,*args,**kwargs):
                    if sql=='BEGIN IMMEDIATE':entered.set()
                    return super().execute(sql,*args,**kwargs)
            with patch.object(self.owner,'connect',side_effect=lambda:self.connect(Contended)),ThreadPoolExecutor(max_workers=1) as pool:
                future=pool.submit(self.owner.accept,'pi01',request,body,signature)
                self.assertTrue(entered.wait(3));self.assertFalse(future.done())
                process.stdin.write('release\n');process.stdin.flush()
                self.assertEqual(future.result(timeout=5),'FULL')
            self.assertEqual(process.wait(timeout=5),0,process.stderr.read())
        finally:
            if process.poll() is None:process.kill();process.wait(timeout=5)
            for stream in (process.stdin,process.stdout,process.stderr):stream.close()
        self.assertEqual(self.db.execute('SELECT value FROM acceptance_writes').fetchall(),[(7,)])
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM cache_registry_events').fetchone()[0],1)
        self.assertTrue(self.coverage().inventory_eligible)

    def test_parallel_worker_reports_and_duplicate_replays_keep_unique_events(self):
        # Worker two has a real independently verified local cache and credential.
        root=self.storage.base/'pi02-cache';root.mkdir(mode=0o700)
        cache=WorkerCache(root,input_mount=self.storage.base/'no-nfs');cache.initialize()
        reporter=InventoryReporter(cache,self.worker.key2,clock=self.worker.clock)
        reporter.initialize(boot_id=BOOT);self.addCleanup(reporter.close)
        messages=[]
        for key,handle in ((self.worker.key,self.worker.reporter),(self.worker.key2,reporter)):
            self.worker.clock.advance();request,headers=self.owner.challenge(key.worker)
            body,signature=handle.reply(request,headers['X-DL-Inventory-MAC'],headers['X-DL-Worker-Key'])
            messages.append((key.worker,request,body,signature))
        with ThreadPoolExecutor(max_workers=6) as pool:
            results=list(pool.map(lambda m:self.owner.accept(*m),messages*3))
        self.assertEqual(results.count('FULL'),2);self.assertEqual(results.count('duplicate'),4)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM cache_registry_events').fetchone()[0],2)
        self.assertEqual(self.db.execute('PRAGMA foreign_key_check').fetchall(),[])

    def test_exact_decimal_MB_fixture_with_actual_objects_on_all_three_local_workers(self):
        needed=1504*MB+128*1024*1024
        if shutil.disk_usage(self.storage.base).free<needed:
            self.skipTest('Exact MB fixture needs at least 1.638 GB free local temporary disk; no scaled substitute')
        artifacts=[]
        for size,value in ((500*MB,b'A'),(2*MB,b'B')):
            stream=RepeatedStream(size,value)
            artifact=self.fixture.registry.register_artifact('alice',stream,expected_size=size)
            self.assertEqual(stream.remaining,0);artifacts.append(artifact)
        manifest=self.fixture.registry.create_dataset('alice',[
            DatasetEntry('large.bin','file',artifacts[0].sha256,500*MB),
            DatasetEntry('alias-large.bin','file',artifacts[0].sha256,500*MB),
            DatasetEntry('small.bin','file',artifacts[1].sha256,2*MB)])
        self.owner.close()
        keys=[self.worker.key,self.worker.key2,p.Credential('pi03','c'*32,b'3'*32)]
        owner=CacheInventory(self.storage.connect,keys,configured_workers=['pi01','pi02','pi03'],
                             state_root=self.storage.base,clock=self.worker.clock)
        owner.start();self.addCleanup(owner.close)
        api=CoverageAPI(self.fixture.registry,owner);caches={}
        for index,key in enumerate(keys):
            owner.enroll(key.worker);root=self.storage.base/(key.worker+'-MB-cache');root.mkdir(mode=0o700)
            cache=WorkerCache(root,input_mount=self.storage.base/'unused-nfs');cache.initialize();caches[key.worker]=cache
            for artifact in artifacts[:2-index] if index<2 else ():
                source=self.storage.objects/artifact.key;target=root/'objects'/artifact.key
                target.parent.parent.mkdir(mode=0o700,exist_ok=True);target.parent.mkdir(mode=0o700,exist_ok=True)
                # Separate real private cache inode: no sparse/truncate/hardlink fixture.
                with source.open('rb') as src,target.open('xb') as dst:
                    shutil.copyfileobj(src,dst,length=1024*1024);dst.flush();os.fsync(dst.fileno())
                target.chmod(0o400)
                self.assertNotEqual(source.stat().st_ino,target.stat().st_ino)
                cache.verify(artifact.sha256,artifact.size_bytes,force=True)
            reporter=InventoryReporter(cache,key,clock=self.worker.clock)
            reporter.initialize(boot_id=BOOT);self.addCleanup(reporter.close)
            self.worker.clock.advance();request,headers=owner.challenge(key.worker)
            body,signature=reporter.reply(request,headers['X-DL-Inventory-MAC'],headers['X-DL-Worker-Key'])
            self.assertEqual(owner.accept(key.worker,request,body,signature),'FULL')
        results=[api.for_dataset('alice',key.worker,manifest.dataset_id,1) for key in keys]
        self.assertEqual([r.required_objects for r in results],[2]*3)
        self.assertEqual([r.required_bytes for r in results],[502*MB]*3)
        self.assertEqual([r.verified_cached_bytes for r in results],[502*MB,500*MB,0])
        self.assertEqual([r.missing_bytes for r in results],[0,2*MB,502*MB])
        self.assertEqual([r.inventory_eligible for r in results],[True]*3)
        self.assertEqual([len(r.missing_hashes) for r in results],[0,1,2])
        self.assertEqual(caches['pi01'].space().observed_verified_bytes,502*MB)
        owner.offline('pi01');self.assertFalse(api.for_dataset('alice','pi01',manifest.dataset_id,1).inventory_eligible)
        self.worker.clock.advance(16)
        for worker in ('pi02','pi03'):
            result=api.for_dataset('alice',worker,manifest.dataset_id,1)
            self.assertFalse(result.inventory_eligible);self.assertEqual(result.verified_cached_bytes,0)
        print('EXACT_MB_FIXTURE '+json.dumps(dict(MB=MB,required_bytes=502*MB,
            cached_bytes=[r.verified_cached_bytes for r in results],missing_bytes=[r.missing_bytes for r in results],
            objects=[dict(sha256=a.sha256,bytes=a.size_bytes) for a in artifacts],
            workers=[key.worker for key in keys],real_local_objects=True,real_Pis=False)),flush=True)


if __name__=='__main__':unittest.main(verbosity=2)
