"""6.2 local real-Flask + disposable cache/registry authentication/reconciliation."""

from concurrent.futures import ThreadPoolExecutor
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import sqlite3
import sys
import time
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import test_storage_registry as fixtures
from hybrid_storage import cache_registry_schema as schema
from hybrid_storage import inventory_protocol as p
from hybrid_storage.cache_inventory import CacheInventory
from hybrid_storage.inventory_reporter import InventoryReporter
from hybrid_storage.worker_cache import WorkerCache
from hybrid_storage.inputs import object_key
from hybrid_storage.config import StorageConfig
from agent.cache_storage import install_inventory_reporter

# Existing fixtures deliberately use copied-controller modules; load this adapter
# by its exact path so no unrelated top-level controller module is shadowed.
def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    result = importlib.util.module_from_spec(spec); spec.loader.exec_module(result)
    return result

heartbeat_module = module('inventory_heartbeat', ROOT/'controller/cache_inventory.py')
agent = module('inventory_test_agent', ROOT/'agent/agent.py')
dashboard = module('inventory_dashboard', ROOT/'controller/controller.py')
cluster_setup = module('inventory_operator_setup', ROOT/'tools/cluster_setup.py')


class Clock:
    def __init__(self): self.now = time.time_ns()
    def __call__(self): return self.now
    def monotonic(self): return self.now / 10**9
    def advance(self, seconds=0.001): self.now += int(seconds*10**9)


class InventoryTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.RegistryTests(methodName='runTest')
        self.addCleanup(self.fixture.doCleanups); self.fixture.setUp()
        self.db = self.fixture.db
        self.artifacts = self.fixture.registry()
        schema.migrate(self.db)
        self.clock = Clock()
        self.key = p.Credential('pi01', 'a'*32, b'1'*32)
        self.key2 = p.Credential('pi02', 'b'*32, b'2'*32)
        self.owner = self.owner_handle()
        self.owner.start(); self.addCleanup(self.owner.close)
        self.owner.enroll('pi01'); self.owner.enroll('pi02')
        self.cache_root = self.fixture.base/'cache'; self.cache_root.mkdir(mode=0o700)
        self.cache = WorkerCache(self.cache_root, input_mount=self.fixture.base/'not-mounted')
        self.cache.initialize()
        self.data = b'authorized fixture input'
        self.artifact = self.register(self.data)
        self.target = self.install(self.data)
        self.reporter = self.reporter_handle()
        self.reporter.initialize(boot_id='01234567-0123-4567-8901-012345678901')
        self.addCleanup(self.reporter.close)
        agent.app.extensions.pop('cache_inventory', None)
        install_inventory_reporter(agent.app, self.reporter)
        self.addCleanup(lambda: agent.app.extensions.pop('cache_inventory', None))
        identity = patch.dict(os.environ, DL_NODE_ID='pi01'); identity.start(); self.addCleanup(identity.stop)
        self.client = agent.app.test_client()
        self.node = dict(id='pi01', ip='192.0.2.1', port=5000)
        self.settings = StorageConfig('nfs', True, True, 'resource')
        self.heartbeat = heartbeat_module.InventoryHeartbeat(self.owner, settings=self.settings, transport=self.transport)
        self.addCleanup(self.heartbeat.close)

    def owner_handle(self, **options):
        return CacheInventory(self.fixture.connect, [self.key,self.key2], configured_workers=['pi01','pi02','pi03'],
                              state_root=self.fixture.base, clock=self.clock, **options)

    def reporter_handle(self):
        return InventoryReporter(self.cache, self.key, clock=self.clock, monotonic=self.clock.monotonic,
                                 full_interval_seconds=10)

    def register(self, data):
        return self.artifacts.register_artifact('alice', io.BytesIO(data), expected_size=len(data))

    def install(self, data):
        digest = hashlib.sha256(data).hexdigest()
        target = self.cache_root/'objects'/object_key(digest)
        target.parent.parent.mkdir(mode=0o700,exist_ok=True); target.parent.mkdir(mode=0o700,exist_ok=True)
        target.write_bytes(data); target.chmod(0o400)
        self.cache.verify(digest,len(data))
        return target

    def transport(self, node, raw, headers):
        response = self.client.post(p.PATH, data=raw, headers=headers)
        if response.status_code != 200:
            raise p.InventoryError('Agent HTTP %s' % response.status_code)
        return response.data, response.headers['X-DL-Inventory-MAC']

    def poll(self):
        self.clock.advance()
        return self.heartbeat.poll(self.node)

    def exchange(self):
        self.clock.advance(); raw, headers = self.owner.challenge('pi01')
        body, signature = self.transport(self.node,raw,headers)
        return raw,body,signature

    def row(self):
        return self.db.execute("SELECT last_sequence,inventory_updated_at_ns,fresh_until_ns,last_seen_ns,state FROM cache_registry_sessions WHERE state='ACTIVE'").fetchone()

    def entries(self):
        return [tuple(r) for r in self.db.execute('''SELECT e.sha256,e.state,e.verified_bytes FROM cache_registry_entries e
            JOIN cache_registry_sessions s USING(session_id) WHERE s.state='ACTIVE' ORDER BY e.sha256''')]

    def test_real_agent_full_inventory_and_signed_heartbeat(self):
        self.assertTrue(self.poll())
        self.assertEqual(self.entries(),[(self.artifact.sha256,'VERIFIED',len(self.data))])
        before=tuple(self.row()); self.assertTrue(self.poll()); after=tuple(self.row())
        self.assertEqual(after[:3],before[:3]); self.assertGreater(after[3],before[3])
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM cache_registry_events').fetchone()[0],1)

    def test_actual_agent_default_off_and_never_lease_or_student_authorized(self):
        self.assertEqual(self.client.post(p.PATH,json={},headers={'X-DL-Worker-Key':self.key.key_id}).status_code,401)
        self.assertEqual(self.client.post(p.PATH,json={},headers={'X-CSRF-Token':'student','X-Attempt-Token':'lease'}).status_code,401)
        agent.app.extensions.pop('cache_inventory')
        self.assertEqual(self.client.post(p.PATH,json={}).status_code,404)
        with self.assertRaises(p.InventoryError):
            heartbeat_module.InventoryHeartbeat(self.owner,settings=StorageConfig())

    def test_forged_signature_wrong_key_and_unicode_signature_rejected_before_scan(self):
        raw,headers=self.owner.challenge('pi01')
        for signature in ('0'*64,'é'*64,''):
            with self.subTest(signature=signature),patch.object(self.reporter,'_scan',side_effect=AssertionError('no scan')):
                self.assertEqual(self.client.post(p.PATH,data=raw,headers=dict(headers,**{'X-DL-Inventory-MAC':signature})).status_code,401)
        headers['X-DL-Worker-Key']=self.key2.key_id
        self.assertEqual(self.client.post(p.PATH,data=raw,headers=headers).status_code,401)
        self.assertFalse(self.owner.eligible('pi01'))

    def test_worker_spoof_and_body_tamper_cannot_update_registry(self):
        request,body,signature=self.exchange()
        value=p.decode(body); value['worker_id']='pi02'; changed=p.encode(value)
        for signed in (signature,p.mac(self.key,'report',changed,request=request)):
            with self.subTest(signature=signed),self.assertRaises(p.AuthenticationError):
                self.owner.accept('pi01',request,changed,signed)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM cache_registry_sessions').fetchone()[0],0)

    def test_duplicate_response_is_idempotent_and_does_not_refresh_ttl_or_last_seen(self):
        request,body,signature=self.exchange()
        self.assertEqual(self.owner.accept('pi01',request,body,signature),'FULL')
        before=tuple(self.row()); self.clock.advance(2)
        self.assertEqual(self.owner.accept('pi01',request,body,signature),'duplicate')
        self.assertEqual(tuple(self.row()),before)

    def test_old_response_wrong_nonce_and_out_of_order_sequence_are_rejected(self):
        old,body,signature=self.exchange(); self.owner.accept('pi01',old,body,signature)
        new,newbody,newsig=self.exchange()
        with self.assertRaises(p.AuthenticationError): self.owner.accept('pi01',old,body,signature)
        with self.assertRaises(p.AuthenticationError): self.owner.accept('pi01',new,body,signature)
        value=p.decode(newbody); value['sequence']+=3
        changed=p.encode(value)
        with self.assertRaises(p.InventoryError):
            self.owner.accept('pi01',new,changed,p.mac(self.key,'report',changed,request=new))
        self.assertEqual(self.owner.accept('pi01',new,newbody,newsig),'HEARTBEAT')

    def test_incremental_added_removed_events_and_unknown_file_not_advertised(self):
        self.assertTrue(self.poll())
        data=b'second object'; artifact=self.register(data); target=self.install(data)
        self.assertTrue(self.poll())
        self.assertEqual(self.db.execute('SELECT kind FROM cache_registry_events ORDER BY sequence DESC LIMIT 1').fetchone()[0],'CACHE_ADDED')
        target.unlink()  # Disposable fixture deletion, never runtime GC.
        self.assertTrue(self.poll())
        self.assertIn((artifact.sha256,'REMOVED',0),self.entries())
        unknown=self.cache_root/'objects'/object_key('f'*64)
        unknown.parent.mkdir(mode=0o700,exist_ok=True); unknown.write_bytes(b'not enrolled');unknown.chmod(0o400)
        self.assertTrue(self.poll()); self.assertNotIn('f'*64,[r[0] for r in self.entries()])

    def test_corrupt_or_deleted_known_object_immediately_removed(self):
        self.assertTrue(self.poll())
        self.target.chmod(0o600); self.target.write_bytes(b'X'*len(self.data));self.target.chmod(0o400)
        self.assertTrue(self.poll()); self.assertEqual(self.entries(),[(self.artifact.sha256,'REMOVED',0)])
        self.assertEqual(self.cache.inventory()[0].state,'corrupt')

    def test_reconnect_after_http_failure_requires_full_and_fences_old_session(self):
        self.assertTrue(self.poll()); old=self.db.execute("SELECT session_id FROM cache_registry_sessions WHERE state='ACTIVE'").fetchone()[0]
        self.heartbeat.transport=Mock(side_effect=OSError('network lost'))
        self.assertFalse(self.poll());self.assertFalse(self.owner.eligible('pi01'))
        self.heartbeat.transport=self.transport;self.assertTrue(self.poll())
        self.assertEqual(self.db.execute('SELECT state FROM cache_registry_sessions WHERE session_id=?',(old,)).fetchone()[0],'STALE')
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM cache_registry_events WHERE kind='FULL'").fetchone()[0],2)

    def test_worker_restart_advances_durable_epoch_and_needs_full_snapshot(self):
        self.assertTrue(self.poll()); cache_id,epoch=self.reporter.cache_id,self.reporter.epoch
        self.reporter.close();self.reporter=self.reporter_handle();self.reporter.initialize(boot_id='11234567-0123-4567-8901-012345678901');self.addCleanup(self.reporter.close)
        agent.app.extensions['cache_inventory']=self.reporter
        self.assertEqual((self.reporter.cache_id,self.reporter.epoch),(cache_id,epoch+1))
        self.assertFalse(self.poll());self.assertFalse(self.owner.eligible('pi01'))
        self.assertTrue(self.poll());self.assertEqual(self.db.execute('SELECT epoch_high_water FROM cache_registry_identities').fetchone()[0],epoch+1)

    def test_controller_restart_invalidates_inventory_until_new_full_handshake(self):
        self.assertTrue(self.poll());epoch=self.owner.controller_epoch
        self.owner.close();self.owner=self.owner_handle();self.owner.start();self.addCleanup(self.owner.close)
        self.assertNotEqual(self.owner.controller_epoch,epoch);self.assertFalse(self.owner.eligible('pi01'))
        self.heartbeat.registry=self.owner;self.assertTrue(self.poll())

    def test_stale_heartbeat_cannot_make_offline_inventory_eligible(self):
        self.assertTrue(self.poll());self.clock.advance(16)
        self.assertFalse(self.owner.eligible('pi01'));self.heartbeat.reconcile()
        self.assertEqual(self.db.execute('SELECT state FROM cache_registry_sessions').fetchone()[0],'STALE')
        self.assertTrue(self.poll())

    def test_full_reconciliation_verifies_bytes_and_refreshes_inventory_not_plain_heartbeat(self):
        self.assertTrue(self.poll());before=tuple(self.row())
        self.clock.advance(11);self.assertTrue(self.poll());after=tuple(self.row())
        self.assertEqual(after[0],before[0]+1);self.assertGreater(after[1],before[1])
        self.assertEqual(self.db.execute('SELECT kind FROM cache_registry_events ORDER BY sequence DESC LIMIT 1').fetchone()[0],'FULL')

    def test_inventory_expiry_not_extended_by_duplicate_or_ordinary_last_seen(self):
        self.assertTrue(self.poll())
        # Long full interval deliberately exceeds TTL to exercise expiry.
        self.reporter.full_interval=300
        self.owner.reconcile_ns=300*10**9  # Deliberately suppress controller full probes in this expiry fixture.
        for _ in range(8):
            self.clock.advance(10);self.assertTrue(self.poll())
        self.clock.advance(11)
        self.assertFalse(self.owner.eligible('pi01'))
        self.assertTrue(self.poll())  # A new session/full, not revival of stale observations.

    def test_revocation_blocks_authenticated_but_disabled_worker(self):
        self.assertTrue(self.poll())
        with self.db:self.db.execute("UPDATE cache_registry_workers SET auth_state='REVOKED' WHERE worker_id='pi01'")
        self.assertFalse(self.poll());self.assertFalse(self.owner.eligible('pi01'))

    def test_not_operator_enrolled_cannot_auto_enroll_from_signed_report(self):
        with self.db:self.db.execute("UPDATE cache_registry_workers SET enabled=0 WHERE worker_id='pi01'")
        self.assertFalse(self.poll());self.assertEqual(self.db.execute('SELECT COUNT(*) FROM cache_registry_identities').fetchone()[0],0)

    def test_missing_controller_artifact_rejects_full_atomically(self):
        self.install(b'not in controller registry')
        self.assertFalse(self.poll());self.assertEqual(self.db.execute('SELECT COUNT(*) FROM cache_registry_sessions').fetchone()[0],0)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM cache_registry_entries').fetchone()[0],0)

    def test_future_or_conflicting_signed_payload_rejected_without_partial_apply(self):
        request,body,signature=self.exchange(); value=p.decode(body)
        for changes in ({'version':2},{'sequence':True},{'disk':dict(total=1,free=2,available=1)},
                        {'entries':[value['entries'][0]]*2}):
            changed=p.encode(dict(value,**changes))
            with self.subTest(changes=changes),self.assertRaises(p.InventoryError):
                self.owner.accept('pi01',request,changed,p.mac(self.key,'report',changed,request=request))
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM cache_registry_sessions').fetchone()[0],0)

    def test_response_deadline_and_old_authenticated_challenge_expire(self):
        request,body,signature=self.exchange();self.clock.advance(11)
        with self.assertRaises(p.AuthenticationError):self.owner.accept('pi01',request,body,signature)
        self.clock.advance(21)
        self.assertEqual(self.client.post(p.PATH,data=request,headers={'X-DL-Worker-Key':self.key.key_id,
            'X-DL-Inventory-MAC':p.mac(self.key,'challenge',request)}).status_code,401)

    def test_exact_challenge_retry_does_not_scan_again_and_lost_response_reuses_body(self):
        request,body,signature=self.exchange()
        with patch.object(self.reporter,'_scan',side_effect=AssertionError('no rehash')):
            self.assertEqual(self.transport(self.node,request,dict(self.owner.challenge('pi02')[1],
                **{'X-DL-Worker-Key':self.key.key_id,'X-DL-Inventory-MAC':p.mac(self.key,'challenge',request)})),(body,signature))
            self.clock.advance();retry,headers=self.owner.challenge('pi01')
            body2,sig2=self.transport(self.node,retry,headers)
        self.assertEqual(body2,body);self.assertNotEqual(sig2,signature)
        self.assertEqual(self.owner.accept('pi01',retry,body2,sig2),'FULL')

    def test_single_reporter_and_controller_owner_no_concurrent_split_brain(self):
        second=self.reporter_handle()
        with self.assertRaises(BlockingIOError):second.initialize(boot_id=self.reporter.boot_id)
        second_owner=self.owner_handle()
        with self.assertRaises(BlockingIOError):second_owner.start()

    def test_stale_cache_epoch_or_boot_change_cannot_reuse_old_verified_inventory(self):
        self.assertTrue(self.poll())
        with self.db:
            self.db.execute('UPDATE cache_registry_identities SET epoch_high_water=epoch_high_water+1')
        self.assertFalse(self.poll());self.assertFalse(self.owner.eligible('pi01'))
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM cache_registry_entries').fetchone()[0],1)

    def test_worker_cache_identity_cannot_impersonate_another_enrolled_worker(self):
        self.assertTrue(self.poll())
        self.clock.advance();request,_=self.owner.challenge('pi02');value=p.challenge(request)
        body=p.decode(self.reporter._last_raw)
        body.update(worker_id='pi02',session_id=value['session_id'],controller_epoch=value['controller_epoch'],
                    base_sequence=0,sequence=1,kind='FULL')
        raw=p.encode(body)
        with self.assertRaises(p.AuthenticationError):
            self.owner.accept('pi02',request,raw,p.mac(self.key2,'report',raw,request=request))
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM cache_registry_identities').fetchone()[0],1)

    def test_concurrent_duplicate_authenticated_writes_commit_one_event(self):
        request,body,signature=self.exchange()
        with ThreadPoolExecutor(max_workers=4) as pool:
            results=list(pool.map(lambda _:self.owner.accept('pi01',request,body,signature),range(4)))
        self.assertEqual(results.count('FULL'),1);self.assertEqual(results.count('duplicate'),3)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM cache_registry_events').fetchone()[0],1)

    def test_private_identity_corruption_is_refused_without_overwriting_or_advancing_epoch(self):
        self.reporter.close();path=self.cache_root/'state/inventory-identity.json'
        path.write_bytes(b'{invalid');before=path.read_bytes()
        handle=self.reporter_handle()
        with self.assertRaises(p.InventoryError):handle.initialize(boot_id=self.reporter.boot_id)
        self.assertEqual(path.read_bytes(),before)

    def test_unreadable_registry_is_ineligible_and_does_not_fall_back_to_advertised_files(self):
        self.assertTrue(self.poll())
        with patch.object(self.owner,'connect',side_effect=sqlite3.OperationalError('database unavailable')):
            self.assertFalse(self.owner.eligible('pi01'));self.assertFalse(self.poll())

    def test_readonly_sqlite_error_keeps_previous_committed_inventory_unchanged(self):
        request,body,signature=self.exchange()
        original=self.owner.connect
        def readonly():
            db=original();db.execute('PRAGMA query_only=ON');return db
        with patch.object(self.owner,'connect',side_effect=readonly),self.assertRaises(sqlite3.OperationalError):
            self.owner.accept('pi01',request,body,signature)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM cache_registry_sessions').fetchone()[0],0)
        self.assertEqual(self.owner.accept('pi01',request,body,signature),'FULL')

    def test_key_rotation_requires_operator_binding_and_old_key_no_longer_counts(self):
        self.assertTrue(self.poll());self.owner.close()
        rotated=p.Credential('pi01','c'*32,b'3'*32)
        self.owner=CacheInventory(self.fixture.connect,[rotated,self.key2],configured_workers=['pi01','pi02'],
            state_root=self.fixture.base,clock=self.clock)
        self.owner.start();self.addCleanup(self.owner.close);self.owner.enroll('pi01')
        self.heartbeat.registry=self.owner;self.assertFalse(self.poll());self.assertFalse(self.owner.eligible('pi01'))
        self.reporter.credential=rotated
        self.assertTrue(self.poll())

    def test_controller_forces_periodic_full_even_if_worker_period_is_longer(self):
        self.reporter.full_interval=300
        self.assertTrue(self.poll())
        for _ in range(2):
            self.clock.advance(11);self.assertTrue(self.poll())
        before=tuple(self.row());self.clock.advance(11)
        request,body,signature=self.exchange()
        self.assertTrue(p.challenge(request)['full']);self.assertEqual(p.report(body)['kind'],'FULL')
        self.owner.accept('pi01',request,body,signature)
        self.assertEqual(self.row()[0],before[0]+1)

    def test_actual_operator_registration_hook_enrolls_only_preconfigured_identity(self):
        self.assertTrue(self.poll())
        path=self.fixture.base/'nodes.json'
        path.write_text(json.dumps(dict(nodes=[self.node,dict(id='pi03',ip='192.0.2.3',port=5000)])))
        cluster_setup.register(self.node,path=path,cache_inventory=self.owner)
        self.assertFalse(self.owner.eligible('pi01'))
        self.assertEqual(len(json.loads(path.read_text())['nodes']),2)
        with self.assertRaises(p.AuthenticationError):
            cluster_setup.register(dict(id='pi03',ip='192.0.2.3',port=5000),path=path,cache_inventory=self.owner)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM cache_registry_workers WHERE worker_id='pi03'").fetchone()[0],0)

    def test_default_http_adapter_limits_response_disables_redirects_and_sends_signed_headers(self):
        monitor=heartbeat_module.InventoryHeartbeat(self.owner,settings=self.settings)
        self.addCleanup(monitor.close)
        response=Mock(status_code=200,headers={'X-DL-Inventory-MAC':'signed'})
        response.iter_content.return_value=iter([b'first',b'second'])
        monitor.session.post=Mock(return_value=Mock(__enter__=Mock(return_value=response),__exit__=Mock(return_value=False)))
        raw,headers=self.owner.challenge('pi01')
        self.assertEqual(monitor._post(self.node,raw,headers),(b'firstsecond','signed'))
        self.assertFalse(monitor.session.trust_env)
        kwargs=monitor.session.post.call_args.kwargs
        self.assertFalse(kwargs['allow_redirects']);self.assertEqual(kwargs['timeout'],(1,3))
        self.assertEqual(kwargs['headers'],headers)
        response.status_code=302
        with self.assertRaises(p.InventoryError):monitor._post(self.node,raw,headers)
        response.status_code=200;response.iter_content.return_value=iter([b'x'*(p.MAX_BYTES+1)])
        with self.assertRaises(p.InventoryError):monitor._post(self.node,raw,headers)

    def test_failed_inventory_auth_keeps_resource_dashboard_output_and_excludes_cache(self):
        self.heartbeat.transport=Mock(return_value=(b'{}','forged'))
        dashboard.node_states['pi01']={'status':'UNKNOWN','failed_heartbeats':0,'last_seen':None}
        with patch.object(dashboard.requests,'get',return_value=Mock(json=lambda:{'node':'pi01'})):
            self.assertEqual(dashboard.check_node(self.node,cache_inventory=self.heartbeat),('pi01',{'node':'pi01'}))
        self.assertFalse(self.owner.eligible('pi01'))

    def test_periodic_full_repairs_missing_inventory_row_without_dropping_history(self):
        self.assertTrue(self.poll())
        with self.db:self.db.execute('DELETE FROM cache_registry_entries')  # Disposable fault fixture.
        self.clock.advance(11);self.assertTrue(self.poll())
        self.assertEqual(self.entries(),[(self.artifact.sha256,'VERIFIED',len(self.data))])
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM cache_registry_events').fetchone()[0],2)

    def test_failed_offline_write_cannot_leave_previously_active_cache_eligible(self):
        self.assertTrue(self.poll())
        with patch.object(self.owner,'connect',side_effect=sqlite3.OperationalError('disk full')):
            self.assertFalse(self.heartbeat.offline('pi01'))
        # Old DB row still exists, but the live owner no longer admits it.
        self.assertEqual(self.db.execute('SELECT state FROM cache_registry_sessions').fetchone()[0],'ACTIVE')
        self.assertFalse(self.owner.eligible('pi01'))
        self.assertTrue(self.poll())

    def test_actual_dashboard_heartbeat_extension_preserves_legacy_resources(self):
        node_id=self.node['id'];dashboard.node_states[node_id]={'status':'UNKNOWN','failed_heartbeats':0,'last_seen':None}
        with patch.object(dashboard.requests,'get',return_value=Mock(json=lambda:{'node':node_id})):
            identity,resources=dashboard.check_node(self.node,cache_inventory=self.heartbeat)
        self.assertEqual((identity,resources),(node_id,{'node':node_id}));self.assertTrue(self.owner.eligible(node_id))
        import requests
        with patch.object(dashboard.requests,'get',side_effect=requests.Timeout):
            dashboard.check_node(self.node,cache_inventory=self.heartbeat)
        self.assertFalse(self.owner.eligible(node_id))

    def test_bounded_request_and_inventory_fail_without_partial_snapshot(self):
        self.assertEqual(self.client.post(p.PATH,data=b'x'*(p.MAX_BYTES+1)).status_code,413)
        with patch.object(p,'MAX_ENTRIES',0):self.assertFalse(self.poll())
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM cache_registry_entries').fetchone()[0],0)

    def test_duplicate_worker_credentials_and_unconfigured_identity_rejected(self):
        for keys in ([self.key,self.key], [self.key,p.Credential('pi02',self.key.key_id,b'3'*32)],
                     [self.key,p.Credential('pi02','c'*32,self.key.secret)]):
            with self.subTest(keys=keys),self.assertRaises(p.InventoryError):
                CacheInventory(self.fixture.connect,keys,configured_workers=['pi01','pi02'],state_root=self.fixture.base)
        with self.assertRaises(p.InventoryError):
            CacheInventory(self.fixture.connect,[self.key],configured_workers=['pi02'],state_root=self.fixture.base)

    def test_private_local_credential_load_no_secrets_in_repr_and_unsafe_paths_rejected(self):
        path=self.fixture.base/'credential.json'; path.write_bytes(p.encode(dict(worker_id='pi01',key_id=self.key.key_id,secret_hex=self.key.secret.hex())));path.chmod(0o600)
        self.assertEqual(p.load_credential(path,expected_worker='pi01'),self.key)
        self.assertNotIn(self.key.secret.hex(),repr(self.key));self.assertNotIn(repr(self.key.secret),repr(self.key))
        path.chmod(0o644)
        with self.assertRaises(PermissionError):p.load_credential(path,expected_worker='pi01')
        path.chmod(0o600);link=self.fixture.base/'link';link.symlink_to(path)
        with self.assertRaises(OSError):p.load_credential(link,expected_worker='pi01')
        with self.assertRaises(p.AuthenticationError):p.load_credential(path,expected_worker='pi02')


if __name__ == '__main__':
    unittest.main(verbosity=2)
