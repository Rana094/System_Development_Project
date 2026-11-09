"""12.2 isolated admin/registry/signed-health/UI integration; no live hardware."""
import io
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
import test_staging_dispatch  # Establish agent package before script path fixtures.
import test_cache_inventory as inventory
import test_student_portal as students
import test_staging_portal as staged
from hybrid_storage import admin_observations as observations
from hybrid_storage import inventory_protocol as protocol
from hybrid_storage.dataset_manifests import DatasetEntry
import portal_admin
import admin_storage_view
import student_portal as portal


class AdminTests(unittest.TestCase):
    def fixture(self):
        f=students.PortalTests(methodName='runTest');f.setUp();self.addCleanup(f.tearDown);return f

    def test_no_implicit_admin_and_server_checks_before_reading_any_storage_data(self):
        f=self.fixture()
        with patch.object(admin_storage_view,'read',side_effect=AssertionError('Do not read for students')):
            self.assertEqual(f.app.test_client().get('/api/admin/storage').status_code,401)
            for owner in ('alice','bob'):
                f.login(owner)
                self.assertFalse(f.client.get('/api/session').json['is_admin'])
                with f.client.session_transaction() as session:session['is_admin']=True
                self.assertEqual(f.client.get('/api/admin/storage').status_code,403)
        portal.add_user(f.auth,'admin','admin-password')
        self.assertFalse(portal_admin.is_admin(f.auth,'admin'))

    def test_grant_existing_user_revoke_immediately_and_no_public_grant_route(self):
        f=self.fixture()
        with self.assertRaises(ValueError):portal_admin.set_admin(f.auth,'missing',enabled=True)
        portal_admin.set_admin(f.auth,'alice',enabled=True)
        self.assertTrue(f.client.get('/api/session').json['is_admin'])
        result=f.client.get('/api/admin/storage')
        self.assertEqual(result.status_code,200,result.json)
        self.assertEqual([w['worker_id'] for w in result.json['workers']],['pi01','pi02','pi03'])
        self.assertTrue(all(w['nfs']['status']=='unknown' for w in result.json['workers']))
        self.assertTrue(all(w['dataset_coverage'] is None for w in result.json['workers']))
        self.assertEqual(f.client.post('/api/admin/storage',json={},headers={'X-CSRF-Token':f.csrf}).status_code,405)
        portal_admin.set_admin(f.auth,'alice',enabled=False)
        self.assertEqual(f.client.get('/api/admin/storage').status_code,403)
        self.assertFalse(f.client.get('/api/session').json['is_admin'])

    def test_insecure_malformed_and_symlinked_grants_fail_closed(self):
        f=self.fixture();path=f.auth/'admins.json'
        portal_admin.set_admin(f.auth,'alice',enabled=True);path.chmod(0o644)
        self.assertEqual(f.client.get('/api/admin/storage').status_code,403)
        path.chmod(0o600);path.write_text('{"alice":true}')
        self.assertEqual(f.client.get('/api/admin/storage').status_code,403)
        path.unlink();path.symlink_to(f.auth/'users.json')
        self.assertEqual(f.client.get('/api/admin/storage').status_code,403)

    def test_readonly_legacy_poll_no_migration_no_remote_calls_no_detail_escalation(self):
        f=self.fixture();job=f.submit();portal_admin.set_admin(f.auth,'bob',enabled=True);f.login('bob')
        before=(f.root/'jobs.db').read_bytes()
        with patch.object(portal.requests,'get',side_effect=AssertionError('No Pi request')):
            for _ in range(3):self.assertEqual(f.client.get('/api/admin/storage').status_code,200)
        self.assertEqual((f.root/'jobs.db').read_bytes(),before)
        self.assertEqual(f.client.get('/api/jobs/'+job).status_code,404)
        self.assertEqual(f.client.get('/api/overview').json['jobs'],[])

    def test_unavailable_query_hides_raw_error_and_response_never_cached(self):
        f=self.fixture();portal_admin.set_admin(f.auth,'alice',enabled=True)
        with patch.object(admin_storage_view,'read',side_effect=sqlite3.OperationalError('/srv/secret credentials')):
            result=f.client.get('/api/admin/storage')
        self.assertEqual(result.status_code,503);self.assertNotIn('secret',result.get_data(as_text=True))
        self.assertEqual(result.headers['Cache-Control'],'no-store')

    def test_actual_browser_script_harness(self):
        result=subprocess.run([os.environ.get('DL_NODE_BINARY','node'),str(ROOT/'tests/test_admin_storage_ui.cjs')],capture_output=True,text=True,timeout=15)
        self.assertEqual(result.returncode,0,result.stdout+result.stderr)


class InventoryViewTests(unittest.TestCase):
    def setUp(self):
        self.f=inventory.InventoryTests(methodName='runTest');self.addCleanup(self.f.doCleanups);self.f.setUp()
        self.sink=observations.AdminObservations(self.f.fixture.base)
        self.f.owner.admin_observations=self.sink
        self.f.reporter.nfs_health=lambda:dict(schema_version=1,status='verified_readonly',observed_at_ns=self.f.clock())

    def view(self):
        with sqlite3.connect(self.f.fixture.db_path.as_uri()+'?mode=ro',uri=True) as db:
            db.execute('BEGIN')
            return admin_storage_view.read(db,['pi01','pi02','pi03'],now_ns=self.f.clock())

    def test_actual_authenticated_v3_health_disk_cache_and_other_nodes_unknown(self):
        f=self.f;self.assertTrue(f.poll())
        view=self.view();one,two,three=view['workers']
        self.assertEqual(one['nfs']['status'],'verified_readonly');self.assertEqual(one['nfs']['io_status'],'untested')
        self.assertEqual(one['verified_cache_bytes'],len(f.data));self.assertGreater(one['disk_total_bytes'],0)
        self.assertEqual(one['inventory_status'],'reported_fresh');self.assertIsNone(one['cache_limit_bytes'])
        self.assertEqual(two['nfs']['status'],'unknown');self.assertEqual(three['inventory_status'],'not_reported')
        self.assertEqual(view['scheduler_eligibility'],'not_evaluated')
        self.assertNotIn('secret',json.dumps(view));self.assertNotIn('sha256',json.dumps(view))

    def test_dataset_digest_aliases_count_once_and_foreign_data_only_aggregate(self):
        f=self.f
        # Two owned datasets share a physical object; duplicate logical paths
        # count once within each dataset, not twice from manifest aliases.
        f.artifacts.create_dataset('alice',(DatasetEntry('alice-private.csv','file',f.artifact.sha256,len(f.data)),
                                          DatasetEntry('alias.csv','file',f.artifact.sha256,len(f.data))))
        f.artifacts.register_artifact('bob',io.BytesIO(f.data),expected_size=len(f.data))
        f.artifacts.create_dataset('bob',(DatasetEntry('bob-secret.csv','file',f.artifact.sha256,len(f.data)),))
        self.assertTrue(f.poll());view=self.view();coverage=view['workers'][0]['dataset_coverage']
        self.assertEqual(coverage,dict(dataset_versions=2,complete_versions=2,required_bytes=2*len(f.data),cached_bytes=2*len(f.data)))
        self.assertNotIn('alice',json.dumps(view));self.assertNotIn('bob',json.dumps(view));self.assertNotIn('csv',json.dumps(view))

    def test_missing_zero_byte_object_not_marked_complete_and_stale_not_certain(self):
        f=self.f;empty=f.register(b'')
        f.artifacts.create_dataset('alice',(DatasetEntry('empty','file',empty.sha256,0),))
        self.assertTrue(f.poll());coverage=self.view()['workers'][0]['dataset_coverage']
        self.assertEqual(coverage['required_bytes'],0);self.assertEqual(coverage['complete_versions'],0)
        f.clock.advance(16);row=self.view()['workers'][0]
        self.assertEqual(row['inventory_status'],'stale_or_untrusted');self.assertIsNone(row['dataset_coverage'])
        self.assertIsNone(row['verified_cache_bytes']);self.assertEqual(row['nfs']['status'],'unknown')

    def test_tamper_duplicate_and_session_change_cannot_refresh_health(self):
        f=self.f;request,body,signature=f.exchange();document=protocol.report(body)
        self.assertEqual(document['version'],3)
        f.owner.accept('pi01',request,body,signature);path=f.fixture.base/observations.FILENAME;before=path.read_bytes()
        f.clock.advance(1);self.assertEqual(f.owner.accept('pi01',request,body,signature),'duplicate')
        self.assertEqual(path.read_bytes(),before)
        changed=protocol.encode(dict(document,nfs=dict(schema_version=1,status='unavailable',observed_at_ns=f.clock())))
        with self.assertRaises(protocol.AuthenticationError):f.owner.accept('pi01',request,changed,signature)
        self.assertEqual(path.read_bytes(),before)
        self.sink.accept('pi01','f'*32,document['sequence'],f.clock(),document['nfs'])
        self.assertEqual(self.view()['workers'][0]['nfs']['status'],'unknown')

    def test_old_reports_missing_or_malformed_health_never_fabricate_healthy(self):
        f=self.f;f.reporter.nfs_health=None;self.assertTrue(f.poll())
        self.assertEqual(self.view()['workers'][0]['nfs']['status'],'unknown')
        for value in (None,dict(status='verified_readonly',path='/secret'),dict(schema_version=True,status='verified_readonly',observed_at_ns=f.clock())):
            f.reporter.nfs_health=lambda:value
            self.assertTrue(f.poll());self.assertEqual(self.view()['workers'][0]['nfs']['status'],'unknown')

    def test_snapshot_failure_does_not_break_inventory_and_insecure_file_not_used(self):
        f=self.f
        with patch.object(self.sink,'_write',side_effect=OSError('disk full')):self.assertTrue(f.poll())
        self.assertTrue(f.owner.eligible('pi01'));self.assertEqual(self.view()['workers'][0]['nfs']['status'],'unknown')
        self.assertTrue(f.poll());path=f.fixture.base/observations.FILENAME;path.chmod(0o644)
        self.assertEqual(self.view()['workers'][0]['nfs']['status'],'unknown')

    def test_offline_inventory_immediately_hides_coverage_and_old_health(self):
        f=self.f;self.assertTrue(f.poll());f.owner.offline('pi01');row=self.view()['workers'][0]
        self.assertEqual(row['inventory_status'],'stale_or_untrusted')
        self.assertIsNone(row['dataset_coverage']);self.assertEqual(row['nfs']['status'],'unknown')

    def test_dataset_window_and_snapshot_worker_count_are_bounded(self):
        f=self.f
        for _ in range(21):f.artifacts.create_dataset('alice',())
        self.assertTrue(f.poll());view=self.view()
        self.assertTrue(view['coverage_truncated']);self.assertEqual(view['workers'][0]['dataset_coverage']['dataset_versions'],20)
        for index in range(260):self.sink.accept(f'worker{index}','a'*32,1,f.clock(),None)
        self.assertEqual(len(observations.read(f.fixture.base)),256)
        self.assertLess((f.fixture.base/observations.FILENAME).stat().st_size,observations.MAX_BYTES)

    def test_mount_probe_uses_only_kernel_guard_and_does_not_claim_io(self):
        f=self.f;probe=observations.NFSHealthProbe('192.0.2.10:/srv/ml-shared/objects',target='/srv/mlnfs/inputs',clock=f.clock)
        with patch.object(observations,'require_input_mount',return_value={}) as guard:
            self.assertEqual(probe()['status'],'verified_readonly');guard.assert_called_once()
        with patch.object(observations,'require_input_mount',side_effect=ValueError('/private')):
            self.assertEqual(probe()['status'],'unavailable')


class TransferViewTests(unittest.TestCase):
    def test_real_signed_stages_history_is_bounded_numeric_only_and_readonly(self):
        f=staged.StagingPortalTests(methodName='runTest');self.addCleanup(f.doCleanups);f.setUp()
        portal_admin.set_admin(f.auth,'alice',enabled=True)
        self.assertTrue(f.f.dispatch())
        before=f.f.fixture.db_path.read_bytes()
        result=f.http.get('/api/admin/storage');self.assertEqual(result.status_code,200,result.json)
        history=result.json['workers'][0]['transfer_history'];self.assertEqual(len(history),1)
        self.assertGreater(history[0]['transferred_bytes'],0);self.assertGreater(history[0]['copy_seconds'],0)
        self.assertNotIn(f.f.job_id,json.dumps(result.json));self.assertNotIn('alice',json.dumps(result.json))
        self.assertEqual(f.f.fixture.db_path.read_bytes(),before)
        f.login('bob');self.assertEqual(f.http.get('/api/admin/storage').status_code,403)


if __name__=='__main__':unittest.main(verbosity=2)
