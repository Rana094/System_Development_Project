"""Existing portal + actual LOCAL registry/preparer, simulated signed transport."""
from contextlib import contextmanager
import io
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import Mock,patch

ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'agent'))
import test_storage_registry as fixtures
import student_portal as portal
from hybrid_storage.portal_service import HybridPortal,PrivateTransport
from hybrid_storage import cache_registry_schema,staging_schema,cleanup_schema,checkpoint_schema,restore_schema
from hybrid_storage.inventory_protocol import Credential
from hybrid_storage.checkpoint_publication import LocalCheckpointPublisher
from hybrid_storage.config import StorageConfig
from hybrid_storage.staging_protocol import StagingClient,PREPARE_PATH
from hybrid_storage.offline_prepare import OfflinePreparer
from checkpoint_recovery import CheckpointDispatch
from hybrid_storage.checkpoint_transfer import CheckpointCollector
from hybrid_storage.checkpoint_restore import CheckpointRestorer

class HybridPortalTests(unittest.TestCase):
    def setUp(self):
        self.f=fixtures.RegistryTests(methodName='runTest');self.addCleanup(self.f.doCleanups);self.f.setUp()
        self.r=self.f.registry();self.db=self.f.db
        import recovery_metrics
        mocked=patch.object(recovery_metrics,'DB_PATH',self.f.db_path);mocked.start();self.addCleanup(mocked.stop)
        recovery_metrics.init_table()
        for m in (cache_registry_schema,staging_schema,cleanup_schema,checkpoint_schema,restore_schema):m.migrate(self.db)
        self.key=Credential('pi02','2'*32,b'2'*32);self.node=dict(id='pi02',ip='192.168.10.150',port=5001)
        with self.db:self.db.execute("INSERT INTO cache_registry_workers(worker_id,auth_principal,auth_state,enabled,created_at_ns,updated_at_ns) VALUES (?,?,'ENROLLED',1,1,1)",(self.key.worker,self.key.principal))
        self.cp=self.f.base/'cp';self.cp.mkdir(mode=0o700);LocalCheckpointPublisher(self.r,self.cp).initialize()
        self.service=HybridPortal(db_path=self.f.db_path,objects=self.f.objects,scratch=self.f.scratch,checkpoints=self.cp,nodes=[self.node],
            credentials={'pi02':self.key},settings=StorageConfig('nfs',True,True,'locality'))
    def test_per_operation_context_has_real_auth_schema_and_closes_connection(self):
        with self.service.context() as runtime:
            self.assertIsInstance(runtime,CheckpointDispatch);self.assertIsNot(runtime.registry.db,self.db)
            self.assertEqual(runtime.client.credentials,{'pi02':self.key});connection=runtime.registry.db
        import sqlite3
        with self.assertRaises(sqlite3.ProgrammingError):connection.execute('SELECT 1')
    def test_revoked_worker_is_not_auto_reenrolled(self):
        with self.db:self.db.execute("UPDATE cache_registry_workers SET enabled=0")
        with self.assertRaises(PermissionError):
            with self.service.context():pass
        self.assertEqual(self.db.execute('SELECT enabled FROM cache_registry_workers').fetchone()[0],0)
    def test_private_transport_rejects_redirect_public_and_unconfigured_endpoints(self):
        session=Mock();transport=PrivateTransport([self.node],session)
        for url in ['http://100.1.2.3:5001/jobs/prepare','http://192.168.10.101:5001/jobs/prepare','https://192.168.10.150:5001/jobs/prepare']:
            with self.assertRaises(PermissionError):transport.post(url,data=b'')
        transport.post('http://192.168.10.150:5001/jobs/prepare',data=b'')
        self.assertFalse(session.post.call_args.kwargs['allow_redirects']);self.assertFalse(session.trust_env)
    def test_new_portal_upload_registers_authorized_inputs_and_queues_without_pip(self):
        auth=self.f.base/'portal';portal.add_user(auth,'alice','test-password');portal.add_user(auth,'bob','test-password')
        app=portal.create_app(auth,hybrid_service=self.service);http=app.test_client()
        csrf=http.get('/api/session').json['csrf'];reply=http.post('/api/login',json=dict(owner='alice',password='test-password'),headers={'X-CSRF-Token':csrf})
        csrf=reply.json['csrf']
        result=http.post('/api/jobs',headers={'X-CSRF-Token':csrf},data={'example':'regression','spec':json.dumps(portal.public_spec('regression'))})
        self.assertEqual(result.status_code,202,result.json);job=result.json['job_id']
        self.assertEqual(fixtures.store.get_job(job)['status'],'HELD')
        queue=app.extensions['preparation'];original=self.service.context
        def prepare_reply(node,path,body):
            self.assertEqual(path,PREPARE_PATH)
            self.assertEqual(body['manifest']['owner'],'alice')
            return dict(prepared=True,record=dict(job_id=job,owner='alice',manifest_sha256=body['manifest_sha256'],profile='pytorch-cpu',environment_key='a'*64,compatibility_key='c'*64))
        @contextmanager
        def context():
            with original() as runtime:
                runtime.client.call=prepare_reply
                yield runtime
        errors=[];prepare=self.service.prepare
        def recorded(*args):
            try:return prepare(*args)
            except Exception as error:errors.append(error);raise
        with patch.object(self.service,'context',side_effect=context),patch.object(self.service,'prepare',side_effect=recorded),patch('subprocess.Popen',side_effect=AssertionError('No pip/SSH preparation')):
            self.assertTrue(queue.run_one())
        if errors:raise errors[0]
        self.assertEqual(fixtures.store.get_job(job)['status'],'QUEUED')
        self.assertIsNotNone(self.r.get_job_inputs('alice',job))
        self.assertEqual(self.db.execute('SELECT state FROM portal_preparation WHERE job_id=?',(job,)).fetchone()[0],'DONE')
        reply=http.post('/api/login',json=dict(owner='bob',password='test-password'),headers={'X-CSRF-Token':csrf})
        self.assertEqual(http.get('/api/jobs/'+job).status_code,404)
        self.assertEqual(http.post('/api/jobs/'+job+'/release',headers={'X-CSRF-Token':reply.json['csrf']}).status_code,404)
    def test_owner_hold_cancel_and_unsupported_profile_never_authorize_preparation(self):
        job=self.f.job_id
        with self.db:self.db.execute("UPDATE jobs SET status='HELD',queue_reason='Owner hold' WHERE job_id=?",(job,))
        with self.assertRaises(PermissionError):self.service.pending(self.db,job,'pytorch-cpu')
        with self.db:self.db.execute("UPDATE jobs SET status='CANCELLED',cancel_requested_at=1 WHERE job_id=?",(job,))
        with self.assertRaises(PermissionError):self.service.pending(self.db,job,'pytorch-cpu')
        with self.assertRaises(ValueError):self.service.pending(self.db,job,'worker-base')

if __name__=='__main__':unittest.main(verbosity=2)
