"""Isolated browser API checks: uploads, owner boundaries, queue controls and downloads."""
from contextlib import closing
import io
import json
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch, Mock
import zipfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'controller'))
import student_portal as portal
import job_store as store
import recovery_metrics
import queue_policy


class PortalTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.patches = [patch.object(store,'DB_FILE',str(self.root/'jobs.db')),
                        patch.object(recovery_metrics,'DB_PATH',self.root/'jobs.db'),
                        patch.dict(os.environ,{'DL_STORAGE_ROOT':str(self.root/'jobs')}),
                        patch.object(portal.ClusterView,'get',return_value=[]),
                        patch.object(portal,'dispatcher_running',return_value=False)]
        for p in self.patches: p.start()
        store.initialize_database(); recovery_metrics.init_table()
        self.auth = self.root/'portal'
        portal.add_user(self.auth,'alice','alice-password')
        portal.add_user(self.auth,'bob','bob-password')
        self.app = portal.create_app(self.auth)
        self.client = self.app.test_client()
        self.login('alice')

    def tearDown(self):
        for p in reversed(self.patches): p.stop()
        self.temp.cleanup()

    def login(self,owner):
        csrf = self.client.get('/api/session').json['csrf']
        result = self.client.post('/api/login',json={'owner':owner,'password':owner+'-password'},headers={'X-CSRF-Token':csrf})
        self.assertEqual(result.status_code,200,result.json)
        self.csrf = result.json['csrf']

    def post(self,url,**kwargs):
        return self.client.post(url,headers={'X-CSRF-Token':self.csrf},**kwargs)

    def submit(self):
        result = self.post('/api/jobs',data={'example':'regression','spec':json.dumps(portal.public_spec('regression'))})
        self.assertEqual(result.status_code,202,result.json)
        return result.json['job_id']

    def test_auth_csrf_and_owner_controls(self):
        anonymous = self.app.test_client()
        self.assertEqual(anonymous.get('/api/overview').status_code,401)
        self.assertEqual(self.client.post('/api/jobs',json={}).status_code,403)
        job = self.submit()
        self.login('bob')
        self.assertEqual(self.client.get('/api/overview').json['jobs'],[])
        for suffix in ('','/logs','/results'):
            self.assertEqual(self.client.get('/api/jobs/'+job+suffix).status_code,404)
        for action in ('cancel','hold','release','prepare'):
            self.assertEqual(self.post('/api/jobs/'+job+'/'+action,json={}).status_code,404)
        self.assertEqual(store.get_job(job)['status'],'STAGED')

    def test_uploaded_source_and_data_survive_temporary_directory(self):
        project = io.BytesIO()
        with zipfile.ZipFile(project,'w') as z:
            z.writestr('train.py','print("training")'); z.writestr('requirements.txt','')
        project.seek(0)
        result=self.post('/api/jobs',data={'spec':json.dumps(portal.public_spec('regression')),
            'project':(project,'my-code.zip'),'dataset':(io.BytesIO(b'x,y\n1,2\n'),'my-data.csv')})
        self.assertEqual(result.status_code,202,result.json)
        job=store.get_job(result.json['job_id']);spec=json.loads(job['job_config'])
        self.assertEqual(Path(spec['project'],'train.py').read_text(),'print("training")')
        self.assertEqual(Path(spec['dataset']).read_text(),'x,y\n1,2\n')
        portal.verify_workspace(Path(job['storage_path']),job)
        self.assertEqual(portal.rows('SELECT state FROM portal_preparation')[0]['state'],'PENDING')

    def test_unsafe_zip_never_creates_job_or_escapes(self):
        for name in ('../escaped','/absolute','folder/../../escaped'):
            project=io.BytesIO()
            with zipfile.ZipFile(project,'w') as z:
                z.writestr(name,'bad');z.writestr('requirements.txt','')
            project.seek(0)
            result=self.post('/api/jobs',data={'spec':json.dumps(portal.public_spec('regression')),'project':(project,'project.zip')})
            self.assertEqual(result.status_code,400,result.json)
        self.assertEqual(store.get_all_jobs(),[])

    def test_no_controller_path_input_or_owner_spoofing(self):
        spec=portal.public_spec('regression');spec.update(project='/etc',dataset='/etc/passwd')
        result=self.post('/api/jobs',data={'spec':json.dumps(spec),'owner':'bob'})
        self.assertEqual(result.status_code,400)
        job=self.submit()
        self.assertEqual(store.get_job(job)['owner'],'alice')

    def test_hold_release_cancel_and_queue_position(self):
        job=self.submit()
        with closing(store.get_connection()) as db,db:
            db.execute("UPDATE jobs SET status='QUEUED',queued_at=1 WHERE job_id=?",(job,))
            db.execute("UPDATE portal_preparation SET state='DONE'")
        self.assertEqual(self.client.get('/api/overview').json['jobs'][0]['position'],1)
        self.assertEqual(self.post('/api/jobs/'+job+'/hold',json={}).json['status'],'HELD')
        self.assertIsNone(self.client.get('/api/overview').json['jobs'][0]['position'])
        self.assertEqual(self.post('/api/jobs/'+job+'/release',json={}).json['status'],'QUEUED')
        self.assertEqual(self.post('/api/jobs/'+job+'/cancel',json={}).json['status'],'CANCELLED')
        self.assertEqual(self.app.extensions['preparation'].run_one(),False)

    def test_prep_request_cancelled_before_execution_does_not_launch(self):
        job=self.submit();self.post('/api/jobs/'+job+'/cancel',json={})
        with patch.object(portal.subprocess,'Popen') as spawn:
            self.app.extensions['preparation'].run_one()
            spawn.assert_not_called()
        self.assertEqual(portal.rows('SELECT state FROM portal_preparation')[0]['state'],'CANCELLED')

    def test_successful_preparation_uses_existing_enqueue(self):
        job=self.submit()
        process=Mock();process.poll.return_value=0
        def spawn(*args,**kwargs):
            with closing(store.get_connection()) as db,db:
                db.execute("UPDATE jobs SET status='READY' WHERE job_id=?",(job,))
            return process
        with patch.object(portal.subprocess,'Popen',side_effect=spawn),patch.object(store,'enqueue_custom_job') as enqueue:
            self.app.extensions['preparation'].run_one()
            enqueue.assert_called_once_with(job,{n['id'] for n in portal.load_nodes()})
        self.assertEqual(portal.rows('SELECT state FROM portal_preparation')[0]['state'],'DONE')

    def test_restart_recovers_preparation_and_error_can_be_retried(self):
        job=self.submit();queue=self.app.extensions['preparation']
        queue.update(job,'PREPARING')
        with patch.object(portal.threading.Thread,'start'):
            queue.start()
        self.assertEqual(portal.rows('SELECT state FROM portal_preparation')[0]['state'],'PENDING')
        process=Mock();process.poll.return_value=1
        with patch.object(portal.subprocess,'Popen',return_value=process): queue.run_one()
        self.assertEqual(portal.rows('SELECT state FROM portal_preparation')[0]['state'],'ERROR')
        self.assertEqual(self.post('/api/jobs/'+job+'/prepare',json={}).status_code,202)
        self.assertEqual(portal.rows('SELECT state FROM portal_preparation')[0]['state'],'PENDING')

    def test_result_zip_and_log_tail_exclude_escape(self):
        job=self.submit();workspace=Path(store.get_job(job)['storage_path'])
        (workspace/'results/summary.json').write_text('{"ok":true}')
        (workspace/'results/outside').symlink_to(self.auth/'users.json')
        (workspace/'logs/training.log').write_text('x'*70000+'END')
        self.assertEqual(self.client.get('/api/jobs/'+job+'/results').status_code,409)
        store.control_job(job,'alice','cancel')
        response=self.client.get('/api/jobs/'+job+'/results')
        with zipfile.ZipFile(io.BytesIO(response.data)) as archive:
            self.assertEqual(archive.namelist(),['summary.json','system-timing.json'])
            timing=json.loads(archive.read('system-timing.json'))
            self.assertEqual(timing['job_id'],job)
            self.assertIsNone(timing['final_attempt_training_loop_seconds'])
        response.close()
        tail=self.client.get('/api/jobs/'+job+'/logs?file=training.log').json['text']
        self.assertEqual(len(tail),portal.LOG_BYTES);self.assertTrue(tail.endswith('END'))
        self.assertEqual(self.client.get('/api/jobs/'+job+'/logs?file=../users.log').status_code,400)

    def test_recovery_history_survives_many_checkpoints(self):
        job=self.submit()
        recovery_metrics.log_event(job,'RESUMED',details={'global_step':42})
        for step in range(310): recovery_metrics.log_event(job,'CHECKPOINT_SYNC',details={'global_step':step})
        detail=self.client.get('/api/jobs/'+job).json
        self.assertEqual(detail['events'][0]['event_type'],'RESUMED')
        self.assertEqual(detail['checkpoint']['details']['global_step'],309)

    def test_live_log_fencing_and_unavailable_worker(self):
        job=self.submit()
        with closing(store.get_connection()) as db,db:
            db.execute("UPDATE jobs SET status='RUNNING',node_id='pi01' WHERE job_id=?",(job,))
        with patch.object(portal.ownership,'params',side_effect=ValueError('FENCED')):
            result=self.client.get('/api/jobs/'+job+'/logs')
        self.assertEqual(result.status_code,200)
        self.assertIn('unavailable',result.json['text'])
        self.assertEqual(result.json['source'],'FENCED')

    def test_short_live_log_retries_unsatisfiable_suffix_range(self):
        job=self.submit()
        with closing(store.get_connection()) as db,db:
            db.execute("UPDATE jobs SET status='RUNNING',node_id='pi01' WHERE job_id=?",(job,))
        short=Mock(status_code=416)
        success=Mock(status_code=200,headers={})
        success.__enter__=Mock(return_value=success);success.__exit__=Mock(return_value=False)
        success.iter_content.return_value=[b'epoch 1 loss 0.5']
        with patch.object(portal.requests,'get',side_effect=[short,success]) as fetch:
            result=self.client.get('/api/jobs/'+job+'/logs')
        self.assertEqual(result.json['text'],'epoch 1 loss 0.5')
        short.close.assert_called_once()
        self.assertNotIn('headers',fetch.call_args.kwargs)

    def test_single_example_rejects_distributed_before_creating_job(self):
        spec=portal.public_spec('regression');spec['execution']={'mode':'distributed','workers':3}
        result=self.post('/api/jobs',data={'example':'regression','spec':json.dumps(spec)})
        self.assertEqual(result.status_code,400)
        self.assertIn('one worker',result.json['error'])
        self.assertEqual(portal.rows('SELECT job_id FROM jobs'),[])

    def test_ddp_batch_must_match_worker_count(self):
        spec=portal.public_spec('ddp');spec['execution']['workers']=3
        result=self.post('/api/jobs',data={'example':'ddp','spec':json.dumps(spec)})
        self.assertEqual(result.status_code,400)
        self.assertIn('divisible',result.json['error'])
        self.assertEqual(portal.rows('SELECT job_id FROM jobs'),[])
        spec['entrypoint'] += ['--global-batch-size','96']
        result=self.post('/api/jobs',data={'example':'ddp','spec':json.dumps(spec)})
        self.assertEqual(result.status_code,202,result.json)

    def test_ddp_single_checkpoint_is_rejected_before_staging(self):
        spec=portal.public_spec('ddp');spec['execution']={'mode':'single','workers':1}
        result=self.post('/api/jobs',data={'example':'ddp','spec':json.dumps(spec)})
        self.assertEqual(result.status_code,400)
        self.assertIn('distributed execution',result.json['error'])
        self.assertEqual(portal.rows('SELECT job_id FROM jobs'),[])

    def test_terminal_detail_reports_timing(self):
        job=self.submit();store.control_job(job,'alice','cancel')
        detail=self.client.get('/api/jobs/'+job).json
        self.assertEqual(detail['timing']['job_id'],job)
        self.assertIsNone(detail['timing']['final_attempt_training_loop_seconds'])

    def test_example_package_and_security_headers(self):
        result=self.client.get('/api/examples/ddp/download')
        with zipfile.ZipFile(io.BytesIO(result.data)) as z:
            self.assertIn('project.zip',z.namelist());self.assertIn('job.json',z.namelist())
            with zipfile.ZipFile(io.BytesIO(z.read('project.zip'))) as project:
                self.assertIn('train.py',project.namelist())
        result.close()
        page=self.client.get('/')
        self.assertIn('frame-ancestors',page.headers['Content-Security-Policy'])
        self.assertEqual(page.headers['Cache-Control'],'no-store')
        page.close()


if __name__=='__main__': unittest.main()
