"""7.4 owner-safe portal + signed stage/cache/SQLite integration, no listeners/Pis.

Uses real local copies and Flask clients; NFS facts, trainer and isolation mocked.
Run only through tools/run_tests.py --isolated (controller imports initialize DBs).
"""

import json
import io
from pathlib import Path
import sqlite3
import unittest
import zipfile
from unittest.mock import patch

import test_staging_dispatch as fixtures
import job_store as store
import student_portal as portal
import recovery_metrics
from hybrid_storage.metrics import MetricsSnapshot, StorageMetrics
from hybrid_storage.staging_metrics import validated
from hybrid_storage.staging_protocol import STAGE_PATH
from hybrid_storage.config import StorageConfig


class MetricTests(unittest.TestCase):
    def test_request_measurements_exclude_concurrent_inventory_work_and_close_on_exception(self):
        import threading
        metrics=StorageMetrics()
        with self.assertRaises(ValueError):
            with metrics.observe() as request:
                metrics.add(verification_ns=7)
                inventory=threading.Thread(target=lambda:metrics.add(verification_ns=100))
                inventory.start();inventory.join()
                raise ValueError('abort scope')
        metrics.add(verification_ns=1)
        self.assertEqual(request.snapshot().verification_ns,7)
        self.assertEqual(metrics.snapshot().verification_ns,108)

    def test_numeric_allowlist_rejects_boolean_overflow_text_extra_and_bad_version(self):
        valid=MetricsSnapshot().document()
        self.assertEqual(validated(valid,required_bytes=0),valid)
        self.assertIsNone(validated(valid,required_bytes=1))
        for value in (None,[],dict(valid,path='/srv/secret'),dict(valid,schema_version=True),
                      dict(valid,schema_version=2)):
            self.assertIsNone(validated(value))
        for value in (-1,True,1.5,float('nan'),float('inf'),2**63,'/srv/secret'):
            self.assertIsNone(validated(dict(valid,staged_bytes=value)))


class StagingPortalTests(unittest.TestCase):
    def setUp(self):
        self.f=fixtures.DispatchTests(methodName='runTest')
        self.addCleanup(self.f.doCleanups);self.f.setUp()
        for mocked in (patch.object(recovery_metrics,'DB_PATH',self.f.fixture.db_path),
                       patch.object(portal.ClusterView,'get',return_value=[]),
                       patch.object(portal,'dispatcher_running',return_value=False)):
            mocked.start();self.addCleanup(mocked.stop)
        recovery_metrics.init_table()
        self.auth=self.f.fixture.base/'portal'
        for owner in ('alice','bob'):portal.add_user(self.auth,owner,owner+'-password')
        self.app=portal.create_app(self.auth);self.http=self.app.test_client()
        self.login('alice')

    def login(self,owner):
        csrf=self.http.get('/api/session').json['csrf']
        result=self.http.post('/api/login',json=dict(owner=owner,password=owner+'-password'),headers={'X-CSRF-Token':csrf})
        self.assertEqual(result.status_code,200);self.csrf=result.json['csrf']

    def detail(self):
        result=self.http.get('/api/jobs/'+self.f.job_id)
        self.assertEqual(result.status_code,200,result.json)
        return result.json

    def staged(self):
        f=self.f;job,lease=f.reserve()
        return f.runtime.stage_group(job,[f.node],[lease])

    def test_staging_readiness_uses_side_state_without_running_or_remote_log_reads(self):
        f=self.f;job,lease=f.reserve();plans=f.runtime.plans(job)
        f.runtime.lifecycle.begin('alice',job['job_id'],plans)
        with patch.object(portal.requests,'get',side_effect=AssertionError('No remote call while staging')):
            detail=self.detail();view=detail['staging']
            log=self.http.get('/api/jobs/'+f.job_id+'/logs').json
        self.assertEqual(detail['job']['status'],'SCHEDULED')
        self.assertEqual((view['phase'],view['ready_ranks'],view['total_ranks']),('STAGING',0,1))
        self.assertIsNone(view['metrics']);self.assertIn('not recorded',log['text'])
        self.assertEqual(self.http.get('/api/overview').json['jobs'][0]['staging'],view)
        f.popen.assert_not_called()

    def test_cold_run_retains_signed_metrics_and_readonly_poll_survives_new_app(self):
        f=self.f;self.assertTrue(f.dispatch());view=self.detail()['staging']
        required=sum({item.sha256:item.size_bytes for item in f.runtime.plans(store.get_job(f.job_id))[0].inputs()}.values())
        self.assertEqual(view['phase'],'STARTED');self.assertEqual(view['measured_ranks'],1)
        self.assertEqual(view['metrics']['staged_bytes'],required)
        self.assertEqual(view['metrics']['cache_hit_bytes'],0)
        self.assertGreater(view['metrics']['verification_ns'],0)
        self.assertGreater(view['metrics']['copy_ns'],0)
        before=f.fixture.db_path.read_bytes();changes=f.db.total_changes
        for _ in range(3):self.assertEqual(self.detail()['staging'],view)
        self.assertEqual(f.fixture.db_path.read_bytes(),before);self.assertEqual(f.db.total_changes,changes)
        self.http=portal.create_app(self.auth).test_client();self.login('alice')
        self.assertEqual(self.detail()['staging'],view)
        text=self.http.get('/api/jobs/'+f.job_id+'/logs?view=staging').json['text']
        self.assertIn(f'staged {required} bytes',text);self.assertIn('Durations overlap',text)

    def test_multipart_website_submission_through_explicit_trusted_staging_and_completion(self):
        f=self.f
        # Only this disposable app accepts NFS intent; production flags untouched.
        with patch.object(portal,'load_config',return_value=StorageConfig('nfs',False,True,'resource')):
            self.app=portal.create_app(self.auth)
        self.http=self.app.test_client();self.login('alice')
        project=io.BytesIO()
        with zipfile.ZipFile(project,'w') as archive:
            archive.writestr('train.py','print("train")\n');archive.writestr('requirements.txt','')
        project.seek(0)
        spec=portal.public_spec('regression');spec['recovery']=dict(mode='restart',max_retries=2)
        with patch.object(portal,'create_job_id',return_value='portal-staged'):
            result=self.http.post('/api/jobs',headers={'X-CSRF-Token':self.csrf},data={
                'spec':json.dumps(spec),'project':(project,'project.zip'),
                'dataset':(io.BytesIO(b'x,y\n1,2\n'),'data.csv')})
        self.assertEqual(result.status_code,202,result.json);self.assertEqual(result.json['status'],'HELD')
        f.job_id=result.json['job_id'];self.assertIsNone(self.detail()['staging'])
        workspace=Path(store.get_job(f.job_id)['storage_path'])
        raw=(workspace/'manifest.json').read_bytes()
        source=f.fixture.source(f.registry,workspace=workspace);data=f.fixture.dataset(f.registry,workspace=workspace)
        f.registry.bind_job('alice',f.job_id,legacy_manifest_bytes=raw,source_sha256=source.sha256,
                            dataset_id=data.dataset_id,dataset_version=data.version)
        f.fixture.raw=raw;f.prepare_environment()
        self.assertTrue(f.dispatch());self.assertEqual(self.detail()['job']['status'],'RUNNING')
        self.assertGreater(self.detail()['staging']['metrics']['staged_bytes'],0)
        _,lease,_=next(iter(f.service._ready.values()))
        f.process.poll.return_value=0;f.service.quiescent=lambda *_:True
        self.assertTrue(f.service.note_exit(lease,0));f.service.tick()
        with patch('job_artifacts.collect'):
            self.assertEqual(f.runtime.reconcile(store.get_job(f.job_id)),'COMPLETED')
        self.assertEqual(self.detail()['job']['status'],'COMPLETED')
        self.assertEqual(self.detail()['staging']['phase'],'TERMINAL')

    def test_warm_new_job_reports_zero_staged_bytes_not_process_cumulative_total(self):
        f=self.f;f.test_warm_second_authorized_job_stages_zero_nfs_bytes()
        view=self.detail()['staging']
        self.assertEqual(view['metrics']['staged_bytes'],0)
        self.assertGreater(view['metrics']['cache_hit_bytes'],0)
        self.assertGreater(f.cache.metrics.snapshot().staged_bytes,0)

    def test_ready_retry_preserves_first_observation_and_deduplicates_event(self):
        f=self.f;job,lease=f.reserve();f.runtime.stage_group(job,[f.node],[lease])
        first=self.detail()['staging']
        f.runtime.stage_group(job,[f.node],[fixtures.leases.ensure(job,'pi01')])
        self.assertEqual(self.detail()['staging'],first)
        self.assertEqual(f.db.execute("SELECT COUNT(*) FROM recovery_events WHERE event_type='INPUTS_STAGED'").fetchone()[0],1)

    def test_duplicate_manifest_paths_count_unique_fetched_bytes_and_separate_private_copies(self):
        f=self.f
        (f.fixture.data/'a/duplicate.csv').write_bytes((f.fixture.data/'a/rows.csv').read_bytes())
        f.job_id,workspace,raw=f.fixture.job()
        source=f.fixture.source(f.registry,workspace=workspace)
        data=f.fixture.dataset(f.registry,workspace=workspace)
        f.registry.bind_job('alice',f.job_id,legacy_manifest_bytes=raw,source_sha256=source.sha256,
                            dataset_id=data.dataset_id,dataset_version=data.version)
        f.fixture.raw=raw;f.prepare_environment()
        inputs=f.runtime.plans(store.get_job(f.job_id))[0].inputs()
        unique=sum({item.sha256:item.size_bytes for item in inputs}.values())
        copied=sum(item.size_bytes for item in inputs)
        self.assertLess(unique,copied);self.assertTrue(f.dispatch())
        metrics=self.detail()['staging']['metrics']
        self.assertEqual(metrics['staged_bytes'],unique)
        self.assertEqual(metrics['workspace_copy_bytes'],copied)

    def test_prior_attempt_measurements_do_not_appear_in_new_staging_attempt(self):
        f=self.f;stage,_=self.staged()
        f.runtime._owned_stages.clear()  # Simulate a new controller runtime.
        self.assertEqual(f.runtime.reconcile(store.get_job(f.job_id)),'QUEUED')
        self.assertIsNone(self.detail()['staging'])
        job,lease=f.reserve();f.runtime.lifecycle.begin('alice',f.job_id,f.runtime.plans(job))
        view=self.detail()['staging']
        self.assertEqual(view['attempt'],2);self.assertIsNone(view['metrics'])
        self.assertEqual(view['measured_ranks'],0)

    def test_other_owner_and_anonymous_cannot_read_staging_status_or_logs(self):
        self.staged();self.login('bob')
        self.assertEqual(self.http.get('/api/overview').json['jobs'],[])
        for suffix in ('','/logs','/logs?view=staging'):
            self.assertEqual(self.http.get('/api/jobs/'+self.f.job_id+suffix).status_code,404)
            self.assertEqual(self.app.test_client().get('/api/jobs/'+self.f.job_id+suffix).status_code,401)

    def test_default_legacy_detail_keeps_existing_representation_and_no_staging_log(self):
        f=self.f;f.job_id,workspace,_=f.fixture.job(backend='legacy')
        detail=self.detail()
        self.assertNotIn('staging',detail)
        self.assertEqual(detail['job']['storage_path'],str(workspace))
        listed=next(job for job in self.http.get('/api/overview').json['jobs'] if job['job_id']==f.job_id)
        self.assertNotIn('staging',listed)
        self.assertEqual(self.http.get('/api/jobs/'+f.job_id+'/logs?view=staging').status_code,404)
        f.popen.assert_not_called();self.assertEqual(f.calls,[])

    def test_raw_service_errors_paths_credentials_and_foreign_events_never_serialized(self):
        f=self.f;self.staged();secret='/srv/ml-shared/other-student/private.csv TOKEN-super-secret'
        with f.db:
            f.db.execute('UPDATE jobs SET last_error=?,checkpoint_path=? WHERE job_id=?',(secret,secret,f.job_id))
            f.db.execute('UPDATE job_attempts SET error=? WHERE job_id=?',(secret,f.job_id))
        recovery_metrics.log_event(f.job_id,'STAGE_FAILURE',details=dict(error=secret,path=secret,credential=secret))
        recovery_metrics.log_event('bobs-job','INPUTS_STAGED',details=dict(rank=0,secret=secret))
        for url in ('/api/overview','/api/jobs/'+f.job_id,'/api/jobs/'+f.job_id+'/logs?view=staging'):
            result=self.http.get(url);self.assertEqual(result.status_code,200)
            text=result.get_data(as_text=True)
            self.assertNotIn(secret,text);self.assertNotIn(str(f.fixture.base),text)
            self.assertNotIn('bobs-job',text);self.assertNotIn(f.key.secret.hex(),text)
        detail=self.detail()
        self.assertNotIn('storage_path',detail['job']);self.assertNotIn('job_config',detail['job'])
        self.assertNotIn('lease_id',detail['leases'][0])

    def test_three_rank_partial_readiness_then_complete_metrics_sum_actual_worker_copies(self):
        import distributed_dispatch as ddp
        f=self.f;nodes=f.ddp_job();observed=[];original=f.runtime.lifecycle.record_ready
        def ready(*args,**kwargs):
            result=original(*args,**kwargs)
            # Rank copies overlap. Observe the committed controller acknowledgment,
            # not a peer RPC's start, which says nothing about another rank's READY.
            observed.append(self.detail()['staging'])
            return result
        with patch.object(f.runtime.lifecycle,'record_ready',side_effect=ready):
            with patch.object(ddp,'select_group',return_value=nodes):
                self.assertTrue(f.dispatch())
        self.assertEqual([(v['ready_ranks'],v['total_ranks'],v['measured_ranks']) for v in observed],
                         [(1,3,1),(2,3,2),(3,3,3)])
        view=self.detail()['staging']
        self.assertEqual((view['ready_ranks'],view['measured_ranks']),(3,3))
        self.assertEqual(view['metrics']['staged_bytes'],sum(rank['metrics']['staged_bytes'] for rank in view['ranks']))
        self.assertEqual(len({rank['metrics']['staged_bytes'] for rank in view['ranks']}),1)

    def test_malformed_signed_metrics_do_not_grant_authority_or_leak_and_receipt_still_required(self):
        f=self.f;original=f.service.stage
        def malformed(body):
            reply=original(body);reply['metrics']=dict(path='/srv/secret',staged_bytes=True)
            return reply
        with patch.object(f.service,'stage',side_effect=malformed):self.assertTrue(f.dispatch())
        view=self.detail()['staging'];self.assertIsNone(view['metrics']);self.assertEqual(view['measured_ranks'],0)
        self.assertEqual(view['ready_ranks'],1)
        self.assertNotIn('/srv/secret',json.dumps(self.detail()))

    def test_lost_authenticated_ack_does_not_claim_ready_or_measurements(self):
        import requests
        f=self.f;original=f.client.post
        def lost(url,**kwargs):
            reply=original(url,**kwargs)
            if url.endswith(STAGE_PATH):raise requests.Timeout('/srv/secret')
            return reply
        with patch.object(f.client,'post',side_effect=lost):
            with self.assertRaises(requests.Timeout):f.dispatch()
        view=self.detail()['staging']
        self.assertEqual(view['ready_ranks'],0);self.assertIsNone(view['metrics']);f.popen.assert_not_called()

    def test_cancel_during_copy_shows_terminal_cleanup_and_never_launches(self):
        f=self.f;original=f.fetcher.ensure
        def cancelled(*args,**kwargs):
            store.control_job(f.job_id,'alice','cancel')
            f.runtime.reconcile(store.get_job(f.job_id))
            return original(*args,**kwargs)
        with patch.object(f.fetcher,'ensure',side_effect=cancelled):
            with self.assertRaises(Exception):f.dispatch()
        f.runtime.reconcile(store.get_job(f.job_id))
        detail=self.detail();self.assertEqual(detail['job']['status'],'CANCELLED')
        self.assertEqual(detail['staging']['phase'],'TERMINAL');self.assertIsNone(detail['staging']['metrics'])
        f.popen.assert_not_called()

    def test_missing_optional_tables_are_readonly_and_not_auto_migrated(self):
        # Separate legacy-style DB, no optional migrations. Real read function,
        # authorizes first, and issues SELECT only even for an NFS held receipt.
        import staging_view
        with sqlite3.connect(':memory:') as db:
            db.execute('CREATE TABLE jobs(job_id,owner,job_type,retry_count)')
            db.execute("INSERT INTO jobs VALUES ('held','alice','custom',0)")
            db.execute('CREATE TABLE job_storage_bindings(job_id,backend)')
            db.execute("INSERT INTO job_storage_bindings VALUES ('held','nfs')")
            db.execute('PRAGMA query_only=ON')
            self.assertIsNone(staging_view.read(db,'held','alice'))
            self.assertEqual(db.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table'").fetchone()[0],2)


if __name__=='__main__':unittest.main(verbosity=2)
