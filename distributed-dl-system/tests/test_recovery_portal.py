"""12.3 actual owner-authorized portal + committed recovery lifecycle fixtures.

Only through tools/run_tests.py --isolated. Fake NFS/launch/isolation; no live Pi.
"""
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
import test_staging_dispatch  # Establish agent package before script imports.
import test_hybrid_checkpoint_recovery as support
import test_student_portal as legacy
import test_staging_portal as restart
import student_portal as portal
import portal_admin
import recovery_view


class RecoveryPortalTests(unittest.TestCase):
    def setUp(self):
        self.f=support.RecoveryTests(methodName='runTest');self.addCleanup(self.f.doCleanups);self.f.setUp()
        for mocked in (patch.object(portal.ClusterView,'get',return_value=[]),patch.object(portal,'dispatcher_running',return_value=False)):
            mocked.start();self.addCleanup(mocked.stop)
        self.auth=self.f.f.fixture.base/'portal'
        for owner in ('alice','bob'):portal.add_user(self.auth,owner,owner+'-password')
        self.app=portal.create_app(self.auth);self.http=self.app.test_client();self.login('alice')

    def login(self,owner):
        csrf=self.http.get('/api/session').json['csrf']
        response=self.http.post('/api/login',json=dict(owner=owner,password=owner+'-password'),headers={'X-CSRF-Token':csrf})
        self.assertEqual(response.status_code,200,response.json)

    def view(self):
        response=self.http.get('/api/jobs/'+self.f.f.job_id)
        self.assertEqual(response.status_code,200,response.json);return response.json['recovery']

    def recovering(self):
        f=self.f;f.cold();saved=f.save();f.fail();self.assertTrue(f.f.dispatch());return saved

    def test_cold_job_no_recovery_then_queue_copy_launch_and_sdk_confirmation(self):
        f=self.f;self.assertIsNone(self.view());f.cold();f.save();f.fail()
        queued=self.view();self.assertEqual(queued['phase'],'QUEUED');self.assertEqual(queued['attempt'],2)
        self.assertEqual(queued['source']['state'],'not_selected');self.assertIsNone(queued['restored_ranks'])
        observed={};original=f.f.fetcher.ensure
        def during_inputs(*args,**kwargs):
            observed['inputs']=self.view();return original(*args,**kwargs)
        original_restore=f.runtime.restorer.stage
        def during_checkpoint(*args,**kwargs):
            observed['checkpoint']=self.view();return original_restore(*args,**kwargs)
        with patch.object(f.f.fetcher,'ensure',side_effect=during_inputs),patch.object(f.runtime.restorer,'stage',side_effect=during_checkpoint):
            self.assertTrue(f.f.dispatch())
        self.assertEqual(observed['inputs']['phase'],'STAGING')
        self.assertEqual(observed['checkpoint']['phase'],'RESTORING')
        pending=self.view();self.assertEqual(pending['phase'],'CONFIRMING')
        self.assertEqual((pending['inputs_ready_ranks'],pending['checkpoint_ready_ranks'],pending['launched_ranks'],pending['restored_ranks']),(1,1,1,0))
        self.assertEqual(pending['source'],dict(state='committed',global_step=5,size_bytes=f.publisher.head('alice',f.f.job_id).size_bytes,from_attempt=1))
        f.ack(bad=True);self.assertEqual(f.runtime.reconcile(f.job()),'RESTORE_PENDING');self.assertEqual(self.view()['restored_ranks'],0)
        f.ack();self.assertEqual(f.runtime.reconcile(f.job()),'RUNNING');view=self.view()
        self.assertEqual(view['phase'],'RESUMED');self.assertEqual(view['restored_ranks'],1)
        self.assertTrue(view['ranks'][0]['training_state_restored'])

    def test_three_rank_progress_counts_real_prepared_copies_and_waits_for_group_ack(self):
        f=self.f;f.ddp();f.cold();f.save();f.fail();copies=[];original=f.runtime.restorer.stage
        def observe(*args,**kwargs):copies.append(self.view());return original(*args,**kwargs)
        with patch.object(f.runtime.restorer,'stage',side_effect=observe):self.assertTrue(f.f.dispatch())
        self.assertEqual([v['checkpoint_ready_ranks'] for v in copies],[0,1,2])
        self.assertTrue(all(v['phase']=='RESTORING' for v in copies))
        self.assertEqual(self.view()['checkpoint_ready_ranks'],3)
        f.ack();f.ack(f.f.extra_services[0]);self.assertEqual(f.runtime.reconcile(f.job()),'RESTORE_PENDING')
        self.assertEqual(self.view()['restored_ranks'],0)
        self.assertTrue(all(not r['training_state_restored'] for r in self.view()['ranks']))
        f.ack(f.f.extra_services[1]);self.assertEqual(f.runtime.reconcile(f.job()),'RUNNING')
        self.assertEqual(self.view()['restored_ranks'],3);self.assertEqual(self.view()['phase'],'RESUMED')

    def test_readonly_poll_survives_controller_recreation_without_relaunch_or_nfs(self):
        f=self.f;self.recovering();f.ack();self.assertEqual(f.runtime.reconcile(f.job()),'RUNNING')
        before=f.f.fixture.db_path.read_bytes();changes=f.db.total_changes
        with patch.object(portal.requests,'get',side_effect=AssertionError('No worker I/O')):
            for _ in range(3):self.assertEqual(self.view()['phase'],'RESUMED')
            self.app=portal.create_app(self.auth);self.http=self.app.test_client();self.login('alice')
            self.assertEqual(self.view()['phase'],'RESUMED')
        self.assertEqual(f.f.fixture.db_path.read_bytes(),before);self.assertEqual(f.db.total_changes,changes)

    def test_new_retry_does_not_reuse_old_stage_or_restore_ack(self):
        f=self.f;self.recovering();f.ack();self.assertEqual(f.runtime.reconcile(f.job()),'RUNNING');f.fail()
        view=self.view();self.assertEqual(view['attempt'],3);self.assertEqual(view['phase'],'QUEUED')
        self.assertEqual(view['ranks'],[]);self.assertIsNone(view['restored_ranks']);self.assertIsNone(view['checkpoint_ready_ranks'])

    def test_stale_outer_job_snapshot_cannot_mix_progress_from_a_new_attempt(self):
        f=self.f;self.recovering();original=portal.store.get_job
        def old_job(identity):
            row=original(identity);return dict(row,retry_count=row['retry_count']-1)
        with patch.object(portal.store,'get_job',side_effect=old_job):
            detail=self.http.get('/api/jobs/'+f.f.job_id).json
            self.assertIsNone(detail['recovery']);self.assertIsNone(detail['staging'])
        self.assertEqual(self.view()['phase'],'CONFIRMING')

    def test_private_progress_logs_and_overview_authorize_each_owner_even_admin(self):
        f=self.f;self.recovering();view=self.view()
        text=json.dumps(view)
        for forbidden in ('sha256','lease','manifest','source_id','publication_id','/srv/',str(f.root),'alice','bob'):
            self.assertNotIn(forbidden,text)
        log=self.http.get('/api/jobs/'+f.f.job_id+'/logs?view=recovery')
        self.assertEqual(log.status_code,200);self.assertIn('step 5',log.json['text']);self.assertIn('0/1',log.json['text'])
        self.assertEqual(self.http.get('/api/overview').json['jobs'][0]['recovery'],view)
        portal_admin.set_admin(self.auth,'bob',enabled=True);self.login('bob')
        for suffix in ('','/logs?view=recovery','/logs?view=staging'):
            self.assertEqual(self.http.get('/api/jobs/'+f.f.job_id+suffix).status_code,404)
        self.assertEqual(self.http.get('/api/overview').json['jobs'],[])
        self.assertEqual(self.app.test_client().get('/api/jobs/'+f.f.job_id+'/logs?view=recovery').status_code,401)

    def test_known_expired_ownership_and_cancellation_do_not_claim_active_restore(self):
        f=self.f;self.recovering()
        with f.db:f.db.execute('UPDATE execution_leases SET expires_at=0,expires_clock=0 WHERE retry_count=1')
        self.assertEqual(self.view()['phase'],'STOPPING')
        support.fixtures.store.control_job(f.f.job_id,'alice','cancel')
        self.assertEqual(self.view()['phase'],'STOPPING')
        self.assertEqual(self.view()['restored_ranks'],0)

    def test_controller_restart_with_pending_sdk_ack_remains_pending(self):
        f=self.f;self.recovering();f.runtime=f.coordinator();f.f.runtime=f.runtime
        self.assertEqual(self.view()['phase'],'CONFIRMING')
        self.assertEqual(f.runtime.reconcile(f.job()),'RESTORE_PENDING')
        f.ack();self.assertEqual(f.runtime.reconcile(f.job()),'RUNNING');self.assertEqual(self.view()['phase'],'RESUMED')

    def test_checkpoint_mode_retry_without_saved_state_is_a_full_restart_not_restore_ack(self):
        f=self.f;f.cold();f.fail();self.assertTrue(f.f.dispatch());view=self.view()
        self.assertEqual(view['phase'],'RESTARTED');self.assertEqual(view['source']['state'],'none')
        self.assertFalse(view['restore_required']);self.assertIsNone(view['restored_ranks'])
        self.assertIsNone(view['checkpoint_ready_ranks'])
        self.assertTrue(all(r['training_state_restored'] is None for r in view['ranks']))


class CompatibilityTests(unittest.TestCase):
    def test_legacy_routes_and_payload_do_not_gain_hybrid_recovery(self):
        f=legacy.PortalTests(methodName='runTest');f.setUp();self.addCleanup(f.tearDown)
        job=f.submit();self.assertNotIn('recovery',f.client.get('/api/jobs/'+job).json)
        self.assertNotIn('recovery',f.client.get('/api/overview').json['jobs'][0])
        self.assertEqual(f.client.get('/api/jobs/'+job+'/logs?view=recovery').status_code,404)

    def test_full_restart_is_never_labeled_checkpoint_resume(self):
        f=restart.StagingPortalTests(methodName='runTest');self.addCleanup(f.doCleanups);f.setUp();worker=f.f
        self.assertTrue(worker.dispatch());job,stage,members=worker.runtime.snapshot(worker.job_id)
        worker.runtime.abort_intent(job,stage,'Disposable restart fixture failure')
        worker.process.poll.return_value=1;worker.service.quiescent=lambda *_:True
        self.assertEqual(worker.runtime.cleanup(job,stage,members),'QUEUED')
        worker.process.poll.return_value=None;worker.agent.current_job=None;worker.agent.job_process=None
        self.assertTrue(worker.dispatch());view=f.http.get('/api/jobs/'+worker.job_id).json['recovery']
        self.assertEqual(view['phase'],'RESTARTED');self.assertFalse(view['restore_required'])
        self.assertEqual(view['source']['state'],'not_required');self.assertIsNone(view['restored_ranks'])

    def test_actual_portal_script_staging_and_recovery_regressions(self):
        executable=os.environ.get('DL_NODE_BINARY','node')
        for filename in ('test_recovery_portal_ui.cjs','test_staging_portal_ui.cjs','test_admin_storage_ui.cjs'):
            result=subprocess.run([executable,str(ROOT/'tests'/filename)],capture_output=True,text=True,timeout=15)
            self.assertEqual(result.returncode,0,result.stdout+result.stderr)


class ReadModelTests(unittest.TestCase):
    def setUp(self):
        self.db=sqlite3.connect(':memory:');self.addCleanup(self.db.close)
        self.db.executescript('''CREATE TABLE jobs(job_id,owner,job_type,retry_count,status,job_config,updated_at);
            CREATE TABLE job_storage_bindings(job_id,backend);
            CREATE TABLE storage_job_inputs(job_id,owner,binding_sha256);
            CREATE TABLE hybrid_restore_attempts(job_id,retry_count,source_id,stage_id,confirmed,binding_sha256);
            CREATE TABLE hybrid_checkpoint_publications(publication_id,job_id,owner,phase,retry_count,binding_sha256,kind,global_step,size_bytes);''')
        spec=json.dumps(dict(recovery=dict(mode='checkpoint'),execution=dict(workers=1)))
        self.db.execute('INSERT INTO jobs VALUES (?,?,?,?,?,?,?)',('j','alice','custom',1,'SCHEDULED',spec,1))
        self.db.execute("INSERT INTO job_storage_bindings VALUES ('j','nfs')")
        self.db.execute("INSERT INTO storage_job_inputs VALUES ('j','alice','binding')")
        self.db.execute("INSERT INTO hybrid_restore_attempts VALUES ('j',1,'pub',NULL,0,'binding')")
        self.db.execute("INSERT INTO hybrid_checkpoint_publications VALUES ('pub','j','alice','COMMITTED',0,'binding','single',5,100)")

    def test_missing_optional_tables_remain_unknown_no_migration_or_false_zero(self):
        self.db.execute('PRAGMA query_only=ON');changes=self.db.total_changes
        view=recovery_view.read(self.db,'j','alice')
        self.assertEqual(view['phase'],'STAGING');self.assertIsNone(view['checkpoint_ready_ranks'])
        self.assertEqual(self.db.total_changes,changes)

    def test_foreign_uncommitted_or_different_version_checkpoint_not_projected(self):
        for field,value in (('owner','bob'),('job_id','other'),('phase','PUBLISHING'),('binding_sha256','foreign'),('kind','ddp'),('size_bytes','/srv/secret')):
            with self.subTest(field=field):
                original=self.db.execute(f'SELECT {field} FROM hybrid_checkpoint_publications').fetchone()[0]
                self.db.execute(f'UPDATE hybrid_checkpoint_publications SET {field}=?',(value,))
                view=recovery_view.read(self.db,'j','alice')
                self.assertEqual(view['source']['state'],'unavailable');self.assertEqual(view['phase'],'UNKNOWN')
                self.assertNotIn('/srv/',json.dumps(view));self.assertIsNone(view['source']['global_step'])
                self.db.execute(f'UPDATE hybrid_checkpoint_publications SET {field}=?',(original,))

    def test_owner_check_precedes_side_table_reads(self):
        observed=[];self.db.set_trace_callback(observed.append)
        self.assertIsNone(recovery_view.read(self.db,'j','bob'))
        self.assertEqual(len(observed),1);self.assertNotIn('hybrid_restore',observed[0])


if __name__=='__main__':unittest.main(verbosity=2)
