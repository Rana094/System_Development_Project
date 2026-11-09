"""12.4 offline acceptance. Use tools/run_tests.py --isolated, never live DBs."""
import json
import logging
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
import test_admin_storage as admin
import test_storage_events as event_support
from hybrid_storage import events
from hybrid_storage import admin_observations as observations
import admin_storage_view
import portal_admin


class PermissionAndGrowthTests(unittest.TestCase):
    def staged(self):
        f=admin.staged.StagingPortalTests(methodName='runTest');self.addCleanup(f.doCleanups);f.setUp()
        self.assertTrue(f.f.dispatch());return f

    def test_multi_student_and_admin_matrix_on_all_progress_routes(self):
        f=self.staged();job=f.f.job_id
        for owner,granted in (('alice',False),('bob',False),('bob',True),('alice',True)):
            portal_admin.set_admin(f.auth,owner,enabled=granted);f.login(owner)
            for suffix in ('','/logs?view=staging','/logs?view=recovery'):
                response=f.http.get('/api/jobs/'+job+suffix)
                self.assertEqual(response.status_code,200 if owner=='alice' else 404,(owner,granted,suffix,response.json))
                self.assertEqual(response.headers['Cache-Control'],'no-store')
            self.assertEqual(len(f.http.get('/api/overview').json['jobs']),int(owner=='alice'))
            self.assertEqual(f.http.get('/api/admin/storage').status_code,200 if granted else 403)
        portal_admin.set_admin(f.auth,'alice',enabled=False)
        with f.http.session_transaction() as session:session['is_admin']=True
        self.assertEqual(f.http.get('/api/admin/storage').status_code,403)
        for url in ('/api/admin/storage','/api/jobs/'+job,'/api/jobs/'+job+'/logs?view=recovery'):
            self.assertEqual(f.app.test_client().get(url).status_code,401)

    def test_repeated_ui_polls_and_event_emission_do_not_grow_or_write_sqlite(self):
        f=self.staged();portal_admin.set_admin(f.auth,'alice',enabled=True)
        before=f.f.fixture.db_path.read_bytes();changes=f.f.db.total_changes
        with patch.object(admin.portal.requests,'get',side_effect=AssertionError('No remote reads')),event_support.capture() as rows:
            for _ in range(30):
                for url in ('/api/admin/storage','/api/overview','/api/jobs/'+f.f.job_id,
                            '/api/jobs/'+f.f.job_id+'/logs?view=staging','/api/jobs/'+f.f.job_id+'/logs?view=recovery'):
                    self.assertEqual(f.http.get(url).status_code,200,url)
                events.emit('storage.staging.started',job_id=f.f.job_id,attempt_id=f.f.job_id+':0')
        self.assertEqual(len(rows),30);self.assertEqual(f.f.fixture.db_path.read_bytes(),before)
        self.assertEqual(f.f.db.total_changes,changes)
        print('EVIDENCE poll_growth '+json.dumps(dict(requests=150,events=30,before_bytes=len(before),
            after_bytes=f.f.fixture.db_path.stat().st_size,db_bytes_identical=True)),flush=True)

    def test_expensive_admin_query_is_interrupted_and_fails_closed(self):
        f=self.staged();portal_admin.set_admin(f.auth,'alice',enabled=True)
        def expensive(db,*args,**kwargs):
            db.execute('WITH RECURSIVE x(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM x WHERE n<100000000) SELECT sum(n) FROM x').fetchone()
            self.fail('Query budget did not interrupt')
        before=f.f.fixture.db_path.read_bytes();start=time.monotonic()
        with patch.object(admin_storage_view,'read',side_effect=expensive):response=f.http.get('/api/admin/storage')
        self.assertEqual(response.status_code,503);self.assertLess(time.monotonic()-start,5)
        self.assertNotIn('interrupted',response.get_data(as_text=True))
        self.assertEqual(f.f.fixture.db_path.read_bytes(),before)

    def test_malformed_history_timestamp_is_discarded_without_leaking_text(self):
        f=self.staged();portal_admin.set_admin(f.auth,'alice',enabled=True)
        with f.f.db:
            f.f.db.execute("UPDATE recovery_events SET event_time='/srv/other-student/secret' WHERE event_type='INPUTS_STAGED'")
        response=f.http.get('/api/admin/storage');self.assertEqual(response.status_code,200)
        self.assertNotIn('other-student',response.get_data(as_text=True))
        self.assertEqual(response.json['workers'][0]['transfer_history'],[])
        for at in (float('inf'),-1,253402300800,0):
            with f.f.db:f.f.db.execute("UPDATE recovery_events SET event_time=? WHERE event_type='INPUTS_STAGED'",(at,))
            history=f.http.get('/api/admin/storage').json['workers'][0]['transfer_history']
            self.assertEqual(len(history),int(at==0))
            if at==0:self.assertEqual(history[0]['recorded_at'],0)

    def test_transfer_scan_and_duplicates_are_bounded_even_with_large_history(self):
        f=self.staged();portal_admin.set_admin(f.auth,'alice',enabled=True)
        row=f.f.db.execute("SELECT job_id,event_time,node_id,event_type,details_json FROM recovery_events WHERE event_type='INPUTS_STAGED'").fetchone()
        with f.f.db:
            f.f.db.executemany('INSERT INTO recovery_events(job_id,event_time,node_id,event_type,details_json) VALUES(?,?,?,?,?)',[tuple(row)]*1000)
        before=f.f.fixture.db_path.read_bytes();view=f.http.get('/api/admin/storage').json
        self.assertEqual(view['transfer_scan_limit'],300);self.assertEqual(len(view['workers'][0]['transfer_history']),1)
        self.assertLess(len(json.dumps(view)),16000);self.assertEqual(f.f.fixture.db_path.read_bytes(),before)


class InventoryAcceptanceTests(unittest.TestCase):
    def setUp(self):
        self.f=admin.InventoryViewTests(methodName='runTest');self.addCleanup(self.f.doCleanups);self.f.setUp()

    def test_one_hundred_signed_heartbeats_update_in_place_without_history_growth(self):
        f=self.f.f;self.assertTrue(f.poll())
        def counts():
            return {name:f.db.execute('SELECT COUNT(*) FROM '+name).fetchone()[0] for name in
                    ('cache_registry_sessions','cache_registry_entries','cache_registry_events')}
        before=counts();pages=f.db.execute('PRAGMA page_count').fetchone()[0]
        for _ in range(100):self.assertTrue(f.poll())
        self.assertEqual(counts(),before)
        self.assertLessEqual(f.db.execute('PRAGMA page_count').fetchone()[0],pages+2)
        self.assertEqual(self.f.view()['workers'][0]['nfs']['status'],'verified_readonly')
        path=f.fixture.base/observations.FILENAME
        self.assertLess(path.stat().st_size,observations.MAX_BYTES)
        self.assertEqual(list(f.fixture.base.glob('.admin-observations-*.tmp')),[])
        print('EVIDENCE heartbeat_growth '+json.dumps(dict(heartbeats=100,before_counts=before,after_counts=counts(),
            before_pages=pages,after_pages=f.db.execute('PRAGMA page_count').fetchone()[0],snapshot_bytes=path.stat().st_size)),flush=True)

    def test_out_of_order_or_duplicate_event_cannot_refresh_snapshot(self):
        f=self.f.f;request,body,signature=f.exchange();f.owner.accept('pi01',request,body,signature)
        path=f.fixture.base/observations.FILENAME;before=path.read_bytes()
        f.clock.advance(2);self.assertEqual(f.owner.accept('pi01',request,body,signature),'duplicate')
        self.assertEqual(path.read_bytes(),before)
        newer,newbody,newsig=f.exchange();f.owner.accept('pi01',newer,newbody,newsig);before=path.read_bytes()
        with self.assertRaises(admin.protocol.AuthenticationError):f.owner.accept('pi01',request,body,signature)
        self.assertEqual(path.read_bytes(),before)
        f.clock.advance(16);row=self.f.view()['workers'][0]
        self.assertEqual(row['inventory_status'],'stale_or_untrusted');self.assertEqual(row['nfs']['status'],'unknown')

    def test_health_clock_skew_stale_future_and_controller_clock_rollback(self):
        f=self.f.f
        for delta in (-31,31):
            f.reporter.nfs_health=lambda:dict(schema_version=1,status='verified_readonly',observed_at_ns=f.clock()+delta*10**9)
            self.assertTrue(f.poll());self.assertEqual(self.f.view()['workers'][0]['nfs']['status'],'unknown')
        f.reporter.nfs_health=lambda:dict(schema_version=1,status='verified_readonly',observed_at_ns=f.clock())
        self.assertTrue(f.poll());now=f.clock();f.clock.advance(-1)
        row=self.f.view()['workers'][0];self.assertEqual(row['inventory_status'],'stale_or_untrusted')
        self.assertIsNone(row['dataset_coverage']);self.assertIsNone(row['inventory_age_seconds'])
        f.clock.now=now+31*10**9;self.assertEqual(self.f.view()['workers'][0]['nfs']['status'],'unknown')

    def test_oversize_malformed_and_insecure_private_snapshot_fail_closed(self):
        f=self.f.f;self.assertTrue(f.poll());path=f.fixture.base/observations.FILENAME
        for raw in (b'[]',b'{',b'x'*(observations.MAX_BYTES+1)):
            path.write_bytes(raw);self.assertEqual(observations.read(f.fixture.base),{})
            self.assertEqual(self.f.view()['workers'][0]['nfs']['status'],'unknown')
        self.assertTrue(f.poll());path.chmod(0o644)
        self.assertEqual(observations.read(f.fixture.base),{})


class EventAndUITests(unittest.TestCase):
    def test_offline_overhead_tool_validates_actual_jsonl_and_argument_bounds(self):
        tool=str(ROOT/'tools/benchmarks/observability_overhead.py')
        result=subprocess.run([sys.executable,tool,'--iterations','10','--repeats','2'],capture_output=True,text=True,timeout=15)
        self.assertEqual(result.returncode,0,result.stderr);value=json.loads(result.stdout)
        self.assertEqual(value['validated_jsonl_records'],20)
        self.assertEqual(set(value['ns_per_call']),{'empty_call','disabled','enabled_memory_jsonl'})
        result=subprocess.run([sys.executable,tool,'--iterations','0'],capture_output=True,text=True,timeout=15)
        self.assertEqual(result.returncode,2)

    def test_jsonl_experiment_reader_keeps_exact_units_and_deduplicates_observations(self):
        # Consume real emitted JSON, including replicated/out-of-order delivery.
        common=dict(job_id='experiment-job',attempt_id='experiment-job:0',worker_id='pi02',rank=0,stage_id='a'*32)
        with event_support.capture() as captured:
            for wall in (1794268800123456789,1794268799123456789):
                with patch.object(events.time,'time_ns',return_value=wall):
                    events.emit('storage.staging.ready',**common,cache_hit_bytes=0,transferred_bytes=502*1024*1024,
                        verified_bytes=502*1024*1024,staging_duration_ns=2_000_000_001,verification_duration_ns=100,copy_duration_ns=200)
        lines='\n'.join(json.dumps(row) for row in (captured[1],captured[0],captured[1]))
        event_ids=set();stages={}
        for line in lines.splitlines():
            row=json.loads(line)
            if row['event_id'] in event_ids:continue
            event_ids.add(row['event_id'])
            key=tuple(row[k] for k in ('job_id','attempt_id','worker_id','rank','stage_id'))
            stages.setdefault(key,row)  # first received READY, not event wall-time authority
        self.assertEqual(len(event_ids),2);self.assertEqual(len(stages),1)
        row=next(iter(stages.values()));self.assertEqual(row['transferred_bytes'],526385152)
        self.assertEqual(row['occurred_at_ns'],1794268799123456789)
        self.assertEqual(row['staging_duration_ns'],2000000001)

    def test_operator_rotation_can_bound_log_disk_without_database_or_runtime_changes(self):
        logger=events.LOGGER;state=(logger.level,logger.handlers[:],logger.propagate,logger.disabled)
        with tempfile.TemporaryDirectory() as folder:
            handler=RotatingFileHandler(Path(folder)/'events.jsonl',maxBytes=2048,backupCount=2)
            handler.setFormatter(logging.Formatter('%(message)s'))
            try:
                logger.handlers=[handler];logger.propagate=False;logger.disabled=False;logger.setLevel(logging.INFO)
                for _ in range(100):events.emit('storage.staging.started',job_id='rotation',attempt_id='rotation:0')
            finally:
                handler.close();logger.setLevel(state[0]);logger.handlers=state[1];logger.propagate=state[2];logger.disabled=state[3]
            paths=list(Path(folder).glob('events.jsonl*'));self.assertEqual(len(paths),3)
            self.assertLessEqual(sum(p.stat().st_size for p in paths),3*2048)
            for p in paths:
                for line in p.read_text().splitlines():self.assertEqual(json.loads(line)['schema_version'],1)

    def test_real_event_timestamps_do_not_change_with_process_timezone(self):
        old=os.environ.get('TZ')
        try:
            for zone in ('UTC','Asia/Dhaka','America/New_York'):
                os.environ['TZ']=zone;time.tzset()
                with event_support.capture() as rows,patch.object(events.time,'time_ns',return_value=1794268800123456789):
                    events.emit('storage.staging.started',job_id='tz',attempt_id='tz:0')
                self.assertEqual(rows[0]['occurred_at_ns'],1794268800123456789)
        finally:
            if old is None:os.environ.pop('TZ',None)
            else:os.environ['TZ']=old
            time.tzset()

    def test_actual_js_in_three_timezones_and_existing_permission_races(self):
        node=os.environ.get('DL_NODE_BINARY','node')
        for zone in ('UTC','Asia/Dhaka','America/New_York'):
            result=subprocess.run([node,str(ROOT/'tests/test_observability_ui.cjs')],env=dict(os.environ,TZ=zone),capture_output=True,text=True,timeout=15)
            self.assertEqual(result.returncode,0,result.stdout+result.stderr)
        for name in ('test_admin_storage_ui.cjs','test_recovery_portal_ui.cjs','test_staging_portal_ui.cjs'):
            result=subprocess.run([node,str(ROOT/'tests'/name)],capture_output=True,text=True,timeout=15)
            self.assertEqual(result.returncode,0,result.stdout+result.stderr)


if __name__=='__main__':unittest.main(verbosity=2)
