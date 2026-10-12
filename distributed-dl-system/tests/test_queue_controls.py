"""Fairness, atomic student quotas, durable controls and cancellation reconciliation."""
import json
import importlib.util
import os
import signal
import subprocess
import time
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch
from concurrent.futures import ThreadPoolExecutor

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT / 'controller'))
import job_store as store
import queue_policy as policy
import dispatcher
import requests


class QueueTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.original = store.DB_FILE
        store.DB_FILE = str(Path(self.temp.name) / 'jobs.db')
        self.file = Path(self.temp.name) / 'policy.json'
        self.file.write_text(json.dumps(policy.DEFAULTS))
        self.patch = patch.object(policy,'POLICY_FILE',self.file)
        self.patch.start()
        store.initialize_database()

    def tearDown(self):
        self.patch.stop()
        store.DB_FILE = self.original
        self.temp.cleanup()

    def job(self,jid,owner,state='QUEUED'):
        store.add_job(jid,1,job_type='custom',status='RECEIVED',owner=owner)
        with store.get_connection() as db:
            db.execute('UPDATE jobs SET status=?,queued_at=? WHERE job_id=?',(state,1,jid))
        return store.get_job(jid)

    def test_owner_turns_fifo_and_service_survive_restart(self):
        self.job('a1','alice');self.job('a2','alice');self.job('b1','bob');self.job('b2','bob')
        self.assertEqual([j['job_id'] for j in store.fair_queue()],['a1','b1','a2','b2'])
        store.mark_scheduled('a1','pi01','ip')
        store.mark_running('a1');store.mark_completed('a1')
        store.initialize_database()
        self.assertEqual(store.fair_queue()[0]['job_id'],'b1')
        store.mark_scheduled('b1','pi01','ip')
        self.assertEqual(store.fair_queue()[0]['job_id'],'a2')

    def test_active_limit_includes_uncertain_start_and_cancelling(self):
        self.job('a1','alice');self.job('a2','alice');self.job('b1','bob')
        store.mark_scheduled('a1','pi01','ip')
        with self.assertRaisesRegex(ValueError,'quota'):
            store.mark_scheduled('a2','pi02','ip')
        self.assertEqual([j['job_id'] for j in store.fair_queue()],['b1'])
        self.assertIn('1/1',store.get_job('a2')['queue_reason'])
        store.control_job('a1','alice','cancel')
        self.assertEqual([j['job_id'] for j in store.fair_queue()],['b1'])
        store.finish_cancellation('a1')
        self.assertIn('a2',[j['job_id'] for j in store.fair_queue()])

    def test_pending_quota_atomic_with_concurrent_submissions(self):
        self.file.write_text(json.dumps({**policy.DEFAULTS,'max_pending_per_owner':1}))
        def submit(jid):
            try:
                store.add_job(jid,1,job_type='custom',status='RECEIVED',owner='alice')
                return True
            except ValueError:
                return False
        with ThreadPoolExecutor(max_workers=2) as workers:
            self.assertEqual(sum(workers.map(submit,['a1','a2'])),1)
        admitted = next(j for j in store.get_all_jobs())
        store.control_job(admitted['job_id'],'alice','cancel')
        self.assertTrue(submit('a3'))

    def test_owner_controls_and_hold_cannot_be_overwritten_by_placement(self):
        self.job('a1','alice')
        for action in ('hold','release','cancel'):
            with self.assertRaises(ValueError): store.control_job('a1','bob',action)
        self.assertEqual(store.control_job('a1','alice','hold'),'HELD')
        self.assertEqual(store.fair_queue(),[])
        with self.assertRaises(ValueError): store.mark_scheduled('a1','pi01','ip')
        self.assertEqual(store.control_job('a1','alice','release'),'QUEUED')
        self.assertEqual(store.control_job('a1','alice','cancel'),'CANCELLED')
        self.assertEqual(store.get_reservations(),[])
        self.assertEqual(store.control_job('a1','alice','cancel'),'CANCELLED')

    def test_cancel_intent_wins_late_start_completion_and_retry(self):
        self.job('a1','alice');store.mark_scheduled('a1','pi01','ip')
        store.control_job('a1','alice','cancel')
        store.mark_running('a1',2)
        self.assertEqual(store.get_job('a1')['status'],'CANCELLING')
        self.assertFalse(store.mark_completed('a1'))
        self.assertEqual(store.requeue_after_failure('a1','exit'),'CANCELLING')
        self.assertEqual(len(store.get_reservations()),1)
        store.finish_cancellation('a1',-15)
        self.assertEqual(store.get_job('a1')['status'],'CANCELLED')
        self.assertEqual(store.get_attempts('a1')[0]['status'],'CANCELLED')
        self.assertEqual(store.get_reservations(),[])

    def test_offline_cancellation_retains_slot_then_confirms_exit(self):
        self.job('a1','alice');store.mark_scheduled('a1','pi01','ip');store.mark_running('a1')
        store.control_job('a1','alice','cancel')
        with patch.object(dispatcher.requests,'get',side_effect=requests.Timeout('offline')):
            dispatcher.reconcile_cancellations()
        self.assertEqual(len(store.get_reservations()),1)
        response = Mock(status_code=200)
        response.json.return_value = {'job':{'attempt_token':'wrong','status':'FAILED'}}
        with patch.object(dispatcher.requests,'get',return_value=response):
            dispatcher.reconcile_cancellations()
        self.assertEqual(len(store.get_reservations()),1)
        response.json.return_value = {'job':{'attempt_token':'a1:0','status':'RUNNING'}}
        with patch.object(dispatcher.requests,'get',return_value=response), patch.object(dispatcher.requests,'post',return_value=Mock()) as stop:
            dispatcher.reconcile_cancellations()
            self.assertEqual(stop.call_args.kwargs['json'],{'attempt_token':'a1:0','force':False})
        self.assertEqual(len(store.get_reservations()),1)
        response.json.return_value = {'job':{'attempt_token':'a1:0','status':'FAILED','return_code':-15}}
        with patch.object(dispatcher.requests,'get',return_value=response), patch('job_artifacts.collect'):
            dispatcher.reconcile_cancellations()
        self.assertEqual(store.get_job('a1')['status'],'CANCELLED')
        self.assertEqual(store.get_reservations(),[])

    def test_worker_cancellation_blocks_late_start_across_reload(self):
        module_spec = importlib.util.spec_from_file_location('tombstone_agent',ROOT / 'agent/agent.py')
        agent = importlib.util.module_from_spec(module_spec)
        module_spec.loader.exec_module(agent)
        base = Path(self.temp.name) / 'worker'
        with patch.object(agent,'BASE_DIR',base), patch.object(agent.subprocess,'Popen') as launch:
            client = agent.app.test_client()
            self.assertEqual(client.post('/jobs/late/stop',json={'attempt_token':'late:0'}).status_code,200)
            self.assertEqual(client.post('/jobs/start',json={'job_id':'late','job_type':'dummy',
                'attempt_token':'late:0','duration':1}).status_code,409)
            launch.assert_not_called()
        restarted = importlib.util.module_from_spec(module_spec)
        module_spec.loader.exec_module(restarted)
        with patch.object(restarted,'BASE_DIR',base), patch.object(restarted.subprocess,'Popen') as launch:
            self.assertEqual(restarted.app.test_client().post('/jobs/start',json={
                'job_id':'late','job_type':'dummy','attempt_token':'late:0','duration':1}).status_code,409)
            launch.assert_not_called()
        self.assertEqual(json.loads((base / 'worker-storage/cancellations/late.json').read_text())['attempt_tokens'],['late:0'])

    def test_missing_attempt_needs_worker_tombstone_before_release(self):
        self.job('a1','alice');store.mark_scheduled('a1','pi01','ip');store.control_job('a1','alice','cancel')
        response = Mock(status_code=404)
        with patch.object(dispatcher.requests,'get',return_value=response), patch.object(dispatcher.requests,'post',side_effect=requests.Timeout('lost stop')):
            dispatcher.reconcile_cancellations()
        self.assertEqual(len(store.get_reservations()),1)
        with patch.object(dispatcher.requests,'get',return_value=response), patch.object(dispatcher.requests,'post',return_value=Mock()) as stop:
            dispatcher.reconcile_cancellations()
            self.assertEqual(stop.call_args.kwargs['json'],{'attempt_token':'a1:0'})
        self.assertEqual(store.get_job('a1')['status'],'CANCELLED')
        self.assertEqual(store.get_reservations(),[])

    def test_worker_cleans_child_that_ignores_termination(self):
        module_spec = importlib.util.spec_from_file_location('queue_agent',ROOT / 'agent/agent.py')
        agent = importlib.util.module_from_spec(module_spec)
        module_spec.loader.exec_module(agent)
        marker = Path(self.temp.name) / 'child.pid'
        log = open(Path(self.temp.name) / 'child.log','w')
        child = "import os,signal,sys,time;from pathlib import Path;signal.signal(signal.SIGTERM,signal.SIG_IGN);Path(sys.argv[1]).write_text(str(os.getpid()));time.sleep(60)"
        parent = "import subprocess,sys,time;subprocess.Popen([sys.executable,'-c',sys.argv[1],sys.argv[2]]);time.sleep(60)"
        process = subprocess.Popen([sys.executable,'-c',parent,child,str(marker)],stdout=log,stderr=log,start_new_session=True)
        try:
            deadline = time.monotonic()+5
            while not marker.exists() and time.monotonic()<deadline: time.sleep(0.02)
            self.assertTrue(marker.exists())
            agent.current_job = {'job_id':'cleanup','pid':process.pid,'status':'RUNNING'}
            agent.job_process = process
            os.killpg(process.pid,signal.SIGTERM)
            agent.watch_job(process,'cleanup',log)
            import psutil
            child_pid = int(marker.read_text())
            deadline = time.monotonic()+3
            while time.monotonic()<deadline:
                try:
                    if psutil.Process(child_pid).status() == psutil.STATUS_ZOMBIE: break
                except psutil.NoSuchProcess: break
                time.sleep(0.02)
            else: self.fail('Child still running after launcher exited')
            self.assertIsNone(agent.job_process)
            self.assertEqual(agent.current_job['status'],'FAILED')
        finally:
            try: os.killpg(process.pid,signal.SIGKILL)
            except ProcessLookupError: pass
            process.wait(timeout=5)
            log.close()

    def test_aging_promotes_waiting_owner_and_overrides_are_validated(self):
        queued = [{'job_id':'a1','job_type':'custom','owner':'alice','created_at':1,'queued_at':1},
                  {'job_id':'b1','job_type':'custom','owner':'bob','created_at':400,'queued_at':400}]
        self.assertEqual(policy.fair_order(queued,[],{'alice':450},policy.DEFAULTS,500)[0]['owner'],'alice')
        self.file.write_text(json.dumps({**policy.DEFAULTS,'owners':{'alice':{'max_active_per_owner':2}}}))
        self.job('a1','alice');self.job('a2','alice');store.mark_scheduled('a1','pi01','ip');store.mark_scheduled('a2','pi02','ip')
        self.file.write_text(json.dumps({**policy.DEFAULTS,'max_active_per_owner':True}))
        with self.assertRaises(ValueError): policy.load_policy()


if __name__ == '__main__':
    unittest.main()
