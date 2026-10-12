"""Check admission, exclusive durable placement and mode gates using an isolated database."""
import json
import importlib.util
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT / 'controller'))
import job_store as store
import scheduler
import dispatcher
import requests


class AllocationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.original = store.DB_FILE
        store.DB_FILE = str(Path(self.temp.name) / 'jobs.db')
        store.initialize_database()

    def tearDown(self):
        store.DB_FILE = self.original
        self.temp.cleanup()

    def test_exclusive_reservation_and_definitive_release(self):
        store.add_job('a',1)
        store.add_job('b',1)
        store.mark_scheduled('a','pi01','ip',{'cpu_cores':1})
        # Upgrade/restart preserves an uncertain SCHEDULED allocation.
        store.initialize_database()
        self.assertEqual(store.get_reservations()[0]['job_id'],'a')
        with self.assertRaises(Exception):
            store.mark_scheduled('b','pi01','ip')
        self.assertEqual(store.get_job('b')['status'],'QUEUED')
        store.return_to_queue('a','definite rejection')
        store.mark_scheduled('b','pi01','ip')
        store.mark_running('b')
        store.mark_completed('b')
        self.assertEqual(store.get_reservations(),[])
        store.mark_scheduled('a','pi01','ip')
        store.mark_running('a')
        store.requeue_after_failure('a','process exited')
        self.assertEqual(store.get_reservations(),[])

    def test_agent_custom_snapshot_admission_and_idempotent_start(self):
        module_spec = importlib.util.spec_from_file_location('allocation_agent', ROOT / 'agent/agent.py')
        agent = importlib.util.module_from_spec(module_spec)
        module_spec.loader.exec_module(agent)
        base = Path(self.temp.name) / 'worker'
        directory = base / 'worker-storage/jobs/custom'
        directory.mkdir(parents=True)
        (base / 'logs').mkdir()
        (base / 'agent').mkdir()
        (base / 'agent/custom_runner.py').write_text('# test launcher')
        allocation = {'cpu_cores':1,'memory_gb':0.001,'gpus':0}
        record = {'job_id':'custom','manifest_sha256':'snapshot','environment_key':'env'}
        descriptor = {'manifest_sha256':'snapshot','manifest':{'execution_spec':{
            'execution':{'mode':'single','workers':1},'recovery':{'mode':'restart'},
            'resources_per_worker':allocation}}}
        (directory / 'prepared.json').write_text(json.dumps(record))
        (directory / 'request.json').write_text(json.dumps(descriptor))
        payload = {'job_id':'custom','job_type':'custom','attempt_token':'custom:0',
                   'manifest_sha256':'snapshot','environment_key':'env','resources':allocation}
        with patch.object(agent,'BASE_DIR',base), patch.object(agent,'LOG_DIR',base / 'logs'), \
             patch.object(agent,'GPU_COUNT',0), patch.object(agent,'get_ip_address',return_value=None), \
             patch.object(agent,'get_network_speed',return_value={'upload_mbps':0,'download_mbps':0}), \
             patch.object(agent.subprocess,'Popen') as launch, \
             patch.object(agent.threading,'Thread'):
            launch.return_value.poll.return_value = None
            launch.return_value.pid = 123
            client = agent.app.test_client()
            self.assertEqual(client.post('/jobs/start',json={**payload,'manifest_sha256':'wrong'}).status_code,400)
            launch.assert_not_called()
            too_large = {'cpu_cores':len(__import__('os').sched_getaffinity(0))+1,'memory_gb':0.001,'gpus':0}
            descriptor['manifest']['execution_spec']['resources_per_worker'] = too_large
            (directory / 'request.json').write_text(json.dumps(descriptor))
            self.assertEqual(client.post('/jobs/start',json={**payload,'resources':too_large}).status_code,409)
            launch.assert_not_called()
            descriptor['manifest']['execution_spec']['resources_per_worker'] = allocation
            (directory / 'request.json').write_text(json.dumps(descriptor))
            self.assertEqual(client.post('/jobs/start',json=payload).status_code,200)
            self.assertEqual(client.post('/jobs/start',json=payload).status_code,200)
            launch.assert_called_once()
            self.assertEqual(client.post('/jobs/start',json={**payload,'job_id':'other','attempt_token':'other:0'}).status_code,409)
            r = client.get('/resources').get_json()
            self.assertEqual(r['slots']['available'],0)
            self.assertEqual(r['allocation'],allocation)
            self.assertFalse(agent.valid_job_id('..'))
            self.assertFalse(agent.valid_job_id('.'))
            # Closing the mocked launch's file avoids leaking handles in this test.
            launch.call_args.kwargs['stdout'].close()

    def test_uncertain_http_start_keeps_reservation_until_reconciled(self):
        store.add_job('uncertain',1)
        job = store.get_job('uncertain')
        selected = {'node':{'id':'pi01','ip':'ip','port':5000}}
        with patch.object(dispatcher,'select_best_node',return_value=selected), \
             patch.object(dispatcher.requests,'post',side_effect=requests.Timeout('lost response')):
            self.assertFalse(dispatcher.dispatch_one_job(job))
        self.assertEqual(store.get_job('uncertain')['status'],'SCHEDULED')
        self.assertEqual(store.get_reservations()[0]['job_id'],'uncertain')
        with patch.object(dispatcher.requests,'get',side_effect=requests.Timeout('offline')):
            dispatcher.reconcile_scheduled_jobs()
        self.assertEqual(len(store.get_reservations()),1)
        response = unittest.mock.Mock(status_code=404)
        with patch.object(dispatcher.requests,'get',return_value=response):
            dispatcher.reconcile_scheduled_jobs()
        self.assertEqual(store.get_job('uncertain')['status'],'QUEUED')
        self.assertEqual(store.get_reservations(),[])
        dispatcher.unhealthy_nodes.clear()

    def test_capacity_and_reserved_worker_filter(self):
        snapshot = {'node_state':'IDLE','cpu':{'logical_cores':4,'usage_percent':1},
                    'memory':{'available_gb':3},'gpu':{'available':0},'temperature_c':40}
        requirement = {'cpu_cores':2,'memory_gb':2,'gpus':0}
        self.assertTrue(scheduler.resources_fit(snapshot,requirement))
        for key,value in [('cpu_cores',5),('memory_gb',4),('gpus',1)]:
            self.assertFalse(scheduler.resources_fit(snapshot,{**requirement,key:value}))
        with patch.object(scheduler,'load_nodes',return_value=[{'id':'pi01'},{'id':'pi02'}]), patch.object(scheduler,'get_resources',return_value=snapshot):
            result = scheduler.select_best_node(requirements=requirement,reserved_nodes={'pi01'})
            self.assertEqual(result['node']['id'],'pi02')
            self.assertIsNone(scheduler.select_best_node(requirements={**requirement,'gpus':1}))

    def ready(self,execution,recovery):
        spec = {'execution':execution,'recovery':recovery}
        store.add_job('custom',0,job_type='custom',status='RECEIVED',job_config=spec,owner='s')
        with store.get_connection() as db:
            db.execute("UPDATE jobs SET status='READY' WHERE job_id='custom'")
            db.execute("INSERT INTO job_environments VALUES(?,?,?,?,?,?,?)",
                       ('custom','pi01','isolated','key','{}',1,1))

    def test_enqueue_requires_supported_execution(self):
        self.ready({'mode':'distributed','workers':2},{'mode':'restart'})
        with self.assertRaisesRegex(ValueError,'step 7'):
            store.enqueue_custom_job('custom',{'pi01'})
        with store.get_connection() as db:
            db.execute("UPDATE jobs SET job_config=?",(json.dumps({'execution':{'mode':'single','workers':1},'recovery':{'mode':'checkpoint'}}),))
        store.enqueue_custom_job('custom',{'pi01'})
        self.assertEqual(store.get_job('custom')['status'],'QUEUED')

    def test_enqueue_requires_configured_prepared_candidate(self):
        self.ready({'mode':'single','workers':1},{'mode':'restart'})
        with self.assertRaises(ValueError):
            store.enqueue_custom_job('custom',{'pi02'})
        store.enqueue_custom_job('custom',{'pi01'})
        self.assertEqual(store.get_job('custom')['status'],'QUEUED')
        with self.assertRaises(ValueError):
            store.enqueue_custom_job('custom',{'pi01'})


if __name__ == '__main__':
    unittest.main()
