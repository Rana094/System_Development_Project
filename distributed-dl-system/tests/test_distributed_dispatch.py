"""Verify gang reservations, uncertain starts, peer cleanup and real DDP gradients."""
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'controller'))
import job_store as store
import distributed_dispatch as ddp


class GroupTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.original = store.DB_FILE
        store.DB_FILE = str(Path(self.temp.name)/'jobs.db')
        store.initialize_database()
        self.nodes = [{'id':'pi01','ip':'192.0.2.1','port':5000},
                      {'id':'pi02','ip':'192.0.2.2','port':5000}]
        self.spec = {'execution':{'mode':'distributed','workers':2},
                     'recovery':{'mode':'restart'},
                     'resources_per_worker':{'cpu_cores':1,'memory_gb':1,'gpus':0}}
        store.add_job('group',1,job_type='custom',job_config=self.spec,status='RECEIVED',owner='student')
        with store.get_connection() as db:
            db.execute("UPDATE jobs SET status='QUEUED',storage_manifest_sha256='manifest' WHERE job_id='group'")
            for node in self.nodes:
                db.execute('INSERT INTO job_environments VALUES(?,?,?,?,?,?,?)',
                    ('group',node['id'],'pytorch-cpu','env',json.dumps({
                        'compatibility_key':'same','environment_key':'env'}),1,1))
        self.context = {'world_size':2,'master_addr':'192.0.2.1','master_port':29611}

    def tearDown(self):
        store.DB_FILE = self.original
        self.temp.cleanup()

    def reserve(self):
        store.mark_scheduled('group','pi01','192.0.2.1',self.spec['resources_per_worker'],
                             self.nodes,self.context)

    def worker(self, rank, status='RUNNING'):
        return {'attempt_token':'group:0','ddp':{**self.context,'rank':rank},
                'status':status,'started_at':1,'return_code':0 if status=='COMPLETED' else None}

    def test_atomic_gang_and_migration_preserve_slots(self):
        store.add_job('other',1)
        store.mark_scheduled('other','pi02','ip')
        with self.assertRaises(Exception):
            self.reserve()
        self.assertEqual(store.get_job('group')['status'],'QUEUED')
        self.assertEqual(store.get_ranks('group'),[])
        self.assertEqual([r['node_id'] for r in store.get_reservations()],['pi02'])
        store.return_to_queue('other','not started')
        self.reserve()
        store.initialize_database()
        self.assertEqual(len(store.get_reservations()),2)
        self.assertEqual(len(store.get_ranks('group')),2)
        # Simulate the pre-DDP table with an active single-worker reservation.
        with store.get_connection() as db:
            db.execute('DELETE FROM resource_reservations')
            db.execute('DROP TABLE resource_reservations')
            db.execute('CREATE TABLE resource_reservations (node_id TEXT PRIMARY KEY, '
                       'job_id TEXT NOT NULL UNIQUE,resources_json TEXT NOT NULL,created_at REAL NOT NULL)')
            db.execute("INSERT INTO resource_reservations VALUES('pi01','group','{}',1)")
        store.initialize_database()
        self.assertEqual(len(store.get_reservations()),2)

    def test_compatibility_and_one_quota_slot_per_group(self):
        snapshot={'node_state':'IDLE','cpu':{'available_cores':4,'usage_percent':1},
                  'memory':{'available_gb':3},'gpu':{'available':0}}
        with patch.object(ddp,'load_nodes',return_value=self.nodes), \
             patch.object(ddp,'get_resources',return_value=snapshot):
            self.assertEqual(len(ddp.select_group(store.get_job('group'),set())),2)
            self.assertIsNone(ddp.select_group(store.get_job('group'),{'pi02'}))
            with store.get_connection() as db:
                db.execute('UPDATE job_environments SET record_json=? WHERE node_id=?',
                           (json.dumps({'compatibility_key':'different'}),'pi02'))
            self.assertIsNone(ddp.select_group(store.get_job('group'),set()))
        policy={'max_active_per_owner':1,'max_pending_per_owner':20,'aging_seconds':300,'owners':{}}
        with patch.object(store,'load_policy',return_value=policy):
            self.reserve()
            store.add_job('second',1,job_type='custom',job_config=self.spec,status='RECEIVED',owner='student')
            with store.get_connection() as db:
                db.execute("UPDATE jobs SET status='QUEUED' WHERE job_id='second'")
            with self.assertRaisesRegex(ValueError,'quota'):
                store.mark_scheduled('second','pi03','ip')

    def test_checkpoint_retries_are_allowed_and_invalid_rank_settings_rejected(self):
        with store.get_connection() as db:
            db.execute("UPDATE jobs SET status='READY' WHERE job_id='group'")
        store.enqueue_custom_job('group',{'pi01','pi02'})
        self.spec['recovery']['mode'] = 'checkpoint'
        self.spec['recovery']['max_retries'] = 1
        with store.get_connection() as db:
            db.execute("UPDATE jobs SET status='READY',job_config=? WHERE job_id='group'",
                       (json.dumps(self.spec),))
        store.enqueue_custom_job('group',{'pi01','pi02'})
        self.spec['recovery']['max_retries'] = 0
        with store.get_connection() as db:
            db.execute("UPDATE jobs SET status='READY',job_config=? WHERE job_id=?",(json.dumps(self.spec),'group'))
        store.enqueue_custom_job('group',{'pi01','pi02'})
        sys.path.insert(0,str(ROOT/'agent'))
        from custom_runner import validate_execution
        self.spec['recovery']['mode'] = 'restart'
        validate_execution(self.spec,{**self.context,'rank':0})
        for context in (None,{**self.context,'rank':2},
                        {**self.context,'rank':0,'master_port':0},
                        {**self.context,'rank':0,'world_size':3}):
            with self.assertRaises(ValueError):
                validate_execution(self.spec,context)

    def test_uncertain_start_and_all_rank_completion(self):
        self.reserve()
        response = unittest.mock.Mock(status_code=200)
        response.json.return_value={'job':self.worker(0)}
        with patch.object(ddp,'worker',return_value=None), \
             patch.object(ddp.requests,'post',side_effect=[response,requests.Timeout('lost reply')]):
            ddp.progress(store.get_job('group'))
        self.assertEqual(store.get_job('group')['status'],'SCHEDULED')
        self.assertEqual(len(store.get_reservations()),2)
        with patch.object(ddp,'worker',side_effect=[self.worker(0),self.worker(1)]):
            ddp.progress(store.get_job('group'))
        self.assertEqual(store.get_job('group')['status'],'RUNNING')
        self.assertEqual(len(store.get_attempts('group')),1)
        with patch.object(ddp,'worker',side_effect=[self.worker(0,'COMPLETED'),self.worker(1)]):
            ddp.progress(store.get_job('group'))
        self.assertEqual(len(store.get_reservations()),2)
        with patch.object(ddp,'worker',side_effect=[self.worker(0,'COMPLETED'),self.worker(1,'COMPLETED')]), \
             patch.object(ddp,'collect_ranks',return_value=True):
            ddp.progress(store.get_job('group'))
        self.assertEqual(store.get_job('group')['status'],'COMPLETED')
        self.assertEqual(store.get_reservations(),[])

    def test_partial_rejection_stops_every_rank_before_retry(self):
        self.reserve()
        response = unittest.mock.Mock(status_code=409)
        with patch.object(ddp,'worker',side_effect=[self.worker(0),None]), \
             patch.object(ddp.requests,'post',return_value=response):
            ddp.progress(store.get_job('group'))
        self.assertEqual(ddp.group(store.get_job('group'))['phase'],'STOPPING')
        with patch.object(ddp.requests,'post',side_effect=[unittest.mock.Mock(),requests.Timeout('offline')]), \
             patch.object(ddp,'worker',return_value=None):
            ddp.progress(store.get_job('group'))
        self.assertEqual(len(store.get_reservations()),2)
        with patch.object(ddp.requests,'post') as stop, patch.object(ddp,'worker',return_value=None), \
             patch.object(ddp,'collect_ranks',return_value=True):
            ddp.progress(store.get_job('group'))
        self.assertEqual(stop.call_count,2)
        self.assertEqual(store.get_job('group')['retry_count'],1)
        self.assertEqual(store.get_job('group')['status'],'QUEUED')
        self.assertEqual(store.get_reservations(),[])

    def test_cancel_and_unreachable_rank_hold_allocations(self):
        self.reserve()
        with patch.object(ddp,'worker',side_effect=[self.worker(0),self.worker(1)]):
            ddp.progress(store.get_job('group'))
        with patch.object(ddp,'worker',side_effect=requests.Timeout('offline')):
            ddp.progress(store.get_job('group'))
        self.assertEqual(len(store.get_reservations()),2)
        store.control_job('group','student','cancel')
        with patch.object(ddp.requests,'post'), patch.object(ddp,'worker',return_value=None), \
             patch.object(ddp,'collect_ranks',return_value=True):
            ddp.progress(store.get_job('group'))
        self.assertEqual(store.get_job('group')['status'],'CANCELLED')
        self.assertEqual(store.get_reservations(),[])

    def test_real_two_process_ddp(self):
        with socket.socket() as listener:
            listener.bind(('127.0.0.1',0))
            port=listener.getsockname()[1]
        processes=[]
        logs=[]
        try:
            for rank in range(2):
                directory=Path(self.temp.name)/f'rank-{rank}'
                directory.mkdir()
                log=(directory/'training.log').open('w');logs.append(log)
                env=dict(os.environ,MASTER_ADDR='127.0.0.1',MASTER_PORT=str(port),
                         RANK=str(rank),WORLD_SIZE='2',LOCAL_RANK='0',OMP_NUM_THREADS='1',
                         GLOO_SOCKET_IFNAME='lo',DL_JOB_ID='local-ddp',DL_OUTPUT_DIR=str(directory),
                         DL_DATASET_PATH=str(ROOT/'examples/binary-classification.csv'))
                processes.append(subprocess.Popen([sys.executable,str(ROOT/'examples/ddp_project/train.py'),
                    '--epochs','2','--batch-delay','0'],env=env,stdout=log,stderr=subprocess.STDOUT))
            for process in processes:
                self.assertEqual(process.wait(timeout=45),0)
            summaries=[json.loads((Path(self.temp.name)/f'rank-{r}'/'summary.json').read_text()) for r in range(2)]
            self.assertEqual(summaries[0]['final_model_sha256'],summaries[1]['final_model_sha256'])
            self.assertNotEqual(summaries[0]['initial_model_sha256'],summaries[0]['final_model_sha256'])
            shards=[set(s['first_epoch_indices']) for s in summaries]
            self.assertFalse(shards[0]&shards[1])
            self.assertEqual(len(shards[0]|shards[1]),256)
            self.assertTrue(all(s['parameters_synchronized'] and s['training_steps']==8 for s in summaries))
            single = Path(self.temp.name)/'individual'
            single.mkdir()
            env.update(WORLD_SIZE='1',RANK='0',DL_OUTPUT_DIR=str(single))
            subprocess.run([sys.executable,str(ROOT/'examples/ddp_project/train.py'),
                            '--epochs','2','--batch-delay','0'],env=env,check=True,
                           stdout=subprocess.DEVNULL,stderr=subprocess.PIPE,timeout=30)
            baseline = json.loads((single/'summary.json').read_text())
            self.assertEqual(baseline['training_steps'],summaries[0]['training_steps'])
            self.assertEqual(baseline['samples_processed_per_rank'],
                             sum(s['samples_processed_per_rank'] for s in summaries))
            self.assertEqual(baseline['initial_model_sha256'],summaries[0]['initial_model_sha256'])
            self.assertEqual(baseline['global_batch_size'],64)
            self.assertEqual(baseline['local_batch_size'],64)
            self.assertGreater(baseline['training_seconds'],0)
        finally:
            for process in processes:
                if process.poll() is None: process.kill();process.wait()
            for log in logs: log.close()


if __name__ == '__main__':
    unittest.main()
