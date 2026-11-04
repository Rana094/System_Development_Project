"""Exercise real watchdog expiry, durable generation replay rejection and stale writes."""
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import Mock,patch
import uuid
import zipfile
import psutil
import requests

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'agent'));sys.path.insert(0,str(ROOT/'controller'))
import execution_lease as worker
import execution_leases as controller
import job_store as store
import custom_checkpoints as checkpoints
import job_artifacts as artifacts


def grant(generation=1,attempt='j:0',ttl=30):
    now=time.time()
    return {'job_id':'j','attempt_token':attempt,'node_id':'unit-node','generation':generation,
            'lease_id':uuid.uuid4().hex,'issued_at':now,'expires_at':now+ttl}


class OwnershipTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name)
        self.old=store.DB_FILE;store.DB_FILE=str(self.root/'jobs.db');store.initialize_database()
        self.spec={'execution':{'mode':'single','workers':1},'recovery':{'mode':'checkpoint','max_retries':2}}
        store.add_job('j',1,max_retries=2,job_type='custom',status='RECEIVED',owner='student',job_config=self.spec)
        workspace=self.root/'central';(workspace/'checkpoints').mkdir(parents=True);(workspace/'results').mkdir();(workspace/'logs').mkdir()
        with store.get_connection() as db:
            db.execute("UPDATE jobs SET status='QUEUED',storage_path=?,storage_manifest_sha256='snapshot' WHERE job_id='j'",(str(workspace),))
        store.mark_scheduled('j','unit-node','192.0.2.1')
        self.job=store.get_job('j');self.lease=controller.ensure(self.job,'unit-node')

    def tearDown(self):
        store.DB_FILE=self.old;self.temp.cleanup()

    def expire(self):
        with store.get_connection() as db:
            db.execute('UPDATE execution_leases SET expires_at=?,expires_clock=?',
                       (time.time()-10,worker.clock()-10))

    def test_watchdog_kills_all_descendants_without_http_or_sdk_cooperation(self):
        lease=grant(ttl=0.8);record=worker.accept(self.root,lease,'j','j:0','unit-node')
        script=self.root/'train.py'
        script.write_text("""import pathlib,subprocess,sys,time,os
root=pathlib.Path(sys.argv[1]);child=subprocess.Popen([sys.executable,'-c','import time;time.sleep(60)'])
(root/'pids').write_text(str(os.getpid())+' '+str(child.pid))
while True:
 (root/'progress').write_text(str(time.time()));time.sleep(0.05)
""")
        process=subprocess.Popen([sys.executable,str(ROOT/'agent/lease_runner.py'),'--lease-path',
                                  str(worker.lease_path(self.root,lease)),'--',sys.executable,str(script),str(self.root)],
                                 start_new_session=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
        try:
            out,err=process.communicate(timeout=8)
            self.assertEqual(process.returncode,-9,err);self.assertIn('EXECUTION_LEASE_EXPIRED',out)
            pids=[int(p) for p in (self.root/'pids').read_text().split()]
            for pid in pids:
                try:self.assertIn(psutil.Process(pid).status(),(psutil.STATUS_ZOMBIE,psutil.STATUS_DEAD))
                except psutil.NoSuchProcess:pass
            progress=(self.root/'progress').read_text();time.sleep(0.1)
            self.assertEqual(progress,(self.root/'progress').read_text())
        finally:
            if process.poll() is None:os.killpg(process.pid,9);process.wait()

    def test_primary_exit_cannot_leave_background_training_running(self):
        lease=grant();worker.accept(self.root,lease,'j','j:0','unit-node')
        script=self.root/'launcher.py'
        script.write_text("import subprocess,sys,pathlib; p=subprocess.Popen([sys.executable,'-c','import time;time.sleep(60)']); pathlib.Path(sys.argv[1]).write_text(str(p.pid))")
        process=subprocess.Popen([sys.executable,str(ROOT/'agent/lease_runner.py'),'--lease-path',
            str(worker.lease_path(self.root,lease)),'--',sys.executable,str(script),str(self.root/'background.pid')],
            start_new_session=True,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE,text=True)
        try:
            _,error=process.communicate(timeout=8);self.assertEqual(process.returncode,0,error)
            pid=int((self.root/'background.pid').read_text())
            try:self.assertIn(psutil.Process(pid).status(),(psutil.STATUS_ZOMBIE,psutil.STATUS_DEAD))
            except psutil.NoSuchProcess:pass
        finally:
            if process.poll() is None:os.killpg(process.pid,9);process.wait()

    def test_api_restart_reaps_durable_orphan_before_advertising_idle(self):
        lease=grant();worker.accept(self.root,lease,'j','j:0','unit-node')
        process=subprocess.Popen([sys.executable,str(ROOT/'agent/lease_runner.py'),'--lease-path',
            str(worker.lease_path(self.root,lease)),'--',sys.executable,'-c','import time;time.sleep(60)'],
            start_new_session=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        try:
            worker.attach(self.root,lease,process.pid)
            worker.reap_previous(self.root)
            # psutil may reap this fixture's child while waiting; Popen then
            # reports zero for the already-reaped process. Its PID must be gone.
            self.assertIn(process.wait(timeout=5),(-9,0))
            self.assertIsNone(worker.process_key(process.pid))
            self.assertFalse(worker.alive(worker.read(worker.authority_path(self.root))))
            worker.reap_previous(self.root)
        finally:
            if process.poll() is None:os.killpg(process.pid,9);process.wait()

    def test_higher_generation_survives_restart_and_rejects_late_start_and_renewal(self):
        old=grant(ttl=0.1);worker.accept(self.root,old,'j','j:0','unit-node');time.sleep(0.15)
        revived={**old,'issued_at':time.time(),'expires_at':time.time()+30}
        with self.assertRaisesRegex(ValueError,'expired'):worker.accept(self.root,revived,'j','j:0','unit-node')
        new=grant(2,'j:1');worker.accept(self.root,new,'j','j:1','unit-node')
        with self.assertRaisesRegex(ValueError,'Stale'):worker.accept(self.root,revived,'j','j:0','unit-node')
        self.assertEqual(worker.read(worker.authority_path(self.root))['lease']['generation'],2)
        with self.assertRaises(ValueError):worker.assert_owned(self.root,'j','j:0',1,old['lease_id'])

    def test_clock_skew_and_controller_clock_jump_do_not_allow_early_failover(self):
        future=grant();future['issued_at']+=5;future['expires_at']+=5
        with self.assertRaisesRegex(ValueError,'clocks'):worker.accept(self.root,future,'j','j:0','unit-node')
        with store.get_connection() as db:
            db.execute('UPDATE execution_leases SET expires_at=?,expires_clock=?',(time.time()-100,worker.clock()+100))
        self.assertFalse(controller.safe_to_replace(self.job,'unit-node'))
        self.expire();self.assertTrue(controller.safe_to_replace(self.job,'unit-node'))

    def test_controller_reboot_holds_old_deadlines_for_maximum_lease_bound(self):
        self.expire()
        with patch.object(controller,'boot_id',return_value='new-controller-boot'):
            self.assertFalse(controller.safe_to_replace(self.job,'unit-node'))
            with patch.object(controller,'clock',return_value=controller.START_CLOCK+66):
                self.assertTrue(controller.safe_to_replace(self.job,'unit-node'))

    def test_lost_renewal_reply_blocks_revival_and_remembers_possible_deadline(self):
        with store.get_connection() as db:db.execute('UPDATE execution_leases SET issued_at=?',(time.time()-10,))
        old=controller.get(self.job,'unit-node')['expires_at']
        with patch.object(controller,'load_nodes',return_value=[{'id':'unit-node','ip':'192.0.2.1','port':5000}]), \
             patch.object(controller.requests,'post',side_effect=requests.Timeout('reply lost')):
            controller.renew(self.job)
        row=controller.get(self.job,'unit-node')
        self.assertTrue(row['renewal_blocked']);self.assertGreater(row['expires_at'],old-1)
        self.assertFalse(controller.safe_to_replace(self.job,'unit-node'))
        with patch.object(controller.requests,'post') as retry:controller.renew(self.job)
        self.assertEqual(retry.call_count,0)

    def test_generations_increase_across_attempts_and_node_reuse(self):
        self.assertEqual(len(store.get_attempts('j')),1)
        store.mark_running('j',1);self.assertEqual(len(store.get_attempts('j')),1)
        store.requeue_after_failure('j','failure');store.mark_scheduled('j','unit-node','192.0.2.1')
        later=store.get_job('j');new=controller.ensure(later,'unit-node')
        self.assertGreater(new['generation'],self.lease['generation'])
        self.assertNotEqual(new['lease_id'],self.lease['lease_id'])
        store.initialize_database();self.assertEqual(controller.ensure(later,'unit-node'),new)
        with self.assertRaisesRegex(ValueError,'FENCED'):controller.current(self.job,'unit-node')

    def archive(self,lease):
        state=b'valid-snapshot';meta={'format_version':1,'job_id':'j','manifest_sha256':'snapshot',
             'compatibility_key':'compat','global_step':5,'completed':False,'created_at':time.time(),
             'attempt_token':'j:0','state_sha256':hashlib.sha256(state).hexdigest(),'lease_fence':worker.fence(lease)}
        output=io.BytesIO()
        with zipfile.ZipFile(output,'w') as archive:
            archive.writestr('metadata.json',json.dumps(meta));archive.writestr('state.pt',state)
        return output.getvalue()

    def test_stale_checkpoint_cannot_replace_central_after_new_attempt(self):
        with store.get_connection() as db:
            db.execute('INSERT INTO job_environments VALUES(?,?,?,?,?,?,?)',
                       ('j','unit-node','pytorch-cpu','key',json.dumps({'compatibility_key':'compat'}),1,1))
        payload=self.archive(self.lease);reply=Mock(status_code=200)
        reply.__enter__=Mock(return_value=reply);reply.__exit__=Mock(return_value=False)
        reply.headers={'X-Attempt-Token':'j:0','X-Lease-Id':self.lease['lease_id'],'X-Execution-Generation':str(self.lease['generation'])}
        reply.iter_content.return_value=[payload]
        with patch.object(checkpoints,'endpoint',return_value='http://worker'),patch.object(checkpoints.requests,'get',return_value=reply):
            self.assertTrue(checkpoints.sync(self.job))
        before=checkpoints.path_for(self.job).read_bytes()
        store.requeue_after_failure('j','expired');store.mark_scheduled('j','unit-node','192.0.2.1')
        controller.ensure(store.get_job('j'),'unit-node')
        with patch.object(checkpoints,'endpoint',return_value='http://worker'),patch.object(checkpoints.requests,'get',return_value=reply) as get:
            self.assertFalse(checkpoints.sync(self.job));self.assertEqual(get.call_count,0)
        self.assertEqual(checkpoints.path_for(self.job).read_bytes(),before)

    def test_fenced_artifact_failure_preserves_original_retry_reason(self):
        import dispatcher
        with patch.object(artifacts,'collect',side_effect=ValueError('FENCED stale files')), \
             patch.object(dispatcher,'log_event'),patch.object(dispatcher,'mark_node_unhealthy'):
            dispatcher.handle_running_job_failure(self.job,'Original network lease failure')
        result=store.get_job('j')
        self.assertEqual(result['status'],'QUEUED');self.assertEqual(result['retry_count'],1)
        self.assertEqual(result['last_error'],'Original network lease failure')

    def test_stale_result_transfer_preserves_existing_artifacts(self):
        target=Path(self.job['storage_path'])/'results/good.txt';target.write_text('previous valid result')
        response=Mock(status_code=200);response.json.return_value={'job':{'attempt_token':'j:0','lease':self.lease,'status':'COMPLETED'}}
        def copied(args,**kwargs):
            (Path(args[-1])/'bad.txt').write_text('stale result')
            store.requeue_after_failure('j','expired')
        with patch.object(artifacts,'load_nodes',return_value=[{'id':'unit-node','ip':'192.0.2.1','port':5000}]), \
             patch.object(artifacts.requests,'get',return_value=response),patch.object(artifacts.subprocess,'run',side_effect=copied):
            with self.assertRaisesRegex(ValueError,'FENCED'):artifacts.collect(self.job)
        self.assertEqual(target.read_text(),'previous valid result')
        self.assertFalse((target.parent/'bad.txt').exists())

    def test_worker_rejects_stale_start_renewal_and_download(self):
        spec=importlib.util.spec_from_file_location('ownership_agent',ROOT/'agent/agent.py')
        agent=importlib.util.module_from_spec(spec);spec.loader.exec_module(agent)
        old=grant();new=grant(2,'j:1');worker.accept(self.root,new,'j','j:1','unit-node')
        (self.root/'config').mkdir();(self.root/'config/execution_leases.json').write_text('{"enabled":true}')
        with patch.object(agent,'BASE_DIR',self.root),patch.object(agent.socket,'gethostname',return_value='unit-node'):
            client=agent.app.test_client()
            self.assertEqual(client.post('/jobs/start',json={'job_id':'j','job_type':'custom','attempt_token':'j:0','lease':old}).status_code,409)
            self.assertEqual(client.post('/jobs/start',json={'job_id':'j','job_type':'custom','attempt_token':'j:0'}).status_code,400)
            self.assertEqual(client.get('/jobs/j/custom-checkpoint',query_string={'attempt_token':'j:0','generation':1,'lease_id':old['lease_id']}).status_code,409)
            self.assertEqual(client.post('/jobs/j/lease',json={'lease':old}).status_code,409)


if __name__=='__main__':unittest.main()
