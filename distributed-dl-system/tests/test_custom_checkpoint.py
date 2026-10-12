"""Real stochastic resume equivalence and corrupt-transfer rejection in isolated storage."""
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch
import zipfile

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'agent'))
sys.path.insert(0,str(ROOT/'controller'))
from checkpoint_protocol import validate
import custom_checkpoints as transport


class CheckpointTests(unittest.TestCase):
    def test_exact_stochastic_model_adam_scheduler_rng_and_position_resume(self):
        import torch
        with tempfile.TemporaryDirectory() as td:
            root=Path(td)
            runner=root/'train.py'
            runner.write_text('''import os,random,sys
from pathlib import Path
import torch
from dl_checkpoint import CheckpointManager
torch.set_num_threads(1);torch.manual_seed(44);random.seed(44)
try:
 import numpy as np
 np.random.seed(44)
except ImportError: np=None
m=torch.nn.Sequential(torch.nn.Linear(3,8),torch.nn.Dropout(0.3),torch.nn.Linear(8,1))
o=torch.optim.Adam(m.parameters(),lr=0.01)
s=torch.optim.lr_scheduler.StepLR(o,step_size=17,gamma=0.9)
c=CheckpointManager();r=c.load(m,o,scheduler=s);start=r['progress']['next_step'] if r else 0
for step in range(start,80):
 x=torch.randn(8,3)*(0.5+random.random())
 if np is not None: x=x*float(np.random.uniform(0.8,1.2))
 y=x.sum(dim=1,keepdim=True)
 o.zero_grad();loss=(m(x)-y).square().mean();loss.backward();o.step();s.step()
 if step==20: c.save(m,o,scheduler=s,global_step=step+1,progress={'next_step':step+1},extra_state={'marker':torch.tensor(7)},force=True)
 if step==22 and sys.argv[1]=='interrupt': os._exit(77)
c.save(m,o,scheduler=s,global_step=80,progress={'next_step':80},completed=True,force=True)
torch.save({'model':m.state_dict(),'optimizer':o.state_dict(),'scheduler':s.state_dict(),'rng':torch.get_rng_state()},Path(os.environ['DL_OUTPUT_DIR'])/'final.pt')
''')
            def environment(folder,attempt='j:0'):
                folder.mkdir()
                return {**os.environ,'PYTHONPATH':str(ROOT/'agent'),'DL_JOB_ID':'j','DL_MANIFEST_SHA256':'snapshot',
                    'DL_COMPATIBILITY_KEY':'compat','DL_ATTEMPT_TOKEN':attempt,'DL_LAUNCHER_PID':'1','DL_CHECKPOINT_DIR':str(folder),
                    'DL_OUTPUT_DIR':str(folder),'DL_CHECKPOINT_INTERVAL_SECONDS':'1',
                    'DL_RESUME_STATUS_PATH':str(folder/'resume.json'),'OMP_NUM_THREADS':'1','MKL_NUM_THREADS':'1'}
            baseline=environment(root/'baseline')
            result=subprocess.run([sys.executable,str(runner),'normal'],env=baseline,capture_output=True,text=True,timeout=30)
            self.assertEqual(result.returncode,0,result.stderr)
            first=environment(root/'first')
            result=subprocess.run([sys.executable,str(runner),'interrupt'],env=first,capture_output=True,text=True,timeout=30)
            self.assertEqual(result.returncode,77,result.stderr)
            checkpoint=root/'first/latest.dlckpt'
            info=validate(checkpoint,'j','snapshot','j:0')
            self.assertEqual(info['global_step'],21)
            resumed=environment(root/'resumed','j:1')
            resumed.update(DL_RESUME_CHECKPOINT=str(checkpoint),DL_RESUME_SHA256=info['sha256'])
            result=subprocess.run([sys.executable,str(runner),'normal'],env=resumed,capture_output=True,text=True,timeout=30)
            self.assertEqual(result.returncode,0,result.stderr)
            def equal(a,b):
                if isinstance(a,torch.Tensor): return torch.equal(a,b)
                if isinstance(a,dict): return a.keys()==b.keys() and all(equal(a[k],b[k]) for k in a)
                if isinstance(a,(list,tuple)): return len(a)==len(b) and all(equal(x,y) for x,y in zip(a,b))
                return a==b
            self.assertTrue(equal(torch.load(root/'baseline/final.pt',weights_only=True),torch.load(root/'resumed/final.pt',weights_only=True)))
            ack=json.loads((root/'resumed/resume.json').read_text())
            self.assertEqual(ack['restored_global_step'],21)
            self.assertTrue(ack['model_restored'] and ack['optimizer_restored'] and ack['scheduler_restored'] and ack['rng_restored'])
            self.assertEqual(ack['checkpoint_sha256'],info['sha256'])
            self.assertTrue(validate(root/'resumed/latest.dlckpt','j','snapshot','j:1')['completed'])
            with self.assertRaises(ValueError): validate(checkpoint,'another','snapshot')
            with self.assertRaises(ValueError): validate(checkpoint,'j','different')
            with self.assertRaises(ValueError): validate(checkpoint,'j','snapshot','j:1')

    def archive(self,path,step=5,completed=False,token='j:0',bad=False):
        raw=b'opaque-tensor-state'
        meta={'format_version':1,'job_id':'j','manifest_sha256':'snapshot','attempt_token':token,
              'global_step':step,'completed':completed,'created_at':1,'compatibility_key':'compat',
              'state_sha256':hashlib.sha256(raw).hexdigest()}
        with zipfile.ZipFile(path,'w') as archive:
            archive.writestr('metadata.json',json.dumps(meta))
            archive.writestr('state.pt',b'corrupted' if bad else raw)

    def test_bad_stale_and_regressing_transfer_preserve_central_bytes(self):
        with tempfile.TemporaryDirectory() as td:
            root=Path(td);(root/'checkpoints').mkdir()
            job={'job_id':'j','storage_path':str(root),'storage_manifest_sha256':'snapshot',
                 'node_id':'pi01','retry_count':0}
            destination=transport.path_for(job);self.archive(destination)
            before=destination.read_bytes()
            for values in ({'bad':True},{'step':4},{'token':'j:1'}):
                candidate=root/'candidate';self.archive(candidate,**values)
                response=Mock(status_code=200)
                response.__enter__=Mock(return_value=response);response.__exit__=Mock(return_value=False)
                response.iter_content.return_value=[candidate.read_bytes()]
                with patch.object(transport,'endpoint',return_value='http://worker'),patch.object(transport.requests,'get',return_value=response):
                    self.assertFalse(transport.sync(job))
                self.assertEqual(destination.read_bytes(),before)
            self.assertEqual(sorted(p.name for p in (root/'checkpoints').iterdir()),['latest.dlckpt'])

    def test_worker_upload_rejects_corruption_without_replacing_resume(self):
        spec=importlib.util.spec_from_file_location('checkpoint_agent',ROOT/'agent/agent.py')
        agent=importlib.util.module_from_spec(spec);spec.loader.exec_module(agent)
        with tempfile.TemporaryDirectory() as td:
            root=Path(td);directory=root/'worker-storage/jobs/j/checkpoints';directory.mkdir(parents=True)
            (directory.parent/'prepared.json').write_text(json.dumps({'manifest_sha256':'snapshot','compatibility_key':'compat'}))
            original=root/'original';self.archive(original)
            damaged=root/'damaged';self.archive(damaged,bad=True)
            with patch.object(agent,'BASE_DIR',root):
                client=agent.app.test_client()
                headers={'X-Manifest-SHA256':'snapshot','X-Resume-SHA256':hashlib.sha256(original.read_bytes()).hexdigest()}
                def upload(path):
                    return client.post('/jobs/j/custom-checkpoint',data={'file':(io.BytesIO(path.read_bytes()),'checkpoint.dlckpt')},headers=headers)
                self.assertEqual(upload(original).status_code,200)
                before=(directory/'resume.dlckpt').read_bytes()
                self.assertEqual(upload(damaged).status_code,422)
                self.assertEqual((directory/'resume.dlckpt').read_bytes(),before)
                agent.job_process=Mock();agent.job_process.poll.return_value=None
                self.assertEqual(upload(original).status_code,409)
                agent.job_process=None

    def test_zero_exit_without_final_checkpoint_cannot_be_completed(self):
        import job_store as store
        import dispatcher
        with tempfile.TemporaryDirectory() as td:
            previous=store.DB_FILE;store.DB_FILE=str(Path(td)/'jobs.db')
            try:
                store.initialize_database()
                store.add_job('missing-final',1,job_type='custom',status='RECEIVED',owner='student',max_retries=0,
                              job_config={'recovery':{'mode':'checkpoint'}})
                with store.get_connection() as db: db.execute("UPDATE jobs SET status='QUEUED',storage_path=? WHERE job_id='missing-final'",(td,))
                store.mark_scheduled('missing-final','pi01','ip');store.mark_running('missing-final')
                response=Mock(status_code=200)
                response.json.return_value={'job':{'status':'COMPLETED','return_code':0}}
                with patch.object(dispatcher,'sync_checkpoint',return_value=False),patch.object(dispatcher.requests,'get',return_value=response), \
                     patch.object(transport,'metadata',return_value=None),patch.object(dispatcher,'log_event'),patch('job_artifacts.collect'):
                    for i in range(3): dispatcher.monitor_running_jobs()
                self.assertEqual(store.get_job('missing-final')['status'],'FAILED')
                self.assertEqual(store.get_reservations(),[])
            finally:
                store.DB_FILE=previous
                dispatcher.unhealthy_nodes.clear()
                dispatcher.final_custom_checkpoint_failures.clear()


if __name__=='__main__': unittest.main()
