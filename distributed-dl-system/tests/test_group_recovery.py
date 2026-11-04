"""Verify coordinated restore with stochastic training and durable controller gates."""
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import Mock, patch
import zipfile

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'agent'))
sys.path.insert(0,str(ROOT/'controller'))
from checkpoint_protocol import group_identity,validate_group
import distributed_dispatch as dispatch
import group_checkpoints as central
import job_store as store
import recovery_metrics as metrics
from test_group_checkpoint import same

WORKER='''
import json,os,random,sys
from datetime import timedelta
from pathlib import Path
import numpy as np
import torch
import torch.distributed as dist
from torch.nn.parallel import DistributedDataParallel as DDP
from dl_checkpoint import DistributedCheckpointManager
rank=int(os.environ['RANK']);root=Path(os.environ['DL_CHECKPOINT_DIR']);case=sys.argv[1]
torch.set_num_threads(1);torch.manual_seed(123)
dist.init_process_group('gloo',timeout=timedelta(seconds=15))
try:
    model=DDP(torch.nn.Sequential(torch.nn.Linear(2,4),torch.nn.BatchNorm1d(4),
                                 torch.nn.Dropout(0.3),torch.nn.Linear(4,1)))
    optimizer=torch.optim.Adam(model.parameters(),lr=0.003)
    scheduler=torch.optim.lr_scheduler.StepLR(optimizer,step_size=2,gamma=0.7)
    generator=torch.Generator().manual_seed(900+rank)
    torch.manual_seed(200+rank);random.seed(300+rank);np.random.seed(400+rank)
    manager=DistributedCheckpointManager()
    def application(extra,progress):
        if case=='callback-error' and rank==1: raise ValueError('invalid application cursor')
        if case=='wait-callback' and rank==1:
            (root/'waiting').write_text('ready')
            import time
            while not (root/'release').exists(): time.sleep(0.05)
        generator.set_state(extra['generator'])
        assert extra['rank']==rank
    try:
        restored=manager.load(model,optimizer,scheduler=scheduler,restore_extra=application)
    except ValueError as error:
        if case not in ('corrupt','different-cut','callback-error'): raise
        (root/'error.json').write_text(json.dumps({'error':str(error)}));sys.exit(0)
    start=restored['global_step']+1 if restored else 1
    if restored:
        assert restored['progress']=={'next_step':start}
        torch.save({'model':model.module.state_dict(),'optimizer':optimizer.state_dict(),
                    'scheduler':scheduler.state_dict(),'torch_rng':torch.get_rng_state()},root/'restored.pt')
    for step in range(start,7):
        (root/'last-step').write_text(str(step))
        inputs=torch.rand(8,2,generator=generator)+random.random()+float(np.random.random())+rank
        optimizer.zero_grad();model(inputs).square().mean().backward()
        optimizer.step();scheduler.step()
        manager.save(model,optimizer,scheduler=scheduler,global_step=step,
                     progress={'next_step':step+1},extra_state={'generator':generator.get_state(),'rank':rank},
                     force=True,completed=step==6)
        if case=='cut' and step==2: sys.exit(77)
finally: dist.destroy_process_group()
'''


class RecoveryTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name)
        self.old_db=store.DB_FILE;self.old_metrics=metrics.DB_PATH
        store.DB_FILE=str(self.root/'jobs.db');metrics.DB_PATH=Path(store.DB_FILE)
        store.initialize_database()
        self.nodes=[{'id':f'pi0{r+1}','ip':f'192.0.2.{r+1}','port':5000} for r in range(2)]
        self.source_context={'world_size':2,'master_addr':'127.0.0.1','master_port':29622}

    def tearDown(self):
        store.DB_FILE=self.old_db;metrics.DB_PATH=self.old_metrics;self.temp.cleanup()

    def run_workers(self,case,source=None):
        import torch
        with socket.socket() as listener:
            listener.bind(('127.0.0.1',0));port=listener.getsockname()[1]
        context={'world_size':2,'master_addr':'127.0.0.1','master_port':port}
        if case=='cut': self.source_context=context
        attempt='group:1' if source else 'group:0'
        script=self.root/'training.py';script.write_text(WORKER)
        folder=self.root/case;folder.mkdir();processes=[];logs=[]
        try:
            for rank in range(2):
                directory=folder/f'rank-{rank}';directory.mkdir()
                env=dict(os.environ,PYTHONPATH=str(ROOT/'agent'),GLOO_SOCKET_IFNAME='lo',
                         RANK=str(rank),WORLD_SIZE='2',MASTER_ADDR='127.0.0.1',MASTER_PORT=str(port),
                         DL_JOB_ID='group',DL_MANIFEST_SHA256='snapshot',DL_COMPATIBILITY_KEY='compat',
                         DL_ATTEMPT_TOKEN=attempt,DL_DDP_GROUP_ID=group_identity('group',attempt,'snapshot','compat',context),
                         DL_CHECKPOINT_DIR=str(directory),DL_CHECKPOINT_INTERVAL_SECONDS='1',
                         DL_RESUME_STATUS_PATH=str(directory/'resume-status.json'),DL_LAUNCHER_PID=str(100+rank))
                if source:
                    data=source.read_bytes()
                    if rank==1 and case in ('corrupt','different-cut'):
                        with zipfile.ZipFile(io.BytesIO(data)) as archive:
                            entries={name:archive.read(name) for name in archive.namelist()}
                        if case=='corrupt': entries['rank-1.pt']=b'bad-state'
                        else:
                            meta=json.loads(entries['metadata.json']);meta['checkpoint_id']='f'*32
                            for ack in meta['rank_acknowledgments']: ack['checkpoint_id']=meta['checkpoint_id']
                            entries['metadata.json']=json.dumps(meta).encode()
                        buffer=io.BytesIO()
                        with zipfile.ZipFile(buffer,'w') as archive:
                            for name,payload in entries.items(): archive.writestr(name,payload)
                        data=buffer.getvalue()
                    resume=directory/'resume.dlckpt';resume.write_bytes(data)
                    env.update(DL_RESUME_CHECKPOINT=str(resume),DL_RESUME_SHA256=hashlib.sha256(data).hexdigest())
                log=(directory/'training.log').open('w');logs.append(log)
                processes.append(subprocess.Popen([sys.executable,str(script),case],env=env,stdout=log,stderr=subprocess.STDOUT))
            if case=='wait-callback':
                waiting=folder/'rank-1/waiting';deadline=time.monotonic()+20
                while not waiting.exists():
                    if time.monotonic()>deadline: self.fail('Rank never entered restore callback')
                    time.sleep(0.05)
                for rank in range(2):
                    self.assertFalse((folder/f'rank-{rank}/resume-status.json').exists())
                    self.assertFalse((folder/f'rank-{rank}/last-step').exists())
                (folder/'rank-1/release').touch()
            for rank,process in enumerate(processes):
                code=process.wait(timeout=40)
                self.assertEqual(code,77 if case=='cut' else 0,(folder/f'rank-{rank}/training.log').read_text())
        finally:
            for process in processes:
                if process.poll() is None: process.kill();process.wait()
            for log in logs: log.close()
        return folder/'rank-0/latest.dlckpt',folder,context

    def states(self,path):
        import torch
        with zipfile.ZipFile(path) as archive:
            return [torch.load(io.BytesIO(archive.read(f'rank-{r}.pt')),weights_only=True,map_location='cpu') for r in range(2)]

    def test_exact_stochastic_resume_with_rank_buffers_all_rng_and_application_state(self):
        import torch
        baseline,_,_=self.run_workers('baseline')
        source,_,_=self.run_workers('cut')
        resumed,folder,context=self.run_workers('resume',source)
        original=self.states(source)
        for rank in range(2):
            ack=json.loads((folder/f'rank-{rank}/resume-status.json').read_text())
            self.assertEqual(ack['restored_global_step'],2)
            self.assertEqual(ack['rank'],rank);self.assertTrue(ack['all_ranks_restored'])
            loaded=torch.load(folder/f'rank-{rank}/restored.pt',weights_only=True)
            for key in loaded: self.assertTrue(same(loaded[key],original[rank][key]),key)
        for expected,actual in zip(self.states(baseline),self.states(resumed)):
            self.assertTrue(same(expected,actual),'Final full rank training state differs')
        self.assertEqual(validate_group(resumed,'group','snapshot')['attempt_token'],'group:1')

    def test_no_rank_trains_or_acknowledges_before_all_application_callbacks_finish(self):
        source,_,_=self.run_workers('cut')
        result,_,_=self.run_workers('wait-callback',source)
        self.assertTrue(validate_group(result,'group','snapshot')['completed'])

    def test_corrupt_different_cut_and_callback_failure_reject_whole_restore(self):
        source,_,_=self.run_workers('cut')
        for case in ('corrupt','different-cut','callback-error'):
            result,folder,_=self.run_workers(case,source)
            self.assertFalse(result.exists())
            for rank in range(2):
                directory=folder/f'rank-{rank}'
                self.assertTrue((directory/'error.json').exists())
                self.assertFalse((directory/'resume-status.json').exists())
                self.assertFalse((directory/'last-step').exists())

    def test_worker_group_upload_preserves_resume_after_bad_bytes_or_world_size(self):
        source,_,_=self.run_workers('cut')
        spec=importlib.util.spec_from_file_location('step9_agent',ROOT/'agent/agent.py')
        agent=importlib.util.module_from_spec(spec);spec.loader.exec_module(agent)
        root=self.root/'worker';directory=root/'worker-storage/jobs/group'
        directory.mkdir(parents=True)
        (directory/'prepared.json').write_text(json.dumps({'manifest_sha256':'snapshot','compatibility_key':'compat'}))
        descriptor={'manifest':{'execution_spec':{'execution':{'mode':'distributed','workers':2}}}}
        (directory/'request.json').write_text(json.dumps(descriptor))
        headers={'X-Manifest-SHA256':'snapshot','X-Resume-SHA256':hashlib.sha256(source.read_bytes()).hexdigest()}
        with patch.object(agent,'BASE_DIR',root):
            client=agent.app.test_client()
            def upload(data):
                return client.post('/jobs/group/custom-checkpoint',data={'file':(io.BytesIO(data),'resume.ddpckpt')},headers=headers)
            self.assertEqual(upload(source.read_bytes()).status_code,200)
            target=directory/'checkpoints/resume.dlckpt';previous=target.read_bytes()
            self.assertEqual(upload(b'corrupt').status_code,422)
            self.assertEqual(target.read_bytes(),previous)
            descriptor['manifest']['execution_spec']['execution']['workers']=3
            (directory/'request.json').write_text(json.dumps(descriptor))
            self.assertEqual(upload(source.read_bytes()).status_code,422)
            self.assertEqual(target.read_bytes(),previous)

    def setup_retry(self,source):
        spec={'execution':{'mode':'distributed','workers':2},'recovery':{'mode':'checkpoint','max_retries':2},
              'resources_per_worker':{'cpu_cores':1,'memory_gb':1,'gpus':0}}
        store.add_job('group',1,max_retries=2,job_type='custom',status='RECEIVED',owner='student',job_config=spec)
        workspace=self.root/'central';(workspace/'checkpoints').mkdir(parents=True)
        (workspace/'checkpoints/latest.ddpckpt').write_bytes(source.read_bytes())
        with store.get_connection() as db:
            db.execute("UPDATE jobs SET status='QUEUED',retry_count=1,storage_path=?,checkpoint_path=?,storage_manifest_sha256='snapshot' WHERE job_id='group'",
                       (str(workspace),str(workspace/'checkpoints/latest.ddpckpt')))
            db.execute('INSERT INTO distributed_attempts VALUES(?,?,?,?,?,?)',
                       ('group',0,'FAILED',json.dumps(self.source_context),'rank failed',1))
            for node in self.nodes:
                db.execute('INSERT INTO job_environments VALUES(?,?,?,?,?,?,?)',
                           ('group',node['id'],'pytorch-cpu','env',json.dumps({'compatibility_key':'compat','environment_key':'env'}),1,1))
        job=store.get_job('group');resume=central.resume_metadata(job)
        context={'world_size':2,'master_addr':'192.0.2.1','master_port':29644,'resume_checkpoint':resume}
        store.mark_scheduled('group','pi01','192.0.2.1',spec['resources_per_worker'],self.nodes,context)
        return store.get_job('group'),context

    def observed(self,job,context,rank,ack=True):
        data={'attempt_token':'group:1','pid':100+rank,'started_at':10,'status':'RUNNING',
              'ddp':{**dispatch.launch_context(context),'rank':rank}}
        if ack:
            resume=context['resume_checkpoint']
            data['resume']={'job_id':'group','attempt_token':'group:1','pid':100+rank,'confirmed_at':11,
                'rank':rank,'world_size':2,'group_id':group_identity('group','group:1','snapshot','compat',context),
                'checkpoint_sha256':resume['sha256'],'checkpoint_id':resume['checkpoint_id'],
                'source_attempt':'group:0','restored_global_step':2,'model_restored':True,'optimizer_restored':True,
                'rng_restored':True,'application_state_restored':True,'all_ranks_restored':True}
        return data

    def test_transfers_finish_before_any_launch_and_restart_skips_busy_uploads(self):
        source,_,_=self.run_workers('cut');job,context=self.setup_retry(source)
        import requests
        with patch.object(central,'sync'),patch.object(central,'restore',side_effect=requests.Timeout('offline')) as upload, \
             patch.object(dispatch.requests,'post') as launch:
            dispatch.progress(job)
        self.assertEqual(launch.call_count,0);self.assertEqual(len(store.get_reservations()),2)
        self.assertFalse(json.loads(dispatch.group(job)['context_json']).get('restore_prepared'))
        order=[]
        def prepared(job,rank,resume): order.append(('transfer',rank['rank']))
        def start(url,**kwargs):
            rank=kwargs['json']['ddp']['rank'];order.append(('launch',rank))
            response=Mock(status_code=200);response.json.return_value={'job':self.observed(job,context,rank,False)}
            return response
        with patch.object(central,'sync'),patch.object(central,'restore',side_effect=prepared), \
             patch.object(dispatch,'worker',return_value=None),patch.object(dispatch.requests,'post',side_effect=start):
            dispatch.progress(job)
        self.assertEqual(order,[('transfer',0),('transfer',1),('launch',0),('launch',1)])
        self.assertEqual(store.get_job('group')['status'],'SCHEDULED')
        # Re-read persisted state as after a controller restart. No upload onto
        # running peers is attempted, and all restore acknowledgments are needed.
        with patch.object(central,'sync'),patch.object(central,'restore') as upload, \
             patch.object(dispatch,'worker',side_effect=[self.observed(job,context,0),self.observed(job,context,1,False)]):
            dispatch.progress(store.get_job('group'))
        self.assertEqual(upload.call_count,0);self.assertEqual(store.get_job('group')['status'],'SCHEDULED')
        with patch.object(central,'sync'),patch.object(dispatch,'worker',side_effect=[self.observed(job,context,0),self.observed(job,context,1)]):
            dispatch.progress(store.get_job('group'))
        self.assertEqual(store.get_job('group')['status'],'RUNNING')
        events=[e for e in metrics.get_events('group') if e['event_type']=='DDP_RESUMED']
        self.assertEqual(len(events),1)
        with store.get_connection() as db:
            db.execute("UPDATE distributed_attempts SET phase='STARTING' WHERE retry_count=1")
        with patch.object(central,'sync'),patch.object(dispatch,'worker',side_effect=[self.observed(job,context,0),self.observed(job,context,1)]):
            dispatch.progress(store.get_job('group'))
        self.assertEqual(len([e for e in metrics.get_events('group') if e['event_type']=='DDP_RESUMED']),1)

    def test_wrong_or_missing_restore_ack_cannot_complete(self):
        source,_,_=self.run_workers('cut');job,context=self.setup_retry(source)
        with store.get_connection() as db:
            context['restore_prepared']=True
            db.execute('UPDATE distributed_attempts SET context_json=? WHERE retry_count=1',(json.dumps(context),))
        bad=self.observed(job,context,1);bad['resume']['checkpoint_sha256']='wrong';bad['status']='COMPLETED'
        with patch.object(central,'sync'),patch.object(dispatch,'worker',side_effect=[self.observed(job,context,0),bad]):
            dispatch.progress(job)
        self.assertEqual(dispatch.group(job)['phase'],'STOPPING')
        self.assertEqual(len(store.get_reservations()),2)

    def test_known_missing_checkpoint_fails_closed_before_reservation(self):
        source,_,_=self.run_workers('cut');job,context=self.setup_retry(source)
        store.return_to_queue('group','setup')
        central.path_for(job).unlink()
        with patch.object(dispatch,'select_group') as choose:
            self.assertFalse(dispatch.dispatch(store.get_job('group'),set()))
        self.assertEqual(choose.call_count,0);self.assertEqual(store.get_job('group')['status'],'FAILED')
        self.assertEqual(store.get_reservations(),[])


if __name__=='__main__': unittest.main()
