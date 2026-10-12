"""Prove a submitted stochastic model resumes exactly after a process failure or Pi reboot."""
import argparse
import fcntl
import json
from pathlib import Path
import subprocess
import sys
import time

import requests
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'controller'))
from job_store import get_job,get_jobs_by_status,get_reservations,get_attempts
from scheduler import load_nodes,get_resources
from recovery_metrics import get_events,log_event
from custom_checkpoints import metadata
from check_step3 import command


def equal(a,b):
    import torch
    if isinstance(a,torch.Tensor): return torch.equal(a,b)
    if isinstance(a,dict): return a.keys()==b.keys() and all(equal(a[k],b[k]) for k in a)
    if isinstance(a,(list,tuple)): return len(a)==len(b) and all(equal(x,y) for x,y in zip(a,b))
    return a==b


def boot_id(node):
    result=subprocess.run(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=5',node['id'],
                           'cat /proc/sys/kernel/random/boot_id'],check=True,capture_output=True,text=True,timeout=15)
    return result.stdout.strip()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--nodes',nargs=3,default=['pi01','pi02','pi03'])
    parser.add_argument('--failure',choices=['process','reboot'],default='process')
    args=parser.parse_args()
    known={n['id']:n for n in load_nodes()}
    if len(set(args.nodes))!=3 or any(n not in known for n in args.nodes): parser.error('Choose three configured workers')
    lock=open(ROOT/'controller/.dispatcher.lock','w')
    try: fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    except BlockingIOError: parser.error('Stop your dispatcher before starting this self-contained demonstration')
    if any(get_jobs_by_status(s) for s in ('QUEUED','SCHEDULED','RUNNING','CANCELLING')): parser.error('Finish existing active jobs first')
    for name in args.nodes:
        snapshot=get_resources(known[name])
        if not snapshot or snapshot['node_state']!='IDLE': parser.error(name+' must be reachable and idle')
    jobs=[]
    for label in ('baseline','recovery'):
        job=command(['controller/submit_job.py','--spec','examples/job.checkpoint.json','--owner','step6-demo'])['job_id']
        command(['controller/prepare_job.py','--job-id',job,'--nodes',*args.nodes,'--profile','pytorch-cpu'])
        jobs.append(job)
        print(f'PREPARED {label}: {job}',flush=True)
    import dispatcher
    dispatcher.init_recovery_table()
    failure_node=None;old_boot=None;injected=False
    for label,jid in zip(('baseline','recovery'),jobs):
        command(['controller/queue_job.py','--job-id',jid])
        deadline=time.monotonic()+240
        previous=None
        while time.monotonic()<deadline:
            dispatcher.reconcile_cancellations();dispatcher.reconcile_scheduled_jobs()
            dispatcher.monitor_running_jobs();dispatcher.dispatch_queued_jobs()
            job=get_job(jid)
            view=(job['status'],job['node_id'],job['retry_count'])
            if view!=previous:
                print(f'{label}: {view}',flush=True);previous=view
            info=metadata(job)
            if label=='recovery' and not injected and job['status']=='RUNNING' and info and 100<=info['global_step']<5000:
                failure_node=known[job['node_id']]
                log_event(jid,'FAILURE_INJECTED',failure_node['id'],details={'kind':args.failure,'central_step':info['global_step'],'sha256':info['sha256']})
                if args.failure=='reboot':
                    old_boot=boot_id(failure_node)
                    reboot=subprocess.run(['ssh','-o','BatchMode=yes',failure_node['id'],'sudo -n reboot'],capture_output=True,text=True,timeout=15)
                    if reboot.returncode not in (0,255): raise SystemExit('Reboot failed: '+reboot.stderr)
                else:
                    requests.post(f"http://{failure_node['ip']}:{failure_node['port']}/jobs/{jid}/stop",
                                  json={'attempt_token':dispatcher.attempt_token(job),'force':True},timeout=5).raise_for_status()
                injected=True
                print(f"FAILURE INJECTED: {args.failure} on {failure_node['id']}; central checkpoint step={info['global_step']}",flush=True)
            if job['status']=='COMPLETED': break
            if job['status'] in ('FAILED','CANCELLED'): raise SystemExit('Training failed: '+str(job['last_error']))
            time.sleep(1)
        else: raise SystemExit('Timed out; restart the normal dispatcher to continue monitoring tracked jobs')
    recovered=get_job(jobs[1]);attempts=get_attempts(jobs[1])
    assert injected and len(attempts)==2 and attempts[0]['status']=='FAILED' and attempts[1]['status']=='COMPLETED'
    assert attempts[0]['node_id']!=attempts[1]['node_id'] and recovered['retry_count']==1
    resumed=[json.loads(e['details_json']) for e in get_events(jobs[1]) if e['event_type']=='RESUMED']
    assert len(resumed)==1 and resumed[0]['model_restored'] and resumed[0]['optimizer_restored'] and resumed[0]['rng_restored']
    restore=[json.loads(e['details_json']) for e in get_events(jobs[1]) if e['event_type']=='RESTORE_PREPARED']
    assert restore[-1]['sha256']==resumed[0]['checkpoint_sha256'] and restore[-1]['global_step']==resumed[0]['restored_global_step']
    import torch
    models=[]
    for jid in jobs:
        job=get_job(jid);workspace=Path(job['storage_path'])
        info=metadata(job)
        assert info['completed'] and info['global_step']==6000
        models.append(torch.load(workspace/'results/model.pt',map_location='cpu',weights_only=True))
    assert equal(models[0],models[1]),'Recovered model/Adam/scheduler differ from uninterrupted baseline'
    summary=json.loads((Path(recovered['storage_path'])/'results/summary.json').read_text())
    assert summary['resumed_from']==resumed[0]['restored_global_step']>0
    if old_boot:
        assert boot_id(failure_node)!=old_boot
        print('WHOLE-PI REBOOT VERIFIED: boot ID changed; node rejoined; recovery used a different worker.',flush=True)
    assert get_reservations()==[]
    print(f"RESUME VERIFIED: step={summary['resumed_from']} on {attempts[1]['node_id']}; model, Adam, scheduler and RNG restored.",flush=True)
    print('EXACT EQUIVALENCE VERIFIED: final model tensors, Adam state and scheduler match uninterrupted training.',flush=True)
    print('PASS: submitted-code checkpoint recovery, real training, different-worker resume, final central checkpoint and exact baseline equivalence.',flush=True)
    print('Inspect: '+str(Path(recovered['storage_path'])),flush=True)


if __name__=='__main__': main()
