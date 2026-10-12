"""Exercise fair student scheduling, holds, releases and running cancellation with real MLP jobs."""
import argparse
import fcntl
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT / 'controller'))
from job_store import get_job, get_jobs_by_status, get_reservations
from queue_policy import limits, load_policy
from scheduler import get_resources, load_nodes
from check_step3 import command
from check_step4 import verify_model


def control(action, job, owner):
    return command(['controller/queue_ctl.py',action,'--job-id',job,'--owner',owner])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--nodes',nargs=3,default=['pi01','pi02','pi03'])
    args = parser.parse_args()
    nodes = args.nodes
    known = {node['id']:node for node in load_nodes()}
    if len(set(nodes)) != 3 or any(n not in known for n in nodes):
        parser.error('Choose three distinct configured workers')
    owners = ['step5-alice','step5-bob','step5-carol','step5-dave']
    if any(limits(load_policy(),owner)['max_active_per_owner'] != 1 for owner in owners):
        parser.error('This demo needs max_active_per_owner=1 for its four student IDs')
    lock = open(ROOT / 'controller/.dispatcher.lock','w')
    try:
        fcntl.flock(lock,fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        parser.error('Stop your dispatcher before running this self-contained demonstration')
    if any(get_jobs_by_status(s) for s in ('QUEUED','SCHEDULED','RUNNING','CANCELLING')):
        parser.error('Finish existing active jobs first')
    for node in nodes:
        snapshot = get_resources(known[node])
        if not snapshot or snapshot['node_state'] != 'IDLE':
            parser.error(node + ' must be reachable and idle')
    jobs = []
    labels = ['alice-1','alice-2','alice-3','bob-1','carol-1','dave-held']
    for label in labels:
        owner = 'step5-' + label.split('-')[0]
        job = command(['controller/submit_job.py','--spec','examples/job.scheduling.json','--owner',owner])['job_id']
        jobs.append(job)
        command(['controller/prepare_job.py','--job-id',job,'--nodes',*nodes,'--profile','pytorch-cpu'])
        command(['controller/queue_job.py','--job-id',job])
        print(f'QUEUED {label}: {job}',flush=True)
    held = jobs[-1]
    assert control('hold',held,'step5-dave')['status']=='HELD'
    assert control('release',held,'step5-dave')['status']=='QUEUED'
    assert control('hold',held,'step5-dave')['status']=='HELD'
    wrong = subprocess.run([sys.executable,'controller/queue_ctl.py','cancel','--job-id',held,
                            '--owner','step5-bob'],cwd=ROOT,capture_output=True,text=True)
    assert wrong.returncode != 0 and get_job(held)['status']=='HELD'
    print('CONTROLS VERIFIED: hold, release, hold again; wrong owner rejected.',flush=True)
    import dispatcher
    dispatcher.init_recovery_table()
    observed = cancelled = replacement = False
    previous = None
    deadline = time.monotonic() + 240
    while time.monotonic() < deadline:
        dispatcher.reconcile_cancellations()
        dispatcher.reconcile_scheduled_jobs()
        dispatcher.monitor_running_jobs()
        dispatcher.dispatch_queued_jobs()
        current = [get_job(job) for job in jobs]
        view = [(label,j['status'],j['node_id']) for label,j in zip(labels,current)]
        if view != previous:
            print('\n' + '\n'.join(f'{label}: {state} on {node or "-"}' for label,state,node in view),flush=True)
            previous = view
        active = [j for j in current if j['status'] in ('RUNNING','SCHEDULED','CANCELLING')]
        for owner in owners:
            assert sum(j['owner']==owner for j in active) <= 1
        if not observed and all(current[i]['status']=='RUNNING' for i in (0,3,4)):
            assert all(current[i]['status']=='QUEUED' for i in (1,2)) and current[5]['status']=='HELD'
            assert len({current[i]['node_id'] for i in (0,3,4)})==3
            print('FAIRNESS VERIFIED: alice, bob and carol each have one worker; alice backlog waits.',flush=True)
            observed = True
        if observed and not cancelled:
            # Wait for a real optimizer step before stopping Alice's running job.
            import requests
            node = known[current[0]['node_id']]
            log = requests.get(f"http://{node['ip']}:{node['port']}/jobs/{jobs[0]}/log",timeout=3)
            if log.ok and 'step=1 ' in log.text:
                assert control('cancel',jobs[0],'step5-alice')['status']=='CANCELLING'
                assert any(r['job_id']==jobs[0] for r in get_reservations())
                assert control('cancel',held,'step5-dave')['status']=='CANCELLED'
                print('CANCEL REQUESTED: running allocation retained until worker confirms exit; held job cancelled immediately.',flush=True)
                cancelled = True
        if cancelled and current[0]['status']=='CANCELLED' and current[1]['status']=='RUNNING':
            assert current[2]['status']=='QUEUED'
            if not replacement:
                print('REPLACEMENT VERIFIED: alice-1 CANCELLED; alice-2 RUNNING; alice-3 still waits for its owner quota.',flush=True)
                replacement = True
        if any(j['status']=='FAILED' for j in current):
            raise SystemExit('Unexpected job failure; inspect queue_ctl.py list --history and worker logs')
        if all(j['status'] in ('COMPLETED','CANCELLED') for j in current):
            break
        time.sleep(2)
    else:
        raise SystemExit('Timed out. Restart the ordinary dispatcher to continue monitoring these tracked jobs.')
    assert observed and cancelled and replacement and get_reservations()==[]
    for i in (1,2,3,4):
        job = get_job(jobs[i])
        assert job['status']=='COMPLETED'
        workspace = Path(job['storage_path'])
        summary = json.loads((workspace / 'results/summary.json').read_text())
        assert summary['parameters_changed'] and summary['training_steps'] >= 10
        assert hashlib.sha256((workspace / 'results/model.pt').read_bytes()).hexdigest()==summary['model_sha256']
        verify_model(workspace / 'results/model.pt',summary['training_steps'])
        print(f"TRAINING VERIFIED {labels[i]}: {summary['training_steps']} Adam steps; {workspace}",flush=True)
    assert not get_job(held)['started_at']
    stopped_log = Path(get_job(jobs[0])['storage_path']) / 'logs/attempt-0.log'
    assert 'step=1 ' in stopped_log.read_text()
    stopped = get_job(jobs[0])
    node = known[stopped['node_id']]
    payload = dispatcher.build_start_payload(stopped)
    payload['attempt_token'] = dispatcher.attempt_token(stopped)
    import requests
    replay = requests.post(f"http://{node['ip']}:{node['port']}/jobs/start",json=payload,timeout=5)
    assert replay.status_code == 409 and 'cancelled' in replay.json()['error'].lower()
    print('LATE START VERIFIED: the cancelled attempt cannot launch again.',flush=True)
    print('PASS: fair owner turns, active quota, hold/release, owner checks, real running cancellation, next-job dispatch and completed model artifacts.',flush=True)


if __name__ == '__main__':
    main()
