"""Submit real MLP jobs, prove capacity queuing, and verify collected training outputs."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT / 'controller'))
from job_store import get_job, get_jobs_by_status, get_reservations
from scheduler import get_resources, load_nodes
from check_step3 import command


def verify_model(path, expected_steps):
    """Inspect actual tensors and Adam counters, in addition to checking file hashes."""
    import torch
    model = torch.load(path, map_location='cpu', weights_only=True)
    assert model['steps'] == expected_steps
    tensors = model['model_state_dict']
    assert len(tensors) == 6 and all(torch.isfinite(tensor).all() for tensor in tensors.values())
    states = model['optimizer_state_dict']['state']
    assert len(states) == 6 and all(int(state['step']) == expected_steps for state in states.values())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--nodes',nargs='+',default=['pi01','pi02','pi03'])
    parser.add_argument('--profile',choices=['pytorch-cpu','isolated'],default='pytorch-cpu')
    args = parser.parse_args()
    nodes = list(dict.fromkeys(args.nodes))
    known = {n['id']:n for n in load_nodes()}
    if not nodes or any(n not in known for n in nodes):
        parser.error('Use configured worker IDs')
    if any(get_jobs_by_status(state) for state in ('QUEUED','SCHEDULED','RUNNING','CANCELLING')):
        parser.error('Finish existing active jobs before running this capacity demonstration')
    for node_id in nodes:
        snapshot = get_resources(known[node_id])
        if not snapshot or snapshot['node_state'] != 'IDLE' or 'slots' not in snapshot:
            parser.error(f'{node_id} must be reachable, idle and running the Step 4 agent')
    # Lock before submitting anything. This demo must own the dispatcher, so a
    # second loop cannot consume the queue during the observation window.
    import fcntl
    lock = open(ROOT / 'controller/.dispatcher.lock','w')
    try:
        fcntl.flock(lock,fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        parser.error('Stop your dispatcher before running this self-contained demo')
    jobs = []
    for number in range(len(nodes)+1):
        receipt = command(['controller/submit_job.py','--spec','examples/job.scheduling.json',
                           '--owner',f'step4-demo-{number+1}'])
        job_id = receipt['job_id']
        jobs.append(job_id)
        command(['controller/prepare_job.py','--job-id',job_id,'--nodes',*nodes,'--profile',args.profile])
        print(f'Prepared job {number+1}: {job_id}',flush=True)
    for job_id in jobs:
        command(['controller/queue_job.py','--job-id',job_id])
    # Use the production dispatcher functions while holding its production lock.
    # This avoids leaving a background controller process behind after the demo.
    import dispatcher
    dispatcher.init_recovery_table()
    deadline = time.monotonic() + 300
    observed = False
    last_states = None
    while time.monotonic() < deadline:
        dispatcher.reconcile_scheduled_jobs()
        dispatcher.monitor_running_jobs()
        dispatcher.dispatch_queued_jobs()
        current = [get_job(job_id) for job_id in jobs]
        states = [(j['job_id'],j['status'],j['node_id']) for j in current]
        if states != last_states:
            print('\n' + '\n'.join(f'{job_id}: {state} on {node or "-"}' for job_id,state,node in states),flush=True)
            last_states = states
        running = [j for j in current if j['status'] == 'RUNNING']
        if len(running) == len(nodes) and current[-1]['status'] == 'QUEUED':
            assert len({j['node_id'] for j in running}) == len(nodes)
            assert len(get_reservations()) == len(nodes)
            if not observed:
                print(f'CAPACITY VERIFIED: {len(nodes)} distinct workers RUNNING; extra job QUEUED.',flush=True)
                for node_id in nodes:
                    snapshot = get_resources(known[node_id])
                    assert snapshot['slots']['available'] == 0
                    assert snapshot['allocation'] == {'cpu_cores':1,'memory_gb':1,'gpus':0}
                observed = True
        if any(j['status'] == 'FAILED' for j in current):
            raise SystemExit('Training failed; inspect job status and worker logs')
        if all(j['status'] == 'COMPLETED' for j in current):
            break
        time.sleep(2)
    else:
        raise SystemExit('Timed out; jobs remain visible in job_status.py for investigation')
    if not observed:
        raise SystemExit('Did not observe the required simultaneous running/queued state')
    assert get_reservations() == []
    summaries = []
    for job_id in jobs:
        workspace = Path(get_job(job_id)['storage_path'])
        summary = json.loads((workspace / 'results/summary.json').read_text())
        summaries.append(summary)
        assert summary['job_id'] == job_id and summary['parameters_changed'] and summary['training_steps'] >= 10
        assert hashlib.sha256((workspace / 'results/model.pt').read_bytes()).hexdigest() == summary['model_sha256']
        assert (workspace / 'logs/attempt-0.log').stat().st_size > 0
        verify_model(workspace / 'results/model.pt', summary['training_steps'])
        print(f"VERIFIED {job_id}: {summary['training_steps']} optimizer steps; model and logs in {workspace}",flush=True)
    overlap = min(s['finished_at'] for s in summaries[:-1]) - max(s['started_at'] for s in summaries[:-1])
    assert overlap > 0, 'The actual training intervals did not overlap'
    assert summaries[-1]['started_at'] >= min(s['finished_at'] for s in summaries[:-1])
    print(f'Actual training overlap across initial workers: {overlap:.2f} seconds.',flush=True)
    print('PASS: concurrent real MLP training, capacity queue, automatic next dispatch, collected artifacts and released reservations.',flush=True)


if __name__ == '__main__':
    main()
