"""Verify a prepared submission and execute its argv in the job's native environment."""
import argparse
import json
import os
from pathlib import Path
import subprocess

from environment_worker import clean_environment, read_request, verify


def run(root, job_id, attempt_token, resume_sha256=None):
    # Verification runs inside the tracked process, so a slow package check cannot
    # make the HTTP launch request ambiguous or leave an untracked training process.
    record = verify(root, job_id, [])['record']
    request = read_request(root / 'jobs' / job_id / 'request.json')
    spec = request['manifest']['execution_spec']
    if spec['execution'] != {'mode':'single','workers':1} or spec['recovery']['mode'] not in ('restart','checkpoint'):
        raise ValueError('Only single-worker submissions are executable')
    allocation = spec['resources_per_worker']
    if hasattr(os, 'sched_getaffinity'):
        cores = sorted(os.sched_getaffinity(0))
        if allocation['cpu_cores'] > len(cores):
            raise ValueError('CPU request exceeds process affinity')
        os.sched_setaffinity(0, cores[:allocation['cpu_cores']])
    environment = clean_environment()
    environment.update({
        'PATH':str(Path(record['python_path']).parent) + os.pathsep + environment.get('PATH',''),
        'VIRTUAL_ENV':str(Path(record['python_path']).parent.parent),
        'DL_JOB_ID':job_id, 'DL_SOURCE_DIR':record['source_path'],
        'DL_OUTPUT_DIR':str(Path(record['workspace_path']) / 'results'),
        'DL_RESULTS_DIR':str(Path(record['workspace_path']) / 'results'),
        'DL_CHECKPOINT_DIR':str(Path(record['workspace_path']) / 'checkpoints'),
        'PYTHONUNBUFFERED':'1',
        'PYTHONPATH':str(Path(__file__).resolve().parent),
        'DL_ATTEMPT_TOKEN':attempt_token,'DL_MANIFEST_SHA256':record['manifest_sha256'],
        'DL_COMPATIBILITY_KEY':record['compatibility_key'],
        'DL_LAUNCHER_PID':str(os.getpid()),
        'DL_CHECKPOINT_INTERVAL_SECONDS':str(spec['recovery'].get('interval_seconds',5)),
        'DL_RESUME_STATUS_PATH':str(Path(record['workspace_path'])/'checkpoints/resume-status.json'),
    })
    for name in ('DL_RESUME_CHECKPOINT','DL_RESUME_SHA256'):
        environment.pop(name,None)
    if resume_sha256:
        environment['DL_RESUME_CHECKPOINT']=str(Path(record['workspace_path'])/'checkpoints/resume.dlckpt')
        environment['DL_RESUME_SHA256']=resume_sha256
    # Bound common tensor-library thread pools to the reserved CPU count.
    for name in ('OMP_NUM_THREADS','MKL_NUM_THREADS','OPENBLAS_NUM_THREADS','NUMEXPR_NUM_THREADS'):
        environment[name] = str(allocation['cpu_cores'])
    if record['dataset_path']:
        environment['DL_DATASET_PATH'] = record['dataset_path']
    else:
        environment.pop('DL_DATASET_PATH',None)
    for name in ('RANK','WORLD_SIZE','LOCAL_RANK','MASTER_ADDR','MASTER_PORT'):
        environment.pop(name,None)
    environment['CUDA_VISIBLE_DEVICES'] = ','.join(str(i) for i in range(allocation['gpus']))
    command = list(spec['entrypoint'])
    if command[0] in ('python','python3'):
        command[0] = record['python_path']
    print(json.dumps({'job_id':job_id,'command':command,'allocation':allocation,
                      'python':record['python_path'],'dataset':record['dataset_path']}),flush=True)
    # The child inherits the agent's process group; stopping the group stops both
    # this wrapper and the submitted trainer, without invoking a shell.
    return subprocess.call(command, cwd=record['source_path'], env=environment)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',type=Path,required=True)
    parser.add_argument('--job-id',required=True)
    parser.add_argument('--attempt-token',default=None)
    parser.add_argument('--resume-sha256')
    args = parser.parse_args()
    raise SystemExit(run(args.root.resolve(),args.job_id,args.attempt_token or args.job_id+':0',args.resume_sha256))
