"""Verify a prepared submission and execute its argv in the job's native environment."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import ipaddress
import socket

from environment_worker import clean_environment, read_request, verify


def validate_execution(spec, ddp):
    """DDP launch settings must agree with the staged worker count and CPU mode."""
    if spec['execution'] == {'mode':'single','workers':1}:
        if ddp is not None:
            raise ValueError('Single-worker jobs cannot receive DDP settings')
        return
    if (spec['execution']['mode'] != 'distributed' or spec['recovery']['mode'] not in ('restart','checkpoint')
            or spec['resources_per_worker']['gpus'] != 0):
        raise ValueError('Distributed execution currently requires CPU training')
    if not isinstance(ddp,dict) or set(ddp) != {'rank','world_size','master_addr','master_port'}:
        raise ValueError('Missing or invalid DDP launch settings')
    if (type(ddp['rank']) is not int or type(ddp['world_size']) is not int
            or ddp['world_size'] != spec['execution']['workers']
            or not 0 <= ddp['rank'] < ddp['world_size']):
        raise ValueError('DDP rank or world size differs from the submission')
    ipaddress.IPv4Address(ddp['master_addr'])
    if type(ddp['master_port']) is not int or not 1024 <= ddp['master_port'] <= 65535:
        raise ValueError('Invalid DDP rendezvous port')


def gloo_interface(address, port):
    """Select the Linux interface on the route to the rendezvous host."""
    import fcntl
    import struct
    with socket.socket(socket.AF_INET,socket.SOCK_DGRAM) as route:
        route.connect((address,port))
        local_ip = route.getsockname()[0]
        for _,name in socket.if_nameindex():
            try:
                result = fcntl.ioctl(route.fileno(),0x8915,struct.pack('256s',name.encode()[:15]))
                if socket.inet_ntoa(result[20:24]) == local_ip:
                    return name
            except OSError:
                continue
    raise ValueError('Cannot find the network interface for DDP rendezvous')


def run(root, job_id, attempt_token, resume_sha256=None, ddp=None):
    # Verification runs inside the tracked process, so a slow package check cannot
    # make the HTTP launch request ambiguous or leave an untracked training process.
    record = verify(root, job_id, [])['record']
    request = read_request(root / 'jobs' / job_id / 'request.json')
    spec = request['manifest']['execution_spec']
    validate_execution(spec,ddp)
    if spec['recovery']['mode'] not in ('restart','checkpoint'):
        raise ValueError('Unsupported recovery mode')
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
        'DL_LAUNCHER_PID':str(os.environ.get('DL_GUARDIAN_PID',os.getpid())),
        'DL_CHECKPOINT_INTERVAL_SECONDS':str(spec['recovery'].get('interval_seconds',5)),
        'DL_RESUME_STATUS_PATH':str(Path(record['workspace_path'])/'checkpoints/resume-status.json'),
        'DL_RECOVERY_MODE':spec['recovery']['mode'],
    })
    if os.environ.get('DL_EXECUTION_LEASE_PATH'):
        environment['DL_EXECUTION_LEASE_PATH']=os.environ['DL_EXECUTION_LEASE_PATH']
        environment['DL_WORKER_ROOT']=str(root.parent)
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
    if ddp is not None:
        environment.update({key.upper():str(value) for key,value in ddp.items()})
        environment['LOCAL_RANK'] = '0'
        environment['DL_DDP_BACKEND'] = 'gloo'
        environment['GLOO_SOCKET_IFNAME'] = gloo_interface(ddp['master_addr'],ddp['master_port'])
        from checkpoint_protocol import group_identity
        environment['DL_DDP_GROUP_ID'] = group_identity(job_id,attempt_token,
            record['manifest_sha256'],record['compatibility_key'],ddp)
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
    parser.add_argument('--ddp-json')
    args = parser.parse_args()
    raise SystemExit(run(args.root.resolve(),args.job_id,args.attempt_token or args.job_id+':0',
                        args.resume_sha256,json.loads(args.ddp_json) if args.ddp_json else None))
