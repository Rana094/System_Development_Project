"""Validate and atomically retain complete DDP checkpoint bundles on the controller."""
import json
import os
from pathlib import Path
import sys
import tempfile
import time

import requests
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'agent'))
from checkpoint_protocol import group_identity, validate_group
from job_store import get_connection, get_job_environments, get_ranks
from recovery_metrics import log_event
import execution_leases as ownership

last_sync = {}
missing_final = {}


def path_for(job):
    return Path(job['storage_path'])/'checkpoints/latest.ddpckpt'


def metadata(job):
    path = path_for(job)
    return validate_group(path,job['job_id'],job['storage_manifest_sha256']) if path.exists() else None


def sync(job, force=False):
    """Partial, corrupt, stale or differently placed archives cannot replace the last cut."""
    if not force and time.monotonic()-last_sync.get(job['job_id'],0) < 1:
        return False
    last_sync[job['job_id']] = time.monotonic()
    token = f"{job['job_id']}:{job['retry_count']}"
    destination = path_for(job)
    temporary = None
    try:
        ranks = get_ranks(job['job_id'],job['retry_count'])
        root = next(rank for rank in ranks if rank['rank']==0)
        url = f"http://{root['node_ip']}:{root['node_port']}/jobs/{job['job_id']}/custom-checkpoint"
        with requests.get(url,params=ownership.params(job,root['node_id']),stream=True,timeout=(2,15)) as response:
            if response.status_code==404:
                return False
            response.raise_for_status()
            ownership.response(job,root['node_id'],response.headers)
            with tempfile.NamedTemporaryFile(dir=destination.parent,delete=False) as output:
                temporary = Path(output.name)
                for chunk in response.iter_content(1024*1024):
                    output.write(chunk)
                output.flush();os.fsync(output.fileno())
        info = validate_group(temporary,job['job_id'],job['storage_manifest_sha256'],token)
        spec = json.loads(job['job_config'])
        if info['world_size'] != spec['execution']['workers'] or len(ranks) != info['world_size']:
            raise ValueError('Checkpoint worker count differs from the reserved group')
        prepared = {row['node_id']:row['record'] for row in get_job_environments(job['job_id'])}
        if any(prepared[rank['node_id']]['compatibility_key'] != info['compatibility_key'] for rank in ranks):
            raise ValueError('Checkpoint environment differs from the reserved workers')
        connection = get_connection()
        try:
            context = json.loads(connection.execute('SELECT context_json FROM distributed_attempts '
                'WHERE job_id=? AND retry_count=?',(job['job_id'],job['retry_count'])).fetchone()[0])
        finally:
            connection.close()
        expected = group_identity(job['job_id'],token,job['storage_manifest_sha256'],
                                  info['compatibility_key'],context)
        if info['group_id'] != expected:
            raise ValueError('Checkpoint belongs to a different rendezvous group')
        previous = metadata(job)
        if previous:
            if info['sha256'] == previous['sha256']:
                return False
            if (info['global_step'] < previous['global_step']
                    or (previous['completed'] and not info['completed'])):
                raise ValueError('Group checkpoint progress regressed')
        ownership.checkpoint(job,info,ranks=ranks)
        os.replace(temporary,destination)
        descriptor = os.open(destination.parent,os.O_RDONLY|os.O_DIRECTORY)
        try: os.fsync(descriptor)
        finally: os.close(descriptor)
        connection = get_connection()
        try:
            with connection:
                connection.execute('UPDATE jobs SET checkpoint_path=?,checkpoint_updated_at=?, '
                    'checkpoint_size_bytes=?,updated_at=? WHERE job_id=?',
                    (str(destination),time.time(),info['size_bytes'],time.time(),job['job_id']))
        finally:
            connection.close()
        log_event(job['job_id'],'DDP_CHECKPOINT_COMMITTED',root['node_id'],details={
            'checkpoint_id':info['checkpoint_id'],'global_step':info['global_step'],
            'world_size':info['world_size'],'acknowledged_ranks':len(info['rank_acknowledgments']),
            'completed':info['completed'],'sha256':info['sha256'],'attempt_token':token,
            'shared_state_sha256':info['shared_state_sha256'],'checkpoint_path':str(destination)})
        print(f"[DDP_CHECKPOINT] {job['job_id']}: step={info['global_step']} "
              f"acknowledged={len(info['rank_acknowledgments'])}/{info['world_size']} completed={info['completed']}")
        return True
    except Exception as error:
        print(f"[DDP_CHECKPOINT_PENDING] {job['job_id']}: {error}")
        return False
    finally:
        if temporary:
            temporary.unlink(missing_ok=True)


def final_ready(job):
    """A successful process exit alone is insufficient for a checkpoint-enabled job."""
    sync(job,force=True)
    info = metadata(job)
    valid = bool(info and info['completed'] and info['attempt_token']==f"{job['job_id']}:{job['retry_count']}")
    if valid:
        missing_final.pop(job['job_id'],None)
        return True
    missing_final[job['job_id']] = missing_final.get(job['job_id'],0)+1
    return False


def resume_metadata(job):
    """Use a validated earlier cut; a missing known checkpoint must never mean restart."""
    if job.get('checkpoint_path') and not path_for(job).is_file():
        raise ValueError('The recorded central DDP checkpoint is missing')
    info = metadata(job)
    if info is None:
        return None
    source_retry = int(info['attempt_token'].rsplit(':',1)[1])
    if source_retry >= job['retry_count']:
        raise ValueError('DDP resume must use an earlier attempt')
    if info['world_size'] != json.loads(job['job_config'])['execution']['workers']:
        raise ValueError('DDP resume requires the saved worker count')
    connection = get_connection()
    try:
        row = connection.execute('SELECT context_json FROM distributed_attempts WHERE job_id=? AND retry_count=?',
                                 (job['job_id'],source_retry)).fetchone()
    finally:
        connection.close()
    if not row or info['group_id'] != group_identity(job['job_id'],info['attempt_token'],
            job['storage_manifest_sha256'],info['compatibility_key'],json.loads(row[0])):
        raise ValueError('DDP resume source group is not a recorded attempt')
    return {key:info[key] for key in ('sha256','global_step','checkpoint_id','compatibility_key',
                                     'world_size','attempt_token','group_id')}


def restore(job, rank, expected):
    """Transfer the frozen source bytes while every rank is still reserved and idle."""
    info = metadata(job)
    if info is None or any(info[key] != value for key,value in expected.items()):
        raise ValueError('Central DDP checkpoint changed during group preparation')
    url = f"http://{rank['node_ip']}:{rank['node_port']}/jobs/{job['job_id']}/custom-checkpoint"
    with path_for(job).open('rb') as stream:
        response = requests.post(url,files={'file':('resume.dlckpt',stream,'application/octet-stream')},
            headers={'X-Manifest-SHA256':job['storage_manifest_sha256'],'X-Resume-SHA256':expected['sha256']},timeout=30)
    response.raise_for_status()
    received = response.json()['checkpoint']
    if any(received[key] != value for key,value in expected.items()):
        raise ValueError('Worker DDP restore identity differs from the frozen source')
