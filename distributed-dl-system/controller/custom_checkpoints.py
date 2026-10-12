"""Central SDK checkpoint synchronization and validated restore transfers."""
import os
from pathlib import Path
import sys
import tempfile
import time

import requests
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'agent'))
from checkpoint_protocol import validate
from job_store import get_connection, get_job_environments
from recovery_metrics import log_event
from scheduler import load_nodes


def path_for(job):
    return Path(job['storage_path'])/'checkpoints/latest.dlckpt'


def metadata(job):
    path=path_for(job)
    return validate(path,job['job_id'],job['storage_manifest_sha256']) if path.exists() else None


def endpoint(job, node=None):
    node=node or next(n for n in load_nodes() if n['id']==job['node_id'])
    return f"http://{node['ip']}:{node['port']}/jobs/{job['job_id']}/custom-checkpoint"


def sync(job):
    """Reject corrupt, wrong-attempt or regressing bytes before replacing the central file."""
    destination=path_for(job)
    temporary=None
    try:
        with requests.get(endpoint(job),params={'attempt_token':f"{job['job_id']}:{job['retry_count']}"},stream=True,timeout=10) as response:
            if response.status_code==404: return False
            response.raise_for_status()
            with tempfile.NamedTemporaryFile(dir=destination.parent,delete=False) as output:
                temporary=Path(output.name)
                for chunk in response.iter_content(1024*1024): output.write(chunk)
                output.flush();os.fsync(output.fileno())
        info=validate(temporary,job['job_id'],job['storage_manifest_sha256'],f"{job['job_id']}:{job['retry_count']}")
        previous=metadata(job)
        if previous:
            if info['global_step']<previous['global_step'] or (previous['completed'] and not info['completed']):
                raise ValueError('Checkpoint progress regressed')
            if info['sha256']==previous['sha256']: return False
        candidate=next(row['record'] for row in get_job_environments(job['job_id']) if row['node_id']==job['node_id'])
        if info['compatibility_key']!=candidate['compatibility_key']:
            raise ValueError('Checkpoint differs from the prepared environment')
        os.replace(temporary,destination)
        descriptor=os.open(destination.parent,os.O_RDONLY|os.O_DIRECTORY)
        try: os.fsync(descriptor)
        finally: os.close(descriptor)
        connection=get_connection()
        try:
            with connection:
                connection.execute('UPDATE jobs SET checkpoint_path=?,checkpoint_updated_at=?,checkpoint_size_bytes=?,updated_at=? WHERE job_id=?',
                    (str(destination),time.time(),info['size_bytes'],time.time(),job['job_id']))
        finally: connection.close()
        log_event(job['job_id'],'CHECKPOINT_SYNC',job['node_id'],details={
            'global_step':info['global_step'],'sha256':info['sha256'],'completed':info['completed'],
            'checkpoint_path':str(destination),'attempt_token':info['attempt_token']})
        print(f"[CUSTOM_CHECKPOINT] {job['job_id']}: step={info['global_step']} completed={info['completed']}")
        return True
    except Exception as error:
        print(f"[CUSTOM_CHECKPOINT_PENDING] {job['job_id']}: {error}")
        return False
    finally:
        if temporary: temporary.unlink(missing_ok=True)


def restore(job, node):
    """Only controller-owned, input-bound checkpoint bytes may seed a new attempt."""
    info=metadata(job)
    if info is None: return None
    with path_for(job).open('rb') as stream:
        response=requests.post(endpoint(job,node),files={'file':('resume.dlckpt',stream,'application/octet-stream')},
            headers={'X-Manifest-SHA256':job['storage_manifest_sha256'],'X-Resume-SHA256':info['sha256']},timeout=30)
    response.raise_for_status()
    if response.json()['checkpoint']['sha256']!=info['sha256']:
        raise ValueError('Worker restore digest mismatch')
    log_event(job['job_id'],'RESTORE_PREPARED',node['id'],details={
        'sha256':info['sha256'],'global_step':info['global_step'],'source_attempt':info['attempt_token']})
    return info
