"""Durable worker lease identity and bounded local execution deadlines."""
import json
import math
import os
from pathlib import Path
import re
import tempfile
import time

PROTOCOL=1
CLOCK_SKEW=2.0
STOP_GRACE=3.0
MAX_TTL=60.0


def boot_id():
    return Path('/proc/sys/kernel/random/boot_id').read_text().strip()


def clock():
    # BOOTTIME includes suspend, so a suspended worker cannot extend its lease.
    return time.clock_gettime(time.CLOCK_BOOTTIME)


def atomic(path,value):
    path.parent.mkdir(parents=True,exist_ok=True)
    temporary=None
    try:
        with tempfile.NamedTemporaryFile(mode='w',dir=path.parent,delete=False) as stream:
            temporary=Path(stream.name)
            json.dump(value,stream,allow_nan=False);stream.flush();os.fsync(stream.fileno())
        os.replace(temporary,path)
        fd=os.open(path.parent,os.O_RDONLY|os.O_DIRECTORY)
        try:os.fsync(fd)
        finally:os.close(fd)
    finally:
        if temporary:temporary.unlink(missing_ok=True)


def fence(lease):
    return {key:lease[key] for key in ('node_id','generation','lease_id')}


def validate_fence(value):
    if (not isinstance(value,dict) or set(value)!={'node_id','generation','lease_id'}
            or type(value['generation']) is not int or value['generation']<1
            or not re.fullmatch(r'[0-9a-f]{32}',str(value['lease_id']))
            or not re.fullmatch(r'[A-Za-z0-9._-]{1,128}',str(value['node_id']))):
        raise ValueError('Invalid execution fence')


def validate(lease,job_id,attempt,node):
    if not isinstance(lease,dict):raise ValueError('An execution lease is required')
    validate_fence(fence(lease))
    if lease['node_id']!=node or lease['job_id']!=job_id or lease['attempt_token']!=attempt:
        raise ValueError('Execution lease identity differs from the launch')
    for key in ('issued_at','expires_at'):
        if type(lease[key]) not in (int,float) or not math.isfinite(lease[key]):
            raise ValueError('Invalid lease time')
    if not 0<lease['expires_at']-lease['issued_at']<=MAX_TTL:
        raise ValueError('Lease duration exceeds the supported limit')
    if abs(time.time()-lease['issued_at'])>CLOCK_SKEW or lease['expires_at']<=time.time():
        raise ValueError('Lease is expired, delayed, or clocks differ by more than two seconds')


def authority_path(root):
    return Path(root)/'worker-storage/execution-authority.json'


def lease_path(root,lease):
    return Path(root)/'worker-storage/leases'/f"{lease['lease_id']}.json"


def read(path):
    return json.loads(Path(path).read_text())


def alive(record):
    return (record['boot_id']==boot_id() and time.time()<record['lease']['expires_at']
            and clock()<record['deadline'] and not record.get('revoked',False))


def accept(root,lease,job_id,attempt,node):
    """Persist a higher generation before launch; equal generations only renew one owner."""
    validate(lease,job_id,attempt,node)
    authority=authority_path(root)
    previous=read(authority) if authority.exists() else None
    if previous:
        old=previous['lease']
        if lease['generation']<old['generation']:
            raise ValueError('Stale execution generation')
        if lease['generation']==old['generation']:
            if (fence(lease)!=fence(old) or lease['job_id']!=old['job_id']
                    or lease['attempt_token']!=old['attempt_token'] or not alive(previous)):
                raise ValueError('Execution generation belongs to another or expired owner')
    deadline=clock()+lease['expires_at']-time.time()
    record={'lease':lease,'boot_id':boot_id(),'deadline':deadline}
    if previous and lease['generation']==previous['lease']['generation']:
        record.update({key:previous[key] for key in ('started','guardian_pid','guardian_key','completed') if key in previous})
    if previous and lease['generation']==previous['lease']['generation']:
        # Late renewals cannot shorten a newer accepted deadline.
        if lease['expires_at']<=previous['lease']['expires_at']:
            return previous
    atomic(lease_path(root,lease),record)
    atomic(authority,record)
    return record


def assert_owned(root,job_id,attempt,generation,lease_id,allow_completed=False):
    record=read(authority_path(root));lease=record['lease']
    if (lease['job_id']!=job_id or lease['attempt_token']!=attempt
            or lease['generation']!=generation or lease['lease_id']!=lease_id):
        raise ValueError('Stale execution ownership')
    if not allow_completed and not alive(record):
        raise ValueError('Execution lease expired')
    return record


def sdk_fence():
    """A save must check the current lease, including immediately before publication."""
    path=os.environ.get('DL_EXECUTION_LEASE_PATH')
    if not path:return None
    record=read(path);lease=record['lease']
    if (not alive(record) or lease['job_id']!=os.environ['DL_JOB_ID']
            or lease['attempt_token']!=os.environ['DL_ATTEMPT_TOKEN']):
        raise ValueError('Training attempt no longer owns a valid execution lease')
    root=Path(os.environ['DL_WORKER_ROOT'])
    assert_owned(root,lease['job_id'],lease['attempt_token'],lease['generation'],lease['lease_id'])
    return fence(lease)


def process_key(pid):
    try:
        return Path(f'/proc/{pid}/stat').read_text().rsplit(')',1)[1].split()[19]
    except (OSError,IndexError):return None


def attach(root,lease,pid):
    record=read(lease_path(root,lease))
    record.update(started=True,guardian_pid=pid,guardian_key=process_key(pid))
    atomic(lease_path(root,lease),record);atomic(authority_path(root),record)


def finished(root,lease,completed):
    record=read(lease_path(root,lease));record['completed']=completed
    atomic(lease_path(root,lease),record)
    authority=read(authority_path(root))
    if fence(authority['lease'])==fence(lease):atomic(authority_path(root),record)


def reap_previous(root):
    """An API restart must not advertise an empty slot while an orphan still runs."""
    path=authority_path(root)
    if not path.exists():return
    record=read(path);pid=record.get('guardian_pid')
    if record['boot_id']!=boot_id() or not pid:return
    actual_key=process_key(pid)
    if not actual_key or actual_key!=record.get('guardian_key'):return
    import signal
    import psutil
    process=psutil.Process(pid)
    if process.status()==psutil.STATUS_ZOMBIE:return
    record['revoked']=True
    atomic(lease_path(root,record['lease']),record);atomic(path,record)
    try:os.killpg(pid,signal.SIGKILL)
    except ProcessLookupError:return
    _,alive_processes=psutil.wait_procs([process],timeout=STOP_GRACE)
    if any(p.status()!=psutil.STATUS_ZOMBIE for p in alive_processes):
        raise RuntimeError('Previous leased guardian has not stopped; worker remains unavailable')
