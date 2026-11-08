"""Controller-issued generations, renewal uncertainty and publication fencing."""
import json
from pathlib import Path
import sys
import threading
import time
import uuid
from contextlib import contextmanager
import requests

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'agent'))
from execution_lease import PROTOCOL,CLOCK_SKEW,STOP_GRACE,boot_id,clock,fence
from job_store import get_connection,get_job,get_jobs_by_status
from scheduler import load_nodes

ROOT=Path(__file__).resolve().parents[1]
renew_lock=threading.Lock()
START_CLOCK=clock()


def settings():
    value=json.loads((ROOT/'config/execution_leases.json').read_text())
    if not 10<=value['duration_seconds']<=60 or not 1<=value['renew_every_seconds']<value['duration_seconds']/2:
        raise ValueError('Invalid execution lease settings')
    return value


def supported(snapshot):
    return bool(snapshot and snapshot.get('execution_ownership',{}).get('protocol')==PROTOCOL
                and abs(snapshot.get('timestamp',0)-time.time())<=CLOCK_SKEW)


def get(job,node_id):
    connection=get_connection()
    try:
        row=connection.execute('SELECT * FROM execution_leases WHERE job_id=? AND retry_count=? AND node_id=?',
                               (job['job_id'],job['retry_count'],node_id)).fetchone()
        return dict(row) if row else None
    finally:connection.close()


def descriptor(row):
    return {key:row[key] for key in ('node_id','generation','lease_id','job_id','attempt_token','issued_at','expires_at')}


def ensure(job,node_id):
    """Persist the greatest possibly delivered deadline before sending any start."""
    connection=get_connection()
    try:
        with connection:
            connection.execute('BEGIN IMMEDIATE')
            current=connection.execute('SELECT status,retry_count,node_id,node_ip FROM jobs WHERE job_id=?',(job['job_id'],)).fetchone()
            reservation=connection.execute('SELECT job_id FROM resource_reservations WHERE node_id=?',(node_id,)).fetchone()
            if (not current or current['retry_count']!=job['retry_count'] or current['status'] not in ('SCHEDULED','RUNNING')
                    or not reservation or reservation[0]!=job['job_id']):raise ValueError('Execution no longer owns this reservation')
            row=connection.execute('SELECT * FROM execution_leases WHERE job_id=? AND retry_count=? AND node_id=?',
                                   (job['job_id'],job['retry_count'],node_id)).fetchone()
            if row:return descriptor(dict(row))
            existing=connection.execute('SELECT 1 FROM job_attempts WHERE job_id=? AND attempt_number=? AND status IN (?,?)',
                                        (job['job_id'],job['retry_count']+1,'SCHEDULED','RUNNING')).fetchone()
            if not existing:
                connection.execute('INSERT INTO job_attempts (job_id,attempt_number,node_id,node_ip,status) VALUES(?,?,?,?,?)',
                                   (job['job_id'],job['retry_count']+1,current['node_id'],current['node_ip'],'SCHEDULED'))
            connection.execute('INSERT INTO node_generations VALUES(?,1) ON CONFLICT(node_id) DO UPDATE SET generation=generation+1',(node_id,))
            generation=connection.execute('SELECT generation FROM node_generations WHERE node_id=?',(node_id,)).fetchone()[0]
            now=time.time();ttl=settings()['duration_seconds']
            connection.execute('INSERT INTO execution_leases VALUES(?,?,?,?,?,?,?,?,?,?,?,?)',
                (job['job_id'],job['retry_count'],node_id,generation,uuid.uuid4().hex,
                 f"{job['job_id']}:{job['retry_count']}",now,now+ttl,boot_id(),clock()+ttl,0,0))
        return descriptor(get(job,node_id))
    finally:connection.close()


def block(job):
    # A lost renewal reply might have reached the worker. Keep its latest
    # persisted deadline and stop extending every member of that attempt.
    connection=get_connection()
    try:
        with connection:connection.execute('UPDATE execution_leases SET renewal_blocked=1 WHERE job_id=? AND retry_count=?',
                                          (job['job_id'],job['retry_count']))
    finally:connection.close()


def safe_to_replace(job,node_id):
    row=get(job,node_id)
    if not row:return False
    margin=CLOCK_SKEW+STOP_GRACE
    return (time.time()>=row['expires_at']+margin and
            (clock()>=row['expires_clock']+margin if row['controller_boot_id']==boot_id()
             else clock()>=START_CLOCK+60+margin))


def revoke(job,node_id):
    connection=get_connection()
    try:
        with connection:connection.execute('UPDATE execution_leases SET revoked=1,renewal_blocked=1 WHERE job_id=? AND retry_count=? AND node_id=?',
                                          (job['job_id'],job['retry_count'],node_id))
    finally:connection.close()


def matches(job,node_id,worker):
    row=get(job,node_id)
    return (not row or (worker.get('attempt_token')==row['attempt_token']
            and all((worker.get('lease') or {}).get(key)==row[key] for key in ('node_id','generation','lease_id'))))


def current(job,node_id):
    if hybrid(job):
        connection=get_connection(durable=True)
        try:
            connection.execute('BEGIN')
            from hybrid_storage.staging_lifecycle import require_running
            require_running(connection,job['job_id'])
            latest=connection.execute('SELECT retry_count FROM jobs WHERE job_id=?',(job['job_id'],)).fetchone()
            row=connection.execute('SELECT * FROM execution_leases WHERE job_id=? AND retry_count=? AND node_id=?',
                                   (job['job_id'],job['retry_count'],node_id)).fetchone()
            if row is None or latest[0]!=job['retry_count']:raise ValueError('FENCED: stale result attempt')
            return dict(row)
        finally:connection.close()
    row=get(job,node_id)
    if not row:return None  # Older attempts keep conservative, unfenced reconciliation.
    latest=get_job(job['job_id'])
    if (not latest or latest['retry_count']!=job['retry_count']
            or latest['status'] not in ('SCHEDULED','RUNNING','CANCELLING') or row['revoked']):
        raise ValueError('FENCED: stale attempt may not publish checkpoints or results')
    return row


def hybrid(job):
    connection=get_connection()
    try:
        import storage_selection
        return storage_selection.binding(connection,job['job_id'])['backend']=='nfs'
    finally:connection.close()


@contextmanager
def publication(job,node_id):
    """Hold cancellation/retry/lease writers out of NFS final result publication."""
    if not hybrid(job):
        yield
        return
    connection=get_connection(durable=True)
    try:
        connection.execute('BEGIN IMMEDIATE')
        from hybrid_storage.staging_lifecycle import require_running
        require_running(connection,job['job_id'])
        latest=connection.execute('SELECT retry_count FROM jobs WHERE job_id=?',(job['job_id'],)).fetchone()
        if latest[0]!=job['retry_count']:raise ValueError('FENCED: stale publication')
        if not connection.execute('SELECT 1 FROM hybrid_staging_ranks r JOIN hybrid_staging_attempts a USING(stage_id) WHERE a.job_id=? AND a.retry_count=? AND r.worker_id=?',
                                  (job['job_id'],job['retry_count'],node_id)).fetchone():
            raise ValueError('FENCED: publication worker was not reserved')
        yield
        connection.commit()
    except BaseException:
        connection.rollback();raise
    finally:connection.close()


def params(job,node_id):
    row=current(job,node_id)
    return ({'attempt_token':row['attempt_token'],'generation':row['generation'],'lease_id':row['lease_id']}
            if row else {'attempt_token':f"{job['job_id']}:{job['retry_count']}"})


def checkpoint(job,info,node_id=None,ranks=None):
    """Validate embedded producer generations before replacing central bytes."""
    if ranks is not None:
        for rank in ranks:
            row=current(job,rank['node_id'])
            if row and info.get('rank_fences',{}).get(str(rank['rank']))!=fence(row):
                raise ValueError('FENCED: checkpoint rank producer differs from reserved generation')
    else:
        row=current(job,node_id or job['node_id'])
        if row and info.get('lease_fence')!=fence(row):
            raise ValueError('FENCED: checkpoint producer differs from reserved generation')


def response(job,node_id,headers):
    row=current(job,node_id)
    if row and (headers.get('X-Attempt-Token')!=row['attempt_token']
                or headers.get('X-Lease-Id')!=row['lease_id']
                or headers.get('X-Execution-Generation')!=str(row['generation'])):
        raise ValueError('FENCED: worker response belongs to a different execution owner')


def renew(job):
    # Hybrid leases require signed lifecycle control and its durable worker intent.
    # Never fall back to the legacy unsigned endpoint after a controller restart.
    if hybrid(job):return
    if not renew_lock.acquire(blocking=False):return
    try:
        connection=get_connection()
        try:
            rows=[dict(r) for r in connection.execute('SELECT * FROM execution_leases WHERE job_id=? AND retry_count=?',
                                                     (job['job_id'],job['retry_count']))]
            group=connection.execute('SELECT phase FROM distributed_attempts WHERE job_id=? AND retry_count=?',
                                     (job['job_id'],job['retry_count'])).fetchone()
        finally:connection.close()
        if (not rows or any(r['renewal_blocked'] or r['revoked'] for r in rows)
                or job['status'] not in ('SCHEDULED','RUNNING') or (group and group[0]=='STOPPING')):return
        nodes={n['id']:n for n in load_nodes()}
        for row in rows:
            if time.time()-row['issued_at']<settings()['renew_every_seconds']:continue
            if safe_to_replace(job,row['node_id']):block(job);return
            now=time.time();ttl=settings()['duration_seconds'];connection=get_connection()
            try:
                with connection:
                    # Recheck cancellation/rollback under the same database lock
                    # used to extend a deadline; a late renewal stays bounded.
                    latest=connection.execute('SELECT status,retry_count FROM jobs WHERE job_id=?',(job['job_id'],)).fetchone()
                    if latest['status'] not in ('SCHEDULED','RUNNING') or latest['retry_count']!=job['retry_count']:return
                    run=connection.execute('SELECT phase FROM distributed_attempts WHERE job_id=? AND retry_count=?',
                                           (job['job_id'],job['retry_count'])).fetchone()
                    if run and run[0]=='STOPPING':return
                    connection.execute('UPDATE execution_leases SET issued_at=?,expires_at=?,expires_clock=?,controller_boot_id=? WHERE lease_id=? AND renewal_blocked=0 AND revoked=0',
                                       (now,now+ttl,clock()+ttl,boot_id(),row['lease_id']))
            finally:connection.close()
            updated=get(job,row['node_id']);node=nodes[row['node_id']]
            try:
                reply=requests.post(f"http://{node['ip']}:{node['port']}/jobs/{job['job_id']}/lease",
                                    json={'lease':descriptor(updated)},timeout=2)
                reply.raise_for_status()
                if not matches(job,row['node_id'],reply.json()['job']):raise ValueError('Lease acknowledgment identity differs')
            except (requests.RequestException,ValueError,KeyError) as error:
                block(job);print(f"[LEASE_RENEWAL_BLOCKED] {job['job_id']}: {error}");return
    finally:renew_lock.release()


def start_renewals():
    def loop():
        while True:
            try:
                for state in ('SCHEDULED','RUNNING'):
                    for job in get_jobs_by_status(state):renew(job)
            except Exception as error:print('[LEASE_RENEWAL_ERROR]',error)
            time.sleep(1)
    threading.Thread(target=loop,daemon=True).start()
