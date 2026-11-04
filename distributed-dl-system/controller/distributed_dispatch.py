"""Persist and reconcile a complete CPU DDP worker group as one scheduled job."""
import json
import secrets
import sqlite3
import time

import requests

from job_store import (get_connection, get_job, get_job_environments, get_ranks,
                       get_reservations, mark_scheduled, mark_running,
                       mark_completed, requeue_after_failure, finish_cancellation)
from scheduler import load_nodes, get_resources, resources_fit
from recovery_metrics import log_event, get_events
import execution_leases as ownership


def is_distributed(job):
    return (job.get('job_type') == 'custom'
            and json.loads(job['job_config']).get('execution',{}).get('mode') == 'distributed')


def attempt(job):
    return f"{job['job_id']}:{job['retry_count']}"


def group(job):
    connection = get_connection()
    try:
        row = connection.execute('SELECT * FROM distributed_attempts WHERE job_id=? AND retry_count=?',
                                 (job['job_id'],job['retry_count'])).fetchone()
        return dict(row) if row else None
    finally:
        connection.close()


def phase(job, value, error=None):
    connection = get_connection()
    try:
        with connection:
            connection.execute('UPDATE distributed_attempts SET phase=?,error=COALESCE(?,error) '
                               'WHERE job_id=? AND retry_count=?',
                               (value,error,job['job_id'],job['retry_count']))
    finally:
        connection.close()


def rank_status(job, rank, status, started_at=None, error=None):
    connection = get_connection()
    try:
        with connection:
            connection.execute('UPDATE job_ranks SET status=?,started_at=COALESCE(started_at,?),error=? '
                               'WHERE job_id=? AND retry_count=? AND rank=?',
                               (status,started_at,error,job['job_id'],job['retry_count'],rank))
    finally:
        connection.close()


def select_group(job, excluded, resume=None, require_ownership=False):
    """Only a full group of idle, prepared, compatible workers is eligible."""
    spec = json.loads(job['job_config'])
    prepared = {row['node_id']:row['record'] for row in get_job_environments(job['job_id'])}
    occupied = {row['node_id'] for row in get_reservations()}
    groups = {}
    for node in load_nodes():
        if node['id'] in occupied or node['id'] in excluded or node['id'] not in prepared:
            continue
        snapshot = get_resources(node)
        if snapshot and resources_fit(snapshot,spec['resources_per_worker']):
            if require_ownership and not ownership.supported(snapshot):continue
            key = prepared[node['id']]['compatibility_key']
            if resume and key != resume['compatibility_key']:
                continue
            groups.setdefault(key,[]).append((snapshot['cpu']['usage_percent'],
                                             -snapshot['memory']['available_gb'],node['id'],node))
    count = spec['execution']['workers']
    choices = [sorted(nodes)[:count] for nodes in groups.values() if len(nodes) >= count]
    if not choices:
        return None
    chosen = min(choices,key=lambda nodes:sum(node[0] for node in nodes))
    return [node[3] for node in chosen]


def dispatch(job, excluded):
    resume = None
    if json.loads(job['job_config'])['recovery']['mode']=='checkpoint':
        from group_checkpoints import resume_metadata
        try:
            resume = resume_metadata(job)
        except Exception as error:
            # Fail closed before allocation; do not silently train from zero
            # after corruption or loss of a previously committed checkpoint.
            connection = get_connection()
            try:
                with connection:
                    connection.execute("UPDATE jobs SET status='FAILED',error=?,last_error=?,finished_at=?,updated_at=? "
                                       "WHERE job_id=? AND status='QUEUED'",
                                       (str(error),str(error),time.time(),time.time(),job['job_id']))
            finally:
                connection.close()
            log_event(job['job_id'],'DDP_RESUME_REJECTED',details={'error':str(error)})
            return False
    leased=ownership.settings()['enabled']
    workers = select_group(job,excluded,resume,require_ownership=leased)
    if not workers:
        return False
    # A distinct rendezvous port per attempt avoids joining a previous group's
    # TCP store. Each worker has one managed slot, including the rank-zero host.
    context = {'world_size':len(workers),'master_addr':workers[0]['ip'],
               'master_port':20000 + secrets.randbelow(30000)}
    if leased:context['leases_enabled']=True
    if resume:
        context['resume_checkpoint'] = resume
    try:
        mark_scheduled(job['job_id'],workers[0]['id'],workers[0]['ip'],
                       json.loads(job['job_config'])['resources_per_worker'],workers,context)
    except (ValueError,sqlite3.IntegrityError):
        return False
    log_event(job['job_id'],'DDP_RESERVED',workers[0]['id'],details={
        **context,'workers':[node['id'] for node in workers],'retry_count':job['retry_count']})
    print(f"[DDP_RESERVED] {job['job_id']}: {[node['id'] for node in workers]}")
    progress(get_job(job['job_id']))
    return True


def url(job, rank):
    return f"http://{rank['node_ip']}:{rank['node_port']}/jobs/{job['job_id']}"


def worker(job, rank):
    response = requests.get(url(job,rank),timeout=2)
    if response.status_code == 404:
        return None
    response.raise_for_status()
    return response.json()['job']


def payload(job, rank, context):
    record = next(row['record'] for row in get_job_environments(job['job_id'])
                  if row['node_id'] == rank['node_id'])
    result = {'job_id':job['job_id'],'job_type':'custom','attempt_token':attempt(job),
            'manifest_sha256':job['storage_manifest_sha256'],
            'environment_key':record['environment_key'],
            'resources':json.loads(job['job_config'])['resources_per_worker'],
            'ddp':{**launch_context(context),'rank':rank['rank']}}
    if context.get('leases_enabled'):
        result['lease']=ownership.ensure(job,rank['node_id'])
    if context.get('resume_checkpoint'):
        result['resume_checkpoint'] = context['resume_checkpoint']
    return result


def launch_context(context):
    return {key:context[key] for key in ('world_size','master_addr','master_port')}


def stop_group(job, reason):
    # Persist rollback before making stop requests. A controller restart must
    # continue stopping peers rather than relaunch a partially failed group.
    phase(job,'STOPPING',reason)
    print(f"[DDP_STOPPING] {job['job_id']}: {reason}")


def collect_ranks(job, ranks, required):
    from job_artifacts import collect
    for rank in ranks:
        if rank['status']=='FENCED':continue
        try:
            collect({**job,'node_id':rank['node_id'],'node_ip':rank['node_ip']},rank=rank['rank'])
        except Exception as error:
            print(f"[DDP_ARTIFACTS_PENDING] {job['job_id']} rank={rank['rank']}: {error}")
            if required:
                return False
    return True


def reconcile_stop(job, run, ranks):
    """No slot is released until every rank is stopped and late starts are fenced."""
    all_stopped = True
    for rank in ranks:
        if rank['status']=='FENCED':continue
        try:
            # A tombstone is required even for a missing or completed rank: an
            # earlier HTTP start may still arrive after this observation.
            response = requests.post(url(job,rank)+'/stop',
                                     json={'attempt_token':attempt(job),'force':True},timeout=3)
            response.raise_for_status()
            observed = worker(job,rank)
            if observed is None or observed['status'] in ('COMPLETED','FAILED'):
                rank_status(job,rank['rank'],'STOPPED')
            else:
                all_stopped = False
        except (requests.RequestException,ValueError,KeyError) as error:
            ownership.block(job)
            never_sent=(json.loads(run['context_json']).get('leases_enabled') and not ownership.get(job,rank['node_id']))
            if ownership.safe_to_replace(job,rank['node_id']) or never_sent:
                ownership.revoke(job,rank['node_id'])
                rank_status(job,rank['rank'],'FENCED',error='Old execution lease expired')
                log_event(job['job_id'],'EXECUTION_FENCED',rank['node_id'],details={'retry_count':job['retry_count']})
            else:
                all_stopped = False
            print(f"[DDP_STOP_PENDING] {job['job_id']} rank={rank['rank']}: {error}")
    if not all_stopped:
        return
    collect_ranks(job,ranks,required=False)
    current = get_job(job['job_id'])
    if current['status'] == 'CANCELLING':
        finish_cancellation(job['job_id'])
        phase(job,'CANCELLED')
    else:
        phase(job,'FAILED')
        state = requeue_after_failure(job['job_id'],run['error'] or 'DDP group failed')
        print(f"[DDP_RESTART] {job['job_id']}: {state}; all ranks stopped")


def progress(job):
    """Reconcile start, group completion and rollback using durable rank identities."""
    run = group(job)
    if not run:
        raise ValueError('Distributed job has no persisted worker group')
    ranks = get_ranks(job['job_id'],job['retry_count'])
    checkpoint_enabled = json.loads(job['job_config'])['recovery']['mode']=='checkpoint'
    if checkpoint_enabled:
        from group_checkpoints import sync
        sync(job)
    if job['status'] == 'CANCELLING' and run['phase'] != 'STOPPING':
        stop_group(job,'Cancelled by owner')
        run = group(job)
    if run['phase'] == 'STOPPING':
        reconcile_stop(job,run,ranks)
        return
    ownership.renew(job)
    context = json.loads(run['context_json'])
    resume = context.get('resume_checkpoint')
    if resume and not context.get('restore_prepared'):
        try:
            from group_checkpoints import restore
            for rank in ranks:
                if get_job(job['job_id'])['status']=='CANCELLING':
                    stop_group(job,'Cancelled during checkpoint transfer')
                    return
                restore(job,rank,resume)
            context['restore_prepared'] = True
            connection = get_connection()
            try:
                with connection:
                    connection.execute('UPDATE distributed_attempts SET context_json=? WHERE job_id=? AND retry_count=?',
                                       (json.dumps(context),job['job_id'],job['retry_count']))
            finally:
                connection.close()
            log_event(job['job_id'],'DDP_RESTORE_PREPARED',details={**resume,'retry_count':job['retry_count']})
            print(f"[DDP_RESTORE_PREPARED] {job['job_id']}: step={resume['global_step']} ranks={len(ranks)}")
        except requests.RequestException as error:
            if isinstance(error,requests.HTTPError) or time.time()-run['created_at']>180:
                stop_group(job,'DDP checkpoint transfer failed: '+str(error))
            else:
                print(f"[DDP_RESTORE_PENDING] {job['job_id']}: {error}")
            return
        except (OSError,ValueError,KeyError) as error:
            stop_group(job,'DDP checkpoint transfer invalid: '+str(error))
            return
    all_started = all_completed = True
    started = []
    restored = []
    for rank in ranks:
        try:
            observed = worker(job,rank)
            if run['phase'] == 'STARTING' and (observed is None or observed.get('attempt_token') != attempt(job)):
                if rank['status'] != 'RESERVED':
                    stop_group(job,f"Previously started rank {rank['rank']} was lost during group launch")
                    return
                # The original placement is retried idempotently. A timeout keeps
                # the whole group reserved until the same agents can be queried.
                if get_job(job['job_id'])['status'] == 'CANCELLING':
                    stop_group(job,'Cancelled during group launch')
                    return
                response = requests.post(
                    f"http://{rank['node_ip']}:{rank['node_port']}/jobs/start",
                    json=payload(job,rank,context),timeout=5)
                if response.status_code != 200:
                    stop_group(job,f"Rank {rank['rank']} launch rejected: HTTP {response.status_code}")
                    return
                observed = response.json()['job']
            expected = {**launch_context(context),'rank':rank['rank']}
            if observed is None or observed.get('attempt_token') != attempt(job) or observed.get('ddp') != expected:
                stop_group(job,f"Rank {rank['rank']} attempt missing or identity differs")
                return
            if not ownership.matches(job,rank['node_id'],observed):
                ownership.block(job)
                stop_group(job,f"Rank {rank['rank']} returned a stale execution owner")
                return
            status = observed['status']
            rank_status(job,rank['rank'],status,observed.get('started_at'))
            if status == 'FAILED':
                stop_group(job,f"Rank {rank['rank']} exited with code {observed.get('return_code')}")
                return
            if status not in ('RUNNING','COMPLETED'):
                all_started = False
            all_completed = all_completed and status == 'COMPLETED'
            if resume:
                if restore_confirmed(job,rank,context,observed):
                    restored.append(rank['rank'])
                else:
                    all_started = all_completed = False
                    if status=='COMPLETED' or time.time()-run['created_at']>180:
                        stop_group(job,f"Rank {rank['rank']} did not confirm the required DDP restore")
                        return
            started.append(observed['started_at'])
        except (requests.RequestException,ValueError,KeyError,StopIteration) as error:
            all_started = all_completed = False
            ownership.block(job)
            if context.get('leases_enabled'):
                stop_group(job,f"Rank {rank['rank']} contact lost; waiting for stop or lease fencing")
            print(f"[DDP_CONTACT_PENDING] {job['job_id']} rank={rank['rank']}: {error}")
    if run['phase'] == 'STARTING' and all_started:
        if resume:
            # A restart between event logging and the phase update must not
            # report the same successful restore as another recovery.
            recorded = any(event['event_type']=='DDP_RESUMED' and
                           json.loads(event['details_json']).get('retry_count')==job['retry_count']
                           for event in get_events(job['job_id']))
            if not recorded:
                log_event(job['job_id'],'DDP_RESUMED',job['node_id'],details={
                    **resume,'retry_count':job['retry_count'],'acknowledged_ranks':restored,
                    'workers':[rank['node_id'] for rank in ranks]})
            print(f"[DDP_RESUMED] {job['job_id']}: step={resume['global_step']} acknowledged={len(restored)}/{len(ranks)}")
        phase(job,'RUNNING')
        log_event(job['job_id'],'DDP_STARTED',job['node_id'],details={
            **context,'workers':[rank['node_id'] for rank in ranks],'retry_count':job['retry_count']})
        print(f"[DDP_RUNNING] {job['job_id']}: {len(ranks)} ranks")
    if all_started:
        mark_running(job['job_id'],min(started))
    if all_completed and checkpoint_enabled:
        from group_checkpoints import final_ready, missing_final
        try:
            ready = final_ready(job)
        except Exception as error:
            stop_group(job,'Final group checkpoint invalid: '+str(error))
            return
        if not ready:
            print(f"[DDP_FINAL_CHECKPOINT_PENDING] {job['job_id']}")
            if missing_final[job['job_id']] >= 3:
                stop_group(job,'Trainer exited without a complete acknowledged final DDP checkpoint')
            return
    if all_completed and collect_ranks(job,ranks,required=True):
        if mark_completed(job['job_id']):
            phase(job,'COMPLETED')
            print(f"[DDP_COMPLETED] {job['job_id']}: all {len(ranks)} ranks finished")


def restore_confirmed(job, rank, context, observed):
    expected = context['resume_checkpoint']
    info = observed.get('resume') or {}
    from checkpoint_protocol import group_identity
    return bool(info.get('job_id')==job['job_id'] and info.get('attempt_token')==attempt(job)
        and info.get('pid')==observed.get('pid') and info.get('confirmed_at',0)>=observed['started_at']
        and info.get('rank')==rank['rank'] and info.get('world_size')==context['world_size']
        and info.get('group_id')==group_identity(job['job_id'],attempt(job),job['storage_manifest_sha256'],
                                               expected['compatibility_key'],context)
        and info.get('checkpoint_sha256')==expected['sha256']
        and info.get('checkpoint_id')==expected['checkpoint_id']
        and info.get('source_attempt')==expected['attempt_token']
        and info.get('restored_global_step')==expected['global_step']
        and all(info.get(key) is True for key in ('model_restored','optimizer_restored','rng_restored',
                                                 'application_state_restored','all_ranks_restored')))
