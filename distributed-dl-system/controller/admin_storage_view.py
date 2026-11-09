"""Bounded, read-only admin projection; reports observations, not eligibility.

No remote calls, schema migrations, manifest/file reads, cache enrollment or
authority reconstruction from historical SQLite rows. Missing metadata is null.
"""
import json
import math
import time

from hybrid_storage import admin_observations
from hybrid_storage.staging_metrics import validated
from hybrid_storage.staging_schema import require_local_read
from hybrid_storage.inventory_protocol import worker_id

MAX_WORKERS=256
MAX_DATASETS=20
MAX_HISTORY=10
MAX_EVENTS=300


def _history(db,tables,workers):
    result={w:[] for w in workers}
    seen=set()
    if not {'recovery_events','hybrid_staging_ranks','hybrid_staging_attempts'}<=tables:return result
    for row in db.execute("SELECT job_id,event_time,details_json FROM recovery_events WHERE event_type='INPUTS_STAGED' AND length(details_json)<=16384 ORDER BY id DESC LIMIT ?",(MAX_EVENTS,)):
        try:
            job,at,raw=row
            # SQLite REAL affinity can still contain text or infinities. Never
            # expose malformed metadata as a timestamp (or arbitrary text).
            if type(at) not in (int,float) or not math.isfinite(at) or not 0<=at<=253402300799:continue
            if len(raw)>16384:continue
            value=json.loads(raw);metrics=validated(value.get('metrics'))
            if metrics is None or type(value.get('rank')) is not int or type(value.get('attempt')) is not int:continue
            identity=(value.get('stage_id'),value['rank'])
            if not isinstance(identity[0],str) or identity in seen:continue
            match=db.execute('''SELECT r.worker_id FROM hybrid_staging_ranks r JOIN hybrid_staging_attempts a USING(stage_id)
                WHERE r.stage_id=? AND r.rank=? AND a.job_id=? AND a.retry_count=? AND r.phase='READY' ''',
                (value.get('stage_id'),value['rank'],job,value['attempt']-1)).fetchone()
            if match is None or match[0] not in result or len(result[match[0]])>=MAX_HISTORY:continue
            seen.add(identity)
            result[match[0]].append(dict(recorded_at=at,cache_hit_bytes=metrics['cache_hit_bytes'],
                transferred_bytes=metrics['staged_bytes'],copy_seconds=metrics['copy_ns']/1e9,
                verification_seconds=metrics['verification_ns']/1e9))
        except (ValueError,TypeError,AttributeError):continue
    return result


def _coverage(db,manifests,session):
    if not manifests:return dict(dataset_versions=0,complete_versions=0,required_bytes=0,cached_bytes=0)
    marks=','.join('?' for _ in manifests)
    rows=db.execute(f'''WITH required AS (
        SELECT manifest_sha256,artifact_sha256,size_bytes FROM storage_manifest_entries
        WHERE type='file' AND manifest_sha256 IN ({marks}) GROUP BY manifest_sha256,artifact_sha256,size_bytes)
        SELECT r.manifest_sha256,SUM(r.size_bytes),SUM(CASE WHEN e.state='VERIFIED' AND e.verified_bytes=r.size_bytes
            AND e.size_bytes=r.size_bytes AND e.last_verified_at_ns IS NOT NULL AND e.inventory_sequence<=?
            THEN r.size_bytes ELSE 0 END),SUM(CASE WHEN e.state='VERIFIED' AND e.verified_bytes=r.size_bytes
            AND e.size_bytes=r.size_bytes AND e.last_verified_at_ns IS NOT NULL AND e.inventory_sequence<=? THEN 0 ELSE 1 END)
        FROM required r LEFT JOIN cache_registry_entries e ON e.session_id=? AND e.sha256=r.artifact_sha256
        GROUP BY r.manifest_sha256''',(*manifests,session['last_sequence'],session['last_sequence'],session['session_id'])).fetchall()
    # Empty datasets are complete; missing zero-byte files are NOT complete.
    return dict(dataset_versions=len(manifests),complete_versions=len(manifests)-sum(r[3]>0 for r in rows),
                required_bytes=sum(r[1] for r in rows),cached_bytes=sum(r[2] for r in rows))


def read(db,workers,*,now_ns=None):
    require_local_read(db)
    now=time.time_ns() if now_ns is None else now_ns
    if type(now) is not int or now<1:raise ValueError('Invalid observation clock')
    identifiers=tuple(dict.fromkeys(worker_id(w) for w in workers))
    if len(identifiers)>MAX_WORKERS:raise ValueError('Worker display limit exceeded')
    tables={r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    cache_tables={'cache_registry_workers','cache_registry_sessions','cache_registry_identities','cache_registry_entries'}
    registry_tables={'storage_datasets','storage_dataset_versions','storage_manifest_entries'}
    manifests=[];truncated=False
    if registry_tables<=tables:
        found=list(db.execute('''SELECT v.manifest_sha256 FROM storage_datasets d JOIN storage_dataset_versions v
            ON v.dataset_id=d.dataset_id AND v.version=d.head_version ORDER BY d.dataset_id LIMIT ?''',(MAX_DATASETS+1,)))
        manifests=[r[0] for r in found[:MAX_DATASETS]];truncated=len(found)>MAX_DATASETS
    main=next(r[2] for r in db.execute('PRAGMA database_list') if r[1]=='main')
    from pathlib import Path
    observations=admin_observations.read(Path(main).parent)
    history=_history(db,tables,identifiers)
    result=[]
    for worker in identifiers:
        row=dict(worker_id=worker,inventory_status='not_reported',inventory_age_seconds=None,last_seen_at=None,
            disk_total_bytes=None,disk_free_bytes=None,disk_available_bytes=None,cache_limit_bytes=None,
            verified_cache_bytes=None,dataset_coverage=None,nfs=dict(status='unknown',observed_at=None,io_status='untested'),
            transfer_history=history[worker])
        session=None
        if cache_tables<=tables:
            cursor=db.execute('''SELECT s.*,w.enabled,w.auth_state,w.auth_principal AS worker_principal,
                w.last_seen_ns AS worker_seen,i.retired,i.epoch_high_water FROM cache_registry_sessions s
                JOIN cache_registry_workers w USING(worker_id) JOIN cache_registry_identities i USING(cache_id,worker_id)
                WHERE s.worker_id=? ORDER BY s.created_at_ns DESC,s.session_id LIMIT 1''',(worker,))
            found=cursor.fetchone()
            if found is not None:session=dict(zip((v[0] for v in cursor.description),found))
        if session is not None:
            at=session['inventory_updated_at_ns'];seen=session['last_seen_ns']
            fresh=(session['state']=='ACTIVE' and session['enabled']==1 and session['auth_state']=='ENROLLED'
                and session['auth_principal']==session['worker_principal'] and not session['retired']
                and session['cache_epoch']==session['epoch_high_water'] and session['has_full_snapshot']==1
                and type(at) is int and at<=now and type(session['fresh_until_ns']) is int and now<session['fresh_until_ns']
                and type(seen) is int and 0<=now-seen<=15*10**9
                and type(session['worker_seen']) is int and 0<=now-session['worker_seen']<=15*10**9)
            row.update(inventory_status='reported_fresh' if fresh else 'stale_or_untrusted',
                inventory_age_seconds=None if at is None or at>now else (now-at)/1e9,
                last_seen_at=None if seen is None else seen/1e9,
                **{k:session[k] for k in ('disk_total_bytes','disk_free_bytes','disk_available_bytes')})
            if fresh:
                row['verified_cache_bytes']=db.execute('''SELECT COALESCE(SUM(verified_bytes),0) FROM cache_registry_entries
                    WHERE session_id=? AND state='VERIFIED' AND verified_bytes=size_bytes AND last_verified_at_ns IS NOT NULL
                    AND inventory_sequence<=?''',(session['session_id'],session['last_sequence'])).fetchone()[0]
                if registry_tables<=tables:row['dataset_coverage']=_coverage(db,manifests,session)
                observed=observations.get(worker)
                if (type(observed) is dict and observed.get('session_id')==session['session_id']
                        and observed.get('sequence')==session['last_sequence'] and observed.get('received_at_ns')==seen):
                    health=admin_observations.health(observed.get('nfs'),now)
                    if health:row['nfs']=dict(status=health['status'],observed_at=health['observed_at_ns']/1e9,io_status='untested')
        result.append(row)
    return dict(schema_version=1,recorded_at=now/1e9,workers=result,
        coverage_scope='current_dataset_heads',coverage_limit=MAX_DATASETS,coverage_truncated=truncated,
        transfer_history_limit=MAX_HISTORY,transfer_scan_limit=MAX_EVENTS,
        inventory_is_advisory=True,scheduler_eligibility='not_evaluated')
