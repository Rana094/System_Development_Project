"""Owner-scoped recovery milestones from controller records; no worker/file I/O.

Read one caller-owned SQLite snapshot. Never deserialize checkpoint/source JSON,
infer percentages from file existence, mutate recovery state or grant execution.
"""
import json
import math
import time

MAX_RANKS=256
LABELS={
    'QUEUED':'Waiting for replacement workers', 'HELD':'Recovery is on hold',
    'STAGING':'Preparing recovery inputs', 'RESTORING':'Preparing checkpoint copies',
    'STARTING':'Starting training processes', 'CONFIRMING':'Waiting for training-state restoration',
    'CONFIRMED':'Training state restored; waiting for running status',
    'RESUMED':'Training resumed from checkpoint', 'RESTARTED':'Training restarted',
    'STOPPING':'Stopping recovery and reconciling execution ownership',
    'CANCELLED':'Recovery cancelled', 'FAILED':'Recovery stopped; job failed',
    'COMPLETED':'Training finished after recovery', 'UNKNOWN':'Recovery details are not recorded',
}


def _number(value):
    return type(value) in (int,float) and math.isfinite(value) and value>=0


def _phase(view,status,stage_phase,blocked,confirmed):
    if status in ('CANCELLED','FAILED','COMPLETED'):return status
    if status=='CANCELLING' or stage_phase in ('ABORTING','TERMINAL') or blocked:return 'STOPPING'
    if status=='HELD':return 'HELD'
    if status=='QUEUED':return 'QUEUED'
    if view['source']['state']=='unavailable' or view['restore_required'] is None:return 'UNKNOWN'
    if status=='RUNNING':
        if view['restore_required']:
            return 'RESUMED' if confirmed and view['restored_ranks']==view['total_ranks'] else 'UNKNOWN'
        return 'RESTARTED'
    if status!='SCHEDULED':return 'UNKNOWN'
    if stage_phase is None or stage_phase=='STAGING':return 'STAGING'
    if view['inputs_ready_ranks']!=view['total_ranks']:return 'UNKNOWN'
    if view['restore_required'] and view['checkpoint_ready_ranks']!=view['total_ranks']:return 'RESTORING'
    if view['launched_ranks']!=view['total_ranks']:return 'STARTING'
    if view['restore_required']:
        return 'CONFIRMED' if confirmed and view['restored_ranks']==view['total_ranks'] else 'CONFIRMING'
    return 'STARTING'


def read(db,job_id,owner,*,now=None):
    # Authorization precedes all optional recovery/checkpoint table access.
    job=db.execute('''SELECT j.owner,j.job_type,COALESCE(b.backend,'legacy'),j.retry_count,j.status,j.job_config,j.updated_at
        FROM jobs j LEFT JOIN job_storage_bindings b ON b.job_id=j.job_id WHERE j.job_id=?''',(job_id,)).fetchone()
    if job is None or tuple(job[:3])!=(owner,'custom','nfs') or type(job[3]) is not int or job[3]<1:return None
    view=dict(schema_version=1,attempt=job[3]+1,mode=None,phase='UNKNOWN',label=LABELS['UNKNOWN'],
        total_ranks=None,inputs_ready_ranks=None,checkpoint_ready_ranks=None,launched_ranks=None,restored_ranks=None,
        restore_required=None,source=dict(state='not_selected',global_step=None,size_bytes=None,from_attempt=None),
        ranks=[],ranks_truncated=False,updated_at=job[6] if _number(job[6]) else None,
        observation='controller_records_only')
    try:
        spec=json.loads(job[5]);mode=spec['recovery']['mode'];total=spec['execution']['workers']
        if mode not in ('checkpoint','restart') or type(total) is not int or not 1<=total<=65536:return view
    except (ValueError,TypeError,KeyError):return view
    view.update(mode=mode,total_ranks=total)
    tables={r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    restored=None;confirmed=False;stage=None;blocked=False
    if mode=='restart':view['restore_required']=False;view['source']['state']='not_required'
    if mode=='checkpoint' and 'hybrid_restore_attempts' in tables:
        restored=db.execute('''SELECT source_id,stage_id,confirmed,binding_sha256 FROM hybrid_restore_attempts
            WHERE job_id=? AND retry_count=?''',(job_id,job[3])).fetchone()
        if restored is not None:
            if restored[0] is None:
                view['restore_required']=False;view['source']['state']='none'
            elif {'hybrid_checkpoint_publications','storage_job_inputs'}<=tables:
                source=db.execute('''SELECT p.global_step,p.size_bytes,p.retry_count FROM hybrid_checkpoint_publications p
                    JOIN storage_job_inputs i ON i.job_id=p.job_id AND i.binding_sha256=p.binding_sha256
                    WHERE p.publication_id=? AND p.job_id=? AND p.owner=? AND i.owner=?
                    AND p.phase='COMMITTED' AND p.retry_count<? AND p.binding_sha256=? AND p.kind=?''',
                    (restored[0],job_id,owner,owner,job[3],restored[3],'ddp' if total>1 else 'single')).fetchone()
                if (source is not None and type(source[0]) is int and source[0]>=0
                        and type(source[1]) is int and source[1]>0 and type(source[2]) is int and 0<=source[2]<job[3]):
                    view['restore_required']=True
                    view['source']=dict(state='committed',global_step=source[0],size_bytes=source[1],from_attempt=source[2]+1)
                else:view['source']['state']='unavailable'
            else:view['source']['state']='unavailable'
    if {'hybrid_staging_attempts','hybrid_staging_ranks'}<=tables:
        stage=db.execute('''SELECT stage_id,phase,world_size,updated_at_ns FROM hybrid_staging_attempts
            WHERE job_id=? AND owner=? AND retry_count=?''',(job_id,owner,job[3])).fetchone()
        if stage is not None and stage[2]!=total:stage=None
        if stage is not None:
            view['updated_at']=max(view['updated_at'] or 0,stage[3]/1e9)
            counts=db.execute("SELECT COUNT(*),SUM(phase='READY') FROM hybrid_staging_ranks WHERE stage_id=?",(stage[0],)).fetchone()
            view['inputs_ready_ranks']=counts[1] if counts[0]==total else None
            can_restore=(restored is not None and restored[1]==stage[0] and view['source']['state'] in ('committed','none')
                         and 'hybrid_restore_ranks' in tables)
            by_rank={}
            if can_restore:
                counts=db.execute('''SELECT COUNT(*),COALESCE(SUM(receipt_sha256 IS NOT NULL),0),
                    COALESCE(SUM(launch_sent=1 AND started_at IS NOT NULL),0),COALESCE(SUM(restored=1),0)
                    FROM hybrid_restore_ranks WHERE stage_id=?''',(stage[0],)).fetchone()
                if counts[0]==total:
                    view.update(checkpoint_ready_ranks=counts[1],launched_ranks=counts[2],restored_ranks=counts[3])
                    confirmed=restored[2]==1 and counts[3]==total
                for row in db.execute('''SELECT rank,receipt_sha256 IS NOT NULL,launch_sent=1 AND started_at IS NOT NULL,restored=1
                    FROM hybrid_restore_ranks WHERE stage_id=? ORDER BY rank LIMIT ?''',(stage[0],MAX_RANKS)):
                    by_rank[row[0]]=tuple(bool(v) for v in row[1:])
            for rank,phase in db.execute('SELECT rank,phase FROM hybrid_staging_ranks WHERE stage_id=? ORDER BY rank LIMIT ?',(stage[0],MAX_RANKS)):
                copied,launched,ack=by_rank.get(rank,(None,None,None))
                view['ranks'].append(dict(rank=rank,inputs_ready=phase=='READY',checkpoint_ready=copied,
                    launch_acknowledged=launched,training_state_restored=ack if confirmed else False if ack is not None else None))
            view['ranks_truncated']=total>MAX_RANKS
            if 'execution_leases' in tables:
                at=time.time() if now is None else now
                blocked=db.execute('''SELECT 1 FROM hybrid_staging_ranks r LEFT JOIN execution_leases l ON l.lease_id=r.lease_id
                    WHERE r.stage_id=? AND (l.lease_id IS NULL OR l.revoked=1 OR l.renewal_blocked=1 OR l.expires_at<=?) LIMIT 1''',
                    (stage[0],at)).fetchone() is not None
            if mode=='restart':
                # Restart mode has no checkpoint restore rows. Current RUNNING is
                # the existing launch barrier; never invent per-rank SDK restores.
                view['launched_ranks']=total if job[4] in ('RUNNING','COMPLETED') and stage[1]=='STARTED' else None
    if view['restore_required'] and not confirmed and view['restored_ranks'] is not None:
        view['restored_ranks']=0  # Controller accepts SDK restoration as a whole-group barrier.
    if view['restore_required'] is False:
        # Cold checkpoint-mode retries also use restore rows internally; their
        # confirmation is NOT evidence that any checkpoint state was restored.
        view['checkpoint_ready_ranks']=None;view['restored_ranks']=None
        for rank in view['ranks']:
            rank['checkpoint_ready']=None;rank['training_state_restored']=None
    view['phase']=_phase(view,job[4],None if stage is None else stage[1],blocked,confirmed)
    view['label']=LABELS[view['phase']]
    return view


def log_text(view):
    if view is None:return 'Recovery has not been recorded for this attempt.\n'
    def count(value):return 'not recorded' if value is None else str(value)
    lines=[f"Attempt {view['attempt']}: {view['label']}.",
           f"Input workspaces ready: {count(view['inputs_ready_ranks'])}/{count(view['total_ranks'])}."]
    source=view['source']
    if source['state']=='committed':
        lines.append(f"Selected committed checkpoint: step {source['global_step']}, from attempt {source['from_attempt']}.")
    if view['restore_required']:
        lines.extend([f"Checkpoint copies prepared: {count(view['checkpoint_ready_ranks'])}/{view['total_ranks']}.",
                      f"Training-state restoration confirmed: {count(view['restored_ranks'])}/{view['total_ranks']}."])
    elif view['restore_required'] is False:lines.append('Full restart; checkpoint restoration is not required.')
    else:lines.append('Checkpoint selection has not been confirmed.')
    lines.append('Progress reflects controller records, not a live worker probe. No byte percentage is inferred.')
    return '\n'.join(lines)+'\n'
