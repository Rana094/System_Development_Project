"""Explicit hybrid committed-source recovery; normal bootstrap remains unchanged.

Reuses resource placement, authenticated staging/launch, watchdog fencing, SDK
acknowledgements and retry cleanup. No mutable latest-file adoption or NFS RW.
"""
import json
import secrets
import time

import requests
from hybrid_storage import restore_schema
from hybrid_storage.input_manifests import encoded
from hybrid_storage.checkpoint_publication import LocalCheckpointPublisher
from hybrid_storage.checkpoint_transfer import CheckpointCollector
from hybrid_storage.checkpoint_restore import CheckpointRestorer
from hybrid_storage.staging_lifecycle import require_ready
from staging_dispatch import StagingDispatch


class CheckpointDispatch(StagingDispatch):
    def __init__(self,registry,client,*,publisher,collector,restorer,settings,stage_parallelism=3):
        super().__init__(registry,client,settings=settings,stage_parallelism=stage_parallelism)
        if (not isinstance(publisher,LocalCheckpointPublisher) or publisher.registry is not registry
                or not isinstance(collector,CheckpointCollector) or collector.publisher is not publisher
                or not isinstance(restorer,CheckpointRestorer) or restorer.publisher is not publisher
                or collector.credentials!=client.credentials or restorer.credentials!=client.credentials):
            raise ValueError('Exact trusted local publisher and per-worker authenticated services required')
        self.publisher,self.collector,self.restorer=publisher,collector,restorer

    def authorize_checkpoint(self,db,job_id):
        if not restore_schema.installed(db):raise ValueError('Explicit LOCAL restore migration required')
        if not {'job_id','event_type','node_id','event_time','details_json'} <= {r[1] for r in db.execute('PRAGMA table_info(recovery_events)')}:
            raise ValueError('Existing recovery event schema must be initialized explicitly')

    def _attempt(self,job):
        return self.registry.db.execute('SELECT source_id,source_json,stage_id,confirmed FROM hybrid_restore_attempts WHERE job_id=? AND retry_count=?',
                                        (job['job_id'],job['retry_count'])).fetchone()

    def recovery_source(self,job):
        if json.loads(job['job_config'])['recovery']['mode']!='checkpoint':return None
        with self.registry._tx(write=False):
            binding=self.authorize(self.registry.db,job['job_id']);frozen=self._attempt(job)
        checkpoint=(self.publisher.committed(job['owner'],frozen[0]) if frozen and frozen[0] else
                    None if frozen else self.publisher.head(job['owner'],job['job_id']))
        if checkpoint is None:
            # A missing head must never turn a previously saved job into a cold
            # restart. No adoption of legacy files or incomplete publication rows.
            if job.get('checkpoint_path') or self.registry.db.execute("SELECT 1 FROM hybrid_checkpoint_publications WHERE job_id=? AND phase='COMMITTED'",(job['job_id'],)).fetchone():
                raise PermissionError('Known checkpoint has no committed recovery head')
            source=None
        else:
            row=self.publisher._publication(checkpoint.publication_id)
            meta=json.loads(row['metadata_json']);spec=json.loads(job['job_config'])
            if (checkpoint.job_id!=job['job_id'] or checkpoint.retry_count>=job['retry_count']
                    or checkpoint.binding_sha256!=binding.sha256
                    or meta['manifest_sha256']!=job['storage_manifest_sha256']
                    or (checkpoint.kind=='ddp')!=(spec['execution']['workers']>1)
                    or meta.get('world_size',1)!=spec['execution']['workers']):
                raise PermissionError('Committed recovery source differs from original job/version/rank set')
            source=dict(publication_id=checkpoint.publication_id,sha256=checkpoint.sha256,
                        size_bytes=checkpoint.size_bytes,binding_sha256=checkpoint.binding_sha256,metadata=meta)
        text=None if source is None else encoded(source).decode()
        with self.registry._tx():
            self.authorize(self.registry.db,job['job_id'])
            current=self.registry.db.execute('SELECT status,retry_count FROM jobs WHERE job_id=?',(job['job_id'],)).fetchone()
            if tuple(current)!=('QUEUED',job['retry_count']):raise PermissionError('Recovery attempt changed')
            frozen=self._attempt(job)
            if frozen is None:self.registry.db.execute('INSERT INTO hybrid_restore_attempts(job_id,retry_count,binding_sha256,source_id,source_json) VALUES(?,?,?,?,?)',
                    (job['job_id'],job['retry_count'],binding.sha256,None if source is None else source['publication_id'],text))
            elif frozen[1]!=text:raise PermissionError('Frozen recovery source changed')
        return source

    def prepare_recovery(self,job,stage,workers,leases,source):
        if json.loads(job['job_config'])['recovery']['mode']!='checkpoint':return
        with self.registry._tx():
            self.registry.db.execute('UPDATE hybrid_restore_attempts SET stage_id=? WHERE job_id=? AND retry_count=?',
                                      (stage.stage_id,job['job_id'],job['retry_count']))
            for rank in range(len(workers)):
                self.registry.db.execute('INSERT INTO hybrid_restore_ranks(stage_id,rank) VALUES(?,?)',(stage.stage_id,rank))
        if source is not None:
            for rank,node in enumerate(workers):
                lease=self.fresh_lease(job,node)
                sha=self.restorer.stage(job['owner'],job['job_id'],node,lease,stage.stage_id,source)
                with self.registry._tx():
                    require_ready(self.registry.db,job['job_id'])
                    self.registry.db.execute('UPDATE hybrid_restore_ranks SET receipt_sha256=? WHERE stage_id=? AND rank=?',(sha,stage.stage_id,rank))
            with self.registry._tx():
                require_ready(self.registry.db,job['job_id'])
                details=dict(sha256=source['sha256'],global_step=source['metadata']['global_step'],
                             attempt_token=source['metadata']['attempt_token'],retry_count=job['retry_count'],
                             publication_id=source['publication_id'],world_size=len(workers))
                self.registry.db.execute('INSERT INTO recovery_events(job_id,event_type,node_id,event_time,details_json) VALUES(?,?,?,?,?)',
                    (job['job_id'],'DDP_RESTORE_PREPARED' if len(workers)>1 else 'RESTORE_PREPARED',job['node_id'],time.time(),encoded(details).decode()))

    def recovery_payload(self,job,rank,source):
        if json.loads(job['job_config'])['recovery']['mode']!='checkpoint':return {}
        return dict(checkpoint_source=source,resume_checkpoint=None if source is None else source['metadata'])

    def start(self,job,node,payload,stage,receipt):
        if 'checkpoint_source' in payload:
            rank=(payload.get('ddp') or {}).get('rank',0)
            with self.registry._tx():
                rows=list(self.registry.db.execute('SELECT receipt_sha256 FROM hybrid_restore_ranks WHERE stage_id=?',(stage.stage_id,)))
                if len(rows)!=json.loads(job['job_config'])['execution']['workers'] or (payload['checkpoint_source'] and any(r[0] is None for r in rows)):
                    raise PermissionError('All rank restores must finish before any launch')
                row=self.registry.db.execute('SELECT launch_sent FROM hybrid_restore_ranks WHERE stage_id=? AND rank=?',(stage.stage_id,rank)).fetchone()
                if row is None or row[0]:raise PermissionError('Ambiguous launch must be reconciled, never replayed')
                self.registry.db.execute('UPDATE hybrid_restore_ranks SET launch_sent=1 WHERE stage_id=? AND rank=?',(stage.stage_id,rank))
        result=super().start(job,node,payload,stage,receipt)
        if 'checkpoint_source' in payload:
            with self.registry._tx():self.registry.db.execute('UPDATE hybrid_restore_ranks SET started_at=? WHERE stage_id=? AND rank=?',
                (result['job']['started_at'],stage.stage_id,rank))
        return result

    def group_started(self,job,stage,workers,results,source,context):
        if json.loads(job['job_config'])['recovery']['mode']!='checkpoint':
            return super().group_started(job,stage,workers,results,source,context)
        if source is None:
            self.confirm(job,stage.stage_id,source)
            return super().group_started(job,stage,workers,results,source,context)
        # SDK restoration happens asynchronously in the existing launcher. Keep
        # the reservation and SCHEDULED state; never claim RUNNING on a copy ACK.
        self._confirm_started(job)

    def confirm(self,job,stage_id,source):
        with self.registry._tx():
            self.authorize(self.registry.db,job['job_id'])
            require_ready(self.registry.db,job['job_id'])
            row=self._attempt(job)
            if row is None or row[2]!=stage_id:raise PermissionError('Restore attempt identity changed')
            if row[3]:return
            self.registry.db.execute('UPDATE hybrid_restore_attempts SET confirmed=1 WHERE job_id=? AND retry_count=?',(job['job_id'],job['retry_count']))
            self.registry.db.execute('UPDATE hybrid_restore_ranks SET restored=1 WHERE stage_id=?',(stage_id,))
            if source is not None:
                info=source['metadata'];details=dict(global_step=info['global_step'],sha256=source['sha256'],retry_count=job['retry_count'],publication_id=source['publication_id'],
                    restored_global_step=info['global_step'],checkpoint_sha256=source['sha256'],
                    attempt_token=f"{job['job_id']}:{job['retry_count']}",source_attempt=info['attempt_token'],
                    started_at=self.registry.db.execute('SELECT MIN(started_at) FROM hybrid_restore_ranks WHERE stage_id=?',(stage_id,)).fetchone()[0])
                if info.get('world_size',1)>1:
                    details.update(world_size=info['world_size'],acknowledged_ranks=list(range(info['world_size'])),attempt_token=info['attempt_token'])
                self.registry.db.execute('INSERT INTO recovery_events(job_id,event_type,node_id,event_time,details_json) VALUES(?,?,?,?,?)',
                    (job['job_id'],'DDP_RESUMED' if info.get('world_size',1)>1 else 'RESUMED',job['node_id'],time.time(),encoded(details).decode()))

    def _confirm_started(self,job):
        import job_store as store
        import distributed_dispatch as ddp
        import execution_leases as ownership
        job,stage,members=self.snapshot(job['job_id'])
        row=self._attempt(job)
        if row is None or row[2]!=stage[0]:raise PermissionError('Missing frozen restore attempt')
        source=None if row[1] is None else json.loads(row[1])
        launches=list(self.registry.db.execute('SELECT launch_sent,started_at FROM hybrid_restore_ranks WHERE stage_id=? ORDER BY rank',(stage[0],)))
        if len(launches)!=stage[2] or any(not r[0] or r[1] is None for r in launches):raise PermissionError('Incomplete launch acknowledgement group')
        with self.registry._tx(write=False):
            self.authorize(self.registry.db,job['job_id']);ready=require_ready(self.registry.db,job['job_id'])
        replies=[self.control(stage[0],member,'status') for member in members]
        context=None
        if stage[2]>1:
            context=json.loads(self.registry.db.execute('SELECT context_json FROM distributed_attempts WHERE job_id=? AND retry_count=?',(job['job_id'],job['retry_count'])).fetchone()[0])
            context['resume_checkpoint']=None if source is None else source['metadata']
        proven=[]
        for (rank,node,plan,lease),reply,launch in zip(members,replies,launches):
            observed=reply.get('job') or {}
            if (reply.get('outcome') not in (None,'COMPLETED') or not ownership.matches(job,node['id'],observed)
                    or observed.get('started_at')!=launch[1] or observed.get('status') not in ('RUNNING','COMPLETED')):
                raise PermissionError('Restore process identity/status differs')
            info=observed.get('resume') or {}
            proof=(source is None or (ddp.restore_confirmed(job,dict(rank=rank),context,observed) if context else
                info.get('job_id')==job['job_id'] and info.get('attempt_token')==f"{job['job_id']}:{job['retry_count']}"
                and info.get('pid')==observed.get('pid') and info.get('confirmed_at',0)>=observed['started_at']
                and info.get('checkpoint_sha256')==source['sha256'] and info.get('restored_global_step')==source['metadata']['global_step']
                and all(info.get(key) is True for key in ('model_restored','optimizer_restored','rng_restored'))))
            if not proof and (observed['status']=='COMPLETED' or time.time()-observed['started_at']>180):
                raise PermissionError('Required SDK restore acknowledgement missing')
            proven.append(bool(proof))
        if not all(proven):
            for member in members:
                if time.time()-member[3]['issued_at']>=ownership.settings()['renew_every_seconds']:
                    self.control(stage[0],member,'renew',self.fresh_lease(job,member[1]))
            return 'RESTORE_PENDING'
        self.confirm(job,stage[0],source)
        super().group_started(job,ready,[m[1] for m in members],[dict(job=r['job']) for r in replies],source,context)
        return 'RUNNING'

    def sync(self,job,members):
        head=self.publisher.head(job['owner'],job['job_id'])
        member=members[0]
        return self.collector.sync(job['owner'],job['job_id'],member[1],member[3],publication_id=secrets.token_hex(16),
                                   expected_head=None if head is None else head.publication_id,skip_unchanged=True)

    def require_final(self,db,job_id):
        row=db.execute('SELECT job_config,retry_count FROM jobs WHERE job_id=?',(job_id,)).fetchone()
        if json.loads(row[0])['recovery']['mode']!='checkpoint':return
        self.authorize(db,job_id)
        final=db.execute('''SELECT p.retry_count,p.completed,p.binding_sha256,r.binding_sha256,r.confirmed
            FROM hybrid_checkpoint_heads h JOIN hybrid_checkpoint_publications p USING(publication_id)
            JOIN hybrid_restore_attempts r ON r.job_id=h.job_id AND r.retry_count=?
            WHERE h.job_id=? AND p.phase='COMMITTED' ''',(row[1],job_id)).fetchone()
        if final is None or final[0]!=row[1] or not final[1] or final[2]!=final[3] or not final[4]:
            raise PermissionError('Current attempt fully committed final checkpoint required')

    def completion_ready(self,job,stage,members,replies):
        if json.loads(job['job_config'])['recovery']['mode']!='checkpoint':return True
        try:
            self.sync(job,members)
            with self.registry._tx(write=False):self.require_final(self.registry.db,job['job_id'])
            return True
        except (OSError,ValueError,KeyError,requests.RequestException):
            with self.registry._tx():
                self.registry.db.execute('UPDATE hybrid_restore_attempts SET final_failures=final_failures+1 WHERE job_id=? AND retry_count=?',(job['job_id'],job['retry_count']))
                count=self.registry.db.execute('SELECT final_failures FROM hybrid_restore_attempts WHERE job_id=? AND retry_count=?',(job['job_id'],job['retry_count'])).fetchone()[0]
            if count>=3:raise ValueError('Final checkpoint could not be committed; retry through fenced cleanup')
            return False

    def reconcile(self,job):
        snapshot=self.snapshot(job['job_id'])
        if snapshot is None:return 'UNKNOWN'
        job,stage,members=snapshot
        checkpoint=json.loads(job['job_config'])['recovery']['mode']=='checkpoint'
        if checkpoint and stage and stage[1]=='READY' and job['status']=='SCHEDULED':
            try:return self._confirm_started(job)
            except (OSError,ValueError,KeyError,TypeError,requests.RequestException):
                self.abort_intent(job,stage,'Restore/launch authority uncertain')
                return self.cleanup(job,stage,members)
        if checkpoint and stage and job['status']=='RUNNING' and stage[1]=='STARTED':
            try:self.sync(job,members)
            except (OSError,ValueError,KeyError,requests.RequestException):pass
        return super().reconcile(job)
