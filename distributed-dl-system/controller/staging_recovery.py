"""Exact-attempt reconciliation; no unsigned control or eager slot reuse."""
import json
import requests
import time

from hybrid_storage import cleanup_schema
from hybrid_storage.staging_lifecycle import _fence,_boot,_clock,_reservation,require_running
from hybrid_storage.staging_protocol import CONTROL_PATH
from hybrid_storage.workspaces import AttemptIdentity,WorkspacePlan


class StagingRecovery:
    def interrupted_before_begin(self,job):
        """Fence the pre-RPC gap under the same lock used by begin/ensure.

        Never rewrite a previous attempt's history when no current history row
        exists yet. Recheck for concurrently committed intent/leases before slot
        release; BEGIN is always persisted before any staging RPC in this API.
        """
        import execution_leases as ownership
        with self.registry._tx():
            db=self.registry.db
            latest=db.execute('SELECT retry_count,status,max_retries FROM jobs WHERE job_id=?',(job['job_id'],)).fetchone()
            if latest[0]!=job['retry_count'] or latest[1] not in ('SCHEDULED','RUNNING','CANCELLING'):return 'OLD_ATTEMPT'
            if db.execute('SELECT 1 FROM hybrid_staging_attempts WHERE job_id=? AND retry_count=?',(job['job_id'],job['retry_count'])).fetchone():return 'PENDING'
            rows=list(db.execute('SELECT node_id FROM execution_leases WHERE job_id=? AND retry_count=?',(job['job_id'],job['retry_count'])))
            db.execute('UPDATE execution_leases SET renewal_blocked=1 WHERE job_id=? AND retry_count=?',(job['job_id'],job['retry_count']))
            if any(not ownership.safe_to_replace(job,row[0]) for row in rows):return 'PENDING'
            cancelled=latest[1]=='CANCELLING';retry=latest[0] if cancelled else latest[0]+1
            state='CANCELLED' if cancelled else 'QUEUED' if retry<=latest[2] else 'FAILED'
            now=time.time()
            db.execute('UPDATE jobs SET status=?,retry_count=?,node_id=NULL,node_ip=NULL,scheduled_at=NULL,started_at=NULL,finished_at=?,updated_at=?,last_error=? WHERE job_id=?',
                       (state,retry,None if state=='QUEUED' else now,now,'Controller interrupted before staging intent',job['job_id']))
            db.execute("UPDATE job_attempts SET status=?,finished_at=?,error=? WHERE job_id=? AND attempt_number=? AND status='SCHEDULED'",
                       ('CANCELLED' if cancelled else 'FAILED',now,'Interrupted before staging intent',job['job_id'],latest[0]+1))
            db.execute('UPDATE execution_leases SET revoked=1 WHERE job_id=? AND retry_count=?',(job['job_id'],job['retry_count']))
            db.execute('DELETE FROM resource_reservations WHERE job_id=?',(job['job_id'],))
            db.execute('UPDATE distributed_attempts SET phase=? WHERE job_id=? AND retry_count=?',('CANCELLED' if cancelled else 'FAILED',job['job_id'],job['retry_count']))
            db.execute("UPDATE job_ranks SET status='STOPPED' WHERE job_id=? AND retry_count=?",(job['job_id'],job['retry_count']))
            return state

    def snapshot(self,job_id):
        with self.registry._tx(write=False):
            db=self.registry.db
            if not cleanup_schema.installed(db):raise ValueError('Explicit cleanup migration required')
            job=db.execute('SELECT * FROM jobs WHERE job_id=?',(job_id,)).fetchone()
            if job is None:return None
            job=dict(zip((c[1] for c in db.execute('PRAGMA table_info(jobs)')),job))
            stage=db.execute('SELECT stage_id,phase,world_size,attempt_id FROM hybrid_staging_attempts WHERE job_id=? AND retry_count=?',
                             (job_id,job['retry_count'])).fetchone()
            if stage is None:return job,None,[]
            return job,tuple(stage),self.members(stage[0])

    def members(self,stage_id):
        """Cleanup uses the pinned manifest even after ACL revocation; never launch."""
        db=self.registry.db
        stage=db.execute('SELECT job_id,retry_count,input_binding_sha256 FROM hybrid_staging_attempts WHERE stage_id=?',(stage_id,)).fetchone()
        binding=self.registry._binding(stage[0])
        if binding is None or binding.sha256!=stage[2]:raise ValueError('Cleanup binding changed')
        source=self.registry._load_manifest(binding.source_sha256,'source-tree')
        data=None if binding.dataset_id is None else self.registry._load_manifest(binding.dataset_sha256,'dataset-manifest')
        from scheduler import load_nodes
        nodes={n['id']:n for n in load_nodes()}
        result=[]
        for row in db.execute('SELECT rank,worker_id,plan_sha256,lease_id,generation FROM hybrid_staging_ranks WHERE stage_id=? ORDER BY rank',(stage_id,)):
            rank,node_id,sha,lease_id,generation=row
            plan=WorkspacePlan(AttemptIdentity(stage[0],f'{stage[0]}:{stage[1]}',rank),stage[2],source,data)
            lease=db.execute('SELECT * FROM execution_leases WHERE lease_id=?',(lease_id,)).fetchone()
            if lease is None or plan.sha256!=sha:raise ValueError('Cleanup rank identity changed')
            lease=dict(zip((c[1] for c in db.execute('PRAGMA table_info(execution_leases)')),lease))
            if lease['node_id']!=node_id or lease['generation']!=generation:raise ValueError('Cleanup lease identity changed')
            result.append((rank,nodes.get(node_id),plan,lease))
        return result

    def control(self,stage_id,member,action,lease=None):
        rank,node,plan,old=member
        if node is None:raise ValueError('Worker address no longer configured')
        import execution_leases as ownership
        reply=self.client.call(node,CONTROL_PATH,dict(action=action,stage_id=stage_id,plan=plan.document(),
                                                     lease=ownership.descriptor(lease or old)))
        if (reply.get('stage_id')!=stage_id or reply.get('plan_sha256')!=plan.sha256
                or reply.get('lease_id')!=old['lease_id'] or reply.get('generation')!=old['generation']):
            raise ValueError('Lifecycle acknowledgment identity differs')
        return reply

    def abort_intent(self,job,stage,reason):
        with self.registry._tx():
            db=self.registry.db
            current=db.execute('SELECT retry_count,status FROM jobs WHERE job_id=?',(job['job_id'],)).fetchone()
            if current[0]!=job['retry_count']:raise ValueError('Abort attempt advanced')
            db.execute('UPDATE execution_leases SET renewal_blocked=1 WHERE job_id=? AND retry_count=?',(job['job_id'],job['retry_count']))
            db.execute("UPDATE hybrid_staging_attempts SET phase='ABORTING',updated_at_ns=MAX(updated_at_ns,?) WHERE stage_id=? AND phase IN ('STAGING','READY','STARTED')",(time.time_ns(),stage[0]))
            db.execute('UPDATE jobs SET last_error=? WHERE job_id=?',(reason,job['job_id']))
            db.execute("UPDATE distributed_attempts SET phase='STOPPING',error=? WHERE job_id=? AND retry_count=?",(reason,job['job_id'],job['retry_count']))
            self.ensure_cleanup(stage[0])

    def ensure_cleanup(self,stage_id):
        self.registry.db.execute("INSERT OR IGNORE INTO hybrid_cleanup_ranks SELECT stage_id,rank,'PENDING',? FROM hybrid_staging_ranks WHERE stage_id=?",(time.time_ns(),stage_id))

    def cleanup(self,job,stage,members):
        import execution_leases as ownership
        for member in members:
            rank,node,plan,lease=member
            proof='PENDING'
            try:
                reply=self.control(stage[0],member,'cancel')
                if reply.get('phase')=='TERMINAL' and reply.get('pins_released') is True:proof='QUIESCENT'
            except (OSError,ValueError,KeyError, requests.RequestException):pass
            if proof!='QUIESCENT' and ownership.safe_to_replace({**job,'retry_count':lease['retry_count']},lease['node_id']):
                proof='LEASE_FENCED'
            with self.registry._tx():
                self.ensure_cleanup(stage[0])
                self.registry.db.execute("UPDATE hybrid_cleanup_ranks SET phase=?,updated_at_ns=MAX(updated_at_ns,?) WHERE stage_id=? AND rank=? AND phase!='QUIESCENT' AND NOT(phase='LEASE_FENCED' AND ?='PENDING')",
                                         (proof,time.time_ns(),stage[0],rank,proof))
        return self.finalize_abort(job,stage)

    def finalize_abort(self,job,stage):
        with self.registry._tx():
            db=self.registry.db
            rows=db.execute('SELECT phase FROM hybrid_cleanup_ranks WHERE stage_id=?',(stage[0],)).fetchall()
            if len(rows)!=stage[2] or any(r[0]=='PENDING' for r in rows):return 'PENDING'
            latest=db.execute('SELECT retry_count,status,max_retries FROM jobs WHERE job_id=?',(job['job_id'],)).fetchone()
            if latest[0]!=job['retry_count'] or latest[1] not in ('SCHEDULED','RUNNING','CANCELLING'):return 'OLD_ATTEMPT'
            # Do not release a reservation belonging to an unexpected incarnation.
            from hybrid_storage.staging_lifecycle import _reservation
            for row in db.execute('SELECT worker_id,reservation_sha256 FROM hybrid_staging_ranks WHERE stage_id=?',(stage[0],)):
                reservation=db.execute('SELECT node_id,job_id,resources_json,created_at FROM resource_reservations WHERE node_id=?',(row[0],)).fetchone()
                if reservation is None or _reservation(dict(zip(('node_id','job_id','resources_json','created_at'),reservation)))!=row[1]:
                    raise ValueError('Abort reservation changed; retained for review')
            cancelled=latest[1]=='CANCELLING';retry=latest[0] if cancelled else latest[0]+1
            state='CANCELLED' if cancelled else 'QUEUED' if retry<=latest[2] else 'FAILED'
            now=time.time()
            db.execute("UPDATE hybrid_staging_attempts SET phase='TERMINAL',updated_at_ns=MAX(updated_at_ns,?) WHERE stage_id=? AND phase='ABORTING'",(time.time_ns(),stage[0]))
            db.execute('UPDATE jobs SET status=?,retry_count=?,node_id=NULL,node_ip=NULL,scheduled_at=NULL,started_at=NULL,finished_at=?,updated_at=? WHERE job_id=?',
                       (state,retry,None if state=='QUEUED' else now,now,job['job_id']))
            db.execute('UPDATE job_attempts SET status=?,finished_at=?,error=? WHERE attempt_id=?',
                       ('CANCELLED' if cancelled else 'FAILED',now,'Staged attempt cancelled or fenced',stage[3]))
            db.execute('UPDATE execution_leases SET revoked=1,renewal_blocked=1 WHERE job_id=? AND retry_count=?',(job['job_id'],job['retry_count']))
            db.execute('DELETE FROM resource_reservations WHERE job_id=?',(job['job_id'],))
            db.execute("UPDATE job_ranks SET status='STOPPED' WHERE job_id=? AND retry_count=?",(job['job_id'],job['retry_count']))
            db.execute('UPDATE distributed_attempts SET phase=? WHERE job_id=? AND retry_count=?',('CANCELLED' if cancelled else 'FAILED',job['job_id'],job['retry_count']))
            return state

    def reconcile(self,job):
        import job_store as store
        import execution_leases as ownership
        snapshot=self.snapshot(job['job_id'])
        if snapshot is None:return 'UNKNOWN'
        job,stage,members=snapshot
        if stage is None:
            return self.interrupted_before_begin(job)
        if stage[1]=='TERMINAL':return 'TERMINAL'
        reason=None
        if job['status']=='CANCELLING':reason='Cancelled by owner'
        elif stage[1] in ('STAGING','READY') and stage[0] not in self._owned_stages:reason='Controller restarted during staging'
        elif stage[1]=='ABORTING':reason='Reconciling aborted staged attempt'
        else:
            try:
                with self.registry._tx(write=False):
                    self.authorize(self.registry.db,job['job_id'])
                    if stage[1]=='STARTED':require_running(self.registry.db,job['job_id'])
                    else:
                        for _,node,_,lease in members:
                            _fence(self.registry.db,job,lease['node_id'],wall=time.time(),boot=_boot(),clock=_clock())
            except (OSError,ValueError):reason='Staging authority expired or revoked'
        if reason:
            self.abort_intent(job,stage,reason)
            return self.cleanup(job,stage,members)
        if stage[1] in ('STAGING','READY'):
            try:
                replies=[self.control(stage[0],member,'status') for member in members]
                if any(r.get('phase') not in ('STAGING','READY') for r in replies):raise ValueError('Worker forgot staging')
                return 'STAGING'
            except (OSError,ValueError,KeyError,requests.RequestException):
                self.abort_intent(job,stage,'Worker lost staging authority')
                return self.cleanup(job,stage,members)
        try:
            replies=[self.control(stage[0],member,'status') for member in members]
            if any(r.get('outcome') not in (None,'COMPLETED') for r in replies):raise ValueError('Worker attempt failed')
            if all(r.get('outcome')=='COMPLETED' and r.get('pins_released') is True for r in replies):
                with self.registry._tx():
                    self.ensure_cleanup(stage[0])
                    self.registry.db.execute("UPDATE hybrid_cleanup_ranks SET phase='QUIESCENT',updated_at_ns=MAX(updated_at_ns,?) WHERE stage_id=?",(time.time_ns(),stage[0]))
                from job_artifacts import collect
                for rank,node,_,_ in members:
                    collect({**job,'node_id':node['id'],'node_ip':node['ip']},rank if len(members)>1 else None)
                self._finishing.add(stage[0])
                try:store.mark_completed(job['job_id'],storage_runtime=self)
                finally:self._finishing.discard(stage[0])
                if len(members)>1:
                    import distributed_dispatch as ddp
                    ddp.phase(job,'COMPLETED')
                return 'COMPLETED'
            if any(r.get('outcome')=='COMPLETED' for r in replies):return 'QUIESCENCE_PENDING'
            for member in members:
                if time.time()-member[3]['issued_at']>=ownership.settings()['renew_every_seconds']:
                    node=member[1];lease=self.fresh_lease(job,node,running=True)
                    self.control(stage[0],member,'renew',lease)
            return 'RUNNING'
        except (OSError,ValueError,KeyError,requests.RequestException):
            self.abort_intent(job,stage,'Worker contact/result/renewal was uncertain')
            return self.cleanup(job,stage,members)

    def tick(self):
        import job_store as store
        for state in ('SCHEDULED','RUNNING','CANCELLING'):
            for job in store.get_jobs_by_status(state):
                if store.get_storage_binding(job['job_id'])['backend']=='nfs':self.reconcile(job)
        # Lease-fenced workers may return after a replacement already runs.
        with self.registry._tx(write=False):
            ids=[r[0] for r in self.registry.db.execute("SELECT DISTINCT stage_id FROM hybrid_cleanup_ranks WHERE phase='LEASE_FENCED'")]
        for stage_id in ids:
            with self.registry._tx(write=False):members=self.members(stage_id)
            for member in members:
                try:
                    reply=self.control(stage_id,member,'cancel')
                    if reply.get('phase')=='TERMINAL' and reply.get('pins_released') is True:
                        with self.registry._tx():self.registry.db.execute("UPDATE hybrid_cleanup_ranks SET phase='QUIESCENT',updated_at_ns=? WHERE stage_id=? AND rank=?",(time.time_ns(),stage_id,member[0]))
                except (OSError,ValueError,KeyError,requests.RequestException):pass
