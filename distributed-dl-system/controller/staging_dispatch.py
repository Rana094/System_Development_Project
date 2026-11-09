"""Opt-in staging coordinator using existing selection/reservation/lease APIs.

Explicit trusted bootstrap only; not installed by config or portal startup.
Failures retain the reservation/lease for reconciliation, never blind requeue.
"""

from copy import copy
import json
from pathlib import Path

from hybrid_storage.config import StorageConfig, StorageConfigError
from hybrid_storage.registry import StorageRegistry
from hybrid_storage import registry_schema, staging_schema, cleanup_schema
from hybrid_storage.staging_lifecycle import StagingLifecycle, require_ready
from hybrid_storage.staging_protocol import STAGE_PATH, START_PATH, StagingClient, receipt_document, receipt_hash
from hybrid_storage.workspaces import AttemptIdentity, WorkspacePlan
from staging_recovery import StagingRecovery


class StagingDispatch(StagingRecovery):
    def __init__(self, registry, client, *, settings=StorageConfig()):
        if not isinstance(registry, StorageRegistry) or not isinstance(client, StagingClient):
            raise TypeError('Expected explicit registry and authenticated control client')
        if settings != StorageConfig('nfs', True, True, 'resource'):
            raise StorageConfigError('Dispatch requires explicit NFS/cache/staging/resource settings')
        self.registry, self.client = registry, client
        self.lifecycle = StagingLifecycle(registry, settings=settings)
        self._launching = set()
        self._finishing=set();self._owned_stages=set()

    def _reader(self, db):
        """Reuse the registry's read-only validators in the caller's locked snapshot.

        No constructor/migration/transaction or mutation is performed. Keeping a
        separate shallow view avoids changing the dedicated registry connection.
        """
        staging_schema.require_local_read(db)
        if not staging_schema._installed(db) or not registry_schema._installed(db):
            raise ValueError('Explicit immutable-input and staging migrations required')
        if not cleanup_schema.installed(db):raise ValueError('Explicit cleanup migration required')
        def main(connection):
            return next(row[2] for row in connection.execute('PRAGMA database_list') if row[1]=='main')
        if Path(main(db)) != Path(main(self.registry.db)):
            raise ValueError('Staging runtime belongs to a different database')
        view = copy(self.registry)
        view.db = db
        return view

    def authorize(self, db, job_id):
        view = self._reader(db)
        row = db.execute('SELECT owner,job_type,cancel_requested_at,job_config FROM jobs WHERE job_id=?',(job_id,)).fetchone()
        if row is None or row[1] != 'custom' or row[2] is not None:
            raise PermissionError('Staging needs an uncancelled owned custom job')
        binding = view._job_inputs(row[0], job_id)
        if binding is None:
            raise PermissionError('Explicit immutable job input registration required')
        if json.loads(row[3])['recovery']['mode'] != 'restart':
            self.authorize_checkpoint(db,job_id)
        return binding

    def authorize_checkpoint(self, db, job_id):
        raise ValueError('NFS checkpoint jobs require explicit trusted recovery coordinator')

    def recovery_source(self, job):return None

    def prepare_recovery(self, job, stage, workers, leases, source):pass

    def recovery_payload(self, job, rank, source):return {}

    def group_started(self, job, stage, workers, results, source, context):
        import job_store as store
        self._launching.add(stage.stage_id)
        try:store.mark_running(job['job_id'],min(r['job']['started_at'] for r in results),storage_runtime=self)
        finally:self._launching.discard(stage.stage_id)
        if context is not None:
            import distributed_dispatch as ddp
            ddp.phase(job,'RUNNING')

    def require_final(self, db, job_id):pass

    def plans(self, job):
        with self.registry._tx(write=False):
            binding = self.authorize(self.registry.db, job['job_id'])
            source = self.registry._source(binding.owner, binding.source_sha256)
            data = (None if binding.dataset_id is None else
                    self.registry._dataset(binding.dataset_id, binding.dataset_version, binding.owner))
            return tuple(WorkspacePlan(AttemptIdentity(job['job_id'],f"{job['job_id']}:{job['retry_count']}",rank),
                                       binding.sha256, source, data)
                         for rank in range(json.loads(job['job_config'])['execution']['workers']))

    def stage_group(self, job, workers, leases):
        """All ranks stage their actual whole-file plans before ANY launch request."""
        plans = self.plans(job)
        if len(plans) != len(workers) or len(workers) != len(leases):
            raise ValueError('Full reserved rank group and leases required')
        stage = self.lifecycle.begin(job['owner'],job['job_id'],plans)
        self._owned_stages.add(stage.stage_id)
        replies = []
        for rank,(plan,node,lease) in enumerate(zip(plans,workers,leases)):
            fresh = self.fresh_lease(job,node)
            if any(fresh[name] != lease[name] for name in ('node_id','lease_id','generation')):
                raise PermissionError('Stage lease identity changed')
            lease = fresh
            response = self.client.call(node,STAGE_PATH,dict(stage_id=stage.stage_id,plan=plan.document(),lease=lease))
            expected = receipt_document(stage.stage_id,plan,lease)
            if response.get('receipt') != expected or response.get('receipt_sha256') != receipt_hash(expected):
                raise PermissionError('Authenticated workspace receipt differs from exact placement')
            from hybrid_storage.staging_metrics import validated
            required=sum({item.sha256:item.size_bytes for item in plan.inputs()}.values())
            metrics=validated(response.get('metrics'),required_bytes=required)
            self.lifecycle.record_ready(job['owner'],stage,rank,plan_sha256=plan.sha256,
                                        receipt_sha256=receipt_hash(expected),metrics=metrics)
            replies.append(response)
        with self.registry._tx(write=False):
            require_ready(self.registry.db,job['job_id'])
        return stage,replies

    def start(self, job, node, payload, stage, receipt):
        # Recheck authority for the COMPLETE group immediately before each launch.
        with self.registry._tx(write=False):
            if require_ready(self.registry.db,job['job_id']) != stage:
                raise PermissionError('Stage attempt changed before launch')
            binding = self.registry._job_inputs(job['owner'],job['job_id'])
        result = self.client.call(node,START_PATH,{**payload, 'staging_id':stage.stage_id,
                                                  'staging_receipt':receipt['receipt_sha256'],
                                                  'execution_spec_sha256':binding.execution_spec_sha256})
        import execution_leases as ownership
        import math
        observed = result['job']
        if (observed.get('job_id') != job['job_id'] or observed.get('status') != 'RUNNING'
                or type(observed.get('started_at')) not in (int,float) or not math.isfinite(observed['started_at'])
                or not ownership.matches(job,node['id'],observed)):
            raise PermissionError('Authenticated start acknowledgment has a stale execution fence')
        return result

    def fresh_lease(self, job, node, *, running=False):
        """Persist a bounded possibly-delivered deadline before the signed start.

        Preserve the existing lease ID/generation; refuse revival after expiry,
        uncertainty, cancellation or controller reboot. Long staging fails closed
        at the original deadline; renewal/reconnect reconciliation belongs to 7.3.
        """
        import time
        import execution_leases as ownership
        from hybrid_storage.staging_lifecycle import _current, _fence, _boot, _clock
        with self.registry._tx():
            db = self.registry.db
            if running:
                from hybrid_storage.staging_lifecycle import require_running
                require_running(db,job['job_id'])
                current=job
            else:current = _current(db,job['job_id'])
            _,lease = _fence(db,current,node['id'],wall=time.time(),boot=_boot(),clock=_clock())
            now = time.time(); ttl = ownership.settings()['duration_seconds']
            db.execute('UPDATE execution_leases SET issued_at=?,expires_at=?,expires_clock=? WHERE lease_id=?',
                       (now,now+ttl,_clock()+ttl,lease['lease_id']))
        return ownership.descriptor(ownership.get(job,node['id']))

    def dispatch(self, job, excluded):
        """Existing resource eligibility, whole-group reservation and active quota."""
        import job_store as store
        import execution_leases as ownership
        import dispatcher
        import distributed_dispatch as ddp
        job = store.get_job(job['job_id'])
        if job is None or job['status'] != 'QUEUED':
            return False
        if not ownership.settings()['enabled']:
            raise ValueError('Hybrid execution requires existing watchdog leases')
        # Validate ACL, immutable binding and every plan before reserving a slot.
        self.plans(job)
        spec = json.loads(job['job_config'])
        source=self.recovery_source(job)
        if spec['execution']['workers'] > 1:
            workers = (ddp.select_group(job,excluded,source['metadata'],require_ownership=True) if source else
                       ddp.select_group(job,excluded,require_ownership=True))
            if not workers:return False
            import secrets
            context = dict(world_size=len(workers),master_addr=workers[0]['ip'],
                           master_port=20000+secrets.randbelow(30000),leases_enabled=True)
        else:
            allowed = [r['node_id'] for r in store.get_job_environments(job['job_id'])
                       if not source or r['record']['compatibility_key']==source['metadata']['compatibility_key']]
            selected = dispatcher.select_best_node(allowed_nodes=allowed,requirements=spec['resources_per_worker'],
                reserved_nodes={r['node_id'] for r in store.get_reservations()},excluded_nodes=excluded)
            if not selected or not ownership.supported(selected.get('resources')):return False
            workers, context = [selected['node']], None
        store.mark_scheduled(job['job_id'],workers[0]['id'],workers[0]['ip'],spec['resources_per_worker'],
                             workers,context,storage_runtime=self)
        job = store.get_job(job['job_id'])
        try:
            leases = [ownership.ensure(job,node['id']) for node in workers]
            stage,replies = self.stage_group(job,workers,leases)
            self.prepare_recovery(job,stage,workers,leases,source)
            started = []
            for rank,(node,receipt,lease) in enumerate(zip(workers,replies,leases)):
                payload = (dispatcher.build_start_payload(job) if context is None else
                           ddp.payload(job,store.get_ranks(job['job_id'],job['retry_count'])[rank],context))
                payload.update(attempt_token=f"{job['job_id']}:{job['retry_count']}",lease=self.fresh_lease(job,node))
                payload.update(self.recovery_payload(job,rank,source))
                result = self.start(job,node,payload,stage,receipt)
                started.append(result)
                if context is not None:ddp.rank_status(job,rank,'RUNNING',result['job']['started_at'])
            self.group_started(job,stage,workers,started,source,context)
            return True
        except Exception:
            # Remote copy/start may have happened. Preserve placement, block
            # renewal and let existing expiry fence execution; 7.3 reconciles.
            ownership.block(job)
            raise
