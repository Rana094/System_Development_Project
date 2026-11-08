"""Explicit trusted staging metadata/barrier; no staging RPC, dispatch or launch.

jobs.status stays SCHEDULED while side phase is STAGING. A receipt is a digest
recorded only by a trusted coordinator AFTER authenticated workspace verification
(7.2); it is not itself a cryptographic proof or permission to run student code.
"""

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import secrets
import time

from . import registry_schema as base
from . import staging_schema as schema
from .config import StorageConfig, StorageConfigError
from .dataset_manifests import principal
from .input_manifests import encoded, job_identity
from .inputs import object_key
from .registry import StorageRegistry
from .workspaces import WorkspacePlan


def _boot():return Path('/proc/sys/kernel/random/boot_id').read_text().strip()
def _clock():return time.clock_gettime(time.CLOCK_BOOTTIME)


def _reservation(row):
    return hashlib.sha256(encoded({key:row[key] for key in ('node_id','job_id','resources_json','created_at')})).hexdigest()


def _current(db, job_id):
    row=db.execute('SELECT * FROM jobs WHERE job_id=?',(job_id,)).fetchone()
    if row is None:raise base.RegistryError('Unknown staging job')
    job=dict(zip((c[1] for c in db.execute('PRAGMA table_info(jobs)')),row))
    if job['status']!='SCHEDULED' or job['cancel_requested_at'] is not None:
        raise base.RegistryError('Staging no longer owns a scheduled uncancelled job')
    return job


def _placements(db, job, world_size):
    ranks=db.execute('SELECT rank,node_id FROM job_ranks WHERE job_id=? AND retry_count=? ORDER BY rank',
                     (job['job_id'],job['retry_count'])).fetchall()
    if world_size==1 and not ranks:return [(0,job['node_id'])]
    if [row[0] for row in ranks]!=list(range(world_size)) or len({row[1] for row in ranks})!=world_size:
        raise base.RegistryError('Exact reserved rank group missing')
    group=db.execute('SELECT phase FROM distributed_attempts WHERE job_id=? AND retry_count=?',
                     (job['job_id'],job['retry_count'])).fetchone()
    if group is None or group[0]!='STARTING':raise base.RegistryError('DDP group no longer starting')
    return [tuple(row) for row in ranks]


def _fence(db, job, node, *, wall, boot, clock):
    reservation=db.execute('SELECT node_id,job_id,resources_json,created_at FROM resource_reservations WHERE node_id=?',
                           (node,)).fetchone()
    if reservation is None or reservation[1]!=job['job_id']:
        raise base.RegistryError('Staging reservation lost')
    lease=db.execute('SELECT * FROM execution_leases WHERE job_id=? AND retry_count=? AND node_id=?',
                     (job['job_id'],job['retry_count'],node)).fetchone()
    if lease is None:raise base.RegistryError('Staging needs an existing execution lease')
    lease=dict(zip((c[1] for c in db.execute('PRAGMA table_info(execution_leases)')),lease))
    if (lease['attempt_token']!=f"{job['job_id']}:{job['retry_count']}" or lease['revoked'] or lease['renewal_blocked']
            or lease['controller_boot_id']!=boot or not lease['issued_at']<=wall<lease['expires_at']
            or clock>=lease['expires_clock']):
        raise base.RegistryError('Staging lease expired, uncertain or fenced')
    return _reservation(dict(zip(('node_id','job_id','resources_json','created_at'),reservation))),lease


@dataclass(frozen=True)
class StagingAttempt:
    stage_id: str
    job_id: str
    retry_count: int
    attempt_id: int
    input_binding_sha256: str


def require_ready(db, job_id, *, wall=None, boot=None, clock=None):
    """Read barrier inside the caller's existing transaction, before RUNNING.

    Does not lift the separate NFS execution gate. No connection/migration and no
    worker I/O. For job_store's NORMAL connection this only reads staging state.
    """
    schema.require_local_read(db)
    if not schema._installed(db):raise base.RegistryError('Explicit staging migration/attempt required')
    job=_current(db,job_id)
    row=db.execute('''SELECT stage_id,attempt_id,input_binding_sha256,world_size,owner,phase
        FROM hybrid_staging_attempts WHERE job_id=? AND retry_count=?''',(job_id,job['retry_count'])).fetchone()
    if row is None or row[5]!='READY' or row[4]!=job['owner']:
        raise base.RegistryError('All inputs/ranks must complete trusted staging before RUNNING')
    binding=db.execute('''SELECT binding_sha256,owner,legacy_manifest_sha256,execution_spec_sha256,
        source_sha256,dataset_id FROM storage_job_inputs WHERE job_id=?''',(job_id,)).fetchone()
    if (binding is None or binding[0]!=row[2] or binding[1]!=job['owner'] or binding[2]!=job['storage_manifest_sha256']
            or binding[3]!=hashlib.sha256(encoded(json.loads(job['job_config']))).hexdigest()):
        raise base.RegistryError('Committed staging input identity drifted')
    if not db.execute('SELECT 1 FROM storage_manifest_owners WHERE owner=? AND manifest_sha256=?',
                      (job['owner'],binding[4])).fetchone():raise PermissionError('Source access revoked')
    if binding[5] is not None and not db.execute('''SELECT 1 FROM storage_datasets d WHERE dataset_id=?
        AND (owner=? OR EXISTS(SELECT 1 FROM storage_dataset_readers r WHERE r.dataset_id=d.dataset_id AND r.reader=?))''',
        (binding[5],job['owner'],job['owner'])).fetchone():raise PermissionError('Dataset access revoked')
    history=db.execute('SELECT job_id,attempt_number,status FROM job_attempts WHERE attempt_id=?',(row[1],)).fetchone()
    if history is None or tuple(history)!=(job_id,job['retry_count']+1,'SCHEDULED'):
        raise base.RegistryError('Staging history attempt changed')
    placements=_placements(db,job,row[3])
    stages=db.execute('''SELECT rank,worker_id,reservation_sha256,lease_id,generation,phase,receipt_sha256
        FROM hybrid_staging_ranks WHERE stage_id=? ORDER BY rank''',(row[0],)).fetchall()
    if [(r[0],r[1]) for r in stages]!=placements or any(r[5]!='READY' or r[6] is None for r in stages):
        raise base.RegistryError('Staging group is incomplete')
    wall=time.time() if wall is None else wall;boot=_boot() if boot is None else boot;clock=_clock() if clock is None else clock
    for stage in stages:
        reservation,lease=_fence(db,job,stage[1],wall=wall,boot=boot,clock=clock)
        if (reservation,lease['lease_id'],lease['generation'])!=tuple(stage[2:5]):
            raise base.RegistryError('Staging reservation/lease identity changed')
    return StagingAttempt(row[0],job_id,job['retry_count'],row[1],row[2])


class StagingLifecycle:
    def __init__(self, registry, *, settings=StorageConfig()):
        if not isinstance(registry,StorageRegistry) or not isinstance(settings,StorageConfig):
            raise TypeError('Expected trusted registry and typed storage settings')
        self.registry,self.settings=registry,settings

    def _enabled(self):
        if not (self.settings.storage_backend=='nfs' and self.settings.storage_aware_staging and self.settings.worker_cache_enabled):
            raise StorageConfigError('Staging metadata requires explicit hybrid component opt-in')

    def begin(self, actor, job_id, plans):
        """Record an already reserved/leased exact group; never allocate or launch.

        NFS reservation is still unreachable through normal dispatch in 7.1.
        Tests establish that prerequisite synthetically on disposable DBs.
        """
        self._enabled();principal(actor);job_identity(job_id)
        if type(plans) is not tuple or not plans or len(plans)>65536 or any(not isinstance(p,WorkspacePlan) for p in plans):
            raise ValueError('Expected a bounded tuple of validated whole-rank workspace plans')
        db=self.registry.db
        with self.registry._tx():
            if not schema._installed(db):raise base.RegistryError('Explicit staging migration required')
            job=_current(db,job_id)
            if not db.execute("SELECT 1 FROM job_storage_bindings WHERE job_id=? AND backend='nfs'",(job_id,)).fetchone():
                raise base.RegistryError('Legacy jobs are never implicitly converted to hybrid staging')
            binding=self.registry._job_inputs(actor,job_id)
            if binding is None:raise base.RegistryError('Committed immutable job inputs required')
            source=self.registry._source(actor,binding.source_sha256)
            data=None if binding.dataset_id is None else self.registry._dataset(binding.dataset_id,binding.dataset_version,actor)
            world=json.loads(job['job_config'])['execution']['workers']
            placements=_placements(db,job,world)
            if len(plans)!=world or [p.identity.rank for p in plans]!=list(range(world)):
                raise ValueError('All exact ranks must be declared together')
            for plan in plans:
                if (plan.identity.job_id!=job_id or plan.identity.attempt_token!=f'{job_id}:{job["retry_count"]}'
                        or plan.input_manifest_sha256!=binding.sha256 or plan.source!=source or plan.dataset!=data):
                    raise base.RegistryError('Workspace plan differs from committed exact job inputs')
            history=db.execute('''SELECT attempt_id FROM job_attempts WHERE job_id=? AND attempt_number=? AND status='SCHEDULED' ''',
                               (job_id,job['retry_count']+1)).fetchall()
            if len(history)!=1:raise base.RegistryError('One existing scheduled history attempt required')
            fences=[_fence(db,job,node,wall=time.time(),boot=_boot(),clock=_clock()) for _,node in placements]
            existing=db.execute('SELECT stage_id,attempt_id,input_binding_sha256,owner,phase FROM hybrid_staging_attempts WHERE job_id=? AND retry_count=?',
                                (job_id,job['retry_count'])).fetchone()
            expected=[(rank,node,plan.sha256,reservation,lease['lease_id'],lease['generation'])
                      for (rank,node),plan,(reservation,lease) in zip(placements,plans,fences)]
            if existing:
                current=[tuple(r) for r in db.execute('''SELECT rank,worker_id,plan_sha256,reservation_sha256,lease_id,generation
                    FROM hybrid_staging_ranks WHERE stage_id=? ORDER BY rank''',(existing[0],))]
                if tuple(existing[1:4])!=(history[0][0],binding.sha256,actor) or current!=expected or existing[4] not in ('STAGING','READY'):
                    raise base.RegistryConflict('Staging identity already pinned; no replacement')
                return StagingAttempt(existing[0],job_id,job['retry_count'],history[0][0],binding.sha256)
            stage=secrets.token_hex(16);now=time.time_ns()
            db.execute('''INSERT INTO hybrid_staging_attempts
                (stage_id,job_id,retry_count,attempt_token,attempt_id,owner,input_binding_sha256,world_size,created_at_ns,updated_at_ns)
                VALUES (?,?,?,?,?,?,?,?,?,?)''',(stage,job_id,job['retry_count'],f'{job_id}:{job["retry_count"]}',history[0][0],actor,binding.sha256,world,now,now))
            db.executemany('''INSERT INTO hybrid_staging_ranks
                (stage_id,rank,worker_id,plan_sha256,reservation_sha256,lease_id,generation,updated_at_ns) VALUES (?,?,?,?,?,?,?,?)''',
                [(stage,*row,now) for row in expected])
            return StagingAttempt(stage,job_id,job['retry_count'],history[0][0],binding.sha256)

    def record_ready(self, actor, attempt, rank, *, plan_sha256, receipt_sha256):
        """Trusted coordinator ONLY, after workspace/auth verification in 7.2.

        Does not authenticate a student-provided hash or inspect a remote path.
        No route exposes this method. Existing execution gate remains closed.
        """
        self._enabled();principal(actor);object_key(plan_sha256);object_key(receipt_sha256)
        if not isinstance(attempt,StagingAttempt) or type(rank) is not int or not 0<=rank<=65535:
            raise ValueError('Expected exact staging attempt and rank')
        db=self.registry.db
        with self.registry._tx():
            if not schema._installed(db):raise base.RegistryError('Explicit staging migration required')
            job=_current(db,attempt.job_id);binding=self.registry._job_inputs(actor,attempt.job_id)
            row=db.execute('SELECT job_id,retry_count,attempt_id,input_binding_sha256,owner,phase,world_size FROM hybrid_staging_attempts WHERE stage_id=?',
                           (attempt.stage_id,)).fetchone()
            if (row is None or tuple(row[:5])!=(attempt.job_id,attempt.retry_count,attempt.attempt_id,attempt.input_binding_sha256,actor)
                    or job['retry_count']!=attempt.retry_count or binding is None or binding.sha256!=attempt.input_binding_sha256
                    or row[5] not in ('STAGING','READY')):raise base.RegistryError('Stale staging attempt or authority')
            stage=db.execute('''SELECT worker_id,plan_sha256,reservation_sha256,lease_id,generation,phase,receipt_sha256
                FROM hybrid_staging_ranks WHERE stage_id=? AND rank=?''',(attempt.stage_id,rank)).fetchone()
            if stage is None or stage[1]!=plan_sha256:raise base.RegistryError('Staging rank/plan mismatch')
            if (rank,stage[0]) not in _placements(db,job,row[6]):raise base.RegistryError('Rank placement changed')
            reservation,lease=_fence(db,job,stage[0],wall=time.time(),boot=_boot(),clock=_clock())
            if (reservation,lease['lease_id'],lease['generation'])!=tuple(stage[2:5]):raise base.RegistryError('Staging fence changed')
            if stage[5]=='READY':
                if stage[6]!=receipt_sha256:raise base.RegistryConflict('Ready receipt cannot change')
            else:
                now=time.time_ns()
                db.execute("UPDATE hybrid_staging_ranks SET phase='READY',receipt_sha256=?,updated_at_ns=? WHERE stage_id=? AND rank=?",
                           (receipt_sha256,now,attempt.stage_id,rank))
                if db.execute("SELECT COUNT(*) FROM hybrid_staging_ranks WHERE stage_id=? AND phase='READY'",(attempt.stage_id,)).fetchone()[0]==row[6]:
                    for saved in db.execute('''SELECT worker_id,reservation_sha256,lease_id,generation
                        FROM hybrid_staging_ranks WHERE stage_id=?''',(attempt.stage_id,)).fetchall():
                        current,current_lease=_fence(db,job,saved[0],wall=time.time(),boot=_boot(),clock=_clock())
                        if (current,current_lease['lease_id'],current_lease['generation'])!=tuple(saved[1:]):
                            raise base.RegistryError('A peer staging fence changed before the group barrier')
                    db.execute("UPDATE hybrid_staging_attempts SET phase='READY',updated_at_ns=? WHERE stage_id=?",(now,attempt.stage_id))
            return db.execute('SELECT phase FROM hybrid_staging_attempts WHERE stage_id=?',(attempt.stage_id,)).fetchone()[0]
