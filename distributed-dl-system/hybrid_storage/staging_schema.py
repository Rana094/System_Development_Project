"""Explicit additive attempt/rank staging side state; never changes job enums.

Dedicated controller-local FULL/FK connection, existing artifact/job tables,
no automatic enrollment, migration or dispatcher/worker activation.
"""

import hashlib
import json
import os
from pathlib import Path
import stat

from . import registry_schema as base
from .backends import _directory
from .mount_identity import require_local_fd

HEX = lambda name: f"length({name})=64 AND {name} NOT GLOB '*[^0-9a-f]*'"
DDL = [
    """CREATE TABLE hybrid_staging_migrations (
        version INTEGER PRIMARY KEY CHECK(version=1),checksum TEXT NOT NULL)""",
    f"""CREATE TABLE hybrid_staging_attempts (
        stage_id TEXT PRIMARY KEY NOT NULL CHECK(length(stage_id)=32 AND stage_id NOT GLOB '*[^0-9a-f]*'),
        job_id TEXT NOT NULL REFERENCES jobs(job_id) ON DELETE RESTRICT,
        retry_count INTEGER NOT NULL CHECK(typeof(retry_count)='integer' AND retry_count>=0),
        attempt_token TEXT NOT NULL CHECK(attempt_token=job_id||':'||retry_count),
        attempt_id INTEGER NOT NULL UNIQUE REFERENCES job_attempts(attempt_id) ON DELETE RESTRICT,
        owner TEXT NOT NULL,
        input_binding_sha256 TEXT NOT NULL REFERENCES storage_job_inputs(binding_sha256) ON DELETE RESTRICT,
        world_size INTEGER NOT NULL CHECK(typeof(world_size)='integer' AND world_size BETWEEN 1 AND 65536),
        phase TEXT NOT NULL DEFAULT 'STAGING' CHECK(phase IN ('STAGING','READY','STARTED','ABORTING','TERMINAL')),
        created_at_ns INTEGER NOT NULL CHECK(typeof(created_at_ns)='integer' AND created_at_ns>0),
        updated_at_ns INTEGER NOT NULL CHECK(typeof(updated_at_ns)='integer' AND updated_at_ns>=created_at_ns),
        UNIQUE(job_id,retry_count))""",
    f"""CREATE TABLE hybrid_staging_ranks (
        stage_id TEXT NOT NULL REFERENCES hybrid_staging_attempts(stage_id) ON DELETE RESTRICT,
        rank INTEGER NOT NULL CHECK(typeof(rank)='integer' AND rank BETWEEN 0 AND 65535),
        worker_id TEXT NOT NULL,
        plan_sha256 TEXT NOT NULL CHECK({HEX('plan_sha256')}),
        reservation_sha256 TEXT NOT NULL CHECK({HEX('reservation_sha256')}),
        lease_id TEXT NOT NULL REFERENCES execution_leases(lease_id) ON DELETE RESTRICT,
        generation INTEGER NOT NULL CHECK(typeof(generation)='integer' AND generation>=1),
        phase TEXT NOT NULL DEFAULT 'STAGING' CHECK(phase IN ('STAGING','READY')),
        receipt_sha256 TEXT CHECK(receipt_sha256 IS NULL OR ({HEX('receipt_sha256')})),
        updated_at_ns INTEGER NOT NULL CHECK(typeof(updated_at_ns)='integer' AND updated_at_ns>0),
        CHECK((phase='STAGING' AND receipt_sha256 IS NULL) OR (phase='READY' AND receipt_sha256 IS NOT NULL)),
        PRIMARY KEY(stage_id,rank),UNIQUE(stage_id,worker_id))""",
    "CREATE INDEX hybrid_staging_worker ON hybrid_staging_ranks(worker_id,phase)",
    "CREATE INDEX hybrid_staging_phase ON hybrid_staging_attempts(phase,job_id,retry_count)",
    """CREATE TRIGGER hybrid_staging_attempt_identity BEFORE UPDATE ON hybrid_staging_attempts
        WHEN NEW.stage_id!=OLD.stage_id OR NEW.job_id!=OLD.job_id OR NEW.retry_count!=OLD.retry_count
          OR NEW.attempt_token!=OLD.attempt_token OR NEW.attempt_id!=OLD.attempt_id OR NEW.owner!=OLD.owner
          OR NEW.input_binding_sha256!=OLD.input_binding_sha256 OR NEW.world_size!=OLD.world_size
          OR NEW.created_at_ns!=OLD.created_at_ns OR NEW.updated_at_ns<OLD.updated_at_ns
        BEGIN SELECT RAISE(ABORT,'Staging attempt identity immutable'); END""",
    """CREATE TRIGGER hybrid_staging_attempt_phase BEFORE UPDATE ON hybrid_staging_attempts
        WHEN NEW.phase!=OLD.phase AND NOT (
          (OLD.phase='STAGING' AND NEW.phase IN ('READY','ABORTING')) OR
          (OLD.phase='READY' AND NEW.phase IN ('STARTED','ABORTING')) OR
          (OLD.phase='STARTED' AND NEW.phase IN ('ABORTING','TERMINAL')) OR
          (OLD.phase='ABORTING' AND NEW.phase='TERMINAL'))
        BEGIN SELECT RAISE(ABORT,'Invalid staging phase transition'); END""",
    """CREATE TRIGGER hybrid_staging_ready_barrier BEFORE UPDATE ON hybrid_staging_attempts
        WHEN NEW.phase IN ('READY','STARTED') AND (
          (SELECT COUNT(*) FROM hybrid_staging_ranks WHERE stage_id=NEW.stage_id AND phase='READY')!=NEW.world_size
          OR (SELECT MIN(rank) FROM hybrid_staging_ranks WHERE stage_id=NEW.stage_id)!=0
          OR (SELECT MAX(rank) FROM hybrid_staging_ranks WHERE stage_id=NEW.stage_id)!=NEW.world_size-1)
        BEGIN SELECT RAISE(ABORT,'All staging ranks must be ready'); END""",
    """CREATE TRIGGER hybrid_staging_rank_identity BEFORE UPDATE ON hybrid_staging_ranks
        WHEN NEW.stage_id!=OLD.stage_id OR NEW.rank!=OLD.rank OR NEW.worker_id!=OLD.worker_id
          OR NEW.plan_sha256!=OLD.plan_sha256 OR NEW.reservation_sha256!=OLD.reservation_sha256
          OR NEW.lease_id!=OLD.lease_id OR NEW.generation!=OLD.generation OR NEW.updated_at_ns<OLD.updated_at_ns
          OR (OLD.phase='READY' AND (NEW.phase!='READY' OR NEW.receipt_sha256 IS NOT OLD.receipt_sha256))
        BEGIN SELECT RAISE(ABORT,'Staging rank identity/receipt immutable'); END""",
    """CREATE TRIGGER hybrid_staging_no_unready_running BEFORE UPDATE OF status ON jobs
        WHEN NEW.status='RUNNING' AND EXISTS(SELECT 1 FROM job_storage_bindings WHERE job_id=NEW.job_id AND backend='nfs')
          AND NOT EXISTS(SELECT 1 FROM hybrid_staging_attempts a WHERE a.job_id=NEW.job_id
            AND a.retry_count=NEW.retry_count AND a.phase IN ('READY','STARTED')
            AND (SELECT COUNT(*) FROM hybrid_staging_ranks r WHERE r.stage_id=a.stage_id AND r.phase='READY')=a.world_size)
        BEGIN SELECT RAISE(ABORT,'NFS attempt has not completed staging'); END""",
]
for table in ('migrations','attempts','ranks'):
    DDL.append(f"CREATE TRIGGER hybrid_staging_{table}_no_delete BEFORE DELETE ON hybrid_staging_{table} "
               "BEGIN SELECT RAISE(ABORT,'Staging history retained'); END")
DDL.append("CREATE TRIGGER hybrid_staging_migrations_no_update BEFORE UPDATE ON hybrid_staging_migrations "
           "BEGIN SELECT RAISE(ABORT,'Staging migration immutable'); END")
CHECKSUM=hashlib.sha256(json.dumps(DDL,separators=(',',':')).encode()).hexdigest()
TABLES={sql.split()[2] for sql in DDL if sql.startswith('CREATE TABLE ')}


def _installed(db):
    expected={sql.split()[2]:base._normalized(sql) for sql in DDL}
    actual={name:base._normalized(sql) for name,table,sql in db.execute(
        'SELECT name,tbl_name,sql FROM sqlite_master WHERE sql IS NOT NULL')
        if name.startswith('hybrid_staging_') or table in TABLES}
    if not actual:return False
    if actual!=expected:raise base.RegistryError('Staging schema mismatch; no automatic repair')
    if [tuple(row) for row in db.execute('SELECT * FROM hybrid_staging_migrations')]!=[(1,CHECKSUM)]:
        raise base.RegistryError('Staging migration checksum mismatch')
    for table in TABLES:
        if db.execute(f'PRAGMA foreign_key_check({table})').fetchone():
            raise base.RegistryError('Staging foreign key violation')
    return True


def _prerequisites(db):
    for table,columns in {
        'jobs':{'job_id','status','retry_count','owner','job_config','storage_manifest_sha256','node_id','cancel_requested_at'},
        'job_attempts':{'attempt_id','job_id','attempt_number','status'},
        'resource_reservations':{'node_id','job_id','resources_json','created_at'},
        'execution_leases':{'job_id','retry_count','node_id','lease_id','generation','attempt_token','issued_at',
            'expires_at','controller_boot_id','expires_clock','renewal_blocked','revoked'},
        'job_ranks':{'job_id','retry_count','rank','node_id'},
        'job_storage_bindings':{'job_id','backend'},
    }.items():
        if not columns<={row[1] for row in db.execute(f'PRAGMA table_info({table})')}:
            raise base.RegistryError('Actual job/reservation/lease schema prerequisite missing')


def require_local_read(db):
    """Read-only attestation for job_store's already-open NORMAL transaction."""
    databases=db.execute('PRAGMA database_list').fetchall()
    main=next((row[2] for row in databases if row[1]=='main'),None)
    if not main or any(row[1] not in ('main','temp') for row in databases):
        raise base.RegistryError('Staging barrier requires one named controller-local DB')
    path=Path(main)
    with _directory(path.parent) as parent:
        # Never open/close an ordinary descriptor behind SQLite's VFS. O_PATH
        # preserves its process-wide POSIX locks while allowing fstat/mnt_id.
        fd=os.open(path.name,os.O_PATH|os.O_NOFOLLOW|os.O_CLOEXEC,dir_fd=parent)
        try:
            if not stat.S_ISREG(os.fstat(fd).st_mode):raise base.RegistryError('Staging DB is not regular')
            require_local_fd(fd)
        finally:os.close(fd)


def migration_plan(db):
    base.require_schema(db,check_foreign_keys=True);_prerequisites(db)
    with base.transaction(db,write=False):
        installed=_installed(db)
        return dict(version=1,checksum=CHECKSUM,action='unchanged' if installed else 'create',
                    statements=[] if installed else list(DDL))


def migrate(db):
    base.require_schema(db,check_foreign_keys=True);_prerequisites(db)
    with base.transaction(db):
        if not _installed(db):
            for sql in DDL:db.execute(sql)
            db.execute('INSERT INTO hybrid_staging_migrations VALUES(1,?)',(CHECKSUM,))
        _installed(db)
    return dict(version=1,checksum=CHECKSUM)
