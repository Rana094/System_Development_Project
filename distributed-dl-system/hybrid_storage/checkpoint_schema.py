"""Explicit additive LOCAL checkpoint journal; no migration or DB open on import."""
import hashlib
import json
from . import registry_schema as base, staging_schema

DDL = [
    'CREATE TABLE hybrid_checkpoint_migrations (version INTEGER PRIMARY KEY CHECK(version=1),checksum TEXT NOT NULL)',
    '''CREATE TABLE hybrid_checkpoint_store (
        singleton INTEGER PRIMARY KEY CHECK(singleton=1),root TEXT NOT NULL,
        device INTEGER NOT NULL,inode INTEGER NOT NULL)''',
    '''CREATE TABLE hybrid_checkpoint_publications (
        publication_id TEXT PRIMARY KEY CHECK(length(publication_id)=32 AND publication_id NOT GLOB '*[^0-9a-f]*'),
        job_id TEXT NOT NULL REFERENCES jobs(job_id) ON DELETE RESTRICT,
        owner TEXT NOT NULL,retry_count INTEGER NOT NULL CHECK(retry_count>=0),
        attempt_id INTEGER NOT NULL REFERENCES job_attempts(attempt_id) ON DELETE RESTRICT,
        binding_sha256 TEXT NOT NULL REFERENCES storage_job_inputs(binding_sha256) ON DELETE RESTRICT,
        worker_id TEXT NOT NULL,kind TEXT NOT NULL CHECK(kind IN ('single','ddp')),
        source_fences TEXT NOT NULL,expected_head TEXT REFERENCES hybrid_checkpoint_publications(publication_id),
        sha256 TEXT NOT NULL CHECK(length(sha256)=64 AND sha256 NOT GLOB '*[^0-9a-f]*'),
        size_bytes INTEGER NOT NULL CHECK(typeof(size_bytes)='integer' AND size_bytes>0),
        phase TEXT NOT NULL CHECK(phase IN ('RECEIVING','VERIFIED','PUBLISHING','COMMITTED','REJECTED')),
        metadata_json TEXT,global_step INTEGER,completed INTEGER,
        created_at_ns INTEGER NOT NULL,updated_at_ns INTEGER NOT NULL CHECK(updated_at_ns>=created_at_ns),
        CHECK(phase NOT IN ('VERIFIED','PUBLISHING','COMMITTED') OR
              (metadata_json IS NOT NULL AND typeof(global_step)='integer' AND global_step>=0 AND completed IN (0,1))))''',
    '''CREATE TABLE hybrid_checkpoint_heads (
        job_id TEXT PRIMARY KEY REFERENCES jobs(job_id) ON DELETE RESTRICT,
        publication_id TEXT NOT NULL UNIQUE REFERENCES hybrid_checkpoint_publications(publication_id) ON DELETE RESTRICT,
        revision INTEGER NOT NULL CHECK(revision>=1))''',
    'CREATE INDEX hybrid_checkpoint_job ON hybrid_checkpoint_publications(job_id,retry_count)',
    'CREATE INDEX hybrid_checkpoint_pending ON hybrid_checkpoint_publications(phase,updated_at_ns)',
    '''CREATE TRIGGER hybrid_checkpoint_identity BEFORE UPDATE ON hybrid_checkpoint_publications
        WHEN NEW.publication_id IS NOT OLD.publication_id OR NEW.job_id IS NOT OLD.job_id
          OR NEW.owner IS NOT OLD.owner OR NEW.retry_count IS NOT OLD.retry_count OR NEW.attempt_id IS NOT OLD.attempt_id
          OR NEW.binding_sha256 IS NOT OLD.binding_sha256 OR NEW.worker_id IS NOT OLD.worker_id
          OR NEW.kind IS NOT OLD.kind OR NEW.source_fences IS NOT OLD.source_fences
          OR NEW.expected_head IS NOT OLD.expected_head OR NEW.sha256 IS NOT OLD.sha256 OR NEW.size_bytes IS NOT OLD.size_bytes
          OR NEW.created_at_ns IS NOT OLD.created_at_ns OR NEW.updated_at_ns<OLD.updated_at_ns
          OR (OLD.metadata_json IS NOT NULL AND (NEW.metadata_json IS NOT OLD.metadata_json
              OR NEW.global_step IS NOT OLD.global_step OR NEW.completed IS NOT OLD.completed))
        BEGIN SELECT RAISE(ABORT,'Checkpoint identity immutable'); END''',
    '''CREATE TRIGGER hybrid_checkpoint_phase BEFORE UPDATE ON hybrid_checkpoint_publications
        WHEN OLD.phase IN ('COMMITTED','REJECTED') OR NOT (
          (OLD.phase='RECEIVING' AND NEW.phase IN ('VERIFIED','REJECTED')) OR
          (OLD.phase='VERIFIED' AND NEW.phase IN ('PUBLISHING','REJECTED')) OR
          (OLD.phase='PUBLISHING' AND NEW.phase IN ('COMMITTED','REJECTED')))
        BEGIN SELECT RAISE(ABORT,'Invalid checkpoint transition'); END''',
    '''CREATE TRIGGER hybrid_checkpoint_head_insert BEFORE INSERT ON hybrid_checkpoint_heads
        WHEN NEW.revision!=1 OR NOT EXISTS (SELECT 1 FROM hybrid_checkpoint_publications p
          WHERE p.publication_id=NEW.publication_id AND p.job_id=NEW.job_id AND p.phase='COMMITTED' AND p.expected_head IS NULL)
        BEGIN SELECT RAISE(ABORT,'Checkpoint head needs committed source'); END''',
    '''CREATE TRIGGER hybrid_checkpoint_head_update BEFORE UPDATE ON hybrid_checkpoint_heads
        WHEN NEW.job_id!=OLD.job_id OR NEW.revision!=OLD.revision+1 OR NOT EXISTS (
          SELECT 1 FROM hybrid_checkpoint_publications p WHERE p.publication_id=NEW.publication_id
          AND p.job_id=NEW.job_id AND p.phase='COMMITTED' AND p.expected_head=OLD.publication_id)
        BEGIN SELECT RAISE(ABORT,'Checkpoint head compare-and-swap failed'); END''',
]
for table in ('migrations','store','publications','heads'):
    DDL.append(f"CREATE TRIGGER hybrid_checkpoint_{table}_no_delete BEFORE DELETE ON hybrid_checkpoint_{table} "
               "BEGIN SELECT RAISE(ABORT,'Checkpoint history retained'); END")
for table in ('migrations','store'):
    DDL.append(f"CREATE TRIGGER hybrid_checkpoint_{table}_no_update BEFORE UPDATE ON hybrid_checkpoint_{table} "
               "BEGIN SELECT RAISE(ABORT,'Checkpoint store identity immutable'); END")
CHECKSUM = hashlib.sha256(json.dumps(DDL,separators=(',',':')).encode()).hexdigest()


def installed(db):
    staging_schema.require_local_read(db)
    if not staging_schema._installed(db):raise base.RegistryError('Explicit staging migration required')
    expected={sql.split()[2]:base._normalized(sql) for sql in DDL}
    actual={name:base._normalized(sql) for name,table,sql in db.execute(
        'SELECT name,tbl_name,sql FROM sqlite_master WHERE sql IS NOT NULL')
        if name.startswith('hybrid_checkpoint_') or table.startswith('hybrid_checkpoint_')}
    if not actual:return False
    if actual!=expected:raise base.RegistryError('Checkpoint schema mismatch; no repair')
    if [tuple(row) for row in db.execute('SELECT * FROM hybrid_checkpoint_migrations')]!=[(1,CHECKSUM)]:
        raise base.RegistryError('Checkpoint migration checksum mismatch')
    for name in ('publications','heads'):
        if db.execute(f'PRAGMA foreign_key_check(hybrid_checkpoint_{name})').fetchone():
            raise base.RegistryError('Checkpoint foreign key violation')
    return True


def migration_plan(db):
    base.require_schema(db,check_foreign_keys=True)
    with base.transaction(db,write=False):
        exists=installed(db)
        return dict(version=1,checksum=CHECKSUM,action='unchanged' if exists else 'create',statements=[] if exists else list(DDL))


def migrate(db):
    base.require_schema(db,check_foreign_keys=True)
    with base.transaction(db):
        if not installed(db):
            for sql in DDL:db.execute(sql)
            db.execute('INSERT INTO hybrid_checkpoint_migrations VALUES(1,?)',(CHECKSUM,))
        installed(db)
    return dict(version=1,checksum=CHECKSUM)
