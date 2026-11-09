"""Explicit additive frozen checkpoint source/restore barrier; no startup migration."""
import hashlib
import json
from . import checkpoint_schema,registry_schema as base

DDL=[
    'CREATE TABLE hybrid_restore_migrations (version INTEGER PRIMARY KEY CHECK(version=1),checksum TEXT NOT NULL)',
    '''CREATE TABLE hybrid_restore_attempts (
        job_id TEXT NOT NULL REFERENCES jobs(job_id),retry_count INTEGER NOT NULL CHECK(retry_count>=0),
        binding_sha256 TEXT NOT NULL REFERENCES storage_job_inputs(binding_sha256),
        source_id TEXT REFERENCES hybrid_checkpoint_publications(publication_id),source_json TEXT,
        stage_id TEXT UNIQUE REFERENCES hybrid_staging_attempts(stage_id),
        confirmed INTEGER NOT NULL DEFAULT 0 CHECK(confirmed IN (0,1)),
        final_failures INTEGER NOT NULL DEFAULT 0 CHECK(final_failures>=0),
        PRIMARY KEY(job_id,retry_count),CHECK((source_id IS NULL)=(source_json IS NULL)))''',
    '''CREATE TABLE hybrid_restore_ranks (
        stage_id TEXT NOT NULL,rank INTEGER NOT NULL,
        receipt_sha256 TEXT,launch_sent INTEGER NOT NULL DEFAULT 0 CHECK(launch_sent IN (0,1)),
        started_at REAL,restored INTEGER NOT NULL DEFAULT 0 CHECK(restored IN (0,1)),
        PRIMARY KEY(stage_id,rank),FOREIGN KEY(stage_id,rank) REFERENCES hybrid_staging_ranks(stage_id,rank))''',
    'CREATE INDEX hybrid_restore_source ON hybrid_restore_attempts(source_id)',
    '''CREATE TRIGGER hybrid_restore_attempt_guard BEFORE UPDATE ON hybrid_restore_attempts
        WHEN NEW.job_id IS NOT OLD.job_id OR NEW.retry_count IS NOT OLD.retry_count
          OR NEW.binding_sha256 IS NOT OLD.binding_sha256 OR NEW.source_id IS NOT OLD.source_id
          OR NEW.source_json IS NOT OLD.source_json OR (OLD.stage_id IS NOT NULL AND NEW.stage_id IS NOT OLD.stage_id)
          OR NEW.confirmed<OLD.confirmed
        BEGIN SELECT RAISE(ABORT,'Frozen restore source immutable'); END''',
    '''CREATE TRIGGER hybrid_restore_rank_guard BEFORE UPDATE ON hybrid_restore_ranks
        WHEN NEW.stage_id IS NOT OLD.stage_id OR NEW.rank IS NOT OLD.rank OR NEW.launch_sent<OLD.launch_sent
          OR NEW.restored<OLD.restored OR (OLD.receipt_sha256 IS NOT NULL AND NEW.receipt_sha256 IS NOT OLD.receipt_sha256)
          OR (OLD.started_at IS NOT NULL AND NEW.started_at IS NOT OLD.started_at)
        BEGIN SELECT RAISE(ABORT,'Restore barrier cannot regress'); END''',
]
for name in ('migrations','attempts','ranks'):
    DDL.append(f"CREATE TRIGGER hybrid_restore_{name}_no_delete BEFORE DELETE ON hybrid_restore_{name} "
               "BEGIN SELECT RAISE(ABORT,'Restore history retained'); END")
DDL.append("CREATE TRIGGER hybrid_restore_migrations_no_update BEFORE UPDATE ON hybrid_restore_migrations "
           "BEGIN SELECT RAISE(ABORT,'Restore migration immutable'); END")
CHECKSUM=hashlib.sha256(json.dumps(DDL,separators=(',',':')).encode()).hexdigest()


def installed(db):
    if not checkpoint_schema.installed(db):raise base.RegistryError('Explicit checkpoint migration required')
    actual={name:base._normalized(sql) for name,table,sql in db.execute('SELECT name,tbl_name,sql FROM sqlite_master WHERE sql IS NOT NULL')
            if name.startswith('hybrid_restore_') or table.startswith('hybrid_restore_')}
    if not actual:return False
    if actual!={sql.split()[2]:base._normalized(sql) for sql in DDL}:raise base.RegistryError('Restore schema mismatch; no repair')
    if [tuple(row) for row in db.execute('SELECT * FROM hybrid_restore_migrations')]!=[(1,CHECKSUM)]:raise base.RegistryError('Restore checksum mismatch')
    for name in ('attempts','ranks'):
        if db.execute(f'PRAGMA foreign_key_check(hybrid_restore_{name})').fetchone():raise base.RegistryError('Restore foreign key violation')
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
            db.execute('INSERT INTO hybrid_restore_migrations VALUES(1,?)',(CHECKSUM,))
        installed(db)
    return dict(version=1,checksum=CHECKSUM)
