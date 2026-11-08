"""Explicit additive LOCAL FULL/FK cleanup acknowledgments; no automatic migration."""
import hashlib
import json
from . import registry_schema as base,staging_schema

DDL=[
    'CREATE TABLE hybrid_cleanup_migrations (version INTEGER PRIMARY KEY CHECK(version=1),checksum TEXT NOT NULL)',
    """CREATE TABLE hybrid_cleanup_ranks (
        stage_id TEXT NOT NULL,rank INTEGER NOT NULL,
        phase TEXT NOT NULL CHECK(phase IN ('PENDING','LEASE_FENCED','QUIESCENT')),
        updated_at_ns INTEGER NOT NULL CHECK(typeof(updated_at_ns)='integer' AND updated_at_ns>0),
        PRIMARY KEY(stage_id,rank),FOREIGN KEY(stage_id,rank) REFERENCES hybrid_staging_ranks(stage_id,rank) ON DELETE RESTRICT)""",
    'CREATE INDEX hybrid_cleanup_pending ON hybrid_cleanup_ranks (phase,stage_id)',
    """CREATE TRIGGER hybrid_cleanup_rank_identity BEFORE UPDATE ON hybrid_cleanup_ranks
        WHEN NEW.stage_id!=OLD.stage_id OR NEW.rank!=OLD.rank OR NEW.updated_at_ns<OLD.updated_at_ns
          OR (OLD.phase='QUIESCENT' AND NEW.phase!='QUIESCENT') OR (OLD.phase='LEASE_FENCED' AND NEW.phase='PENDING')
        BEGIN SELECT RAISE(ABORT,'Cleanup proof cannot regress'); END""",
]
for table in ('migrations','ranks'):
    DDL.append(f"CREATE TRIGGER hybrid_cleanup_{table}_no_delete BEFORE DELETE ON hybrid_cleanup_{table} "
               "BEGIN SELECT RAISE(ABORT,'Cleanup history retained'); END")
DDL.append("CREATE TRIGGER hybrid_cleanup_migrations_no_update BEFORE UPDATE ON hybrid_cleanup_migrations "
           "BEGIN SELECT RAISE(ABORT,'Cleanup migration immutable'); END")
CHECKSUM=hashlib.sha256(json.dumps(DDL,separators=(',',':')).encode()).hexdigest()


def installed(db):
    staging_schema.require_local_read(db)
    if not staging_schema._installed(db):raise base.RegistryError('Explicit staging migration required')
    expected={sql.split()[2]:base._normalized(sql) for sql in DDL}
    actual={name:base._normalized(sql) for name,table,sql in db.execute('SELECT name,tbl_name,sql FROM sqlite_master WHERE sql IS NOT NULL')
            if name.startswith('hybrid_cleanup_') or table in ('hybrid_cleanup_migrations','hybrid_cleanup_ranks')}
    if not actual:return False
    if actual!=expected:raise base.RegistryError('Cleanup schema mismatch; no repair')
    if [tuple(row) for row in db.execute('SELECT * FROM hybrid_cleanup_migrations')]!=[(1,CHECKSUM)]:
        raise base.RegistryError('Cleanup checksum mismatch')
    if db.execute('PRAGMA foreign_key_check(hybrid_cleanup_ranks)').fetchone():raise base.RegistryError('Cleanup foreign key violation')
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
            db.execute('INSERT INTO hybrid_cleanup_migrations VALUES(1,?)',(CHECKSUM,))
        installed(db)
    return dict(version=1,checksum=CHECKSUM)
