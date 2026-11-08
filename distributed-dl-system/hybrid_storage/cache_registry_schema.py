"""6.1 explicit controller-local cache inventory schema; no runtime enrollment.

Requires the existing immutable artifact registry v1 on a dedicated local FULL/FK
connection. No connections, migrations, imports of job_store, auth assertions,
inventory ingestion or scheduler hooks occur on import. All records are advisory.
"""

import hashlib
import json

from . import registry_schema as base


def _int(name, minimum=0, *, optional=False):
    check = f"typeof({name})='integer' AND {name}>={minimum}"
    return f"({name} IS NULL OR ({check}))" if optional else f"({check})"


def _hex(name, length=64):
    return f"typeof({name})='text' AND length({name})={length} AND {name} NOT GLOB '*[^0-9a-f]*'"


DDL = [
    """CREATE TABLE cache_registry_migrations (
        version INTEGER PRIMARY KEY CHECK(version>=1), checksum TEXT NOT NULL,
        applied_at_ns INTEGER NOT NULL CHECK(typeof(applied_at_ns)='integer' AND applied_at_ns>0))""",
    f"""CREATE TABLE cache_registry_workers (
        worker_id TEXT PRIMARY KEY NOT NULL CHECK(typeof(worker_id)='text' AND length(worker_id) BETWEEN 1 AND 63
            AND worker_id!='local' AND substr(worker_id,1,1) GLOB '[A-Za-z0-9]'
            AND worker_id NOT GLOB '*[^A-Za-z0-9_-]*'),
        auth_principal TEXT UNIQUE CHECK(auth_principal IS NULL OR
            (typeof(auth_principal)='text' AND length(auth_principal) BETWEEN 1 AND 256)),
        auth_state TEXT NOT NULL DEFAULT 'UNENROLLED' CHECK(auth_state IN ('UNENROLLED','ENROLLED','REVOKED')),
        enabled INTEGER NOT NULL DEFAULT 0 CHECK(enabled IN (0,1)),
        last_seen_ns INTEGER CHECK({_int('last_seen_ns',1,optional=True)}),
        created_at_ns INTEGER NOT NULL CHECK({_int('created_at_ns',1)}),
        updated_at_ns INTEGER NOT NULL CHECK({_int('updated_at_ns',1)} AND updated_at_ns>=created_at_ns),
        CHECK(auth_state!='ENROLLED' OR auth_principal IS NOT NULL))""",
    f"""CREATE TABLE cache_registry_identities (
        cache_id TEXT PRIMARY KEY NOT NULL CHECK({_hex('cache_id',32)}),
        worker_id TEXT NOT NULL REFERENCES cache_registry_workers(worker_id) ON DELETE RESTRICT,
        epoch_high_water INTEGER NOT NULL DEFAULT 0 CHECK({_int('epoch_high_water')}),
        retired INTEGER NOT NULL DEFAULT 0 CHECK(retired IN (0,1)),
        created_at_ns INTEGER NOT NULL CHECK({_int('created_at_ns',1)}),
        updated_at_ns INTEGER NOT NULL CHECK({_int('updated_at_ns',1)} AND updated_at_ns>=created_at_ns),
        UNIQUE(cache_id,worker_id))""",
    f"""CREATE TABLE cache_registry_sessions (
        session_id TEXT PRIMARY KEY NOT NULL CHECK({_hex('session_id',32)}),
        worker_id TEXT NOT NULL REFERENCES cache_registry_workers(worker_id) ON DELETE RESTRICT,
        cache_id TEXT NOT NULL,
        cache_epoch INTEGER NOT NULL CHECK({_int('cache_epoch',1)}),
        worker_boot_id TEXT NOT NULL CHECK(length(worker_boot_id)=36 AND length(replace(worker_boot_id,'-',''))=32
            AND worker_boot_id NOT GLOB '*[^0-9a-f-]*'
            AND substr(worker_boot_id,9,1)='-' AND substr(worker_boot_id,14,1)='-'
            AND substr(worker_boot_id,19,1)='-' AND substr(worker_boot_id,24,1)='-'),
        controller_epoch TEXT NOT NULL CHECK({_hex('controller_epoch',32)}),
        auth_principal TEXT,
        authenticated_at_ns INTEGER CHECK({_int('authenticated_at_ns',1,optional=True)}),
        state TEXT NOT NULL DEFAULT 'UNVERIFIED' CHECK(state IN ('UNVERIFIED','ACTIVE','STALE','CLOSED')),
        last_sequence INTEGER NOT NULL DEFAULT 0 CHECK({_int('last_sequence')}),
        last_body_sha256 TEXT CHECK(last_body_sha256 IS NULL OR ({_hex('last_body_sha256')})),
        has_full_snapshot INTEGER NOT NULL DEFAULT 0 CHECK(has_full_snapshot IN (0,1)),
        inventory_updated_at_ns INTEGER CHECK({_int('inventory_updated_at_ns',1,optional=True)}),
        fresh_until_ns INTEGER CHECK({_int('fresh_until_ns',1,optional=True)}),
        last_seen_ns INTEGER CHECK({_int('last_seen_ns',1,optional=True)}),
        disk_total_bytes INTEGER CHECK({_int('disk_total_bytes',optional=True)}),
        disk_free_bytes INTEGER CHECK({_int('disk_free_bytes',optional=True)}),
        disk_available_bytes INTEGER CHECK({_int('disk_available_bytes',optional=True)}),
        created_at_ns INTEGER NOT NULL CHECK({_int('created_at_ns',1)}),
        updated_at_ns INTEGER NOT NULL CHECK({_int('updated_at_ns',1)} AND updated_at_ns>=created_at_ns),
        FOREIGN KEY(cache_id,worker_id) REFERENCES cache_registry_identities(cache_id,worker_id) ON DELETE RESTRICT,
        UNIQUE(session_id,worker_id), UNIQUE(session_id,worker_id,cache_id,cache_epoch),
        CHECK((last_sequence=0 AND last_body_sha256 IS NULL AND has_full_snapshot=0
            AND inventory_updated_at_ns IS NULL AND fresh_until_ns IS NULL)
            OR (last_sequence>=1 AND last_body_sha256 IS NOT NULL AND inventory_updated_at_ns IS NOT NULL)),
        CHECK(fresh_until_ns IS NULL OR (inventory_updated_at_ns IS NOT NULL AND fresh_until_ns>inventory_updated_at_ns)),
        CHECK((disk_total_bytes IS NULL AND disk_free_bytes IS NULL AND disk_available_bytes IS NULL)
            OR (disk_total_bytes IS NOT NULL AND disk_free_bytes IS NOT NULL AND disk_available_bytes IS NOT NULL
                AND disk_available_bytes<=disk_free_bytes AND disk_free_bytes<=disk_total_bytes)),
        CHECK(state!='ACTIVE' OR (auth_principal IS NOT NULL AND authenticated_at_ns IS NOT NULL
            AND last_sequence>=1 AND has_full_snapshot=1 AND fresh_until_ns IS NOT NULL
            AND last_seen_ns IS NOT NULL AND disk_total_bytes IS NOT NULL)))""",
    f"""CREATE TABLE cache_registry_entries (
        session_id TEXT NOT NULL, worker_id TEXT NOT NULL, cache_id TEXT NOT NULL,
        cache_epoch INTEGER NOT NULL CHECK({_int('cache_epoch',1)}),
        sha256 TEXT NOT NULL CHECK({_hex('sha256')}),
        size_bytes INTEGER NOT NULL CHECK({_int('size_bytes')}),
        state TEXT NOT NULL DEFAULT 'UNVERIFIED' CHECK(state IN ('UNVERIFIED','VERIFIED','MISSING','CORRUPT','REMOVED')),
        verified_bytes INTEGER NOT NULL DEFAULT 0 CHECK({_int('verified_bytes')}),
        inventory_sequence INTEGER NOT NULL CHECK({_int('inventory_sequence',1)}),
        last_verified_at_ns INTEGER CHECK({_int('last_verified_at_ns',1,optional=True)}),
        updated_at_ns INTEGER NOT NULL CHECK({_int('updated_at_ns',1)}),
        PRIMARY KEY(session_id,sha256),
        FOREIGN KEY(session_id,worker_id,cache_id,cache_epoch)
            REFERENCES cache_registry_sessions(session_id,worker_id,cache_id,cache_epoch) ON DELETE RESTRICT,
        FOREIGN KEY(sha256,size_bytes) REFERENCES storage_artifacts(sha256,size_bytes) ON DELETE RESTRICT,
        CHECK((state='VERIFIED' AND verified_bytes=size_bytes AND last_verified_at_ns IS NOT NULL)
            OR (state!='VERIFIED' AND verified_bytes=0)))""",
    f"""CREATE TABLE cache_registry_events (
        session_id TEXT NOT NULL, worker_id TEXT NOT NULL,
        sequence INTEGER NOT NULL CHECK({_int('sequence',1)}),
        kind TEXT NOT NULL CHECK(kind IN ('FULL','CACHE_ADDED','CACHE_REMOVED')),
        base_sequence INTEGER CHECK({_int('base_sequence',optional=True)}),
        body_sha256 TEXT NOT NULL CHECK({_hex('body_sha256')}),
        received_at_ns INTEGER NOT NULL CHECK({_int('received_at_ns',1)}),
        PRIMARY KEY(session_id,sequence),
        FOREIGN KEY(session_id,worker_id) REFERENCES cache_registry_sessions(session_id,worker_id) ON DELETE RESTRICT,
        CHECK((kind='FULL' AND base_sequence IS NULL)
            OR (kind!='FULL' AND base_sequence IS NOT NULL AND base_sequence=sequence-1)))""",
    "CREATE INDEX cache_registry_worker_seen ON cache_registry_workers(enabled,auth_state,last_seen_ns)",
    "CREATE INDEX cache_registry_identity_worker ON cache_registry_identities(worker_id,retired,epoch_high_water)",
    "CREATE UNIQUE INDEX cache_registry_active_worker ON cache_registry_sessions(worker_id) WHERE state='ACTIVE'",
    "CREATE INDEX cache_registry_session_fresh ON cache_registry_sessions(worker_id,state,fresh_until_ns,last_seen_ns)",
    "CREATE INDEX cache_registry_entry_worker ON cache_registry_entries(worker_id,session_id,state)",
    "CREATE INDEX cache_registry_entry_digest ON cache_registry_entries(sha256,worker_id)",
    "CREATE INDEX cache_registry_event_time ON cache_registry_events(received_at_ns)",
    """CREATE TRIGGER cache_registry_worker_identity BEFORE UPDATE ON cache_registry_workers
        WHEN NEW.worker_id!=OLD.worker_id BEGIN SELECT RAISE(ABORT,'Worker identity immutable'); END""",
    """CREATE TRIGGER cache_registry_worker_revoked AFTER UPDATE ON cache_registry_workers
        WHEN NEW.enabled=0 OR NEW.auth_state!='ENROLLED' OR NEW.auth_principal IS NOT OLD.auth_principal
        BEGIN UPDATE cache_registry_sessions SET state='STALE' WHERE worker_id=NEW.worker_id AND state='ACTIVE'; END""",
    """CREATE TRIGGER cache_registry_identity_guard BEFORE UPDATE ON cache_registry_identities
        WHEN NEW.cache_id!=OLD.cache_id OR NEW.worker_id!=OLD.worker_id
            OR NEW.epoch_high_water<OLD.epoch_high_water OR NEW.retired<OLD.retired
        BEGIN SELECT RAISE(ABORT,'Cache identity/high-water cannot roll back'); END""",
    """CREATE TRIGGER cache_registry_identity_stale AFTER UPDATE ON cache_registry_identities
        WHEN NEW.retired=1 OR NEW.epoch_high_water>OLD.epoch_high_water
        BEGIN UPDATE cache_registry_sessions SET state='STALE' WHERE cache_id=NEW.cache_id AND state='ACTIVE'; END""",
    """CREATE TRIGGER cache_registry_session_identity BEFORE UPDATE ON cache_registry_sessions
        WHEN NEW.session_id!=OLD.session_id OR NEW.worker_id!=OLD.worker_id OR NEW.cache_id!=OLD.cache_id
            OR NEW.cache_epoch!=OLD.cache_epoch OR NEW.worker_boot_id!=OLD.worker_boot_id
            OR NEW.controller_epoch!=OLD.controller_epoch
        BEGIN SELECT RAISE(ABORT,'Inventory session identity immutable'); END""",
    """CREATE TRIGGER cache_registry_session_cursor BEFORE UPDATE ON cache_registry_sessions
        WHEN NEW.last_sequence<OLD.last_sequence OR (NEW.last_sequence=OLD.last_sequence AND (
            NEW.last_body_sha256 IS NOT OLD.last_body_sha256 OR NEW.has_full_snapshot!=OLD.has_full_snapshot
            OR NEW.inventory_updated_at_ns IS NOT OLD.inventory_updated_at_ns OR NEW.fresh_until_ns IS NOT OLD.fresh_until_ns))
        BEGIN SELECT RAISE(ABORT,'Sequence/body/freshness replay'); END""",
    """CREATE TRIGGER cache_registry_session_no_reactivate BEFORE UPDATE ON cache_registry_sessions
        WHEN OLD.state IN ('STALE','CLOSED') AND NEW.state IN ('UNVERIFIED','ACTIVE')
        BEGIN SELECT RAISE(ABORT,'Stale/closed sessions require a new authenticated handshake'); END""",
]
for operation in ('INSERT', 'UPDATE'):
    DDL.append(f"""CREATE TRIGGER cache_registry_session_admit_{operation.lower()} BEFORE {operation} ON cache_registry_sessions
        WHEN (NEW.state IN ('UNVERIFIED','ACTIVE') AND NOT EXISTS(SELECT 1 FROM cache_registry_identities
            WHERE cache_id=NEW.cache_id AND worker_id=NEW.worker_id AND retired=0 AND epoch_high_water=NEW.cache_epoch))
        OR (NEW.state='ACTIVE' AND NOT EXISTS(SELECT 1 FROM cache_registry_workers WHERE worker_id=NEW.worker_id
            AND enabled=1 AND auth_state='ENROLLED' AND auth_principal=NEW.auth_principal AND last_seen_ns IS NOT NULL))
        BEGIN SELECT RAISE(ABORT,'Untrusted worker/cache session'); END""")
    for table, sequence in (('entries', 'inventory_sequence'), ('events', 'sequence')):
        DDL.append(f"""CREATE TRIGGER cache_registry_{table}_sequence_{operation.lower()} BEFORE {operation} ON cache_registry_{table}
            WHEN NOT EXISTS(SELECT 1 FROM cache_registry_sessions WHERE session_id=NEW.session_id
                AND worker_id=NEW.worker_id AND last_sequence>=NEW.{sequence})
            BEGIN SELECT RAISE(ABORT,'Observation sequence exceeds session'); END""")
for table, operations in (('migrations', ('UPDATE', 'DELETE')), ('events', ('UPDATE', 'DELETE')),
                          ('identities', ('DELETE',)), ('workers', ('DELETE',)), ('sessions', ('DELETE',))):
    for operation in operations:
        DDL.append(f"CREATE TRIGGER cache_registry_{table}_no_{operation.lower()} BEFORE {operation} ON cache_registry_{table} "
                   "BEGIN SELECT RAISE(ABORT,'Registry history immutable'); END")

CHECKSUM = hashlib.sha256(json.dumps(DDL, separators=(',', ':')).encode()).hexdigest()
TABLES = {statement.split()[2] for statement in DDL if statement.startswith('CREATE TABLE ')}


def _installed(db, *, foreign_keys=True):
    expected = {sql.split()[3] if sql.startswith('CREATE UNIQUE INDEX') else sql.split()[2]: base._normalized(sql) for sql in DDL}
    rows = db.execute('SELECT name,tbl_name,sql FROM sqlite_master WHERE sql IS NOT NULL').fetchall()
    actual = {row[0]: base._normalized(row[2]) for row in rows
              if row[0].startswith('cache_registry_') or row[1] in TABLES}
    if not actual:
        return False
    if actual != expected:
        raise base.RegistryError('Cache registry shape/index/trigger mismatch; no automatic repair')
    if [tuple(row) for row in db.execute('SELECT version,checksum FROM cache_registry_migrations')] != [(1, CHECKSUM)]:
        raise base.RegistryError('Unknown cache registry migration/checksum')
    if foreign_keys:
        for table in TABLES:
            if db.execute(f'PRAGMA foreign_key_check({table})').fetchone():
                raise base.RegistryError('Cache registry foreign key violation')
    return True


def migration_plan(db):
    base.require_schema(db, check_foreign_keys=True)
    with base.transaction(db, write=False):
        installed = _installed(db)
        return dict(version=1, checksum=CHECKSUM, action='unchanged' if installed else 'create',
                    statements=[] if installed else list(DDL))


def migrate(db):
    """Explicit additive transactional migration; never opens/copies a DB itself."""
    import time
    base.require_schema(db, check_foreign_keys=True)
    with base.transaction(db):
        if not base._installed(db):
            raise base.RegistryError('Artifact registry prerequisite disappeared')
        if not _installed(db):
            for sql in DDL:
                db.execute(sql)
            db.execute('INSERT INTO cache_registry_migrations VALUES (1,?,?)', (CHECKSUM, time.time_ns()))
        _installed(db)
    return dict(version=1, checksum=CHECKSUM)


def require_schema(db):
    base.require_schema(db, check_foreign_keys=True)
    with base.transaction(db, write=False):
        if not _installed(db):
            raise base.RegistryError('Cache registry migration must be applied explicitly')
