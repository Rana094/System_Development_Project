"""Explicit additive registry migration on a dedicated controller-local connection.

Never imports job_store, opens a DB, or applies a migration on import. Callers
must configure FK=ON and synchronous=FULL outside transactions. No backfill.
"""

from contextlib import contextmanager
import hashlib
import json
import os
import stat
from pathlib import Path

from .backends import _directory
from .mount_identity import require_local_fd

SHA = "length(sha256)=64 AND sha256 NOT GLOB '*[^0-9a-f]*'"
DDL = [
    """CREATE TABLE hybrid_schema_migrations (
        version INTEGER PRIMARY KEY CHECK(version=1), checksum TEXT NOT NULL)""",
    f"""CREATE TABLE storage_artifacts (
        sha256 TEXT PRIMARY KEY NOT NULL CHECK({SHA}),
        size_bytes INTEGER NOT NULL CHECK(typeof(size_bytes)='integer' AND size_bytes>=0),
        object_key TEXT NOT NULL UNIQUE CHECK(object_key='sha256/'||substr(sha256,1,2)||'/'||sha256),
        UNIQUE(sha256,size_bytes))""",
    """CREATE TABLE storage_artifact_owners (
        owner TEXT NOT NULL, sha256 TEXT NOT NULL REFERENCES storage_artifacts(sha256) ON DELETE RESTRICT,
        PRIMARY KEY(owner,sha256))""",
    f"""CREATE TABLE storage_manifests (
        sha256 TEXT PRIMARY KEY NOT NULL CHECK({SHA}),
        kind TEXT NOT NULL CHECK(kind IN ('source-tree','dataset-manifest')),
        canonical BLOB NOT NULL CHECK(typeof(canonical)='blob' AND length(canonical)<=4194304))""",
    """CREATE TABLE storage_manifest_entries (
        manifest_sha256 TEXT NOT NULL REFERENCES storage_manifests(sha256) ON DELETE RESTRICT,
        path TEXT NOT NULL, type TEXT NOT NULL CHECK(type IN ('file','directory')),
        artifact_sha256 TEXT, size_bytes INTEGER, executable INTEGER,
        PRIMARY KEY(manifest_sha256,path),
        FOREIGN KEY(artifact_sha256,size_bytes) REFERENCES storage_artifacts(sha256,size_bytes) ON DELETE RESTRICT,
        CHECK((type='directory' AND artifact_sha256 IS NULL AND size_bytes IS NULL AND executable IS NULL)
           OR (type='file' AND artifact_sha256 IS NOT NULL AND size_bytes IS NOT NULL
               AND typeof(size_bytes)='integer' AND size_bytes>=0 AND executable IN (0,1) AND executable IS NOT NULL)))""",
    """CREATE TABLE storage_manifest_owners (
        owner TEXT NOT NULL, manifest_sha256 TEXT NOT NULL REFERENCES storage_manifests(sha256) ON DELETE RESTRICT,
        PRIMARY KEY(owner,manifest_sha256))""",
    """CREATE TABLE storage_datasets (
        dataset_id TEXT PRIMARY KEY NOT NULL, owner TEXT NOT NULL,
        head_version INTEGER NOT NULL CHECK(typeof(head_version)='integer' AND head_version>=1),
        FOREIGN KEY(dataset_id,head_version) REFERENCES storage_dataset_versions(dataset_id,version)
            DEFERRABLE INITIALLY DEFERRED)""",
    """CREATE TABLE storage_dataset_versions (
        dataset_id TEXT NOT NULL REFERENCES storage_datasets(dataset_id) ON DELETE RESTRICT,
        version INTEGER NOT NULL CHECK(typeof(version)='integer' AND version>=1),
        manifest_sha256 TEXT NOT NULL UNIQUE REFERENCES storage_manifests(sha256) ON DELETE RESTRICT,
        PRIMARY KEY(dataset_id,version), UNIQUE(dataset_id,version,manifest_sha256))""",
    """CREATE TABLE storage_dataset_readers (
        dataset_id TEXT NOT NULL REFERENCES storage_datasets(dataset_id) ON DELETE RESTRICT,
        reader TEXT NOT NULL, PRIMARY KEY(dataset_id,reader))""",
    """CREATE TABLE storage_job_inputs (
        job_id TEXT PRIMARY KEY NOT NULL REFERENCES jobs(job_id) ON DELETE RESTRICT,
        owner TEXT NOT NULL,
        source_sha256 TEXT NOT NULL REFERENCES storage_manifests(sha256) ON DELETE RESTRICT,
        dataset_id TEXT, dataset_version INTEGER, dataset_sha256 TEXT,
        requirements_path TEXT NOT NULL, requirements_sha256 TEXT NOT NULL,
        requirements_size_bytes INTEGER NOT NULL,
        legacy_manifest_sha256 TEXT NOT NULL, execution_spec_sha256 TEXT NOT NULL,
        binding_sha256 TEXT NOT NULL UNIQUE,
        FOREIGN KEY(dataset_id,dataset_version,dataset_sha256)
            REFERENCES storage_dataset_versions(dataset_id,version,manifest_sha256) ON DELETE RESTRICT,
        FOREIGN KEY(requirements_sha256,requirements_size_bytes)
            REFERENCES storage_artifacts(sha256,size_bytes) ON DELETE RESTRICT,
        CHECK((dataset_id IS NULL AND dataset_version IS NULL AND dataset_sha256 IS NULL)
           OR (dataset_id IS NOT NULL AND dataset_version IS NOT NULL AND dataset_sha256 IS NOT NULL)))""",
    "CREATE INDEX storage_entries_artifact ON storage_manifest_entries(artifact_sha256)",
    "CREATE INDEX storage_owners_artifact ON storage_artifact_owners(sha256)",
    "CREATE INDEX storage_manifest_grants ON storage_manifest_owners(manifest_sha256)",
    "CREATE INDEX storage_readers_principal ON storage_dataset_readers(reader)",
    "CREATE INDEX storage_jobs_source ON storage_job_inputs(source_sha256)",
    "CREATE INDEX storage_jobs_dataset ON storage_job_inputs(dataset_id,dataset_version)",
    "CREATE INDEX storage_jobs_requirements ON storage_job_inputs(requirements_sha256)",
    """CREATE TRIGGER storage_dataset_head_guard BEFORE UPDATE ON storage_datasets
       WHEN NEW.dataset_id!=OLD.dataset_id OR NEW.owner!=OLD.owner OR NEW.head_version!=OLD.head_version+1
       BEGIN SELECT RAISE(ABORT,'Dataset owner/identity immutable; head advances by one'); END""",
    """CREATE TRIGGER storage_dataset_no_delete BEFORE DELETE ON storage_datasets
       BEGIN SELECT RAISE(ABORT,'Dataset deletion unavailable'); END""",
]
IMMUTABLE = ('hybrid_schema_migrations', 'storage_artifacts', 'storage_artifact_owners',
             'storage_manifests', 'storage_manifest_entries', 'storage_manifest_owners',
             'storage_dataset_versions', 'storage_job_inputs')
for table in IMMUTABLE:
    for operation in ('UPDATE', 'DELETE'):
        DDL.append(f"CREATE TRIGGER {table}_{operation.lower()} BEFORE {operation} ON {table} "
                   "BEGIN SELECT RAISE(ABORT,'Registry record is immutable'); END")

CHECKSUM = hashlib.sha256(json.dumps(DDL, separators=(',', ':')).encode()).hexdigest()
TABLES = {statement.split()[2] for statement in DDL if statement.startswith('CREATE TABLE ')}


class RegistryError(ValueError):
    """Incompatible registry, input binding or explicit connection contract."""


class RegistryConflict(RegistryError):
    """A concurrent writer advanced the head or a pinned identity conflicts."""


def connection_ready(db):
    if db.in_transaction:
        raise RegistryError("Registry needs an idle dedicated connection; caller work is not committed")
    if db.execute('PRAGMA foreign_keys').fetchone()[0] != 1 or db.execute('PRAGMA synchronous').fetchone()[0] < 2:
        raise RegistryError("Registry requires foreign_keys=ON and synchronous=FULL (or EXTRA)")
    databases = db.execute('PRAGMA database_list').fetchall()
    main = next((row[2] for row in databases if row[1] == 'main'), None)
    if not main or any(row[1] not in ('main', 'temp') for row in databases):
        raise RegistryError("Registry requires a named local main DB, without attachments")
    if db.execute('SELECT 1 FROM sqlite_temp_master LIMIT 1').fetchone():
        raise RegistryError("Registry connections must not have temporary schema objects")
    path = Path(main)
    with _directory(path.parent) as parent:
        # Linux O_PATH is metadata-only: closing an ordinary DB descriptor here
        # would cancel ALL this process's POSIX SQLite locks on that inode,
        # including locks held by other live connections/threads.
        fd = os.open(path.name, os.O_PATH | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=parent)
        try:
            if not stat.S_ISREG(os.fstat(fd).st_mode):
                raise RegistryError("Registry main path is not a regular file")
            require_local_fd(fd)
        finally:
            os.close(fd)


@contextmanager
def transaction(db, *, write=True):
    connection_ready(db)
    db.execute('BEGIN IMMEDIATE' if write else 'BEGIN')
    try:
        yield
        db.commit()
    except BaseException:
        db.rollback()
        raise


def _normalized(sql):
    return ' '.join(sql.split()).lower()


def _objects(db):
    rows = db.execute("SELECT type,name,tbl_name,sql FROM sqlite_master WHERE sql IS NOT NULL").fetchall()
    return {row[1]: _normalized(row[3]) for row in rows if row[2] in TABLES
            or row[1].startswith('storage_') or row[1] == 'hybrid_schema_migrations'}


def _installed(db, *, check_foreign_keys=True):
    expected = {}
    for sql in DDL:
        words = sql.split()
        expected[words[2]] = _normalized(sql)
    actual = _objects(db)
    if not actual:
        return False
    if actual != expected:
        raise RegistryError("Registry shape/index/trigger mismatch; no automatic repair")
    rows = db.execute('SELECT version,checksum FROM hybrid_schema_migrations').fetchall()
    if [tuple(row) for row in rows] != [(1, CHECKSUM)]:
        raise RegistryError("Registry migration version/checksum mismatch")
    if check_foreign_keys:
        for table in TABLES:
            if db.execute(f'PRAGMA foreign_key_check({table})').fetchone():
                raise RegistryError("Registry has broken foreign keys")
    return True


def _jobs_ready(db):
    columns = db.execute('PRAGMA table_info(jobs)').fetchall()
    if not any(row[1] == 'job_id' and row[2].upper() == 'TEXT' and row[5] == 1 for row in columns):
        raise RegistryError("Existing jobs(job_id TEXT PRIMARY KEY) required; no controller import/backfill")


def migration_plan(db):
    connection_ready(db)
    _jobs_ready(db)
    installed = _installed(db)
    return {'version': 1, 'checksum': CHECKSUM, 'action': 'unchanged' if installed else 'create',
            'statements': [] if installed else list(DDL)}


def migrate(db):
    with transaction(db):
        _jobs_ready(db)
        if not _installed(db):
            for statement in DDL:
                db.execute(statement)
            db.execute('INSERT INTO hybrid_schema_migrations(version,checksum) VALUES(1,?)', (CHECKSUM,))
        _installed(db)
    return {'version': 1, 'checksum': CHECKSUM}


def require_schema(db, *, check_foreign_keys=False):
    connection_ready(db)
    if not _installed(db, check_foreign_keys=check_foreign_keys):
        raise RegistryError("Registry migration has not been explicitly applied")
