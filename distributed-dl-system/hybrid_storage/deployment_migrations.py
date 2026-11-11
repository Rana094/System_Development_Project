"""Explicit idle-controller migration step; backup/rehearsal first, never restore live DB."""
from contextlib import closing
import hashlib
import importlib
from pathlib import Path
import secrets
import sqlite3
import time

from . import deployment_executor as host
from . import state_backup

MIGRATIONS = ('registry_schema', 'cache_registry_schema', 'staging_schema',
              'cleanup_schema', 'checkpoint_schema', 'restore_schema')


def connect(path, mode='rw'):
    db = state_backup._connect(path, mode)
    db.execute('PRAGMA foreign_keys=ON')
    db.execute('PRAGMA synchronous=FULL')
    return db


def idle(db):
    tables = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    for required in ('jobs', 'resource_reservations', 'execution_leases'):
        if required not in tables:
            raise ValueError('Actual legacy job schema prerequisite missing')
    if db.execute("SELECT 1 FROM jobs WHERE status IN ('SCHEDULED','RUNNING','CANCELLING') LIMIT 1").fetchone():
        raise ValueError('Active controller job prevents migration')
    if db.execute('SELECT 1 FROM resource_reservations LIMIT 1').fetchone():
        raise ValueError('Outstanding reservation prevents migration')
    if db.execute('SELECT 1 FROM execution_leases WHERE revoked=0 AND expires_at>? LIMIT 1', (time.time(),)).fetchone():
        raise ValueError('Unexpired execution lease prevents migration')
    if 'hybrid_staging_attempts' in tables and db.execute("SELECT 1 FROM hybrid_staging_attempts WHERE phase!='TERMINAL' LIMIT 1").fetchone():
        raise ValueError('Nonterminal staging attempt prevents migration')


def legacy_fingerprint(db, names):
    result = {}
    for name in names:
        quoted = '"' + name.replace('"', '""') + '"'
        rows = hashlib.sha256()
        count = 0
        for row in db.execute('SELECT * FROM ' + quoted):
            rows.update(repr(tuple(row)).encode('utf-8'))
            rows.update(b'\n')
            count += 1
        result[name] = dict(rows=count, content_sha256=rows.hexdigest(),
                           sql=db.execute('SELECT sql FROM sqlite_master WHERE type=\'table\' AND name=?', (name,)).fetchone()[0])
    return result


def migrate(plan, receipt, *, services=host.service_properties):
    app = host.validate_plan(plan)
    if plan['role'] != 'controller':
        raise ValueError('SQLite migrations are controller-only')
    host.approved(plan, receipt, 'migrate')
    host.quiescent(plan, services=services)
    modules = []
    for name in MIGRATIONS:
        filename = 'hybrid_storage/' + name + '.py'
        expected = plan['files'][filename]
        actual = host.record(app / filename)
        module = importlib.import_module('hybrid_storage.' + name)
        if ({k: actual[k] for k in ('bytes', 'sha256')} != expected
                or host.digest(Path(module.__file__).read_bytes()) != expected['sha256']):
            raise ValueError('Installed/imported migration source differs from reviewed bytes')
        modules.append(module)
    dbpath = app / 'controller/jobs.db'
    state_backup._attest_source(dbpath)
    deadline = time.monotonic() + 120
    with host.lock(plan) as base:
        tx = base / 'snapshots' / (plan['snapshot_id'] + '-' + plan['host'])
        host.private(tx)
        if host.source._read(tx, 'plan.json') != host.encoded(plan):
            raise ValueError('Source snapshot plan differs')
        if host.decode(host.source._read(tx, 'INSTALLED.json')) != {'plan_sha256': host.digest(host.encoded(plan))}:
            raise ValueError('Complete source installation required')
        if (tx / 'ROLLED_BACK.json').exists():
            raise ValueError('Cannot migrate a rolled-back release')
        for name in plan['files']:
            if host.record(app / name) != host.wanted(plan, name):
                raise ValueError('Source drift before migration')
        with closing(connect(dbpath, 'ro')) as db:
            idle(db)
            state_backup._validate(db)
            if (tx / 'MIGRATED.json').exists():
                completed = host.decode(host.source._read(tx, 'MIGRATED.json'))
                if state_backup._fingerprint(db, deadline) != completed['after']:
                    raise ValueError('DB changed after completed migration; new review required')
                return dict(status='ALREADY_MIGRATED', database_restored=False)
        recovery = tx / 'database-recovery'
        if not recovery.exists():
            state_backup.backup(dbpath, recovery)
        manifest = host.decode(host.source._read(recovery, 'backup.json'))
        if not manifest['restored_copy_verified'] or manifest['live_restore']:
            raise ValueError('Verified recovery copies required')
        for name, expected in manifest['files'].items():
            with (recovery / name).open('rb') as stream:
                if hashlib.file_digest(stream, 'sha256').hexdigest() != expected['sha256']:
                    raise ValueError('Recovery copy drift')
        rehearsal = tx / ('rehearsal-' + secrets.token_hex(12) + '.sqlite')
        host.write_new(rehearsal, b'')
        # immutable=1 is ONLY for this verified CLOSED private backup, never the
        # live database. It prevents read-only WAL opens from changing its SHM.
        # The backup helper closed both destination writers before hashing them.
        for name in ('snapshot.sqlite-wal', 'restored.sqlite-wal'):
            sidecar = recovery / name
            if sidecar.exists() and sidecar.stat().st_size:
                raise ValueError('Closed recovery copy unexpectedly has uncheckpointed WAL')
        snapshot_uri = state_backup._uri(recovery / 'restored.sqlite', 'ro') + '&immutable=1'
        with closing(sqlite3.connect(snapshot_uri, uri=True)) as src, closing(connect(rehearsal)) as copy:
            src.backup(copy, pages=128, progress=lambda *_: state_backup._check_deadline(deadline))
            names = [r[0] for r in copy.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
            original = legacy_fingerprint(copy, names)
            version = copy.execute('PRAGMA user_version').fetchone()[0]
            for module in modules:
                module.migrate(copy)
            first = state_backup._fingerprint(copy, deadline)
            for module in modules:
                module.migrate(copy)
            if first != state_backup._fingerprint(copy, deadline) or original != legacy_fingerprint(copy, names):
                raise ValueError('Migration is not idempotent/legacy-compatible')
            if copy.execute('PRAGMA user_version').fetchone()[0] != version:
                raise ValueError('Legacy user_version changed')
            state_backup._validate(copy)
        host.approved(plan, receipt, 'migrate')
        host.quiescent(plan, services=services)
        with closing(connect(dbpath)) as db:
            db.set_progress_handler(lambda: int(time.monotonic() > deadline), 1000)
            idle(db)
            if legacy_fingerprint(db, names) != original or db.execute('PRAGMA user_version').fetchone()[0] != version:
                raise ValueError('Original controller rows changed since recovery point')
            host.append(tx, dict(event='MIGRATION_INTENT', sequence=list(MIGRATIONS)))
            for module in modules:
                module.migrate(db)
            if original != legacy_fingerprint(db, names) or db.execute('PRAGMA user_version').fetchone()[0] != version:
                raise ValueError('Legacy data changed during migration; retain evidence, no automatic DB restore')
            state_backup._validate(db)
            after = state_backup._fingerprint(db, deadline)
        host.write_new(tx / 'MIGRATED.json', host.encoded(dict(after=after, legacy_preserved=True,
                                                           rehearsed_twice=True, database_restored=False)))
        host.append(tx, dict(event='MIGRATION_DONE'))
        return dict(status='MIGRATED_SERVICES_STOPPED', legacy_preserved=True,
                    rehearsed_twice=True, database_restored=False, recovery_directory=str(recovery))
