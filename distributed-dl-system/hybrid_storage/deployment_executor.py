"""Explicit operator-only, journaled source delivery; never enables hybrid runtime.

No import side effects. Inspect/dry-run are read-only. Install and rollback need
an exact, fresh operator approval receipt and stopped/quiescent service cgroups.
No package downloads, arbitrary scripts, unit changes, DB restore or data deletion.
"""
import argparse
from contextlib import contextmanager
import fcntl
import fnmatch
import hashlib
import json
import os
from pathlib import Path
import secrets
import re
import socket
import stat
import subprocess
import sys
import time

from .backends import _directory
from . import deployment_review as source
from .config import StorageConfig, validate_config
from .mount_identity import require_local_fd

HOSTS = ('controller', 'pi01', 'pi02', 'pi03')
MAX_PLAN = 4 * 1024**2
RESERVE = 512 * 1024**2


def selected_workers(workers=None):
    """Default all three; a smaller deployment requires explicit selection."""
    result = list(HOSTS[1:] if workers is None else workers)
    if (len(result) not in (2, 3) or len(set(result)) != len(result)
            or any(name not in HOSTS[1:] for name in result)):
        raise ValueError('Select two or three distinct known workers explicitly')
    return result


def encoded(value):
    return (json.dumps(value, sort_keys=True, indent=2, allow_nan=False) + '\n').encode()


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def decode(raw):
    def pairs(items):
        value = {}
        for key, item in items:
            if key in value:
                raise ValueError('Duplicate JSON key')
            value[key] = item
        return value
    return json.loads(raw, object_pairs_hook=pairs,
                      parse_constant=lambda _: (_ for _ in ()).throw(ValueError('Nonfinite JSON')))


def absolute(path):
    path = Path(path)
    if not path.is_absolute() or path == Path('/') or '..' in path.parts or any(ord(c) < 32 for c in str(path)):
        raise ValueError('Canonical absolute path required')
    return path


def relative(name):
    p = Path(name)
    if p.is_absolute() or '..' in p.parts or str(p) != name or not p.parts:
        raise ValueError('Unsafe relative source path')
    return p


def record(path, *, data=False):
    """No-follow parents, bounded regular single-link file, stable read."""
    path = absolute(path)
    try:
        raw = source._read(path.parent, path.name)
    except FileNotFoundError:
        return None
    with _directory(path.parent) as parent:
        fd = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                raise ValueError('Expected single-link regular file')
            with os.fdopen(os.dup(fd), 'rb') as stream:
                if stream.read(source.LIMIT + 1) != raw:
                    raise ValueError('File changed during inspection')
        finally:
            os.close(fd)
    result = dict(sha256=digest(raw), bytes=len(raw), mode=stat.S_IMODE(info.st_mode), uid=info.st_uid, gid=info.st_gid)
    if data:
        return result, raw
    return result


def write_new(path, raw, mode=0o600):
    with _directory(path.parent) as parent:
        fd = os.open(path.name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, mode, dir_fd=parent)
        with os.fdopen(fd, 'wb') as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        os.fsync(parent)


def mkdir(path):
    with _directory(path.parent) as parent:
        os.mkdir(path.name, 0o700, dir_fd=parent)
        os.fsync(parent)


def ensure_directory(path, *, owner=None):
    try:
        with _directory(path):
            return
    except FileNotFoundError:
        ensure_directory(path.parent, owner=owner)
        mkdir(path)
        if owner is not None:
            with _directory(path) as fd:
                if os.geteuid() == 0:
                    os.fchown(fd, *owner)
                os.fchmod(fd, 0o755)
                os.fsync(fd)


def private(path):
    with _directory(path) as fd:
        info = os.fstat(fd)
        if info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o700:
            raise PermissionError('Caller-owned private deployment directory required')
        require_local_fd(fd)


def service_properties(unit, role):
    prefix = ['systemctl', '--user'] if role == 'controller' else ['systemctl']
    keys = ['Id', 'ActiveState', 'SubState', 'MainPID', 'ControlGroup', 'FragmentPath',
            'DropInPaths', 'UnitFileState', 'WorkingDirectory', 'ExecStart', 'User', 'Group']
    done = subprocess.run(prefix + ['show', unit, '--no-pager', '--property=' + ','.join(keys)],
                          capture_output=True, text=True, timeout=15, check=True)
    props = dict(line.split('=', 1) for line in done.stdout.splitlines() if '=' in line)
    if props.get('Id') != unit or not props.get('FragmentPath') or props.get('ActiveState') == 'not-found':
        raise ValueError('Required service not found')
    if not isinstance(exec_identity(props.get('ExecStart', '')), dict):
        raise ValueError('Unsupported systemd ExecStart representation')
    return props


def exec_identity(value):
    """Exclude volatile start/stop timestamps/PIDs from systemd command identity."""
    match = re.fullmatch(r'\{ path=(.+?) ; argv\[\]=(.+?) ; ignore_errors=(yes|no) ; .* \}', value)
    if match:
        path, argv, errors = match.groups()
        absolute(path)
        return dict(path=path, argv=argv, ignore_errors=errors)
    return value


def inspect(request, *, services=service_properties):
    """Read host identity/source/config/unit metadata only; no database open."""
    host = request['host']
    if host not in HOSTS:
        raise ValueError('Unknown host')
    if request.get('hostname', socket.gethostname()) != socket.gethostname():
        raise ValueError('Unexpected host name')
    role = 'controller' if host == 'controller' else 'worker'
    app = absolute(request['application_root'])
    with _directory(app) as fd:
        require_local_fd(fd)
        info = os.fstat(fd)
        usage = os.fstatvfs(fd)
    if (info.st_uid, info.st_gid) != (request['uid'], request['gid']):
        raise ValueError('Application ownership differs from reviewed identity')
    units = ['dl-student-portal.service', 'dl-dispatcher.service'] if role == 'controller' else ['dl-node-agent.service']
    observed = {unit: services(unit, role) for unit in units}
    for props in observed.values():
        if props['WorkingDirectory'] != str(app):
            raise ValueError('Service application root differs')
        if props.get('DropInPaths'):
            # systemd escaped whitespace needs a reviewed parser before support.
            raise ValueError('Service drop-ins present: explicit additional review required')
    unit_files = {p['FragmentPath']: record(p['FragmentPath']) for p in observed.values()}
    if any(v is None for v in unit_files.values()):
        raise ValueError('Service unit disappeared')
    machine = record(Path('/etc/machine-id'), data=True)
    result = dict(schema_version=1, host=host, role=role, application_root=str(app),
                  hostname=socket.gethostname(), uid=info.st_uid, gid=info.st_gid,
                  machine_id_sha256=digest(machine[1]), inspected_at_ns=time.time_ns(),
                  python=list(sys.version_info[:3]), free_bytes=usage.f_bavail * usage.f_frsize,
                  services=observed, unit_files=unit_files,
                  files={name: record(app / relative(name)) for name in request['files']},
                  database_opened=False, changes=False)
    raw = source._read(app, 'config/storage.json') if record(app / 'config/storage.json') else b'{}'
    if validate_config(decode(raw)) != StorageConfig():
        raise ValueError('Only legacy/OFF/resource deployment is supported')
    result['storage_config'] = record(app / 'config/storage.json')
    return result


def validate_plan(plan):
    if (plan.get('schema_version') != 1 or plan.get('kind') != 'reviewed-host-source-install'
            or plan.get('host') not in HOSTS or plan.get('runtime_profile') != 'legacy-off-resource'
            or plan.get('hybrid_activation') is not False
            or plan.get('unit_changes') != [] or plan.get('config_changes') != []
            or plan.get('database_restore') is not False):
        raise ValueError('Unsupported installation contract')
    app = absolute(plan['application_root'])
    workers = selected_workers(plan.get('selected_workers'))
    if plan['host'] != 'controller' and plan['host'] not in workers:
        raise ValueError('Host is outside reviewed deployment scope')
    if not plan['files'] or len(plan['files']) > source.MAX_FILES:
        raise ValueError('Invalid source inventory')
    for name, entry in plan['files'].items():
        relative(name)
        # Never expand the source publication authority to data or active configs.
        patterns = source.ROLES[plan['role']]
        if not (any(Path(name).parent == Path(p).parent and fnmatch.fnmatchcase(Path(name).name, Path(p).name) for p in patterns)
                or name == 'config/runtime-requirements.txt'
                or name in {'config/storage-examples/' + n for n in source.EXAMPLES}):
            raise ValueError('Path is outside role source allowlist')
        if not isinstance(entry['bytes'], int) or not 0 <= entry['bytes'] <= source.LIMIT or len(entry['sha256']) != 64:
            raise ValueError('Invalid payload identity')
    if set(plan['files']) != set(plan['baseline']['files']):
        raise ValueError('Baseline differs from payload inventory')
    if plan['baseline']['application_root'] != str(app) or plan['baseline']['host'] != plan['host']:
        raise ValueError('Baseline host/root mismatch')
    if not plan['snapshot_id'].startswith('snap-') or not all(c.isalnum() or c == '-' for c in plan['snapshot_id']):
        raise ValueError('Invalid snapshot namespace')
    if plan['role'] != ('controller' if plan['host'] == 'controller' else 'worker'):
        raise ValueError('Host/role mismatch')
    expected_units = {'dl-student-portal.service', 'dl-dispatcher.service'} if plan['role'] == 'controller' else {'dl-node-agent.service'}
    if set(plan['baseline']['services']) != expected_units:
        raise ValueError('Exact role service set required')
    return app


def approved(plan, receipt, action):
    """A receipt is an operator attestation, not a cryptographic signature."""
    now = time.time_ns()
    workers = selected_workers(plan.get('selected_workers'))
    receipt_workers = receipt.get('selected_workers', receipt.get('all_three_workers'))
    if (receipt.get('schema_version') != 1 or receipt.get('plan_sha256') != digest(encoded(plan))
            or receipt.get('host') != plan['host'] or receipt.get('action') != action
            or receipt.get('operator_approval') is not True
            or receipt_workers != workers
            or ('all_three_workers' in receipt and
                (workers != list(HOSTS[1:]) or receipt['all_three_workers'] != workers))
            or receipt.get('local_private_data_only') is not True
            or not 0 <= now - receipt.get('issued_at_ns', 0) <= 600 * 10**9
            or not now < receipt.get('expires_at_ns', 0) <= receipt['issued_at_ns'] + 600 * 10**9
            or receipt.get('maintenance_and_descendants_quiescent') is not True):
        raise PermissionError('Exact fresh per-host/action approval and selected-worker maintenance attestation required')
    gates = receipt.get('resolved_gate_evidence', {})
    if set(gates) != set(plan['required_gate_ids']) or any(not isinstance(v, str) or len(v) != 64 or any(c not in '0123456789abcdef' for c in v) for v in gates.values()):
        raise PermissionError('Every readiness gate requires separately reviewed evidence SHA-256')


def quiescent(plan, *, services=service_properties, inventory_fresh=True):
    baseline = plan['baseline']
    if inventory_fresh and not 0 <= time.time_ns() - baseline['inspected_at_ns'] <= 24 * 3600 * 10**9:
        raise ValueError('Host inventory older than 24 hours; refresh review')
    if socket.gethostname() != baseline['hostname'] or digest(source._read(Path('/etc'), 'machine-id')) != baseline['machine_id_sha256']:
        raise ValueError('Host identity changed')
    app = absolute(plan['application_root'])
    with _directory(app) as fd:
        info = os.fstat(fd)
        require_local_fd(fd)
        if (info.st_uid, info.st_gid) != (baseline['uid'], baseline['gid']):
            raise ValueError('Application owner changed')
    if plan['role'] == 'controller' and os.geteuid() != baseline['uid']:
        raise PermissionError('Controller must run as its reviewed user, never sudo')
    if plan['role'] == 'worker' and os.geteuid() != 0:
        raise PermissionError('Worker installation requires later approved sudo')
    for unit, old in baseline['services'].items():
        props = services(unit, plan['role'])
        for key in ('FragmentPath', 'DropInPaths', 'WorkingDirectory', 'User', 'Group', 'UnitFileState'):
            if props.get(key) != old.get(key):
                raise ValueError('Service identity/configuration drift: ' + key)
        if exec_identity(props.get('ExecStart', '')) != exec_identity(old.get('ExecStart', '')):
            raise ValueError('Service identity/configuration drift: ExecStart')
        if props.get('ActiveState') != 'inactive' or props.get('MainPID') != '0':
            raise ValueError('Service must already be stopped; installer never kills jobs')
        group = props.get('ControlGroup', '')
        if group:
            group_path = absolute('/sys/fs/cgroup' + group)
            if not group_path.is_relative_to(Path('/sys/fs/cgroup')):
                raise ValueError('Invalid control group')
            # A missing inactive cgroup is empty; a populated descendant is not.
            if group_path.exists():
                for file in group_path.rglob('cgroup.procs'):
                    if file.is_symlink() or file.read_text().strip():
                        raise ValueError('Service cgroup descendants are not quiescent')
    for name, old in baseline['unit_files'].items():
        if record(Path(name)) != old:
            raise ValueError('Unit bytes/metadata changed')
    if record(app / 'config/storage.json') != baseline['storage_config']:
        raise ValueError('Active storage configuration drift')


@contextmanager
def lock(plan):
    base = absolute(plan['application_root']) / '.hybrid-deploy'
    try:
        mkdir(base)
    except FileExistsError:
        pass
    private(base)
    with _directory(base) as parent:
        fd = os.open('operation.lock', os.O_WRONLY | os.O_CREAT | os.O_NOFOLLOW, 0o600, dir_fd=parent)
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode) or os.fstat(fd).st_nlink != 1:
            raise ValueError('Invalid operation lock')
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield base
    finally:
        os.close(fd)


def publish(path, raw, metadata):
    """Atomic same-directory replacement; callers hold lock and verify preimages."""
    ensure_directory(path.parent, owner=(metadata['uid'], metadata['gid']))
    with _directory(path.parent) as parent:
        temp = '.hybrid-' + secrets.token_hex(16) + '.tmp'
        fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=parent)
        with os.fdopen(fd, 'wb') as stream:
            stream.write(raw)
            stream.flush()
            if os.geteuid() == 0:
                os.fchown(stream.fileno(), metadata['uid'], metadata['gid'])
            elif (metadata['uid'], metadata['gid']) != (os.geteuid(), os.getegid()):
                raise PermissionError('Cannot restore differing ownership without root')
            os.fchmod(stream.fileno(), metadata['mode'])
            os.fsync(stream.fileno())
        os.replace(temp, path.name, src_dir_fd=parent, dst_dir_fd=parent)
        os.fsync(parent)


def append(tx, value):
    with _directory(tx) as parent:
        fd = os.open('journal.jsonl', os.O_WRONLY | os.O_APPEND | os.O_NOFOLLOW, dir_fd=parent)
        with os.fdopen(fd, 'ab') as stream:
            stream.write(encoded(value).replace(b'\n', b'') + b'\n')
            stream.flush()
            os.fsync(stream.fileno())


def wanted(plan, name):
    old = plan['baseline']['files'][name]
    return {**plan['files'][name], 'mode': old['mode'] if old else 0o644,
            'uid': old['uid'] if old else plan['baseline']['uid'],
            'gid': old['gid'] if old else plan['baseline']['gid']}


def payload(plan, directory):
    private(directory)
    total = 0
    for name, item in plan['files'].items():
        actual = record(directory / relative(name))
        if actual is None or {key: actual[key] for key in ('sha256', 'bytes')} != item:
            raise ValueError('Payload content differs: ' + name)
        total += item['bytes']
    if total > source.MAX_TOTAL:
        raise ValueError('Payload exceeds total bound')


def dry_run(plan, directory):
    app = validate_plan(plan)
    payload(plan, directory)
    for name, old in plan['baseline']['files'].items():
        if record(app / name) != old:
            raise ValueError('Installed baseline drift: ' + name)
    old_bytes = sum((r or {}).get('bytes', 0) for r in plan['baseline']['files'].values())
    new_bytes = sum(r['bytes'] for r in plan['files'].values())
    # Source backup + rollback temporary copies + incoming files; DB separately bounded.
    budget = 2 * old_bytes + 2 * new_bytes + RESERVE
    if plan['role'] == 'controller':
        with _directory(app / 'controller') as parent:
            fd = os.open('jobs.db', os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
        try:
            require_local_fd(fd)
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                raise ValueError('Existing single-link local controller DB required')
            db_bytes = info.st_size
        finally:
            os.close(fd)
        # Live DB may have WAL; online backup performs a second page-count check.
        budget += 4 * (db_bytes + 512 * 1024**2)
    with _directory(app) as fd:
        require_local_fd(fd)
        usage = os.fstatvfs(fd)
        free = usage.f_bavail * usage.f_frsize
    if free < budget:
        raise OSError('Insufficient conservative deployment/DB/rollback budget')
    return dict(status='DRY_RUN_ONLY', host=plan['host'], plan_sha256=digest(encoded(plan)),
                files=len(plan['files']), required_free_bytes=budget, observed_free_bytes=free,
                service_unit_changes=[], active_config_changes=[], hybrid_activation=False,
                readiness_gate_ids=plan['required_gate_ids'], mutations=False)


def install(plan, directory, receipt, *, services=service_properties):
    app = validate_plan(plan)
    approved(plan, receipt, 'install')
    quiescent(plan, services=services)
    payload(plan, directory)
    with lock(plan) as base:
        snapshots = base / 'snapshots'
        if not snapshots.exists():
            mkdir(snapshots)
        private(snapshots)
        tx = snapshots / (plan['snapshot_id'] + '-' + plan['host'])
        if not tx.exists():
            dry_run(plan, directory)
            mkdir(tx)
            mkdir(tx / 'before')
            write_new(tx / 'plan.json', encoded(plan))
            write_new(tx / 'journal.jsonl', b'')
            # No source changes until every preimage/unit/config is durably saved.
            for name, old in plan['baseline']['files'].items():
                if old is not None:
                    found, raw = record(app / name, data=True)
                    if found != old:
                        raise ValueError('Baseline changed during snapshot')
                    target = tx / 'before' / name
                    ensure_directory(target.parent)
                    write_new(target, raw)
            write_new(tx / 'service-state.json', encoded(plan['baseline']['services']))
            for name, old in plan['baseline']['unit_files'].items():
                found, raw = record(Path(name), data=True)
                if found != old:
                    raise ValueError('Service unit drift')
                write_new(tx / ('unit-' + digest(name.encode())), raw)
            if plan['baseline']['storage_config'] is not None:
                found, raw = record(app / 'config/storage.json', data=True)
                if found != plan['baseline']['storage_config']:
                    raise ValueError('Storage config drift')
                write_new(tx / 'storage-before.json', raw)
            write_new(tx / 'SNAPSHOT_COMPLETE.json', encoded({'plan_sha256': digest(encoded(plan))}))
        private(tx)
        if source._read(tx, 'plan.json') != encoded(plan) or decode(source._read(tx, 'SNAPSHOT_COMPLETE.json')) != {'plan_sha256': digest(encoded(plan))}:
            raise ValueError('Incomplete/different snapshot; retain and investigate')
        if (tx / 'ROLLED_BACK.json').exists():
            raise ValueError('Rolled-back transaction cannot be reapplied')
        # Preflight the entire set before resuming even one file.
        for name, old in plan['baseline']['files'].items():
            if record(app / name) not in (old, wanted(plan, name)):
                raise ValueError('External source edit; refuse resume: ' + name)
        if (tx / 'INSTALLED.json').exists():
            if any(record(app / name) != wanted(plan, name) for name in plan['files']):
                raise ValueError('Completed transaction drifted')
            return dict(status='ALREADY_INSTALLED', snapshot=str(tx), services_started=False)
        append(tx, dict(event='INSTALL_APPROVED', approval_sha256=digest(encoded(receipt))))
        for name in plan['files']:
            if record(app / name) == wanted(plan, name):
                continue
            approved(plan, receipt, 'install')
            quiescent(plan, services=services)
            append(tx, dict(event='INSTALL_INTENT', path=name, sha256=plan['files'][name]['sha256']))
            publish(app / name, source._read(directory, name), wanted(plan, name))
            if record(app / name) != wanted(plan, name):
                raise ValueError('Published file verification failed')
            append(tx, dict(event='INSTALL_DONE', path=name))
        write_new(tx / 'INSTALLED.json', encoded({'plan_sha256': digest(encoded(plan))}))
        return dict(status='INSTALLED_SERVICES_STOPPED', snapshot=str(tx), services_started=False)


def rollback(plan, receipt, *, services=service_properties):
    app = validate_plan(plan)
    approved(plan, receipt, 'rollback')
    quiescent(plan, services=services, inventory_fresh=False)
    with lock(plan) as base:
        tx = base / 'snapshots' / (plan['snapshot_id'] + '-' + plan['host'])
        private(tx)
        if source._read(tx, 'plan.json') != encoded(plan):
            raise ValueError('Snapshot plan differs')
        if decode(source._read(tx, 'SNAPSHOT_COMPLETE.json')) != {'plan_sha256': digest(encoded(plan))}:
            raise ValueError('Incomplete source snapshot')
        for name, old in plan['baseline']['files'].items():
            current = record(app / name)
            if current not in (old, wanted(plan, name)):
                raise ValueError('External edit; do not overwrite: ' + name)
            if old:
                saved = record(tx / 'before' / name)
                if saved is None or (saved['sha256'], saved['bytes']) != (old['sha256'], old['bytes']):
                    raise ValueError('Backup changed/missing')
        if (tx / 'ROLLED_BACK.json').exists():
            if any(record(app / name) != old for name, old in plan['baseline']['files'].items()):
                raise ValueError('Rollback completion drift')
            return dict(status='ALREADY_ROLLED_BACK', snapshot=str(tx), services_started=False)
        quarantine = tx / 'quarantine'
        if not quarantine.exists():
            mkdir(quarantine)
        for name, old in plan['baseline']['files'].items():
            if record(app / name) == old:
                continue
            approved(plan, receipt, 'rollback')
            quiescent(plan, services=services, inventory_fresh=False)
            append(tx, dict(event='ROLLBACK_INTENT', path=name))
            if old:
                publish(app / name, source._read(tx / 'before', name), old)
            else:
                dest = quarantine / name
                ensure_directory(dest.parent)
                if record(dest) is not None:
                    raise ValueError('Quarantine collision')
                with _directory((app / name).parent) as parent, _directory(dest.parent) as target:
                    os.rename(Path(name).name, dest.name, src_dir_fd=parent, dst_dir_fd=target)
                    os.fsync(parent)
                    os.fsync(target)
            append(tx, dict(event='ROLLBACK_DONE', path=name))
        write_new(tx / 'ROLLED_BACK.json', encoded({'plan_sha256': digest(encoded(plan)), 'database_restored': False}))
        return dict(status='ROLLED_BACK_SERVICES_STOPPED', snapshot=str(tx), database_restored=False, services_started=False)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    read = sub.add_parser('inspect')
    read.add_argument('--request', type=Path, required=True)
    for action in ('dry-run', 'install', 'migrate', 'rollback'):
        item = sub.add_parser(action)
        item.add_argument('--plan', type=Path, required=True)
        if action in ('dry-run', 'install'):
            item.add_argument('--payload', type=Path, required=True)
        if action != 'dry-run':
            item.add_argument('--approval', type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == 'inspect':
            result = inspect(decode(source._read(absolute(args.request).parent, args.request.name)))
        else:
            plan = decode(source._read(absolute(args.plan).parent, args.plan.name))
            if args.command == 'dry-run':
                result = dry_run(plan, absolute(args.payload))
            else:
                approval_path = absolute(args.approval)
                approval_record = record(approval_path)
                if approval_record['uid'] != os.geteuid() or approval_record['mode'] != 0o600:
                    raise PermissionError('Approval must be caller-owned mode0600')
                receipt = decode(source._read(approval_path.parent, approval_path.name))
                if args.command == 'migrate':
                    from .deployment_migrations import migrate
                    result = migrate(plan, receipt)
                else:
                    result = (install(plan, absolute(args.payload), receipt) if args.command == 'install'
                              else rollback(plan, receipt))
    except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError) as error:
        parser.exit(2, 'Deployment refused: ' + str(error) + '\n')
    print(json.dumps(result, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
