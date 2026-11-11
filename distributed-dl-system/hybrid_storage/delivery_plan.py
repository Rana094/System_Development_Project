"""Offline, all-three-Pi delivery preparation. No deployment executor exists here.

Only explicit prepare() writes, into a new private local review directory. verify()
reads it without extraction, database access, SSH, install or service operations.
A verified package is an internally consistent plan, never deployment approval.
"""
import argparse
from dataclasses import asdict
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import stat
import tarfile
from datetime import datetime, timezone

from . import deployment_review as source
from .backends import _directory
from .config import require_legacy_runtime, validate_config

ROOT = source.ROOT
WORKERS = ('pi01', 'pi02', 'pi03')
AUDIT = 'docs/storage/57-production-readiness-evidence.json'
PUBLIC_CONFIGS = ('storage.json', 'nodes.json', 'nfs-server.json', 'nfs-clients.json',
                  'execution_leases.json', 'queue_policy.json')
MIGRATIONS = ('registry_schema', 'cache_registry_schema', 'staging_schema',
              'cleanup_schema', 'checkpoint_schema', 'restore_schema')
SNAPSHOT = re.compile(r'snap-[0-9]{8}T[0-9]{6}Z-[0-9a-f]{16}\Z')
PACKAGE_LIMIT = source.MAX_TOTAL + source.MAX_FILES * 2048
EXPANDED_LIMIT = source.MAX_TOTAL + source.MAX_FILES * 4096 + 10240


class _TarBudget:
    """Bound decompression before tarfile can allocate an extended header body."""
    def __init__(self, stream):
        self.stream, self.remaining = stream, EXPANDED_LIMIT

    def read(self, size):
        if size < 0:
            raise ValueError('Unbounded archive read prohibited')
        data = self.stream.read(min(size, self.remaining + 1))
        self.remaining -= len(data)
        if self.remaining < 0:
            raise ValueError('Expanded archive exceeds review budget')
        return data


def _json(data):
    return (json.dumps(data, indent=2, sort_keys=True, allow_nan=False)+'\n').encode()


def _object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('Duplicate JSON key: '+key)
        result[key] = value
    return result


def _decode(raw):
    return json.loads(raw, object_pairs_hook=_object,
                      parse_constant=lambda _: (_ for _ in ()).throw(ValueError('Nonfinite JSON')))


def _absolute(path):
    path = Path(path)
    if (not path.is_absolute() or path == Path('/') or '..' in path.parts
            or any(ord(c) < 32 for c in str(path))):
        raise ValueError('Expected explicit canonical absolute path other than /')
    return path


def _checked_directory(path, *, private=False):
    with _directory(_absolute(path)) as fd:
        info = os.fstat(fd)
        if private and (info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o700):
            raise PermissionError('Review directory must be caller-owned mode 0700')


def _digest(data):
    return dict(bytes=len(data), sha256=hashlib.sha256(data).hexdigest())


def _write(parent, name, data):
    with _directory(parent) as fd:
        target = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                         0o600, dir_fd=fd)
        with os.fdopen(target, 'wb') as stream:
            stream.write(data); stream.flush(); os.fsync(stream.fileno())
        os.fsync(fd)


def _audit(root):
    raw = source._read(root, AUDIT); audit = _decode(raw)
    if audit.get('phase') != '14.1' or set(audit.get('hosts', {})) != {'controller', *WORKERS}:
        raise ValueError('Required controller/all-three-Pi Phase 14.1 evidence missing')
    if _absolute(audit['repository']['app_root']) != root:
        raise ValueError('Audit application root differs; obtain a new reviewed audit')
    if not audit.get('blockers') or not all(b.get('id') for b in audit['blockers']):
        raise ValueError('Expected explicit audited blockers; cannot infer readiness')
    return raw, audit


def _steps(hosts):
    return [
        dict(id='01-review', action='Re-probe all host identities, full mount tables, dependency compatibility, disk budgets and export exposure; resolve every gate; approve exact plan hash.', executable=False),
        dict(id='02-drain', action='Use reviewed admission-maintenance provider; drain jobs and staging, verify leases/reservations and all descendants quiescent on all three workers. No forced termination of user jobs.', executable=False),
        dict(id='03-snapshot', action='Create exclusive per-host snapshot/journal below planned namespace. Back up exact changed files, metadata, unit/drop-ins/config, offline environment identity and protected credential recovery. Record absent paths and original service states.', executable=False),
        dict(id='04-freeze-controller', action='After drain proof, stop portal then dispatcher using listed argv. Verify no other SQLite writers; capture LOCAL SQLite backup with sqlite3.Connection.backup, FULL/FK and bounded busy handling, verify quick_check/foreign_key_check and an isolated restore. Record matching manifests, committed checkpoints and object references.', executable=False),
        dict(id='05-stage', action='Transfer only role archive to its immutable release directory; verify digest and regular member allowlist before bounded extraction. Do not touch authoritative inputs, checkpoints, jobs, caches or active source yet. Provision pinned offline dependencies in a separate reviewed environment.', executable=False),
        dict(id='06-migrations', action='Rehearse listed additive migrations twice on a separate restored local DB copy; check row/state/lease and FK/schema invariants. After explicit DB-change approval, apply the same reviewed sequence to idle production DB; no job backfill, table repair or enum rewrite.', executable=False),
        dict(id='07-install', action='Stop idle agents only after quiescence. Journal/compare old file hashes and metadata, then publish only exact reviewed source files atomically; record intent before each operation and completion after fsync. Preserve unrelated files; proposed unit/config deltas remain unresolved and cannot be installed.', executable=False),
        dict(id='08-start', action='Start and verify pi01, pi02, pi03 agents; then portal and dispatcher using listed argv. Restore only originally enabled/active states; no enable/reboot/linger action inferred. Admission remains closed until acceptance.', executable=False),
        dict(id='09-accept', action='Run approved disposable all-three acceptance; on any failure fence new attempts and execute reviewed rollback. Never disrupt unrelated students.', executable=False),
    ]


def _document(root, snapshot_id):
    if not SNAPSHOT.fullmatch(snapshot_id):
        raise ValueError('Invalid snapshot identity')
    audit_raw, audit = _audit(root)
    review = source.review(WORKERS, root=root)
    config_raw = source._read(root, 'config/storage.json')
    settings = require_legacy_runtime(validate_config(_decode(config_raw)))
    hosts = {}
    for name in ('controller', *WORKERS):
        observed = audit['hosts'][name]['observed']
        units = ('dl-dispatcher.service', 'dl-student-portal.service') if name == 'controller' else ('dl-node-agent.service',)
        services = {}
        for unit in units:
            props = observed['services'][unit]['properties']
            app = _absolute(props['WorkingDirectory'])
            services[unit] = dict(scope='user' if name == 'controller' else 'system',
                unit_file=str(_absolute(props['FragmentPath'])),
                audited_pid=props['MainPID'], audited_state=props['ActiveState'],
                desired_unit_delta=None, unit_delta_status='UNRESOLVED_NOT_GENERATED')
        if len({observed['services'][u]['properties']['WorkingDirectory'] for u in units}) != 1:
            raise ValueError('Service roots differ; explicit layout review needed')
        host = dict(role='controller' if name == 'controller' else 'worker',
            application_root=str(app), service_user='rana' if name == 'controller' else 'pi',
            audited_uid=observed['uid'], audited_gid=observed['gid'],
            audited_free_bytes=observed['root_metadata']['free_bytes'], observations_not_live=True,
            release_root=str(app/'.hybrid-deploy/releases'),
            snapshot_directory=str(app/'.hybrid-deploy/snapshots'/snapshot_id/name),
            services=services, disk_budget=None, disk_budget_status='UNRESOLVED_NO_CAPACITY_RESERVED')
        if name != 'controller':
            alias = audit['ssh_aliases'][name]['resolved']
            endpoint = next(n for n in review['selected_workers'] if n['id'] == name)
            if alias['hostname'] != [endpoint['ip']] or alias['user'] != ['pi'] or alias['port'] != ['22']:
                raise ValueError('SSH/config/audit worker identity mismatch')
            host['ssh'] = dict(alias=name, hostname=endpoint['ip'], user='pi', port=22,
                host_key_alias=alias['hostkeyalias'][0], host_key_alias_is_current_lan=False,
                options=['BatchMode=yes','StrictHostKeyChecking=yes','UpdateHostKeys=no',
                         'ControlMaster=no','ControlPath=none','PermitLocalCommand=no'])
        hosts[name] = host
    migrations = [dict(order=i+1, module='hybrid_storage.'+m, function='migrate',
                       file='hybrid_storage/'+m+'.py', **_digest(source._read(root, 'hybrid_storage/'+m+'.py')))
                  for i,m in enumerate(MIGRATIONS)]
    role_hashes = {role: hashlib.sha256(_json(files)).hexdigest() for role, files in review['files'].items()}
    release_id = 'release-'+hashlib.sha256(_json(role_hashes)).hexdigest()[:24]
    for host in hosts.values():
        host['release_directory'] = host['release_root']+'/'+release_id+'/'+host['role']
    commands = []
    for name, host in hosts.items():
        for unit, service in host['services'].items():
            prefix = ['systemctl', '--user'] if service['scope']=='user' else ['sudo','--','systemctl']
            for action in ('stop','start'):
                commands.append(dict(host=name, scope=service['scope'], action=action, unit=unit,
                    argv=prefix+[action,unit], execute=False, approval_required=True))
    return dict(schema_version=1, kind='hybrid-delivery-dry-run', apply_ready=False,
        executor_implemented=False, all_three_pi_acceptance_proven=False, selected_workers=list(WORKERS),
        snapshot_id=snapshot_id, release_id=release_id,
        source_root=str(root), audit=dict(path=AUDIT, **_digest(audit_raw)),
        planner=dict(file='hybrid_storage/delivery_plan.py', **_digest(source._read(root,'hybrid_storage/delivery_plan.py'))),
        wrapper=dict(file='tools/hybrid_deploy.py', **_digest(source._read(root,'tools/hybrid_deploy.py'))),
        source_catalogs=review['files'], hosts=hosts, blockers=audit['blockers'],
        configuration=dict(current=asdict(settings), proposal_only=True, change_active_config=False,
            input_files={'config/'+name:_digest(source._read(root,'config/'+name)) for name in PUBLIC_CONFIGS},
            new_runtime_file=None, worker_credentials='SEPARATE_PROTECTED_PROVISIONING_REQUIRED',
            nfs_topology='pi01 route/export/client setup unresolved; do not infer IP from HostKeyAlias'),
        migrations=dict(execute=False, db_path=hosts['controller']['application_root']+'/controller/jobs.db',
            local_disk_required=True, legacy_initializer='controller/job_store.py::initialize_database',
            legacy_initializer_automatically_called=False,
            sequence=migrations, rehearse_twice_on_restored_copy=True, no_automatic_backfill=True),
        restart_plan=dict(commands_are_data_only=True, commands=commands,
            stop_order=['controller:dl-student-portal.service','controller:dl-dispatcher.service',
                        *[n+':dl-node-agent.service' for n in WORKERS]],
            start_order=[*[n+':dl-node-agent.service' for n in WORKERS],
                         'controller:dl-student-portal.service','controller:dl-dispatcher.service'],
            missing_prerequisites=['reviewed admission pause','all-job/attempt/rank drain and quiescence',
                                   'concrete trusted bootstrap/unit/config deltas','explicit later apply approval']),
        steps=_steps(hosts),
        acceptance=dict(execute=False, required_workers=list(WORKERS), tests=[
            'strict SSH/API identity and dependency/version match', 'NFS mount source/flags, authorized TEST SHA256 and RO write rejection',
            'authenticated full and incremental inventory; stale/spoofed events rejected',
            'authorized cold/warm job execution and legacy flag-OFF regression',
            'cancellation during staging; expired attempt cannot launch',
            'isolated worker fault and committed-checkpoint recovery',
            'three-rank DDP full-group stage/checkpoint/recovery and stale rank fencing',
            'student/admin data boundaries and service boot under reviewed isolation']),
        rollback=dict(automatic_executor=False, retain_all_user_data=True, retained_additive_schema_preferred=True,
            steps=['Close admission; fence new attempts and prove descendants quiescent before releasing pins or slots.',
                   'Stop only reviewed affected services, record failure evidence and service states.',
                   'For each journaled source/config/unit path compare installed release hash before restoring saved bytes/metadata; on external changes stop for review.',
                   'Quarantine release-created source files outside import paths; never remove arbitrary paths, jobs, cache objects or checkpoint data.',
                   'Restore prior unit/drop-in/offline environment/config identities; keep legacy/OFF/resource as approved fallback.',
                   'Keep additive schema only if old-version regression was proven. Never down-migrate or blindly replace live DB. Any DB restore needs stopped writers, separately retained failed state, matching manifest/checkpoint/object recovery point and explicit approval.',
                   'Restore originally active/enabled service states without enabling new services; verify legacy health before reopening admission.']),
        idempotency=dict(review='Existing complete output is verified and reused byte-for-byte; any drift or partial output fails without writes.',
            future_apply='Exclusive per-snapshot lock and intent/completion journal; skip only exact verified files/schema versions, never refresh a backup or replay a restart blindly.',
            incomplete_output='Retain for diagnosis; select a NEW output/snapshot, no overwrite or cleanup'),
        prohibited=['network or subprocess execution by this tool','sudo/remote config/service/mount changes',
                    'automatic package download','copying secrets or user data into source archives','claiming audit data is fresh readiness'])


def _runbook(plan):
    lines = ['# Generated delivery review — NOT executable / NOT approved', '',
        'Release: `'+plan['release_id']+'`; snapshot namespace: `'+plan['snapshot_id']+'`.',
        'All three Pis are mandatory. apply_ready=false; no deployment executor is provided.', '',
        '## Ordered gates and operations', '']
    for step in plan['steps']:
        lines.append(step['id']+': '+step['action']+'\n')
    lines += ['## Host-specific destinations and service argv', '']
    for name, host in plan['hosts'].items():
        lines += [name+':', '- source release: `'+host['release_directory']+'`',
                  '- unique backup/journal: `'+host['snapshot_directory']+'`',
                  '- audited service UID:GID: '+str(host['audited_uid'])+':'+str(host['audited_gid'])+'; fresh verification required.', '']
    lines += ['Commands below are JSON argv DATA, not a shell script; DO NOT RUN until gates and later approval are satisfied.', '']
    for command in plan['restart_plan']['commands']:
        lines.append(command['host']+': `'+json.dumps(command['argv'])+'`')
    lines += ['', '## DB plan (no DB was opened)', '',
              'Dedicated LOCAL FULL/FK connection; preserve existing job states. Rehearse every migration twice on a restored copy.']
    lines += ['- '+item['module']+'.migrate(db), source SHA256 '+item['sha256'] for item in plan['migrations']['sequence']]
    lines += ['', '## Rollback after failed acceptance', '']+plan['rollback']['steps']
    lines += ['', '## Open blockers', '']+[b['id']+': '+b['issue'] for b in plan['blockers']]
    return ('\n'.join(lines)+'\n').encode()


def _payloads(plan, root):
    return {'plan.json':_json(plan), 'RUNBOOK.md':_runbook(plan),
            'storage-current.json':_json(plan['configuration']['current']),
            'storage-opt-in.example.json':_json(dict(storage_backend='nfs', worker_cache_enabled=True,
                storage_aware_staging=True, scheduler_policy='resource')),
            'nodes-current.json':source._read(root,'config/nodes.json')}


def _archive(output, role, entry, files):
    name=entry['filename']
    if name != role+'-source.tar.gz':
        raise ValueError('Unexpected archive filename')
    with _directory(output/'source') as parent:
        fd=os.open(name,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK,dir_fd=parent)
    with os.fdopen(fd,'rb') as stream:
        before=os.fstat(stream.fileno())
        if (not stat.S_ISREG(before.st_mode) or before.st_nlink!=1 or before.st_size>PACKAGE_LIMIT
                or stat.S_IMODE(before.st_mode)!=0o600):
            raise ValueError('Unsafe/big review archive')
        actual=hashlib.file_digest(stream,'sha256').hexdigest()
        if (before.st_size,actual)!=(entry['bytes'],entry['sha256']):
            raise ValueError('Archive digest/size differs')
        stream.seek(0);seen=set()
        with gzip.GzipFile(fileobj=stream,mode='rb') as expanded, \
                tarfile.open(fileobj=_TarBudget(expanded),mode='r|') as archive:
            for member in archive:
                if (member.name not in files or member.name in seen or member.type!=tarfile.REGTYPE
                        or member.size!=files[member.name]['bytes'] or member.size>source.LIMIT
                        or member.mode!=0o644):
                    raise ValueError('Unexpected/unsafe archive member')
                seen.add(member.name)
                with archive.extractfile(member) as data:
                    if _digest(data.read(source.LIMIT+1))!=files[member.name]:
                        raise ValueError('Archive member content mismatch')
        if seen!=set(files):raise ValueError('Archive source member missing')
        after=os.fstat(stream.fileno())
        if (before.st_size,before.st_mtime_ns,before.st_ctime_ns)!=(after.st_size,after.st_mtime_ns,after.st_ctime_ns):
            raise ValueError('Archive changed while verifying')


def verify(output, *, root=ROOT):
    """Verify exact local source/config/audit/package bytes; never attest live hosts."""
    root=_absolute(root);output=_absolute(output)
    _checked_directory(output,private=True)
    _checked_directory(output/'source',private=True)
    marker=_decode(source._read(output,'delivery.json'))
    stored=_decode(source._read(output,'plan.json'))
    expected=_document(root,stored['snapshot_id'])
    if stored!=expected:raise ValueError('Plan differs from current source/config/audit or mandatory gates')
    payloads=_payloads(expected,root)
    for name,data in payloads.items():
        if source._read(output,name)!=data:raise ValueError('Changed plan payload: '+name)
    reviewed=_decode(source._read(output,'source/review.json'))
    base={k:v for k,v in reviewed.items() if k!='archives'}
    if base!=source.review(WORKERS,root=root):raise ValueError('Source review changed')
    if set(reviewed.get('archives',{}))!=set(source.ROLES):raise ValueError('Missing role archive')
    for role,entry in reviewed['archives'].items():
        _archive(output,role,entry,expected['source_catalogs'][role])
    manifest={name:_digest(data) for name,data in payloads.items()}
    manifest['source/review.json']=_digest(source._read(output,'source/review.json'))
    for role,entry in reviewed['archives'].items():
        manifest['source/'+entry['filename']]={k:entry[k] for k in ('bytes','sha256')}
    wanted=dict(schema_version=1,status='COMPLETE_DRY_RUN_ONLY',apply_ready=False,
                files=manifest,plan_sha256=manifest['plan.json']['sha256'])
    if marker!=wanted:raise ValueError('Delivery completion marker mismatch')
    if set(os.listdir(output))!=set(payloads)|{'source','delivery.json'}:
        raise ValueError('Unexpected files in private review directory')
    if set(os.listdir(output/'source'))!={'review.json','worker-source.tar.gz','controller-source.tar.gz'}:
        raise ValueError('Unexpected source review files')
    return dict(verified=True, apply_ready=False, executor_implemented=False,
                live_readiness_verified=False, plan_sha256=marker['plan_sha256'],
                release_id=expected['release_id'],snapshot_id=expected['snapshot_id'],
                selected_workers=list(WORKERS),blocker_ids=[b['id'] for b in expected['blockers']])


def prepare(output, *, root=ROOT):
    """Idempotently prepare a private review; no backup of live runtime data occurs."""
    root=_absolute(root);output=_absolute(output)
    # Validate all inputs before creating any output; no root/DB/config mutation.
    snapshot='snap-'+datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-'+secrets.token_hex(8)
    plan=_document(root,snapshot)
    with _directory(output.parent) as parent:
        try:os.mkdir(output.name,mode=0o700,dir_fd=parent)
        except FileExistsError:return verify(output,root=root)
        os.fsync(parent)
    _checked_directory(output,private=True)
    reviewed=source.build(WORKERS,output/'source',root=root)
    payloads=_payloads(plan,root)
    for name,data in payloads.items():_write(output,name,data)
    # Re-read inputs after building; no completed package may mix source versions.
    if _document(root,snapshot)!=plan:raise ValueError('Inputs changed during preparation')
    manifest={name:_digest(data) for name,data in payloads.items()}
    manifest['source/review.json']=_digest(source._read(output,'source/review.json'))
    for role,entry in reviewed['archives'].items():
        manifest['source/'+entry['filename']]={k:entry[k] for k in ('bytes','sha256')}
    _write(output,'delivery.json',_json(dict(schema_version=1,status='COMPLETE_DRY_RUN_ONLY',
        apply_ready=False,files=manifest,plan_sha256=manifest['plan.json']['sha256'])))
    return verify(output,root=root)


def main(argv=None, *, root=ROOT):
    parser=argparse.ArgumentParser(description=__doc__)
    commands=parser.add_subparsers(dest='command',required=True)
    for command in ('plan','verify','apply'):
        sub=commands.add_parser(command)
        sub.add_argument('--output',required=True,type=Path,help='Absolute private local delivery review directory')
    args=parser.parse_args(argv)
    # Refuse before reading a package: even tampering apply_ready cannot activate anything.
    if args.command=='apply':
        print(json.dumps(dict(status='REFUSED',apply_ready=False,
            reason='No deployment executor: unresolved 14.1 gates, reviewed concrete runtime/unit/config changes and later explicit approval required.')))
        return 3
    try:
        result=(prepare if args.command=='plan' else verify)(args.output,root=root)
    except (OSError,ValueError,KeyError,TypeError,tarfile.TarError) as error:
        parser.exit(2,'Delivery preparation refused: '+str(error)+'\n')
    print(json.dumps(result,indent=2,sort_keys=True));return 0


if __name__=='__main__':raise SystemExit(main())
