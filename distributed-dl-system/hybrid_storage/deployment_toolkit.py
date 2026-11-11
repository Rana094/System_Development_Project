"""Offline all-host install kit, driven by fresh read-only host inventories.

The output is reviewable executable tooling, not readiness or permission to apply.
Unit/config changes are explicitly zero; legacy/OFF/resource is preserved.
"""
import argparse
import json
import os
from pathlib import Path
import secrets
from datetime import datetime, timezone

from . import delivery_plan as delivery
from . import deployment_executor as host
from . import deployment_review as source


def requests(*, root=source.ROOT, workers=None):
    root = host.absolute(root)
    _, audit = delivery._audit(root)
    workers = host.selected_workers(workers)
    source.review(workers, root=root)
    result = {}
    for name in ('controller', *workers):
        observed = audit['hosts'][name]['observed']
        role = 'controller' if name == 'controller' else 'worker'
        units = ['dl-student-portal.service', 'dl-dispatcher.service'] if role == 'controller' else ['dl-node-agent.service']
        roots = {observed['services'][u]['properties']['WorkingDirectory'] for u in units}
        if len(roots) != 1:
            raise ValueError('Service roots differ')
        result[name] = dict(host=name, application_root=roots.pop(),
                            hostname=observed['hostname'],
                            uid=observed['uid'], gid=observed['gid'],
                            files=list(source.catalog(role, root=root)))
    return result


def inspect_program(request, *, root=source.ROOT):
    """Standalone stdlib read-only inspector, suitable for reviewed SSH stdin."""
    code = 'import sys, types\nfrom pathlib import Path\np=types.ModuleType("hybrid_storage");p.__path__=[];sys.modules["hybrid_storage"]=p\n'
    for name in ('nfs_mount', 'mount_identity', 'backends', 'config', 'deployment_review', 'deployment_executor'):
        module = 'hybrid_storage.' + name
        raw = source._read(root, 'hybrid_storage/' + name + '.py').decode()
        code += 'm=types.ModuleType(' + repr(module) + ');m.__package__="hybrid_storage";m.__file__="/portable/hybrid_storage/' + name + '.py";sys.modules[' + repr(module) + ']=m\n'
        code += 'exec(compile(' + repr(raw) + ',' + repr(module) + ',"exec"),m.__dict__)\n'
    code += 'import json\nprint(json.dumps(m.inspect(' + repr(request) + '),indent=2))\n'
    return code.encode()


def inventories(raw, required):
    data = host.decode(raw)
    if set(data) != set(required):
        raise ValueError('Fresh inventories must match the exact selected hosts')
    import time
    for name, request in required.items():
        observed = data[name]
        if (observed.get('schema_version') != 1 or observed.get('host') != name
                or observed.get('role') != ('controller' if name == 'controller' else 'worker')
                or observed.get('application_root') != request['application_root']
                or observed.get('hostname') != request['hostname']
                or (observed.get('uid'), observed.get('gid')) != (request['uid'], request['gid'])
                or set(observed.get('files', {})) != set(request['files'])
                or observed.get('database_opened') is not False or observed.get('changes') is not False
                or not 0 <= time.time_ns() - observed.get('inspected_at_ns', 0) <= 3600 * 10**9):
            raise ValueError('Stale/incomplete/mismatched host inventory: ' + name)
    return data


LAUNCHER = b'''"""Run only from a protected reviewed kit; no automatic deployment."""
from pathlib import Path
import sys
sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent / 'payload'))
from hybrid_storage.deployment_executor import main
raise SystemExit(main())
'''


def verify(output, *, root=source.ROOT, workers=None):
    output = host.absolute(output)
    host.private(output)
    manifest = host.decode(source._read(output, 'KIT.json'))
    selected = host.selected_workers(manifest['selected_workers'])
    if (manifest['kind'] not in ('all-three-reviewed-source-install-kit', 'selected-workers-reviewed-source-install-kit')
            or workers is not None and selected != host.selected_workers(workers)):
        raise ValueError('Invalid or mismatched worker scope')
    paths = list(output.rglob('*'))
    if any(p.is_symlink() for p in paths):
        raise ValueError('Symlink in private kit')
    actual_names = {str(p.relative_to(output)) for p in paths if not p.is_dir()}
    if actual_names != set(manifest['files']) | {'KIT.json'}:
        raise ValueError('Unexpected/missing kit files')
    for name, expected in manifest['files'].items():
        raw = source._read(output, str(host.relative(name)))
        if delivery._digest(raw) != expected:
            raise ValueError('Kit file drift: ' + name)
    for role in source.ROLES:
        if source.catalog(role, root=root) != manifest['source_catalogs'][role]:
            raise ValueError('Checkout source changed; prepare a new review')
    for name in ('controller', *selected):
        plan = host.decode(source._read(output, name + '/host-plan.json'))
        host.validate_plan(plan)
        if host.selected_workers(plan.get('selected_workers')) != selected:
            raise ValueError('Host plan differs from kit worker scope')
        if plan['baseline'] != manifest['inventory'][name]:
            raise ValueError('Host baseline drift')
        if plan['files'] != manifest['source_catalogs'][plan['role']]:
            raise ValueError('Host payload catalog differs')
        host.payload(plan, output / name / 'payload')
    return dict(status='VERIFIED_INSTALL_TOOLKIT_NOT_APPLIED', executor_implemented=True,
                selected_workers=selected, deferred_workers=[n for n in delivery.WORKERS if n not in selected], apply_ready=False,
                hybrid_activation=False, kit_sha256=host.digest(source._read(output, 'KIT.json')),
                snapshot_id=manifest['snapshot_id'], live_readiness_verified=False)


def prepare(output, inventory_raw, *, root=source.ROOT, workers=None):
    output = host.absolute(output)
    workers = host.selected_workers(workers)
    required = requests(root=root, workers=workers)
    observed = inventories(inventory_raw, required)
    if output.exists():
        result = verify(output, root=root, workers=workers)
        marker = host.decode(source._read(output, 'KIT.json'))
        if marker['inventory'] != observed:
            raise ValueError('Inventory differs; preserve old kit, select new output')
        return result
    _, audit = delivery._audit(root)
    snapshot = 'snap-' + datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '-' + secrets.token_hex(8)
    catalogs = {role: source.catalog(role, root=root) for role in source.ROLES}
    host.mkdir(output)
    host.private(output)
    files = {}
    def put(name, raw):
        target = output / name
        host.ensure_directory(target.parent)
        host.write_new(target, raw)
        files[name] = delivery._digest(raw)
    operations = []
    gates = [b['id'] for b in audit['blockers'] if b['id'] != 'B3' or 'pi01' in workers]
    deferred = [n for n in delivery.WORKERS if n not in workers]
    for name in ('controller', *workers):
        role = observed[name]['role']
        plan = dict(schema_version=1, kind='reviewed-host-source-install', host=name, role=role,
                    runtime_profile='legacy-off-resource', hybrid_activation=False,
                    application_root=observed[name]['application_root'], snapshot_id=snapshot,
                    files=catalogs[role], baseline=observed[name],
                    unit_changes=[], config_changes=[], database_restore=False,
                    selected_workers=workers, deferred_workers=deferred,
                    required_gate_ids=gates,
                    deferred_gate_ids=['B3'] if 'pi01' not in workers else [],
                    install_kit_does_not_resolve_readiness=True)
        host.validate_plan(plan)
        put(name + '/host-plan.json', host.encoded(plan))
        put(name + '/run.py', LAUNCHER)
        for filename in catalogs[role]:
            raw = source._read(root, filename)
            if delivery._digest(raw) != catalogs[role][filename]:
                raise ValueError('Source changed while preparing')
            put(name + '/payload/' + filename, raw)
        approval = dict(schema_version=1, plan_sha256=host.digest(host.encoded(plan)), host=name,
                        action='install', operator_approval=False, issued_at_ns=0, expires_at_ns=0,
                        selected_workers=workers, local_private_data_only=True,
                        maintenance_and_descendants_quiescent=False,
                        resolved_gate_evidence={gate: 'UNRESOLVED' for gate in gates})
        put(name + '/approval.example.json', host.encoded(approval))
        units = list(observed[name]['services'])
        prefix = ['systemctl', '--user'] if role == 'controller' else ['sudo', '--', 'systemctl']
        executable_paths = {host.exec_identity(p['ExecStart'])['path'] for p in observed[name]['services'].values()}
        if len(executable_paths) != 1:
            raise ValueError('Service interpreters differ; explicit review required')
        executable = executable_paths.pop()
        destination = (Path(observed[name]['application_root']) / '.hybrid-deploy/kits' / snapshot / name
                       if role == 'controller' else Path('/var/lib/distributed-ml-delivery') / snapshot / name)
        argv = [executable, '-I', '-B', str(destination / 'run.py')]
        args = ['--plan', str(destination / 'host-plan.json')]
        operations.append(dict(host=name, service_changes='NONE: preserve exact unit/ExecStart/User/Group/drop-ins and enablement',
                               config_changes='NONE: preserve existing config and credentials; legacy/OFF/resource only',
                               protected_kit_directory=str(destination), interpreter=executable,
                               dry_run_argv=argv + ['dry-run'] + args + ['--payload', str(destination / 'payload')],
                               install_argv=(['sudo', '--'] if role == 'worker' else []) + argv + ['install'] + args + ['--payload', str(destination / 'payload'), '--approval', str(destination / 'install-approval.json')],
                               migrate_argv=argv + ['migrate'] + args + ['--approval', str(destination / 'migrate-approval.json')] if role == 'controller' else None,
                               rollback_argv=(['sudo', '--'] if role == 'worker' else []) + argv + ['rollback'] + args + ['--approval', str(destination / 'rollback-approval.json')],
                               stop_argv=[prefix + ['stop', unit] for unit in units],
                               start_argv=[prefix + ['start', unit] for unit in units if observed[name]['services'][unit]['ActiveState'] == 'active'],
                               approval_required=True, execute=False))
    put('OPERATIONS.json', host.encoded(dict(schema_version=1, stop_order=['controller', *workers],
        start_order=[*workers, 'controller'], hosts=operations,
        stop_requires='Explicit later approval plus closed admission, drained jobs/leases/reservations, no descendants; never use IDLE alone',
        start_requires='Source verify, controller migration completion, reviewed acceptance/reopen procedure; never enable or reboot automatically',
        rollback_order=list(reversed(('controller', *workers))), rollback='Per-host run.py rollback with distinct exact fresh approval; retains additive DB and all data',
        private_data_only=True, nfs_routing_changes=[], deploy_during_phase14_2=False)))
    if any(source.catalog(role, root=root) != catalogs[role] for role in source.ROLES):
        raise ValueError('Source changed while preparing kit')
    manifest = dict(schema_version=1, kind='all-three-reviewed-source-install-kit' if not deferred else 'selected-workers-reviewed-source-install-kit',
                    selected_workers=workers, deferred_workers=deferred, snapshot_id=snapshot,
                    executor_implemented=True, apply_ready=False, hybrid_activation=False,
                    inventory=observed, source_catalogs=catalogs, files=files)
    host.write_new(output / 'KIT.json', host.encoded(manifest))
    return verify(output, root=root, workers=workers)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    programs = sub.add_parser('inspect-program')
    programs.add_argument('--host', choices=host.HOSTS, required=True)
    programs.add_argument('--workers', choices=delivery.WORKERS, nargs='+', default=None)
    build = sub.add_parser('prepare')
    build.add_argument('--inventory', type=Path, required=True)
    build.add_argument('--output', type=Path, required=True)
    build.add_argument('--workers', choices=delivery.WORKERS, nargs='+', default=None)
    check = sub.add_parser('verify')
    check.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == 'inspect-program':
            sys_stdout = __import__('sys').stdout
            sys_stdout.write(inspect_program(requests(workers=args.workers)[args.host]).decode())
            return 0
        if args.command == 'prepare':
            path = host.absolute(args.inventory)
            result = prepare(args.output, source._read(path.parent, path.name), workers=args.workers)
        else:
            result = verify(args.output)
    except (OSError, ValueError, KeyError, TypeError) as error:
        parser.exit(2, 'Install kit refused: ' + str(error) + '\n')
    print(json.dumps(result, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
