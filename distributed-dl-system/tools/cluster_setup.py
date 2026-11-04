"""Install trusted Linux workers and controller services without changing scheduler code."""
import argparse
from contextlib import contextmanager
import fcntl
import hashlib
import io
import ipaddress
import json
import os
from pathlib import Path
import re
import shlex
import socket
import sqlite3
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
INVENTORY = ROOT / 'config/nodes.json'


def run(args, **kwargs):
    return subprocess.run(args, check=True, **kwargs)


def unit_quote(value):
    """Quote systemd values, including paths containing spaces and literal percent signs."""
    value = str(value)
    if any(ord(c) < 32 for c in value) or '$' in value:
        raise ValueError('Service paths must not contain control characters or dollar signs')
    return '"' + value.replace('\\', '\\\\').replace('"', '\\"').replace('%', '%%') + '"'


def validate_node(node):
    # The logical ID is also an operator-configured SSH alias, never a shell expression.
    if not re.fullmatch(r'[a-zA-Z0-9][a-zA-Z0-9_-]{0,62}', node['id']) or node['id'] == 'local':
        raise ValueError('Use a safe SSH alias as the worker ID (not local)')
    address = ipaddress.ip_address(node['ip'])
    if address.version != 4 or address.is_unspecified or address.is_multicast or address.is_loopback:
        raise ValueError('Use the reachable LAN IPv4 address of the worker')
    if type(node['port']) is not int or not 1 <= node['port'] <= 65535:
        raise ValueError('Worker port must be between 1 and 65535')
    root = node.get('worker_root', 'distributed-dl-system')
    if not root or root in {'.', '/'} or '..' in Path(root).parts or root.startswith('~'):
        raise ValueError('Worker root must be an absolute path or a path relative to its SSH home')
    unit_quote(root)
    unit_quote(node.get('worker_python', '.venv/bin/python'))
    if node.get('interface') and not re.fullmatch(r'[A-Za-z0-9_.:-]+', node['interface']):
        raise ValueError('Invalid network interface')
    return node


def load_nodes(path=INVENTORY):
    nodes = json.loads(path.read_text())['nodes']
    ids, addresses = set(), set()
    for node in nodes:
        validate_node(node)
        endpoint = (node['ip'], node['port'])
        if node['id'] in ids or endpoint in addresses:
            raise ValueError('Duplicate worker ID or address/port')
        ids.add(node['id'])
        addresses.add(endpoint)
    return nodes


def register(node, path=INVENTORY):
    """Replace one verified inventory entry atomically; keep unrelated node settings."""
    validate_node(node)
    with path.with_suffix('.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        document = json.loads(path.read_text()) if path.exists() else {'nodes': []}
        nodes = document['nodes']
        for old in nodes:
            if old['id'] != node['id'] and (old['ip'], old['port']) == (node['ip'], node['port']):
                raise ValueError('That endpoint is already registered under another worker ID')
        document['nodes'] = [dict(old, **node) if old['id'] == node['id'] else old for old in nodes]
        if not any(old['id'] == node['id'] for old in nodes):
            document['nodes'].append(node)
        fd, name = tempfile.mkstemp(dir=path.parent)
        try:
            with os.fdopen(fd, 'w') as out:
                json.dump(document, out, indent=2)
                out.write('\n')
                out.flush()
                os.fsync(out.fileno())
            os.replace(name, path)
            descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        finally:
            Path(name).unlink(missing_ok=True)


def assert_no_active_jobs(root=ROOT):
    database = root / 'controller/jobs.db'
    if not database.exists():
        return
    with sqlite3.connect(database.as_uri() + '?mode=ro', uri=True) as db:
        active = db.execute("SELECT job_id FROM jobs WHERE status IN ('SCHEDULED','RUNNING','CANCELLING')").fetchall()
        if active or db.execute('SELECT 1 FROM resource_reservations LIMIT 1').fetchone():
            raise ValueError('Active jobs/reservations exist; finish or cancel them before maintenance')


@contextmanager
def maintenance(root=ROOT):
    """Exclude both dispatch and background environment preparation during worker updates."""
    handles = []
    try:
        for path in (root / 'controller/.dispatcher.lock', root / 'storage/portal/.server.lock'):
            path.parent.mkdir(parents=True, exist_ok=True)
            handle = path.open('a')
            handles.append(handle)
            try:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise ValueError('Stop the dispatcher and student portal before worker installation') from None
        assert_no_active_jobs(root)
        yield
    finally:
        for handle in handles:
            handle.close()


def bundle(root=ROOT):
    """Copy only executable source and service configuration, never controller or runtime data."""
    paths = sorted([*root.glob('agent/*.py'), *root.glob('training/*.py'),
                    *root.glob('training/reference/*.py'),
                    root / 'config/execution_leases.json', root / 'config/runtime-requirements.txt'])
    stream = io.BytesIO()
    hashes = {}
    with tarfile.open(fileobj=stream, mode='w:gz') as archive:
        for path in paths:
            name = str(path.relative_to(root))
            hashes[name] = hashlib.sha256(path.read_bytes()).hexdigest()
            archive.add(path, arcname=name, recursive=False)
    return stream.getvalue(), hashes


def probe(node):
    def get(route):
        with urllib.request.urlopen(f"http://{node['ip']}:{node['port']}/{route}", timeout=8) as response:
            return json.load(response)
    health = get('health')
    resources = get('resources')
    if health.get('node') != node['id'] or resources.get('node') != node['id']:
        raise ValueError(f"Worker identity does not match {node['id']}")
    sys.path.insert(0, str(ROOT / 'agent'))
    import execution_lease
    if health.get('execution_ownership', {}).get('protocol') != execution_lease.PROTOCOL:
        raise ValueError('Worker does not support the current execution ownership protocol')
    skew = abs(time.time() - resources['timestamp'])
    if skew > 2:
        raise ValueError(f'Worker clock differs by {skew:.2f}s; enable time synchronization')
    return {'id': node['id'], 'address': node['ip'], 'state': resources['node_state'],
            'architecture': health.get('architecture'), 'python': health.get('python'),
            'clock_skew_seconds': round(skew, 3), 'memory_gb': resources['memory']['total_gb'],
            'interface': resources['network']['interface']}


# This program executes as the SSH user. Only system service installation uses sudo.
# It keeps one previous managed source snapshot, leaving checkpoints and caches untouched.
REMOTE_INSTALL = r'''
import compileall, hashlib, io, json, os, pathlib, pwd, shutil, subprocess, sys, tarfile, tempfile, urllib.request
payload = json.loads(sys.argv[1])
node = payload['node']
root = pathlib.Path(node.get('worker_root', 'distributed-dl-system')).expanduser().absolute()
root.mkdir(parents=True, exist_ok=True)
def run(args):
    subprocess.run(args, check=True)
def quote(s):
    return '"' + str(s).replace('\\','\\\\').replace('"','\\"').replace('%','%%') + '"'
unit_path = pathlib.Path('/etc/systemd/system/dl-node-agent.service')
old_unit = unit_path.read_bytes() if unit_path.exists() else None
was_active = subprocess.run(['systemctl','is-active','--quiet','dl-node-agent']).returncode == 0
was_enabled = subprocess.run(['systemctl','is-enabled','--quiet','dl-node-agent'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL).returncode == 0
if was_active:
    with urllib.request.urlopen('http://127.0.0.1:%s/resources' % node['port'],timeout=5) as response:
        if json.load(response)['node_state'] != 'IDLE':
            raise SystemExit('Worker is busy; refusing to stop it')
run(['sudo','-n','true'])
with tempfile.TemporaryDirectory(prefix='.deployment-',dir=root) as directory:
    stage = pathlib.Path(directory)
    with tarfile.open(fileobj=io.BytesIO(sys.stdin.buffer.read()),mode='r:gz') as archive:
        for member in archive.getmembers():
            if member.name not in payload['hashes'] or not member.isfile():
                raise SystemExit('Unexpected deployment archive entry')
            target = stage/member.name
            target.parent.mkdir(parents=True,exist_ok=True)
            target.write_bytes(archive.extractfile(member).read())
    for name, digest in payload['hashes'].items():
        if hashlib.sha256((stage/name).read_bytes()).hexdigest() != digest:
            raise SystemExit('Deployment source checksum mismatch')
    python = root/node.get('worker_python','.venv/bin/python')
    if not python.exists():
        if node.get('worker_python','.venv/bin/python') != '.venv/bin/python':
            raise SystemExit('Custom worker interpreter must already exist')
        base = os.path.expanduser(payload['python'])
        run([base,'-m','venv',str(root/'.venv')])
    run([str(python),'-m','pip','install','--disable-pip-version-check','-r',str(stage/'config/runtime-requirements.txt')])
    run([str(python),'-c','import flask, psutil, requests, waitress'])
    run([str(python),'-m','compileall','-q',str(stage/'agent'),str(stage/'training')])
    user = pwd.getpwuid(os.getuid()).pw_name
    env = {'DL_NODE_ID':node['id'],'DL_NODE_ADDRESS':node['ip'],'DL_PORT':str(node['port']),
           'DL_PRODUCTION':'1','PYTHONUNBUFFERED':'1'}
    if node.get('interface'): env['DL_NETWORK_INTERFACE'] = node['interface']
    unit = '\n'.join(['[Unit]','Description=Distributed DL Node Agent','Wants=network-online.target','After=network-online.target',
                     '[Service]','Type=simple','User='+user,'WorkingDirectory='+str(root).replace('%','%%'),
                     'ExecStart='+quote(python)+' '+quote(root/'agent/agent.py'),
                     *['Environment='+quote(k+'='+v) for k,v in env.items()],
                     'Restart=on-failure','RestartSec=3','KillMode=control-group','TimeoutStopSec=15',
                     '[Install]','WantedBy=multi-user.target',''])
    candidate = stage/'dl-node-agent.service'
    candidate.write_text(unit)
    run(['systemd-analyze','verify',str(candidate)])
    backup = root/'worker-storage/deployment-previous'
    if backup.exists(): shutil.rmtree(backup)
    backup.mkdir(parents=True)
    existed = []
    for name in payload['hashes']:
        source = root/name
        if source.exists():
            target = backup/name
            target.parent.mkdir(parents=True,exist_ok=True)
            shutil.copy2(source,target)
            existed.append(name)
    if old_unit: (backup/'dl-node-agent.service').write_bytes(old_unit)
    try:
        run(['sudo','-n','systemctl','stop','dl-node-agent']) if was_active else None
        for name in payload['hashes']:
            target = root/name
            target.parent.mkdir(parents=True,exist_ok=True)
            shutil.copy2(stage/name,target)
        run(['sudo','-n','install','-m','644',str(candidate),str(unit_path)])
        run(['sudo','-n','systemctl','daemon-reload'])
        run(['sudo','-n','systemctl','enable','--now','dl-node-agent'])
        import time
        for attempt in range(30):
            try:
                with urllib.request.urlopen('http://127.0.0.1:%s/health' % node['port'],timeout=2) as response:
                    assert json.load(response)['node'] == node['id']
                break
            except Exception:
                if attempt == 29: raise
                time.sleep(1)
    except BaseException:
        subprocess.run(['sudo','-n','systemctl','stop','dl-node-agent'])
        for name in payload['hashes']:
            if name in existed: shutil.copy2(backup/name,root/name)
            else: (root/name).unlink(missing_ok=True)
        if old_unit:
            run(['sudo','-n','install','-m','644',str(backup/'dl-node-agent.service'),str(unit_path)])
        else:
            run(['sudo','-n','rm','-f',str(unit_path)])
        run(['sudo','-n','systemctl','daemon-reload'])
        if not was_enabled: subprocess.run(['sudo','-n','systemctl','disable','dl-node-agent'])
        if was_active: run(['sudo','-n','systemctl','start','dl-node-agent'])
        raise
    print(json.dumps({'installed':node['id'],'root':str(root),'source_files':len(payload['hashes'])}))
'''


def install_worker(node, python):
    archive, hashes = bundle()
    payload = json.dumps({'node': node, 'python': python, 'hashes': hashes})
    command = shlex.join(['python3', '-c', REMOTE_INSTALL, payload])
    run(['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=8', node['id'], command], input=archive, timeout=900)
    result = probe(node)
    result['source_sha256'] = hashes
    return result


def controller_units(root=ROOT, host='127.0.0.1', port=8080):
    python = root / '.controller-venv/bin/python'
    units = {}
    for name, script, args in (
        ('dl-dispatcher', 'dispatcher.py', []),
        ('dl-student-portal', 'student_portal.py', ['--production', '--host', host, '--port', str(port)]),
    ):
        command = ' '.join(unit_quote(arg) for arg in [python, root/'controller'/script, *args])
        units[name+'.service'] = '\n'.join([
            '[Unit]', 'Description=Distributed DL '+name, 'After=network.target',
            '[Service]', 'Type=simple', 'WorkingDirectory='+str(root).replace('%','%%'),
            'ExecStart='+command, 'Environment=PYTHONUNBUFFERED=1',
            'Restart=on-failure', 'RestartSec=3', 'KillMode=control-group', 'TimeoutStopSec=20',
            '[Install]', 'WantedBy=default.target', '',
        ])
    return units


def install_controller(host, port):
    socket.inet_aton(host)
    if not 1 <= port <= 65535:
        raise ValueError('Invalid portal port')
    with maintenance():
        python = ROOT/'.controller-venv/bin/python'
        if not python.exists():
            run([sys.executable, '-m', 'venv', str(python.parent.parent)])
        run([str(python), '-m', 'pip', 'install', '-r', str(ROOT/'config/runtime-requirements.txt')])
        run([str(python), '-m', 'compileall', '-q', str(ROOT/'controller')])
        target = Path.home()/'.config/systemd/user'
        target.mkdir(parents=True, exist_ok=True)
        units = controller_units(host=host, port=port)
        for name, content in units.items():
            path = target/name
            if path.exists() and path.read_text() != content:
                backup = ROOT/'storage/deployment'
                backup.mkdir(parents=True, exist_ok=True)
                (backup/(name+'.previous')).write_text(path.read_text())
            path.write_text(content)
        run(['systemd-analyze', '--user', 'verify', *[str(target/name) for name in units]])
        run(['systemctl', '--user', 'daemon-reload'])
        # Lingering runs the user's service manager at boot without a desktop login.
        result = subprocess.run(['loginctl', '--no-ask-password', 'enable-linger', str(os.getuid())])
        if result.returncode:
            run(['sudo', '-n', 'loginctl', 'enable-linger', str(os.getuid())])
        run(['systemctl', '--user', 'enable', *units])
    run(['systemctl', '--user', 'start', 'dl-dispatcher', 'dl-student-portal'])
    time.sleep(2)
    run(['systemctl', '--user', 'is-active', 'dl-dispatcher', 'dl-student-portal'])
    with urllib.request.urlopen(f'http://{host if host != "0.0.0.0" else "127.0.0.1"}:{port}/',timeout=10) as response:
        if response.status != 200: raise ValueError('Portal failed startup check')
    print(f'Controller services installed. Portal: http://{host}:{port}')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    deploy = commands.add_parser('install-workers', help='Update registered idle workers; controller services must be stopped')
    deploy.add_argument('--nodes', nargs='+', help='Default: every registered node')
    deploy.add_argument('--python', default='python3', help='Remote base Python used only if .venv is absent')
    add = commands.add_parser('add-worker', help='Install and verify a new SSH alias, then register it')
    add.add_argument('--id', required=True)
    add.add_argument('--address', required=True)
    add.add_argument('--port', type=int, default=5000)
    add.add_argument('--root', default='distributed-dl-system')
    add.add_argument('--python', default='python3')
    add.add_argument('--interface')
    commands.add_parser('check', help='Read worker identity, runtime, clock and resources')
    controller = commands.add_parser('install-controller', help='Install and start persistent user services')
    controller.add_argument('--host', default='127.0.0.1')
    controller.add_argument('--port', type=int, default=8080)
    args = parser.parse_args()
    try:
        if args.command == 'install-controller':
            install_controller(args.host, args.port)
            return
        if args.command == 'check':
            results = []
            failed = False
            for node in load_nodes():
                try: results.append(probe(node))
                except Exception as error:
                    results.append({'id': node['id'], 'error': str(error)})
                    failed = True
            print(json.dumps(results, indent=2))
            if failed: raise SystemExit(1)
            return
        with maintenance():
            nodes = load_nodes()
            if args.command == 'add-worker':
                node = validate_node({'id':args.id,'ip':args.address,'port':args.port,'worker_root':args.root})
                if args.interface: node['interface'] = args.interface
                validate_node(node)
                if any(old['id'] == node['id'] or (old['ip'],old['port']) == (node['ip'],node['port']) for old in nodes):
                    raise ValueError('Worker already registered; use install-workers to update it')
                result = install_worker(node, args.python)
                register(node)
                results = [result]
            else:
                selected = args.nodes or [node['id'] for node in nodes]
                unknown = set(selected)-{node['id'] for node in nodes}
                if unknown: raise ValueError('Unknown workers: '+', '.join(sorted(unknown)))
                results = [install_worker(node,args.python) for node in nodes if node['id'] in selected]
        path = ROOT/'logs/deployment-verification.json'
        path.parent.mkdir(exist_ok=True)
        path.write_text(json.dumps({'checked_at':time.time(),'workers':results},indent=2)+'\n')
        print(f'Deployment verified. Evidence: {path}')
    except (ValueError, OSError, subprocess.SubprocessError) as error:
        message = (f'Command {error.cmd[0]} failed with exit status {error.returncode}'
                   if isinstance(error,subprocess.CalledProcessError) else str(error))
        parser.exit(1, message+'\n')


if __name__ == '__main__':
    main()
