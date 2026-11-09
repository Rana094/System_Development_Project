"""Offline source review bundles; never upload, extract, install or start services.

Explicit selected identities affect the review manifest only. Operator configs,
credentials, databases, jobs, caches and dependencies are never bundled. A source
bundle does not supply the required OS/network containment or trusted bootstrap.
"""
import argparse
from contextlib import ExitStack
import gzip
import hashlib
import ipaddress
import io
import json
import os
from pathlib import Path
import stat
import tarfile

from .backends import _directory

ROOT = Path(__file__).resolve().parents[1]
LIMIT = 8 * 1024 * 1024
MAX_FILES = 2048
MAX_TOTAL = 128 * 1024 * 1024
ROLES = {
    'worker': ('agent/*.py', 'hybrid_storage/*.py', 'training/*.py', 'training/reference/*.py'),
    'controller': ('controller/*.py', 'hybrid_storage/*.py', 'agent/*.py',
                   'training/*.py', 'training/reference/*.py', 'controller/portal_static/*.html',
                   'controller/portal_static/*.js', 'controller/portal_static/*.css',
                   'controller/portal_static/*.svg'),
}
EXAMPLES = ('legacy.json', 'nfs-resource.json', 'nfs-locality.json', 'nfs-bandwidth.json')
REQUIRED = {
    'worker': ('agent/agent.py', 'agent/staging.py', 'agent/checkpoint_service.py',
               'hybrid_storage/worker_cache.py', 'hybrid_storage/transfer_metrics.py',
               'hybrid_storage/cache_eviction.py'),
    'controller': ('controller/dispatcher.py', 'controller/staging_dispatch.py',
                   'controller/checkpoint_recovery.py', 'hybrid_storage/bandwidth.py'),
}


def _read(root, name):
    """Anchor every component; reject links/special files and bounded-size races."""
    with ExitStack() as stack:
        parent = stack.enter_context(_directory(root))
        parts = Path(name).parts
        for part in parts[:-1]:
            fd = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
            stack.callback(os.close, fd); parent = fd
        fd = os.open(parts[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
        stream = stack.enter_context(os.fdopen(fd, 'rb'))
        before = os.fstat(stream.fileno())
        if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or before.st_size > LIMIT:
            raise ValueError('Source must be a bounded, unlinked regular file: '+name)
        data = stream.read(LIMIT+1)
        after = os.fstat(stream.fileno())
        if (len(data) > LIMIT or len(data) != before.st_size or
                (before.st_size, before.st_mtime_ns, before.st_ctime_ns) !=
                (after.st_size, after.st_mtime_ns, after.st_ctime_ns)):
            raise ValueError('Source changed or exceeded bound: '+name)
        return data


def catalog(role, *, root=ROOT):
    if role not in ROLES:
        raise ValueError('Unknown bundle role')
    root = Path(root)
    names = {str(p.relative_to(root)) for pattern in ROLES[role] for p in root.glob(pattern)}
    names.add('config/runtime-requirements.txt')
    names.update('config/storage-examples/'+name for name in EXAMPLES)
    if not set(REQUIRED[role]) <= names or len(names) > MAX_FILES:
        raise ValueError('Incomplete or oversized source set')
    result = {}; total = 0
    for name in sorted(names):
        data = _read(root, name); total += len(data)
        if total > MAX_TOTAL:
            raise ValueError('Source set exceeds total bound')
        result[name] = dict(bytes=len(data), sha256=hashlib.sha256(data).hexdigest())
    return result


def review(nodes, *, root=ROOT):
    nodes = tuple(nodes)
    if (not nodes or len(set(nodes)) != len(nodes) or
            any(node not in ('pi01', 'pi02', 'pi03') for node in nodes)):
        raise ValueError('Select distinct configured Pi identities explicitly')
    configured = json.loads(_read(Path(root), 'config/nodes.json'))['nodes']
    inventory = {node['id']: node for node in configured}
    if len(inventory) != len(configured) or any(node not in inventory for node in nodes):
        raise ValueError('Selected worker missing or duplicate in inventory')
    for node in nodes:
        endpoint = inventory[node]
        address = ipaddress.IPv4Address(endpoint['ip'])
        if (address.is_unspecified or address.is_multicast or address.is_loopback or
                type(endpoint['port']) is not int or not 1 <= endpoint['port'] <= 65535):
            raise ValueError('Invalid configured worker endpoint')
    return dict(schema_version=1, scope='offline_source_review_only', apply_ready=False,
        selected_workers=[dict(id=node, ip=inventory[node]['ip'], port=inventory[node]['port']) for node in nodes],
        deferred_workers=sorted(set(inventory)-set(nodes)),
        all_three_workers_selected=len(nodes) == 3,
        all_three_pi_acceptance_proven=False,
        blockers=['Reviewed OS AND network containment, termination/quiescence and trusted bootstrap required',
                  'Protected local roots and separately provisioned credentials required',
                  'Fresh worker/controller identity, disk, RO NFS mount and dependency compatibility required',
                  'Exact deployment/system/service changes and rollback require explicit approval'],
        activation='No SSH, extraction, install, migration, mount, service or config action',
        dependencies='Requirements only; no wheels/weights supplied and no Internet runtime download allowed',
        files={role: catalog(role, root=root) for role in ROLES})


def build(nodes, output, *, root=ROOT):
    """Create a new local review directory; existing destinations are untouched."""
    document = review(nodes, root=root)
    output = Path(output)
    if not output.is_absolute():
        raise ValueError('Output must be an absolute new directory')
    # Parent traversal is checked before mkdir; never reuse an existing directory.
    with _directory(output.parent) as parent:
        os.mkdir(output.name, mode=0o700, dir_fd=parent)
    with _directory(output) as target:
        document['archives'] = {}
        for role, files in document['files'].items():
            name = role+'-source.tar.gz'
            fd = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=target)
            with os.fdopen(fd, 'wb') as stream:
                with gzip.GzipFile(filename='', mode='wb', fileobj=stream, mtime=0) as compressed:
                    with tarfile.open(fileobj=compressed, mode='w|') as archive:
                        for path, expected in files.items():
                            data = _read(Path(root), path)
                            if dict(bytes=len(data), sha256=hashlib.sha256(data).hexdigest()) != expected:
                                raise ValueError('Source changed after review: '+path)
                            info = tarfile.TarInfo(path); info.size = len(data); info.mode = 0o644
                            archive.addfile(info, io.BytesIO(data))
                stream.flush(); os.fsync(stream.fileno())
            # Archive size is bounded by the catalog limits; stream its checksum.
            fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=target)
            with os.fdopen(fd, 'rb') as stream:
                digest = hashlib.file_digest(stream, 'sha256').hexdigest()
                size = os.fstat(stream.fileno()).st_size
            document['archives'][role] = dict(filename=name, bytes=size, sha256=digest)
        # Review completion marker appears only after both complete archives.
        fd = os.open('review.json', os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=target)
        with os.fdopen(fd, 'w') as stream:
            json.dump(document, stream, indent=2, sort_keys=True); stream.write('\n')
            stream.flush(); os.fsync(stream.fileno())
        os.fsync(target)
    return document


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--nodes', nargs='+', required=True, choices=('pi01', 'pi02', 'pi03'))
    parser.add_argument('--output', type=Path, help='New absolute local review directory; omission prints catalog only')
    args = parser.parse_args()
    document = review(args.nodes) if args.output is None else build(args.nodes, args.output)
    print(json.dumps(document, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
