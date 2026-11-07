"""Explicit owner-authorized registration of one existing local managed snapshot.

No imports of controller/job_store, connections, migration, route or discovery.
Caller supplies existing read-only job_storage.verify_workspace as the verifier.
The legacy files/backend/recovery identity remain authoritative and unchanged.
"""

import hashlib
import os
from pathlib import Path

from .artifacts import ArtifactMismatch
from .backends import LegacyStorageBackend, _directory, _root
from .dataset_manifests import DatasetEntry, MAX_MANIFEST_BYTES
from .input_manifests import SourceFile, SourceManifest, decoded
from .mount_identity import require_local_fd
from .registry import StorageRegistry
from .registry_schema import RegistryError


def _signature(stream):
    info = os.fstat(stream.fileno())
    return (info.st_dev, info.st_ino, info.st_mode, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def _metadata(backend, name):
    with backend.open_artifact(name) as stream:
        require_local_fd(stream.fileno())
        before = _signature(stream)
        raw = stream.read(MAX_MANIFEST_BYTES + 1)
        if len(raw) > MAX_MANIFEST_BYTES or _signature(stream) != before:
            raise ArtifactMismatch('Legacy metadata is oversized or changed during read')
        return raw


def register_legacy_inputs(registry, actor, job_id, *, managed_root: Path, verify_workspace):
    """Register one requested job; never migrate, move or silently enable it.

    managed_root and verifier are trusted operator/controller capabilities, not
    request fields. Reuse the existing verifier without importing a module that
    initializes SQLite. Source snapshots must remain controller-protected during
    inspection/copy; this is not an OS snapshot or a lock against administrators.
    """
    if not isinstance(registry, StorageRegistry) or not callable(verify_workspace):
        raise TypeError('Expected the trusted registry and existing workspace verifier')
    job = registry.legacy_snapshot_job(actor, job_id)  # Authorize BEFORE any path lookup.
    root = _root(managed_root)
    for output in (registry.publisher.root, registry.publisher.staging_root):
        if root.is_relative_to(output) or output.is_relative_to(root):
            raise RegistryError('Managed snapshots and CAS/scratch roots must not overlap')
    workspace = root / job_id
    if job['storage_path'] != str(workspace):
        raise RegistryError('Job snapshot path differs from the controlled managed root')
    for path in (root, workspace, workspace / 'source', workspace / 'data'):
        with _directory(path) as fd:
            require_local_fd(fd)
    reader = LegacyStorageBackend(workspace)
    raw = _metadata(reader, 'manifest.json')
    original = _metadata(reader, 'submission.json')
    manifest = decoded(raw)
    if not isinstance(manifest, dict) or hashlib.sha256(raw).hexdigest() != job['storage_manifest_sha256']:
        raise ArtifactMismatch('Legacy manifest differs from the DB pin')
    if hashlib.sha256(original).hexdigest() != manifest.get('submission_sha256'):
        raise ArtifactMismatch('Original submission metadata changed')
    checked, digest = verify_workspace(workspace, job)
    if checked != manifest or digest != job['storage_manifest_sha256']:
        raise ArtifactMismatch('Existing workspace verification disagrees with pinned bytes')
    source_inventory = manifest['source']['inventory']
    if manifest['source']['kind'] not in ('directory', 'zip') or manifest['data']['kind'] not in ('generated', 'file', 'directory', 'zip'):
        raise RegistryError('Unrecognized legacy source/data kind; no publication')
    source = SourceManifest([SourceFile(entry['path'], entry['sha256'], entry['size'], entry['executable'])
                             for entry in source_inventory['files']], source_inventory['directories'])
    listing = manifest['data']['inventory']
    entries = [DatasetEntry(path, 'directory') for path in listing['directories']]
    entries += [DatasetEntry(entry['path'], 'file', entry['sha256'], entry['size']) for entry in listing['files']]
    # Validate the entire plan before publishing even the first artifact.
    from .dataset_manifests import checked_entries
    entries = checked_entries(entries)
    if manifest['data']['kind'] == 'generated' and entries:
        raise ArtifactMismatch('Generated input snapshot must be empty')
    data_entries = None if manifest['data']['kind'] == 'generated' else entries
    for section, inventory in (('source', source_inventory), ('data', listing)):
        backend = LegacyStorageBackend(workspace / section)
        for entry in inventory['files']:
            with backend.open_artifact(entry['path']) as stream:
                require_local_fd(stream.fileno())
                before = _signature(stream)
                if before[3] != entry['size'] or bool(before[2] & 0o111) != entry['executable']:
                    raise ArtifactMismatch('Legacy file size/mode changed before copying')
                registry.register_artifact(actor, stream, expected_size=entry['size'], expected_sha256=entry['sha256'])
                if _signature(stream) != before:
                    raise ArtifactMismatch('Legacy file changed during registration')
    current = registry.legacy_snapshot_job(actor, job_id)
    for field in ('owner', 'job_config', 'storage_path', 'storage_manifest_sha256'):
        if current[field] != job[field]:
            raise ArtifactMismatch('Legacy job authority changed during registration')
    checked, digest = verify_workspace(workspace, current)
    if checked != manifest or digest != job['storage_manifest_sha256'] or _metadata(reader, 'manifest.json') != raw:
        raise ArtifactMismatch('Legacy snapshot changed before enrollment commit')
    return registry.register_snapshot(actor, job_id, legacy_manifest_bytes=raw, source=source, data_entries=data_entries)
