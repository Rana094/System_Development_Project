"""Explicit trusted controller registry; no route, connection, migration or enrollment on import."""

from contextlib import contextmanager
from dataclasses import asdict
import errno
import hashlib
import json
import os
import stat

from .artifacts import ArtifactRegistrar, ArtifactMismatch, _verify
from .dataset_manifests import (
    DatasetACL, MAX_INTEGER, checked_entries, create_dataset, dataset_id,
    next_version, parse_manifest, principal,
)
from .input_manifests import JobInputs, SourceManifest, decoded, encoded, job_identity, parse_source
from .inputs import LocalObjectStorageBackend, object_key
from .mount_identity import require_local_fd
from .registry_schema import RegistryConflict, RegistryError, require_schema, transaction


def _denied():
    raise PermissionError(errno.EACCES, 'Storage access denied')


def _entries(manifest):
    if isinstance(manifest, SourceManifest):
        result = [(path, 'directory', None, None, None) for path in manifest.directories]
        result += [(f.path, 'file', f.sha256, f.size_bytes, int(f.executable)) for f in manifest.files]
    else:
        result = [(e.path, e.type, e.sha256, e.size_bytes, 0 if e.type == 'file' else None) for e in manifest.entries]
    return sorted(result)


def _files(manifest):
    return manifest.files if isinstance(manifest, SourceManifest) else tuple(e for e in manifest.entries if e.type == 'file')


class StorageRegistry:
    """Dedicated caller-owned local SQLite connection and trusted CAS publisher.

    Caller authenticates actor IDs and provisions protected local paths. Schema
    migration is explicit/separate. This API never enables NFS execution or reads
    arbitrary submitted filesystem paths. Only register_artifact can grant new
    byte ownership, and it always consumes/verifies the complete incoming stream.
    """

    def __init__(self, db, publisher: ArtifactRegistrar):
        if not isinstance(publisher, ArtifactRegistrar):
            raise TypeError('Expected a trusted controller-local ArtifactRegistrar')
        require_schema(db, check_foreign_keys=True)
        self.db = db
        self.publisher = publisher
        self.objects = LocalObjectStorageBackend(publisher.root)

    @contextmanager
    def _tx(self, *, write=True):
        require_schema(self.db)
        with transaction(self.db, write=write):
            yield

    def _owned(self, actor, files):
        for file in files:
            row = self.db.execute('''SELECT a.size_bytes,a.object_key FROM storage_artifacts a
                JOIN storage_artifact_owners o ON o.sha256=a.sha256 WHERE o.owner=? AND a.sha256=?''',
                                  (actor, file.sha256)).fetchone()
            if row is None:
                _denied()
            if tuple(row) != (file.size_bytes, object_key(file.sha256)):
                raise ArtifactMismatch('Owned artifact metadata mismatch')

    def _verify_bytes(self, files):
        # No file hashing inside a SQLite write transaction. Recheck authority
        # within the later commit; immutable/protected bytes still need staging
        # verification again before use, since disks can fail after this check.
        seen = set()
        for file in files:
            if file.sha256 in seen:
                continue
            seen.add(file.sha256)
            with self.objects.open_artifact(object_key(file.sha256)) as stream:
                require_local_fd(stream.fileno())
                info = os.fstat(stream.fileno())
                if info.st_uid != os.geteuid() or info.st_mode & 0o222 or not stat.S_ISREG(info.st_mode):
                    raise PermissionError('Stored artifact is not protected/immutable')
                _verify(stream, file.sha256, file.size_bytes)

    def _verify_owned(self, actor, files):
        with self._tx(write=False):
            self._owned(actor, files)
        self._verify_bytes(files)

    def register_artifact(self, actor, source, *, expected_size, expected_sha256=None):
        principal(actor)
        require_schema(self.db)
        if type(expected_size) is not int or not 0 <= expected_size <= MAX_INTEGER:
            raise ValueError('Artifact size exceeds registry integer bounds')
        artifact = self.publisher.register(source, expected_size=expected_size, expected_sha256=expected_sha256)
        with self._tx():
            existing = self.db.execute('SELECT size_bytes,object_key FROM storage_artifacts WHERE sha256=?',
                                       (artifact.sha256,)).fetchone()
            if existing is None:
                self.db.execute('INSERT INTO storage_artifacts(sha256,size_bytes,object_key) VALUES(?,?,?)',
                                (artifact.sha256, artifact.size_bytes, artifact.key))
            elif tuple(existing) != (artifact.size_bytes, artifact.key):
                raise ArtifactMismatch('Digest registry conflict')
            self.db.execute('INSERT OR IGNORE INTO storage_artifact_owners(owner,sha256) VALUES(?,?)',
                            (actor, artifact.sha256))
        return artifact

    def _save_manifest(self, manifest, actor):
        raw = manifest.canonical_bytes()
        kind = manifest.document()['kind']
        row = self.db.execute('SELECT kind,canonical FROM storage_manifests WHERE sha256=?', (manifest.sha256,)).fetchone()
        if row is None:
            self.db.execute('INSERT INTO storage_manifests(sha256,kind,canonical) VALUES(?,?,?)', (manifest.sha256, kind, raw))
            self.db.executemany('''INSERT INTO storage_manifest_entries
                (manifest_sha256,path,type,artifact_sha256,size_bytes,executable) VALUES(?,?,?,?,?,?)''',
                                [(manifest.sha256, *entry) for entry in _entries(manifest)])
        elif row[0] != kind or row[1] != raw:
            raise ArtifactMismatch('Manifest digest/content conflict')
        self._load_manifest(manifest.sha256, kind)
        self.db.execute('INSERT OR IGNORE INTO storage_manifest_owners(owner,manifest_sha256) VALUES(?,?)',
                        (actor, manifest.sha256))

    def _load_manifest(self, sha, kind):
        row = self.db.execute('SELECT kind,canonical FROM storage_manifests WHERE sha256=?', (sha,)).fetchone()
        if row is None or row[0] != kind:
            raise RegistryError('Missing/wrong manifest family')
        manifest = parse_source(row[1]) if kind == 'source-tree' else parse_manifest(row[1])
        entries = self.db.execute('''SELECT path,type,artifact_sha256,size_bytes,executable
            FROM storage_manifest_entries WHERE manifest_sha256=? ORDER BY path''', (sha,)).fetchall()
        if manifest.sha256 != sha or [tuple(entry) for entry in entries] != _entries(manifest):
            raise ArtifactMismatch('Manifest hash/normalized entry mismatch')
        return manifest

    def register_source(self, actor, manifest: SourceManifest):
        principal(actor)
        if not isinstance(manifest, SourceManifest):
            raise ValueError('Expected a validated source manifest')
        self._verify_owned(actor, manifest.files)
        with self._tx():
            self._owned(actor, manifest.files)
            self._save_manifest(manifest, actor)
        return manifest

    def _source(self, actor, sha):
        if self.db.execute('SELECT 1 FROM storage_manifest_owners WHERE owner=? AND manifest_sha256=?',
                           (actor, sha)).fetchone() is None:
            _denied()
        return self._load_manifest(sha, 'source-tree')

    def get_source(self, actor, sha):
        principal(actor)
        object_key(sha)
        with self._tx(write=False):
            return self._source(actor, sha)

    def _acl(self, identity, actor, *, write=False):
        dataset_id(identity)
        row = self.db.execute('SELECT owner,head_version FROM storage_datasets WHERE dataset_id=?', (identity,)).fetchone()
        if row is None:
            _denied()
        readers = self.db.execute('SELECT reader FROM storage_dataset_readers WHERE dataset_id=? ORDER BY reader', (identity,)).fetchall()
        acl = DatasetACL(identity, row[0], tuple(reader[0] for reader in readers))
        acl._require(actor, write=write)
        return acl, row[1]

    def _dataset(self, identity, version, actor):
        acl, head = self._acl(identity, actor)
        if type(version) is not int or not 1 <= version <= MAX_INTEGER:
            raise ValueError('Exact dataset version required')
        row = self.db.execute('SELECT manifest_sha256 FROM storage_dataset_versions WHERE dataset_id=? AND version=?',
                              (identity, version)).fetchone()
        if row is None:
            raise RegistryError('Dataset version not committed')
        manifest = self._load_manifest(row[0], 'dataset-manifest')
        acl.require_read(actor, manifest)
        if manifest.version != version:
            raise RegistryError('Dataset version binding mismatch')
        if version > 1:
            previous = self.db.execute('SELECT manifest_sha256 FROM storage_dataset_versions WHERE dataset_id=? AND version=?',
                                      (identity, version - 1)).fetchone()
            if previous is None or manifest.previous_manifest_sha256 != previous[0]:
                raise RegistryError('Dataset predecessor mismatch')
        return manifest

    def create_dataset(self, actor, entries, *, readers=()):
        manifest, acl = create_dataset(actor, entries, readers=readers)
        files = _files(manifest)
        self._verify_owned(actor, files)
        with self._tx():
            self._owned(actor, files)
            self._save_manifest(manifest, actor)
            self.db.execute('INSERT INTO storage_datasets(dataset_id,owner,head_version) VALUES(?,?,1)', (manifest.dataset_id, actor))
            self.db.execute('INSERT INTO storage_dataset_versions(dataset_id,version,manifest_sha256) VALUES(?,1,?)',
                            (manifest.dataset_id, manifest.sha256))
            self.db.executemany('INSERT INTO storage_dataset_readers(dataset_id,reader) VALUES(?,?)',
                                [(manifest.dataset_id, reader) for reader in acl.readers])
        return manifest

    def update_dataset(self, actor, identity, *, expected_head_sha256, entries):
        principal(actor)
        object_key(expected_head_sha256)
        with self._tx(write=False):
            acl, head = self._acl(identity, actor, write=True)
            previous = self._dataset(identity, head, actor)
            if previous.sha256 != expected_head_sha256:
                raise RegistryConflict('Dataset head changed')
        manifest = next_version(previous, acl, actor, entries)
        files = _files(manifest)
        self._verify_owned(actor, files)
        with self._tx():
            self._acl(identity, actor, write=True)
            row = self.db.execute('SELECT head_version FROM storage_datasets WHERE dataset_id=?', (identity,)).fetchone()
            if row[0] != head:
                raise RegistryConflict('Dataset head changed')
            self._owned(actor, files)
            self._save_manifest(manifest, actor)
            self.db.execute('INSERT INTO storage_dataset_versions(dataset_id,version,manifest_sha256) VALUES(?,?,?)',
                            (identity, manifest.version, manifest.sha256))
            changed = self.db.execute('UPDATE storage_datasets SET head_version=? WHERE dataset_id=? AND head_version=?',
                                      (manifest.version, identity, head))
            if changed.rowcount != 1:
                raise RegistryConflict('Dataset head changed')
        return manifest

    def set_readers(self, actor, identity, readers):
        principal(actor)
        with self._tx():
            old, head = self._acl(identity, actor, write=True)
            acl = old.replace_readers(actor, readers)
            self.db.execute('DELETE FROM storage_dataset_readers WHERE dataset_id=?', (identity,))
            self.db.executemany('INSERT INTO storage_dataset_readers(dataset_id,reader) VALUES(?,?)',
                                [(identity, reader) for reader in acl.readers])
        return acl

    def get_dataset(self, actor, identity, version):
        principal(actor)
        with self._tx(write=False):
            return self._dataset(identity, version, actor)

    def _job(self, identity, actor):
        row = self.db.execute('''SELECT owner,status,job_type,job_config,storage_manifest_sha256
            FROM jobs WHERE job_id=?''', (identity,)).fetchone()
        if row is None or row[0] != actor:
            _denied()
        return row

    def _binding(self, identity):
        columns = tuple(JobInputs.__dataclass_fields__)
        row = self.db.execute('SELECT ' + ','.join(columns) + ',binding_sha256 FROM storage_job_inputs WHERE job_id=?', (identity,)).fetchone()
        if row is None:
            return None
        binding = JobInputs(**dict(zip(columns, row[:-1])))
        if binding.sha256 != row[-1]:
            raise ArtifactMismatch('Job binding hash mismatch')
        return binding

    def _candidate(self, identity, actor, raw, source_sha, data_identity, data_version):
        job = self._job(identity, actor)
        selected = self.db.execute('SELECT backend FROM job_storage_bindings WHERE job_id=?', (identity,)).fetchone()
        if selected is None or selected[0] != 'nfs' or job[1] != 'HELD' or job[2] != 'custom':
            raise RegistryError('Only explicitly selected HELD NFS custom jobs can be bound')
        source = self._source(actor, source_sha)
        data = None if data_identity is None and data_version is None else self._dataset(data_identity, data_version, actor)
        return self._snapshot_binding(identity, actor, raw, source, data)

    def _snapshot_binding(self, identity, actor, raw, source, data):
        job = self._job(identity, actor)
        legacy = decoded(raw)
        if hashlib.sha256(raw).hexdigest() != job[4]:
            raise ArtifactMismatch('Legacy manifest does not match DB pin')
        if not isinstance(legacy, dict):
            raise RegistryError('Expected a DB-pinned legacy manifest object')
        inventory = legacy['source']['inventory']
        source_inventory = dict(inventory, files=sorted(inventory['files'], key=lambda file: file['path']))
        if (type(legacy.get('storage_version')) is not int or legacy.get('storage_version') != 1
                or legacy.get('job_id') != identity or legacy.get('owner') != actor
                or legacy.get('execution_spec') != json.loads(job[3])
                or source_inventory != source.legacy_inventory()):
            raise ArtifactMismatch('Source/owner/spec differs from the pinned snapshot')
        requirements_path = json.loads(job[3])['environment']['requirements']
        requirements = next((file for file in source.files if file.path == requirements_path), None)
        if requirements is None:
            raise RegistryError('Pinned source lacks the declared requirements file')
        if data is None:
            if legacy['data']['kind'] != 'generated' or legacy['data']['inventory'] != {'directories': [], 'files': []}:
                raise ArtifactMismatch('Uploaded data needs an exact dataset binding')
        else:
            listing = legacy['data']['inventory']
            files = sorted([{k: v for k, v in entry.items() if k != 'executable'} for entry in listing['files']], key=lambda file: file['path'])
            expected = [{'path': entry.path, 'sha256': entry.sha256, 'size': entry.size_bytes}
                        for entry in data.entries if entry.type == 'file']
            if (legacy['data']['kind'] not in ('file', 'directory', 'zip') or files != expected
                    or listing['directories'] != [entry.path for entry in data.entries if entry.type == 'directory']):
                raise ArtifactMismatch('Dataset differs from the pinned legacy inventory')
        binding = JobInputs(identity, actor, source.sha256, requirements.path, requirements.sha256,
                            requirements.size_bytes, job[4], hashlib.sha256(encoded(json.loads(job[3]))).hexdigest(),
                            None if data is None else data.dataset_id,
                            None if data is None else data.version, None if data is None else data.sha256)
        return binding, (*source.files, *(() if data is None else _files(data)))

    def _pin(self, binding):
        existing = self._binding(binding.job_id)
        if existing is not None:
            if existing != binding:
                raise RegistryConflict('Job inputs are already pinned')
        else:
            values = asdict(binding)
            values['binding_sha256'] = binding.sha256
            self.db.execute('INSERT INTO storage_job_inputs(' + ','.join(values) + ') VALUES(' +
                            ','.join('?' for _ in values) + ')', tuple(values.values()))

    def bind_job(self, actor, identity, *, legacy_manifest_bytes, source_sha256, dataset_id=None, dataset_version=None):
        principal(actor)
        job_identity(identity)
        object_key(source_sha256)
        with self._tx(write=False):
            binding, files = self._candidate(identity, actor, legacy_manifest_bytes, source_sha256, dataset_id, dataset_version)
        self._verify_bytes(files)
        with self._tx():
            current, _ = self._candidate(identity, actor, legacy_manifest_bytes, source_sha256, dataset_id, dataset_version)
            if current != binding:
                raise RegistryConflict('Job input authority changed')
            self._pin(binding)
        return binding

    def _snapshot_job(self, actor, identity):
        fields = ('job_id', 'owner', 'status', 'job_type', 'job_config', 'storage_path', 'storage_manifest_sha256')
        columns = {row[1] for row in self.db.execute('PRAGMA table_info(jobs)')}
        if not set(fields) <= columns:
            raise RegistryError('Legacy database lacks managed snapshot metadata; no automatic upgrade or owner inference')
        row = self.db.execute('SELECT ' + ','.join(fields) + ' FROM jobs WHERE job_id=?', (identity,)).fetchone()
        if row is None or row[1] != actor:
            _denied()
        job = dict(zip(fields, row))
        if row[3] != 'custom' or row[2] not in ('STAGED', 'READY', 'QUEUED', 'HELD', 'SCHEDULED', 'RUNNING',
                                               'CANCELLING', 'COMPLETED', 'FAILED', 'CANCELLED'):
            raise RegistryError('Explicit registration requires a committed managed custom-job snapshot')
        object_key(job['storage_manifest_sha256'])
        selected = None
        if self.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='job_storage_bindings'").fetchone():
            selected = self.db.execute('SELECT binding_version,backend FROM job_storage_bindings WHERE job_id=?', (identity,)).fetchone()
        if selected is not None and (selected[0] != 1 or selected[1] not in ('legacy', 'nfs')):
            raise RegistryError('Unknown backend selection')
        if selected is not None and selected[1] == 'nfs' and row[2] != 'HELD':
            raise RegistryError('NFS registration remains limited to HELD jobs')
        existing = self._binding(identity)
        if existing is not None:
            if (existing.owner != actor or existing.legacy_manifest_sha256 != job['storage_manifest_sha256']
                    or existing.execution_spec_sha256 != hashlib.sha256(encoded(json.loads(job['job_config']))).hexdigest()):
                raise ArtifactMismatch('Existing snapshot binding drifted')
            self._source(actor, existing.source_sha256)
            if existing.dataset_id is not None:
                data = self._dataset(existing.dataset_id, existing.dataset_version, actor)
                if data.sha256 != existing.dataset_sha256:
                    raise ArtifactMismatch('Existing dataset binding drifted')
        return job

    def legacy_snapshot_job(self, actor, identity):
        """Owner-authorized metadata only; does not open paths or change execution."""
        principal(actor)
        job_identity(identity)
        with self._tx(write=False):
            return self._snapshot_job(actor, identity)

    def register_snapshot(self, actor, identity, *, legacy_manifest_bytes, source, data_entries):
        """Atomic reference-only enrollment after verified stream registration.

        Does not change backend selection, state, paths or recovery identity.
        Both legacy snapshots and already-HELD NFS intents can be referenced.
        The legacy-registration facade authorizes/validates actual managed paths.
        """
        principal(actor)
        job_identity(identity)
        if not isinstance(source, SourceManifest):
            raise ValueError('Expected a validated source manifest')
        entries = None if data_entries is None else checked_entries(data_entries)
        files = (*source.files, *(() if entries is None else (entry for entry in entries if entry.type == 'file')))
        self.legacy_snapshot_job(actor, identity)
        self._verify_owned(actor, files)
        with self._tx():
            self._snapshot_job(actor, identity)
            existing = self._binding(identity)
            if existing is not None:
                data = None if existing.dataset_id is None else self._dataset(existing.dataset_id, existing.dataset_version, actor)
                if (None if data is None else data.entries) != entries:
                    raise RegistryConflict('Snapshot data is already pinned')
            else:
                data = None if entries is None else create_dataset(actor, entries)[0]
            binding, _ = self._snapshot_binding(identity, actor, legacy_manifest_bytes, source, data)
            if existing is not None:
                self._pin(binding)
                return binding
            self._owned(actor, files)
            self._save_manifest(source, actor)
            if data is not None:
                self._save_manifest(data, actor)
                self.db.execute('INSERT INTO storage_datasets(dataset_id,owner,head_version) VALUES(?,?,1)', (data.dataset_id, actor))
                self.db.execute('INSERT INTO storage_dataset_versions(dataset_id,version,manifest_sha256) VALUES(?,1,?)', (data.dataset_id, data.sha256))
            self._pin(binding)
            return binding

    def get_job_inputs(self, actor, identity):
        principal(actor)
        job_identity(identity)
        with self._tx(write=False):
            job = self._job(identity, actor)
            binding = self._binding(identity)
            if binding is None:
                return None  # Explicit old-job fallback, independent of settings.
            if binding.owner != actor or binding.legacy_manifest_sha256 != job[4]:
                raise ArtifactMismatch('Job owner/legacy pin changed')
            if hashlib.sha256(encoded(json.loads(job[3]))).hexdigest() != binding.execution_spec_sha256:
                raise ArtifactMismatch('Pinned execution specification changed')
            source = self._source(actor, binding.source_sha256)
            file = next((file for file in source.files if file.path == binding.requirements_path), None)
            if (file is None or file.sha256 != binding.requirements_sha256
                    or file.size_bytes != binding.requirements_size_bytes
                    or json.loads(job[3])['environment']['requirements'] != binding.requirements_path):
                raise ArtifactMismatch('Requirements identity changed')
            if binding.dataset_id is not None:
                data = self._dataset(binding.dataset_id, binding.dataset_version, actor)
                if data.sha256 != binding.dataset_sha256:
                    raise ArtifactMismatch('Pinned dataset identity changed')
            return binding
