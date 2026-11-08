"""Owner-authorized, snapshot-consistent advisory coverage; no scheduler hook.

Only immutable registered inputs are counted. The trusted controller supplies
the authenticated actor and any explicit whole-file rank declaration. Results
grant no staging, cache-listing, resource reservation or execution authority.
"""

from dataclasses import asdict, dataclass
import json
from pathlib import Path
import sqlite3

from . import cache_registry_schema as schema
from . import inventory_protocol as protocol
from .cache_inventory import CacheInventory
from .dataset_manifests import MAX_ENTRIES, MAX_INTEGER, principal
from .input_manifests import job_identity
from .inputs import object_key
from .registry import StorageRegistry
from .registry_schema import RegistryError


class CoverageUnavailable(RegistryError):
    """No exact registered input set; never guess from legacy paths/filenames."""


@dataclass(frozen=True)
class RankRequirements:
    """Trusted planner declaration: common source plus these whole data objects.

    This is a diagnostic input, not a signed staging grant or runtime shard plan.
    The existing trainers need the entire dataset on every rank by default.
    """
    rank: int
    world_size: int
    job_binding_sha256: str
    dataset_hashes: tuple[str, ...]

    def __post_init__(self):
        if (type(self.world_size) is not int or not 1 <= self.world_size <= MAX_INTEGER
                or type(self.rank) is not int or not 0 <= self.rank < self.world_size):
            raise ValueError('Invalid rank/world size')
        object_key(self.job_binding_sha256)
        if type(self.dataset_hashes) is not tuple or len(self.dataset_hashes) > MAX_ENTRIES:
            raise ValueError('Expected a bounded tuple of required whole-file dataset hashes')
        for sha in self.dataset_hashes: object_key(sha)
        object.__setattr__(self, 'dataset_hashes', tuple(sorted(set(self.dataset_hashes))))


@dataclass(frozen=True)
class CacheCoverage:
    worker_id: str
    target_kind: str
    target_id: str
    target_version: int | None
    input_manifest_sha256: str
    rank: int | None
    world_size: int | None
    required_objects: int
    required_bytes: int
    verified_cached_bytes: int
    missing_bytes: int
    locality_ratio: float
    missing_hashes: tuple[str, ...]
    inventory_eligible: bool
    inventory_status: str
    inventory_age_ns: int | None
    inventory_session_id: str | None
    inventory_sequence: int | None
    computed_at_ns: int

    def document(self):
        result = asdict(self)
        result['missing_hashes'] = list(self.missing_hashes)
        return result


def _unique(files):
    sizes = {}
    for file in files:
        object_key(file.sha256)
        if type(file.size_bytes) is not int or not 0 <= file.size_bytes <= MAX_INTEGER:
            raise RegistryError('Invalid registered artifact size')
        if file.sha256 in sizes and sizes[file.sha256] != file.size_bytes:
            raise RegistryError('Conflicting sizes for one required digest')
        sizes[file.sha256] = file.size_bytes
    if sum(sizes.values()) > MAX_INTEGER:
        raise RegistryError('Unique required byte total exceeds signed-64-bit bounds')
    return sizes


class CoverageAPI:
    def __init__(self, registry: StorageRegistry, inventory: CacheInventory):
        if not isinstance(registry, StorageRegistry) or not isinstance(inventory, CacheInventory):
            raise TypeError('Expected explicit controller registry and inventory owner')
        self.registry, self.inventory = registry, inventory

    def for_dataset(self, actor, worker_id, dataset_id, version):
        """Current ACL + exact immutable dataset version; no implicit head lookup."""
        principal(actor); protocol.worker_id(worker_id)
        with self.inventory._lock, self.registry._tx(write=False):
            manifest = self.registry._dataset(dataset_id, version, actor)
            required = _unique(e for e in manifest.entries if e.type == 'file')
            return self._coverage(worker_id, required, target_kind='dataset', target_id=dataset_id,
                                  target_version=version, input_manifest_sha256=manifest.sha256,
                                  rank=None, world_size=None)

    def for_job(self, actor, worker_id, job_id, *, rank=None, requirements=None):
        """All source/requirements + pinned data, or a checked whole-file rank set.

        An unregistered legacy job raises CoverageUnavailable, without changing
        that job or its existing execution path. No lazy registration occurs.
        """
        principal(actor); protocol.worker_id(worker_id); job_identity(job_id)
        with self.inventory._lock, self.registry._tx(write=False):
            binding = self.registry._job_inputs(actor, job_id)
            if binding is None:
                raise CoverageUnavailable('Job has no immutable input binding; explicit registration required')
            source = self.registry._source(actor, binding.source_sha256)
            data = None if binding.dataset_id is None else self.registry._dataset(binding.dataset_id, binding.dataset_version, actor)
            data_files = () if data is None else tuple(e for e in data.entries if e.type == 'file')
            world_size = None
            if rank is not None:
                execution = json.loads(self.registry._job(job_id, actor)[3])['execution']
                world_size = execution['workers']
                if (type(world_size) is not int or not 1 <= world_size <= MAX_INTEGER
                        or type(rank) is not int or not 0 <= rank < world_size):
                    raise ValueError('Rank must belong to the pinned job execution declaration')
            if requirements is not None:
                if (not isinstance(requirements, RankRequirements) or rank is None
                        or requirements.rank != rank or requirements.world_size != world_size
                        or requirements.job_binding_sha256 != binding.sha256):
                    raise ValueError('Rank declaration does not match the exact pinned job/rank/world size')
                allowed = {e.sha256 for e in data_files}
                if not set(requirements.dataset_hashes) <= allowed:
                    raise PermissionError('Rank declaration contains objects outside the authorized pinned dataset')
                data_files = tuple(e for e in data_files if e.sha256 in requirements.dataset_hashes)
            required = _unique((*source.files, *data_files))
            return self._coverage(worker_id, required, target_kind='job', target_id=job_id,
                                  target_version=None, input_manifest_sha256=binding.sha256,
                                  rank=rank, world_size=world_size)

    def _view(self, db, worker, now):
        owner = self.inventory
        # Check the namespace before reading any historical VERIFIED rows.
        try:
            if not schema._installed(db):
                return False, 'no_inventory_schema', None, None, None
            context = owner._contexts.get(worker)
            if context:
                row = db.execute('''SELECT session_id,last_sequence,inventory_updated_at_ns
                    FROM cache_registry_sessions WHERE worker_id=? AND session_id=?''',
                    (worker, context['session_id'])).fetchone()
            else:
                row = db.execute('''SELECT session_id,last_sequence,inventory_updated_at_ns
                    FROM cache_registry_sessions WHERE worker_id=?
                    ORDER BY inventory_updated_at_ns DESC,created_at_ns DESC,session_id LIMIT 1''', (worker,)).fetchone()
            age = None if row is None or row[2] is None or row[2] > now else now-row[2]
            metadata = (age, None if row is None else row[0], None if row is None else row[1])
            if owner._owner is None: return False, 'owner_unavailable', *metadata
            if worker not in owner.credentials: return False, 'worker_not_enrolled', *metadata
            if not context or context['sequence'] == 0 or row is None:
                return False, 'no_current_inventory', *metadata
            if row[1] != context['sequence'] or not owner._eligible(db, worker, now, session_id=context['session_id']):
                return False, 'untrusted_or_stale', *metadata
            return True, 'current', *metadata
        except (sqlite3.Error, RegistryError):
            return False, 'invalid_inventory', None, None, None

    def _coverage(self, worker, required, **target):
        db, owner = self.registry.db, self.inventory
        actual = next(row[2] for row in db.execute('PRAGMA database_list') if row[1] == 'main')
        if hasattr(owner, '_database_path') and Path(actual) != owner._database_path:
            raise RegistryError('Coverage and inventory must reference the same controller-local DB')
        # Missing authority cannot be turned into a fabricated zero-byte target.
        for sha, size in required.items():
            row = db.execute('SELECT size_bytes,object_key FROM storage_artifacts WHERE sha256=?', (sha,)).fetchone()
            if row is None or tuple(row) != (size, object_key(sha)):
                raise RegistryError('Required artifact authority is missing or inconsistent')
        now = owner.clock()
        eligible, status, age, session, sequence = self._view(db, worker, now)
        cached = set()
        if eligible:
            try:
                rows = db.execute('''SELECT sha256,size_bytes,state,verified_bytes,last_verified_at_ns,inventory_sequence
                    FROM cache_registry_entries WHERE worker_id=? AND session_id=?''', (worker,session)).fetchall()
                for sha, size, state, verified, last_verified, observed_sequence in rows:
                    if (sha in required and required[sha] == size and state == 'VERIFIED' and verified == size
                            and last_verified is not None and 1 <= observed_sequence <= sequence):
                        cached.add(sha)
            except sqlite3.Error:
                eligible, status, cached = False, 'invalid_inventory', set()
        total = sum(required.values())
        hits = sum(required[sha] for sha in cached)
        ratio = (hits/total if total else 1.0) if eligible else 0.0
        return CacheCoverage(worker_id=worker, **target, required_objects=len(required), required_bytes=total,
            verified_cached_bytes=hits, missing_bytes=total-hits, locality_ratio=ratio,
            missing_hashes=tuple(sorted(set(required)-cached)), inventory_eligible=eligible,
            inventory_status=status, inventory_age_ns=age, inventory_session_id=session,
            inventory_sequence=sequence, computed_at_ns=now)
