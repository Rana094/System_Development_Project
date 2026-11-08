"""Trusted private-copy materialization and durable exact-attempt pin lifecycle.

No mounts, trainer launch, chown, HTTP route or runtime activation. Full local
copies are the supported fallback; no hardlinks or shared writable cache inode.
Untrusted execution still requires a separate identity and filesystem boundary.
"""

from contextlib import contextmanager, ExitStack
from dataclasses import dataclass
import errno
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import re
import secrets
import stat
import time
from typing import Callable

from .artifacts import _rename_noreplace, _verify
from .backends import _directory, _root
from .dataset_manifests import DatasetEntry, DatasetManifest, MAX_ENTRIES, _unique
from .input_manifests import SourceManifest, encoded, job_identity
from .inputs import InputFile, object_key
from .worker_cache import (CHUNK_BYTES, MAX_INTEGER, CacheError, WorkerCache,
                           _child, _private_file, _protected, _signature)

MAX_PLAN_BYTES = 4 * 1024 * 1024
MAX_CONTROL_BYTES = MAX_PLAN_BYTES + 8192
OUTCOMES = frozenset(("COMPLETED", "FAILED", "CANCELLED", "LEASE_EXPIRED", "LAUNCH_FAILED",
                      "STAGING_FAILED", "SUPERSEDED", "RESTART_ABORTED"))
PHASES = frozenset(("MATERIALIZING", "READY", "STARTED", "ABORTING", "TERMINAL"))


class WorkspaceError(CacheError):
    """Invalid plan, workspace/control record or conflicting attempt identity."""


class WorkspaceBusy(WorkspaceError):
    """Still active, terminal, interrupted, or awaiting quiescence reconciliation."""


class WorkspaceCancelled(WorkspaceError):
    pass


class WorkspaceLeaseExpired(WorkspaceError):
    pass


class WorkspaceLaunchFailed(WorkspaceError):
    pass


@dataclass(frozen=True)
class AttemptIdentity:
    job_id: str
    attempt_token: str
    rank: int = 0
    stage_revision: int = 1

    def __post_init__(self):
        job_identity(self.job_id)
        # Preserve the existing launch protocol's job_id:retry spelling.
        if (type(self.attempt_token) is not str or len(self.attempt_token) > 160
                or not re.fullmatch(re.escape(self.job_id) + r":[0-9]+", self.attempt_token)):
            raise ValueError("Expected the existing job_id:retry attempt token")
        if type(self.rank) is not int or not 0 <= self.rank <= 65535:
            raise ValueError("Rank must be a nonnegative bounded integer")
        if type(self.stage_revision) is not int or not 1 <= self.stage_revision <= MAX_INTEGER:
            raise ValueError("Stage revision must be a positive integer")

    def document(self):
        return dict(job_id=self.job_id, attempt_token=self.attempt_token,
                    rank=self.rank, stage_revision=self.stage_revision)

    @property
    def key(self):
        return hashlib.sha256(encoded(self.document())).hexdigest()

@dataclass(frozen=True)
class WorkspacePlan:
    identity: AttemptIdentity
    input_manifest_sha256: str
    source: SourceManifest
    dataset: DatasetManifest | None = None

    def __post_init__(self):
        if not isinstance(self.identity, AttemptIdentity) or not isinstance(self.source, SourceManifest):
            raise ValueError("Expected typed attempt identity and immutable source manifest")
        object_key(self.input_manifest_sha256)
        if self.dataset is not None and not isinstance(self.dataset, DatasetManifest):
            raise ValueError("Expected an exact immutable dataset manifest or None")
        entries = len(self.source.files) + len(self.source.directories)
        entries += len(self.dataset.entries) if self.dataset else 0
        if entries > MAX_ENTRIES or self.input_bytes > MAX_INTEGER:
            raise ValueError("Workspace entry/byte bound exceeded")
        if len(encoded(self.document())) > MAX_PLAN_BYTES:
            raise ValueError("Workspace plan exceeds 4 MiB")
        sizes = {}
        for file in self.inputs():
            if file.sha256 in sizes and sizes[file.sha256] != file.size_bytes:
                raise ValueError("Source/dataset disagree on one digest's size")
            sizes[file.sha256] = file.size_bytes

    def document(self):
        return dict(schema_version=1, identity=self.identity.document(),
                    input_manifest_sha256=self.input_manifest_sha256,
                    source=self.source.document(), dataset=self.dataset.document() if self.dataset else None)

    @property
    def sha256(self):
        return hashlib.sha256(encoded(self.document())).hexdigest()

    @property
    def input_bytes(self):
        size = sum(file.size_bytes for file in self.source.files)
        if self.dataset:
            size += sum(entry.size_bytes for entry in self.dataset.entries if entry.type == "file")
        return size

    def inputs(self):
        files = [InputFile("source/" + file.path, file.sha256, file.size_bytes) for file in self.source.files]
        if self.dataset:
            files.extend(InputFile("data/" + entry.path, entry.sha256, entry.size_bytes)
                         for entry in self.dataset.entries if entry.type == "file")
        return tuple(files)


@dataclass(frozen=True)
class WorkspaceReceipt:
    identity_key: str
    plan_sha256: str
    root: Path
    dataset_present: bool

    @property
    def source(self): return self.root / "inputs/source"
    @property
    def data(self): return self.root / "inputs/data" if self.dataset_present else None
    @property
    def outputs(self): return self.root / "outputs"
    @property
    def checkpoints(self): return self.root / "checkpoints"


class WorkspaceManager:
    """Explicit local service. Protected root must be preprovisioned mode 0700.

    Control JSON follows the existing worker's durable local-record convention;
    no controller/cache SQLite schema changes. Per-identity OS locks serialize
    prepare/start/finish across processes. A receipt is diagnostic, not a grant.
    """

    def __init__(self, cache: WorkerCache, root: Path, *, max_input_bytes: int,
                 reserve_free_bytes: int = 64 * 1024 * 1024, lock_timeout: float = 2):
        if not isinstance(cache, WorkerCache):
            raise ValueError("Expected an explicit worker cache")
        self.cache, self.root = cache, _root(root)
        for other in (cache.root, cache.input_mount):
            if self.root.is_relative_to(other) or other.is_relative_to(self.root):
                raise ValueError("Private workspace root must not overlap cache or NFS")
        for name, value in (("max_input_bytes", max_input_bytes), ("reserve_free_bytes", reserve_free_bytes)):
            if type(value) is not int or not 0 <= value <= MAX_INTEGER:
                raise ValueError(name + " must be a bounded byte count")
        if (type(lock_timeout) not in (int, float) or not math.isfinite(lock_timeout)
                or not 0 < lock_timeout <= 30):
            raise ValueError("Lock timeout must be finite and within 30 seconds")
        self.max_input_bytes, self.reserve_free_bytes, self.lock_timeout = max_input_bytes, reserve_free_bytes, lock_timeout

    def initialize(self):
        """Explicit local setup only; never initializes a cache or scans old jobs."""
        with _directory(self.root) as root:
            _protected(root, directory=True)
            with _child(root, "state", create=True), _child(root, "attempts", create=True):
                pass

    def pin_holder(self, plan):
        """Scope exact-attempt references to this workspace/cache root pair."""
        return "ws-" + hashlib.sha256(encoded(dict(kind="private-workspace-pin",
            workspace_root=str(self.root), cache_root=str(self.cache.root),
            identity=plan.identity.document()))).hexdigest()

    @contextmanager
    def _locked(self, plan):
        if not isinstance(plan, WorkspacePlan):
            raise ValueError("Expected a validated WorkspacePlan")
        with _directory(self.root) as root:
            _protected(root, directory=True)
            with _child(root, "state") as state, _child(root, "attempts") as attempts:
                lock = _private_file(state, plan.identity.key + ".lock", create=True)
                try:
                    end = time.monotonic() + self.lock_timeout
                    while True:
                        try:
                            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                            break
                        except BlockingIOError:
                            if time.monotonic() >= end:
                                raise WorkspaceBusy("Exact attempt still owned by another operation")
                            time.sleep(0.01)
                    yield root, state, attempts
                finally:
                    os.close(lock)

    def _read(self, state, plan):
        try:
            fd = os.open(plan.identity.key + ".json", os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=state)
        except FileNotFoundError:
            return None
        try:
            _protected(fd, directory=False)
            stream = os.fdopen(fd, "rb")
        except BaseException:
            os.close(fd)
            raise
        with stream:
            raw = stream.read(MAX_CONTROL_BYTES + 1)
        try:
            record = json.loads(raw, object_pairs_hook=_unique) if len(raw) <= MAX_CONTROL_BYTES else None
            fields = {"schema_version", "identity_key", "plan_sha256", "plan", "phase", "outcome",
                      "cache_root", "workspace_root", "pin_holder",
                      "pins_released", "work_name", "created_ns", "updated_ns"}
            if (type(record) is not dict or set(record) != fields or type(record["schema_version"]) is not int
                    or record["schema_version"] != 1 or record["identity_key"] != plan.identity.key
                    or record["cache_root"] != str(self.cache.root) or record["workspace_root"] != str(self.root)
                    or record["pin_holder"] != self.pin_holder(plan)
                    or record["plan_sha256"] != plan.sha256 or encoded(record["plan"]) != encoded(plan.document())
                    or record["phase"] not in PHASES or type(record["pins_released"]) is not bool
                    or any(type(record[field]) is not int or record[field] < 1 for field in ("created_ns", "updated_ns"))
                    or record["updated_ns"] < record["created_ns"] or encoded(record) != raw):
                raise ValueError()
            if record["phase"] in ("ABORTING", "TERMINAL"):
                if record["outcome"] not in OUTCOMES:
                    raise ValueError()
            elif record["outcome"] is not None:
                raise ValueError()
            if record["pins_released"] and record["phase"] != "TERMINAL":
                raise ValueError()
            name = record["work_name"]
            if name is not None and (type(name) is not str or not (name == plan.identity.key
                    or re.fullmatch(r"\.building-" + plan.identity.key + "-[0-9a-f]{32}", name))):
                raise ValueError()
            if record["phase"] in ("READY", "STARTED") and name != plan.identity.key:
                raise ValueError()
            if record["phase"] == "MATERIALIZING" and name is None:
                raise ValueError()
            return record
        except (ValueError, TypeError, KeyError, RecursionError) as error:
            raise WorkspaceError("Invalid/conflicting control record; never reset it") from error

    def _save(self, state, plan, record, *, new=False):
        name = ".control-" + secrets.token_hex(16) + ".tmp"
        fd = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=state)
        moved = False
        try:
            with os.fdopen(fd, "wb") as stream:
                stream.write(encoded(record))
                stream.flush()
                os.fsync(stream.fileno())
            target = plan.identity.key + ".json"
            if new:
                _rename_noreplace(state, name, state, target)
            else:
                os.replace(name, target, src_dir_fd=state, dst_dir_fd=state)
            moved = True
            os.fsync(state)
        finally:
            if not moved:
                os.unlink(name, dir_fd=state)  # Only this O_EXCL-created control temp.

    def _new(self, plan, *, phase, outcome=None, work_name=None):
        now = time.time_ns()
        return dict(schema_version=1, identity_key=plan.identity.key, plan_sha256=plan.sha256,
                    cache_root=str(self.cache.root), workspace_root=str(self.root), pin_holder=self.pin_holder(plan),
                    plan=plan.document(), phase=phase, outcome=outcome, pins_released=False,
                    work_name=work_name, created_ns=now, updated_ns=now)

    @staticmethod
    def _authority(callback, plan):
        if not callable(callback) or callback(plan) is not True:
            raise PermissionError("Current exact job/manifest/attempt authority is required")

    @contextmanager
    def _relative(self, base, parts, *, create=False):
        with ExitStack() as stack:
            current = base
            for part in parts:
                current = stack.enter_context(_child(current, part, create=create))
            yield current

    def _copy_file(self, base, path, sha, size, executable, plan, authorize):
        with self.cache.metrics.span("copy_ns"):
            return self._copy_file_impl(base, path, sha, size, executable, plan, authorize)

    def _copy_file_impl(self, base, path, sha, size, executable, plan, authorize):
        parts = path.split("/")
        with self._relative(base, parts[:-1]) as parent, self.cache.open_verified(sha, size) as source:
            before = _signature(os.fstat(source.fileno()))
            temp = ".input-" + secrets.token_hex(16) + ".part"
            fd = os.open(temp, os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=parent)
            try:
                _protected(fd, directory=False)
                outgoing = os.fdopen(fd, "w+b")
            except BaseException:
                os.close(fd)
                raise
            with outgoing:
                total = 0
                while True:
                    self._authority(authorize, plan)
                    chunk = source.read(min(CHUNK_BYTES, size - total + 1))
                    if not chunk:
                        break
                    total += len(chunk)
                    if total > size:
                        raise WorkspaceError("Cache replica grew during private copy")
                    outgoing.write(chunk)
                outgoing.flush()
                outgoing.seek(0)
                with self.cache.metrics.span("verification_ns"):
                    _verify(outgoing, sha, size)
                if _signature(os.fstat(source.fileno())) != before:
                    raise WorkspaceError("Cache inode changed during private copy")
                os.fchmod(outgoing.fileno(), 0o500 if executable else 0o400)
                os.fsync(outgoing.fileno())
            self._authority(authorize, plan)
            _rename_noreplace(parent, temp, parent, parts[-1])
            os.fsync(parent)
            # On failure retain this attempt's partial tree for review, never READY.

    def _trees(self, plan):
        source = [DatasetEntry(path, "directory") for path in plan.source.directories]
        source.extend(DatasetEntry(file.path, "file", file.sha256, file.size_bytes) for file in plan.source.files)
        return (("source", tuple(source), {file.path for file in plan.source.files if file.executable}),
                ("data", plan.dataset.entries if plan.dataset else (), set()))

    def _build(self, work, plan, authorize):
        with _child(work, "inputs", create=True) as inputs, \
                _child(work, "outputs", create=True) as outputs, _child(work, "checkpoints", create=True) as checkpoints:
            for name, entries, executable in self._trees(plan):
                with _child(inputs, name, create=True) as tree:
                    for entry in sorted(entries, key=lambda entry: (entry.path.count("/"), entry.path)):
                        if entry.type == "directory":
                            with self._relative(tree, entry.path.split("/"), create=True) as directory:
                                os.fsync(directory)
                        else:
                            self._copy_file(tree, entry.path, entry.sha256, entry.size_bytes,
                                            entry.path in executable, plan, authorize)
                    os.fsync(tree)
            os.fsync(inputs)
            os.fsync(outputs)
            os.fsync(checkpoints)
        os.fsync(work)

    def _verify_tree(self, tree, entries, executable):
        expected = {entry.path: entry for entry in entries}
        actual = set()
        def walk(parent, prefix=""):
            with os.scandir(parent) as children:
                for child in children:
                    path = prefix + child.name
                    if path not in expected or path in actual:
                        raise WorkspaceError("Unexpected private input entry")
                    actual.add(path)
                    entry = expected[path]
                    if entry.type == "directory":
                        with _child(parent, child.name) as directory:
                            walk(directory, path + "/")
                    else:
                        fd = os.open(child.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
                        try:
                            info = _protected(fd, directory=False)
                            if stat.S_IMODE(info.st_mode) != (0o500 if path in executable else 0o400):
                                raise WorkspaceError("Unexpected private input mode")
                            incoming = os.fdopen(fd, "rb")
                        except BaseException:
                            os.close(fd)
                            raise
                        with incoming:
                            with self.cache.metrics.span("verification_ns"):
                                _verify(incoming, entry.sha256, entry.size_bytes)
            if not prefix and actual != set(expected):
                raise WorkspaceError("Private inputs are incomplete")
        walk(tree)

    def _receipt(self, attempts, plan):
        with _child(attempts, plan.identity.key) as work:
            if set(os.listdir(work)) != {"inputs", "outputs", "checkpoints"}:
                raise WorkspaceError("Unexpected attempt workspace layout")
            with _child(work, "inputs") as inputs, _child(work, "outputs"), _child(work, "checkpoints"):
                if set(os.listdir(inputs)) != {"source", "data"}:
                    raise WorkspaceError("Unexpected private input layout")
                for name, entries, executable in self._trees(plan):
                    with _child(inputs, name) as tree:
                        self._verify_tree(tree, entries, executable)
        return WorkspaceReceipt(plan.identity.key, plan.sha256,
                                self.root / "attempts" / plan.identity.key, plan.dataset is not None)

    def _pins_present(self, plan):
        with self.cache._session() as (db, _, __):
            for sha in {file.sha256 for file in plan.inputs()}:
                if not db.execute("SELECT 1 FROM cache_pins WHERE job_id=? AND attempt_id=? AND sha256=?",
                                  (plan.identity.job_id, self.pin_holder(plan), sha)).fetchone():
                    raise WorkspaceBusy("Exact workspace pin is missing; reconcile rather than launch")

    def prepare(self, plan: WorkspacePlan, *, authorize: Callable):
        try:
            return self._prepare(plan, authorize=authorize)
        except BaseException:
            self.cache.metrics.add(workspace_failures=1)
            raise

    def _prepare(self, plan: WorkspacePlan, *, authorize: Callable):
        self._authority(authorize, plan)
        with self._locked(plan) as (root, state, attempts):
            self._authority(authorize, plan)
            record = self._read(state, plan)
            if record is not None:
                if record["phase"] != "READY":
                    raise WorkspaceBusy("Existing attempt is active/interrupted/terminal; reconcile or use a new revision")
                try:
                    self._pins_present(plan)
                    receipt = self._receipt(attempts, plan)
                    self._authority(authorize, plan)
                    return receipt
                except BaseException:
                    self._finish(state, plan, "STAGING_FAILED", lambda _: True)
                    raise
            name = ".building-" + plan.identity.key + "-" + secrets.token_hex(16)
            record = self._new(plan, phase="MATERIALIZING", work_name=name)
            self._save(state, plan, record, new=True)  # Durable intent BEFORE pins.
            try:
                free = os.fstatvfs(root)
                if (plan.input_bytes > self.max_input_bytes
                        or free.f_bavail * free.f_frsize < plan.input_bytes + self.reserve_free_bytes):
                    raise OSError(errno.ENOSPC, "Private copies exceed input/free-space admission")
                unique = {file.sha256: file for file in plan.inputs()}
                for file in unique.values():
                    self._authority(authorize, plan)
                    self.cache.pin(file.sha256, file.size_bytes, job_id=plan.identity.job_id,
                                   attempt_id=self.pin_holder(plan))
                os.mkdir(name, 0o700, dir_fd=attempts)  # Never adopt an existing tree.
                os.fsync(attempts)
                with _child(attempts, name) as work:
                    self._build(work, plan, authorize)
                self._authority(authorize, plan)
                _rename_noreplace(attempts, name, attempts, plan.identity.key)
                os.fsync(attempts)
                receipt = self._receipt(attempts, plan)
                self._authority(authorize, plan)
                record.update(phase="READY", work_name=plan.identity.key,
                              updated_ns=max(record["updated_ns"], time.time_ns()))
                self._save(state, plan, record)
                self.cache.metrics.add(workspace_copy_bytes=plan.input_bytes)
                return receipt
            except BaseException:
                # Synchronous materializer has unwound; no trainer can start
                # through this manager while this exact-attempt lock is held.
                self._finish(state, plan, "STAGING_FAILED", lambda _: True)
                raise

    def mark_started(self, plan, *, authorize: Callable):
        self._authority(authorize, plan)
        with self._locked(plan) as (_, state, attempts):
            record = self._read(state, plan)
            if record is None or record["phase"] != "READY":
                raise WorkspaceBusy("Only one exact READY attempt may enter STARTED")
            self._pins_present(plan)
            receipt = self._receipt(attempts, plan)
            self._authority(authorize, plan)
            record.update(phase="STARTED", updated_ns=max(record["updated_ns"], time.time_ns()))
            self._save(state, plan, record)
            return receipt

    def _finish(self, state, plan, outcome, quiescent):
        record = self._read(state, plan)
        if record is not None and record["phase"] == "TERMINAL" and record["pins_released"]:
            return record
        if record is None:
            record = self._new(plan, phase="ABORTING", outcome=outcome)
            self._save(state, plan, record, new=True)
        elif record["phase"] not in ("ABORTING", "TERMINAL"):
            record.update(phase="ABORTING", outcome=outcome,
                          updated_ns=max(record["updated_ns"], time.time_ns()))
            self._save(state, plan, record)
        # Expired/cancelled authority must still permit cleanup. This callback
        # proves exact producer/trainer/descendant quiescence, not a live lease.
        if quiescent(plan.identity) is not True:
            return record  # Durable ABORTING; retain all pins.
        if record["phase"] != "TERMINAL":
            record.update(phase="TERMINAL", updated_ns=max(record["updated_ns"], time.time_ns()))
            self._save(state, plan, record)  # Tombstone BEFORE releasing pins.
        self.cache.unpin(job_id=plan.identity.job_id, attempt_id=self.pin_holder(plan))
        record.update(pins_released=True, updated_ns=max(record["updated_ns"], time.time_ns()))
        self._save(state, plan, record)
        return record

    def finish(self, plan, outcome: str, *, quiescent: Callable):
        """All terminal paths converge here; first terminal reason wins.

        Failed proof retains ABORTING/pins. Cache failure retains a TERMINAL
        tombstone with pins_released=False. Exact retries safely finish cleanup.
        No files/outputs/checkpoints are deleted, even after successful release.
        """
        if outcome not in OUTCOMES or not callable(quiescent):
            raise ValueError("Expected a terminal outcome and exact quiescence callback")
        with self._locked(plan) as (_, state, __):
            return self._finish(state, plan, outcome, quiescent)

    def status(self, plan):
        """Read exact protected control state; no byte verification or authority."""
        with self._locked(plan) as (_, state, __):
            return self._read(state, plan)

    @contextmanager
    def attempt(self, plan, *, authorize: Callable, quiescent: Callable):
        """Managed lifecycle for a future trusted launcher using existing leases.

        Caller launches/monitors with existing guardians, calls mark_started
        at its fenced launch boundary, and proves all descendants stopped.
        This method never launches code or interprets a receipt as permission.
        """
        if not callable(quiescent):
            raise ValueError("Exact quiescence callback required")
        receipt = self.prepare(plan, authorize=authorize)
        try:
            yield receipt
        except BaseException as error:
            outcome = ("CANCELLED" if isinstance(error, WorkspaceCancelled) else
                       "LEASE_EXPIRED" if isinstance(error, WorkspaceLeaseExpired) else
                       "LAUNCH_FAILED" if isinstance(error, WorkspaceLaunchFailed) else "FAILED")
            self.finish(plan, outcome, quiescent=quiescent)
            raise
        else:
            record = self.finish(plan, "COMPLETED", quiescent=quiescent)
            if not record["pins_released"]:
                raise WorkspaceBusy("Attempt is not quiescent; terminal cleanup remains pending")
