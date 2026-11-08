"""Explicit trusted-worker cache inventory. No fetch, eviction or runtime activation.

Only service-protected, administrator-provisioned local roots are accepted. A
digest is not authorization: callers must first authorize the exact job inputs.
Persistent observations/pins are advisory; open_verified always hashes the bytes.
"""

from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
import errno
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import stat
import time
from typing import BinaryIO, Iterator

from .backends import _directory, _root
from .inputs import object_key
from .mount_identity import require_local_fd
from .metrics import StorageMetrics

CHUNK_BYTES = 1024 * 1024
MAX_INTEGER = 2**63 - 1
DDL = (
    "CREATE TABLE cache_meta (version INTEGER PRIMARY KEY CHECK(version=1))",
    """CREATE TABLE cache_objects (
        sha256 TEXT PRIMARY KEY NOT NULL CHECK(length(sha256)=64 AND sha256 NOT GLOB '*[^0-9a-f]*'),
        size_bytes INTEGER NOT NULL CHECK(size_bytes>=0),
        state TEXT NOT NULL CHECK(state IN ('verified','missing','corrupt')),
        last_verified_ns INTEGER, last_used_ns INTEGER, fingerprint TEXT)""",
    """CREATE TABLE cache_pins (
        job_id TEXT NOT NULL, attempt_id TEXT NOT NULL,
        sha256 TEXT NOT NULL REFERENCES cache_objects(sha256),
        PRIMARY KEY(job_id,attempt_id,sha256))""",
    "CREATE INDEX cache_pins_digest ON cache_pins(sha256)",
)


class CacheError(ValueError):
    """Invalid cache metadata or conflicting trusted input identity."""


class CacheMiss(CacheError):
    """No complete verified local replica; later staging may fetch it."""


class CacheCorrupt(CacheMiss):
    """Replica bytes or identity changed. Retained untouched for later repair."""


@dataclass(frozen=True)
class CacheRecord:
    sha256: str
    size_bytes: int
    state: str
    last_verified_ns: int | None
    last_used_ns: int | None
    pinned_references: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class CacheSpace:
    total_bytes: int
    free_bytes: int
    available_bytes: int
    observed_verified_bytes: int  # Advisory DB sum, not quota/physical usage.


def _protected(fd: int, *, directory: bool) -> os.stat_result:
    require_local_fd(fd)  # Reject remote, overlay, tmpfs and unknown filesystems.
    info = os.fstat(fd)
    valid_type = stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode)
    if (not valid_type or info.st_uid != os.geteuid() or info.st_mode & 0o077
            or (not directory and info.st_nlink != 1)):
        raise PermissionError("Cache state must be private, service-owned and not linked")
    return info


@contextmanager
def _child(parent: int, name: str, *, create: bool = False) -> Iterator[int]:
    if create:
        try:
            os.mkdir(name, 0o700, dir_fd=parent)
        except FileExistsError:
            pass
        os.fsync(parent)
    fd = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
    try:
        _protected(fd, directory=True)
        if os.fstat(parent).st_dev != os.fstat(fd).st_dev:
            raise OSError(errno.EXDEV, "Cache children must share the local root filesystem")
        yield fd
    finally:
        os.close(fd)


def _private_file(parent: int, name: str, *, create: bool) -> int:
    flags = os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK
    try:
        fd = os.open(name, flags | (os.O_CREAT | os.O_EXCL if create else 0), 0o600, dir_fd=parent)
    except FileExistsError:
        fd = os.open(name, flags, dir_fd=parent)
    try:
        _protected(fd, directory=False)
        return fd
    except BaseException:
        os.close(fd)
        raise


def _signature(info: os.stat_result) -> str:
    return json.dumps([getattr(info, field) for field in
                       ("st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns")])


def _expected(sha256: str, size: int) -> None:
    object_key(sha256)
    if type(size) is not int or not 0 <= size <= MAX_INTEGER:
        raise ValueError("Expected size must be a nonnegative SQLite integer")


def _reference(value: str) -> None:
    if (type(value) is not str or value in (".", "..")
            or not re.fullmatch(r"[A-Za-z0-9._-]{1,128}", value)):
        raise ValueError("Expected a controlled job/attempt identifier")


class WorkerCache:
    """Persistent local observations and conservative pins; construct without I/O.

    root must already exist with mode 0700, owned by the staging service. Only
    initialize() creates objects/, state/ and the dedicated cache.sqlite3. Never
    point this at existing jobs, checkpoints, an NFS mount or a trainer directory.
    input_mount is an explicit operator path, used only to reject overlapping
    trees; actual local filesystem admission examines opened FDs, not spelling.
    Service-protected ancestors and no untrusted same-UID writers are required.
    """

    def __init__(self, root: Path, *, input_mount: Path, verify_after_seconds: float = 300,
                 metrics: StorageMetrics | None = None):
        self.root, self.input_mount = _root(root), _root(input_mount)
        if self.root.is_relative_to(self.input_mount) or self.input_mount.is_relative_to(self.root):
            raise ValueError("Local cache and NFS input mount must not overlap")
        if (type(verify_after_seconds) not in (int, float) or not math.isfinite(verify_after_seconds)
                or not 0 <= verify_after_seconds <= 365 * 24 * 3600):
            raise ValueError("Verification age must be finite and between zero and one year")
        self.verify_after_ns = int(verify_after_seconds * 1_000_000_000)
        if metrics is not None and not isinstance(metrics, StorageMetrics):
            raise ValueError("Expected an explicit StorageMetrics collector or None")
        self.metrics = metrics if metrics is not None else StorageMetrics()

    @contextmanager
    def _session(self, *, initialize: bool = False):
        with _directory(self.root) as root:
            _protected(root, directory=True)
            with _child(root, "objects", create=initialize) as objects, \
                    _child(root, "state", create=initialize) as state:
                lock = _private_file(state, "cache.lock", create=initialize)
                try:
                    deadline = time.monotonic() + 10
                    while True:
                        try:
                            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                            break
                        except BlockingIOError:
                            if time.monotonic() >= deadline:
                                raise TimeoutError("Cache metadata busy; retry outside lease/API threads")
                            time.sleep(0.01)
                    # SQLite opens paths itself. Protected parent + no-follow
                    # checks for every known DB/sidecar are a prerequisite.
                    db_fd = _private_file(state, "cache.sqlite3", create=initialize)
                    os.close(db_fd)
                    for name in ("cache.sqlite3-journal", "cache.sqlite3-wal", "cache.sqlite3-shm"):
                        try:
                            sidecar = _private_file(state, name, create=False)
                        except FileNotFoundError:
                            continue
                        os.close(sidecar)
                    db = sqlite3.connect(f"/proc/self/fd/{state}/cache.sqlite3", timeout=10)
                    try:
                        db.execute("PRAGMA foreign_keys=ON")
                        db.execute("PRAGMA synchronous=FULL")
                        db.execute("BEGIN IMMEDIATE")
                        actual = sorted(row[0] for row in db.execute(
                            "SELECT sql FROM sqlite_master WHERE sql IS NOT NULL"))
                        version = db.execute("PRAGMA user_version").fetchone()[0]
                        if not actual and version == 0 and initialize:
                            for statement in DDL:
                                db.execute(statement)
                            db.execute("INSERT INTO cache_meta VALUES (1)")
                            db.execute("PRAGMA user_version=1")
                        elif (actual != sorted(DDL) or version != 1
                              or db.execute("SELECT version FROM cache_meta").fetchall() != [(1,)]):
                            raise CacheError("Incompatible cache schema; never reset or overwrite it")
                        yield db, objects, root
                        db.commit()
                        os.fsync(state)
                    except BaseException:
                        db.rollback()
                        raise
                    finally:
                        db.close()
                finally:
                    os.close(lock)

    def initialize(self) -> None:
        """Explicit, idempotent v1 setup. No cache scan, deletion or migration."""
        with self._session(initialize=True) as (db, _, __):
            if db.execute("PRAGMA quick_check").fetchall() != [("ok",)]:
                raise CacheError("Cache metadata integrity check failed")

    def _record(self, db, sha: str) -> CacheRecord | None:
        row = db.execute("SELECT sha256,size_bytes,state,last_verified_ns,last_used_ns "
                         "FROM cache_objects WHERE sha256=?", (sha,)).fetchone()
        if row is None:
            return None
        pins = tuple(db.execute("SELECT job_id,attempt_id FROM cache_pins WHERE sha256=? "
                                "ORDER BY job_id,attempt_id", (sha,)))
        return CacheRecord(*row, pins)

    def inventory(self) -> tuple[CacheRecord, ...]:
        """Advisory persisted observations only, never a staging/ACL grant."""
        with self._session() as (db, _, __):
            return tuple(self._record(db, row[0]) for row in db.execute(
                "SELECT sha256 FROM cache_objects ORDER BY sha256").fetchall())

    def space(self) -> CacheSpace:
        """Current statvfs accounting; no reservation, quota or eviction policy."""
        with self._session() as (db, _, root):
            values = os.fstatvfs(root)
            observed = db.execute("SELECT COALESCE(SUM(size_bytes),0) FROM cache_objects "
                                  "WHERE state='verified'").fetchone()[0]
            return CacheSpace(values.f_blocks * values.f_frsize, values.f_bfree * values.f_frsize,
                              values.f_bavail * values.f_frsize, observed)

    @contextmanager
    def _object(self, objects: int, sha: str):
        with _child(objects, "sha256") as algorithm, _child(algorithm, sha[:2]) as prefix:
            fd = os.open(sha, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=prefix)
            try:
                info = os.fstat(fd)
                require_local_fd(fd)
                if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
                        or info.st_mode & 0o222 or info.st_nlink != 1
                        or info.st_dev != os.fstat(objects).st_dev):
                    raise PermissionError("Cache objects must be local, service-owned read-only regular files without links")
                stream = os.fdopen(fd, "rb")
            except BaseException:
                os.close(fd)
                raise
            with stream:
                yield stream

    def _check(self, db, stream, sha: str, size: int, *, force: bool, touch: bool):
        now, before = time.time_ns(), os.fstat(stream.fileno())
        old = db.execute("SELECT state,last_verified_ns,fingerprint FROM cache_objects "
                         "WHERE sha256=?", (sha,)).fetchone()
        recent = (old is not None and old[0] == "verified" and old[1] is not None
                  and 0 <= now - old[1] < self.verify_after_ns and old[2] == _signature(before))
        if before.st_size != size:
            raise CacheCorrupt("Cached size differs from the authorized manifest")
        if force or not recent:
            with self.metrics.span("verification_ns"):
                digest, total = hashlib.sha256(), 0
                while True:
                    chunk = stream.read(min(CHUNK_BYTES, size - total + 1))
                    if not chunk:
                        break
                    total += len(chunk)
                    if total > size:
                        raise CacheCorrupt("Cache object grew during verification")
                    digest.update(chunk)
                if (total != size or digest.hexdigest() != sha
                        or _signature(before) != _signature(os.fstat(stream.fileno()))):
                    raise CacheCorrupt("Cached digest/size/identity verification failed")
            db.execute("UPDATE cache_objects SET state='verified',last_verified_ns=?,fingerprint=? "
                       "WHERE sha256=?", (time.time_ns(), _signature(before), sha))
        if touch:
            db.execute("UPDATE cache_objects SET last_used_ns=? WHERE sha256=?", (time.time_ns(), sha))
        stream.seek(0)

    @contextmanager
    def _verified(self, sha: str, size: int, *, force: bool, touch: bool, reference=None):
        _expected(sha, size)
        if type(force) is not bool:
            raise ValueError("force must be boolean")
        # Keep the opened, verified inode alive after committing observations.
        with ExitStack() as opened:
            error = None
            with self._session() as (db, objects, _):
                row = db.execute("SELECT size_bytes FROM cache_objects WHERE sha256=?", (sha,)).fetchone()
                if row is not None and row[0] != size:
                    raise CacheError("Conflicting sizes for one digest; do not reinterpret existing pins")
                db.execute("INSERT OR IGNORE INTO cache_objects(sha256,size_bytes,state) VALUES (?,?,'missing')",
                           (sha, size))
                try:
                    stream = opened.enter_context(self._object(objects, sha))
                    self._check(db, stream, sha, size, force=force, touch=touch)
                except (OSError, CacheCorrupt) as failure:
                    if isinstance(failure, CacheCorrupt):
                        self.metrics.add(cache_corruptions=1)
                    state = "missing" if isinstance(failure, FileNotFoundError) else "corrupt"
                    db.execute("UPDATE cache_objects SET state=?,fingerprint=NULL WHERE sha256=?", (state, sha))
                    error = CacheMiss("Cache object missing") if state == "missing" else failure
                if error is None and reference is not None:
                    db.execute("INSERT OR IGNORE INTO cache_pins VALUES (?,?,?)", (*reference, sha))
                record = self._record(db, sha)
            if error is not None:
                raise error
            yield stream, record

    def verify(self, sha256: str, expected_size: int, *, force: bool = True) -> CacheRecord:
        """Hash and enroll a COMPLETE existing replica. No copy/rename/fetch.

        Explicit force=False allows recent same-inode metadata observations to
        avoid hashing; stale/changed/future timestamps always trigger hashing.
        Never use that optimization for staging; open_verified has no opt-out.
        Missing/corrupt entries remain invalid with pins preserved. I/O fails.
        """
        with self._verified(sha256, expected_size, force=force, touch=False) as (_, record):
            return record

    @contextmanager
    def open_verified(self, sha256: str, expected_size: int) -> Iterator[BinaryIO]:
        """Stage-time read: always fully hash, then rewind the same read-only FD."""
        with self._verified(sha256, expected_size, force=True, touch=True) as (stream, _):
            yield stream

    def pin(self, sha256: str, expected_size: int, *, job_id: str, attempt_id: str) -> CacheRecord:
        """Persist an idempotent exact-attempt reference only after full verification."""
        _reference(job_id)
        _reference(attempt_id)
        with self._verified(sha256, expected_size, force=True, touch=True,
                            reference=(job_id, attempt_id)) as (_, record):
            return record

    def unpin(self, *, job_id: str, attempt_id: str) -> int:
        """Release only this exact attempt's references. Does not delete bytes.

        No lease assertion here: the future trusted staging lifecycle owns it.
        Reboot never clears pins automatically, since that could evict live data.
        """
        _reference(job_id)
        _reference(attempt_id)
        with self._session() as (db, _, __):
            return db.execute("DELETE FROM cache_pins WHERE job_id=? AND attempt_id=?",
                              (job_id, attempt_id)).rowcount
