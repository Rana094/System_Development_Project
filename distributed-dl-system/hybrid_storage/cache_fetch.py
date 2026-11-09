"""Explicit authorized missing-only NFS cache copy; no HTTP or runtime activation.

One transfer per cache root across processes bounds NFS I/O and serializes quota
admission. Helpers write only private parts. Only the parent may publish after
reaping, local verification and renewed authorization. Timed-out helpers retain
their slot/part until reaped; they can never publish a late object.
"""

from contextlib import ExitStack
from dataclasses import dataclass
import errno
import fcntl
import json
import math
import os
import re
from pathlib import Path
import secrets
import stat
import subprocess
import sys
import tempfile
import threading
import time
from typing import Callable, Iterable

from .artifacts import ArtifactMismatch, _rename_noreplace, _verify
from .backends import _directory
from .inputs import InputFile
from . import events
from .nfs_mount import MountNotReady, validate_source
from .worker_cache import (CacheCorrupt, CacheError, CacheMiss, WorkerCache,
                           MAX_INTEGER, _child, _private_file, _protected)

TRANSIENT = frozenset((errno.EIO, errno.ESTALE, errno.ETIMEDOUT, errno.EAGAIN,
                       errno.ECONNRESET, errno.ENETUNREACH, errno.EHOSTUNREACH))


class CacheFetchBusy(CacheError):
    """A prior owned transfer or another process still holds the cache slot."""


class CacheCapacityError(OSError):
    """Quota/free-space admission failed; no input transfer should start."""


class CacheFetchTimeout(TimeoutError):
    """Caller deadline elapsed; inspect pending() and keep reconciling ownership."""


@dataclass(frozen=True)
class FetchResult:
    sha256: str
    size_bytes: int
    cache_hit: bool
    attempts: int


class CacheFetcher:
    """Inactive trusted-service component with one owned NFS producer per root.

    initialize() is explicit. All paths/settings are operator-owned. ensure()
    requires exact authorized InputFiles plus a current local authority callback;
    this is not a student route, controller grant protocol or launch permission.
    No caller-managed thread pool, unbounded job queue or background retry loop.
    """

    def __init__(self, cache: WorkerCache, *, source: str, quota_bytes: int,
                 reserve_free_bytes: int = 64 * 1024 * 1024, attempts: int = 3,
                 io_timeout: float = 10, lock_timeout: float = 2, retry_delay: float = 0.1,
                 transfer_metrics=None):
        if not isinstance(cache, WorkerCache):
            raise ValueError("Expected an explicit WorkerCache")
        self.cache, self.source = cache, validate_source(source)
        if transfer_metrics is not None:
            from .transfer_metrics import TransferMetrics
            if not isinstance(transfer_metrics, TransferMetrics):
                raise ValueError('Expected explicit worker-local TransferMetrics')
        self.transfer_metrics = transfer_metrics
        for name, value in (("quota_bytes", quota_bytes), ("reserve_free_bytes", reserve_free_bytes)):
            if type(value) is not int or not 0 <= value <= MAX_INTEGER or (name == "quota_bytes" and not value):
                raise ValueError(name + " must be a bounded byte count")
        if type(attempts) is not int or not 1 <= attempts <= 5:
            raise ValueError("attempts must be between 1 and 5")
        for name, value, limit in (("io_timeout", io_timeout, 300), ("lock_timeout", lock_timeout, 30),
                                   ("retry_delay", retry_delay, 5)):
            if (type(value) not in (int, float) or not math.isfinite(value)
                    or not 0 <= value <= limit or (name != "retry_delay" and not value)):
                raise ValueError(name + " is outside the finite supported range")
        self.quota_bytes, self.reserve_free_bytes, self.attempts = quota_bytes, reserve_free_bytes, attempts
        self.io_timeout, self.lock_timeout, self.retry_delay = io_timeout, lock_timeout, retry_delay
        self._operation = threading.Lock()
        self._guard = threading.RLock()
        self._owned = {}
        self._closed = False

    def initialize(self) -> None:
        """Add only private parts/quarantine and slot file; no new SQLite schema."""
        self.cache.initialize()
        with _directory(self.cache.root) as root:
            _protected(root, directory=True)
            with _child(root, "parts", create=True), _child(root, "quarantine", create=True), \
                    _child(root, "state") as state:
                fd = _private_file(state, "fetch.lock", create=True)
                os.close(fd)
                os.fsync(state)

    def _authorize(self, callback, inputs):
        with self._guard:
            if self._closed:
                raise CacheError("Cache fetcher is closed")
        if callback(inputs) is not True:
            raise PermissionError("Exact manifest/attempt authority is absent or expired")

    def _slot(self, stack):
        root = stack.enter_context(_directory(self.cache.root))
        _protected(root, directory=True)
        objects = stack.enter_context(_child(root, "objects"))
        state = stack.enter_context(_child(root, "state"))
        parts = stack.enter_context(_child(root, "parts"))
        quarantine = stack.enter_context(_child(root, "quarantine"))
        lock = _private_file(state, "fetch.lock", create=False)
        stack.callback(os.close, lock)
        end = time.monotonic() + self.lock_timeout
        while True:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= end:
                    raise CacheFetchBusy("Another transfer owns this cache; retry later")
                time.sleep(0.01)
        return root, objects, parts, quarantine, lock

    def _usage(self, fd: int, budget: list[int], depth: int = 0, *, mount_id=None) -> int:
        """Conservative logical bytes, including crash parts/quarantined replicas."""
        total = 0
        if mount_id is not None:
            from .mount_identity import require_local_fd
            if require_local_fd(fd)['id'] != mount_id:
                raise PermissionError('Cache maintenance refuses nested mounts')
        if depth > 4:
            raise CacheError("Unexpectedly deep cache accounting tree")
        with os.scandir(fd) as entries:
            for entry in entries:
                budget[0] -= 1
                if budget[0] < 0:
                    raise CacheError("Cache accounting exceeds 100000 entries; operator review required")
                info = os.stat(entry.name, dir_fd=fd, follow_symlinks=False)
                if stat.S_ISDIR(info.st_mode):
                    with _child(fd, entry.name) as child:
                        total += self._usage(child, budget, depth + 1, mount_id=mount_id)
                elif (stat.S_ISREG(info.st_mode) and info.st_uid == os.geteuid()
                      and info.st_nlink == 1 and not info.st_mode & 0o022):
                    total += info.st_size
                else:
                    raise PermissionError("Unsafe cache entry during quota accounting")
        return total

    def _admit(self, root, objects, parts, quarantine, size):
        budget = [100000]
        used = sum(self._usage(fd, budget) for fd in (objects, parts, quarantine))
        if used + size > self.quota_bytes:
            raise CacheCapacityError(errno.EDQUOT, "Cache quota includes objects, parts and quarantine")
        free = os.fstatvfs(root)
        if free.f_bavail * free.f_frsize < size + self.reserve_free_bytes:
            raise CacheCapacityError(errno.ENOSPC, "Insufficient service-available disk plus reserve")

    def _spawn(self, request, output, lock):
        return subprocess.Popen(
            [sys.executable, "-I", "-B", str(Path(__file__).with_name("cache_copy_worker.py")), json.dumps(request)],
            stdin=subprocess.DEVNULL, stdout=output, stderr=subprocess.DEVNULL,
            pass_fds=(request["output_fd"], lock), start_new_session=True,
        )

    @staticmethod
    def _cleanup(owned):
        """Remove only this invocation's O_EXCL-created name after producer exit."""
        try:
            actual = os.stat(owned["name"], dir_fd=owned["parts"], follow_symlinks=False)
        except FileNotFoundError:
            return
        original = os.fstat(owned["fd"])
        if (actual.st_dev, actual.st_ino) != (original.st_dev, original.st_ino):
            raise CacheError("Part identity changed; retain ownership for operator reconciliation")
        os.unlink(owned["name"], dir_fd=owned["parts"])
        os.fsync(owned["parts"])

    def pending(self) -> list[dict]:
        with self._guard:
            return [{"copy_id": key, "pid": value["process"].pid, "expired": value["expired"],
                     "detached": value["detached"]} for key, value in self._owned.items()]

    def reconcile(self) -> list[str]:
        """Reap/clean expired owned helpers only; late readiness never publishes."""
        reaped = []
        with self._guard:
            for key, owned in list(self._owned.items()):
                if owned["detached"] and owned["process"].poll() is not None:
                    self._cleanup(owned)
                    owned["stack"].close()
                    del self._owned[key]
                    reaped.append(key)
        return reaped

    def close(self) -> list[dict]:
        """Stop admission, signal owned helpers, retain unreaped producers/parts."""
        with self._guard:
            self._closed = True
            for owned in self._owned.values():
                owned["expired"] = True
                try:
                    owned["process"].kill()
                except ProcessLookupError:
                    pass
        self.reconcile()
        return self.pending()

    def interrupt(self):
        """Fence/kill current producers without closing future attempt admission."""
        with self._guard:
            for owned in self._owned.values():
                owned['expired']=True
                try:owned['process'].kill()
                except ProcessLookupError:pass
        self.reconcile()

    def reconcile_orphans(self):
        """Reap known helpers, then prove cache-wide producer quiescence.

        An inherited fetch-slot flock fences helpers orphaned by agent death.
        Delete only private, single-link O_EXCL-style .part names in parts/;
        no cache objects, workspaces or unknown files are removed.
        """
        self.reconcile()
        if not self._operation.acquire(blocking=False):return False
        try:
            if self.pending():return False
            with ExitStack() as stack:
                try:_,__,parts,___,____=self._slot(stack)
                except CacheFetchBusy:return False
                for name in os.listdir(parts):
                    if not re.fullmatch(r'[0-9a-f]{64}\.[0-9a-f]{32}\.part',name):continue
                    fd=os.open(name,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK,dir_fd=parts)
                    try:
                        info=_protected(fd,directory=False)
                        current=os.stat(name,dir_fd=parts,follow_symlinks=False)
                        if (info.st_dev,info.st_ino)!=(current.st_dev,current.st_ino):raise CacheError('Orphan part changed')
                        os.unlink(name,dir_fd=parts)
                    finally:os.close(fd)
                os.fsync(parts)
                return True
        finally:self._operation.release()

    def _copy(self, stack, sha, size, parts, lock):
        self.cache.metrics.add(copy_attempts=1)
        started = None
        if self.transfer_metrics is not None:
            try:
                started = time.perf_counter_ns()
            except Exception:
                pass  # Optional measurement must not change copy authority/outcome.
        try:
            with self.cache.metrics.span("copy_ns"):
                owned = self._copy_impl(stack, sha, size, parts, lock)
        except BaseException as error:
            self.cache.metrics.add(copy_failures=1, copy_timeouts=int(isinstance(error, CacheFetchTimeout)))
            raise
        if started is not None and owned.get('transferred_bytes') == size and size > 0:
            try:
                duration = time.perf_counter_ns() - started
                transfer = owned.get('transfer_ns')
                if type(transfer) is not int or not 0 < transfer <= duration:
                    transfer = None  # Older helpers still supply inclusive 11.1 observations.
                self.transfer_metrics.record(size, duration, transfer_ns=transfer)
            except Exception:
                pass  # Fail-soft telemetry; hash/lease/publication gates remain mandatory.
        return owned

    def _copy_impl(self, stack, sha, size, parts, lock):
        name = sha + "." + secrets.token_hex(16) + ".part"
        fd = os.open(name, os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=parts)
        stack.callback(os.close, fd)
        owned = dict(name=name, fd=fd, parts=parts, expired=False, detached=False)
        complete = False
        try:
            _protected(fd, directory=False)
            os.fsync(parts)
            output = stack.enter_context(tempfile.TemporaryFile(mode="w+b", dir=f"/proc/self/fd/{parts}"))
            request = dict(root=str(self.cache.input_mount), source=self.source,
                           sha256=sha, size_bytes=size, output_fd=fd)
            started = time.monotonic()
            with self._guard:
                if self._closed:
                    raise CacheError("Fetcher closed before helper launch")
                process = self._spawn(request, output, lock)
                owned["process"] = process
                self._owned[name] = owned
            try:
                process.wait(timeout=max(0, self.io_timeout - (time.monotonic() - started)))
                if time.monotonic() - started > self.io_timeout:
                    raise subprocess.TimeoutExpired("NFS copy", self.io_timeout)
            except BaseException:
                owned["expired"] = True
                try:
                    process.kill()
                except ProcessLookupError:
                    pass
                # Transfer every FD and the cache slot to retained ownership.
                owned["stack"], owned["detached"] = stack.pop_all(), True
                raise
            if owned["expired"] or self._closed:
                raise CacheError("Copy closed/expired; no late publication")
            if process.returncode != 0:
                raise OSError(errno.EIO, "NFS copy helper failed")
            output.seek(0)
            raw = output.read(4097)
            try:
                result = json.loads(raw) if len(raw) <= 4096 else None
                if not isinstance(result, dict) or result.get("status") not in (
                        "ready", "mount_unavailable", "corrupt", "io_error"):
                    raise ValueError()
            except (ValueError, TypeError, RecursionError) as error:
                raise CacheError("Invalid copy helper result") from error
            status = result["status"]
            if status == "mount_unavailable":
                raise MountNotReady("Expected read-only NFS mount is unavailable")
            if status == "corrupt":
                raise CacheCorrupt("NFS source does not match authorized input")
            if status == "io_error":
                code = result.get("errno")
                if type(code) is not int or not 1 <= code <= 4095:
                    raise CacheError("Invalid copy helper errno")
                raise OSError(code, "NFS copy helper I/O failure")
            if (result.get("sha256") != sha or type(result.get("size_bytes")) is not int
                    or result["size_bytes"] != size):
                raise CacheError("Wrong copy helper identity")
            info = _protected(fd, directory=False)
            if info.st_mode & 0o222:
                raise CacheError("Completed part must be read-only")
            with os.fdopen(os.dup(fd), "rb") as stream:
                stream.seek(0)
                with self.cache.metrics.span("verification_ns"):
                    _verify(stream, sha, size)
                os.fsync(stream.fileno())
            complete = True
            # Old trusted helpers remain usable; absent/malformed telemetry only
            # loses a sample, never relaxes validation or blocks valid staging.
            transferred = result.get('transferred_bytes')
            if type(transferred) is int and transferred == size:
                owned['transferred_bytes'] = transferred
                owned['transfer_ns'] = result.get('transfer_ns')
            return owned
        except subprocess.TimeoutExpired as error:
            raise CacheFetchTimeout("NFS copy deadline elapsed; retain/reconcile pending ownership") from error
        finally:
            if not owned["detached"]:
                with self._guard:
                    self._owned.pop(name, None)
                # Successful parts remain caller-owned until publication.
                if not complete:
                    self._cleanup(owned)

    def _publish(self, owned, objects, quarantine, sha, size):
        # Prevent pin/metadata races during quarantine/publication. No NFS work
        # or external callbacks inside this local metadata transaction.
        with self.cache._session() as (db, _, __):
            if db.execute("SELECT 1 FROM cache_pins WHERE sha256=? LIMIT 1", (sha,)).fetchone():
                raise CacheFetchBusy("Pinned corrupt/missing replica requires attempt reconciliation")
            with _child(objects, "sha256", create=True) as algorithm, \
                    _child(algorithm, sha[:2], create=True) as prefix:
                try:
                    with self.cache._object(objects, sha) as existing:
                        try:
                            with self.cache.metrics.span("verification_ns"):
                                _verify(existing, sha, size)
                        except ArtifactMismatch:
                            # Preserve the corrupt replica, never overwrite it.
                            _rename_noreplace(prefix, sha, quarantine, sha + "." + secrets.token_hex(16))
                            os.fsync(quarantine)
                            os.fsync(prefix)
                        else:
                            return  # Another trusted publisher won; retain its verified bytes.
                except FileNotFoundError:
                    pass
                _rename_noreplace(owned["parts"], owned["name"], prefix, sha)
                os.fsync(prefix)
                os.fsync(owned["parts"])

    def ensure(self, inputs: Iterable[InputFile], *, authorize: Callable[[tuple[InputFile, ...]], bool]) -> tuple[FetchResult, ...]:
        """Ensure the exact preauthorized manifest's unique digests, serially.

        authorize(inputs) must recheck current exact owner/job/attempt authority
        locally and return True, or raise/deny. No digest-only access. At most
        10000 entries. Retry returned transient errnos only; kernel timeouts
        fence this call instead of starting another producer behind a live one.
        """
        if not callable(authorize):
            raise ValueError("An explicit current authority callback is required")
        files, unique, paths = [], {}, set()
        for item in inputs:
            if len(files) >= 10000 or not isinstance(item, InputFile) or item.path in paths:
                raise ValueError("Expected a bounded exact manifest with unique logical paths")
            if item.size_bytes > MAX_INTEGER or (item.sha256 in unique and unique[item.sha256] != item.size_bytes):
                raise ValueError("Conflicting/unbounded digest sizes in manifest")
            files.append(item)
            paths.add(item.path)
            unique[item.sha256] = item.size_bytes
        files = tuple(files)
        self._authorize(authorize, files)
        if not self._operation.acquire(timeout=self.lock_timeout):
            raise CacheFetchBusy("This fetcher is busy; retry later")
        activity = ExitStack()
        try:
            activity.enter_context(self.cache.activity())
            self.reconcile()
            if self.pending():
                raise CacheFetchBusy("An expired helper still owns capacity")
            results = []
            for sha, size in sorted(unique.items()):
                with ExitStack() as stack:
                    root, objects, parts, quarantine, lock = self._slot(stack)
                    self._authorize(authorize, files)
                    try:
                        with self.cache.open_verified(sha, size) as stream:
                            os.fsync(stream.fileno())
                        with _child(objects, "sha256") as algorithm, _child(algorithm, sha[:2]) as prefix:
                            os.fsync(prefix)
                        os.fsync(parts)
                    except CacheMiss:
                        pass
                    else:
                        self._authorize(authorize, files)
                        self.cache.metrics.add(cache_hit_bytes=size)
                        results.append(FetchResult(sha, size, True, 0))
                        continue
                    with self.cache._session() as (db, _, __):
                        if db.execute("SELECT 1 FROM cache_pins WHERE sha256=? LIMIT 1", (sha,)).fetchone():
                            raise CacheFetchBusy("Cannot repair a pinned replica; reconcile its exact attempts")
                    for attempt in range(1, self.attempts + 1):
                        self._authorize(authorize, files)
                        self._admit(root, objects, parts, quarantine, size)
                        try:
                            owned = self._copy(stack, sha, size, parts, lock)
                        except Exception as error:
                            events.copy_error(error, attempt)
                            if (isinstance(error, OSError) and not isinstance(error, CacheFetchTimeout)
                                    and error.errno in TRANSIENT and attempt < self.attempts):
                                events.copy_error(error, attempt, retry=True)
                                time.sleep(self.retry_delay)
                                continue
                            raise
                        try:
                            self._authorize(authorize, files)
                            # close() cannot race this local publication gate.
                            with self._guard:
                                if self._closed:
                                    raise CacheError("Fetcher closed before publication")
                                self._publish(owned, objects, quarantine, sha, size)
                        finally:
                            self._cleanup(owned)
                        with self.cache.open_verified(sha, size):
                            pass
                        self._authorize(authorize, files)
                        self.cache.metrics.add(staged_bytes=size)
                        results.append(FetchResult(sha, size, False, attempt))
                        break
            return tuple(results)
        finally:
            try:activity.close()
            finally:self._operation.release()
