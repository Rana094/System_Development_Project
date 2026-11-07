"""Read-only input verification and bounded caller waits with retained ownership.

Keep one long-lived supervisor per trusted service. A timeout is not proof of
kernel I/O cancellation: pending owned processes consume capacity until reaped.
Probes never publish, stage, update a DB or grant launch authorization.
"""

from dataclasses import asdict, dataclass
import errno
import hashlib
import json
import math
import os
from pathlib import Path
import stat
import subprocess
import sys
import tempfile
import threading
import time
import uuid

from .backends import _directory, _root
from .inputs import LocalObjectStorageBackend, NFSObjectStorageBackend, object_key
from .mount_identity import require_local_fd
from .nfs_mount import MountNotReady, validate_source


class CorruptInput(ValueError):
    """Opened bytes do not match the authorized immutable identity."""


@dataclass(frozen=True)
class ProbeRequest:
    role: str
    root: str
    sha256: str
    size_bytes: int
    source: str | None = None

    def __post_init__(self):
        if self.role not in ("local", "nfs"):
            raise ValueError("Probe role must be local or nfs")
        if not isinstance(self.root, str) or str(_root(Path(self.root))) != self.root:
            raise ValueError("Probe root must be a canonical absolute directory")
        object_key(self.sha256)
        if type(self.size_bytes) is not int or self.size_bytes < 0:
            raise ValueError("Probe size must be a nonnegative integer")
        if self.role == "nfs":
            validate_source(self.source)
        elif self.source is not None:
            raise ValueError("Local probe must not specify a network source")
        if len(json.dumps(asdict(self)).encode()) > 8192:
            raise ValueError("Probe request is too large")


def verify_request(request: ProbeRequest) -> dict:
    """Synchronous worker operation; only invoke through the supervisor for NFS.

    A successful read of the exact authorized object's entire bytes is the
    availability check. No write probe or substitute underlying directory is used.
    It can be served by the kernel cache; it is not proof of wire reachability.
    """
    try:
        backend = (NFSObjectStorageBackend(Path(request.root), source=request.source)
                   if request.role == "nfs" else LocalObjectStorageBackend(Path(request.root)))
        key = object_key(request.sha256)
        with backend.open_artifact(key) as stream:
            if request.role == "local":
                require_local_fd(stream.fileno())
            before = os.fstat(stream.fileno())
            if not stat.S_ISREG(before.st_mode) or before.st_size != request.size_bytes:
                raise CorruptInput("Unexpected input size/type")
            digest = hashlib.sha256()
            total = 0
            while True:
                chunk = stream.read(min(1024 * 1024, request.size_bytes - total + 1))
                if not chunk:
                    break
                total += len(chunk)
                if total > request.size_bytes:
                    raise CorruptInput("Input grew beyond its declared size")
                digest.update(chunk)
            after = os.fstat(stream.fileno())
            fields = ("st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns")
            if (total != request.size_bytes or digest.hexdigest() != request.sha256
                    or any(getattr(before, name) != getattr(after, name) for name in fields)):
                raise CorruptInput("Input bytes or identity changed")
        return {"ready": True, "status": "ready", "bytes": total, "sha256": digest.hexdigest()}
    except MountNotReady:
        return {"ready": False, "status": "mount_unavailable"}
    except CorruptInput:
        return {"ready": False, "status": "corrupt"}
    except OSError as error:
        status = ("source_missing" if error.errno == errno.ENOENT else
                  "permission_denied" if error.errno in (errno.EACCES, errno.EPERM) else
                  "io_error")
        return {"ready": False, "status": status, "errno": error.errno}
    except ValueError:
        # Symlinks/special files/malformed metadata are never healthy inputs.
        return {"ready": False, "status": "invalid_input"}


class HealthSupervisor:
    """One owned read-only child at a time; never wait indefinitely after timeout.

    Call reconcile() until pending() is empty, including after close(). Keep the
    supervisor alive while it owns a child. This is not a durable worker service
    or a restart journal: integrating a daemon/restart reconciler is a later gate.
    """

    def __init__(self, *, spool_dir: Path | None = None):
        self.spool_dir = _root(Path(spool_dir) if spool_dir is not None else Path(tempfile.gettempdir()))
        self._owned = {}
        self._lock = threading.RLock()
        self._closed = False

    def _spawn(self, request: ProbeRequest, output):
        return subprocess.Popen(
            [sys.executable, "-I", "-B", str(Path(__file__).with_name("health_worker.py")), json.dumps(asdict(request))],
            stdin=subprocess.DEVNULL, stdout=output, stderr=subprocess.DEVNULL,
            close_fds=True, start_new_session=True,
        )

    def _result(self, token, owned):
        process, output = owned["process"], owned["output"]
        if owned["expired"]:
            result = {"ready": False, "status": "timeout"}
        elif process.returncode != 0:
            result = {"ready": False, "status": "probe_failed"}
        else:
            output.seek(0)
            raw = output.read(4097)
            try:
                result = json.loads(raw) if len(raw) <= 4096 else None
                allowed = {"ready", "mount_unavailable", "corrupt", "source_missing", "permission_denied", "io_error", "invalid_input"}
                if not isinstance(result, dict) or result.get("status") not in allowed or type(result.get("ready")) is not bool:
                    raise ValueError("Invalid probe result")
                if result["ready"] != (result["status"] == "ready"):
                    raise ValueError("Inconsistent probe result")
                if result["ready"] and (result.get("sha256") != owned["request"].sha256
                                        or type(result.get("bytes")) is not int
                                        or result["bytes"] != owned["request"].size_bytes):
                    raise ValueError("Wrong probe identity")
            except (ValueError, TypeError, RecursionError):
                result = {"ready": False, "status": "probe_failed"}
        output.close()
        return dict(result, probe_id=token, pid=process.pid, pending=False)

    def reconcile(self) -> list[dict]:
        """Reap only our Popen handles; a late success after timeout stays failed."""
        results = []
        with self._lock:
            for token, owned in list(self._owned.items()):
                if owned["process"].poll() is not None:
                    results.append(self._result(token, owned))
                    del self._owned[token]
        return results

    def pending(self) -> list[dict]:
        with self._lock:
            return [{"probe_id": token, "pid": row["process"].pid, "timed_out": row["expired"]}
                    for token, row in self._owned.items()]

    def check(self, request: ProbeRequest, *, timeout: float = 5.0) -> dict:
        if not isinstance(request, ProbeRequest):
            raise ValueError("Expected validated ProbeRequest")
        if type(timeout) not in (int, float) or not math.isfinite(timeout) or not 0 < timeout <= 300:
            raise ValueError("Probe timeout must be finite, positive and at most 300 seconds")
        started = time.monotonic()
        with self._lock:
            self.reconcile()
            if self._closed or self._owned:
                return {"ready": False, "status": "closed" if self._closed else "busy", "pending": bool(self._owned)}
            # Local-only output spool; no NFS write probe or remote temporary.
            with _directory(self.spool_dir) as directory:
                require_local_fd(directory)
                # Create through the already-attested directory FD, so a
                # concurrent pathname/mount replacement cannot redirect writes.
                output = tempfile.TemporaryFile(mode="w+b", dir=f"/proc/self/fd/{directory}")
            try:
                require_local_fd(output.fileno())
                process = self._spawn(request, output)
            except BaseException:
                output.close()
                raise
            token = uuid.uuid4().hex
            owned = {"process": process, "output": output, "expired": False, "request": request}
            self._owned[token] = owned
        try:
            process.wait(timeout=max(0, timeout - (time.monotonic() - started)))
        except BaseException as error:
            with self._lock:
                owned["expired"] = True
                # Signal only the owned child; do not wait for hard-I/O exit.
                try:
                    process.terminate()
                except ProcessLookupError:
                    pass
                result = {"ready": False, "status": "timeout", "probe_id": token,
                          "pid": process.pid, "pending": process.poll() is None}
                self.reconcile()
            if not isinstance(error, subprocess.TimeoutExpired):
                raise
            return result
        with self._lock:
            # Another observer may have reaped it while wait() returned.
            if token not in self._owned:
                return {"ready": False, "status": "probe_reconciled", "probe_id": token, "pid": process.pid, "pending": False}
            if time.monotonic() - started > timeout:
                owned["expired"] = True
            result = self._result(token, owned)
            del self._owned[token]
            return result

    def close(self) -> list[dict]:
        """Stop admission, request child termination, retain any still-live handles."""
        with self._lock:
            self._closed = True
            for owned in self._owned.values():
                owned["expired"] = True
                try:
                    owned["process"].terminate()
                except ProcessLookupError:
                    pass
            self.reconcile()
            return self.pending()
