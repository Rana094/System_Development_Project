"""Trusted NFS-to-private-FD helper. Cannot publish names or update cache metadata."""

import errno
import hashlib
import json
import os
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from hybrid_storage.artifacts import ArtifactMismatch, _verify
from hybrid_storage.inputs import NFSObjectStorageBackend, object_key
from hybrid_storage.nfs_mount import MountNotReady
from hybrid_storage.worker_cache import CHUNK_BYTES, _expected, _protected, _signature


def copy_request(root: str, source: str, sha256: str, size_bytes: int, output_fd: int) -> dict:
    """Synchronous helper only. Parent owns deadline, FD, name and publication."""
    try:
        _expected(sha256, size_bytes)
        info = _protected(output_fd, directory=False)
        if info.st_size != 0 or info.st_mode & 0o777 != 0o600:
            raise PermissionError("Copy target must be a fresh private local file")
        backend = NFSObjectStorageBackend(Path(root), source=source)
        with os.fdopen(os.dup(output_fd), "w+b") as outgoing:
            with backend.open_artifact(object_key(sha256)) as incoming:
                before = os.fstat(incoming.fileno())
                if before.st_size != size_bytes:
                    raise ArtifactMismatch("NFS source size differs from authorized manifest")
                digest, total = hashlib.sha256(), 0
                while True:
                    count = min(CHUNK_BYTES, size_bytes - total + 1)
                    chunk = incoming.read(count)
                    if not isinstance(chunk, bytes) or len(chunk) > count:
                        raise ArtifactMismatch("Source did not honor bounded binary reads")
                    if not chunk:
                        break
                    total += len(chunk)
                    if total > size_bytes:
                        raise ArtifactMismatch("Source exceeds authorized size")
                    digest.update(chunk)
                    outgoing.write(chunk)
                if (total != size_bytes or digest.hexdigest() != sha256
                        or _signature(before) != _signature(os.fstat(incoming.fileno()))):
                    raise ArtifactMismatch("Source digest/size/identity changed")
            # NFS context exit also rechecks mount identity before readiness.
            outgoing.flush()
            outgoing.seek(0)
            _verify(outgoing, sha256, size_bytes)
            os.fchmod(outgoing.fileno(), 0o400)
            os.fsync(outgoing.fileno())
        return {"status": "ready", "sha256": sha256, "size_bytes": size_bytes}
    except MountNotReady:
        return {"status": "mount_unavailable"}
    except ArtifactMismatch:
        return {"status": "corrupt"}
    except OSError as error:
        return {"status": "io_error", "errno": error.errno or errno.EIO}
    except (TypeError, ValueError):
        return {"status": "invalid_input"}


def main():
    if len(sys.argv) != 2 or len(sys.argv[1].encode()) > 8192:
        return 2
    try:
        result = copy_request(**json.loads(sys.argv[1]))
    except (ValueError, TypeError):
        return 2
    print(json.dumps(result), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
