"""Filesystem adapters for trusted services, without mounting or database side effects.

Keys are authorized, canonical relative file paths, not arbitrary student paths or
an authorization mechanism. The caller must bind them to an owned job/manifest.
All roots and parent directories must be protected from untrusted writers.
"""

from contextlib import contextmanager
import errno
import hashlib
import os
from pathlib import Path, PurePosixPath
import re
import secrets
import stat
from typing import BinaryIO, ContextManager, Iterator, Protocol, runtime_checkable


@runtime_checkable
class StorageBackend(Protocol):
    """Input-file access only; no job states, transport, cache or DB policy.

    Existence/path queries are advisory. Use open_artifact for a no-follow read,
    and verify bytes against the authorized manifest at staging time. I/O errors
    (including NFS outages) propagate; they must not masquerade as cache misses.
    """

    def artifact_exists(self, key: str) -> bool:
        """Return False only for a missing file/parent; reject unsafe file types."""
        ...

    def resolve_path(self, key: str) -> Path:
        """Return the checked existing path for diagnostics, not a safe-open token."""
        ...

    def open_artifact(self, key: str) -> ContextManager[BinaryIO]:
        """Open a regular input file read-only without following any symlink."""
        ...

    def publish_artifact(
        self, key: str, source: BinaryIO, *, expected_sha256: str, expected_size: int
    ) -> Path:
        """Publish a verified new file without replacement, or reject writes.

        Only a trusted controller-side publisher may use this operation. It does
        not commit a manifest/checkpoint or authenticate a caller.
        """
        ...


def _root(path: Path) -> Path:
    result = Path(path)
    if not result.is_absolute() or ".." in result.parts or result == Path("/"):
        raise ValueError("Storage roots must be explicit absolute directories, not /")
    return result


def _parts(key: str) -> tuple[str, ...]:
    if not isinstance(key, str):
        raise ValueError("Artifact key must be a canonical relative POSIX path")
    path = PurePosixPath(key)
    if (
        not key
        or path.is_absolute()
        or not path.parts
        or ".." in path.parts
        or path.as_posix() != key
        or any(char in key for char in ("\\", ":", "\0"))
    ):
        raise ValueError("Artifact key must be a canonical relative POSIX path")
    return path.parts


@contextmanager
def _directory(path: Path) -> Iterator[int]:
    """Anchor every absolute path component with no-follow directory descriptors."""
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    fd = os.open(path.anchor, flags)
    try:
        for part in path.parts[1:]:
            child = os.open(part, flags, dir_fd=fd)
            os.close(fd)
            fd = child
        yield fd
    finally:
        os.close(fd)


class _FilesystemReader:
    def __init__(self, root: Path):
        self.root = _root(root)

    @contextmanager
    def _parent(self, key: str) -> Iterator[tuple[int, str]]:
        parts = _parts(key)
        with _directory(self.root.joinpath(*parts[:-1])) as fd:
            yield fd, parts[-1]

    @contextmanager
    def open_artifact(self, key: str) -> Iterator[BinaryIO]:
        with self._parent(key) as (parent, name):
            # NONBLOCK prevents a malicious FIFO from hanging before fstat rejects it.
            fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
        try:
            if not stat.S_ISREG(os.fstat(fd).st_mode):
                raise ValueError("Artifact must be a regular file")
            stream = os.fdopen(fd, "rb")
        except BaseException:
            os.close(fd)
            raise
        with stream:
            yield stream

    def artifact_exists(self, key: str) -> bool:
        try:
            with self.open_artifact(key):
                return True
        except FileNotFoundError:
            return False

    def resolve_path(self, key: str) -> Path:
        with self.open_artifact(key):
            return self.root.joinpath(*_parts(key))


class LegacyStorageBackend(_FilesystemReader):
    """Local view of existing files; does not replace job_storage.stage_job.

    Publication is disabled unless an explicit private staging directory is
    supplied. Provision roots/target parents separately: this adapter never
    creates them. Staging must be outside the input root on the same filesystem.
    """

    def __init__(self, root: Path, *, staging_root: Path | None = None):
        super().__init__(root)
        self.staging_root = _root(staging_root) if staging_root is not None else None
        if self.staging_root is not None and (
            self.root.is_relative_to(self.staging_root)
            or self.staging_root.is_relative_to(self.root)
        ):
            raise ValueError("Publication staging and input roots must not overlap")

    def publish_artifact(
        self, key: str, source: BinaryIO, *, expected_sha256: str, expected_size: int
    ) -> Path:
        if self.staging_root is None:
            raise PermissionError("Controller publication requires a private staging root")
        _parts(key)
        if not isinstance(expected_sha256, str) or not re.fullmatch(r"[0-9a-f]{64}", expected_sha256):
            raise ValueError("Expected SHA-256 must be 64 lowercase hexadecimal characters")
        if type(expected_size) is not int or expected_size < 0:
            raise ValueError("Expected size must be a nonnegative integer")
        with self._parent(key) as (parent, name), _directory(self.staging_root) as staging:
            self._check_publication_directories(parent, staging)
            staging_info = os.fstat(staging)
            if staging_info.st_uid != os.geteuid() or staging_info.st_mode & 0o077:
                raise PermissionError("Publication staging must be owner-only (0700)")
            if staging_info.st_dev != os.fstat(parent).st_dev:
                raise OSError(errno.EXDEV, "Publication staging must be on the input filesystem")
            temporary = ".artifact-" + secrets.token_hex(16)
            fd = os.open(
                temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                0o600, dir_fd=staging,
            )
            try:
                with os.fdopen(fd, "wb") as outgoing:
                    actual_hash = hashlib.sha256()
                    size = 0
                    while True:
                        chunk = source.read(min(1024 * 1024, expected_size - size + 1))
                        if not chunk:
                            break
                        size += len(chunk)
                        if size > expected_size:
                            raise ValueError("Artifact exceeds expected size")
                        actual_hash.update(chunk)
                        outgoing.write(chunk)
                    if size != expected_size or actual_hash.hexdigest() != expected_sha256:
                        raise ValueError("Artifact size or SHA-256 does not match")
                    outgoing.flush()
                    os.fchmod(outgoing.fileno(), 0o444)
                    os.fsync(outgoing.fileno())
                # A same-filesystem link atomically creates the final name without
                # replacing an existing file (unlike rename/replace). The temporary
                # inode is our private copy, never a mutable student/cache file.
                os.link(temporary, name, src_dir_fd=staging, dst_dir_fd=parent, follow_symlinks=False)
                os.fsync(parent)
            finally:
                os.unlink(temporary, dir_fd=staging)
            # A directory-fsync error after link can leave a complete file visible;
            # callers must reconcile before retry, never overwrite it.
        return self.root.joinpath(*_parts(key))

    def _check_publication_directories(self, parent: int, staging: int) -> None:
        """Optional strict-wrapper admission on the actual anchored descriptors.

        The legacy contract is unchanged. The strict controller object adapter
        overrides this before temporary creation or publication can write bytes.
        """


class NFSStorageBackend(_FilesystemReader):
    """Read-only view of a pre-mounted input export; no mount or network setup.

    The controller publishes through a separate LegacyStorageBackend on its local
    export directory. The worker adapter never writes, even if a mount is RW.
    It does not attest mount flags, host isolation, authentication or NFS health.
    """

    def publish_artifact(
        self, key: str, source: BinaryIO, *, expected_sha256: str, expected_size: int
    ) -> Path:
        raise PermissionError(errno.EROFS, "NFS input access is read-only; use the trusted controller publisher")
