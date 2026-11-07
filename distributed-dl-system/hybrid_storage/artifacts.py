"""Trusted controller-local immutable registration; no DB, ACL, route or activation.

The caller authorizes an opened binary source and an exact size. Digests identify
bytes, never grant access. Roots are operator-provisioned and protected from
untrusted writers. Linux renameat2(RENAME_NOREPLACE) arbitrates concurrent writers;
unsupported kernels/filesystems fail closed instead of replacing an object.
"""

from contextlib import contextmanager
import ctypes
from dataclasses import dataclass
import errno
import hashlib
import os
from pathlib import Path
import secrets
import stat
from typing import BinaryIO, Iterator

from .backends import _directory, _root
from .inputs import object_key
from .mount_identity import require_local_fd

CHUNK_BYTES = 1024 * 1024
RENAME_NOREPLACE = 1  # Linux UAPI linux/fs.h and glibc stdio.h.


class ArtifactMismatch(ValueError):
    """Expected, staged or pre-existing content does not match its identity."""


@dataclass(frozen=True)
class Artifact:
    sha256: str
    size_bytes: int
    key: str
    created: bool


def _rename_noreplace(source_fd: int, source: str, target_fd: int, target: str) -> None:
    """No plain rename/replace fallback: those can overwrite immutable objects."""
    library = ctypes.CDLL(None, use_errno=True)
    try:
        rename = library.renameat2
    except AttributeError as error:
        raise OSError(errno.ENOTSUP, "Linux renameat2 is required for registration") from error
    rename.argtypes = (ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint)
    rename.restype = ctypes.c_int
    if rename(source_fd, os.fsencode(source), target_fd, os.fsencode(target), RENAME_NOREPLACE):
        code = ctypes.get_errno()
        raise OSError(code, os.strerror(code), target)


def _protected_directory(fd: int) -> os.stat_result:
    require_local_fd(fd)
    info = os.fstat(fd)
    if info.st_uid != os.geteuid() or info.st_mode & 0o022:
        raise PermissionError("Artifact directories must be service-owned and not group/world writable")
    return info


@contextmanager
def _child_directory(parent: int, name: str) -> Iterator[int]:
    try:
        os.mkdir(name, 0o755, dir_fd=parent)
    except FileExistsError:
        pass
    fd = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
    try:
        _protected_directory(fd)
        # Also sync on retry: a prior mkdir may have succeeded before fsync failed.
        os.fsync(parent)
        yield fd
    finally:
        os.close(fd)


def _identity(info: os.stat_result) -> tuple:
    return tuple(getattr(info, field) for field in
                 ("st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns"))


def _verify(stream: BinaryIO, sha: str, size: int, *, identical: BinaryIO | None = None) -> None:
    before = os.fstat(stream.fileno())
    if not stat.S_ISREG(before.st_mode) or before.st_size != size:
        raise ArtifactMismatch("Object size/type does not match")
    digest = hashlib.sha256()
    total = 0
    if identical is not None:
        identical.seek(0)
    while True:
        chunk = stream.read(min(CHUNK_BYTES, size - total + 1))
        if not chunk:
            break
        total += len(chunk)
        if total > size or (identical is not None and chunk != identical.read(len(chunk))):
            raise ArtifactMismatch("Object content does not match; never overwrite a digest")
        digest.update(chunk)
    if (total != size or digest.hexdigest() != sha
            or _identity(before) != _identity(os.fstat(stream.fileno()))
            or (identical is not None and identical.read(1))):
        raise ArtifactMismatch("Object digest/size/identity does not match")


def _existing(parent: int, name: str, staged: BinaryIO, sha: str, size: int) -> None:
    fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode):
            raise ArtifactMismatch("Existing digest must be a regular immutable object")
        if info.st_uid != os.geteuid() or info.st_mode & 0o222:
            raise PermissionError("Existing object must be service-owned and read-only")
        stream = os.fdopen(fd, "rb")
    except BaseException:
        os.close(fd)
        raise
    with stream:
        # Byte comparison rejects a hypothetical same-digest/different-content
        # candidate as well as ordinary corruption. A digest alone is not enough.
        _verify(stream, sha, size, identical=staged)
        os.fsync(stream.fileno())


class ArtifactRegistrar:
    """Register bounded streams into <root>/sha256/<prefix>/<digest>.

    Construct explicitly in a trusted service, never from student-supplied paths.
    Root and private scratch must already exist, not overlap, share a supported
    local filesystem, and be service-owned. No runtime call site uses this yet.
    The registrar creates only derived sha256/prefix directories and its own
    unique scratch files. It never deletes/replaces an object or adopts job files
    by hard link. A full verified retry reconciles post-rename failures safely.
    """

    def __init__(self, root: Path, *, staging_root: Path):
        self.root = _root(root)
        self.staging_root = _root(staging_root)
        if self.root.is_relative_to(self.staging_root) or self.staging_root.is_relative_to(self.root):
            raise ValueError("Private scratch and exported objects must not overlap")

    def register(self, source: BinaryIO, *, expected_size: int,
                 expected_sha256: str | None = None) -> Artifact:
        """Consume bytes once; optional expected digest must match before publication.

        Exact expected_size is required as a trusted admission bound, not a disk
        quota. Every source read is at most 1 MiB (plus one-byte overflow checking
        within that bound); no seek on source is needed and it stays caller-owned.
        """
        if type(expected_size) is not int or expected_size < 0:
            raise ValueError("Expected size must be a nonnegative integer")
        if expected_sha256 is not None:
            object_key(expected_sha256)
        with _directory(self.root) as root, _directory(self.staging_root) as staging:
            root_info = _protected_directory(root)
            staging_info = _protected_directory(staging)
            if staging_info.st_mode & 0o077:
                raise PermissionError("Publication scratch must be owner-only (0700)")
            if root_info.st_dev != staging_info.st_dev:
                raise OSError(errno.EXDEV, "Publication scratch must share the object filesystem")
            for _ in range(8):
                temporary = ".registration-" + secrets.token_hex(16)
                try:
                    fd = os.open(temporary, os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                                 0o600, dir_fd=staging)
                    break
                except FileExistsError:
                    continue
            else:
                raise FileExistsError(errno.EEXIST, "Cannot allocate a unique private scratch file")
            moved = False
            try:
                try:
                    outgoing = os.fdopen(fd, "w+b")
                except BaseException:
                    os.close(fd)
                    raise
                with outgoing:
                    digest = hashlib.sha256()
                    total = 0
                    while True:
                        count = min(CHUNK_BYTES, expected_size - total + 1)
                        chunk = source.read(count)
                        if not isinstance(chunk, bytes) or len(chunk) > count:
                            raise ValueError("Source must honor bounded binary reads")
                        if not chunk:
                            break
                        total += len(chunk)
                        if total > expected_size:
                            raise ArtifactMismatch("Source exceeds expected size")
                        digest.update(chunk)
                        outgoing.write(chunk)
                    sha = digest.hexdigest()
                    if total != expected_size or (expected_sha256 is not None and sha != expected_sha256):
                        raise ArtifactMismatch("Source size or digest does not match")
                    outgoing.flush()
                    outgoing.seek(0)
                    _verify(outgoing, sha, total)  # Verify copied bytes, not just source reads.
                    os.fchmod(outgoing.fileno(), 0o444)
                    os.fsync(outgoing.fileno())
                    with _child_directory(root, "sha256") as algorithm, \
                            _child_directory(algorithm, sha[:2]) as parent:
                        if os.fstat(parent).st_dev != staging_info.st_dev:
                            raise OSError(errno.EXDEV, "Object prefix must share the scratch filesystem")
                        try:
                            _rename_noreplace(staging, temporary, parent, sha)
                            moved = True
                        except FileExistsError:
                            _existing(parent, sha, outgoing, sha, total)
                        # Failure here can leave a complete object visible. Never
                        # remove it: retry re-verifies bytes and repeats fsync.
                        os.fsync(parent)
                        os.fsync(staging)
                    return Artifact(sha, total, object_key(sha), created=moved)
            finally:
                if not moved:
                    # Only this invocation's O_EXCL-created scratch name is ours.
                    os.unlink(temporary, dir_fd=staging)
                    os.fsync(staging)
