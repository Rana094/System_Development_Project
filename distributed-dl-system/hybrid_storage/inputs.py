"""Strict input identities over the existing adapters; no runtime selection.

Construct these views only in trusted services after owner/job/manifest checks.
An InputFile or SHA-256 is data, not an authorization credential. Mount identity,
availability and stage-time byte verification are separate admission requirements.
"""

from contextlib import contextmanager
from dataclasses import dataclass
import errno
from pathlib import Path, PurePosixPath
import re
from types import MappingProxyType
from typing import BinaryIO, Iterable, Iterator

from .backends import LegacyStorageBackend, NFSStorageBackend
from .mount_identity import fd_mount_id, require_local_fd
from .nfs_mount import MountNotReady, require_input_mount, validate_source


def object_key(sha256: str) -> str:
    """Derive the only supported object path from a lowercase SHA-256 digest."""
    if not isinstance(sha256, str) or not re.fullmatch(r"[0-9a-f]{64}", sha256):
        raise ValueError("SHA-256 must be 64 lowercase hexadecimal characters")
    return f"sha256/{sha256[:2]}/{sha256}"


def validate_object_key(key: str) -> str:
    """Reject arbitrary paths, alternate prefixes and digest/prefix mismatches."""
    if not isinstance(key, str) or not re.fullmatch(r"sha256/[0-9a-f]{2}/[0-9a-f]{64}", key):
        raise ValueError("Expected a canonical SHA-256 object key")
    if object_key(key.rsplit("/", 1)[1]) != key:
        raise ValueError("Object prefix must match its SHA-256 digest")
    return key


def project_path(path: str) -> str:
    """Validate an exact, relative manifest path without normalizing its spelling."""
    if not isinstance(path, str) or not path:
        raise ValueError("Expected a canonical relative project path")
    parts = path.split("/")
    if (
        any(part in ("", ".", "..") for part in parts)
        or PurePosixPath(path).is_absolute()
        or any(char in path for char in ("\\", ":"))
        or any(ord(char) < 32 or ord(char) == 127 or 0xD800 <= ord(char) <= 0xDFFF for char in path)
    ):
        raise ValueError("Expected a canonical relative project path")
    return path


@dataclass(frozen=True)
class InputFile:
    """Expected file identity from an already authorized manifest/inventory."""

    path: str
    sha256: str
    size_bytes: int

    def __post_init__(self):
        project_path(self.path)
        object_key(self.sha256)
        if type(self.size_bytes) is not int or self.size_bytes < 0:
            raise ValueError("Input size must be a nonnegative integer")


class _ObjectBackend:
    """Keep the general-key Phase 1 adapters backwards compatible underneath."""

    _backend: LegacyStorageBackend | NFSStorageBackend

    def artifact_exists(self, key: str) -> bool:
        return self._backend.artifact_exists(validate_object_key(key))

    def resolve_path(self, key: str) -> Path:
        """Advisory diagnostic path only; use open_artifact for actual access."""
        return self._backend.resolve_path(validate_object_key(key))

    @contextmanager
    def open_artifact(self, key: str) -> Iterator[BinaryIO]:
        with self._backend.open_artifact(validate_object_key(key)) as stream:
            yield stream

    def publish_artifact(
        self, key: str, source: BinaryIO, *, expected_sha256: str, expected_size: int
    ) -> Path:
        validate_object_key(key)
        if object_key(expected_sha256) != key:
            raise ValueError("Publication digest must match its object key")
        return self._backend.publish_artifact(
            key, source, expected_sha256=expected_sha256, expected_size=expected_size
        )


class LocalObjectStorageBackend(_ObjectBackend):
    """Trusted controller-local input store; never a worker publication channel.

    Roots/sha256-prefix directories must be provisioned separately. Without an
    explicit private staging root this is a read-only local view. A supplied
    local root is operator-owned configuration, never a submitted job path.
    """

    def __init__(self, root: Path, *, staging_root: Path | None = None):
        self._backend = _LocalPublicationBackend(root, staging_root=staging_root)


class _LocalPublicationBackend(LegacyStorageBackend):
    def _check_publication_directories(self, parent: int, staging: int) -> None:
        require_local_fd(parent)
        require_local_fd(staging)


class NFSObjectStorageBackend(_ObjectBackend):
    """Worker's read-only exported view; no mounting, publication or fallback.

    Construction is side-effect free. Reads require an explicit expected source,
    exact RO NFS admission and matching opened-descriptor mount identity. Direct
    reads remain synchronous: use the health supervisor for bounded probes, and
    never run NFS I/O on Flask/scheduler/lease threads.
    """

    def __init__(self, mounted_root: Path, *, source: str | None = None):
        self._backend = NFSStorageBackend(mounted_root)
        self.source = validate_source(source) if source is not None else None

    def _admit(self) -> dict:
        if self.source is None:
            raise MountNotReady("An explicit expected NFS source is required")
        return require_input_mount(self.source, target=str(self._backend.root))

    def _same_mount(self, admitted: dict) -> None:
        if self._admit()["mount_id"] != admitted["mount_id"]:
            raise MountNotReady("Input mount changed during access")

    @contextmanager
    def _open_admitted(self, key: str, admitted: dict) -> Iterator[BinaryIO]:
        try:
            with self._backend.open_artifact(key) as stream:
                if fd_mount_id(stream.fileno()) != admitted["mount_id"]:
                    raise MountNotReady("Opened file belongs to a different mount")
                self._same_mount(admitted)
                yield stream
                self._same_mount(admitted)
        except FileNotFoundError:
            # Missing mount metadata or an unmount race is not an object miss.
            self._same_mount(admitted)
            raise

    @contextmanager
    def open_artifact(self, key: str) -> Iterator[BinaryIO]:
        key = validate_object_key(key)
        with self._open_admitted(key, self._admit()) as stream:
            yield stream

    def artifact_exists(self, key: str) -> bool:
        key = validate_object_key(key)
        admitted = self._admit()
        try:
            with self._open_admitted(key, admitted):
                return True
        except FileNotFoundError:
            return False

    def resolve_path(self, key: str) -> Path:
        with self.open_artifact(key):
            return self._backend.root.joinpath(*validate_object_key(key).split("/"))


class ProjectInputView:
    """Read-only allowlist over a controlled legacy project or object store.

    The trusted caller binds the file list to its authenticated owner/job first.
    Constructor arguments are not a student-facing schema or an authentication
    boundary. No bytes are copied, registered or silently migrated here.
    """

    def __init__(
        self,
        backend: LegacyStorageBackend | LocalObjectStorageBackend | NFSObjectStorageBackend,
        files: Iterable[InputFile],
        *,
        objects: bool = False,
    ):
        if type(objects) is not bool:
            raise ValueError("Expected an explicit object-layout boolean")
        allowed = {}
        sizes = {}
        for entry in files:
            if not isinstance(entry, InputFile):
                raise ValueError("Expected validated InputFile entries")
            if entry.path in allowed:
                raise ValueError("Duplicate project file path")
            if entry.sha256 in sizes and sizes[entry.sha256] != entry.size_bytes:
                raise ValueError("One SHA-256 cannot have different declared sizes")
            sizes[entry.sha256] = entry.size_bytes
            allowed[entry.path] = entry
        for path in allowed:
            parts = path.split("/")
            if any("/".join(parts[:index]) in allowed for index in range(1, len(parts))):
                raise ValueError("A project file cannot be another file's parent")
        if objects and not isinstance(backend, (LocalObjectStorageBackend, NFSObjectStorageBackend)):
            raise ValueError("Object view requires a strict SHA-256 backend")
        if not objects and not isinstance(backend, LegacyStorageBackend):
            raise ValueError("Project view requires a controlled local legacy root")
        self._backend = backend
        self._files = MappingProxyType(allowed)
        self._objects = objects

    def reference(self, path: str) -> InputFile:
        path = project_path(path)
        try:
            return self._files[path]
        except KeyError:
            raise PermissionError(errno.EACCES, "Input path is not authorized") from None

    def _key(self, path: str) -> str:
        entry = self.reference(path)
        return object_key(entry.sha256) if self._objects else entry.path

    def artifact_exists(self, key: str) -> bool:
        return self._backend.artifact_exists(self._key(key))

    def resolve_path(self, key: str) -> Path:
        return self._backend.resolve_path(self._key(key))

    @contextmanager
    def open_artifact(self, key: str) -> Iterator[BinaryIO]:
        with self._backend.open_artifact(self._key(key)) as stream:
            yield stream

    def publish_artifact(
        self, key: str, source: BinaryIO, *, expected_sha256: str, expected_size: int
    ) -> Path:
        raise PermissionError(errno.EROFS, "Authorized project input views cannot publish")
