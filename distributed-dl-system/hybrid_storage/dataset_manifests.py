"""Immutable dataset metadata and trusted ACL checks; no DB or runtime activation.

Parse/type validation never authenticates a document. Callers must retrieve the
current ACL and version binding from controller-owned authority, not a request.
Persistence/unique version allocation and job bindings belong to Phase 4.3.
"""

from dataclasses import dataclass
import errno
import hashlib
import json
import re
import unicodedata
import uuid
from typing import Iterable

from .inputs import object_key, project_path

MAX_ENTRIES = 10_000
MAX_PATH_BYTES = 1024
MAX_DEPTH = 64
MAX_MANIFEST_BYTES = 4 * 1024 * 1024
MAX_INTEGER = (1 << 63) - 1
MAX_READERS = 256


def principal(value: str) -> str:
    # Match controller.job_spec.validate_owner without importing DB-owning code.
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.@-]{0,127}", value):
        raise ValueError("Expected an existing student ID/email, at most 128 characters")
    return value


def dataset_id(value: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"ds_[0-9a-f]{32}", value):
        raise ValueError("Expected an opaque controller-issued dataset ID")
    return value


def logical_path(value: str) -> str:
    project_path(value)
    if len(value.encode("utf-8")) > MAX_PATH_BYTES or len(value.split("/")) > MAX_DEPTH:
        raise ValueError("Dataset path exceeds the byte/depth limit")
    return value


@dataclass(frozen=True)
class DatasetEntry:
    path: str
    type: str
    sha256: str | None = None
    size_bytes: int | None = None

    def __post_init__(self):
        logical_path(self.path)
        if self.type == "file":
            object_key(self.sha256)
            if type(self.size_bytes) is not int or not 0 <= self.size_bytes <= MAX_INTEGER:
                raise ValueError("File size must be a nonnegative signed-64-bit integer")
        elif self.type == "directory":
            if self.sha256 is not None or self.size_bytes is not None:
                raise ValueError("Directory entries carry no file digest/size")
        else:
            raise ValueError("Dataset entries must be regular files or directories")

    def document(self) -> dict:
        result = {"path": self.path, "type": self.type}
        if self.type == "file":
            result.update(sha256=self.sha256, size_bytes=self.size_bytes)
        return result


def checked_entries(entries: Iterable[DatasetEntry], *, require_parents: bool = True) -> tuple[DatasetEntry, ...]:
    paths, aliases, sizes = {}, {}, {}
    for entry in entries:
        if not isinstance(entry, DatasetEntry):
            raise ValueError("Expected validated DatasetEntry instances")
        if len(paths) >= MAX_ENTRIES or entry.path in paths:
            raise ValueError("Dataset entry limit or duplicate path")
        paths[entry.path] = entry
        # Check every component prefix, including implicit ZIP parent folders.
        # Preserve original spellings; reject aliases rather than normalize them.
        parts = entry.path.split("/")
        for index in range(1, len(parts) + 1):
            prefix = "/".join(parts[:index])
            folded = unicodedata.normalize("NFC", prefix).casefold()
            if folded in aliases and aliases[folded] != prefix:
                raise ValueError("Dataset paths conflict under Unicode NFC/case folding")
            aliases[folded] = prefix
        if entry.type == "file":
            if entry.sha256 in sizes and sizes[entry.sha256] != entry.size_bytes:
                raise ValueError("One digest cannot have different sizes")
            sizes[entry.sha256] = entry.size_bytes
    for entry in paths.values():
        parts = entry.path.split("/")
        for index in range(1, len(parts)):
            parent = paths.get("/".join(parts[:index]))
            if parent is not None and parent.type != "directory":
                raise ValueError("A file cannot be another entry's parent")
            if require_parents and parent is None:
                raise ValueError("Manifest must explicitly include parent directories")
    return tuple(paths[path] for path in sorted(paths))


@dataclass(frozen=True)
class DatasetManifest:
    dataset_id: str
    version: int
    owner: str
    entries: tuple[DatasetEntry, ...]
    previous_manifest_sha256: str | None = None

    def __post_init__(self):
        dataset_id(self.dataset_id)
        principal(self.owner)
        if type(self.version) is not int or not 1 <= self.version <= MAX_INTEGER:
            raise ValueError("Dataset version must be a positive signed-64-bit integer")
        if self.version == 1:
            if self.previous_manifest_sha256 is not None:
                raise ValueError("First version has no predecessor")
        else:
            object_key(self.previous_manifest_sha256)
        object.__setattr__(self, "entries", checked_entries(self.entries))
        if len(self.canonical_bytes()) > MAX_MANIFEST_BYTES:
            raise ValueError("Dataset manifest exceeds 4 MiB")

    def document(self) -> dict:
        return {"schema_version": 1, "kind": "dataset-manifest", "dataset_id": self.dataset_id,
                "version": self.version, "owner": self.owner,
                "previous_manifest_sha256": self.previous_manifest_sha256,
                "entries": [entry.document() for entry in self.entries]}

    def canonical_bytes(self) -> bytes:
        return (json.dumps(self.document(), sort_keys=True, separators=(",", ":"),
                           ensure_ascii=True, allow_nan=False) + "\n").encode("utf-8")

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes()).hexdigest()


@dataclass(frozen=True)
class DatasetACL:
    """Snapshot of CURRENT trusted authority, not an ACL accepted from a client.

    Immutable snapshots are replaced by the owner on updates. Every access must
    fetch the current record; retaining an old snapshot bypasses revocation.
    Manifest contents do not grant access, even if their hashes are known.
    """

    dataset_id: str
    owner: str
    readers: tuple[str, ...] = ()

    def __post_init__(self):
        dataset_id(self.dataset_id)
        principal(self.owner)
        if isinstance(self.readers, (str, bytes)):
            raise ValueError("Reader ACL must be a collection of explicit principals")
        readers = []
        for reader in self.readers:
            principal(reader)
            if len(readers) >= MAX_READERS or reader == self.owner or reader in readers:
                raise ValueError("Reader ACL must be unique, explicit and exclude its owner")
            readers.append(reader)
        object.__setattr__(self, "readers", tuple(sorted(readers)))

    def _require(self, actor: str, *, write: bool, manifest: DatasetManifest | None = None) -> None:
        principal(actor)
        if manifest is not None and (not isinstance(manifest, DatasetManifest)
                or manifest.dataset_id != self.dataset_id or manifest.owner != self.owner):
            raise PermissionError(errno.EACCES, "Dataset access denied")
        if actor != self.owner and (write or actor not in self.readers):
            raise PermissionError(errno.EACCES, "Dataset access denied")

    def require_read(self, actor: str, manifest: DatasetManifest) -> None:
        if not isinstance(manifest, DatasetManifest):
            raise PermissionError(errno.EACCES, "Dataset access denied")
        self._require(actor, write=False, manifest=manifest)

    def require_write(self, actor: str, manifest: DatasetManifest) -> None:
        if not isinstance(manifest, DatasetManifest):
            raise PermissionError(errno.EACCES, "Dataset access denied")
        self._require(actor, write=True, manifest=manifest)

    def replace_readers(self, actor: str, readers: Iterable[str]) -> "DatasetACL":
        self._require(actor, write=True)
        return DatasetACL(self.dataset_id, self.owner, readers)


def create_dataset(actor: str, entries: Iterable[DatasetEntry], *, readers: Iterable[str] = ()) -> tuple[DatasetManifest, DatasetACL]:
    principal(actor)
    identity = "ds_" + uuid.uuid4().hex
    manifest = DatasetManifest(identity, 1, actor, entries)
    return manifest, DatasetACL(identity, actor, readers)


def next_version(previous: DatasetManifest, acl: DatasetACL, actor: str,
                 entries: Iterable[DatasetEntry]) -> DatasetManifest:
    acl.require_write(actor, previous)
    # The authority must bind previous to its current head transactionally in 4.3.
    return DatasetManifest(previous.dataset_id, previous.version + 1, previous.owner,
                           entries, previous.sha256)


def _fields(value, names):
    if not isinstance(value, dict) or set(value) != set(names):
        raise ValueError("Unexpected/missing dataset manifest fields")


def _unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate dataset manifest JSON key")
        result[key] = value
    return result


def parse_manifest(raw: bytes) -> DatasetManifest:
    """Bounded strict canonical decoding; this does not establish provenance/ACL."""
    if not isinstance(raw, bytes) or len(raw) > MAX_MANIFEST_BYTES:
        raise ValueError("Expected at most 4 MiB of dataset manifest bytes")
    def constant(value):
        raise ValueError("Nonfinite dataset manifest number")
    try:
        document = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique, parse_constant=constant)
        _fields(document, ("schema_version", "kind", "dataset_id", "version", "owner",
                           "previous_manifest_sha256", "entries"))
        if type(document["schema_version"]) is not int or document["schema_version"] != 1 or document["kind"] != "dataset-manifest":
            raise ValueError("Unsupported dataset manifest kind/version")
        if type(document["entries"]) is not list or len(document["entries"]) > MAX_ENTRIES:
            raise ValueError("Expected a bounded dataset entry array")
        entries = []
        for entry in document["entries"]:
            fields = ("path", "type", "sha256", "size_bytes") if isinstance(entry, dict) and entry.get("type") == "file" else ("path", "type")
            _fields(entry, fields)
            entries.append(DatasetEntry(**entry))
        manifest = DatasetManifest(document["dataset_id"], document["version"], document["owner"],
                                   entries, document["previous_manifest_sha256"])
        if raw != manifest.canonical_bytes():
            raise ValueError("Dataset manifest must use canonical JSON bytes/order")
        return manifest
    except (UnicodeError, RecursionError, TypeError, OverflowError) as error:
        raise ValueError("Malformed dataset manifest") from error
