"""Validated source-tree v1 and immutable job-input identity; no DB side effects."""

from dataclasses import asdict, dataclass
import hashlib
import json
import re

from .dataset_manifests import (
    DatasetEntry, MAX_INTEGER, MAX_MANIFEST_BYTES, checked_entries, dataset_id,
    logical_path, principal, _unique,
)
from .inputs import object_key


def encoded(document):
    return (json.dumps(document, sort_keys=True, separators=(",", ":"),
                       ensure_ascii=True, allow_nan=False) + "\n").encode()


def decoded(raw):
    if not isinstance(raw, bytes) or len(raw) > MAX_MANIFEST_BYTES:
        raise ValueError("Expected bounded manifest bytes")
    def constant(value):
        raise ValueError("Nonfinite manifest number")
    try:
        return json.loads(raw.decode("utf-8"), object_pairs_hook=_unique, parse_constant=constant)
    except (UnicodeError, RecursionError) as error:
        raise ValueError("Malformed manifest") from error


def job_identity(value):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", value):
        raise ValueError("Invalid job identity")
    return value


@dataclass(frozen=True)
class SourceFile:
    path: str
    sha256: str
    size_bytes: int
    executable: bool = False

    def __post_init__(self):
        DatasetEntry(self.path, "file", self.sha256, self.size_bytes)
        if type(self.executable) is not bool:
            raise ValueError("Source executable flag must be boolean")


@dataclass(frozen=True)
class SourceManifest:
    files: tuple[SourceFile, ...]
    directories: tuple[str, ...] = ()

    def __post_init__(self):
        # Validate in one bounded pass, retaining the historical draft shape.
        if isinstance(self.files, (str, bytes)) or isinstance(self.directories, (str, bytes)):
            raise ValueError("Expected source file/directory collections")
        entries, files = [], []
        from .dataset_manifests import MAX_ENTRIES
        for directory in self.directories:
            if len(entries) >= MAX_ENTRIES:
                raise ValueError("Source entry limit exceeded")
            entries.append(DatasetEntry(directory, "directory"))
        for file in self.files:
            if len(entries) >= MAX_ENTRIES or not isinstance(file, SourceFile):
                raise ValueError("Expected bounded validated source files")
            files.append(file)
            entries.append(DatasetEntry(file.path, "file", file.sha256, file.size_bytes))
        checked_entries(entries)
        object.__setattr__(self, "directories", tuple(sorted(entry.path for entry in entries if entry.type == "directory")))
        object.__setattr__(self, "files", tuple(sorted(files, key=lambda file: file.path)))
        if len(self.canonical_bytes()) > MAX_MANIFEST_BYTES:
            raise ValueError("Source manifest too large")

    def document(self):
        return {"schema_version": 1, "kind": "source-tree", "directories": list(self.directories),
                "files": [{"path": f.path, "sha256": f.sha256, "size_bytes": f.size_bytes,
                           "executable": f.executable} for f in self.files]}

    def canonical_bytes(self):
        return encoded(self.document())

    @property
    def sha256(self):
        return hashlib.sha256(self.canonical_bytes()).hexdigest()

    def legacy_inventory(self):
        return {"directories": list(self.directories), "files": [
            {"path": f.path, "sha256": f.sha256, "size": f.size_bytes, "executable": f.executable}
            for f in self.files]}


def parse_source(raw):
    document = decoded(raw)
    if (not isinstance(document, dict) or set(document) != {"schema_version", "kind", "files", "directories"}
            or type(document["schema_version"]) is not int or document["schema_version"] != 1
            or document["kind"] != "source-tree" or type(document["files"]) is not list
            or type(document["directories"]) is not list):
        raise ValueError("Invalid source-tree v1")
    try:
        manifest = SourceManifest(tuple(SourceFile(**file) for file in document["files"]), document["directories"])
    except TypeError as error:
        raise ValueError("Invalid source entries") from error
    if manifest.canonical_bytes() != raw:
        raise ValueError("Source manifest is not canonical")
    return manifest


@dataclass(frozen=True)
class JobInputs:
    job_id: str
    owner: str
    source_sha256: str
    requirements_path: str
    requirements_sha256: str
    requirements_size_bytes: int
    legacy_manifest_sha256: str
    execution_spec_sha256: str
    dataset_id: str | None = None
    dataset_version: int | None = None
    dataset_sha256: str | None = None

    def __post_init__(self):
        job_identity(self.job_id)
        principal(self.owner)
        for sha in (self.source_sha256, self.requirements_sha256, self.legacy_manifest_sha256, self.execution_spec_sha256):
            object_key(sha)
        DatasetEntry(self.requirements_path, 'file', self.requirements_sha256, self.requirements_size_bytes)
        if any(value is not None for value in (self.dataset_id, self.dataset_version, self.dataset_sha256)):
            dataset_id(self.dataset_id)
            object_key(self.dataset_sha256)
            if type(self.dataset_version) is not int or not 1 <= self.dataset_version <= MAX_INTEGER:
                raise ValueError("Invalid pinned dataset version")

    def document(self):
        return dict(asdict(self), schema_version=1, kind='registered-job-inputs')

    @property
    def sha256(self):
        return hashlib.sha256(encoded(self.document())).hexdigest()
