"""Publish self-contained, fingerprinted student inputs on the controller filesystem."""

from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import tempfile
import time
import zipfile

from job_store import get_job, mark_staged, record_staging_error

DEFAULT_ROOT = Path(__file__).resolve().parents[1] / "storage/jobs"
MUTABLE_DIRECTORIES = ("checkpoints", "logs", "results")


class StorageError(ValueError):
    """A snapshot cannot be safely copied, published or verified."""


def storage_root():
    """Allow the lab controller to place storage on a larger disk or shared mount."""
    return (
        Path(os.environ.get("DL_STORAGE_ROOT", str(DEFAULT_ROOT)))
        .expanduser()
        .resolve()
    )


def encoded(value):
    """Canonical JSON makes content versions independent of metadata field ordering."""
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        + "\n"
    ).encode()


def digest(path):
    """Hash files incrementally so datasets need not fit in controller memory."""
    result = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(chunk)
    return result.hexdigest()


def sync_directory(path):
    """Persist completed entries before publishing a directory or database pointer."""
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def write_metadata(path, value):
    """Metadata is written inside an unpublished workspace and flushed before rename."""
    with path.open("xb") as stream:
        stream.write(encoded(value))
        stream.flush()
        os.fsync(stream.fileno())


def signature(info):
    """Detect replacement or modification of an input while its snapshot is copied."""
    return (
        info.st_dev,
        info.st_ino,
        info.st_mode,
        info.st_size,
        info.st_mtime_ns,
        info.st_ctime_ns,
    )


def tree_entries(root):
    """List only regular files/directories; snapshots cannot depend on external links."""
    files, directories = [], []

    def walk(path):
        for child in sorted(path.iterdir()):
            info = child.lstat()
            relative = child.relative_to(root).as_posix()
            if stat.S_ISDIR(info.st_mode):
                directories.append(relative)
                walk(child)
            elif stat.S_ISREG(info.st_mode):
                files.append((relative, signature(info)))
            else:
                raise StorageError(f"Unsupported link or special file: {child}")

    if not stat.S_ISDIR(root.lstat().st_mode):
        raise StorageError(f"Expected a real directory: {root}")
    walk(root)
    return files, sorted(directories)


def copy_file(source, target):
    """Copy through a regular-file descriptor and reject concurrently changed inputs."""
    before = signature(source.lstat())
    fd = os.open(source, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, "rb") as incoming:
        info = os.fstat(incoming.fileno())
        if not stat.S_ISREG(info.st_mode) or signature(info) != before:
            raise StorageError(f"Input changed before copying: {source}")
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("xb") as outgoing:
            shutil.copyfileobj(incoming, outgoing, 1024 * 1024)
            outgoing.flush()
            os.fchmod(outgoing.fileno(), 0o755 if info.st_mode & 0o111 else 0o644)
            os.fsync(outgoing.fileno())
        if (
            signature(os.fstat(incoming.fileno())) != before
            or signature(source.lstat()) != before
        ):
            raise StorageError(f"Input changed during copying: {source}")


def copy_directory(source, target):
    """Copy a stable tree, retaining empty directories and executable entry scripts."""
    before = tree_entries(source)
    target.mkdir()
    for relative in before[1]:
        (target / relative).mkdir(parents=True, exist_ok=True)
    for relative, _ in before[0]:
        copy_file(source / relative, target / relative)
    if tree_entries(source) != before:
        raise StorageError(f"Input directory changed during copying: {source}")


def unpack_zip(archive_path, target):
    """Extract regular ZIP members explicitly; never follow links or escaping paths."""
    with zipfile.ZipFile(archive_path) as archive:
        members, seen = [], set()
        for member in archive.infolist():
            name = member.filename
            relative = PurePosixPath(name)
            mode = member.external_attr >> 16
            if (
                relative.is_absolute()
                or ".." in relative.parts
                or not relative.parts
                or "\\" in name
                or ":" in name
                or "\0" in name
                or stat.S_IFMT(mode) not in (0, stat.S_IFREG, stat.S_IFDIR)
            ):
                raise StorageError(f"Unsafe ZIP member: {name}")
            normalized = relative.as_posix()
            if normalized in seen:
                raise StorageError(f"Duplicate ZIP member: {name}")
            seen.add(normalized)
            members.append((member, relative, mode))
        target.mkdir()
        for member, relative, mode in members:
            destination = target / str(relative)
            if member.is_dir():
                destination.mkdir(parents=True, exist_ok=True)
            else:
                destination.parent.mkdir(parents=True, exist_ok=True)
                with archive.open(member) as incoming, destination.open(
                    "xb"
                ) as outgoing:
                    shutil.copyfileobj(incoming, outgoing, 1024 * 1024)
                    outgoing.flush()
                    os.fchmod(outgoing.fileno(), 0o755 if mode & 0o111 else 0o644)
                    os.fsync(outgoing.fileno())


def inventory(root):
    """Fingerprint paths and bytes, including empty folders, to detect later tampering."""
    files, directories = tree_entries(root)
    result = {"directories": directories, "files": []}
    for relative, _ in files:
        path = root / relative
        result["files"].append(
            {
                "path": relative,
                "size": path.stat().st_size,
                "sha256": digest(path),
                "executable": bool(path.stat().st_mode & 0o111),
            }
        )
    return result


def copy_input(source, target, temporary_archive):
    """Directories, ZIP uploads and ordinary dataset files become managed snapshots."""
    if source.is_dir():
        copy_directory(source, target)
        return "directory", None
    if source.suffix.lower() == ".zip":
        copy_file(source, temporary_archive)
        archive_hash = digest(temporary_archive)
        unpack_zip(temporary_archive, target)
        temporary_archive.unlink()
        return "zip", archive_hash
    target.mkdir()
    copy_file(source, target / source.name)
    return "file", None


def verify_workspace(workspace, job, published_path=None):
    """Reconstruct a job entirely from managed metadata and compare every input hash."""
    if workspace.is_symlink() or not workspace.is_dir():
        raise StorageError("Managed workspace is missing or is a symbolic link")
    for name in (
        "manifest.json",
        "submission.json",
        "source",
        "data",
        *MUTABLE_DIRECTORIES,
    ):
        path = workspace / name
        if path.is_symlink() or not path.exists():
            raise StorageError(f"Managed workspace entry is missing or linked: {name}")
        if name in MUTABLE_DIRECTORIES and not path.is_dir():
            raise StorageError(f"Runtime storage entry is not a directory: {name}")
    manifest_path = workspace / "manifest.json"
    manifest_hash = digest(manifest_path)
    if (
        job.get("storage_manifest_sha256")
        and manifest_hash != job["storage_manifest_sha256"]
    ):
        raise StorageError("Storage manifest hash does not match the database")
    manifest = json.loads(manifest_path.read_text())
    original = json.loads((workspace / "submission.json").read_text())
    if (
        manifest.get("storage_version") != 1
        or manifest.get("job_id") != job["job_id"]
        or manifest.get("owner") != job["owner"]
    ):
        raise StorageError("Storage metadata belongs to a different submission")
    if manifest["submission_sha256"] != digest(workspace / "submission.json"):
        raise StorageError("Original submission metadata changed")
    spec = json.loads(job["job_config"])
    expected_spec = (
        manifest["execution_spec"] if job["status"] != "RECEIVED" else original
    )
    if spec != expected_spec:
        raise StorageError("Storage definition does not match the database submission")
    for name in ("source", "data"):
        actual = inventory(workspace / name)
        expected = manifest[name]["inventory"]
        if actual != expected or manifest[name]["version"] != (
            None
            if manifest[name]["kind"] == "generated"
            else "sha256:" + hashlib.sha256(encoded(actual)).hexdigest()
        ):
            raise StorageError(f"{name}: stored content no longer matches its version")
    managed = published_path or workspace
    effective = {
        **original,
        "project": str(managed / "source"),
        "dataset": manifest["data"]["path"],
    }
    if effective != manifest["execution_spec"]:
        raise StorageError("Managed execution paths changed")
    data_path = manifest["data"]["path"]
    if original["dataset"] is None:
        expected_data = None
    elif manifest["data"]["kind"] == "file":
        expected_data = str(managed / "data" / Path(original["dataset"]).name)
    elif manifest["data"]["kind"] in ("directory", "zip"):
        expected_data = str(managed / "data")
    else:
        raise StorageError("Unrecognized stored dataset kind")
    if data_path != expected_data or manifest["source"]["path"] != str(
        managed / "source"
    ):
        raise StorageError("Stored input paths do not match the managed workspace")
    if not (workspace / "source" / original["environment"]["requirements"]).is_file():
        raise StorageError("Managed project is missing its dependency file")
    return manifest, manifest_hash


@contextmanager
def workspace_lock(root, job_id):
    """One staging process owns a job; the OS releases its lock after a crash."""
    locks = root / ".locks"
    locks.mkdir(parents=True, exist_ok=True)
    with (locks / (job_id + ".lock")).open("a") as stream:
        try:
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise StorageError("This submission is already being staged") from error
        yield


def stage_job(job_id):
    """Publish a snapshot or reconcile an already-published one after a restart."""
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", job_id):
        raise StorageError("Invalid job ID")
    job = get_job(job_id)
    if (
        job is None
        or job["job_type"] != "custom"
        or job["status"] not in ("RECEIVED", "STAGED", "READY")
    ):
        raise StorageError("Only RECEIVED or STAGED student submissions can be staged")
    root = (
        Path(job["storage_path"]).parent if job.get("storage_path") else storage_root()
    )
    workspace = root / job_id
    try:
        if not workspace.exists():
            spec = json.loads(job["job_config"])
            for value in (spec["project"], spec["dataset"]):
                path = Path(value) if value is not None else None
                if path is not None and path.is_dir() and root.is_relative_to(path):
                    raise StorageError(
                        "Storage root is inside an input directory; choose a separate DL_STORAGE_ROOT"
                    )
        root.mkdir(parents=True, exist_ok=True)
        with workspace_lock(root, job_id):
            job = get_job(job_id)
            if workspace.exists() or workspace.is_symlink():
                manifest, manifest_hash = verify_workspace(workspace, job)
            else:
                if job["status"] in ("STAGED", "READY"):
                    raise StorageError(
                        "Committed workspace is missing; refusing to replace it"
                    )
                spec = json.loads(job["job_config"])
                project = Path(spec["project"])
                dataset = Path(spec["dataset"]) if spec["dataset"] is not None else None
                # A crashed unpublished copy has no database authority. Remove only
                # this job's temporary workspaces while holding its staging lock.
                for abandoned in root.glob(f".{job_id}.staging-*"):
                    if abandoned.is_symlink():
                        abandoned.unlink()
                    elif abandoned.is_dir():
                        shutil.rmtree(abandoned)
                temporary = Path(
                    tempfile.mkdtemp(prefix=f".{job_id}.staging-", dir=root)
                )
                try:
                    source_kind, source_archive_hash = copy_input(
                        project, temporary / "source", temporary / "project.zip"
                    )
                    if dataset is None:
                        (temporary / "data").mkdir()
                        data_kind, data_archive_hash = "generated", None
                    else:
                        data_kind, data_archive_hash = copy_input(
                            dataset, temporary / "data", temporary / "dataset.zip"
                        )
                    for name in MUTABLE_DIRECTORIES:
                        (temporary / name).mkdir()
                    data_path = (
                        None
                        if dataset is None
                        else (
                            str(workspace / "data" / dataset.name)
                            if data_kind == "file"
                            else str(workspace / "data")
                        )
                    )
                    effective = {
                        **spec,
                        "project": str(workspace / "source"),
                        "dataset": data_path,
                    }
                    write_metadata(temporary / "submission.json", spec)
                    manifest = {
                        "storage_version": 1,
                        "job_id": job_id,
                        "owner": job["owner"],
                        "created_at": time.time(),
                        "submission_sha256": digest(temporary / "submission.json"),
                        "execution_spec": effective,
                    }
                    for name, kind, archive_hash in (
                        ("source", source_kind, source_archive_hash),
                        ("data", data_kind, data_archive_hash),
                    ):
                        listing = inventory(temporary / name)
                        manifest[name] = {
                            "kind": kind,
                            "path": (
                                str(workspace / "source")
                                if name == "source"
                                else data_path
                            ),
                            "inventory": listing,
                            "version": (
                                None
                                if kind == "generated"
                                else "sha256:"
                                + hashlib.sha256(encoded(listing)).hexdigest()
                            ),
                            "upload_sha256": archive_hash,
                        }
                    write_metadata(temporary / "manifest.json", manifest)
                    verify_workspace(temporary, job, published_path=workspace)
                    for directory, _, _ in os.walk(temporary, topdown=False):
                        sync_directory(directory)
                    temporary.rename(workspace)
                    sync_directory(root)
                finally:
                    if temporary.exists():
                        shutil.rmtree(temporary)
                manifest, manifest_hash = verify_workspace(workspace, job)
            mark_staged(
                job_id, str(workspace), manifest_hash, manifest["execution_spec"]
            )
            return receipt(manifest, workspace, get_job(job_id)["status"])
    except (OSError, ValueError, RuntimeError, KeyError, zipfile.BadZipFile) as error:
        record_staging_error(job_id, str(error))
        raise StorageError(f"{job_id}: {error}") from error


def receipt(manifest, workspace, status="STAGED"):
    """Expose storage locations and versions without printing a potentially large file list."""
    return {
        "job_id": manifest["job_id"],
        "status": status,
        "storage_path": str(workspace),
        "project_path": manifest["source"]["path"],
        "dataset_path": manifest["data"]["path"],
        "project_version": manifest["source"]["version"],
        "dataset_version": (
            None
            if manifest["data"]["kind"] == "generated"
            else manifest["data"]["version"]
        ),
    }
