"""Read-only dataset tree inspection and ZIP preflight; never extraction/enrollment."""

import hashlib
import os
from pathlib import Path
import stat
import zipfile

from .backends import _directory, _root
from .dataset_manifests import DatasetEntry, MAX_ENTRIES, checked_entries, logical_path


def _signature(info):
    return tuple(getattr(info, name) for name in
                 ("st_dev", "st_ino", "st_mode", "st_size", "st_mtime_ns", "st_ctime_ns"))


def inventory_tree(root: Path) -> tuple[DatasetEntry, ...]:
    """Hash regular files through no-follow FDs under an authorized source root.

    This is inspection, not an atomic snapshot or artifact registration. The
    trusted caller must protect the source against edits for the entire operation
    and later registration; detected changes fail closed, never authorize bytes.
    """
    entries = []
    def walk(parent, prefix=""):
        before = _signature(os.fstat(parent))
        with os.scandir(parent) as children:
            for child in children:
                path = logical_path(prefix + child.name)
                if len(entries) >= MAX_ENTRIES:
                    raise ValueError("Dataset entry limit exceeded")
                info = child.stat(follow_symlinks=False)
                if stat.S_ISDIR(info.st_mode):
                    fd = os.open(child.name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
                    try:
                        if _signature(info) != _signature(os.fstat(fd)):
                            raise ValueError("Dataset directory changed before inspection")
                        entries.append(DatasetEntry(path, "directory"))
                        walk(fd, path + "/")
                    finally:
                        os.close(fd)
                elif stat.S_ISREG(info.st_mode):
                    fd = os.open(child.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
                    try:
                        opened = os.fstat(fd)
                        if not stat.S_ISREG(opened.st_mode) or _signature(info) != _signature(opened):
                            raise ValueError("Dataset file changed before inspection")
                        stream = os.fdopen(fd, "rb")
                    except BaseException:
                        os.close(fd)
                        raise
                    with stream:
                        digest = hashlib.sha256()
                        total = 0
                        while True:
                            chunk = stream.read(min(1024 * 1024, info.st_size - total + 1))
                            if not chunk:
                                break
                            total += len(chunk)
                            if total > info.st_size:
                                raise ValueError("Dataset file grew during inspection")
                            digest.update(chunk)
                        if total != info.st_size or _signature(os.fstat(stream.fileno())) != _signature(info):
                            raise ValueError("Dataset file changed during inspection")
                    entries.append(DatasetEntry(path, "file", digest.hexdigest(), total))
                else:
                    raise ValueError("Dataset links/special files are forbidden")
        if _signature(os.fstat(parent)) != before:
            raise ValueError("Dataset directory changed during inspection")
    with _directory(_root(root)) as fd:
        walk(fd)
    return checked_entries(entries)


def validate_zip_members(archive: zipfile.ZipFile, *, max_uncompressed_bytes: int) -> tuple[str, ...]:
    """Validate central-directory names/types/budget WITHOUT extracting or hashing.

    The caller bounds the uploaded archive before constructing ZipFile. Metadata
    approval alone is not CRC/content verification or a safe extraction token.
    A later extractor must enforce limits on actual streamed decompressed bytes.
    """
    if type(max_uncompressed_bytes) is not int or max_uncompressed_bytes < 0:
        raise ValueError("Expected a nonnegative uncompressed-byte budget")
    members = archive.infolist()
    if len(members) > MAX_ENTRIES:
        raise ValueError("ZIP entry limit exceeded")
    entries, total = [], 0
    for member in members:
        name = member.orig_filename
        if name != member.filename:  # ZipInfo truncates embedded NUL names.
            raise ValueError("ZIP member name was truncated/rewritten")
        directory = member.is_dir()
        path = logical_path(name[:-1] if directory else name)
        kind = stat.S_IFMT(member.external_attr >> 16)
        if (kind not in (0, stat.S_IFREG, stat.S_IFDIR)
                or (directory and kind == stat.S_IFREG)
                or (not directory and kind == stat.S_IFDIR)
                or (not directory and member.external_attr & 0x10)
                or member.flag_bits & 1
                or (directory and member.file_size != 0)):
            raise ValueError("ZIP links/special files/encrypted or inconsistent members forbidden")
        if type(member.file_size) is not int or member.file_size < 0:
            raise ValueError("ZIP member has an invalid byte size")
        total += member.file_size
        if total > max_uncompressed_bytes:
            raise ValueError("ZIP uncompressed-byte budget exceeded")
        # A sentinel is metadata preflight only; no entry leaves this function as
        # a file identity. Actual content hashes must come from verified bytes.
        entries.append(DatasetEntry(path, "directory") if directory else
                       DatasetEntry(path, "file", "0" * 64, 0))
    validated = checked_entries(entries, require_parents=False)
    return tuple(entry.path for entry in validated)
