"""Linux descriptor mount identity; metadata only, no remote file lookup."""

import errno
from pathlib import Path

from .nfs_mount import MountNotReady, parse_mountinfo

# Deliberately conservative publication support. Unknown/FUSE/remote/ephemeral
# filesystems fail closed. Adding a filesystem requires an explicit review/test.
LOCAL_PUBLICATION_TYPES = frozenset(("ext4", "xfs", "btrfs"))


def fd_mount_id(fd: int) -> int:
    try:
        rows = Path(f"/proc/self/fdinfo/{fd}").read_text().splitlines()
    except FileNotFoundError as error:
        raise MountNotReady("Opened descriptor identity unavailable") from error
    values = [row.split(":", 1)[1].strip() for row in rows if row.startswith("mnt_id:")]
    if len(values) != 1 or not values[0].isdigit():
        raise MountNotReady("Malformed descriptor mount identity")
    return int(values[0])


def require_local_fd(fd: int) -> dict:
    identity = fd_mount_id(fd)
    mounts = parse_mountinfo(Path("/proc/self/mountinfo").read_text())
    matching = [mount for mount in mounts if mount["id"] == identity]
    if len(matching) != 1 or matching[0]["fstype"] not in LOCAL_PUBLICATION_TYPES:
        raise OSError(errno.EXDEV, "Controller publication requires a supported local filesystem")
    return matching[0]
