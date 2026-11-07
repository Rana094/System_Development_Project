"""Read-only NFS mount admission check, also installable as a standalone guard.

This reads kernel metadata only: it never looks up a file in the mount, triggers
an automount, stages inputs or authorizes/launches training. The future trusted
stager must use the check in its own mount namespace before accessing inputs.
"""

import argparse
import ipaddress
import json
from pathlib import Path
import re

INPUTS = "/srv/mlnfs/inputs"
EXPORT = "/srv/ml-shared/objects"
REQUIRED = {"ro", "nosuid", "nodev", "noexec", "hard", "proto=tcp", "sec=sys", "vers=4.1"}
FORBIDDEN = {"rw", "soft", "softerr", "softreval", "suid", "dev", "exec", "user", "users"}


class MountNotReady(ValueError):
    """The visible mount metadata does not meet the reviewed input contract."""


def unescape(value):
    return re.sub(r"\\([0-7]{3})", lambda match: chr(int(match[1], 8)), value)


def parse_mountinfo(text):
    mounts = []
    for line in text.splitlines():
        if not line:
            continue
        try:
            left, right = line.split(" - ", 1)
            a, b = left.split(), right.split()
            mounts.append({"id": int(a[0]), "parent": int(a[1]), "device": a[2],
                           "root": unescape(a[3]), "target": unescape(a[4]),
                           "vfs_options": a[5].split(","), "fstype": b[0],
                           "source": unescape(b[1]), "fs_options": b[2].split(",")})
        except (ValueError, IndexError) as error:
            raise MountNotReady("Malformed kernel mount metadata") from error
    return mounts


def validate_source(source):
    try:
        host, path = source.split(":", 1)
        ip = ipaddress.IPv4Address(host)
    except (ValueError, AttributeError) as error:
        raise MountNotReady("Expected a literal IPv4 controller and fixed input export") from error
    if path != EXPORT or ip.is_loopback or ip.is_multicast or ip.is_unspecified or ip.is_reserved or ip.is_link_local:
        raise MountNotReady("Unsafe controller input source")
    return str(ip) + ":" + EXPORT


def require_input_mount(source, mountinfo=None, *, target=INPUTS):
    source = validate_source(source)
    target = str(target)
    path = Path(target)
    if not path.is_absolute() or path == Path("/") or ".." in path.parts or str(path) != target:
        raise MountNotReady("Expected an exact absolute input mount target other than /")
    try:
        text = Path("/proc/self/mountinfo").read_text() if mountinfo is None else mountinfo
    except FileNotFoundError as error:
        raise MountNotReady("Kernel mount metadata unavailable") from error
    mounts = parse_mountinfo(text)
    exact = [m for m in mounts if m["target"] == target]
    nfs = [m for m in exact if m["fstype"] in ("nfs", "nfs4")]
    if len(nfs) != 1 or any(m["fstype"] not in ("autofs", "nfs", "nfs4") for m in exact) or len(exact) > 2:
        raise MountNotReady("Exact input NFS mount absent or shadowed; an ordinary directory/autofs is not ready")
    mount = nfs[0]
    if mount["source"] != source or mount["root"] != "/":
        raise MountNotReady("Wrong input source or bind-subdirectory mount")
    by_id = {item["id"]: item for item in mounts}
    if len(by_id) != len(mounts):
        raise MountNotReady("Duplicate kernel mount identity")
    ancestors = set()
    current = mount
    while current is not None:
        if current["id"] in ancestors:
            raise MountNotReady("Cyclic kernel mount ancestry")
        ancestors.add(current["id"])
        current = by_id.get(current["parent"])
    for item in mounts:
        ancestor = item["target"]
        if ancestor != target and (ancestor == "/" or target.startswith(ancestor.rstrip("/") + "/")) and item["id"] not in ancestors:
            raise MountNotReady("Input mount hidden by an ancestor overmount")
    vfs = set(mount["vfs_options"])
    values = vfs | set(mount["fs_options"])
    # Linux may report an rw NFS superblock beneath a read-only mount. Access
    # restrictions are per-mount VFS flags; server export policy is checked
    # separately. Never accept rw (or missing security flags) in the VFS field.
    if not REQUIRED <= values or values & (FORBIDDEN - {"rw"}) or "rw" in vfs or not {"ro", "nosuid", "nodev", "noexec"} <= vfs:
        raise MountNotReady("Input mount lacks required read-only/security/protocol options")
    if any(m["target"].startswith(target + "/") for m in mounts):
        raise MountNotReady("Unexpected nested mount inside input tree")
    return {"ready": True, "mount_id": mount["id"], "source": source, "target": target,
            "version": "4.1", "scope": "Metadata admission only; not server liveness, authorization or byte verification"}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True)
    args = parser.parse_args(argv)
    try:
        print(json.dumps(require_input_mount(args.source), sort_keys=True))
        return 0
    except (MountNotReady, OSError) as error:
        print(json.dumps({"ready": False, "error": str(error)}, sort_keys=True))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
