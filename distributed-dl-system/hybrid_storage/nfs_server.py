"""Plan and stage one narrowly scoped NFS server configuration; never activate it.

The CLI defaults to a read-only plan. Future --apply stages directories/config
only, requires root and the current reviewed plan digest, and makes a backup
first. Package installation, exportfs and systemd activation are separate actions.
"""

import argparse
from datetime import datetime, timezone
import hashlib
import fcntl
import ipaddress
import json
import os
from pathlib import Path
import pwd
import grp
import re
import stat
import subprocess
import tempfile
import uuid

SHARED = "/srv/ml-shared"
OBJECTS = SHARED + "/objects"
STAGING = SHARED + "/.publish"
MARKER = SHARED + "/.nfs-setup.json"
EXPORTS = "/etc/exports.d/distributed-ml.exports"
BACKUPS = "/var/backups/distributed-ml-nfs"
HEADER = "# Managed by distributed-dl-system nfs-server v1\n"
CONFIG_FILE = Path(__file__).resolve().parents[1] / "config/nfs-server.json"
MAX_BYTES = 256 * 1024
LOCAL_FILESYSTEMS = {"ext4", "ext3", "ext2", "xfs", "btrfs", "f2fs", "overlay", "tmpfs"}


class SetupError(ValueError):
    """A proposed setup cannot be safely interpreted or applied."""


def encoded(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def digest(value):
    return hashlib.sha256(encoded(value)).hexdigest()


def _unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise SetupError("Duplicate configuration field: " + key)
        result[key] = value
    return result


def address(value):
    if type(value) is not str:
        raise SetupError("Clients/server must be literal IPv4 host addresses")
    try:
        ip = ipaddress.IPv4Address(value)
    except ipaddress.AddressValueError as error:
        raise SetupError("Clients/server must be literal IPv4 host addresses") from error
    if ip.is_loopback or ip.is_multicast or ip.is_unspecified or ip.is_link_local or ip.is_reserved or int(ip) == 0xffffffff:
        raise SetupError("Use a routable, explicitly reviewed IPv4 host address")
    return str(ip)


def selected_nodes(config):
    """Default to all three; a smaller deployment requires an explicit selection."""
    nodes = config.get("deployment_nodes", ["pi01", "pi02", "pi03"])
    if not isinstance(nodes, list) or not nodes or any(type(node) is not str or node not in {"pi01", "pi02", "pi03"} for node in nodes) or len(nodes) != len(set(nodes)):
        raise SetupError("deployment_nodes must be a nonempty unique list of known Pi IDs")
    return sorted(nodes)


def validate_config(value):
    fields = {"schema_version", "server_address", "clients", "publisher_uid", "publisher_gid", "layout"}
    if not isinstance(value, dict) or set(value) not in (fields, fields | {"deployment_nodes"}) or type(value["schema_version"]) is not int or value["schema_version"] != 1:
        raise SetupError("Expected exactly the version-one NFS server configuration fields")
    selected_nodes(value)
    address(value["server_address"])
    clients = value["clients"]
    if not isinstance(clients, dict) or set(clients) != {"pi01", "pi02", "pi03"}:
        raise SetupError("Explicitly list pi01, pi02 and pi03; unknown addresses must be null")
    known = [address(ip) for ip in clients.values() if ip is not None]
    if len(set(known)) != len(known) or value["server_address"] in known:
        raise SetupError("Client addresses must be distinct and different from the server")
    for field in ("publisher_uid", "publisher_gid"):
        number = value[field]
        if number is not None and (type(number) is not int or not 1 <= number < 2**31):
            raise SetupError(field + " must be an existing dedicated non-root identity, or null")
    if value["layout"] not in ("subdirectory", "dedicated-filesystem"):
        raise SetupError("layout must be subdirectory or dedicated-filesystem")
    return value


def load_config(path):
    with Path(path).open("rb") as stream:
        raw = stream.read(MAX_BYTES + 1)
    if len(raw) > MAX_BYTES:
        raise SetupError("NFS server configuration is too large")
    return validate_config(json.loads(raw.decode("utf-8"), object_pairs_hook=_unique))


def options(layout):
    if layout == "dedicated-filesystem":
        return "ro,sync,root_squash,no_subtree_check,mountpoint,secure,sec=sys"
    return "ro,sync,root_squash,subtree_check,secure,sec=sys"


def render_exports(config):
    entries = [ip + "(" + options(config["layout"]) + ")"
               for node, ip in sorted(config["clients"].items()) if node in selected_nodes(config) and ip is not None]
    return HEADER + (OBJECTS + " " + " ".join(entries) + "\n" if entries else "")


def check_exports(text, *, managed=False):
    """Conservative syntax/policy checker, not the complete nfs-utils grammar.

    Unmanaged tables may contain comments only: refuse, never reinterpret or
    activate existing unrelated exports. Managed rows allow only this exact root,
    literal hosts and fixed safe options. Unknown syntax is a review blocker.
    """
    if managed and not text.startswith(HEADER):
        raise SetupError("Existing managed filename lacks our ownership marker")
    rows = [line.strip() for line in text.splitlines() if line.strip() and not line.lstrip().startswith("#")]
    if not managed:
        if rows:
            raise SetupError("Unmanaged noncomment exports require separate syntax/policy review")
        return []
    if len(rows) > 1:
        raise SetupError("Managed export must contain exactly one input-only export row")
    clients = []
    for line in rows:
        parts = line.split()
        if not parts or parts[0] != OBJECTS or len(parts) < 2:
            raise SetupError("Only the dedicated immutable objects path may be exported")
        for part in parts[1:]:
            match = re.fullmatch(r"([0-9.]+)\(([^()]*)\)", part)
            if not match or match[2] not in (options("subdirectory"), options("dedicated-filesystem")):
                raise SetupError("Malformed or unsafe export client/options")
            clients.append(address(match[1]))
    if len(set(clients)) != len(clients):
        raise SetupError("Duplicate export client")
    return clients


def check_active_table(text, config):
    """Accept only this export's reviewed literal clients and safe known defaults."""
    allowed = set(options(config["layout"]).split(",")) | {
        "wdelay", "hide", "nocrossmnt", "no_all_squash", "secure_locks", "acl",
        "no_pnfs", "anonuid=65534", "anongid=65534",
    }
    expected_clients = {config["clients"][node] for node in selected_nodes(config) if config["clients"][node] is not None}
    for line in text.splitlines():
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        parts = line.split()
        if len(parts) < 2 or parts[0] != OBJECTS:
            raise SetupError("Unexpected existing active export path")
        for client in parts[1:]:
            match = re.fullmatch(r"([0-9.]+)\(([^()]*)\)", client)
            if not match or address(match[1]) not in expected_clients:
                raise SetupError("Unexpected active export client")
            values = set(match[2].split(","))
            if not set(options(config["layout"]).split(",")) <= values or not values <= allowed:
                raise SetupError("Active export options require separate review")


class Host:
    """All paths are fixed logical host paths; tests use an isolated fake root."""

    def __init__(self, root=Path("/"), admin_uid=0):
        self.root = Path(root)
        self.admin_uid = admin_uid

    def path(self, logical):
        return self.root / logical.lstrip("/")

    def facts(self):
        def run(args):
            result = subprocess.run(args, capture_output=True, text=True, timeout=5)
            if result.returncode:
                raise SetupError("Read-only prerequisite query failed: " + " ".join(args))
            return result.stdout
        packages = {}
        for name in ("nfs-kernel-server", "nfs-common"):
            result = subprocess.run(["dpkg-query", "-W", "-f=${Status}", name], capture_output=True, text=True, timeout=5)
            packages[name] = result.returncode == 0 and result.stdout == "install ok installed"
        interfaces = json.loads(run(["ip", "-j", "address", "show"]))
        addresses = sorted(a["local"] for i in interfaces for a in i.get("addr_info", []) if a.get("family") == "inet")
        mounts = []
        for line in Path("/proc/self/mountinfo").read_text().splitlines():
            left, right = line.split(" - ", 1)
            a, b = left.split(), right.split()
            target = re.sub(r"\\([0-7]{3})", lambda m: chr(int(m[1], 8)), a[4])
            mounts.append({"target": target, "root": a[3], "device": a[2], "fstype": b[0]})
        return {"packages": packages, "addresses": addresses, "mounts": mounts}

    def metadata(self, logical, mounts):
        path = self.path(logical)
        for parent in [*reversed(path.relative_to(self.root).parents), path.relative_to(self.root)]:
            logical_parent = "/" if parent == Path(".") else "/" + parent.as_posix()
            matches = [m for m in mounts if logical_parent == m["target"] or logical_parent.startswith(m["target"].rstrip("/") + "/")]
            if matches and max(matches, key=lambda m: len(m["target"]))["fstype"] not in LOCAL_FILESYSTEMS:
                raise SetupError("Refusing remote/automount/unknown filesystem traversal: " + logical)
            actual = self.root / parent
            try:
                info = actual.lstat()
            except FileNotFoundError:
                return None
            if stat.S_ISLNK(info.st_mode):
                raise SetupError("Symlink in host setup path: " + str(actual))
            if actual != path and (not stat.S_ISDIR(info.st_mode) or info.st_uid != self.admin_uid or info.st_mode & 0o022):
                raise SetupError("Host setup ancestor is not a protected admin directory: " + str(actual))
        return {"uid": info.st_uid, "gid": info.st_gid, "mode": stat.S_IMODE(info.st_mode),
                "directory": stat.S_ISDIR(info.st_mode), "regular": stat.S_ISREG(info.st_mode)}

    def read(self, logical, mounts):
        info = self.metadata(logical, mounts)
        if info is None:
            return None
        if not info["regular"]:
            raise SetupError("Expected a regular administrative file: " + logical)
        with self.path(logical).open("rb") as stream:
            raw = stream.read(MAX_BYTES + 1)
        if len(raw) > MAX_BYTES:
            raise SetupError("Administrative file exceeds inspection bound: " + logical)
        return {"text": raw.decode("utf-8"), **info, "sha256": hashlib.sha256(raw).hexdigest()}


def build_plan(config, host=None):
    config = validate_config(config)
    host = host or Host()
    facts = host.facts()
    blockers = []
    admin_directories = {}
    for name in ("/srv", "/etc", "/var/backups", BACKUPS):
        info = host.metadata(name, facts["mounts"])
        admin_directories[name] = info
        if info and (not info["directory"] or info["uid"] != host.admin_uid or info["mode"] & 0o022):
            raise SetupError("Unprotected administrative directory: " + name)
        if info is None and name != BACKUPS:
            blockers.append("Required administrative parent directory missing: " + name)
    for node, ip in config["clients"].items():
        if node in selected_nodes(config) and ip is None:
            blockers.append(node + ": verified client address missing; never broaden the allowlist to compensate")
    if config["server_address"] not in facts["addresses"]:
        blockers.append("Proposed server address is not currently assigned to this controller")
    for name, installed in facts["packages"].items():
        if not installed:
            blockers.append(name + " missing: package provisioning needs separate approval; script never installs it")
    for name in ("publisher_uid", "publisher_gid"):
        if config[name] is None:
            blockers.append(name + " missing: review/preprovision the dedicated trusted publisher identity")
    if host.root == Path("/") and config["publisher_uid"] is not None and config["publisher_gid"] is not None:
        try:
            pwd.getpwuid(config["publisher_uid"])
            grp.getgrgid(config["publisher_gid"])
        except KeyError:
            blockers.append("Publisher UID/GID must already exist; no account creation is performed")
    files = {}
    names = ["/etc/exports", EXPORTS]
    directory = host.metadata("/etc/exports.d", facts["mounts"])
    if directory:
        if not directory["directory"] or directory["uid"] != host.admin_uid or directory["mode"] & 0o022:
            raise SetupError("exports.d must be a protected admin directory")
        candidates = sorted(p.name for p in host.path("/etc/exports.d").iterdir() if p.name.endswith(".exports") and not p.name.startswith("."))
        if len(candidates) > 32:
            raise SetupError("Too many export tables for this bounded preparer")
        names += ["/etc/exports.d/" + name for name in candidates]
    for name in sorted(set(names)):
        record = host.read(name, facts["mounts"])
        files[name] = record
        if record:
            if record["uid"] != host.admin_uid or record["mode"] & 0o022:
                raise SetupError("Export configuration must be admin-owned and not group/world writable")
            check_exports(record["text"], managed=name == EXPORTS)
    # Read the managed export table directly; do not invoke exportfs for inspection.
    etab = host.read("/var/lib/nfs/etab", facts["mounts"])
    if etab:
        if etab["uid"] != host.admin_uid or etab["mode"] & 0o022:
            raise SetupError("Active export table must be protected admin-owned metadata")
        check_active_table(etab["text"], config)
    dirs = {name: host.metadata(name, facts["mounts"]) for name in (SHARED, OBJECTS, STAGING)}
    marker = host.read(MARKER, facts["mounts"])
    expected_marker = {"version": 1, "objects": OBJECTS, "publisher_uid": config["publisher_uid"], "publisher_gid": config["publisher_gid"]}
    if marker:
        if marker["uid"] != host.admin_uid or marker["mode"] != 0o600 or json.loads(marker["text"]) != expected_marker:
            raise SetupError("Existing setup marker differs; never change publisher ownership automatically")
    elif dirs[SHARED] and any(host.path(SHARED).iterdir()):
        raise SetupError("Refusing to adopt nonempty unmarked /srv/ml-shared")
    expected = {SHARED: (host.admin_uid, None, 0o755), OBJECTS: (config["publisher_uid"], config["publisher_gid"], 0o755), STAGING: (config["publisher_uid"], config["publisher_gid"], 0o700)}
    for name, current in dirs.items():
        uid, gid, mode = expected[name]
        if current and (not current["directory"] or current["mode"] != mode or (uid is not None and current["uid"] != uid) or (gid is not None and current["gid"] != gid)):
            raise SetupError("Existing directory attributes differ; no chown/chmod of existing data: " + name)
    mount = next((m for m in facts["mounts"] if m["target"] == OBJECTS), None)
    parent_mounts = [m for m in facts["mounts"] if SHARED == m["target"] or SHARED.startswith(m["target"].rstrip("/") + "/")]
    parent_mount = max(parent_mounts, key=lambda m: len(m["target"])) if parent_mounts else None
    if config["layout"] == "dedicated-filesystem" and not (mount and mount["root"] == "/" and parent_mount and mount["device"] != parent_mount["device"]):
        blockers.append("no_subtree_check requires a mounted whole dedicated filesystem, not a bind/empty directory")
    desired = render_exports(config)
    check_exports(desired, managed=True)
    observed = {"facts": facts, "config_files": files, "active_table": etab, "directories": dirs,
                "marker": marker, "exports_directory": directory, "admin_directories": admin_directories}
    plan = {"version": 1, "config": config, "desired_exports": desired, "observed": observed,
            "blockers": blockers, "apply_ready": not blockers, "mode": "dry-run",
            "scope": "Stage configuration only; never install packages, activate exports or change services",
            "proposed_directories": [
                {"path": SHARED, "uid": host.admin_uid, "gid": "admin primary group", "mode": "0755"},
                {"path": OBJECTS, "uid": config["publisher_uid"], "gid": config["publisher_gid"], "mode": "0755"},
                {"path": STAGING, "uid": config["publisher_uid"], "gid": config["publisher_gid"], "mode": "0700"}],
            "config_file_to_stage": EXPORTS, "existing_data_policy": "Verify existing attributes; never recurse, delete or change them",
            "package_command_for_separate_review_only": ["apt-get", "--no-download", "install", "nfs-kernel-server", "nfs-common"],
            "activation_after_separate_approval": ["systemctl start nfs-server.service", "exportfs -ra"],
            "backup_before_changes": BACKUPS, "syntax_check": "Conservative managed grammar; unmanaged files must be comments/empty. Native exportfs not invoked."}
    plan["plan_sha256"] = digest(plan)
    return plan


def _write_new(path, raw, mode, uid, gid):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, mode)
    with os.fdopen(fd, "wb") as stream:
        stream.write(raw)
        stream.flush()
        os.fchmod(stream.fileno(), mode)
        os.fchown(stream.fileno(), uid, gid)
        os.fsync(stream.fileno())


def _sync(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def stage_configuration(config, reviewed_sha256, host=None):
    """Future root-only configuration staging. Never call without user approval.

    A matching digest binds review to current facts; it is not itself permission.
    Failures leave backups and any new directories intact, without activating NFS.
    """
    host = host or Host()
    if os.geteuid() != host.admin_uid:
        raise SetupError("Configuration staging requires the host administrator")
    plan = build_plan(config, host)
    if reviewed_sha256 != plan["plan_sha256"]:
        raise SetupError("Current plan differs from reviewed digest; review a fresh dry-run")
    if plan["blockers"]:
        raise SetupError("Cannot stage configuration: " + "; ".join(plan["blockers"]))
    current = plan["observed"]["config_files"][EXPORTS]
    if current and current["text"] == plan["desired_exports"] and plan["observed"]["marker"] and all(plan["observed"]["directories"].values()):
        return {"status": "no_changes", "activated": False}
    # Only future explicitly approved staging creates this private lock. Dry-runs
    # do not create it. Protected /etc and no-follow prevent untrusted lock paths.
    lock = host.path("/etc/.distributed-ml-nfs.lock")
    fd = os.open(lock, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_uid != host.admin_uid or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1:
            raise SetupError("Unprotected NFS setup lock")
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fresh = build_plan(config, host)
        if fresh["plan_sha256"] != reviewed_sha256 or fresh["blockers"]:
            raise SetupError("Controller changed while obtaining setup lock; review again")
        return _stage_reviewed(fresh, host)
    finally:
        os.close(fd)


def _stage_reviewed(plan, host):
    config = plan["config"]
    state = plan["observed"]
    mounts = state["facts"]["mounts"]
    # Check all administrative ancestors before any creation. No recursive fixups.
    for name in ("/srv", "/etc", "/var/backups", BACKUPS):
        info = host.metadata(name, mounts)
        if info and (not info["directory"] or info["uid"] != host.admin_uid or info["mode"] & 0o022):
            raise SetupError("Unprotected administrative directory: " + name)
    backup_root = host.path(BACKUPS)
    backup_root.mkdir(mode=0o700, exist_ok=True)
    _sync(backup_root.parent)
    backup = backup_root / (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex)
    backup.mkdir(mode=0o700)
    record = {"reviewed_plan_sha256": plan["plan_sha256"], "files": state["config_files"],
              "installed_exports_sha256": hashlib.sha256(plan["desired_exports"].encode()).hexdigest(),
              "restore_scope": "Restore only the managed drop-in after hash/ownership checks; other exports were never modified. Preserve all input/staging directories."}
    _write_new(backup / "before.json", encoded(record), 0o600, host.admin_uid, os.getegid())
    _sync(backup)
    _sync(backup_root)
    def create_directory(name, uid, gid, mode):
        if not host.path(name).exists():
            host.path(name).mkdir(mode=mode)
            os.chown(host.path(name), uid, gid)
            os.chmod(host.path(name), mode)
            _sync(host.path(name).parent)
    create_directory(SHARED, host.admin_uid, os.getegid(), 0o755)
    if state["marker"] is None:
        marker = {"version": 1, "objects": OBJECTS, "publisher_uid": config["publisher_uid"], "publisher_gid": config["publisher_gid"]}
        _write_new(host.path(MARKER), encoded(marker), 0o600, host.admin_uid, os.getegid())
        _sync(host.path(SHARED))
    # The marker precedes child directories, so interrupted setup can be safely
    # resumed without adopting an unmarked nonempty directory.
    create_directory(OBJECTS, config["publisher_uid"], config["publisher_gid"], 0o755)
    create_directory(STAGING, config["publisher_uid"], config["publisher_gid"], 0o700)
    host.path("/etc/exports.d").mkdir(mode=0o755, exist_ok=True)
    _sync(host.path("/etc"))
    fd, temporary = tempfile.mkstemp(prefix=".distributed-ml-", dir=host.path("/etc/exports.d"))
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(plan["desired_exports"].encode())
            stream.flush()
            os.fchmod(stream.fileno(), 0o644)
            os.fchown(stream.fileno(), host.admin_uid, os.getegid())
            os.fsync(stream.fileno())
        if host.read(EXPORTS, mounts) != state["config_files"][EXPORTS]:
            raise SetupError("Managed config changed before publication; preserving it")
        if state["config_files"][EXPORTS] is None:
            os.link(temporary, host.path(EXPORTS), follow_symlinks=False)
        else:
            os.replace(temporary, host.path(EXPORTS))
        _sync(host.path("/etc/exports.d"))
    finally:
        Path(temporary).unlink(missing_ok=True)
    return {"status": "configuration_staged_not_activated", "backup": str(backup), "activated": False}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=CONFIG_FILE)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="Read-only plan (also the default)")
    mode.add_argument("--apply", action="store_true", help="Future approved root-only config staging; never activates NFS")
    parser.add_argument("--reviewed-plan-sha256", help="Required with --apply, after explicit approval")
    args = parser.parse_args(argv)
    try:
        config = load_config(args.config)
        if args.apply:
            if not args.reviewed_plan_sha256:
                raise SetupError("--apply requires a current --reviewed-plan-sha256 and prior user approval")
            result = stage_configuration(config, args.reviewed_plan_sha256)
        else:
            if args.reviewed_plan_sha256:
                raise SetupError("Review digest is only used with --apply")
            result = build_plan(config)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 2 if result.get("blockers") else 0
    except (SetupError, OSError, ValueError, subprocess.SubprocessError) as error:
        parser.exit(2, "NFS server setup refused: " + str(error) + "\n")


if __name__ == "__main__":
    raise SystemExit(main())
