"""Prepare boot-independent NFS client units; default is an offline selected-Pi plan.

--inspect-local requires an explicit node and only reads local state. Future
approved administrator actions provision a missing package or stage config;
neither enables units, activates mounts, edits fstab or alters the legacy agent.
"""

import argparse
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import uuid

from .nfs_mount import INPUTS, EXPORT, parse_mountinfo, unescape
from .nfs_server import Host, SetupError, MAX_BYTES, address, digest, encoded, selected_nodes, _unique, _write_new, _sync

CONFIG_FILE = Path(__file__).resolve().parents[1] / "config/nfs-clients.json"
ROOT = "/srv/mlnfs"
MARKER = ROOT + "/.nfs-client-setup.json"
LIBEXEC = "/usr/local/libexec/distributed-ml"
GUARD = LIBEXEC + "/nfs_mount.py"
MOUNT = "srv-mlnfs-inputs.mount"
AUTOMOUNT = "srv-mlnfs-inputs.automount"
READY = "ml-nfs-ready.service"
UNITS = "/etc/systemd/system"
BACKUPS = "/var/backups/distributed-ml-nfs-client"
LOCK = "/etc/.distributed-ml-nfs-client.lock"
HEADER = "# Managed by distributed-dl-system nfs-client v1\n"
MOUNT_OPTIONS = "ro,nosuid,nodev,noexec,hard,vers=4.1,proto=tcp,sec=sys,resvport,timeo=50,retrans=2,retry=0,nofail"
PACKAGE_COMMAND = ["/usr/bin/apt-get", "--no-download", "--no-remove", "--assume-yes", "install", "nfs-common"]
QUERY_ENV = {"PATH": "/usr/sbin:/usr/bin:/sbin:/bin", "LC_ALL": "C"}


def validate_config(config):
    fields = {"schema_version", "server_address", "clients"}
    if not isinstance(config, dict) or set(config) not in (fields, fields | {"deployment_nodes"}) or type(config["schema_version"]) is not int or config["schema_version"] != 1:
        raise SetupError("Expected exactly version-one NFS client fields")
    selected_nodes(config)
    address(config["server_address"])
    clients = config["clients"]
    if not isinstance(clients, dict) or set(clients) != {"pi01", "pi02", "pi03"}:
        raise SetupError("List all three Pis, with null for unknown LAN addresses")
    known = [address(value) for value in clients.values() if value is not None]
    if len(known) != len(set(known)) or config["server_address"] in known:
        raise SetupError("Clients must be distinct and different from the controller")
    return config


def load_config(path):
    with Path(path).open("rb") as stream:
        raw = stream.read(MAX_BYTES + 1)
    if len(raw) > MAX_BYTES:
        raise SetupError("Client proposal is too large")
    return validate_config(json.loads(raw.decode(), object_pairs_hook=_unique))


def source(config):
    return config["server_address"] + ":" + EXPORT


def render_files(config, node):
    validate_config(config)
    if node not in selected_nodes(config):
        raise SetupError("Selected Pi is excluded from this deployment")
    mount = HEADER + "\n".join([
        "[Unit]", "Description=Distributed ML trusted input mount (" + node + ")",
        "[Mount]", "What=" + source(config), "Where=" + INPUTS, "Type=nfs",
        "Options=" + MOUNT_OPTIONS, "TimeoutSec=15s", "DirectoryMode=0700", ""])
    automount = HEADER + "\n".join([
        "[Unit]", "Description=Distributed ML on-demand input automount (" + node + ")",
        "[Automount]", "Where=" + INPUTS, "DirectoryMode=0700", "TimeoutIdleSec=0",
        "[Install]", "WantedBy=multi-user.target", ""])
    ready = HEADER + "\n".join([
        "[Unit]", "Description=Distributed ML input mount admission (" + node + ")",
        "Requires=" + MOUNT, "After=" + MOUNT,
        "[Service]", "Type=oneshot", "User=root", "Group=root", "UMask=0077",
        "ExecStart=/usr/bin/python3 -I " + GUARD + " --source " + source(config),
        "NoNewPrivileges=yes", "ProtectSystem=strict", "ProtectHome=yes", "PrivateTmp=yes",
        "TimeoutStartSec=5s", ""])
    guard = HEADER + Path(__file__).with_name("nfs_mount.py").read_text()
    return {UNITS + "/" + MOUNT: mount, UNITS + "/" + AUTOMOUNT: automount,
            UNITS + "/" + READY: ready, GUARD: guard}


def plan_all(config):
    validate_config(config)
    nodes = selected_nodes(config)
    blockers = [node + ": verified LAN address missing; deployment remains deferred"
                for node in nodes if config["clients"][node] is None]
    return {"version": 1, "mode": "offline-three-client-preview" if len(nodes) == 3 else "offline-selected-client-preview", "config": config,
            "apply_ready": False, "blockers": blockers + ["Fresh local inspection on each Pi and explicit 2.4 approval required"],
            "clients": {node: {"address": config["clients"][node], "files": render_files(config, node)} for node in nodes},
            "package_if_missing_after_approval": PACKAGE_COMMAND,
            "activation_after_separate_approval": ["systemctl daemon-reload", "systemctl enable --now " + AUTOMOUNT],
            "fstab_policy": "Never written; existing mount and agent units unchanged",
            "runtime_enablement": "Blocked until authorization, job isolation and supervised staging are implemented"}


class ClientHost(Host):
    def facts(self):
        result = subprocess.run(["dpkg-query", "-W", "-f=${Status}", "nfs-common"], capture_output=True, text=True, timeout=5, env=QUERY_ENV)
        installed = result.returncode == 0 and result.stdout == "install ok installed"
        result = subprocess.run(["ip", "-j", "address", "show"], capture_output=True, text=True, timeout=5, env=QUERY_ENV)
        if result.returncode:
            raise SetupError("Local read-only interface query failed")
        interfaces = json.loads(result.stdout)
        mounts = parse_mountinfo(Path("/proc/self/mountinfo").read_text())
        return {"nfs_common_installed": installed, "mount_helper_available": shutil.which("mount.nfs", path=QUERY_ENV["PATH"]) is not None,
                "addresses": sorted(a["local"] for interface in interfaces for a in interface.get("addr_info", []) if a.get("family") == "inet"),
                "mounts": mounts}


def inspect_local(config, node, host=None):
    validate_config(config)
    if node not in selected_nodes(config):
        raise SetupError("Selected Pi is excluded from this deployment")
    host = host or ClientHost()
    facts = host.facts()
    mounts = facts["mounts"]
    blockers = [name + ": verified LAN address missing" for name in selected_nodes(config) if config["clients"][name] is None]
    if config["clients"][node] not in facts["addresses"]:
        blockers.append("Selected Pi LAN address is not assigned to this host; never apply on the controller")
    if not facts["nfs_common_installed"]:
        blockers.append("nfs-common missing; separate approved offline package provisioning required")
    elif not facts["mount_helper_available"]:
        blockers.append("mount.nfs missing despite installed nfs-common; repair requires review")
    files = render_files(config, node)
    observed = {"facts": facts, "files": {}, "directories": {}}
    for name in ("/srv", "/etc", UNITS, "/var/backups", "/usr/local", "/usr/local/libexec", LIBEXEC, BACKUPS):
        current = host.metadata(name, mounts)
        observed["directories"][name] = current
        if current and (not current["directory"] or current["uid"] != host.admin_uid or current["mode"] & 0o022):
            raise SetupError("Unprotected administrative directory: " + name)
        if current is None and name in ("/srv", "/etc", UNITS, "/var/backups", "/usr/local"):
            blockers.append("Required administrative parent missing: " + name)
    fstab = host.read("/etc/fstab", mounts)
    observed["fstab"] = fstab
    if fstab:
        if fstab["uid"] != host.admin_uid or fstab["mode"] & 0o022:
            raise SetupError("Unprotected fstab; never overwrite it")
        for line in fstab["text"].splitlines():
            if not line.strip() or line.lstrip().startswith("#"):
                continue
            fields = line.split()
            if len(fields) < 4:
                raise SetupError("Malformed existing fstab requires manual review")
            target = unescape(fields[1])
            if target == ROOT or target.startswith(ROOT + "/"):
                raise SetupError("Conflicting fstab input path; preserve it for manual review")
    # Never stat/traverse mounted input paths, including an autofs trigger.
    occupied = any(m["target"] == ROOT or m["target"].startswith(ROOT + "/") for m in mounts)
    if occupied:
        raise SetupError("Managed input path is already mounted; drain and review before configuration staging")
    for name, mode in ((ROOT, 0o700), (INPUTS, 0o700)):
        current = host.metadata(name, mounts)
        observed["directories"][name] = current
        if current and (not current["directory"] or current["uid"] != host.admin_uid or current["mode"] != mode):
            raise SetupError("Input path must be a protected admin-owned 0700 directory")
    marker = host.read(MARKER, mounts)
    expected_marker = {"version": 1, "node": node, "source": source(config)}
    observed["marker"] = marker
    if marker:
        if marker["uid"] != host.admin_uid or marker["mode"] != 0o600 or json.loads(marker["text"]) != expected_marker:
            raise SetupError("Client setup marker differs; never silently repurpose a mount root")
    elif observed["directories"][ROOT] and any(host.path(ROOT).iterdir()):
        raise SetupError("Refusing nonempty unmarked input root")
    if observed["directories"][INPUTS] and any(host.path(INPUTS).iterdir()):
        raise SetupError("Underlying input mountpoint must be empty; preserve its data")
    for unit in (MOUNT, AUTOMOUNT, READY):
        for base in (UNITS, "/run/systemd/system", "/usr/lib/systemd/system"):
            if host.metadata(base + "/" + unit + ".d", mounts) is not None:
                raise SetupError("Existing unit override requires manual review: " + unit)
            if base != UNITS and host.metadata(base + "/" + unit, mounts) is not None:
                raise SetupError("Existing runtime/vendor unit requires manual review: " + unit)
    for target in ("multi-user.target.wants", "remote-fs.target.wants", "local-fs.target.wants"):
        if host.metadata(UNITS + "/" + target + "/" + MOUNT, mounts) is not None:
            raise SetupError("Direct boot dependency on input mount requires manual review")
    for name in files:
        record = host.read(name, mounts)
        observed["files"][name] = record
        if record and (record["uid"] != host.admin_uid or record["mode"] != 0o644 or not record["text"].startswith(HEADER)):
            raise SetupError("Unowned/unsafe client configuration target: " + name)
    plan = {"version": 1, "mode": "local-read-only-plan", "node": node, "config": config,
            "files": files, "observed": observed, "blockers": blockers, "apply_ready": not blockers,
            "package_if_missing_after_approval": PACKAGE_COMMAND, "backup_before_changes": BACKUPS,
            "scope": "Stage only; never edit fstab, mount, enable/reload units or restart the legacy agent"}
    plan["plan_sha256"] = digest(plan)
    return plan


def _review(config, node, sha, host):
    if os.geteuid() != host.admin_uid:
        raise SetupError("Live client action requires the host administrator and separate approval")
    plan = inspect_local(config, node, host)
    if plan["plan_sha256"] != sha:
        raise SetupError("Local plan changed; review a fresh digest")
    return plan


def provision_package(config, node, sha, host=None):
    """Future explicitly approved offline provisioning; package scripts may affect services."""
    host = host or ClientHost()
    plan = _review(config, node, sha, host)
    if plan["observed"]["facts"]["nfs_common_installed"]:
        return {"status": "package_already_installed", "command_executed": False}
    if any(not blocker.startswith("nfs-common missing;") for blocker in plan["blockers"]):
        raise SetupError("Resolve non-package blockers before offline provisioning")
    result = subprocess.run(PACKAGE_COMMAND, check=True, env={"PATH": "/usr/sbin:/usr/bin:/sbin:/bin", "DEBIAN_FRONTEND": "noninteractive"})
    return {"status": "package_command_completed_review_new_plan", "returncode": result.returncode,
            "command_executed": True, "warning": "Package maintainer scripts may change services; unit staging/activation not performed"}


def stage_configuration(config, node, sha, host=None):
    host = host or ClientHost()
    plan = _review(config, node, sha, host)
    if plan["blockers"]:
        raise SetupError("Client configuration blocked: " + "; ".join(plan["blockers"]))
    state = plan["observed"]
    if state["marker"] and all(state["directories"].get(name) for name in (ROOT, INPUTS, LIBEXEC)) and all(state["files"][name] and state["files"][name]["text"] == text for name, text in plan["files"].items()):
        return {"status": "no_changes", "activated": False}
    fd = os.open(host.path(LOCK), os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_uid != host.admin_uid or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1:
            raise SetupError("Unprotected client setup lock")
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fresh = _review(config, node, sha, host)
        return _stage(fresh, host)
    finally:
        os.close(fd)


def _stage(plan, host):
    state = plan["observed"]
    mounts = state["facts"]["mounts"]
    backup_root = host.path(BACKUPS)
    backup_root.mkdir(mode=0o700, exist_ok=True)
    _sync(backup_root.parent)
    backup = backup_root / (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex)
    backup.mkdir(mode=0o700)
    journal = {"plan_sha256": plan["plan_sha256"], "node": plan["node"], "files": state["files"],
               "fstab_unchanged": state["fstab"],
               "installed_sha256": {name: hashlib.sha256(text.encode()).hexdigest() for name, text in plan["files"].items()},
               "restore_scope": "Only listed managed files after current hash checks; preserve data, fstab, unrelated units and backups"}
    _write_new(backup / "before.json", encoded(journal), 0o600, host.admin_uid, os.getegid())
    _sync(backup)
    _sync(backup_root)
    def directory(name, mode):
        if not host.path(name).exists():
            host.path(name).mkdir(mode=mode)
            os.chmod(host.path(name), mode)
            _sync(host.path(name).parent)
    directory(ROOT, 0o700)
    if state["marker"] is None:
        _write_new(host.path(MARKER), encoded({"version": 1, "node": plan["node"], "source": source(plan["config"])}), 0o600, host.admin_uid, os.getegid())
        _sync(host.path(ROOT))
    directory(INPUTS, 0o700)
    directory("/usr/local/libexec", 0o755)
    directory(LIBEXEC, 0o755)
    for name, text in plan["files"].items():
        if state["files"][name] and state["files"][name]["text"] == text:
            continue
        path = host.path(name)
        temporary = path.parent / (".ml-nfs-client-" + uuid.uuid4().hex)
        try:
            _write_new(temporary, text.encode(), 0o644, host.admin_uid, os.getegid())
            if host.read(name, mounts) != state["files"][name]:
                raise SetupError("Client config changed before publication; preserve it: " + name)
            if state["files"][name] is None:
                os.link(temporary, path, follow_symlinks=False)
            else:
                os.replace(temporary, path)
            _sync(path.parent)
        finally:
            temporary.unlink(missing_ok=True)
    return {"status": "configuration_staged_not_activated", "backup": str(backup), "activated": False}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=CONFIG_FILE)
    parser.add_argument("--node", choices=("pi01", "pi02", "pi03"))
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="Offline preview for selected Pis (all three by default)")
    mode.add_argument("--inspect-local", action="store_true", help="Read-only inspection on the selected Pi")
    mode.add_argument("--apply", action="store_true", help="Future approved root-only config staging, no activation")
    mode.add_argument("--install-missing-package", action="store_true", help="Future separately approved root-only offline package provisioning")
    parser.add_argument("--reviewed-plan-sha256")
    args = parser.parse_args(argv)
    try:
        config = load_config(args.config)
        local = args.inspect_local or args.apply or args.install_missing_package
        if local and not args.node:
            raise SetupError("Local inspection/actions require an explicit --node")
        if not local and args.node:
            raise SetupError("Offline preview covers the configured deployment nodes; omit --node")
        if args.apply or args.install_missing_package:
            if not args.reviewed_plan_sha256:
                raise SetupError("Live action requires a current reviewed digest and prior explicit approval")
            action = stage_configuration if args.apply else provision_package
            result = action(config, args.node, args.reviewed_plan_sha256)
        else:
            if args.reviewed_plan_sha256:
                raise SetupError("Review digest is only used for approved live actions")
            result = inspect_local(config, args.node) if local else plan_all(config)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 2 if result.get("blockers") else 0
    except (SetupError, OSError, ValueError, subprocess.SubprocessError) as error:
        parser.exit(2, "NFS client setup refused: " + str(error) + "\n")


if __name__ == "__main__":
    raise SystemExit(main())
