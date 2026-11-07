"""Fake clients and kernel mount metadata; no real mounts, SSH or package actions."""

from contextlib import redirect_stderr, redirect_stdout
from copy import deepcopy
import io
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from hybrid_storage import nfs_client as client
from hybrid_storage import nfs_mount as guard

BASE_MOUNTS = "1 0 8:1 / / rw,relatime - ext4 /dev/fake rw\n"
NFS_MOUNT = "20 1 0:50 / /srv/mlnfs/inputs ro,nosuid,nodev,noexec - nfs4 192.0.2.1:/srv/ml-shared/objects ro,vers=4.1,hard,proto=tcp,sec=sys,timeo=50,retrans=2\n"
AUTO_MOUNT = "19 1 0:49 / /srv/mlnfs/inputs rw,relatime - autofs systemd-1 rw,fd=42\n"


class FakeClient(client.ClientHost):
    def __init__(self, root, node):
        super().__init__(root, admin_uid=os.geteuid())
        self.observations = {"nfs_common_installed": True, "mount_helper_available": True,
                             "addresses": [{"pi01": "192.0.2.2", "pi02": "192.0.2.3", "pi03": "192.0.2.4"}[node]],
                             "mounts": guard.parse_mountinfo(BASE_MOUNTS)}

    def facts(self):
        return deepcopy(self.observations)


class ClientSetupTests(unittest.TestCase):
    def setUp(self):
        previous = os.umask(0o022)
        self.addCleanup(os.umask, previous)
        directory = tempfile.TemporaryDirectory(prefix="fake-nfs-clients-")
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.config = {"schema_version": 1, "server_address": "192.0.2.1",
                       "clients": {"pi01": "192.0.2.2", "pi02": "192.0.2.3", "pi03": "192.0.2.4"}}
        self.hosts = {}
        for node in self.config["clients"]:
            host = FakeClient(self.root / node, node)
            host.root.mkdir()
            for name in ("/etc/systemd/system", "/var/backups", "/srv", "/usr/local"):
                host.path(name).mkdir(parents=True, exist_ok=True)
            host.path("/etc/fstab").write_text("# Preserve old mount\n192.0.2.3:/legacy /mnt/old nfs defaults,_netdev 0 0\n")
            self.hosts[node] = host
        self.host = self.hosts["pi02"]

    def snapshot(self, host=None):
        host = host or self.host
        return {str(path.relative_to(host.root)): (path.read_bytes() if path.is_file() else None, stat.S_IMODE(path.lstat().st_mode))
                for path in host.root.rglob("*")}

    def stage(self, node="pi02"):
        host = self.hosts[node]
        plan = client.inspect_local(self.config, node, host)
        return client.stage_configuration(self.config, node, plan["plan_sha256"], host)

    def test_offline_default_covers_all_three_without_host_commands_or_changes(self):
        before = self.snapshot()
        with patch.object(client.ClientHost, "facts", side_effect=AssertionError("Offline preview must not inspect any host")), patch.object(client.subprocess, "run", side_effect=AssertionError("No commands")):
            plan = client.plan_all(self.config)
        self.assertEqual(set(plan["clients"]), {"pi01", "pi02", "pi03"})
        self.assertFalse(plan["apply_ready"])
        self.assertEqual(before, self.snapshot())

    def test_schema_literal_hosts_unknown_identity_and_duplicates(self):
        for change in ({"schema_version": True}, {"schema_version": 2}, {"mount_path": "/home"}):
            with self.subTest(change=change), self.assertRaises(client.SetupError):
                client.validate_config({**self.config, **change})
        for value in ("*", "192.0.2.0/24", "pi02", "192.0.2.2\nWhat=/", "127.0.0.1", "192.0.2.1", "192.0.2.4"):
            config = deepcopy(self.config); config["clients"]["pi01"] = value
            with self.subTest(value=value), self.assertRaises(client.SetupError):
                client.validate_config(config)
        path = self.root / "duplicate.json"
        path.write_text('{"schema_version":1,"schema_version":1}')
        with self.assertRaises(client.SetupError): client.load_config(path)

    def test_explicit_two_node_preview_and_staging_exclude_pi01_before_inspection(self):
        self.config["clients"]["pi01"] = None
        self.config["deployment_nodes"] = ["pi02", "pi03"]
        plan = client.plan_all(self.config)
        self.assertEqual(set(plan["clients"]), {"pi02", "pi03"})
        self.assertFalse(any("pi01" in item for item in plan["blockers"]))
        self.assertTrue(client.inspect_local(self.config, "pi02", self.host)["apply_ready"])
        with patch.object(self.host, "facts", side_effect=AssertionError("Excluded node must not even be inspected")):
            with self.assertRaises(client.SetupError): client.inspect_local(self.config, "pi01", self.host)
        with self.assertRaises(client.SetupError): client.render_files(self.config, "pi01")
        self.stage()
        self.assertEqual(self.stage(), {"status": "no_changes", "activated": False})
        self.config["clients"]["pi02"] = None
        self.assertFalse(client.inspect_local(self.config, "pi02", self.host)["apply_ready"])
        with self.assertRaises(client.SetupError): self.stage()

    def test_invalid_deployment_selection_fails_closed_and_default_keeps_three(self):
        self.assertEqual(set(client.plan_all(self.config)["clients"]), {"pi01", "pi02", "pi03"})
        for selection in ([], ["pi02", "pi02"], ["pi04"], "pi02", None, [True]):
            with self.subTest(selection=selection), self.assertRaises(client.SetupError):
                client.validate_config({**self.config, "deployment_nodes": selection})

    def test_units_keep_boot_and_legacy_agent_independent_of_network_mount(self):
        for node in self.hosts:
            files = client.render_files(self.config, node)
            mount, automount, ready = [files[client.UNITS + "/" + name] for name in (client.MOUNT, client.AUTOMOUNT, client.READY)]
            self.assertNotIn("[Install]", mount)
            self.assertIn("TimeoutSec=15s", mount)
            self.assertNotIn("soft,", mount)
            self.assertIn("hard,vers=4.1,proto=tcp", mount)
            self.assertIn("nofail", mount)
            self.assertIn("WantedBy=multi-user.target", automount)
            self.assertIn("DirectoryMode=0700", automount)
            self.assertNotIn("network-online.target", automount)
            self.assertNotIn("Requires=", automount)
            self.assertIn("Requires=" + client.MOUNT, ready)
            self.assertIn("After=" + client.MOUNT, ready)
            self.assertNotIn("RemainAfterExit=yes", ready)
            self.assertNotIn("[Install]", ready)
            self.assertNotIn("dl-node-agent", "".join(files.values()))
            self.assertNotIn("/etc/fstab", files)

    def test_all_three_stage_idempotently_preserving_fstab_and_without_activation(self):
        with patch.object(client.subprocess, "run", side_effect=AssertionError("No commands while staging fake config")):
            for node, host in self.hosts.items():
                with self.subTest(node=node):
                    original = host.path("/etc/fstab").read_bytes()
                    result = self.stage(node)
                    self.assertFalse(result["activated"])
                    journal = json.loads((Path(result["backup"]) / "before.json").read_text())
                    self.assertEqual(journal["node"], node)
                    self.assertTrue(all(record is None for record in journal["files"].values()))
                    self.assertEqual(journal["fstab_unchanged"]["text"].encode(), original)
                    self.assertEqual(host.path("/etc/fstab").read_bytes(), original)
                    self.assertEqual(stat.S_IMODE(host.path(client.ROOT).stat().st_mode), 0o700)
                    self.assertEqual(stat.S_IMODE(host.path(guard.INPUTS).stat().st_mode), 0o700)
                    before = self.snapshot(host)
                    self.assertEqual(self.stage(node), {"status": "no_changes", "activated": False})
                    self.assertEqual(before, self.snapshot(host))

    def test_missing_pi_or_wrong_local_node_or_package_blocks_staging(self):
        for variant in ("pi01", "address", "package", "helper"):
            with self.subTest(variant=variant):
                config = deepcopy(self.config)
                saved = deepcopy(self.host.observations)
                if variant == "pi01": config["clients"]["pi01"] = None
                if variant == "address": self.host.observations["addresses"] = ["192.0.2.1"]
                if variant == "package": self.host.observations["nfs_common_installed"] = False
                if variant == "helper": self.host.observations["mount_helper_available"] = False
                before = self.snapshot()
                plan = client.inspect_local(config, "pi02", self.host)
                self.assertFalse(plan["apply_ready"])
                with self.assertRaises(client.SetupError): client.stage_configuration(config, "pi02", plan["plan_sha256"], self.host)
                self.assertEqual(before, self.snapshot())
                self.host.observations = saved

    def test_existing_fstab_collision_is_not_rewritten(self):
        self.host.path("/etc/fstab").write_text("192.0.2.8:/old /srv/mlnfs/inputs nfs defaults 0 0\n")
        before = self.snapshot()
        with self.assertRaises(client.SetupError): client.inspect_local(self.config, "pi02", self.host)
        self.assertEqual(before, self.snapshot())

    def test_nonempty_unmarked_root_and_mountpoint_data_are_preserved(self):
        self.host.path(client.ROOT).mkdir(mode=0o700)
        secret = self.host.path(client.ROOT) / "private-file"
        secret.write_text("preserve")
        before = self.snapshot()
        with self.assertRaises(client.SetupError): client.inspect_local(self.config, "pi02", self.host)
        self.assertEqual(before, self.snapshot())
        secret.unlink()
        self.stage()
        secret = self.host.path(guard.INPUTS) / "not-mounted-data"
        secret.write_text("preserve")
        before = self.snapshot()
        with self.assertRaises(client.SetupError): client.inspect_local(self.config, "pi02", self.host)
        self.assertEqual(before, self.snapshot())

    def test_symlinks_world_writable_parents_and_unowned_units_fail_closed(self):
        outside = self.root / "outside"; outside.mkdir()
        self.host.path(client.ROOT).symlink_to(outside, target_is_directory=True)
        with self.assertRaises(client.SetupError): client.inspect_local(self.config, "pi02", self.host)
        self.host.path(client.ROOT).unlink()
        self.host.path("/var/backups").chmod(0o777)
        with self.assertRaises(client.SetupError): client.inspect_local(self.config, "pi02", self.host)
        self.host.path("/var/backups").chmod(0o755)
        self.host.path(client.UNITS + "/" + client.MOUNT).write_text("# another owner's unit\n")
        with self.assertRaises(client.SetupError): client.inspect_local(self.config, "pi02", self.host)

    def test_existing_unit_override_prevents_shadowed_policy(self):
        self.host.path(client.UNITS + "/" + client.MOUNT + ".d").mkdir()
        with self.assertRaises(client.SetupError): client.inspect_local(self.config, "pi02", self.host)

    def test_direct_mount_boot_dependency_is_rejected(self):
        directory = self.host.path(client.UNITS + "/remote-fs.target.wants")
        directory.mkdir()
        (directory / client.MOUNT).write_text("unexpected direct boot mount dependency")
        with self.assertRaisesRegex(client.SetupError, "Direct boot dependency"):
            client.inspect_local(self.config, "pi02", self.host)

    def test_active_autofs_or_nfs_paths_are_not_traversed_during_setup(self):
        for row in (AUTO_MOUNT, NFS_MOUNT):
            self.host.observations["mounts"] = guard.parse_mountinfo(BASE_MOUNTS + row)
            with self.assertRaisesRegex(client.SetupError, "already mounted"):
                client.inspect_local(self.config, "pi02", self.host)

    def test_backup_failure_prevents_all_config_and_mount_directory_changes(self):
        with patch.object(client, "_write_new", side_effect=OSError("simulated backup disk full")):
            with self.assertRaises(OSError): self.stage()
        self.assertFalse(self.host.path(client.ROOT).exists())
        for name in client.render_files(self.config, "pi02"):
            self.assertFalse(self.host.path(name).exists())

    def test_partial_multifile_staging_resumes_with_backup_and_preserves_prior_files(self):
        actual_write = client._write_new
        count = 0
        def interrupted(path, *args):
            nonlocal count
            if path.name.startswith(".ml-nfs-client-"):
                count += 1
                if count == 2:
                    raise OSError("interrupted between unit files")
            return actual_write(path, *args)
        with patch.object(client, "_write_new", side_effect=interrupted):
            with self.assertRaises(OSError): self.stage()
        self.assertTrue(self.host.path(client.MARKER).is_file())
        self.assertTrue(self.host.path(client.UNITS + "/" + client.MOUNT).is_file())
        self.assertEqual(self.stage()["status"], "configuration_staged_not_activated")
        self.assertEqual(len(list(self.host.path(client.BACKUPS).iterdir())), 2)

    def test_concurrent_unit_edit_is_never_overwritten(self):
        actual_write = client._write_new
        destination = self.host.path(client.UNITS + "/" + client.MOUNT)
        def competing_write(path, *args):
            if path.name.startswith(".ml-nfs-client-"):
                destination.write_text("# concurrent admin edit\n")
            return actual_write(path, *args)
        with patch.object(client, "_write_new", side_effect=competing_write):
            with self.assertRaisesRegex(client.SetupError, "changed before publication"): self.stage()
        self.assertEqual(destination.read_text(), "# concurrent admin edit\n")

    def test_stale_digest_and_non_admin_prevent_changes(self):
        plan = client.inspect_local(self.config, "pi02", self.host)
        self.host.path("/etc/fstab").write_text("# edited after review\n")
        before = self.snapshot()
        with self.assertRaises(client.SetupError): client.stage_configuration(self.config, "pi02", plan["plan_sha256"], self.host)
        with patch.object(client.os, "geteuid", return_value=self.host.admin_uid + 1):
            with self.assertRaises(client.SetupError): self.stage()
        self.assertEqual(before, self.snapshot())

    def test_package_step_is_conditional_offline_and_does_not_stage_units(self):
        plan = client.inspect_local(self.config, "pi02", self.host)
        with patch.object(client.subprocess, "run", side_effect=AssertionError("Already installed means no command")):
            self.assertFalse(client.provision_package(self.config, "pi02", plan["plan_sha256"], self.host)["command_executed"])
        self.host.observations["nfs_common_installed"] = False
        plan = client.inspect_local(self.config, "pi02", self.host)
        with patch.object(client.subprocess, "run", return_value=subprocess.CompletedProcess(client.PACKAGE_COMMAND, 0)) as run:
            self.assertTrue(client.provision_package(self.config, "pi02", plan["plan_sha256"], self.host)["command_executed"])
        self.assertEqual(run.call_args.args[0], client.PACKAGE_COMMAND)
        self.assertIn("--no-download", run.call_args.args[0])
        self.assertFalse(self.host.path(client.ROOT).exists())
        config = deepcopy(self.config); config["clients"]["pi01"] = None
        plan = client.inspect_local(config, "pi02", self.host)
        with patch.object(client.subprocess, "run", side_effect=AssertionError("Unknown node must block package action")):
            with self.assertRaises(client.SetupError): client.provision_package(config, "pi02", plan["plan_sha256"], self.host)

    def test_package_failure_propagates_and_never_stages_units(self):
        self.host.observations["nfs_common_installed"] = False
        plan = client.inspect_local(self.config, "pi02", self.host)
        with patch.object(client.subprocess, "run", side_effect=subprocess.CalledProcessError(100, client.PACKAGE_COMMAND)):
            with self.assertRaises(subprocess.CalledProcessError): client.provision_package(self.config, "pi02", plan["plan_sha256"], self.host)
        self.assertFalse(self.host.path(client.ROOT).exists())

    def test_cli_preview_is_default_and_local_actions_require_node_and_digest(self):
        path = self.root / "proposal.json"; path.write_text(json.dumps(self.config))
        output = io.StringIO()
        with redirect_stdout(output): self.assertEqual(client.main(["--config", str(path)]), 2)
        self.assertEqual(set(json.loads(output.getvalue())["clients"]), set(self.hosts))
        for args in (["--inspect-local"], ["--node", "pi02"], ["--node", "pi02", "--apply"], ["--node", "pi02", "--install-missing-package"]):
            with self.subTest(args=args), redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
                client.main(["--config", str(path), *args])
            self.assertEqual(error.exception.code, 2)


class MountAdmissionTests(unittest.TestCase):
    def test_readonly_vfs_with_rw_nfs_superblock_is_valid_but_vfs_flags_remain_required(self):
        row = NFS_MOUNT.replace("objects ro,vers=", "objects rw,vers=")
        self.assertTrue(guard.require_input_mount("192.0.2.1:/srv/ml-shared/objects", BASE_MOUNTS + row)["ready"])
        for flag in ("ro", "nosuid", "nodev", "noexec"):
            unsafe = row.replace(" ro,nosuid,nodev,noexec ", " " + ",".join(value for value in ["ro", "nosuid", "nodev", "noexec"] if value != flag) + " ")
            unsafe = unsafe.replace("rw,vers=", "rw," + flag + ",vers=")
            with self.subTest(flag=flag), self.assertRaises(guard.MountNotReady):
                guard.require_input_mount("192.0.2.1:/srv/ml-shared/objects", BASE_MOUNTS + unsafe)

    def test_valid_mount_and_automount_overlay_admit_exact_source(self):
        for text in (BASE_MOUNTS + NFS_MOUNT, BASE_MOUNTS + AUTO_MOUNT + NFS_MOUNT):
            result = guard.require_input_mount("192.0.2.1:/srv/ml-shared/objects", text)
            self.assertTrue(result["ready"])
            self.assertEqual(result["mount_id"], 20)

    def test_absent_empty_local_directory_and_autofs_only_never_admit(self):
        variants = (BASE_MOUNTS, BASE_MOUNTS + AUTO_MOUNT,
                    BASE_MOUNTS + NFS_MOUNT.replace("nfs4", "ext4"),
                    BASE_MOUNTS + NFS_MOUNT.replace("/srv/mlnfs/inputs", "/mnt/elsewhere"))
        with patch.object(Path, "stat", side_effect=AssertionError("Never inspect data or trigger an automount")):
            for text in variants:
                with self.subTest(text=text), self.assertRaises(guard.MountNotReady):
                    guard.require_input_mount("192.0.2.1:/srv/ml-shared/objects", text)

    def test_wrong_source_bind_rw_soft_missing_security_or_wrong_protocol_are_rejected(self):
        variants = [NFS_MOUNT.replace("192.0.2.1:", "192.0.2.8:"), NFS_MOUNT.replace("/srv/ml-shared/objects", "/other"),
                    NFS_MOUNT.replace("0:50 / ", "0:50 /subdir "), NFS_MOUNT.replace("ro,nosuid", "rw,nosuid"),
                    NFS_MOUNT.replace("hard", "soft"), NFS_MOUNT.replace("hard", "softerr"),
                    NFS_MOUNT.replace("noexec", "exec"), NFS_MOUNT.replace("nosuid,", ""),
                    NFS_MOUNT.replace("nodev,", ""), NFS_MOUNT.replace("vers=4.1", "vers=3"),
                    NFS_MOUNT.replace("sec=sys", "sec=none"), NFS_MOUNT.replace("proto=tcp", "proto=udp")]
        for row in variants:
            with self.subTest(row=row), self.assertRaises(guard.MountNotReady):
                guard.require_input_mount("192.0.2.1:/srv/ml-shared/objects", BASE_MOUNTS + row)

    def test_shadowed_duplicate_and_nested_mounts_are_rejected(self):
        extra = "21 20 8:1 / /srv/mlnfs/inputs/private ro - ext4 /dev/fake ro\n"
        for text in (BASE_MOUNTS + NFS_MOUNT * 2, BASE_MOUNTS + NFS_MOUNT + extra,
                     BASE_MOUNTS + NFS_MOUNT + NFS_MOUNT.replace("nfs4", "tmpfs")):
            with self.subTest(text=text), self.assertRaises(guard.MountNotReady):
                guard.require_input_mount("192.0.2.1:/srv/ml-shared/objects", text)

    def test_ancestor_overmount_cannot_make_a_hidden_nfs_mount_count_as_ready(self):
        hidden = "21 1 0:51 / /srv/mlnfs rw - tmpfs tmpfs rw\n"
        with self.assertRaisesRegex(guard.MountNotReady, "hidden by an ancestor"):
            guard.require_input_mount("192.0.2.1:/srv/ml-shared/objects", BASE_MOUNTS + NFS_MOUNT + hidden)
        # A legitimate separate /srv mount must instead be in the NFS ancestry.
        parent = "21 1 8:2 / /srv rw - ext4 /dev/fake2 rw\n"
        child = NFS_MOUNT.replace("20 1 ", "20 21 ")
        self.assertTrue(guard.require_input_mount("192.0.2.1:/srv/ml-shared/objects", BASE_MOUNTS + parent + child)["ready"])

    def test_malformed_metadata_and_unsafe_source_fail_closed(self):
        with self.assertRaises(guard.MountNotReady): guard.parse_mountinfo("bad mountinfo")
        for source in ("controller:/srv/ml-shared/objects", "192.0.2.1:/home", "127.0.0.1:/srv/ml-shared/objects"):
            with self.subTest(source=source), self.assertRaises(guard.MountNotReady):
                guard.require_input_mount(source, BASE_MOUNTS + NFS_MOUNT)
        parsed = guard.parse_mountinfo(BASE_MOUNTS.replace("/dev/fake", "/dev/with\\040space"))
        self.assertEqual(parsed[0]["source"], "/dev/with space")

    def test_guard_cli_exit_codes_use_metadata_only(self):
        for text, expected in ((BASE_MOUNTS, 2), (BASE_MOUNTS + NFS_MOUNT, 0)):
            with patch.object(Path, "read_text", return_value=text), redirect_stdout(io.StringIO()) as output:
                self.assertEqual(guard.main(["--source", "192.0.2.1:/srv/ml-shared/objects"]), expected)
            self.assertEqual(json.loads(output.getvalue())["ready"], expected == 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
