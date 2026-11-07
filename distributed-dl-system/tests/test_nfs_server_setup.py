"""Fake-host server setup checks: no sudo, actual exports, services or Pi access."""

from copy import deepcopy
from contextlib import redirect_stderr, redirect_stdout
import io
import json
import os
from pathlib import Path
import stat
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from hybrid_storage import nfs_server as nfs


class FakeHost(nfs.Host):
    def __init__(self, root):
        super().__init__(root, admin_uid=os.geteuid())
        self.observations = {"packages": {"nfs-kernel-server": True, "nfs-common": True},
                             "addresses": ["192.0.2.1"],
                             "mounts": [{"target": "/", "root": "/", "device": "8:1", "fstype": "ext4"}]}

    def facts(self):
        return deepcopy(self.observations)


class ServerSetupTests(unittest.TestCase):
    def setUp(self):
        previous_umask = os.umask(0o022)
        self.addCleanup(os.umask, previous_umask)
        temporary = tempfile.TemporaryDirectory(prefix="fake-nfs-server-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.host = FakeHost(self.root)
        for name in ("etc", "srv", "var/backups", "var/lib/nfs"):
            (self.root / name).mkdir(parents=True, exist_ok=True)
        # Documentation-only address block; these are not claimed Pi addresses.
        self.config = {"schema_version": 1, "server_address": "192.0.2.1",
                       "clients": {"pi01": "192.0.2.2", "pi02": "192.0.2.3", "pi03": "192.0.2.4"},
                       "publisher_uid": os.geteuid() or 1001, "publisher_gid": os.getegid() or 1001,
                       "layout": "subdirectory"}

    def snapshot(self):
        return {str(p.relative_to(self.root)): (p.read_bytes() if p.is_file() else None, stat.S_IMODE(p.lstat().st_mode))
                for p in self.root.rglob("*")}

    def stage(self):
        plan = nfs.build_plan(self.config, self.host)
        return nfs.stage_configuration(self.config, plan["plan_sha256"], self.host)

    def test_dry_run_is_read_only_and_renders_only_literal_clients(self):
        before = self.snapshot()
        plan = nfs.build_plan(self.config, self.host)
        self.assertTrue(plan["apply_ready"])
        self.assertEqual(nfs.check_exports(plan["desired_exports"], managed=True), ["192.0.2.2", "192.0.2.3", "192.0.2.4"])
        self.assertIn("ro,sync,root_squash,subtree_check,secure,sec=sys", plan["desired_exports"])
        self.assertNotIn("no_subtree_check", plan["desired_exports"])
        self.assertEqual(self.snapshot(), before)

    def test_missing_pi01_publisher_packages_or_server_address_prevent_staging(self):
        variants = []
        config = deepcopy(self.config); config["clients"]["pi01"] = None
        variants.append((config, deepcopy(self.host.observations)))
        config = deepcopy(self.config); config["publisher_uid"] = None; config["publisher_gid"] = None
        variants.append((config, deepcopy(self.host.observations)))
        facts = deepcopy(self.host.observations); facts["packages"]["nfs-kernel-server"] = False
        variants.append((deepcopy(self.config), facts))
        facts = deepcopy(self.host.observations); facts["addresses"] = []
        variants.append((deepcopy(self.config), facts))
        for config, facts in variants:
            with self.subTest(config=config, facts=facts):
                self.host.observations = facts
                before = self.snapshot()
                plan = nfs.build_plan(config, self.host)
                self.assertFalse(plan["apply_ready"])
                with self.assertRaises(nfs.SetupError):
                    nfs.stage_configuration(config, plan["plan_sha256"], self.host)
                self.assertEqual(self.snapshot(), before)

    def test_wildcards_subnets_hostnames_injection_and_duplicate_clients_are_rejected(self):
        for value in ("*", "192.0.2.0/24", "pi01", "192.0.2.2(ro)", "192.0.2.2\n/ rw", "127.0.0.1", "0.0.0.0", "224.0.0.1"):
            config = deepcopy(self.config); config["clients"]["pi01"] = value
            with self.subTest(value=value), self.assertRaises(nfs.SetupError):
                nfs.validate_config(config)
        config = deepcopy(self.config); config["clients"]["pi01"] = config["clients"]["pi02"]
        with self.assertRaises(nfs.SetupError): nfs.validate_config(config)

    def test_explicit_two_node_selection_stages_without_adding_deferred_pi01(self):
        self.config["clients"]["pi01"] = None
        self.config["deployment_nodes"] = ["pi02", "pi03"]
        plan = nfs.build_plan(self.config, self.host)
        self.assertTrue(plan["apply_ready"])
        self.assertEqual(nfs.check_exports(plan["desired_exports"], managed=True), ["192.0.2.3", "192.0.2.4"])
        self.stage()
        self.assertEqual(self.stage(), {"status": "no_changes", "activated": False})
        self.config["clients"]["pi02"] = None
        self.assertFalse(nfs.build_plan(self.config, self.host)["apply_ready"])
        with self.assertRaises(nfs.SetupError): self.stage()

    def test_excluded_known_client_is_not_exported_or_accepted_as_active(self):
        self.config["deployment_nodes"] = ["pi02", "pi03"]
        self.assertNotIn("192.0.2.2", nfs.render_exports(self.config))
        stale = nfs.HEADER + nfs.OBJECTS + " 192.0.2.2(" + nfs.options("subdirectory") + ")\n"
        with self.assertRaises(nfs.SetupError): nfs.check_active_table(stale, self.config)

    def test_deployment_selection_rejects_empty_duplicate_unknown_and_coerced_nodes(self):
        for selection in ([], ["pi02", "pi02"], ["pi04"], "pi02", None, [True]):
            with self.subTest(selection=selection), self.assertRaises(nfs.SetupError):
                nfs.validate_config({**self.config, "deployment_nodes": selection})

    def test_schema_keys_types_and_duplicate_json_fields_are_rejected(self):
        for change in ({"schema_version": True}, {"schema_version": 2}, {"publisher_uid": 0},
                       {"publisher_gid": True}, {"layout": "automatic"}, {"export_path": "/home"}):
            with self.subTest(change=change), self.assertRaises(nfs.SetupError):
                nfs.validate_config({**self.config, **change})
        path = self.root / "duplicate.json"
        path.write_text('{"schema_version":1,"schema_version":1}')
        with self.assertRaises(nfs.SetupError): nfs.load_config(path)

    def test_conservative_exports_syntax_and_policy_reject_unsafe_rows(self):
        for row in ("/ 192.0.2.2(ro)", "/home 192.0.2.2(ro)", "/home/student/jobs.db 192.0.2.2(ro)",
                    nfs.OBJECTS + " *(ro,sync,root_squash)", nfs.OBJECTS + " 192.0.2.2 (ro,sync,root_squash)",
                    nfs.OBJECTS + " 192.0.2.2(rw,sync,no_root_squash)", nfs.OBJECTS + " 192.0.2.2(ro,sync,root_squash,subtree_check,crossmnt)"):
            with self.subTest(row=row), self.assertRaises(nfs.SetupError):
                nfs.check_exports(nfs.HEADER + row + "\n", managed=True)
        self.assertEqual(nfs.check_exports("# Existing comments\n\n"), [])

    def test_unrelated_exports_and_unowned_managed_filename_are_preserved(self):
        for path, text in (("/etc/exports", "/somewhere *(rw)\n"),
                           (nfs.EXPORTS, "# another administrator owns this filename\n")):
            actual = self.host.path(path); actual.parent.mkdir(exist_ok=True)
            actual.write_text(text)
            before = self.snapshot()
            with self.assertRaises(nfs.SetupError): nfs.build_plan(self.config, self.host)
            self.assertEqual(self.snapshot(), before)
            actual.unlink()

    def test_symlink_and_unprotected_config_paths_fail_closed(self):
        outside = self.root / "outside"
        outside.mkdir()
        self.host.path("/etc/exports.d").symlink_to(outside, target_is_directory=True)
        with self.assertRaises(nfs.SetupError): nfs.build_plan(self.config, self.host)
        self.host.path("/etc/exports.d").unlink()
        self.host.path("/etc/exports").write_text("# comments\n")
        self.host.path("/etc/exports").chmod(0o666)
        with self.assertRaises(nfs.SetupError): nfs.build_plan(self.config, self.host)

    def test_nonempty_unmarked_root_is_never_adopted_or_exposed(self):
        self.host.path(nfs.SHARED).mkdir()
        secret = self.host.path(nfs.SHARED) / "private-user-data"
        secret.write_text("preserve me")
        before = self.snapshot()
        with self.assertRaises(nfs.SetupError): nfs.build_plan(self.config, self.host)
        self.assertEqual(self.snapshot(), before)

    def test_unsafe_backup_parent_is_rejected_during_read_only_plan(self):
        self.host.path("/var/backups").chmod(0o777)
        before = self.snapshot()
        with self.assertRaises(nfs.SetupError):
            nfs.build_plan(self.config, self.host)
        self.assertEqual(self.snapshot(), before)
        self.assertFalse(self.host.path("/etc/.distributed-ml-nfs.lock").exists())

    def test_stage_backs_up_before_changes_and_is_idempotent_without_activation(self):
        original = b"# Original exports config; keep exactly\n"
        self.host.path("/etc/exports").write_bytes(original)
        with patch.object(nfs.subprocess, "run", side_effect=AssertionError("No service/exportfs/install execution")):
            result = self.stage()
            self.assertEqual(result["status"], "configuration_staged_not_activated")
            self.assertFalse(result["activated"])
            backup = json.loads((Path(result["backup"]) / "before.json").read_text())
            self.assertEqual(backup["files"]["/etc/exports"]["text"].encode(), original)
            self.assertIsNone(backup["files"][nfs.EXPORTS])
            self.assertEqual(self.host.path("/etc/exports").read_bytes(), original)
            self.assertEqual(self.host.path(nfs.EXPORTS).read_text(), nfs.render_exports(self.config))
            self.assertEqual(stat.S_IMODE(self.host.path(nfs.STAGING).stat().st_mode), 0o700)
            self.assertEqual(stat.S_IMODE((Path(result["backup"]) / "before.json").stat().st_mode), 0o600)
            before = self.snapshot()
            self.assertEqual(self.stage(), {"status": "no_changes", "activated": False})
            self.assertEqual(self.snapshot(), before)

    def test_changed_allowlist_has_new_backup_and_keeps_existing_inputs(self):
        self.stage()
        artifact = self.host.path(nfs.OBJECTS) / "already-published"
        artifact.write_bytes(b"immutable data")
        artifact.chmod(0o444)
        old_exports = self.host.path(nfs.EXPORTS).read_text()
        self.config["clients"]["pi03"] = "192.0.2.5"
        result = self.stage()
        backup = json.loads((Path(result["backup"]) / "before.json").read_text())
        self.assertEqual(backup["files"][nfs.EXPORTS]["text"], old_exports)
        self.assertEqual(artifact.read_bytes(), b"immutable data")
        self.assertEqual(stat.S_IMODE(artifact.stat().st_mode), 0o444)

    def test_review_digest_and_admin_gate_prevent_unreviewed_changes(self):
        plan = nfs.build_plan(self.config, self.host)
        self.host.path("/etc/exports").write_text("# changed after review\n")
        before = self.snapshot()
        with self.assertRaises(nfs.SetupError):
            nfs.stage_configuration(self.config, plan["plan_sha256"], self.host)
        self.assertEqual(self.snapshot(), before)
        with patch.object(nfs.os, "geteuid", return_value=self.host.admin_uid + 1):
            with self.assertRaises(nfs.SetupError): self.stage()
        self.assertEqual(self.snapshot(), before)

    def test_backup_failure_prevents_config_and_input_directory_creation(self):
        with patch.object(nfs, "_write_new", side_effect=OSError("simulated full backup disk")):
            with self.assertRaises(OSError): self.stage()
        self.assertFalse(self.host.path(nfs.SHARED).exists())
        self.assertFalse(self.host.path(nfs.EXPORTS).exists())

    def test_config_changed_during_staging_is_not_overwritten(self):
        actual_mkstemp = nfs.tempfile.mkstemp

        def competing_admin(*args, **kwargs):
            self.host.path(nfs.EXPORTS).write_text("# concurrent admin change\n")
            return actual_mkstemp(*args, **kwargs)

        with patch.object(nfs.tempfile, "mkstemp", side_effect=competing_admin):
            with self.assertRaisesRegex(nfs.SetupError, "changed before publication"):
                self.stage()
        self.assertEqual(self.host.path(nfs.EXPORTS).read_text(), "# concurrent admin change\n")

    def test_interrupted_marked_setup_can_resume_without_deleting_data(self):
        actual_mkdir = Path.mkdir

        def interrupted(path, *args, **kwargs):
            if path == self.host.path(nfs.OBJECTS):
                raise OSError("interrupted before object directory creation")
            return actual_mkdir(path, *args, **kwargs)

        with patch.object(Path, "mkdir", interrupted):
            with self.assertRaises(OSError): self.stage()
        self.assertTrue(self.host.path(nfs.MARKER).is_file())
        self.assertFalse(self.host.path(nfs.EXPORTS).exists())
        self.assertEqual(self.stage()["status"], "configuration_staged_not_activated")

    def test_no_subtree_requires_verified_whole_dedicated_filesystem(self):
        self.stage()
        self.config["layout"] = "dedicated-filesystem"
        for root, device in (("/", "8:1"), ("/bind-subdirectory", "8:2")):
            self.host.observations["mounts"] = self.host.observations["mounts"][:1] + [{"target": nfs.OBJECTS, "root": root, "device": device, "fstype": "ext4"}]
            self.assertFalse(nfs.build_plan(self.config, self.host)["apply_ready"])
        self.host.observations["mounts"][-1].update(root="/", device="8:2")
        plan = nfs.build_plan(self.config, self.host)
        self.assertTrue(plan["apply_ready"])
        self.assertIn("no_subtree_check,mountpoint", plan["desired_exports"])

    def test_remote_or_automount_layout_is_not_traversed(self):
        self.host.observations["mounts"].append({"target": "/srv", "root": "/", "device": "0:4", "fstype": "autofs"})
        with self.assertRaises(nfs.SetupError): nfs.build_plan(self.config, self.host)

    def test_existing_active_exports_must_be_only_our_exact_safe_scope(self):
        safe = nfs.render_exports(self.config).splitlines()[1] + "\n"
        nfs.check_active_table(safe, self.config)
        for text in ("/home *(rw)\n", safe.replace("root_squash", "no_root_squash"), safe.replace("ro,", "rw,"), safe.replace("192.0.2.2", "192.0.2.99")):
            with self.subTest(text=text), self.assertRaises(nfs.SetupError):
                nfs.check_active_table(text, self.config)
        self.host.path("/var/lib/nfs/etab").write_text(safe)
        self.stage()
        self.assertEqual(self.stage()["status"], "no_changes")

    def test_cli_default_is_dry_run_and_apply_needs_explicit_review_digest(self):
        path = self.root / "proposal.json"
        config = deepcopy(self.config); config["clients"]["pi01"] = None
        path.write_text(json.dumps(config))
        before = self.snapshot()
        output = io.StringIO()
        with patch.object(nfs, "Host", return_value=self.host), redirect_stdout(output):
            self.assertEqual(nfs.main(["--config", str(path)]), 2)
        self.assertEqual(json.loads(output.getvalue())["mode"], "dry-run")
        explicit = io.StringIO()
        with patch.object(nfs, "Host", return_value=self.host), redirect_stdout(explicit):
            self.assertEqual(nfs.main(["--config", str(path), "--dry-run"]), 2)
        self.assertEqual(json.loads(explicit.getvalue()), json.loads(output.getvalue()))
        self.assertEqual(self.snapshot(), before)
        with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as raised:
            nfs.main(["--config", str(path), "--apply"])
        self.assertEqual(raised.exception.code, 2)
        self.assertEqual(self.snapshot(), before)


if __name__ == "__main__":
    unittest.main(verbosity=2)
