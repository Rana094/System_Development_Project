"""Strict flags and default-path compatibility using only isolated controller state."""

from contextlib import redirect_stdout
from dataclasses import asdict, FrozenInstanceError
import errno
import io
import itertools
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from hybrid_storage.config import (
    load_config, main, MAX_CONFIG_BYTES, require_legacy_runtime,
    StorageConfig, StorageConfigError, validate_config,
)

OFF = {"storage_backend": "legacy", "worker_cache_enabled": False,
       "storage_aware_staging": False, "scheduler_policy": "resource"}
NFS = {**OFF, "storage_backend": "nfs", "storage_aware_staging": True}


class ConfigTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="storage-config-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.path = self.root / "storage.json"

    def write(self, document):
        self.path.write_text(json.dumps(document))
        return self.path

    def test_missing_empty_partial_and_explicit_off_are_identical(self):
        self.assertEqual(load_config(self.path), StorageConfig())
        for document in ({}, OFF, {"storage_backend": "legacy"}, {"worker_cache_enabled": False},
                         {"storage_aware_staging": False}, {"scheduler_policy": "resource"}):
            with self.subTest(document=document):
                self.assertEqual(load_config(self.write(document)), StorageConfig())
                self.assertEqual(asdict(require_legacy_runtime(validate_config(document))), OFF)
        self.assertEqual(json.loads((ROOT / "config/storage.json").read_text()), OFF)

    def test_settings_are_immutable_and_direct_construction_validates(self):
        settings = StorageConfig()
        with self.assertRaises(FrozenInstanceError):
            settings.worker_cache_enabled = True
        with self.assertRaises(StorageConfigError):
            StorageConfig(worker_cache_enabled="false")
        self.assertEqual(StorageConfig(), settings)

    def test_all_24_combinations_are_checked(self):
        valid = {
            ("legacy", False, False, "resource"),
            ("nfs", False, True, "resource"),
            ("nfs", True, True, "resource"),
            ("nfs", True, True, "locality"),
            ("nfs", True, True, "bandwidth"),
        }
        for values in itertools.product(("legacy", "nfs"), (False, True), (False, True), ("resource", "locality", "bandwidth")):
            document = dict(zip(OFF, values))
            with self.subTest(values=values):
                if values in valid:
                    settings = validate_config(document)
                    if values[0] == "legacy":
                        require_legacy_runtime(settings)
                    else:
                        with self.assertRaisesRegex(StorageConfigError, "not implemented/enabled"):
                            require_legacy_runtime(settings)
                else:
                    with self.assertRaises(StorageConfigError):
                        validate_config(document)

    def test_booleans_do_not_coerce_integers_strings_or_null(self):
        for name in ("worker_cache_enabled", "storage_aware_staging"):
            for value in (0, 1, "off", "on", "false", None, [], {}):
                with self.subTest(name=name, value=value):
                    with self.assertRaises(StorageConfigError):
                        validate_config({name: value})

    def test_invalid_enums_and_nonobject_values_fail_closed(self):
        for name in ("storage_backend", "scheduler_policy"):
            for value in ("", "LEGACY", " nfs", "resource ", 0, True, [], None):
                with self.subTest(name=name, value=value):
                    with self.assertRaises(StorageConfigError):
                        validate_config({name: value})
        for document in (None, [], "legacy", False, 0, {1: "bad"}):
            with self.assertRaises(StorageConfigError):
                validate_config(document)

    def test_unknown_and_old_draft_names_are_not_silently_ignored(self):
        for name in ("worker_cache", "storage_backend_typo", "nfs_root", "arbitrary"):
            with self.assertRaisesRegex(StorageConfigError, "Unknown storage settings"):
                validate_config({name: "on"})

    def test_duplicate_malformed_nonfinite_and_invalid_utf8_json_are_rejected(self):
        for raw in (b'{"worker_cache_enabled":false,"worker_cache_enabled":true}', b'{',
                    b'{"worker_cache_enabled":NaN}', b'{"worker_cache_enabled":Infinity}', b'\xff'):
            with self.subTest(raw=raw):
                self.path.write_bytes(raw)
                with self.assertRaises(StorageConfigError):
                    load_config(self.path)

    def test_oversized_and_excessively_nested_documents_are_rejected(self):
        for raw in (b" " * (MAX_CONFIG_BYTES + 1), b"[" * 2000 + b"0" + b"]" * 2000):
            self.path.write_bytes(raw)
            with self.assertRaises(StorageConfigError):
                load_config(self.path)

    def test_missing_file_creates_nothing(self):
        missing = self.root / "absent-parent/storage.json"
        self.assertEqual(load_config(missing), StorageConfig())
        self.assertEqual(list(self.root.iterdir()), [])

    def test_symlinks_fifo_and_directory_are_not_treated_as_missing_config(self):
        target = self.root / "target"
        target.write_text("{}")
        self.path.symlink_to(target)
        with self.assertRaises(StorageConfigError):
            load_config(self.path)
        target.unlink()
        with self.assertRaises(StorageConfigError):
            load_config(self.path)
        self.path.unlink()
        os.mkfifo(self.path)
        with self.assertRaises(StorageConfigError):
            load_config(self.path)
        with self.assertRaises(StorageConfigError):
            load_config(self.root)

    def test_permission_and_io_errors_do_not_fall_back(self):
        for code in (errno.EACCES, errno.EIO, errno.ESTALE):
            with patch("hybrid_storage.config.os.open", side_effect=OSError(code, "simulated error")):
                with self.assertRaises(StorageConfigError):
                    load_config(self.path)

    def test_cli_checks_legacy_held_nfs_and_rejects_unimplemented_features(self):
        output = io.StringIO()
        with redirect_stdout(output):
            self.assertEqual(main(["--config", str(self.write(OFF))]), 0)
        self.assertEqual(json.loads(output.getvalue()), {"runtime": "legacy", "settings": OFF})
        self.write(NFS)
        output = io.StringIO()
        with redirect_stdout(output):
            self.assertEqual(main(["--config", str(self.path)]), 0)
        self.assertEqual(json.loads(output.getvalue()),
                         {"runtime": "legacy", "settings": NFS, "new_job_default": "nfs", "nfs_execution": "blocked"})
        self.write({**NFS, "worker_cache_enabled": True})
        with patch("sys.stderr", new_callable=io.StringIO) as errors:
            with self.assertRaises(SystemExit) as raised:
                main(["--config", str(self.path)])
        self.assertEqual(raised.exception.code, 2)
        self.assertIn("not implemented/enabled", errors.getvalue())

    def test_import_and_loading_have_no_db_or_backend_side_effects(self):
        script = (
            "import sys; from pathlib import Path; from hybrid_storage.config import load_config; "
            "load_config(Path(sys.argv[1])); "
            "assert 'job_store' not in sys.modules and 'job_storage' not in sys.modules; "
            "assert 'sqlite3' not in sys.modules; print('no DB imports')"
        )
        result = subprocess.run([sys.executable, "-B", "-c", script, str(self.path)], cwd=ROOT,
                                capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "no DB imports")
        self.assertEqual(list(self.root.iterdir()), [])


class LegacyStartupTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.copy = tempfile.TemporaryDirectory(prefix="storage-config-controller-")
        cls.root = Path(cls.copy.name)
        # Existing modules initialize DBs on import; import only these copies.
        for name in ("controller", "agent", "config", "examples"):
            shutil.copytree(ROOT / name, cls.root / name,
                            ignore=shutil.ignore_patterns("jobs.db*", "*.lock", "__pycache__"))
        sys.path.insert(0, str(cls.root / "controller"))
        import job_store
        import scheduler
        import dispatcher
        import student_portal
        cls.store, cls.scheduler = job_store, scheduler
        cls.dispatcher, cls.portal = dispatcher, student_portal

    @classmethod
    def tearDownClass(cls):
        sys.path.remove(str(cls.root / "controller"))
        cls.copy.cleanup()

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="storage-startup-")
        self.addCleanup(self.temporary.cleanup)
        self.base = Path(self.temporary.name)
        (self.base / "config").mkdir()
        (self.base / "controller").mkdir()
        self.path = self.base / "config/storage.json"

    def setting(self, document):
        if document is None:
            self.path.unlink(missing_ok=True)
        else:
            self.path.write_text(json.dumps(document))

    def test_dispatcher_defaults_and_held_nfs_preserve_legacy_monitor_order(self):
        class StopLoop(Exception):
            pass
        traces = []
        for document in (None, {}, OFF, NFS):
            self.setting(document)
            calls = Mock()
            handles = []
            actual_open = open

            def tracked_open(*args, **kwargs):
                handle = actual_open(*args, **kwargs)
                handles.append(handle)
                return handle

            with patch.object(self.dispatcher, "PROJECT_DIR", self.base), \
                 patch.object(self.dispatcher, "CONTROLLER_DIR", self.base / "controller"), \
                 patch.object(self.store, "initialize_database", calls.initialize), \
                 patch.object(self.dispatcher, "init_recovery_table", calls.recovery), \
                 patch.object(self.dispatcher.ownership, "start_renewals", calls.renewals), \
                 patch.object(self.dispatcher, "reconcile_cancellations", calls.cancellations), \
                 patch.object(self.dispatcher, "reconcile_scheduled_jobs", calls.scheduled), \
                 patch.object(self.dispatcher, "monitor_running_jobs", calls.monitor), \
                 patch.object(self.dispatcher, "dispatch_queued_jobs", calls.dispatch), \
                 patch.object(self.dispatcher.time, "sleep", side_effect=StopLoop), \
                 patch("builtins.open", side_effect=tracked_open), \
                 redirect_stdout(io.StringIO()):
                with self.assertRaises(StopLoop):
                    self.dispatcher.main()
            for handle in handles:
                handle.close()
            traces.append([entry[0] for entry in calls.mock_calls])
        self.assertEqual(traces, [["initialize", "recovery", "renewals", "cancellations", "scheduled", "monitor", "dispatch"]] * 4)

    def test_unsupported_cache_blocks_dispatcher_before_lock_or_renewals(self):
        self.setting({**NFS, "worker_cache_enabled": True})
        with patch.object(self.dispatcher, "PROJECT_DIR", self.base), \
             patch.object(self.dispatcher, "CONTROLLER_DIR", self.base / "controller"), \
             patch.object(self.dispatcher.ownership, "start_renewals") as renewals:
            with self.assertRaises(StorageConfigError):
                self.dispatcher.main()
        renewals.assert_not_called()
        self.assertFalse((self.base / "controller/.dispatcher.lock").exists())

    def test_unsupported_or_malformed_config_blocks_portal_before_auth_storage(self):
        for raw in (json.dumps({**NFS, "worker_cache_enabled": True}), '{"worker_cache_enabled":"false"}'):
            self.path.write_text(raw)
            with patch.object(self.portal, "ROOT", self.base), \
                 patch.object(self.portal, "configure_storage") as configure:
                with self.assertRaises(StorageConfigError):
                    self.portal.create_app(self.base / "auth")
            configure.assert_not_called()
            self.assertFalse((self.base / "auth").exists())

    def test_off_and_missing_preserve_uploaded_inputs_and_resource_selection(self):
        outcomes = []
        for index, document in enumerate((None, {}, OFF)):
            self.setting(document)
            work = self.base / str(index)
            work.mkdir()
            with patch.object(self.portal, "ROOT", self.base), \
                 patch.object(self.store, "DB_FILE", str(work / "jobs.db")), \
                 patch.dict(os.environ, {"DL_STORAGE_ROOT": str(work / "jobs")}):
                self.store.initialize_database()
                app = self.portal.create_app(work / "auth")
                self.portal.add_user(work / "auth", "student1", "student-password")
                client = app.test_client()
                csrf = client.get("/api/session").json["csrf"]
                logged = client.post("/api/login", json={"owner": "student1", "password": "student-password"}, headers={"X-CSRF-Token": csrf})
                spec = json.loads((ROOT / "examples/job.single.json").read_text())
                spec.update(project="project.zip", dataset="dataset.csv")
                import zipfile
                project = io.BytesIO()
                with zipfile.ZipFile(project, "w") as archive:
                    archive.writestr("train.py", 'print("legacy")\n')
                    archive.writestr("requirements.txt", "")
                project.seek(0)
                response = client.post("/api/jobs", headers={"X-CSRF-Token": logged.json["csrf"]}, data={
                    "spec": json.dumps(spec), "project": (project, "project.zip"),
                    "dataset": (io.BytesIO(b"x,y\n1,2\n"), "dataset.csv"),
                })
                self.assertEqual(response.status_code, 202, response.json)
                job = self.store.get_job(response.json["job_id"])
                workspace = Path(job["storage_path"])
                manifest, _ = self.portal.verify_workspace(workspace, job)
                outcomes.append((job["status"], job["owner"], manifest["source"]["version"], manifest["data"]["version"],
                                 (workspace / "source/train.py").read_bytes(), (workspace / "data/dataset.csv").read_bytes()))
                self.assertTrue(all((workspace / name).is_dir() for name in ("checkpoints", "logs", "results")))
            nodes = [{"id": name, "ip": "unused", "port": 1} for name in ("pi01", "pi02", "pi03")]
            snapshots = {
                "pi01": {"node_state": "IDLE", "cpu": {"usage_percent": 1, "available_cores": 4}, "memory": {"available_gb": 5}, "temperature_c": 40},
                "pi02": {"node_state": "IDLE", "cpu": {"usage_percent": 2, "available_cores": 4}, "memory": {"available_gb": 4}, "temperature_c": 40},
                "pi03": {"node_state": "IDLE", "cpu": {"usage_percent": 2, "available_cores": 4}, "memory": {"available_gb": 6}, "temperature_c": 40},
            }
            with patch.object(self.scheduler, "load_nodes", return_value=nodes), \
                 patch.object(self.scheduler, "get_resources", side_effect=lambda node: snapshots[node["id"]]), \
                 redirect_stdout(io.StringIO()):
                choice = self.scheduler.select_best_node(reserved_nodes={"pi01"}, requirements={"cpu_cores": 2, "memory_gb": 1})
            self.assertEqual(choice["node"]["id"], "pi03")
        self.assertEqual(outcomes, [outcomes[0]] * 3)
        self.assertEqual(outcomes[0][:2], ("STAGED", "student1"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
