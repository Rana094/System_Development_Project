"""Pinned selection and unchanged legacy flows; every controller/DB is temporary."""

import atexit
from contextlib import redirect_stdout
import io
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
import zipfile

ROOT = Path(__file__).resolve().parents[1]
COPY = tempfile.TemporaryDirectory(prefix="storage-selection-controller-")
atexit.register(COPY.cleanup)
APP = Path(COPY.name)
for name in ("controller", "hybrid_storage", "config", "examples", "agent"):
    shutil.copytree(ROOT / name, APP / name, ignore=shutil.ignore_patterns("jobs.db*", "*.lock", "__pycache__"))
sys.path.insert(0, str(APP))
sys.path.insert(0, str(APP / "controller"))
import job_store as store
import job_storage
import storage_selection
import student_portal as portal
import prepare_job
import dispatcher
import distributed_dispatch
import recovery_metrics
import submit_job
from job_spec import validate_spec
from hybrid_storage.config import (
    StorageConfig, StorageConfigError, require_controller_config, select_submission_backend,
)

OFF = {"storage_backend": "legacy", "worker_cache_enabled": False,
       "storage_aware_staging": False, "scheduler_policy": "resource"}
NFS = {**OFF, "storage_backend": "nfs", "storage_aware_staging": True}


class SelectionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="storage-selection-")
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.inputs = self.base / "uploaded"
        shutil.copytree(ROOT / "examples/student_project", self.inputs / "project")
        (self.inputs / "data.csv").write_text("x,y\n1,2\n")
        self.document = json.loads((ROOT / "examples/job.single.json").read_text())
        self.document.update(project="project", dataset="data.csv")
        self.spec = validate_spec(self.document, self.inputs)
        patches = [patch.object(store, "DB_FILE", str(self.base / "jobs.db")),
                   patch.object(recovery_metrics, "DB_PATH", self.base / "jobs.db"),
                   patch.dict(os.environ, DL_STORAGE_ROOT=str(self.base / "jobs")),
                   patch.object(portal.ClusterView, "get", return_value=[]),
                   patch.object(portal, "dispatcher_running", return_value=False)]
        for mocked in patches:
            mocked.start()
            self.addCleanup(mocked.stop)
        self.configure(OFF)
        store.initialize_database()
        recovery_metrics.init_table()
        self.serial = 0

    def configure(self, settings):
        (APP / "config/storage.json").write_text(json.dumps(settings))

    def new(self, backend="legacy"):
        self.serial += 1
        job_id = f"selection-{self.serial}"
        store.add_job(job_id, 1, job_type="custom", status="RECEIVED", owner="alice",
                      job_config=self.spec, storage_backend=backend)
        return job_id

    def staged(self, backend="legacy"):
        job_id = self.new(backend)
        return job_id, job_storage.stage_job(job_id)

    def state(self, job_id, state):
        with store.get_connection() as db:
            db.execute("UPDATE jobs SET status=? WHERE job_id=?", (state, job_id))

    def client(self, settings):
        self.configure(settings)
        auth = self.base / "auth"
        portal.add_user(auth, "alice", "student-password")
        app = portal.create_app(auth)
        client = app.test_client()
        csrf = client.get("/api/session").json["csrf"]
        login = client.post("/api/login", json={"owner": "alice", "password": "student-password"}, headers={"X-CSRF-Token": csrf})
        self.assertEqual(login.status_code, 200)
        return app, client, {"X-CSRF-Token": login.json["csrf"]}

    def upload(self, client, headers, **extra):
        project = io.BytesIO()
        with zipfile.ZipFile(project, "w") as archive:
            archive.writestr("train.py", 'print("owned")\n')
            archive.writestr("requirements.txt", "")
        project.seek(0)
        return client.post("/api/jobs", headers=headers, data={
            "spec": json.dumps(self.document), "project": (project, "project.zip"),
            "dataset": (io.BytesIO(b"x,y\n1,2\n"), "data.csv"), **extra,
        })

    def test_selection_defaults_overrides_and_operator_opt_in(self):
        off, nfs = StorageConfig(), StorageConfig(**NFS)
        self.assertEqual(select_submission_backend(off), "legacy")
        self.assertEqual(select_submission_backend(nfs), "nfs")
        self.assertEqual(select_submission_backend(nfs, "legacy"), "legacy")
        with self.assertRaises(StorageConfigError):
            select_submission_backend(off, "nfs")
        for value in ("", "NFS", "unknown", True, 1):
            with self.subTest(value=value), self.assertRaises(StorageConfigError):
                select_submission_backend(nfs, value)
        self.assertEqual(require_controller_config(nfs), nfs)
        with self.assertRaises(StorageConfigError):
            require_controller_config(StorageConfig(**{**NFS, "worker_cache_enabled": True}))

    def test_additive_schema_is_idempotent_and_does_not_backfill_old_jobs(self):
        db = sqlite3.connect(":memory:")
        self.addCleanup(db.close)
        db.execute("CREATE TABLE jobs(job_id TEXT PRIMARY KEY,status TEXT,checkpoint_path TEXT)")
        db.execute("INSERT INTO jobs VALUES('old','RUNNING','keep/checkpoint.zip')")
        before = db.execute("SELECT * FROM jobs").fetchall()
        for _ in range(2):
            storage_selection.create_schema(db)
        self.assertEqual(before, db.execute("SELECT * FROM jobs").fetchall())
        self.assertEqual(db.execute("SELECT count(*) FROM job_storage_bindings").fetchone()[0], 0)
        self.assertEqual(storage_selection.binding(db, "old")["origin"], "implicit_legacy")

    def test_new_job_binding_is_explicit_and_config_changes_cannot_rebind(self):
        legacy, nfs = self.new(), self.new("nfs")
        self.configure(NFS)
        self.assertEqual(store.get_storage_binding(legacy)["backend"], "legacy")
        self.configure(OFF)
        self.assertEqual(store.get_storage_binding(nfs)["backend"], "nfs")
        self.assertEqual(store.get_storage_binding(legacy)["origin"], "pinned")

    def test_implicit_legacy_running_job_keeps_state_and_checkpoint_across_migration(self):
        job_id = self.new()
        with store.get_connection() as db:
            db.execute("DELETE FROM job_storage_bindings WHERE job_id=?", (job_id,))
            db.execute("UPDATE jobs SET status='RUNNING',node_id='old-node',checkpoint_path='keep.pt',retry_count=3 WHERE job_id=?", (job_id,))
        before = store.get_job(job_id)
        self.configure(NFS)
        store.initialize_database()
        self.assertEqual(store.get_job(job_id), before)
        self.assertEqual(store.get_storage_binding(job_id)["origin"], "implicit_legacy")
        store.require_storage_execution(job_id)

    def test_selection_and_job_insert_rollback_together(self):
        with patch.object(storage_selection, "pin", side_effect=RuntimeError("metadata failure")), self.assertRaises(RuntimeError):
            self.new("nfs")
        self.assertIsNone(store.get_job("selection-1"))
        with store.get_connection() as db:
            self.assertEqual(db.execute("SELECT count(*) FROM job_storage_bindings").fetchone()[0], 0)

    def test_duplicate_id_and_sql_update_cannot_change_backend(self):
        job_id = self.new()
        with self.assertRaises(sqlite3.IntegrityError):
            store.add_job(job_id, 1, job_type="custom", status="RECEIVED", owner="alice", storage_backend="nfs")
        with store.get_connection() as db, self.assertRaises(sqlite3.IntegrityError):
            db.execute("UPDATE job_storage_bindings SET backend='nfs' WHERE job_id=?", (job_id,))
        self.assertEqual(store.get_storage_binding(job_id)["backend"], "legacy")

    def test_unknown_binding_version_fails_closed(self):
        job_id = self.new()
        with store.get_connection() as db:
            db.execute("DROP TRIGGER immutable_job_storage_binding")
            db.execute("PRAGMA ignore_check_constraints=ON")
            db.execute("UPDATE job_storage_bindings SET binding_version=2 WHERE job_id=?", (job_id,))
        with self.assertRaises(ValueError):
            store.require_storage_execution(job_id)

    def test_nfs_upload_is_snapshotted_locally_and_held_without_object_copy(self):
        job_id, receipt = self.staged("nfs")
        self.assertEqual(receipt["status"], "HELD")
        job = store.get_job(job_id)
        workspace = Path(job["storage_path"])
        manifest, digest = job_storage.verify_workspace(workspace, job)
        self.assertEqual(digest, job["storage_manifest_sha256"])
        self.assertEqual((workspace / "data/data.csv").read_bytes(), b"x,y\n1,2\n")
        self.assertTrue((self.inputs / "project/train.py").exists())
        self.assertIn("NFS", job["queue_reason"])
        self.assertFalse((self.base / "objects").exists())
        self.assertEqual(store.get_reservations(), [])

    def test_release_is_blocked_but_owner_cancel_preserves_snapshot(self):
        job_id, _ = self.staged("nfs")
        with self.assertRaisesRegex(ValueError, "does not belong"):
            store.control_job(job_id, "bob", "release")
        with self.assertRaisesRegex(ValueError, "NFS"):
            store.control_job(job_id, "alice", "release")
        self.assertEqual(store.control_job(job_id, "alice", "cancel"), "CANCELLED")
        self.assertTrue((Path(store.get_job(job_id)["storage_path"]) / "manifest.json").exists())

    def test_prepare_and_verify_refuse_before_worker_transport(self):
        job_id, _ = self.staged("nfs")
        self.state(job_id, "STAGED")  # defense against a bypassed hold
        with patch.object(prepare_job, "Worker", side_effect=AssertionError("No worker traffic")):
            with self.assertRaisesRegex(ValueError, "NFS"):
                prepare_job.prepare_on_worker(job_id, {"id": "local"}, "isolated")
            with self.assertRaisesRegex(ValueError, "NFS"):
                prepare_job.verify_on_worker(job_id, {"id": "local"}, [])

    def test_queue_preparation_and_reservation_guards_survive_state_tampering(self):
        job_id, _ = self.staged("nfs")
        self.state(job_id, "READY")
        for operation in (lambda: store.record_environment(job_id, "pi02", {}),
                          lambda: store.enqueue_custom_job(job_id, {"pi02"}),
                          lambda: portal.PreparationQueue().enqueue(job_id, "isolated")):
            with self.assertRaisesRegex(ValueError, "NFS"):
                operation()
        self.state(job_id, "QUEUED")
        with self.assertRaisesRegex(ValueError, "NFS"):
            store.mark_scheduled(job_id, "pi02", "unused", {}, [{"id": "pi02"}], {"world_size": 1})
        self.assertEqual(store.fair_queue(), [])
        self.assertEqual(store.get_reservations(), [])
        self.assertEqual(store.get_ranks(job_id), [])

    def test_single_and_ddp_dispatch_refuse_before_selection_or_http(self):
        job_id, _ = self.staged("nfs")
        self.state(job_id, "QUEUED")
        job = store.get_job(job_id)
        with patch.object(dispatcher, "select_best_node", side_effect=AssertionError("No placement")), patch.object(distributed_dispatch, "select_group", side_effect=AssertionError("No group selection")):
            self.assertFalse(dispatcher.dispatch_one_job(job))
            self.assertFalse(distributed_dispatch.dispatch(job, set()))

    def test_legacy_http_response_and_preparation_contract_are_unchanged(self):
        app, client, headers = self.client(OFF)
        result = self.upload(client, headers)
        self.assertEqual(result.status_code, 202, result.json)
        self.assertEqual(set(result.json), {"job_id", "status"})
        self.assertEqual(result.json["status"], "STAGED")
        job_id = result.json["job_id"]
        self.assertEqual(store.get_storage_binding(job_id)["backend"], "legacy")
        with store.get_connection() as db:
            self.assertEqual(db.execute("SELECT state FROM portal_preparation WHERE job_id=?", (job_id,)).fetchone()[0], "PENDING")

    def test_http_nfs_default_is_held_without_preparation_and_visible_to_owner(self):
        app, client, headers = self.client(NFS)
        with patch.object(app.extensions["preparation"], "enqueue", side_effect=AssertionError("Do not prepare NFS")):
            result = self.upload(client, headers)
        self.assertEqual(result.status_code, 202, result.json)
        self.assertEqual(result.json["status"], "HELD")
        self.assertEqual(result.json["storage_backend"], "nfs")
        job_id = result.json["job_id"]
        self.assertEqual(client.get("/api/jobs/" + job_id).json["job"]["status"], "HELD")
        self.assertEqual(client.post(f"/api/jobs/{job_id}/release", headers=headers).status_code, 400)
        with store.get_connection() as db:
            self.assertEqual(db.execute("SELECT count(*) FROM portal_preparation").fetchone()[0], 0)
        self.assertEqual(store.get_attempts(job_id), [])

    def test_http_legacy_override_under_nfs_default_keeps_legacy_flow(self):
        app, client, headers = self.client(NFS)
        result = self.upload(client, headers, storage_backend="legacy")
        self.assertEqual(result.status_code, 202, result.json)
        self.assertEqual(result.json["status"], "STAGED")
        self.assertEqual(store.get_storage_binding(result.json["job_id"])["backend"], "legacy")

    def test_http_operator_gate_and_invalid_override_reject_before_job_insert(self):
        app, client, headers = self.client(OFF)
        for value in ("nfs", "", "not-a-backend"):
            result = self.upload(client, headers, storage_backend=value)
            self.assertEqual(result.status_code, 400, result.json)
        self.assertEqual(store.get_all_jobs(), [])

    def test_restart_with_legacy_default_does_not_release_or_reinterpret_nfs_job(self):
        app, client, headers = self.client(NFS)
        result = self.upload(client, headers)
        job_id = result.json["job_id"]
        manifest = Path(store.get_job(job_id)["storage_path"]) / "manifest.json"
        before = manifest.read_bytes()
        self.client(OFF)
        self.assertEqual(store.get_storage_binding(job_id)["backend"], "nfs")
        with self.assertRaises(ValueError):
            store.control_job(job_id, "alice", "release")
        self.assertEqual(manifest.read_bytes(), before)

    def test_cli_selection_uses_same_snapshot_and_hold(self):
        self.configure(NFS)
        with redirect_stdout(io.StringIO()) as output:
            job_id = submit_job.create_custom_job(self.spec, "alice", storage_backend="nfs")
        receipt = json.loads(output.getvalue())
        self.assertEqual(receipt["status"], "HELD")
        self.assertEqual(receipt["storage_selection"]["backend"], "nfs")
        self.assertEqual(store.get_storage_binding(job_id)["backend"], "nfs")
        self.configure(OFF)
        with self.assertRaises(StorageConfigError):
            submit_job.create_custom_job(self.spec, "alice", storage_backend="nfs")

    def test_controller_only_cli_missing_config_stays_legacy(self):
        directory = self.base / "standalone/controller"
        directory.mkdir(parents=True)
        with patch.object(submit_job, "CONTROLLER_DIR", directory):
            self.assertEqual(submit_job.submission_backend(), "legacy")
            self.assertEqual(submit_job.submission_backend("legacy"), "legacy")
            with self.assertRaises(ValueError):
                submit_job.submission_backend("nfs")
            with self.assertRaises(ValueError):
                submit_job.submission_backend("invalid")

    def test_cli_parser_accepts_nfs_flag_only_for_custom_submissions(self):
        self.configure(NFS)
        definition = self.base / "job.json"
        definition.write_text(json.dumps(dict(self.document, project=str(self.inputs / "project"), dataset=str(self.inputs / "data.csv"))))
        command = [sys.executable, str(APP / "controller/submit_job.py"), "--spec", str(definition),
                   "--owner", "alice", "--storage-backend", "nfs"]
        result = subprocess.run(command, capture_output=True, text=True, timeout=20)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)["status"], "HELD")
        invalid = subprocess.run([sys.executable, str(APP / "controller/submit_job.py"), "--storage-backend", "legacy"], capture_output=True, text=True, timeout=20)
        self.assertEqual(invalid.returncode, 2)
        self.assertIn("require --spec", invalid.stderr)

    def test_present_config_without_package_does_not_fall_back(self):
        original_import = __import__
        def import_without_package(name, *args, **kwargs):
            if name == "hybrid_storage.config":
                raise ModuleNotFoundError("package absent", name="hybrid_storage")
            return original_import(name, *args, **kwargs)
        with patch("builtins.__import__", side_effect=import_without_package), self.assertRaisesRegex(ValueError, "requires the hybrid_storage package"):
            submit_job.submission_backend()


if __name__ == "__main__":
    unittest.main(verbosity=2)
