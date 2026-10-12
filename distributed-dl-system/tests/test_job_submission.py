"""Submission contract and CLI checks using temporary inputs and an isolated database."""

import copy
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest
import zipfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "controller"))
from job_spec import JobSpecError, load_spec, validate_owner, validate_spec


class SubmissionValidationTests(unittest.TestCase):
    """Reject ambiguous declarations before any job is accepted."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="submission-inputs-")
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        shutil.copytree(
            ROOT / "examples/student_project", self.base / "student_project"
        )
        shutil.copyfile(ROOT / "examples/dataset.csv", self.base / "dataset.csv")
        self.spec = json.loads((ROOT / "examples/job.single.json").read_text())

    def test_both_modes_and_lab_resource_declarations(self):
        single = validate_spec(self.spec, self.base)
        self.assertEqual(single["project"], str(self.base / "student_project"))
        self.assertEqual(single["recovery"]["max_retries"], 2)
        self.spec["execution"] = {"mode": "distributed", "workers": 30}
        self.spec["resources_per_worker"]["gpus"] = 1
        result = validate_spec(self.spec, self.base)
        self.assertEqual(result["execution"]["workers"], 30)
        self.assertEqual(result["resources_per_worker"]["gpus"], 1)

    def test_zip_project_and_generated_dataset(self):
        with zipfile.ZipFile(self.base / "project.zip", "w") as archive:
            archive.write(
                self.base / "student_project/requirements.txt", "requirements.txt"
            )
            archive.write(self.base / "student_project/train.py", "train.py")
        self.spec["project"] = "project.zip"
        self.spec["dataset"] = None
        self.spec["recovery"] = {"mode": "checkpoint", "interval_seconds": 30}
        self.assertEqual(
            validate_spec(self.spec, self.base)["recovery"]["interval_seconds"], 30
        )

    def test_reject_invalid_fields_and_cross_field_settings(self):
        changes = [
            ("schema_version", 2),
            ("schema_version", True),
            ("name", " "),
            ("entrypoint", "python train.py"),
            ("entrypoint", []),
            ("entrypoint", ["python", "bad\0argument"]),
            ("execution", {"mode": "single", "workers": 2}),
            ("execution", {"mode": "distributed", "workers": 1}),
            ("execution", {"mode": "single", "workers": True}),
            ("execution", {"mode": "parallel", "workers": 2}),
            ("resources_per_worker", {"cpu_cores": 0, "memory_gb": 1, "gpus": 0}),
            ("resources_per_worker", {"cpu_cores": 1.5, "memory_gb": 1, "gpus": 0}),
            (
                "resources_per_worker",
                {"cpu_cores": 1, "memory_gb": float("inf"), "gpus": 0},
            ),
            ("resources_per_worker", {"cpu_cores": 1, "memory_gb": True, "gpus": 0}),
            ("resources_per_worker", {"cpu_cores": 1, "memory_gb": 1, "gpus": -1}),
            ("environment", {"requirements": "../dataset.csv"}),
            ("environment", {"requirements": "/etc/passwd"}),
            ("environment", {"requirements": "missing.txt"}),
            ("environment", {"requirements": "requirements.txt", "python": "latest"}),
            ("recovery", {"mode": "checkpoint"}),
            ("recovery", {"mode": "checkpoint", "interval_seconds": 0}),
            ("recovery", {"mode": "restart", "interval_seconds": 30}),
            ("recovery", {"mode": "restart", "max_retries": 11}),
        ]
        for field, value in changes:
            with self.subTest(field=field, value=value):
                document = copy.deepcopy(self.spec)
                document[field] = value
                with self.assertRaises(JobSpecError):
                    validate_spec(document, self.base)
        for document in (
            {**self.spec, "resouces": {}},
            {k: v for k, v in self.spec.items() if k != "dataset"},
        ):
            with self.assertRaises(JobSpecError):
                validate_spec(document, self.base)

    def test_missing_input_corrupt_zip_and_escaping_dependency(self):
        for field in ("project", "dataset"):
            document = {**self.spec, field: "missing"}
            with self.assertRaises(JobSpecError):
                validate_spec(document, self.base)
        (self.base / "broken.zip").write_text("not a ZIP")
        with self.assertRaises(JobSpecError):
            validate_spec({**self.spec, "project": "broken.zip"}, self.base)
        requirements = self.base / "student_project/requirements.txt"
        requirements.unlink()
        requirements.symlink_to(self.base / "dataset.csv")
        with self.assertRaises(JobSpecError):
            validate_spec(self.spec, self.base)

    def test_strict_json_and_owner(self):
        manifest = self.base / "bad.json"
        for content in (
            '{"name":"a","name":"b"}',
            '{"memory":NaN}',
            "[1]",
            "{",
            '{"number":' + "9" * 5000 + "}",
        ):
            manifest.write_text(content)
            with self.assertRaises(JobSpecError):
                load_spec(manifest)
        for owner in (None, "", "student name", "../student"):
            with self.assertRaises(JobSpecError):
                validate_owner(owner)
        self.assertEqual(validate_owner("student@example.edu"), "student@example.edu")


class SubmissionIntegrationTests(unittest.TestCase):
    """Exercise the real CLI, legacy migration and dispatcher against temporary state."""

    def test_cli_atomic_persistence_and_dispatch_boundary(self):
        with tempfile.TemporaryDirectory(prefix="submission-controller-") as directory:
            root = Path(directory)
            shutil.copytree(
                ROOT / "controller",
                root / "controller",
                ignore=shutil.ignore_patterns("jobs.db*", "*.lock", "__pycache__"),
            )
            shutil.copytree(ROOT / "config", root / "config")
            shutil.copytree(ROOT / "examples", root / "examples")
            db = root / "controller/jobs.db"
            # An older database must retain history when the owner field is added.
            with sqlite3.connect(db) as connection:
                connection.execute(
                    "CREATE TABLE jobs (job_id TEXT PRIMARY KEY, status TEXT NOT NULL, duration INTEGER NOT NULL, node_id TEXT, node_ip TEXT, created_at REAL NOT NULL, scheduled_at REAL, started_at REAL, finished_at REAL, return_code INTEGER, error TEXT)"
                )
                connection.execute(
                    "INSERT INTO jobs (job_id,status,duration,created_at) VALUES ('legacy','COMPLETED',1,1)"
                )
            env = dict(
                os.environ,
                PYTHONDONTWRITEBYTECODE="1",
                DL_STORAGE_ROOT=str(root / "storage/jobs"),
            )

            def cli(*arguments):
                return subprocess.run(
                    [
                        sys.executable,
                        str(root / "controller/submit_job.py"),
                        *arguments,
                    ],
                    cwd=root / "config",
                    env=env,
                    capture_output=True,
                    text=True,
                    timeout=30,
                )

            single = str(root / "examples/job.single.json")
            result = cli("--spec", single, "--validate-only")
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertTrue(json.loads(result.stdout)["valid"])
            for arguments in (
                ("--spec", single),
                ("--spec", single, "--owner", "student", "--epochs", "1"),
                ("30", "--spec", single, "--owner", "student"),
                ("--spec", single, "--owner", "student", "--epo", "1"),
            ):
                result = cli(*arguments)
                self.assertNotEqual(result.returncode, 0)
            with sqlite3.connect(db) as connection:
                self.assertEqual(
                    connection.execute("SELECT COUNT(*) FROM jobs").fetchone()[0], 1
                )
            accepted = []
            for manifest in ("job.single.json", "job.distributed.json"):
                result = cli(
                    "--spec",
                    str(root / "examples" / manifest),
                    "--owner",
                    "student-2026",
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                receipt = json.loads(result.stdout)
                self.assertEqual(receipt["status"], "STAGED")
                accepted.append(receipt["job_id"])
                with sqlite3.connect(db) as connection:
                    row = connection.execute(
                        "SELECT status,owner,job_type,job_config,max_retries FROM jobs WHERE job_id=?",
                        (receipt["job_id"],),
                    ).fetchone()
                    self.assertEqual(row[:3], ("STAGED", "student-2026", "custom"))
                    self.assertEqual(json.loads(row[3]), receipt["submission"])
                    self.assertEqual(row[4], 2)
                status = subprocess.run(
                    [
                        sys.executable,
                        str(root / "controller/job_status.py"),
                        receipt["job_id"],
                    ],
                    env=env,
                    capture_output=True,
                    text=True,
                    timeout=30,
                )
                self.assertEqual(status.returncode, 0, status.stderr)
                self.assertIn("student-2026", status.stdout)
                self.assertIn("STAGED", status.stdout)
            self.assertNotEqual(*accepted)

            # A new interpreter observes the saved submissions but dispatches none.
            checker = root / "check.py"
            checker.write_text("""import sys
from unittest.mock import patch
sys.path.insert(0, "controller")
import job_store as store
import dispatcher
assert len(store.get_jobs_by_status("STAGED")) == 2
assert not store.get_jobs_by_status("QUEUED")
with patch.object(dispatcher, "dispatch_one_job") as dispatch:
    dispatcher.dispatch_queued_jobs()
    dispatch.assert_not_called()
assert store.get_job("legacy")["status"] == "COMPLETED"
assert store.get_job("legacy")["owner"] is None
try:
    store.add_job("invalid-custom", 1, job_type="custom")
except ValueError:
    pass
else:
    raise AssertionError("Custom jobs must not reach the legacy dispatcher")
""")
            result = subprocess.run(
                [sys.executable, str(checker)],
                cwd=root,
                env=env,
                capture_output=True,
                text=True,
                timeout=60,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            legacy = cli("5", "--type", "dummy")
            self.assertEqual(legacy.returncode, 0, legacy.stderr)
            with sqlite3.connect(db) as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM jobs WHERE status='QUEUED' AND job_type='dummy'"
                    ).fetchone()[0],
                    1,
                )


if __name__ == "__main__":
    unittest.main(verbosity=2)
