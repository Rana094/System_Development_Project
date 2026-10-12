"""Managed input storage, crash reconciliation and content integrity in temporary state."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
import zipfile
import stat

ROOT = Path(__file__).resolve().parents[1]
# Import copies of controller modules so their database initialization is isolated.
ISOLATED = tempfile.TemporaryDirectory(prefix="storage-controller-")
CONTROLLER = Path(ISOLATED.name) / "controller"
shutil.copytree(
    ROOT / "controller",
    CONTROLLER,
    ignore=shutil.ignore_patterns("jobs.db*", "*.lock", "__pycache__"),
)
sys.path.insert(0, str(CONTROLLER))
import job_store as store
import job_storage as storage
from job_spec import validate_spec


class StorageTests(unittest.TestCase):
    """Exercise immutable inputs and mutable runtime areas through real storage code."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="storage-inputs-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.inputs = self.root / "inputs"
        self.inputs.mkdir()
        shutil.copytree(ROOT / "examples/student_project", self.inputs / "project")
        (self.inputs / "project/empty").mkdir()
        (self.inputs / "project/run.sh").write_text("#!/bin/sh\nexit 0\n")
        (self.inputs / "project/run.sh").chmod(0o755)
        (self.inputs / "dataset.csv").write_text("x,y\n1,2\n")
        self.storage = self.root / "managed"
        self.env = patch.dict(os.environ, DL_STORAGE_ROOT=str(self.storage))
        self.env.start()
        self.addCleanup(self.env.stop)
        self.doc = json.loads((ROOT / "examples/job.single.json").read_text())
        self.doc["project"] = "project"
        self.doc["dataset"] = "dataset.csv"
        self.next_id = 0

    def received(self, doc=None):
        self.next_id += 1
        jid = "storage-" + self.root.name + "-" + str(self.next_id)
        spec = validate_spec(doc or self.doc, self.inputs)
        store.add_job(
            jid,
            1,
            job_type="custom",
            status="RECEIVED",
            owner="student",
            job_config=spec,
        )
        return jid

    def verify(self, jid):
        job = store.get_job(jid)
        return storage.verify_workspace(Path(job["storage_path"]), job)[0]

    def test_owned_copy_survives_input_removal_and_controller_restart(self):
        jid = self.received()
        receipt = storage.stage_job(jid)
        workspace = Path(receipt["storage_path"])
        for name in ("source", "data", "checkpoints", "logs", "results"):
            self.assertTrue((workspace / name).is_dir())
        self.assertTrue((workspace / "source/empty").is_dir())
        self.assertTrue((workspace / "source/run.sh").stat().st_mode & 0o111)
        self.assertNotEqual(
            (workspace / "source/train.py").stat().st_ino,
            (self.inputs / "project/train.py").stat().st_ino,
        )
        shutil.rmtree(self.inputs)
        manifest = self.verify(jid)
        self.assertEqual(store.get_job(jid)["status"], "STAGED")
        self.assertEqual(
            json.loads(store.get_job(jid)["job_config"])["project"],
            str(workspace / "source"),
        )
        for area in ("checkpoints", "logs", "results"):
            (workspace / area / "runtime.txt").write_text("mutable runtime data")
        self.verify(jid)
        self.assertEqual(
            storage.stage_job(jid)["dataset_version"], receipt["dataset_version"]
        )
        run = subprocess.run(
            [sys.executable, str(CONTROLLER / "stage_job.py"), "--verify", jid],
            env=dict(os.environ, PYTHONDONTWRITEBYTECODE="1"),
            capture_output=True,
            text=True,
            timeout=30,
        )
        self.assertEqual(run.returncode, 0, run.stderr)
        self.assertTrue(json.loads(run.stdout)["verified"])
        self.assertEqual(manifest["job_id"], jid)

    def test_versions_depend_on_content_and_isolate_students(self):
        first = self.received()
        second = self.received()
        a, b = storage.stage_job(first), storage.stage_job(second)
        self.assertEqual(a["dataset_version"], b["dataset_version"])
        self.assertNotEqual(a["storage_path"], b["storage_path"])
        (self.inputs / "dataset.csv").write_text("x,y\n3,4\n")
        c = storage.stage_job(self.received())
        self.assertNotEqual(a["dataset_version"], c["dataset_version"])
        self.verify(first)
        self.verify(second)

    def test_source_data_and_manifest_tampering_are_detected(self):
        for relative in (
            "source/train.py",
            "data/dataset.csv",
            "manifest.json",
            "submission.json",
        ):
            with self.subTest(relative=relative):
                jid = self.received()
                result = storage.stage_job(jid)
                path = Path(result["storage_path"]) / relative
                with path.open("a") as stream:
                    stream.write("changed")
                with self.assertRaises((storage.StorageError, ValueError)):
                    self.verify(jid)
                with self.assertRaises(storage.StorageError):
                    storage.stage_job(jid)
                self.assertEqual(store.get_job(jid)["status"], "STAGED")

    def test_crash_after_publish_reconciles_without_original_inputs(self):
        jid = self.received()
        with patch.object(
            storage,
            "mark_staged",
            side_effect=RuntimeError("simulated controller crash"),
        ):
            with self.assertRaises(storage.StorageError):
                storage.stage_job(jid)
        workspace = self.storage / jid
        self.assertTrue((workspace / "manifest.json").is_file())
        self.assertEqual(store.get_job(jid)["status"], "RECEIVED")
        original_manifest = (workspace / "manifest.json").read_bytes()
        shutil.rmtree(self.inputs)
        result = subprocess.run(
            [sys.executable, str(CONTROLLER / "stage_job.py"), "--pending"],
            env=dict(os.environ, PYTHONDONTWRITEBYTECODE="1"),
            capture_output=True,
            text=True,
            timeout=30,
        )
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertEqual(store.get_job(jid)["status"], "STAGED")
        self.assertEqual((workspace / "manifest.json").read_bytes(), original_manifest)
        self.verify(jid)

    def test_interrupted_copy_never_publishes_and_retry_cleans_partial_workspace(self):
        jid = self.received()
        with patch.object(
            storage, "copy_file", side_effect=OSError("simulated disk full")
        ):
            with self.assertRaises(storage.StorageError):
                storage.stage_job(jid)
        self.assertFalse((self.storage / jid).exists())
        self.assertEqual(store.get_job(jid)["status"], "RECEIVED")
        self.assertIn("disk full", store.get_job(jid)["last_error"])
        partial = self.storage / ("." + jid + ".staging-crashed")
        partial.mkdir()
        (partial / "partial.txt").write_text("incomplete")
        storage.stage_job(jid)
        self.assertFalse(partial.exists())
        self.assertIsNone(store.get_job(jid)["last_error"])

    def test_zip_projects_and_datasets_and_generated_data(self):
        with zipfile.ZipFile(self.inputs / "project.zip", "w") as archive:
            for path in (self.inputs / "project").rglob("*"):
                archive.write(path, path.relative_to(self.inputs / "project"))
        with zipfile.ZipFile(self.inputs / "data.zip", "w") as archive:
            archive.writestr("images/example.bin", b"dataset bytes")
        doc = {**self.doc, "project": "project.zip", "dataset": "data.zip"}
        jid = self.received(doc)
        result = storage.stage_job(jid)
        manifest = self.verify(jid)
        self.assertEqual(
            (Path(result["dataset_path"]) / "images/example.bin").read_bytes(),
            b"dataset bytes",
        )
        self.assertEqual(manifest["source"]["kind"], "zip")
        self.assertEqual(len(manifest["data"]["upload_sha256"]), 64)
        doc["dataset"] = None
        generated = storage.stage_job(self.received(doc))
        self.assertIsNone(generated["dataset_path"])
        self.assertIsNone(generated["dataset_version"])

    def test_unsafe_zip_and_links_do_not_escape_storage(self):
        for kind in ("traversal", "absolute", "symlink", "duplicate"):
            with self.subTest(kind=kind):
                archive_path = self.inputs / "unsafe.zip"
                with zipfile.ZipFile(archive_path, "w") as archive:
                    archive.writestr("requirements.txt", "")
                    if kind == "symlink":
                        member = zipfile.ZipInfo("linked")
                        member.create_system = 3
                        member.external_attr = (stat.S_IFLNK | 0o777) << 16
                        archive.writestr(member, "../../outside")
                    elif kind == "duplicate":
                        archive.writestr("folder/file", "one")
                        archive.writestr("folder//file", "two")
                    else:
                        archive.writestr(
                            "../../outside" if kind == "traversal" else "/outside",
                            "bad",
                        )
                jid = self.received({**self.doc, "project": "unsafe.zip"})
                with self.assertRaises(storage.StorageError):
                    storage.stage_job(jid)
                self.assertFalse((self.storage / jid).exists())
                self.assertFalse((self.root / "outside").exists())
                # Remove failed records so --pending tests only see their own inputs.
                with store.get_connection() as con:
                    con.execute("DELETE FROM jobs WHERE job_id=?", (jid,))
        (self.inputs / "project/linked").symlink_to(self.inputs / "dataset.csv")
        jid = self.received()
        with self.assertRaises(storage.StorageError):
            storage.stage_job(jid)
        with store.get_connection() as con:
            con.execute("DELETE FROM jobs WHERE job_id=?", (jid,))

    def test_concurrent_staging_lock_and_input_mutation(self):
        jid = self.received()
        self.storage.mkdir()
        with storage.workspace_lock(self.storage, jid):
            with self.assertRaises(storage.StorageError):
                storage.stage_job(jid)
        original_copy = storage.copy_file

        def modifying_copy(source, target):
            original_copy(source, target)
            if source.name == "train.py":
                source.write_text(source.read_text() + "\n# changed during staging\n")

        with patch.object(storage, "copy_file", side_effect=modifying_copy):
            with self.assertRaises(storage.StorageError):
                storage.stage_job(jid)
        self.assertFalse((self.storage / jid).exists())
        storage.stage_job(jid)
        self.verify(jid)

    def test_dataset_directory_and_recursive_destination(self):
        dataset = self.inputs / "directory-data"
        (dataset / "empty").mkdir(parents=True)
        (dataset / "sample.bin").write_bytes(b"directory dataset")
        jid = self.received({**self.doc, "dataset": "directory-data"})
        result = storage.stage_job(jid)
        self.assertEqual(Path(result["dataset_path"]), self.storage / jid / "data")
        self.assertTrue((Path(result["dataset_path"]) / "empty").is_dir())
        self.verify(jid)
        other = self.received()
        bad_root = self.inputs / "project/recursive-storage"
        with patch.dict(os.environ, DL_STORAGE_ROOT=str(bad_root)):
            with self.assertRaises(storage.StorageError):
                storage.stage_job(other)
        self.assertFalse(bad_root.exists())
        storage.stage_job(other)


if __name__ == "__main__":
    try:
        unittest.main(verbosity=2)
    finally:
        ISOLATED.cleanup()
