"""Strict input views and the controller seam; fake NFS directories only."""

from dataclasses import FrozenInstanceError
import errno
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from hybrid_storage import LegacyStorageBackend, StorageBackend
from hybrid_storage.inputs import (
    InputFile, LocalObjectStorageBackend, NFSObjectStorageBackend,
    ProjectInputView, object_key, project_path, validate_object_key,
)

spec = importlib.util.spec_from_file_location("controller_input_storage", ROOT / "controller/input_storage.py")
controller = importlib.util.module_from_spec(spec)
spec.loader.exec_module(controller)


class InputTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="storage-input-views-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.objects = self.root / "objects"
        self.objects.mkdir()
        self.staging = self.root / "private"
        self.staging.mkdir(mode=0o700)
        self.project = self.root / "project"
        self.project.mkdir()
        self.data = b"owned immutable input\n"
        self.sha = hashlib.sha256(self.data).hexdigest()
        self.key = object_key(self.sha)
        self.entry = InputFile("package/train.py", self.sha, len(self.data))
        (self.project / "package").mkdir()
        (self.project / self.entry.path).write_bytes(self.data)
        (self.project / "not-authorized.txt").write_text("not in manifest")
        (self.objects / self.key).parent.mkdir(parents=True)
        # Explicit test-only kernel identity mocks: ordinary directories are not
        # accepted as NFS by the production adapter after Phase 3.2.
        for name, result in (("require_input_mount", {"mount_id": 123}), ("fd_mount_id", 123)):
            mocked = patch("hybrid_storage.inputs." + name, return_value=result)
            mocked.start()
            self.addCleanup(mocked.stop)

    def nfs(self):
        return NFSObjectStorageBackend(self.objects, source="192.0.2.1:/srv/ml-shared/objects")

    def publisher(self):
        return controller.local_objects(self.objects, staging_root=self.staging)

    def publish(self, backend=None):
        return (backend or self.publisher()).publish_artifact(
            self.key, io.BytesIO(self.data), expected_sha256=self.sha, expected_size=len(self.data)
        )

    def test_sha256_layout_and_strict_digest_validation(self):
        self.assertEqual(self.key, "sha256/" + self.sha[:2] + "/" + self.sha)
        self.assertEqual(validate_object_key(self.key), self.key)
        for value in (None, 1, b"0" * 64, "", "0" * 63, "0" * 65, "A" * 64, "g" * 64, "0" * 64 + "\n", "sha256:" + self.sha):
            with self.subTest(value=value), self.assertRaises(ValueError):
                object_key(value)

    def test_general_relative_or_mismatched_object_keys_fail_before_filesystem_access(self):
        invalid = (None, self.sha, "nested/data.bin", "/etc/passwd", "../file", "sha256/zz/" + self.sha,
                   "sha256/" + ("00" if self.sha[:2] != "00" else "01") + "/" + self.sha,
                   self.key + "/", "./" + self.key, self.key + "\n", self.key.replace("/", "//", 1))
        for cls in (LocalObjectStorageBackend, NFSObjectStorageBackend):
            backend = cls(self.objects)
            for key in invalid:
                with self.subTest(backend=cls.__name__, key=key), patch("hybrid_storage.backends.os.open", side_effect=AssertionError("No I/O for invalid key")):
                    for method in (backend.artifact_exists, backend.resolve_path):
                        with self.assertRaises(ValueError):
                            method(key)
                    with self.assertRaises(ValueError), backend.open_artifact(key):
                        self.fail("Unsafe object opened")
                    with self.assertRaises(ValueError):
                        backend.publish_artifact(key, io.BytesIO(self.data), expected_sha256=self.sha, expected_size=len(self.data))

    def test_local_verified_publication_and_worker_read_share_existing_protocol(self):
        target = self.publish()
        self.assertEqual(target, self.objects / self.key)
        self.assertEqual(target.stat().st_mode & 0o777, 0o444)
        for backend in (self.publisher(), self.nfs()):
            self.assertIsInstance(backend, StorageBackend)
            self.assertTrue(backend.artifact_exists(self.key))
            self.assertEqual(backend.resolve_path(self.key), target)
            with backend.open_artifact(self.key) as stream:
                self.assertEqual(stream.read(), self.data)
                self.assertFalse(stream.writable())
            self.assertTrue(stream.closed)
        self.assertEqual(list(self.staging.iterdir()), [])

    def test_object_key_and_publication_hash_must_agree_before_consuming_source(self):
        stream = io.BytesIO(self.data)
        with self.assertRaises(ValueError):
            self.publisher().publish_artifact(self.key, stream, expected_sha256="0" * 64, expected_size=len(self.data))
        self.assertEqual(stream.tell(), 0)
        self.assertFalse((self.objects / self.key).exists())

    def test_wrong_publication_bytes_never_become_object(self):
        with self.assertRaises(ValueError):
            self.publisher().publish_artifact(self.key, io.BytesIO(b"wrong"), expected_sha256=self.sha, expected_size=len(self.data))
        self.assertFalse((self.objects / self.key).exists())
        self.assertEqual(list(self.staging.iterdir()), [])

    def test_local_publication_requires_private_capability_and_preprovisioned_prefix(self):
        with self.assertRaises(PermissionError):
            self.publish(LocalObjectStorageBackend(self.objects))
        empty = self.root / "unprovisioned"
        empty.mkdir()
        with self.assertRaises(FileNotFoundError):
            self.publish(LocalObjectStorageBackend(empty, staging_root=self.staging))
        self.assertEqual(list(empty.iterdir()), [])

    def test_existing_object_cannot_be_overwritten(self):
        target = self.publish()
        inode = target.stat().st_ino
        with self.assertRaises(FileExistsError):
            self.publish()
        self.assertEqual(target.read_bytes(), self.data)
        self.assertEqual(target.stat().st_ino, inode)

    def test_worker_never_publishes_even_to_writable_fake_nfs_directory(self):
        stream = io.BytesIO(self.data)
        with self.assertRaises(PermissionError) as raised:
            NFSObjectStorageBackend(self.objects).publish_artifact(self.key, stream, expected_sha256=self.sha, expected_size=len(self.data))
        self.assertEqual(raised.exception.errno, errno.EROFS)
        self.assertEqual(stream.tell(), 0)
        self.assertFalse((self.objects / self.key).exists())

    def test_empty_object_is_a_valid_sha256_identity(self):
        sha = hashlib.sha256(b"").hexdigest()
        key = object_key(sha)
        (self.objects / key).parent.mkdir(parents=True, exist_ok=True)
        self.publisher().publish_artifact(key, io.BytesIO(), expected_sha256=sha, expected_size=0)
        with self.nfs().open_artifact(key) as stream:
            self.assertEqual(stream.read(), b"")

    def test_controlled_project_allowlist_refuses_unlisted_existing_files_before_io(self):
        view = controller.project_inputs(self.project, [self.entry])
        self.assertIsInstance(view, StorageBackend)
        self.assertEqual(view.reference(self.entry.path), self.entry)
        with view.open_artifact(self.entry.path) as stream:
            self.assertEqual(stream.read(), self.data)
        with patch("hybrid_storage.backends.os.open", side_effect=AssertionError("Denied before I/O")):
            for method in (view.artifact_exists, view.resolve_path, view.reference):
                with self.assertRaises(PermissionError) as raised:
                    method("not-authorized.txt")
                self.assertEqual(raised.exception.errno, errno.EACCES)
            with self.assertRaises(PermissionError), view.open_artifact("not-authorized.txt"):
                self.fail("Unlisted file opened")

    def test_approved_missing_file_is_distinct_from_unapproved_path(self):
        view = controller.project_inputs(self.project, [self.entry])
        (self.project / self.entry.path).unlink()
        self.assertFalse(view.artifact_exists(self.entry.path))
        with self.assertRaises(FileNotFoundError), view.open_artifact(self.entry.path):
            self.fail("Missing input opened")
        with self.assertRaises(PermissionError):
            view.artifact_exists("not-authorized.txt")

    def test_object_project_view_derives_key_from_allowlisted_identity(self):
        self.publish()
        view = ProjectInputView(self.nfs(), [self.entry], objects=True)
        with view.open_artifact(self.entry.path) as stream:
            self.assertEqual(stream.read(), self.data)
        self.assertEqual(view.resolve_path(self.entry.path), self.objects / self.key)
        with self.assertRaises(PermissionError):
            view.artifact_exists(self.key)
        with self.assertRaises(PermissionError):
            view.artifact_exists("other-owner.py")

    def test_project_paths_reject_traversal_aliases_controls_and_surrogates(self):
        invalid = (None, "", ".", "..", "../file", "/etc/passwd", "a/../b", "./a", "a//b", "a/", "a\\b", "C:a", "a\x00", "a\n", "a\x7f", "a\ud800")
        for path in invalid:
            with self.subTest(path=path), self.assertRaises(ValueError):
                InputFile(path, self.sha, len(self.data))
        self.assertEqual(project_path("পাঠ/train.py"), "পাঠ/train.py")
        self.assertEqual(project_path("literal%2fpath"), "literal%2fpath")

    def test_grant_snapshot_is_immutable_and_rejects_ambiguous_file_sets(self):
        files = [self.entry]
        view = controller.project_inputs(self.project, files)
        files.append(InputFile("not-authorized.txt", self.sha, len(self.data)))
        with self.assertRaises(PermissionError):
            view.artifact_exists("not-authorized.txt")
        with self.assertRaises(FrozenInstanceError):
            self.entry.path = "other"
        invalid = ([self.entry, self.entry], [self.entry, InputFile("package", self.sha, len(self.data))],
                   [self.entry, InputFile("other", self.sha, 1)], [{"path": "forged"}])
        for entries in invalid:
            with self.subTest(entries=entries), self.assertRaises(ValueError):
                controller.project_inputs(self.project, entries)
        for size in (True, -1, 1.0, "1", None):
            with self.subTest(size=size), self.assertRaises(ValueError):
                InputFile("train.py", self.sha, size)

    def test_project_view_cannot_publish_or_accept_wrong_layout_backend(self):
        for view in (controller.project_inputs(self.project, [self.entry]), ProjectInputView(self.publisher(), [self.entry], objects=True)):
            with self.assertRaises(PermissionError) as raised:
                view.publish_artifact(self.entry.path, io.BytesIO(self.data), expected_sha256=self.sha, expected_size=len(self.data))
            self.assertEqual(raised.exception.errno, errno.EROFS)
        for backend, objects in ((self.publisher(), False), (LegacyStorageBackend(self.objects), True), (LegacyStorageBackend(self.objects), "yes")):
            with self.assertRaises(ValueError):
                ProjectInputView(backend, [self.entry], objects=objects)

    def test_no_follow_is_preserved_for_project_file_parent_and_root(self):
        outside = self.root / "outside"
        outside.write_text("other owner's bytes")
        file = self.project / self.entry.path
        file.unlink()
        file.symlink_to(outside)
        view = controller.project_inputs(self.project, [self.entry])
        with self.assertRaises(OSError), view.open_artifact(self.entry.path):
            self.fail("Symlink opened")
        alias = self.root / "alias"
        alias.symlink_to(self.project, target_is_directory=True)
        with self.assertRaises(OSError), controller.project_inputs(alias, [self.entry]).open_artifact(self.entry.path):
            self.fail("Symlink root opened")
        file.unlink()
        (self.project / "package").rmdir()
        (self.project / "package").symlink_to(self.root, target_is_directory=True)
        with self.assertRaises(OSError), view.open_artifact(self.entry.path):
            self.fail("Symlink parent followed")

    def test_construction_and_controller_import_have_no_db_copy_or_directory_side_effects(self):
        missing = self.root / "not-created"
        for cls in (LocalObjectStorageBackend, NFSObjectStorageBackend):
            cls(missing)
        controller.project_inputs(missing, [self.entry])
        self.assertFalse(missing.exists())
        script = "import sys;from pathlib import Path;sys.path.insert(0,sys.argv[1]);import input_storage;assert 'job_store' not in sys.modules;assert 'job_storage' not in sys.modules"
        result = subprocess.run([sys.executable, "-B", "-c", script, str(ROOT / "controller")], capture_output=True, text=True, timeout=15)
        self.assertEqual(result.returncode, 0, result.stderr)


class ControllerIntegrationTests(unittest.TestCase):
    def test_existing_snapshot_reads_and_explicit_object_roundtrip_preserve_legacy_manifest(self):
        with tempfile.TemporaryDirectory(prefix="input-seam-controller-") as temporary:
            base = Path(temporary)
            shutil.copytree(ROOT / "controller", base / "controller", ignore=shutil.ignore_patterns("jobs.db*", "*.lock", "__pycache__"))
            shutil.copytree(ROOT / "hybrid_storage", base / "hybrid_storage", ignore=shutil.ignore_patterns("__pycache__"))
            shutil.copytree(ROOT / "examples/student_project", base / "uploaded/project")
            (base / "uploaded/dataset.csv").write_text("x,y\n1,2\n")
            document = json.loads((ROOT / "examples/job.single.json").read_text())
            document.update(project="project", dataset="dataset.csv")
            (base / "document.json").write_text(json.dumps(document))
            script = '''import hashlib,io,json,os,sys
from pathlib import Path
base=Path(sys.argv[1]);sys.path.insert(0,str(base/'controller'))
os.environ['DL_STORAGE_ROOT']=str(base/'managed')
import job_store,job_storage,input_storage
from job_spec import validate_spec
from hybrid_storage.inputs import InputFile,NFSObjectStorageBackend,ProjectInputView,object_key
from unittest.mock import patch
spec=validate_spec(json.loads((base/'document.json').read_text()),base/'uploaded')
job_store.add_job('test-input-seam',1,job_type='custom',status='RECEIVED',owner='student',job_config=spec)
receipt=job_storage.stage_job('test-input-seam');workspace=Path(receipt['storage_path'])
job=job_store.get_job('test-input-seam');manifest,_=job_storage.verify_workspace(workspace,job)
before=(workspace/'manifest.json').read_bytes()
files=[InputFile(x['path'],x['sha256'],x['size']) for x in manifest['source']['inventory']['files']]
view=input_storage.project_inputs(workspace/'source',files)
objects=base/'objects';objects.mkdir();private=base/'private';private.mkdir(mode=0o700)
publisher=input_storage.local_objects(objects,staging_root=private)
for entry in files:
 key=object_key(entry.sha256);(objects/key).parent.mkdir(parents=True,exist_ok=True)
 with view.open_artifact(entry.path) as incoming: publisher.publish_artifact(key,incoming,expected_sha256=entry.sha256,expected_size=entry.size_bytes)
remote=ProjectInputView(NFSObjectStorageBackend(objects,source='192.0.2.1:/srv/ml-shared/objects'),files,objects=True)
with patch('hybrid_storage.inputs.require_input_mount',return_value={'mount_id':123}),patch('hybrid_storage.inputs.fd_mount_id',return_value=123):
 for entry in files:
  with remote.open_artifact(entry.path) as incoming: assert hashlib.sha256(incoming.read()).hexdigest()==entry.sha256
assert before==(workspace/'manifest.json').read_bytes()
assert job_store.get_job('test-input-seam')['status']=='STAGED'
job_storage.verify_workspace(workspace,job)
print(json.dumps({'files_read':len(files),'legacy_manifest_unchanged':True,'status':'STAGED','nfs':'local fake directory only'}))
'''
            result = subprocess.run([sys.executable, "-I", "-B", "-c", script, str(base)], capture_output=True, text=True, timeout=30)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertGreater(json.loads(result.stdout)["files_read"], 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
