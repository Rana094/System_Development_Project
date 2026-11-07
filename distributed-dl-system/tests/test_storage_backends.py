"""Pi-less storage adapter contracts; all files and simulated failures are local."""

import errno
import hashlib
import io
import os
from pathlib import Path
import stat
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from hybrid_storage import LegacyStorageBackend, NFSStorageBackend, StorageBackend


class BackendTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="storage-backends-")
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name)
        self.inputs = self.base / "inputs"
        self.inputs.mkdir()
        (self.inputs / "nested").mkdir()
        (self.inputs / "nested/data.bin").write_bytes(b"authorized input")
        self.outside = self.base / "outside"
        self.outside.mkdir()
        (self.outside / "data.bin").write_bytes(b"other student's input")
        self.staging = self.base / "private-staging"
        self.staging.mkdir(mode=0o700)

    def readers(self):
        return (LegacyStorageBackend(self.inputs), NFSStorageBackend(self.inputs))

    def publisher(self):
        return LegacyStorageBackend(self.inputs, staging_root=self.staging)

    def publish(self, backend=None, key="published.bin", data=b"verified bytes", **expected):
        return (backend or self.publisher()).publish_artifact(
            key, io.BytesIO(data),
            expected_sha256=expected.get("sha256", hashlib.sha256(data).hexdigest()),
            expected_size=expected.get("size", len(data)),
        )

    def test_both_adapters_implement_protocol_and_read_existing_files(self):
        for backend in self.readers():
            with self.subTest(backend=type(backend).__name__):
                self.assertIsInstance(backend, StorageBackend)
                self.assertTrue(backend.artifact_exists("nested/data.bin"))
                self.assertEqual(backend.resolve_path("nested/data.bin"), self.inputs / "nested/data.bin")
                with backend.open_artifact("nested/data.bin") as stream:
                    self.assertEqual(stream.read(), b"authorized input")
                    self.assertFalse(stream.writable())
                    with self.assertRaises(io.UnsupportedOperation):
                        stream.write(b"changed")
                self.assertTrue(stream.closed)

    def test_missing_files_and_parents_are_misses_but_open_raises(self):
        for backend in self.readers():
            for key in ("missing", "missing/child"):
                with self.subTest(backend=type(backend).__name__, key=key):
                    self.assertFalse(backend.artifact_exists(key))
                    with self.assertRaises(FileNotFoundError):
                        backend.resolve_path(key)
                    with self.assertRaises(FileNotFoundError):
                        with backend.open_artifact(key):
                            self.fail("Missing input opened")

    def test_noncanonical_and_escaping_keys_are_rejected(self):
        keys = ("", ".", "..", "../outside/data.bin", "/etc/passwd", "nested/../x",
                "nested//data.bin", "./nested/data.bin", "nested/./data.bin",
                "nested/data.bin/", "nested\\data.bin", "C:data", "bad\0name", None)
        for backend in self.readers():
            for key in keys:
                with self.subTest(backend=type(backend).__name__, key=key):
                    with self.assertRaises(ValueError):
                        backend.artifact_exists(key)

    def test_symlink_files_and_parent_directories_are_rejected(self):
        (self.inputs / "linked.bin").symlink_to(self.outside / "data.bin")
        (self.inputs / "linked-directory").symlink_to(self.outside, target_is_directory=True)
        (self.inputs / "dangling").symlink_to(self.outside / "missing")
        for backend in self.readers():
            for key in ("linked.bin", "linked-directory/data.bin", "dangling"):
                with self.subTest(backend=type(backend).__name__, key=key):
                    with self.assertRaises(OSError):
                        backend.artifact_exists(key)

    def test_symlink_roots_and_root_ancestors_are_rejected(self):
        alias = self.base / "alias"
        alias.symlink_to(self.inputs, target_is_directory=True)
        parent_alias = self.base / "parent-alias"
        parent_alias.symlink_to(self.base, target_is_directory=True)
        for backend_type in (LegacyStorageBackend, NFSStorageBackend):
            for root in (alias, parent_alias / "inputs"):
                with self.subTest(backend=backend_type.__name__, root=root):
                    with self.assertRaises(OSError):
                        backend_type(root).artifact_exists("nested/data.bin")

    def test_directory_and_fifo_are_rejected_without_waiting_for_a_writer(self):
        os.mkfifo(self.inputs / "pipe")
        for backend in self.readers():
            for key in ("nested", "pipe"):
                with self.subTest(backend=type(backend).__name__, key=key):
                    with self.assertRaises(ValueError):
                        backend.artifact_exists(key)

    def test_path_resolution_is_not_used_as_a_safe_open_token(self):
        for backend in self.readers():
            with self.subTest(backend=type(backend).__name__):
                path = backend.resolve_path("nested/data.bin")
                original = path.read_bytes()
                path.unlink()
                path.symlink_to(self.outside / "data.bin")
                try:
                    with self.assertRaises(OSError):
                        with backend.open_artifact("nested/data.bin"):
                            self.fail("A substituted symlink opened")
                finally:
                    path.unlink()
                    path.write_bytes(original)

    def test_directory_descriptor_keeps_read_anchored_during_parent_swap(self):
        actual_open = os.open

        def swap_then_open(path, flags, *args, **kwargs):
            if path == "data.bin":
                (self.inputs / "nested").rename(self.inputs / "original")
                (self.inputs / "nested").symlink_to(self.outside, target_is_directory=True)
            return actual_open(path, flags, *args, **kwargs)

        with patch("hybrid_storage.backends.os.open", side_effect=swap_then_open):
            with NFSStorageBackend(self.inputs).open_artifact("nested/data.bin") as stream:
                self.assertEqual(stream.read(), b"authorized input")

    def test_io_and_permission_errors_are_not_reported_as_missing(self):
        for backend in self.readers():
            for code in (errno.EIO, errno.ETIMEDOUT, errno.EACCES, errno.ESTALE):
                with self.subTest(backend=type(backend).__name__, errno=code):
                    with patch("hybrid_storage.backends.os.open", side_effect=OSError(code, "simulated outage")):
                        with self.assertRaises(OSError) as raised:
                            backend.artifact_exists("nested/data.bin")
                    self.assertEqual(raised.exception.errno, code)

    def test_construction_and_import_do_not_create_roots_or_load_controller(self):
        for backend_type in (LegacyStorageBackend, NFSStorageBackend):
            backend_type(self.base / "not-created")
            with self.assertRaises(ValueError):
                backend_type(Path("relative"))
            with self.assertRaises(ValueError):
                backend_type(Path("/"))
            with self.assertRaises(ValueError):
                backend_type(self.base / ".." / "escape")
        self.assertFalse((self.base / "not-created").exists())
        self.assertNotIn("job_store", sys.modules)
        self.assertNotIn("job_storage", sys.modules)

    def test_nfs_rejects_publication_even_on_writable_mock_mount(self):
        source = io.BytesIO(b"unread")
        with self.assertRaises(PermissionError) as raised:
            NFSStorageBackend(self.inputs).publish_artifact(
                "new", source, expected_sha256=hashlib.sha256(b"unread").hexdigest(), expected_size=6,
            )
        self.assertEqual(raised.exception.errno, errno.EROFS)
        self.assertEqual(source.tell(), 0)
        self.assertFalse((self.inputs / "new").exists())

    def test_local_publication_requires_explicit_private_staging(self):
        with self.assertRaises(PermissionError):
            self.publish(LegacyStorageBackend(self.inputs))
        self.staging.chmod(0o755)
        with self.assertRaises(PermissionError):
            self.publish()
        self.assertEqual(list(self.staging.iterdir()), [])
        self.assertFalse((self.inputs / "published.bin").exists())

    def test_staging_roots_cannot_overlap_or_follow_symlinks(self):
        for path in (self.inputs, self.inputs / "nested", self.base):
            with self.subTest(staging=path):
                with self.assertRaises(ValueError):
                    LegacyStorageBackend(self.inputs, staging_root=path)
        alias = self.base / "staging-alias"
        alias.symlink_to(self.staging, target_is_directory=True)
        with self.assertRaises(OSError):
            self.publish(LegacyStorageBackend(self.inputs, staging_root=alias))

    def test_verified_publication_is_visible_to_both_adapters(self):
        data = b"verified\0input\xff"
        path = self.publish(key="nested/published.bin", data=data)
        self.assertEqual(path, self.inputs / "nested/published.bin")
        self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o444)
        self.assertEqual(path.stat().st_nlink, 1)
        self.assertEqual(list(self.staging.iterdir()), [])
        for backend in self.readers():
            with backend.open_artifact("nested/published.bin") as stream:
                self.assertEqual(stream.read(), data)

    def test_empty_artifact_can_be_published(self):
        path = self.publish(data=b"")
        self.assertEqual(path.read_bytes(), b"")

    def test_publication_never_overwrites_existing_files_or_symlinks(self):
        (self.inputs / "linked").symlink_to(self.outside / "data.bin")
        (self.inputs / "dangling").symlink_to(self.outside / "missing")
        for key in ("nested/data.bin", "linked", "dangling"):
            with self.subTest(key=key):
                with self.assertRaises(FileExistsError):
                    self.publish(key=key)
        self.assertEqual((self.inputs / "nested/data.bin").read_bytes(), b"authorized input")
        self.assertEqual((self.outside / "data.bin").read_bytes(), b"other student's input")
        self.assertFalse((self.outside / "missing").exists())
        self.assertEqual(list(self.staging.iterdir()), [])

    def test_publication_rejects_unsafe_parent_and_does_not_create_parents(self):
        (self.inputs / "linked-directory").symlink_to(self.outside, target_is_directory=True)
        for key, error in (("linked-directory/new", OSError), ("missing/new", FileNotFoundError), ("../new", ValueError)):
            with self.subTest(key=key):
                with self.assertRaises(error):
                    self.publish(key=key)
        self.assertFalse((self.outside / "new").exists())
        self.assertFalse((self.inputs / "missing").exists())
        self.assertEqual(list(self.staging.iterdir()), [])

    def test_bad_expected_metadata_is_rejected_before_creating_temporary_files(self):
        for expected in ({"size": -1}, {"size": True}, {"size": 1.5}, {"sha256": "bad"}, {"sha256": "A" * 64}):
            with self.subTest(expected=expected):
                with self.assertRaises(ValueError):
                    self.publish(**expected)
        self.assertEqual(list(self.staging.iterdir()), [])

    def test_hash_or_size_mismatch_never_publishes_partial_bytes(self):
        for expected in ({"size": 1}, {"size": 100}, {"sha256": "0" * 64}):
            with self.subTest(expected=expected):
                with self.assertRaises(ValueError):
                    self.publish(**expected)
                self.assertFalse((self.inputs / "published.bin").exists())
                self.assertEqual(list(self.staging.iterdir()), [])

    def test_expected_size_bounds_consumption_of_oversized_input(self):
        source = io.BytesIO(b"x" * 100)
        with self.assertRaises(ValueError):
            self.publisher().publish_artifact("new", source, expected_size=2, expected_sha256="0" * 64)
        self.assertEqual(source.tell(), 3)
        self.assertFalse(source.closed)
        self.assertEqual(list(self.staging.iterdir()), [])

    def test_interrupted_source_and_disk_full_remove_only_own_temporary_file(self):
        unrelated = self.staging / "leave-me"
        unrelated.write_bytes(b"preexisting")
        broken = io.BytesIO(b"partial")
        with patch.object(broken, "read", side_effect=[b"partial", OSError(errno.EIO, "interrupted")]):
            with self.assertRaises(OSError):
                self.publisher().publish_artifact("new", broken, expected_size=10, expected_sha256="0" * 64)
        with patch("hybrid_storage.backends.os.fsync", side_effect=OSError(errno.ENOSPC, "disk full")):
            with self.assertRaises(OSError):
                self.publish()
        self.assertEqual(list(self.staging.iterdir()), [unrelated])
        self.assertFalse((self.inputs / "new").exists())
        self.assertFalse((self.inputs / "published.bin").exists())

    def test_before_commit_only_private_staging_contains_partial_input(self):
        data = b"input"
        source = io.BytesIO(data)
        actual_read = source.read

        def inspect_then_read(size):
            self.assertFalse((self.inputs / "new").exists())
            self.assertEqual(len(list(self.staging.iterdir())), 1)
            return actual_read(size)

        with patch.object(source, "read", side_effect=inspect_then_read):
            self.publisher().publish_artifact(
                "new", source, expected_size=len(data), expected_sha256=hashlib.sha256(data).hexdigest(),
            )
        self.assertEqual((self.inputs / "new").read_bytes(), data)

    def test_competing_publication_cannot_replace_winner(self):
        actual_link = os.link

        def competitor_then_link(*args, **kwargs):
            (self.inputs / "published.bin").write_bytes(b"winner")
            return actual_link(*args, **kwargs)

        with patch("hybrid_storage.backends.os.link", side_effect=competitor_then_link):
            with self.assertRaises(FileExistsError):
                self.publish()
        self.assertEqual((self.inputs / "published.bin").read_bytes(), b"winner")
        self.assertEqual(list(self.staging.iterdir()), [])

    def test_post_commit_fsync_error_leaves_only_complete_file_for_reconciliation(self):
        with patch("hybrid_storage.backends.os.fsync", side_effect=[None, OSError(errno.EIO, "directory sync failed")]):
            with self.assertRaises(OSError):
                self.publish()
        self.assertEqual((self.inputs / "published.bin").read_bytes(), b"verified bytes")
        self.assertEqual(list(self.staging.iterdir()), [])
        with self.assertRaises(FileExistsError):
            self.publish()


if __name__ == "__main__":
    unittest.main(verbosity=2)
