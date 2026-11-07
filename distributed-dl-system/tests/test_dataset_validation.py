"""Read-only source/ZIP safety on disposable directories, without extraction."""

import hashlib
import io
import os
from pathlib import Path
import stat
import sys
import tempfile
import unittest
import warnings
import zipfile
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from hybrid_storage.dataset_validation import inventory_tree, validate_zip_members


class ValidationTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="dataset-inspection-")
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name)
        self.root = self.base / "dataset"
        self.root.mkdir()

    def zip(self, names):
        stream = io.BytesIO()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)  # Deliberate duplicate fixtures.
            with zipfile.ZipFile(stream, "w") as archive:
                for name, payload in names:
                    archive.writestr(name, payload)
        return zipfile.ZipFile(io.BytesIO(stream.getvalue()))

    def test_inventory_hashes_regular_files_and_retains_sorted_empty_directories(self):
        (self.root / "empty").mkdir()
        (self.root / "nested").mkdir()
        data = b"validated bytes\n" * 100000
        (self.root / "nested/input.csv").write_bytes(data)
        (self.root / "zero").write_bytes(b"")
        entries = inventory_tree(self.root)
        self.assertEqual([entry.path for entry in entries], ["empty", "nested", "nested/input.csv", "zero"])
        self.assertEqual(entries[2].sha256, hashlib.sha256(data).hexdigest())
        self.assertEqual(entries[2].size_bytes, len(data))
        self.assertEqual(entries[3].sha256, hashlib.sha256(b"").hexdigest())
        self.assertEqual((self.root / "nested/input.csv").read_bytes(), data)
        self.assertEqual(sorted(path.name for path in self.base.iterdir()), ["dataset"])

    def test_symlink_root_or_ancestor_is_never_followed(self):
        alias = self.base / "alias"
        alias.symlink_to(self.root, target_is_directory=True)
        with self.assertRaises(OSError): inventory_tree(alias)
        alias.unlink()
        alias.symlink_to(self.base, target_is_directory=True)
        with self.assertRaises(OSError): inventory_tree(alias / "dataset")

    def test_symlink_file_directory_and_fifo_are_rejected(self):
        outside = self.base / "outside"
        outside.mkdir()
        (outside / "secret").write_bytes(b"other owner")
        link = self.root / "input"
        for target in (outside / "secret", outside):
            link.symlink_to(target)
            with self.assertRaises(ValueError): inventory_tree(self.root)
            link.unlink()
        os.mkfifo(link)
        with self.assertRaises(ValueError): inventory_tree(self.root)
        self.assertEqual((outside / "secret").read_bytes(), b"other owner")

    def test_case_and_unicode_conflicting_real_files_are_rejected(self):
        for left, right in (("Data", "data"), ("é", "e\u0301")):
            a, b = self.root / left, self.root / right
            a.write_bytes(b"a")
            b.write_bytes(b"b")
            with self.subTest(left=left), self.assertRaises(ValueError): inventory_tree(self.root)
            a.unlink()
            b.unlink()

    def mutate_on_read(self, mutation):
        original = os.fdopen
        def opened(fd, mode):
            stream = original(fd, mode)
            class Changed:
                changed = False
                def __enter__(inner): return inner
                def __exit__(inner, *args): stream.close()
                def fileno(inner): return stream.fileno()
                def read(inner, count):
                    data = stream.read(count)
                    if not inner.changed:
                        inner.changed = True
                        mutation()
                    return data
            return Changed()
        return opened

    def test_file_growth_or_metadata_change_during_hash_is_rejected(self):
        file = self.root / "input"
        for growth in (True, False):
            file.write_bytes(b"dataset")
            def mutation():
                if growth:
                    with file.open("ab") as stream: stream.write(b"changed")
                else:
                    info = file.stat()
                    os.utime(file, ns=(info.st_atime_ns, info.st_mtime_ns + 1000000))
            with self.subTest(growth=growth), patch("hybrid_storage.dataset_validation.os.fdopen", side_effect=self.mutate_on_read(mutation)):
                with self.assertRaises(ValueError): inventory_tree(self.root)

    def test_directory_mutation_during_inspection_is_rejected(self):
        (self.root / "input").write_bytes(b"dataset")
        def mutation(): (self.root / "appeared").write_bytes(b"late")
        with patch("hybrid_storage.dataset_validation.os.fdopen", side_effect=self.mutate_on_read(mutation)):
            with self.assertRaises(ValueError): inventory_tree(self.root)

    def test_file_swap_to_symlink_before_open_is_rejected_without_reading_target(self):
        (self.root / "input").write_bytes(b"dataset")
        outside = self.base / "outside"
        outside.write_bytes(b"secret")
        original = os.open
        def changed(name, flags, *args, **kwargs):
            if name == "input":
                (self.root / "input").unlink()
                (self.root / "input").symlink_to(outside)
            return original(name, flags, *args, **kwargs)
        with patch("hybrid_storage.dataset_validation.os.open", side_effect=changed):
            with self.assertRaises(OSError): inventory_tree(self.root)
        self.assertEqual(outside.read_bytes(), b"secret")

    def test_missing_permission_and_entry_limits_fail_without_creation(self):
        with self.assertRaises(FileNotFoundError): inventory_tree(self.base / "missing")
        self.assertFalse((self.base / "missing").exists())
        with patch("hybrid_storage.dataset_validation.os.open", side_effect=PermissionError("denied")):
            with self.assertRaises(PermissionError): inventory_tree(self.root)
        for name in ("one", "two"): (self.root / name).write_bytes(b"bytes")
        with patch("hybrid_storage.dataset_validation.MAX_ENTRIES", 1):
            with self.assertRaises(ValueError): inventory_tree(self.root)

    def test_safe_zip_preflight_does_not_extract_or_register_anything(self):
        with self.zip([("nested/input.csv", b"data"), ("empty/", b"")]) as archive:
            with patch.object(archive, "extract", side_effect=AssertionError("No extraction")), \
                    patch.object(archive, "extractall", side_effect=AssertionError("No extraction")), \
                    patch.object(archive, "open", side_effect=AssertionError("Metadata preflight only")):
                self.assertEqual(validate_zip_members(archive, max_uncompressed_bytes=4), ("empty", "nested/input.csv"))
        self.assertEqual(list(self.root.iterdir()), [])

    def test_zip_slip_traversal_noncanonical_and_nul_names_are_rejected(self):
        for name in ("../escape", "/escape", "a/../escape", "./input", "a//input", "a\\input", "C:input", "a\ninput", "a\0input"):
            with self.subTest(name=repr(name)):
                if "\0" in name:
                    # ZipInfo preserves original before truncation; test the
                    # parser-supplied metadata explicitly, without rewriting it.
                    member = zipfile.ZipInfo(name)
                    with self.zip([]) as archive, patch.object(archive, "infolist", return_value=[member]):
                        with self.assertRaises(ValueError): validate_zip_members(archive, max_uncompressed_bytes=100)
                else:
                    with self.zip([(name, b"data")]) as archive:
                        with self.assertRaises(ValueError): validate_zip_members(archive, max_uncompressed_bytes=100)
        self.assertFalse((self.base / "escape").exists())

    def test_zip_case_unicode_duplicates_and_file_parent_conflicts_are_rejected(self):
        for names in (("A", "a"), ("A/x", "a/y"), ("é", "e\u0301"), ("same", "same"), ("input", "input/child")):
            with self.subTest(names=names), self.zip([(name, b"data") for name in names]) as archive:
                with self.assertRaises(ValueError): validate_zip_members(archive, max_uncompressed_bytes=100)

    def test_zip_unix_symlink_special_and_inconsistent_modes_are_rejected(self):
        for mode, name, payload in ((stat.S_IFLNK | 0o777, "link", b"../outside"),
                                     (stat.S_IFIFO | 0o600, "fifo", b""),
                                     (stat.S_IFDIR | 0o755, "not-dir", b""),
                                     (stat.S_IFREG | 0o644, "not-file/", b""),
                                     (stat.S_IFDIR | 0o755, "data-dir/", b"bytes")):
            member = zipfile.ZipInfo(name)
            member.create_system = 3
            member.external_attr = mode << 16
            with self.subTest(name=name), self.zip([(member, payload)]) as archive:
                with self.assertRaises(ValueError): validate_zip_members(archive, max_uncompressed_bytes=100)

    def test_zip_metadata_limits_encryption_and_dos_directory_mismatch(self):
        with self.zip([("one", b"1234"), ("two", b"56")]) as archive:
            for budget in (True, -1, 5):
                with self.subTest(budget=budget), self.assertRaises(ValueError):
                    validate_zip_members(archive, max_uncompressed_bytes=budget)
            with patch("hybrid_storage.dataset_validation.MAX_ENTRIES", 1):
                with self.assertRaises(ValueError): validate_zip_members(archive, max_uncompressed_bytes=100)
            first = archive.infolist()[0]
            first.flag_bits |= 1
            with self.assertRaises(ValueError): validate_zip_members(archive, max_uncompressed_bytes=100)
            first.flag_bits &= ~1
            first.external_attr |= 0x10
            with self.assertRaises(ValueError): validate_zip_members(archive, max_uncompressed_bytes=100)


if __name__ == "__main__":
    unittest.main(verbosity=2)
