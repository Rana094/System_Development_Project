"""Trusted registration acceptance on disposable local files; no jobs/DB/NFS."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import FrozenInstanceError
import errno
import hashlib
import io
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from hybrid_storage import artifacts
from hybrid_storage.artifacts import ArtifactMismatch, ArtifactRegistrar, CHUNK_BYTES
from hybrid_storage.inputs import object_key


class RegistrationTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="artifact-registration-")
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name)
        self.root = self.base / "objects"
        self.root.mkdir(mode=0o755)
        self.scratch = self.base / "private"
        self.scratch.mkdir(mode=0o700)
        self.registrar = ArtifactRegistrar(self.root, staging_root=self.scratch)
        self.data = b"authorized input fixture\n"
        self.sha = hashlib.sha256(self.data).hexdigest()

    def register(self, data=None, **changes):
        data = self.data if data is None else data
        return self.registrar.register(io.BytesIO(data), **dict({"expected_size": len(data)}, **changes))

    def assert_no_objects(self):
        self.assertEqual(list(self.root.rglob("[0-9a-f]" * 64)), [])

    def test_streaming_nonseekable_source_is_bounded_verified_and_caller_owned(self):
        data = bytes(range(256)) * (CHUNK_BYTES // 256 * 3) + b"tail"
        class Source:
            position = 0
            calls = []
            def read(inner, count):
                self.assertGreater(count, 0)
                self.assertLessEqual(count, CHUNK_BYTES)
                inner.calls.append(count)
                chunk = data[inner.position:inner.position + count]
                inner.position += len(chunk)
                return chunk
        source = Source()
        result = self.registrar.register(source, expected_size=len(data))
        self.assertEqual(result.sha256, hashlib.sha256(data).hexdigest())
        self.assertEqual(result.key, object_key(result.sha256))
        self.assertEqual(result.size_bytes, len(data))
        self.assertTrue(result.created)
        self.assertGreater(len(source.calls), 3)
        target = self.root / result.key
        self.assertEqual(target.read_bytes(), data)
        self.assertEqual(stat.S_IMODE(target.stat().st_mode), 0o444)
        self.assertEqual(list(self.scratch.iterdir()), [])
        with self.assertRaises(FrozenInstanceError):
            result.created = False

    def test_identical_streams_reuse_one_inode_without_overwrite(self):
        first = self.register(expected_sha256=self.sha)
        inode = (self.root / first.key).stat().st_ino
        second = self.register()
        self.assertTrue(first.created)
        self.assertFalse(second.created)
        self.assertEqual(first.sha256, second.sha256)
        self.assertEqual((self.root / second.key).stat().st_ino, inode)
        self.assertEqual(len(list(self.root.glob("sha256/*/*"))), 1)
        self.assertEqual(list(self.scratch.iterdir()), [])

    def test_changed_bytes_create_different_digest_without_editing_prior_object(self):
        first = self.register()
        second = self.register(self.data + b"changed")
        self.assertNotEqual(first.key, second.key)
        self.assertTrue(first.created and second.created)
        self.assertEqual((self.root / first.key).read_bytes(), self.data)

    def test_empty_content_is_a_deduplicated_valid_object(self):
        first, second = self.register(b""), self.register(b"")
        self.assertEqual(first.sha256, hashlib.sha256(b"").hexdigest())
        self.assertTrue(first.created)
        self.assertFalse(second.created)

    def test_invalid_expected_metadata_rejected_before_io_or_consumption(self):
        for options in ({"expected_size": True}, {"expected_size": -1}, {"expected_size": 1.0},
                        {"expected_sha256": "A" * 64}, {"expected_sha256": "../file"},
                        {"expected_sha256": "0" * 63}):
            source = io.BytesIO(self.data)
            with self.subTest(options=options), patch("hybrid_storage.artifacts._directory", side_effect=AssertionError("No I/O")):
                with self.assertRaises(ValueError):
                    self.registrar.register(source, **dict({"expected_size": len(self.data)}, **options))
            self.assertEqual(source.tell(), 0)

    def test_wrong_size_or_digest_leaves_no_object_or_private_file(self):
        for options in ({"expected_size": len(self.data) - 1}, {"expected_size": len(self.data) + 1},
                        {"expected_sha256": "0" * 64}):
            with self.subTest(options=options), self.assertRaises(ArtifactMismatch):
                self.register(**options)
            self.assert_no_objects()
            self.assertEqual(list(self.scratch.iterdir()), [])

    def test_nonbinary_or_unbounded_stream_response_is_rejected(self):
        for value in (None, "text", b"overlong"):
            class BadSource:
                def read(inner, count): return value
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.registrar.register(BadSource(), expected_size=0)
            self.assertEqual(list(self.scratch.iterdir()), [])
        self.assert_no_objects()

    def test_source_failure_after_first_byte_cleans_only_its_scratch(self):
        retained = self.scratch / "unrelated"
        retained.write_bytes(b"preserve")
        class FailingSource:
            started = False
            def read(inner, count):
                if inner.started:
                    raise OSError(errno.EIO, "source failed")
                inner.started = True
                return b"a"
        with self.assertRaises(OSError) as error:
            self.registrar.register(FailingSource(), expected_size=2)
        self.assertEqual(error.exception.errno, errno.EIO)
        self.assert_no_objects()
        self.assertEqual(list(self.scratch.iterdir()), [retained])
        self.assertEqual(retained.read_bytes(), b"preserve")

    def wrapped_output(self, transform):
        real = os.fdopen
        def open_output(fd, mode):
            stream = real(fd, mode)
            if mode != "w+b":
                return stream
            class Output:
                def __enter__(inner): return inner
                def __exit__(inner, *args): stream.close()
                def __getattr__(inner, name): return getattr(stream, name)
                def write(inner, data): return transform(stream, data)
            return Output()
        return open_output

    def test_copy_is_read_back_before_publication(self):
        def corrupt(stream, data): return stream.write(bytes([data[0] ^ 1]) + data[1:])
        with patch("hybrid_storage.artifacts.os.fdopen", side_effect=self.wrapped_output(corrupt)):
            with self.assertRaises(ArtifactMismatch):
                self.register()
        self.assert_no_objects()
        self.assertEqual(list(self.scratch.iterdir()), [])

    def test_full_disk_during_write_does_not_publish_partial_object(self):
        def full(stream, data): raise OSError(errno.ENOSPC, "simulated full disk")
        with patch("hybrid_storage.artifacts.os.fdopen", side_effect=self.wrapped_output(full)):
            with self.assertRaises(OSError) as error:
                self.register()
        self.assertEqual(error.exception.errno, errno.ENOSPC)
        self.assert_no_objects()
        self.assertEqual(list(self.scratch.iterdir()), [])

    def test_unprotected_directories_and_remote_admission_fail_before_source(self):
        for target, mode in ((self.root, 0o777), (self.scratch, 0o755)):
            original = stat.S_IMODE(target.stat().st_mode)
            target.chmod(mode)
            source = io.BytesIO(self.data)
            try:
                with self.assertRaises(PermissionError):
                    self.registrar.register(source, expected_size=len(self.data))
                self.assertEqual(source.tell(), 0)
            finally:
                target.chmod(original)
        source = io.BytesIO(self.data)
        with patch("hybrid_storage.artifacts.require_local_fd", side_effect=OSError(errno.EXDEV, "remote")):
            with self.assertRaises(OSError) as error:
                self.registrar.register(source, expected_size=len(self.data))
        self.assertEqual(error.exception.errno, errno.EXDEV)
        self.assertEqual(source.tell(), 0)
        self.assert_no_objects()

    def test_cross_filesystem_scratch_is_rejected_before_source(self):
        real = artifacts._protected_directory
        class DifferentDevice:
            st_mode = self.scratch.stat().st_mode
            st_dev = self.root.stat().st_dev + 1
        calls = 0
        def check(fd):
            nonlocal calls
            calls += 1
            return real(fd) if calls == 1 else DifferentDevice()
        source = io.BytesIO(self.data)
        with patch("hybrid_storage.artifacts._protected_directory", side_effect=check):
            with self.assertRaises(OSError) as error:
                self.registrar.register(source, expected_size=len(self.data))
        self.assertEqual(error.exception.errno, errno.EXDEV)
        self.assertEqual(source.tell(), 0)

    def test_roots_and_prefixes_reject_symlinks_and_unsafe_permissions(self):
        alias = self.base / "alias"
        alias.symlink_to(self.root, target_is_directory=True)
        with self.assertRaises(OSError):
            ArtifactRegistrar(alias, staging_root=self.scratch).register(io.BytesIO(self.data), expected_size=len(self.data))
        algorithm = self.root / "sha256"
        algorithm.symlink_to(self.base, target_is_directory=True)
        with self.assertRaises(OSError): self.register()
        algorithm.unlink()
        algorithm.mkdir(mode=0o755)
        prefix = algorithm / self.sha[:2]
        prefix.symlink_to(self.base, target_is_directory=True)
        with self.assertRaises(OSError): self.register()
        prefix.unlink()
        prefix.mkdir(mode=0o777)
        prefix.chmod(0o777)
        with self.assertRaises(PermissionError): self.register()
        self.assert_no_objects()
        self.assertEqual(list(self.scratch.iterdir()), [])

    def test_corrupt_or_writable_existing_digest_is_never_adopted_or_repaired(self):
        target = self.root / object_key(self.sha)
        target.parent.mkdir(parents=True)
        target.parent.parent.chmod(0o755)
        target.parent.chmod(0o755)
        for data, mode, exception in ((b"short", 0o444, ArtifactMismatch),
                                      (b"x" * len(self.data), 0o444, ArtifactMismatch),
                                      (self.data, 0o644, PermissionError)):
            target.chmod(0o600) if target.exists() else None
            target.write_bytes(data)
            target.chmod(mode)
            inode = target.stat().st_ino
            with self.subTest(mode=mode, data=data), self.assertRaises(exception): self.register()
            self.assertEqual(target.read_bytes(), data)
            self.assertEqual(target.stat().st_ino, inode)
        self.assertEqual(list(self.scratch.iterdir()), [])

    def test_existing_symlink_directory_or_fifo_cannot_be_deduplicated(self):
        target = self.root / object_key(self.sha)
        target.parent.mkdir(parents=True)
        target.parent.parent.chmod(0o755)
        target.parent.chmod(0o755)
        target.symlink_to(self.base / "outside")
        with self.assertRaises(OSError): self.register()
        target.unlink()
        target.mkdir()
        with self.assertRaises(ArtifactMismatch): self.register()
        target.rmdir()
        os.mkfifo(target)
        with self.assertRaises(ArtifactMismatch): self.register()
        self.assertTrue(stat.S_ISFIFO(target.stat().st_mode))
        self.assertEqual(list(self.scratch.iterdir()), [])

    def test_scratch_name_collision_does_not_delete_an_unowned_file(self):
        existing = self.scratch / ".registration-fixed"
        existing.write_bytes(b"another invocation")
        with patch("hybrid_storage.artifacts.secrets.token_hex", return_value="fixed"):
            with self.assertRaises(FileExistsError): self.register()
        self.assertEqual(existing.read_bytes(), b"another invocation")
        self.assert_no_objects()

    def test_unsupported_atomic_rename_has_no_overwriting_fallback(self):
        for code in (errno.ENOSYS, errno.EOPNOTSUPP, errno.EIO):
            with self.subTest(errno=code), \
                    patch("hybrid_storage.artifacts._rename_noreplace", side_effect=OSError(code, "injected")), \
                    patch("hybrid_storage.artifacts.os.replace", side_effect=AssertionError("No replace")), \
                    patch("hybrid_storage.artifacts.os.link", side_effect=AssertionError("No link fallback")):
                with self.assertRaises(OSError) as error: self.register()
                self.assertEqual(error.exception.errno, code)
            self.assert_no_objects()
            self.assertEqual(list(self.scratch.iterdir()), [])

    def test_threads_publish_one_complete_object_and_reuse_it(self):
        barrier = threading.Barrier(6, timeout=10)
        data = self.data
        class Source(io.BytesIO):
            def read(inner, count):
                if inner.tell() == 0: barrier.wait()
                return super().read(count)
        def register(_): return self.registrar.register(Source(data), expected_size=len(data))
        with ThreadPoolExecutor(max_workers=6) as pool:
            results = list(pool.map(register, range(6)))
        self.assertEqual(sum(result.created for result in results), 1)
        self.assertEqual(len({result.key for result in results}), 1)
        self.assertEqual((self.root / results[0].key).read_bytes(), data)
        self.assertEqual(len(list(self.root.glob("sha256/*/*"))), 1)
        self.assertEqual(list(self.scratch.iterdir()), [])

    def child(self, body, *args):
        script = ("import io,json,os,sys\n"
                  f"sys.path.insert(0,{str(ROOT)!r})\n"
                  "from pathlib import Path\nfrom dataclasses import asdict\n"
                  "from hybrid_storage import artifacts\n"
                  "registrar=artifacts.ArtifactRegistrar(Path(sys.argv[1]),staging_root=Path(sys.argv[2]))\n"
                  f"data={self.data!r}\n" + body)
        return subprocess.Popen([sys.executable, "-B", "-c", script, str(self.root), str(self.scratch), *args],
                                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    def test_independent_processes_deduplicate_without_python_locks(self):
        body = "sys.stdin.read(1)\nprint(json.dumps(asdict(registrar.register(io.BytesIO(data),expected_size=len(data)))))\n"
        children = [self.child(body) for _ in range(3)]
        try:
            for child in children:
                child.stdin.write("x")
                child.stdin.flush()
            results = []
            for child in children:
                output, errors = child.communicate(timeout=15)
                self.assertEqual(child.returncode, 0, errors)
                results.append(json.loads(output))
            self.assertEqual(sum(result["created"] for result in results), 1)
            self.assertEqual((self.root / object_key(self.sha)).read_bytes(), self.data)
            self.assertEqual(list(self.scratch.iterdir()), [])
        finally:
            for child in children:
                if child.poll() is None: child.kill()
                child.communicate(timeout=10)

    def test_process_crash_before_rename_leaves_private_orphan_not_an_object(self):
        body = ("def crash(*args): os._exit(71)\n"
                "artifacts._rename_noreplace=crash\n"
                "registrar.register(io.BytesIO(data),expected_size=len(data))\n")
        child = self.child(body)
        output, errors = child.communicate(timeout=15)
        self.assertEqual(child.returncode, 71, errors)
        self.assert_no_objects()
        orphans = list(self.scratch.iterdir())
        self.assertEqual(len(orphans), 1)
        self.assertEqual(stat.S_IMODE(orphans[0].stat().st_mode), 0o444)
        self.assertTrue(self.register().created)
        self.assertEqual(list(self.scratch.iterdir()), orphans)  # Never sweep others' scratch.

    def test_process_crash_after_rename_reconciles_by_verified_deduplication(self):
        body = ("original=artifacts._rename_noreplace\n"
                "def crash(*args):\n original(*args)\n os._exit(72)\n"
                "artifacts._rename_noreplace=crash\n"
                "registrar.register(io.BytesIO(data),expected_size=len(data))\n")
        child = self.child(body)
        output, errors = child.communicate(timeout=15)
        self.assertEqual(child.returncode, 72, errors)
        target = self.root / object_key(self.sha)
        inode = target.stat().st_ino
        self.assertEqual(target.read_bytes(), self.data)
        self.assertFalse(self.register().created)
        self.assertEqual(target.stat().st_ino, inode)
        self.assertEqual(list(self.scratch.iterdir()), [])

    def test_post_rename_fsync_error_preserves_complete_object_for_retry(self):
        real_rename, real_sync = artifacts._rename_noreplace, os.fsync
        renamed = False
        failed = False
        def rename(*args):
            nonlocal renamed
            real_rename(*args)
            renamed = True
        def sync(fd):
            nonlocal failed
            if renamed and not failed:
                failed = True
                raise OSError(errno.EIO, "destination directory sync failed")
            return real_sync(fd)
        with patch("hybrid_storage.artifacts._rename_noreplace", side_effect=rename), \
                patch("hybrid_storage.artifacts.os.fsync", side_effect=sync):
            with self.assertRaises(OSError): self.register()
        self.assertEqual((self.root / object_key(self.sha)).read_bytes(), self.data)
        self.assertFalse(self.register().created)
        self.assertEqual(list(self.scratch.iterdir()), [])

    def test_simulated_hash_collision_still_rejects_different_bytes(self):
        sha = self.sha
        class CollisionDigest:
            def update(inner, data): pass
            def hexdigest(inner): return sha
        with patch("hybrid_storage.artifacts.hashlib.sha256", side_effect=CollisionDigest):
            first = self.register()
            with self.assertRaises(ArtifactMismatch): self.register(b"x" * len(self.data))
        self.assertEqual((self.root / first.key).read_bytes(), self.data)
        self.assertEqual(list(self.scratch.iterdir()), [])

    def test_constructor_and_import_have_no_filesystem_db_or_config_side_effects(self):
        missing = self.base / "missing"
        ArtifactRegistrar(missing, staging_root=self.base / "other-missing")
        self.assertFalse(missing.exists())
        for root, scratch in ((self.root, self.root), (self.root, self.root / "scratch"),
                              (Path("relative"), self.scratch), (Path("/"), self.scratch)):
            with self.assertRaises(ValueError): ArtifactRegistrar(root, staging_root=scratch)
        script = (f"import sys; sys.path.insert(0,{str(ROOT)!r}); "
                  "import hybrid_storage.artifacts; "
                  "assert 'controller.job_store' not in sys.modules; "
                  "assert 'hybrid_storage.config' not in sys.modules; "
                  "assert 'sqlite3' not in sys.modules")
        result = subprocess.run([sys.executable, "-B", "-c", script], capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main(verbosity=2)
