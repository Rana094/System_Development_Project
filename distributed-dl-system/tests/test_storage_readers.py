"""Concurrent, read-failure and shipped-config acceptance; no live NFS or DB."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import errno
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from hybrid_storage.backends import LegacyStorageBackend, NFSStorageBackend
from hybrid_storage.health import ProbeRequest, verify_request
from hybrid_storage.inputs import InputFile, NFSObjectStorageBackend, ProjectInputView, object_key
from hybrid_storage.nfs_mount import MountNotReady, require_input_mount

SOURCE = "192.0.2.1:/srv/ml-shared/objects"  # Documentation-only address.


class ReaderTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="storage-readers-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name) / "objects"
        self.root.mkdir()
        self.data = bytes(range(256)) * 8192
        self.sha = self.store(self.data)
        self.backend = NFSObjectStorageBackend(self.root, source=SOURCE)

    def store(self, data):
        sha = hashlib.sha256(data).hexdigest()
        target = self.root / object_key(sha)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
        return sha

    def request(self, sha=None, size=None):
        return ProbeRequest(role="nfs", root=str(self.root), sha256=sha or self.sha,
                            size_bytes=len(self.data) if size is None else size, source=SOURCE)

    @contextmanager
    def fake_mount(self):
        # Exercise the real metadata parser, but explicitly fake kernel identity.
        row = ("1 0 0:1 / / rw - ext4 /dev/fake rw\n"
               f"42 1 0:2 / {self.root} ro,nosuid,nodev,noexec - nfs4 {SOURCE} "
               "rw,vers=4.1,hard,proto=tcp,sec=sys\n")
        def admit(source, *, target):
            return require_input_mount(source, row, target=target)
        with patch("hybrid_storage.inputs.require_input_mount", side_effect=admit), \
                patch("hybrid_storage.inputs.fd_mount_id", return_value=42):
            yield

    def test_shared_backend_readers_have_independent_offsets_and_close_descriptors(self):
        barrier = threading.Barrier(6, timeout=10)
        def read(index):
            with self.backend.open_artifact(object_key(self.sha)) as stream:
                prefix = stream.read(index + 1)
                barrier.wait()  # Every descriptor remains open simultaneously.
                self.assertEqual(stream.tell(), index + 1)
                data = prefix + stream.read()
                self.assertFalse(stream.writable())
                fd = stream.fileno()
                barrier.wait()
            return data, fd, stream.closed
        with self.fake_mount(), ThreadPoolExecutor(max_workers=6) as pool:
            results = list(pool.map(read, range(6)))
        self.assertEqual(len({fd for _, fd, _ in results}), 6)
        self.assertTrue(all(data == self.data and closed for data, _, closed in results))
        self.assertEqual((self.root / object_key(self.sha)).read_bytes(), self.data)

    def test_parallel_project_views_keep_same_path_bound_to_their_own_grants(self):
        other = b"different authorized project"
        other_sha = self.store(other)
        views = [ProjectInputView(self.backend, [InputFile("train.py", sha, len(data)),
                    InputFile(f"owner-{index}.txt", sha, len(data))], objects=True)
                 for index, (sha, data) in enumerate(((self.sha, self.data), (other_sha, other)))]
        barrier = threading.Barrier(2, timeout=10)
        def read(index):
            view = views[index]
            barrier.wait()
            for denied in (f"owner-{1-index}.txt", object_key(other_sha), "unlisted.txt"):
                # Denial must occur before invoking even the underlying adapter.
                with self.assertRaises(PermissionError) as raised:
                    with view.open_artifact(denied):
                        self.fail("Unlisted input opened")
                self.assertEqual(raised.exception.errno, errno.EACCES)
            with view.open_artifact("train.py") as stream:
                barrier.wait()
                return stream.read()
        original = self.backend.open_artifact
        with self.fake_mount(), patch.object(self.backend, "open_artifact", wraps=original) as opened, \
                ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(read, range(2)))
        self.assertEqual(results, [self.data, other])
        self.assertEqual(opened.call_count, 2)
        # These are trusted caller grants, not a replacement for owner authentication.

    def test_concurrent_integrity_checks_separate_ready_corrupt_and_missing(self):
        corrupt_sha = self.store(b"correct")
        (self.root / object_key(corrupt_sha)).write_bytes(b"changed")
        missing_sha = hashlib.sha256(b"not stored").hexdigest()
        requests = [self.request(), self.request(corrupt_sha, 7), self.request(missing_sha, 10)]
        barrier = threading.Barrier(3, timeout=10)
        def check(request):
            barrier.wait()
            return verify_request(request)
        with self.fake_mount(), ThreadPoolExecutor(max_workers=3) as pool:
            results = list(pool.map(check, requests))
        self.assertEqual([result["status"] for result in results], ["ready", "corrupt", "source_missing"])
        self.assertEqual(results[0]["sha256"], self.sha)
        self.assertEqual(results[2]["errno"], errno.ENOENT)

    def test_mount_replacement_fences_all_open_readers_and_closes_them(self):
        opened = threading.Barrier(4, timeout=10)
        replaced = threading.Event()
        streams = []
        def admit(*args, **kwargs):
            return {"mount_id": 43 if replaced.is_set() else 42}
        def read(_):
            with self.assertRaises(MountNotReady):
                with self.backend.open_artifact(object_key(self.sha)) as stream:
                    streams.append(stream)
                    self.assertEqual(stream.read(1), self.data[:1])
                    opened.wait()
                    self.assertTrue(replaced.wait(10))
            return stream.closed
        with patch("hybrid_storage.inputs.require_input_mount", side_effect=admit), \
                patch("hybrid_storage.inputs.fd_mount_id", return_value=42), \
                ThreadPoolExecutor(max_workers=3) as pool:
            futures = [pool.submit(read, index) for index in range(3)]
            try:
                opened.wait()
            finally:
                replaced.set()
            self.assertTrue(all(future.result(timeout=10) for future in futures))
        self.assertEqual(len(streams), 3)
        self.assertTrue(all(stream.closed for stream in streams))

    def test_errors_after_partial_read_are_not_ready_and_release_descriptor(self):
        original = NFSStorageBackend.open_artifact
        for code, status in ((errno.EACCES, "permission_denied"), (errno.EPERM, "permission_denied"),
                             (errno.ESTALE, "io_error"), (errno.ETIMEDOUT, "io_error"),
                             (errno.EIO, "io_error")):
            streams = []
            @contextmanager
            def failing(backend, key):
                with original(backend, key) as stream:
                    streams.append(stream)
                    class Partial:
                        started = False
                        def fileno(inner): return stream.fileno()
                        def read(inner, count):
                            if inner.started:
                                raise OSError(code, "simulated failure after first byte")
                            inner.started = True
                            return stream.read(min(count, 1))
                    yield Partial()
            with self.subTest(errno=code), self.fake_mount(), \
                    patch.object(NFSStorageBackend, "open_artifact", failing):
                self.assertEqual(verify_request(self.request()),
                                 {"ready": False, "status": status, "errno": code})
            self.assertEqual(len(streams), 1)
            self.assertTrue(streams[0].closed)
        with self.fake_mount():
            self.assertTrue(verify_request(self.request())["ready"])

    def test_legacy_concurrent_project_reads_never_consult_nfs_admission(self):
        project = self.root.parent / "legacy"
        project.mkdir()
        (project / "train.py").write_bytes(self.data)
        view = ProjectInputView(LegacyStorageBackend(project), [InputFile("train.py", self.sha, len(self.data))])
        barrier = threading.Barrier(3, timeout=10)
        def read(_):
            with view.open_artifact("train.py") as stream:
                barrier.wait()
                return stream.read()
        with patch("hybrid_storage.inputs.require_input_mount", side_effect=AssertionError("Legacy NFS lookup")), \
                patch("hybrid_storage.inputs.fd_mount_id", side_effect=AssertionError("Legacy NFS identity")), \
                ThreadPoolExecutor(max_workers=3) as pool:
            self.assertEqual(list(pool.map(read, range(3))), [self.data] * 3)
        self.assertEqual((project / "train.py").read_bytes(), self.data)


class ConfigExampleTests(unittest.TestCase):
    def report(self, name):
        result = subprocess.run([sys.executable, "-B", "-m", "hybrid_storage.config", "--config",
                                 str(ROOT / "config/storage-examples" / name)],
                                cwd=ROOT, capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stderr, "")
        return json.loads(result.stdout)

    def test_shipped_legacy_example_matches_previous_runtime_contract(self):
        self.assertEqual(self.report("legacy.json"), {"runtime": "legacy", "settings": {
            "storage_backend": "legacy", "worker_cache_enabled": False,
            "storage_aware_staging": False, "scheduler_policy": "resource"}})

    def test_shipped_nfs_example_explicitly_reports_execution_blocked(self):
        self.assertEqual(self.report("nfs-held.json"), {
            "runtime": "legacy", "new_job_default": "nfs", "nfs_execution": "blocked", "settings": {
                "storage_backend": "nfs", "worker_cache_enabled": False,
                "storage_aware_staging": True, "scheduler_policy": "resource"}})


if __name__ == "__main__":
    unittest.main(verbosity=2)
