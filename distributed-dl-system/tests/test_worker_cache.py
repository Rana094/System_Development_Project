"""Phase 5.1: disposable local cache persistence/integrity; no agent import/NFS."""

from concurrent.futures import ThreadPoolExecutor
import errno
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from agent.cache_storage import local_cache
from hybrid_storage import worker_cache as cache_module
from hybrid_storage.config import StorageConfig, StorageConfigError, require_controller_config
from hybrid_storage.inputs import object_key
from hybrid_storage.worker_cache import CacheCorrupt, CacheError, CacheMiss, WorkerCache


class WorkerCacheTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="worker-cache-5-1-")
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name)
        self.root = self.base / "cache"
        self.root.mkdir(mode=0o700)
        self.mount = self.base / "not-contacted-nfs"
        self.cache = self.handle()
        self.data = b"authorized local test input\n"
        self.sha = hashlib.sha256(self.data).hexdigest()

    def handle(self, **options):
        return local_cache(self.root, input_mount=self.mount, **options)

    def install(self, data=None):
        """Test fixture, not a publication API. 5.2 must supply safe staging."""
        data = self.data if data is None else data
        sha = hashlib.sha256(data).hexdigest()
        target = self.root / "objects" / object_key(sha)
        target.parent.parent.mkdir(mode=0o700, exist_ok=True)
        target.parent.mkdir(mode=0o700, exist_ok=True)
        target.write_bytes(data)
        target.chmod(0o400)
        return target, sha

    def ready(self):
        self.cache.initialize()
        return self.install()[0]

    def row(self):
        return self.cache.inventory()[0]

    def test_construction_and_import_have_no_filesystem_or_database_effect(self):
        missing = self.base / "absent"
        with patch.object(cache_module, "_directory", side_effect=AssertionError("no I/O")):
            handle = local_cache(missing, input_mount=self.mount)
        self.assertEqual(handle.root, missing)
        self.assertFalse(missing.exists())
        result = subprocess.run([sys.executable, "-c",
            "import sys; import agent.cache_storage; "
            "assert 'job_store' not in sys.modules; assert 'agent.agent' not in sys.modules"],
            cwd=ROOT, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_explicit_initialization_is_idempotent_and_retains_unrelated_files(self):
        retained = self.root / "retain.txt"
        retained.write_text("keep")
        self.cache.initialize()
        self.cache.initialize()
        self.assertEqual(self.cache.inventory(), ())
        self.assertEqual(retained.read_text(), "keep")
        self.assertEqual(set(path.name for path in self.root.iterdir()), {"objects", "state", "retain.txt"})

    def test_root_must_be_provisioned_never_implicitly_created(self):
        with self.assertRaises(FileNotFoundError):
            local_cache(self.base / "missing", input_mount=self.mount).initialize()
        self.assertFalse((self.base / "missing").exists())

    def test_invalid_root_overlap_and_age_rejected_before_io(self):
        for root in (Path("relative"), Path("/"), self.mount, self.mount / "child", self.base):
            with self.subTest(root=root), self.assertRaises(ValueError):
                local_cache(root, input_mount=self.mount)
        for value in (True, -1, float("inf"), float("nan"), "300", 365 * 86400 + 1):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.handle(verify_after_seconds=value)

    def test_symlink_root_or_ancestor_rejected(self):
        link = self.base / "link"
        link.symlink_to(self.root, target_is_directory=True)
        for root in (link, link / "child"):
            with self.subTest(root=root), self.assertRaises(OSError):
                local_cache(root, input_mount=self.mount).initialize()
        self.assertEqual(list(self.root.iterdir()), [])

    def test_remote_or_unknown_filesystem_fails_before_creating_state(self):
        with patch.object(cache_module, "require_local_fd", side_effect=OSError(errno.EXDEV, "NFS")):
            with self.assertRaises(OSError) as error:
                self.cache.initialize()
        self.assertEqual(error.exception.errno, errno.EXDEV)
        self.assertEqual(list(self.root.iterdir()), [])

    def test_public_or_wrong_owner_root_rejected(self):
        self.root.chmod(0o755)
        with self.assertRaises(PermissionError):
            self.cache.initialize()
        self.root.chmod(0o700)
        with patch.object(cache_module.os, "geteuid", return_value=os.geteuid() + 1):
            with self.assertRaises(PermissionError):
                self.cache.initialize()
        self.assertEqual(list(self.root.iterdir()), [])

    def test_symlink_state_directory_and_database_rejected_without_touching_target(self):
        other = self.base / "other"
        other.mkdir(mode=0o700)
        (self.root / "state").symlink_to(other, target_is_directory=True)
        with self.assertRaises(OSError):
            self.cache.initialize()
        self.assertEqual(list(other.iterdir()), [])
        (self.root / "state").unlink()  # Only our disposable malicious fixture.
        self.cache.initialize()
        db = self.root / "state/cache.sqlite3"
        db.unlink()  # Replace only the disposable test DB with a link fixture.
        target = other / "retained"
        target.write_bytes(b"preserve")
        db.symlink_to(target)
        with self.assertRaises(OSError):
            self.cache.initialize()
        self.assertEqual(target.read_bytes(), b"preserve")

    def test_unsafe_sqlite_sidecar_rejected_without_target_mutation(self):
        self.cache.initialize()
        target = self.base / "retained"
        target.write_bytes(b"preserve")
        (self.root / "state/cache.sqlite3-journal").symlink_to(target)
        with self.assertRaises(OSError):
            self.cache.inventory()
        self.assertEqual(target.read_bytes(), b"preserve")

    def test_future_or_foreign_schema_refused_and_not_reset(self):
        self.cache.initialize()
        db_path = self.root / "state/cache.sqlite3"
        for sql in ("PRAGMA user_version=2", "CREATE TABLE unrelated (data TEXT)"):
            with sqlite3.connect(db_path) as db:
                db.execute(sql)
            before = db_path.read_bytes()
            with self.assertRaises(CacheError):
                self.cache.initialize()
            self.assertEqual(db_path.read_bytes(), before)

    def test_filename_existence_is_not_integrity_or_inventory(self):
        target = self.ready()
        self.assertEqual(self.cache.inventory(), ())
        self.cache.verify(self.sha, len(self.data))
        self.assertEqual(self.row().state, "verified")
        self.assertEqual(self.row().size_bytes, len(self.data))
        self.assertIsNotNone(self.row().last_verified_ns)
        self.assertIsNone(self.row().last_used_ns)
        self.assertEqual(target.read_bytes(), self.data)

    def test_same_name_wrong_bytes_never_accepted(self):
        target = self.ready()
        target.chmod(0o600)
        target.write_bytes(b"X" * len(self.data))
        target.chmod(0o400)
        with self.assertRaises(CacheCorrupt):
            self.cache.verify(self.sha, len(self.data))
        self.assertEqual(self.row().state, "corrupt")
        self.assertIsNone(self.row().last_verified_ns)
        self.assertTrue(target.exists())

    def test_missing_complete_file_or_part_file_never_accepted(self):
        self.cache.initialize()
        (self.root / "objects" / (self.sha + ".part")).write_bytes(self.data)
        with self.assertRaises(CacheMiss):
            self.cache.verify(self.sha, len(self.data))
        self.assertEqual(self.row().state, "missing")
        self.assertIsNone(self.row().last_verified_ns)

    def test_wrong_size_rejected_and_conflicting_registry_size_not_reinterpreted(self):
        self.ready()
        with self.assertRaises(CacheCorrupt):
            self.cache.verify(self.sha, len(self.data) + 1)
        with self.assertRaises(CacheError):
            self.cache.verify(self.sha, len(self.data))
        self.assertEqual(self.row().size_bytes, len(self.data) + 1)

    def test_invalid_digest_sizes_and_identifiers_rejected_before_io(self):
        with patch.object(cache_module, "_directory", side_effect=AssertionError("no I/O")):
            for sha, size in (("../bad", 0), ("A" * 64, 0), (self.sha, True),
                              (self.sha, -1), (self.sha, 2**63)):
                with self.subTest(sha=sha, size=size), self.assertRaises(ValueError):
                    self.cache.verify(sha, size)
            for value in ("../escape", "..", "", "a/attempt", "a" * 129):
                with self.subTest(value=value), self.assertRaises(ValueError):
                    self.cache.pin(self.sha, len(self.data), job_id=value, attempt_id="try1")

    def test_staging_always_hashes_even_with_fresh_unchanged_metadata(self):
        self.ready()
        self.cache.verify(self.sha, len(self.data))
        with patch.object(cache_module.hashlib, "sha256", wraps=hashlib.sha256) as hasher:
            with self.cache.open_verified(self.sha, len(self.data)) as stream:
                self.assertEqual(stream.tell(), 0)
                self.assertEqual(stream.read(), self.data)
            self.assertEqual(hasher.call_count, 1)
        self.assertIsNotNone(self.row().last_used_ns)

    def test_recent_metadata_can_skip_hash_only_with_explicit_nonstaging_request(self):
        self.ready()
        first = self.cache.verify(self.sha, len(self.data))
        with patch.object(cache_module.hashlib, "sha256", side_effect=AssertionError("recent")):
            second = self.cache.verify(self.sha, len(self.data), force=False)
        self.assertEqual(first, second)

    def test_stale_metadata_rehashed_and_corruption_invalidated(self):
        target = self.ready()
        self.cache.verify(self.sha, len(self.data))
        stale = self.handle(verify_after_seconds=0)
        with patch.object(cache_module.hashlib, "sha256", wraps=hashlib.sha256) as hasher:
            stale.verify(self.sha, len(self.data), force=False)
            self.assertEqual(hasher.call_count, 1)
        target.chmod(0o600)
        target.write_bytes(b"X" * len(self.data))
        target.chmod(0o400)
        with self.assertRaises(CacheCorrupt):
            self.cache.verify(self.sha, len(self.data), force=False)
        self.assertEqual(self.row().state, "corrupt")

    def test_clock_rollback_rehashes_instead_of_extending_trust(self):
        self.ready()
        previous = self.cache.verify(self.sha, len(self.data))
        with patch.object(cache_module.time, "time_ns", return_value=previous.last_verified_ns - 100), \
                patch.object(cache_module.hashlib, "sha256", wraps=hashlib.sha256) as hasher:
            self.cache.verify(self.sha, len(self.data), force=False)
        self.assertEqual(hasher.call_count, 1)

    def test_removed_object_invalidates_persisted_observation(self):
        target = self.ready()
        self.cache.verify(self.sha, len(self.data))
        target.unlink()
        with self.assertRaises(CacheMiss):
            with self.cache.open_verified(self.sha, len(self.data)):
                self.fail("missing input yielded")
        self.assertEqual(self.row().state, "missing")

    def test_symlink_writable_fifo_and_hardlinked_objects_fail_closed(self):
        target = self.ready()
        target.chmod(0o600)
        with self.assertRaises(PermissionError):
            self.cache.verify(self.sha, len(self.data))
        target.chmod(0o400)
        os.link(target, self.base / "hardlink")
        with self.assertRaises(PermissionError):
            self.cache.verify(self.sha, len(self.data))
        target.unlink()
        target.symlink_to(self.base / "hardlink")
        with self.assertRaises(OSError):
            self.cache.verify(self.sha, len(self.data))
        target.unlink()
        os.mkfifo(target)
        with self.assertRaises(PermissionError):
            self.cache.verify(self.sha, len(self.data))

    def test_read_errors_propagate_and_invalidate_previous_observation(self):
        self.ready()
        self.cache.verify(self.sha, len(self.data))
        with patch.object(cache_module.WorkerCache, "_check", side_effect=OSError(errno.EIO, "read failed")):
            with self.assertRaises(OSError) as error:
                self.cache.verify(self.sha, len(self.data))
        self.assertEqual(error.exception.errno, errno.EIO)
        self.assertEqual(self.row().state, "corrupt")

    def test_pins_are_exact_attempt_idempotent_and_release_is_nondestructive(self):
        target = self.ready()
        for job, attempt in (("job1", "try1"), ("job1", "try1"), ("job1", "try2"), ("job2", "try1")):
            self.cache.pin(self.sha, len(self.data), job_id=job, attempt_id=attempt)
        self.assertEqual(self.row().pinned_references, (("job1", "try1"), ("job1", "try2"), ("job2", "try1")))
        self.assertEqual(self.cache.unpin(job_id="job1", attempt_id="try1"), 1)
        self.assertEqual(self.cache.unpin(job_id="job1", attempt_id="try1"), 0)
        self.assertEqual(len(self.row().pinned_references), 2)
        self.assertEqual(target.read_bytes(), self.data)

    def test_failed_pin_does_not_create_reference_and_corruption_keeps_old_pins(self):
        target = self.ready()
        self.cache.pin(self.sha, len(self.data), job_id="job1", attempt_id="try1")
        target.chmod(0o600)
        target.write_bytes(b"bad")
        target.chmod(0o400)
        with self.assertRaises(CacheMiss):
            self.cache.pin(self.sha, len(self.data), job_id="job2", attempt_id="try2")
        self.assertEqual(self.row().pinned_references, (("job1", "try1"),))
        self.assertEqual(self.row().state, "corrupt")

    def test_cache_and_pins_survive_fresh_python_process_restart(self):
        self.ready()
        self.cache.pin(self.sha, len(self.data), job_id="job1", attempt_id="try1")
        before = self.row()
        program = """
import json, sys
from pathlib import Path
from agent.cache_storage import local_cache
c = local_cache(Path(sys.argv[1]), input_mount=Path(sys.argv[2]))
c.initialize()
assert c.inventory()[0].pinned_references == (('job1','try1'),)
with c.open_verified(sys.argv[3], int(sys.argv[4])) as f:
    print(json.dumps({'bytes': f.read().decode(), 'pins': c.inventory()[0].pinned_references}))
"""
        result = subprocess.run([sys.executable, "-c", program, str(self.root), str(self.mount),
                                 self.sha, str(len(self.data))], cwd=ROOT, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)["bytes"], self.data.decode())
        self.assertEqual(self.handle().inventory()[0].pinned_references, before.pinned_references)

    def test_threaded_independent_handles_serialize_without_lost_pins(self):
        self.ready()
        def pin(index):
            handle = self.handle()
            handle.initialize()
            handle.pin(self.sha, len(self.data), job_id="job", attempt_id=f"try{index}")
        with ThreadPoolExecutor(max_workers=4) as executor:
            list(executor.map(pin, range(12)))
        self.assertEqual(len(self.row().pinned_references), 12)

    def test_separate_processes_serialize_and_preserve_each_others_pins(self):
        self.ready()
        program = """
import sys
from pathlib import Path
from agent.cache_storage import local_cache
c = local_cache(Path(sys.argv[1]), input_mount=Path(sys.argv[2]))
c.initialize()
c.pin(sys.argv[3], int(sys.argv[4]), job_id='job', attempt_id=sys.argv[5])
"""
        processes = [subprocess.Popen([sys.executable, "-c", program, str(self.root), str(self.mount),
                     self.sha, str(len(self.data)), f"try{index}"], cwd=ROOT,
                     stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) for index in range(4)]
        for process in processes:
            _, error = process.communicate(timeout=20)
            self.assertEqual(process.returncode, 0, error)
        self.assertEqual(len(self.row().pinned_references), 4)

    def test_busy_metadata_has_bounded_lock_wait_and_no_pin_change(self):
        self.ready()
        lock = os.open(self.root / "state/cache.lock", os.O_RDWR)
        self.addCleanup(os.close, lock)
        cache_module.fcntl.flock(lock, cache_module.fcntl.LOCK_EX)
        with patch.object(cache_module.time, "monotonic", side_effect=(0, 11)):
            with self.assertRaises(TimeoutError):
                self.cache.pin(self.sha, len(self.data), job_id="job", attempt_id="try1")
        cache_module.fcntl.flock(lock, cache_module.fcntl.LOCK_UN)
        self.assertEqual(self.cache.inventory(), ())

    def test_interrupted_uncommitted_pin_transaction_rolls_back_on_restart(self):
        self.ready()
        self.cache.pin(self.sha, len(self.data), job_id="job", attempt_id="committed")
        program = """
import os, sqlite3, sys
db = sqlite3.connect(sys.argv[1])
db.execute('PRAGMA synchronous=FULL')
db.execute('BEGIN IMMEDIATE')
db.execute('DELETE FROM cache_pins')
os._exit(17)
"""
        result = subprocess.run([sys.executable, "-c", program,
                                 str(self.root / "state/cache.sqlite3")], capture_output=True, text=True)
        self.assertEqual(result.returncode, 17)
        self.handle().initialize()
        self.assertEqual(self.row().pinned_references, (("job", "committed"),))

    def test_empty_input_is_valid_and_large_hash_reads_are_bounded(self):
        self.cache.initialize()
        _, empty_sha = self.install(b"")
        self.cache.verify(empty_sha, 0)
        data = b"x" * (cache_module.CHUNK_BYTES * 3 + 7)
        _, sha = self.install(data)
        counts = []
        original = cache_module.os.fdopen
        def wrapped(fd, mode):
            stream = original(fd, mode)
            class Reader:
                def __enter__(self): return self
                def __exit__(self, *args): stream.close()
                def __getattr__(self, name): return getattr(stream, name)
                def read(self, count):
                    counts.append(count)
                    return stream.read(count)
            return Reader()
        with patch.object(cache_module.os, "fdopen", side_effect=wrapped):
            self.cache.verify(sha, len(data))
        self.assertTrue(counts)
        self.assertTrue(all(0 < count <= cache_module.CHUNK_BYTES for count in counts))

    def test_object_mutation_during_hash_is_not_accepted(self):
        target = self.ready()
        original = cache_module.hashlib.sha256
        def hasher():
            target.chmod(0o600)
            target.write_bytes(b"X" * len(self.data))
            target.chmod(0o400)
            return original()
        with patch.object(cache_module.hashlib, "sha256", side_effect=hasher):
            with self.assertRaises(CacheCorrupt):
                self.cache.verify(self.sha, len(self.data))
        self.assertEqual(self.row().state, "corrupt")

    def test_disk_space_is_current_service_available_not_a_capacity_reservation(self):
        self.ready()
        self.cache.verify(self.sha, len(self.data))
        values = os.statvfs_result((4096, 4096, 100, 30, 20, 10, 8, 8, 0, 255))
        with patch.object(cache_module.os, "fstatvfs", return_value=values):
            space = self.cache.space()
        self.assertEqual((space.total_bytes, space.free_bytes, space.available_bytes),
                         (409600, 122880, 81920))
        self.assertEqual(space.observed_verified_bytes, len(self.data))

    def test_configuration_still_gates_cache_execution_and_has_unchanged_defaults(self):
        self.assertEqual(require_controller_config(StorageConfig()), StorageConfig())
        with self.assertRaises(StorageConfigError):
            require_controller_config(StorageConfig(storage_backend="nfs", storage_aware_staging=True,
                                                   worker_cache_enabled=True))


if __name__ == "__main__":
    unittest.main(verbosity=2)
