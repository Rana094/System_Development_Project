"""5.2 fake-NFS/local integration. Helpers really copy; mount facts are mocked."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import errno
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from agent.cache_storage import local_cache, nfs_cache_fetcher
from hybrid_storage import cache_fetch, cache_copy_worker
from hybrid_storage.cache_fetch import (CacheCapacityError, CacheFetchBusy, CacheFetcher,
                                       CacheFetchTimeout)
from hybrid_storage.inputs import InputFile, object_key
from hybrid_storage.nfs_mount import MountNotReady
from hybrid_storage.worker_cache import CacheCorrupt, CacheError

SOURCE = "192.0.2.10:/srv/ml-shared/objects"  # RFC 5737 fixture, never contacted.
FAKE_HELPER = """
import json, sys
from unittest.mock import patch
sys.path.insert(0, sys.argv[1])
from hybrid_storage.cache_copy_worker import copy_request
with patch('hybrid_storage.inputs.require_input_mount', return_value={'mount_id': 42}), \
     patch('hybrid_storage.inputs.fd_mount_id', return_value=42):
    print(json.dumps(copy_request(**json.loads(sys.argv[2]))), flush=True)
"""


class FetchTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="cache-fetch-5-2-")
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name)
        self.root, self.nfs = self.base / "cache", self.base / "fake-nfs"
        self.root.mkdir(mode=0o700)
        self.nfs.mkdir()
        self.cache = local_cache(self.root, input_mount=self.nfs)
        self.fetcher = self.fetcher_handle()
        self.fetcher.initialize()
        self.data = b"authorized NFS input fixture\n"
        self.sha = hashlib.sha256(self.data).hexdigest()
        self.input = InputFile("data/rows.csv", self.sha, len(self.data))
        self.target = self.root / "objects" / object_key(self.sha)
        self.source_file(self.data)

    def fetcher_handle(self, **options):
        handle = nfs_cache_fetcher(self.cache, source=SOURCE,
            **dict({"quota_bytes": 1024 * 1024 * 16, "reserve_free_bytes": 0,
                    "retry_delay": 0, "io_timeout": 3}, **options))
        self.addCleanup(handle.close)
        return handle

    def source_file(self, data):
        sha = hashlib.sha256(data).hexdigest()
        path = self.nfs / object_key(sha)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        return path, sha

    def fake_spawn(self, request, output, lock):
        return subprocess.Popen([sys.executable, "-I", "-B", "-c", FAKE_HELPER, str(ROOT), json.dumps(request)],
            stdin=subprocess.DEVNULL, stdout=output, stderr=subprocess.DEVNULL,
            pass_fds=(request["output_fd"], lock))

    def ensure(self, files=None, handle=None, authorize=None):
        return (handle or self.fetcher).ensure(files or [self.input], authorize=authorize or (lambda inputs: True))

    def fake_nfs(self, handle=None):
        return patch.object(handle or self.fetcher, "_spawn", side_effect=self.fake_spawn)

    def assert_no_partial_or_final(self):
        self.assertFalse(self.target.exists())
        self.assertEqual(list((self.root / "parts").iterdir()), [])

    def test_cold_copy_publishes_verified_readonly_object_and_warm_repeat_reads_zero_nfs(self):
        with self.fake_nfs() as spawned:
            cold = self.ensure()[0]
            warm = self.ensure()[0]
        self.assertFalse(cold.cache_hit)
        self.assertEqual(cold.attempts, 1)
        self.assertTrue(warm.cache_hit)
        self.assertEqual(warm.attempts, 0)
        self.assertEqual(spawned.call_count, 1)
        self.assertEqual(self.target.read_bytes(), self.data)
        self.assertEqual(self.target.stat().st_mode & 0o777, 0o400)
        self.assertEqual(self.cache.inventory()[0].state, "verified")
        self.assertEqual(list((self.root / "parts").iterdir()), [])

    def test_warm_cache_remains_usable_when_nfs_source_disappears(self):
        with self.fake_nfs():
            self.ensure()
        (self.nfs / object_key(self.sha)).unlink()
        with patch.object(self.fetcher, "_spawn", side_effect=AssertionError("no NFS on warm hit")):
            self.assertTrue(self.ensure()[0].cache_hit)

    def test_duplicate_digests_fetched_once_and_only_actual_missing_files_read(self):
        more, sha = self.source_file(b"second input")
        inputs = [self.input, InputFile("alias.csv", self.sha, len(self.data)),
                  InputFile("source/train.py", sha, more.stat().st_size)]
        with self.fake_nfs() as spawned:
            self.assertEqual(len(self.ensure(inputs)), 2)
            self.assertEqual(spawned.call_count, 2)
        with self.fake_nfs() as spawned:
            extra, new_sha = self.source_file(b"third")
            self.ensure(inputs + [InputFile("extra", new_sha, extra.stat().st_size)])
            self.assertEqual(spawned.call_count, 1)
            self.assertEqual(spawned.call_args[0][0]["sha256"], new_sha)

    def test_empty_input_is_verified_and_reused(self):
        _, sha = self.source_file(b"")
        with self.fake_nfs() as spawned:
            self.assertFalse(self.ensure([InputFile("empty", sha, 0)])[0].cache_hit)
            self.assertTrue(self.ensure([InputFile("empty", sha, 0)])[0].cache_hit)
        self.assertEqual(spawned.call_count, 1)

    def test_large_source_reads_are_bounded_and_copied_bytes_verified(self):
        data = b"large input" * (3 * 1024 * 1024 // 11) + b"tail"
        _, sha = self.source_file(data)
        original = cache_copy_worker.NFSObjectStorageBackend.open_artifact
        counts = []
        @contextmanager
        def bounded(backend, key):
            with original(backend, key) as incoming:
                class Reader:
                    def fileno(self): return incoming.fileno()
                    def read(self, count):
                        counts.append(count)
                        self_test.assertGreater(count, 0)
                        self_test.assertLessEqual(count, 1024 * 1024)
                        return incoming.read(count)
                yield Reader()
        self_test = self
        with tempfile.NamedTemporaryFile(dir=self.root / "parts") as output, \
                patch("hybrid_storage.inputs.require_input_mount", return_value={"mount_id":42}), \
                patch("hybrid_storage.inputs.fd_mount_id", return_value=42), \
                patch.object(cache_copy_worker.NFSObjectStorageBackend, "open_artifact", bounded):
            result = cache_copy_worker.copy_request(str(self.nfs), SOURCE, sha, len(data), output.fileno())
            self.assertEqual(result["status"], "ready")
            output.seek(0)
            self.assertEqual(output.read(), data)
        self.assertGreater(len(counts), 3)

    def test_corrupt_unpinned_replica_is_refetched_and_original_bytes_quarantined(self):
        with self.fake_nfs():
            self.ensure()
        self.target.chmod(0o600)
        self.target.write_bytes(b"wrong bytes")
        self.target.chmod(0o400)
        with self.fake_nfs() as spawned:
            repaired = self.ensure()[0]
        self.assertFalse(repaired.cache_hit)
        self.assertEqual(spawned.call_count, 1)
        self.assertEqual(self.target.read_bytes(), self.data)
        quarantined = list((self.root / "quarantine").iterdir())
        self.assertEqual(len(quarantined), 1)
        self.assertEqual(quarantined[0].read_bytes(), b"wrong bytes")

    def test_pinned_corruption_refuses_repair_preserving_pins_and_bytes(self):
        with self.fake_nfs():
            self.ensure()
        self.cache.pin(self.sha, len(self.data), job_id="job1", attempt_id="try1")
        self.target.chmod(0o600)
        self.target.write_bytes(b"bad")
        self.target.chmod(0o400)
        with self.fake_nfs() as spawned, self.assertRaises(CacheFetchBusy):
            self.ensure()
        self.assertEqual(spawned.call_count, 0)
        self.assertEqual(self.target.read_bytes(), b"bad")
        self.assertEqual(self.cache.inventory()[0].pinned_references, (("job1", "try1"),))

    def test_repair_admission_includes_old_corrupt_bytes_retained_in_quarantine(self):
        with self.fake_nfs():
            self.ensure()
        self.target.chmod(0o600)
        self.target.write_bytes(b"bad")
        self.target.chmod(0o400)
        small = self.fetcher_handle(quota_bytes=len(self.data) + 2)
        with self.fake_nfs(small) as spawned, self.assertRaises(CacheCapacityError):
            self.ensure(handle=small)
        self.assertEqual(spawned.call_count, 0)
        self.assertEqual(self.target.read_bytes(), b"bad")

    def test_transient_failure_retries_from_new_part_then_succeeds(self):
        calls, names = [], []
        def transient(request, output, lock):
            calls.append(request)
            names.extend(path.name for path in (self.root / "parts").glob("*.part"))
            if len(calls) == 1:
                os.write(request["output_fd"], b"incomplete")
                program = "import json; print(json.dumps({'status':'io_error','errno':116}))"
                return subprocess.Popen([sys.executable, "-c", program], stdout=output)
            return self.fake_spawn(request, output, lock)
        with patch.object(self.fetcher, "_spawn", side_effect=transient):
            result = self.ensure()[0]
        self.assertEqual(result.attempts, 2)
        self.assertEqual(len(names), 2)
        self.assertNotEqual(names[0], names[1])
        self.assertEqual(self.target.read_bytes(), self.data)
        self.assertEqual(list((self.root / "parts").iterdir()), [])

    def test_real_interrupted_read_after_partial_copy_retries_cleanly(self):
        interrupted = FAKE_HELPER.replace("    print(json.dumps(copy_request", """
    import errno
    from contextlib import contextmanager
    from hybrid_storage.inputs import NFSObjectStorageBackend
    original = NFSObjectStorageBackend.open_artifact
    @contextmanager
    def broken(self, key):
        with original(self, key) as source:
            class Reader:
                started = False
                def fileno(self): return source.fileno()
                def read(self, count):
                    if self.started: raise OSError(errno.ESTALE, 'interrupted read')
                    self.started = True
                    return source.read(min(count, 3))
            yield Reader()
    NFSObjectStorageBackend.open_artifact = broken
    print(json.dumps(copy_request"""
        )
        calls = 0
        def spawn(request, output, lock):
            nonlocal calls
            calls += 1
            program = interrupted if calls == 1 else FAKE_HELPER
            return subprocess.Popen([sys.executable, "-I", "-B", "-c", program, str(ROOT), json.dumps(request)],
                stdout=output, stderr=subprocess.DEVNULL, pass_fds=(request["output_fd"], lock))
        with patch.object(self.fetcher, "_spawn", side_effect=spawn):
            self.assertEqual(self.ensure()[0].attempts, 2)
        self.assertEqual(self.target.read_bytes(), self.data)
        self.assertEqual(list((self.root / "parts").iterdir()), [])

    def test_actual_copy_write_enospc_is_not_published_or_retried(self):
        disk_full = FAKE_HELPER.replace("    print(json.dumps(copy_request", """
    import errno, os
    original = os.fdopen
    def full(fd, mode):
        stream = original(fd, mode)
        class Writer:
            def __enter__(self): return self
            def __exit__(self, *args): stream.close()
            def __getattr__(self, name): return getattr(stream, name)
            def write(self, chunk): raise OSError(errno.ENOSPC, 'modeled full disk')
        return Writer() if mode == 'w+b' else stream
    os.fdopen = full
    print(json.dumps(copy_request"""
        )
        def spawn(request, output, lock):
            return subprocess.Popen([sys.executable, "-I", "-B", "-c", disk_full, str(ROOT), json.dumps(request)],
                stdout=output, stderr=subprocess.DEVNULL, pass_fds=(request["output_fd"], lock))
        with patch.object(self.fetcher, "_spawn", side_effect=spawn) as spawned:
            with self.assertRaises(OSError) as raised:
                self.ensure()
        self.assertEqual(raised.exception.errno, errno.ENOSPC)
        self.assertEqual(spawned.call_count, 1)
        self.assert_no_partial_or_final()

    def test_exhausted_retry_never_publishes_and_preserves_unrelated_part(self):
        unrelated = self.root / "parts/previous-crash.part"
        unrelated.write_bytes(b"retain")
        unrelated.chmod(0o600)
        def failure(request, output, lock):
            os.write(request["output_fd"], b"partial")
            return subprocess.Popen([sys.executable, "-c",
                "print('{\"status\":\"io_error\",\"errno\":5}')"], stdout=output)
        with patch.object(self.fetcher, "_spawn", side_effect=failure) as spawned:
            with self.assertRaises(OSError) as raised:
                self.ensure()
        self.assertEqual(raised.exception.errno, errno.EIO)
        self.assertEqual(spawned.call_count, 3)
        self.assertFalse(self.target.exists())
        self.assertEqual(list((self.root / "parts").iterdir()), [unrelated])
        self.assertEqual(unrelated.read_bytes(), b"retain")

    def test_permission_source_missing_and_disk_full_errors_do_not_retry(self):
        for code in (errno.EACCES, errno.ENOENT, errno.ENOSPC, errno.EDQUOT, errno.EROFS):
            def failure(request, output, lock):
                os.write(request["output_fd"], b"partial")
                return subprocess.Popen([sys.executable, "-c",
                    f"print('{{\"status\":\"io_error\",\"errno\":{code}}}')"], stdout=output)
            with self.subTest(code=code), patch.object(self.fetcher, "_spawn", side_effect=failure) as spawned:
                with self.assertRaises(OSError) as raised:
                    self.ensure()
                self.assertEqual(raised.exception.errno, code)
                self.assertEqual(spawned.call_count, 1)
                self.assert_no_partial_or_final()

    def test_source_wrong_digest_short_size_and_oversize_never_publish(self):
        source = self.nfs / object_key(self.sha)
        for data in (b"X" * len(self.data), self.data[:-1], self.data + b"extra"):
            source.write_bytes(data)
            with self.subTest(data=data), self.fake_nfs() as spawned:
                with self.assertRaises(CacheCorrupt):
                    self.ensure()
                self.assertEqual(spawned.call_count, 1)
                self.assert_no_partial_or_final()

    def test_source_mount_replaced_at_context_exit_discards_complete_part(self):
        changed_mount = FAKE_HELPER.replace("return_value={'mount_id': 42}",
            "side_effect=[{'mount_id':42}, {'mount_id':42}, {'mount_id':43}]")
        def spawn(request, output, lock):
            return subprocess.Popen([sys.executable, "-I", "-B", "-c", changed_mount, str(ROOT), json.dumps(request)],
                stdout=output, stderr=subprocess.DEVNULL, pass_fds=(request["output_fd"], lock))
        with patch.object(self.fetcher, "_spawn", side_effect=spawn), self.assertRaises(MountNotReady):
            self.ensure()
        self.assert_no_partial_or_final()

    def test_real_helper_rejects_ordinary_directory_as_nfs_without_fallback(self):
        with self.assertRaises(MountNotReady):
            self.ensure()
        self.assert_no_partial_or_final()
        self.assertEqual((self.nfs / object_key(self.sha)).read_bytes(), self.data)

    def test_no_authority_no_cache_or_nfs_access(self):
        with patch.object(self.cache, "open_verified", side_effect=AssertionError("no lookup")), self.fake_nfs() as spawned:
            for denied in (False, None, 1):
                with self.subTest(denied=denied), self.assertRaises(PermissionError):
                    self.fetcher.ensure([self.input], authorize=lambda inputs: denied)
        self.assertEqual(spawned.call_count, 0)
        self.assert_no_partial_or_final()

    def test_revoked_authority_after_copy_prevents_publication(self):
        revoked = False
        def spawn(request, output, lock):
            nonlocal revoked
            revoked = True
            return self.fake_spawn(request, output, lock)
        with patch.object(self.fetcher, "_spawn", side_effect=spawn):
            with self.assertRaises(PermissionError):
                self.ensure(authorize=lambda inputs: not revoked)
        self.assert_no_partial_or_final()

    def test_bad_manifest_and_options_rejected_before_staging(self):
        for inputs in ([self.input, self.input], [object()], [self.input, InputFile("other", self.sha, 1)]):
            with self.subTest(inputs=inputs), self.fake_nfs() as spawned, self.assertRaises(ValueError):
                self.ensure(inputs)
            self.assertEqual(spawned.call_count, 0)
        for changes in ({"quota_bytes": 0}, {"attempts": 6}, {"io_timeout": float("nan")},
                        {"reserve_free_bytes": -1}, {"lock_timeout": True}, {"source": "bad"}):
            options = dict(source=SOURCE, quota_bytes=100, **{})
            options.update(changes)
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                CacheFetcher(self.cache, **options)

    def test_quota_refusal_precedes_nfs_copy_and_counts_retained_crash_parts(self):
        orphan = self.root / "parts/previous.part"
        orphan.write_bytes(b"retained")
        orphan.chmod(0o600)
        small = self.fetcher_handle(quota_bytes=len(self.data) + len(b"retained") - 1)
        with self.fake_nfs(small) as spawned, self.assertRaises(CacheCapacityError) as raised:
            self.ensure(handle=small)
        self.assertEqual(raised.exception.errno, errno.EDQUOT)
        self.assertEqual(spawned.call_count, 0)
        self.assertEqual(orphan.read_bytes(), b"retained")

    def test_free_disk_reserve_refusal_precedes_nfs_copy(self):
        values = os.statvfs_result((4096, 4096, 100, 30, 0, 10, 8, 8, 0, 255))
        with patch.object(cache_fetch.os, "fstatvfs", return_value=values), self.fake_nfs() as spawned:
            with self.assertRaises(CacheCapacityError) as raised:
                self.ensure()
        self.assertEqual(raised.exception.errno, errno.ENOSPC)
        self.assertEqual(spawned.call_count, 0)
        self.assert_no_partial_or_final()

    def test_writable_symlink_and_fifo_cache_entries_not_repaired(self):
        self.target.parent.mkdir(parents=True, mode=0o700)
        self.target.parent.parent.chmod(0o700)
        self.target.write_bytes(self.data)
        self.target.chmod(0o600)
        with self.fake_nfs() as spawned, self.assertRaises(PermissionError):
            self.ensure()
        self.assertEqual(spawned.call_count, 0)
        self.target.unlink()
        self.target.symlink_to(self.nfs / object_key(self.sha))
        with self.fake_nfs() as spawned, self.assertRaises(OSError):
            self.ensure()
        self.assertEqual(spawned.call_count, 0)
        self.target.unlink()
        os.mkfifo(self.target)
        with self.fake_nfs() as spawned, self.assertRaises(PermissionError):
            self.ensure()
        self.assertEqual(spawned.call_count, 0)

    def test_local_concurrent_handles_fetch_one_digest_only_once(self):
        other = self.fetcher_handle()
        with self.fake_nfs() as first, self.fake_nfs(other) as second:
            with ThreadPoolExecutor(max_workers=2) as executor:
                a = executor.submit(self.ensure)
                b = executor.submit(self.ensure, handle=other)
                results = [a.result(timeout=10)[0], b.result(timeout=10)[0]]
        self.assertEqual(first.call_count + second.call_count, 1)
        self.assertEqual(sorted(value.cache_hit for value in results), [False, True])

    def process_program(self):
        return """
import json, os, subprocess, sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from agent.cache_storage import local_cache, nfs_cache_fetcher
from hybrid_storage.inputs import InputFile
cache = local_cache(Path(sys.argv[2]), input_mount=Path(sys.argv[3]))
fetch = nfs_cache_fetcher(cache, source=sys.argv[4], quota_bytes=int(sys.argv[9]), reserve_free_bytes=0)
fetch.initialize()
def spawn(request, output, lock):
    with open(sys.argv[8], 'ab') as count: count.write(b'x')
    return subprocess.Popen([sys.executable, '-I', '-B', '-c', sys.argv[7], sys.argv[1], json.dumps(request)],
        stdout=output, stderr=subprocess.DEVNULL, pass_fds=(request['output_fd'],lock))
fetch._spawn = spawn
try:
    result = fetch.ensure([InputFile('input',sys.argv[5],int(sys.argv[6]))], authorize=lambda inputs: True)
    print(json.dumps({'hit': result[0].cache_hit}), flush=True)
except OSError as error:
    print(json.dumps({'errno': error.errno}), flush=True)
finally:
    fetch.close()
"""

    def test_separate_processes_download_same_digest_only_once(self):
        counter = self.base / "copy-count"
        args = [sys.executable, "-B", "-c", self.process_program(), str(ROOT), str(self.root), str(self.nfs),
                SOURCE, self.sha, str(len(self.data)), FAKE_HELPER, str(counter), str(len(self.data))]
        processes = [subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) for _ in range(2)]
        results = []
        for process in processes:
            out, error = process.communicate(timeout=10)
            self.assertEqual(process.returncode, 0, error)
            results.append(json.loads(out))
        self.assertEqual(counter.read_bytes(), b"x")
        self.assertEqual(sorted(value["hit"] for value in results), [False, True])

    def test_separate_processes_do_not_overcommit_quota_for_different_digests(self):
        counter = self.base / "copy-count"
        _, second_sha = self.source_file(b"second payload of same size!\n")
        payload_size = len(b"second payload of same size!\n")
        processes = []
        quota = max(len(self.data), payload_size)
        for sha, size in ((self.sha, len(self.data)), (second_sha, payload_size)):
            args = [sys.executable, "-B", "-c", self.process_program(), str(ROOT), str(self.root), str(self.nfs),
                    SOURCE, sha, str(size), FAKE_HELPER, str(counter), str(quota)]
            processes.append(subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True))
        results = []
        for process in processes:
            out, error = process.communicate(timeout=10)
            self.assertEqual(process.returncode, 0, error)
            results.append(json.loads(out))
        self.assertEqual(counter.read_bytes(), b"x")
        self.assertEqual(sum(value.get("errno") == errno.EDQUOT for value in results), 1)
        self.assertEqual(sum(value.get("hit") is False for value in results), 1)

    def test_parent_crash_leaves_nonpublishing_child_holding_global_slot(self):
        marker = self.base / "helper-started"
        sleeper = f"import pathlib,time;pathlib.Path({str(marker)!r}).write_text('ready');time.sleep(1.5)"
        parent_program = self.process_program().replace(
            "[sys.executable, '-I', '-B', '-c', sys.argv[7], sys.argv[1], json.dumps(request)]",
            f"[sys.executable, '-I', '-B', '-c', {sleeper!r}]")
        args = [sys.executable, "-B", "-c", parent_program, str(ROOT), str(self.root), str(self.nfs),
                SOURCE, self.sha, str(len(self.data)), FAKE_HELPER, str(self.base / "count"), "10000"]
        process = subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            deadline = time.monotonic() + 3
            while not marker.exists() and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertTrue(marker.exists())
            process.kill()
            process.wait(timeout=3)
            fast_lock = self.fetcher_handle(lock_timeout=0.02)
            with self.assertRaises(CacheFetchBusy):
                self.ensure(handle=fast_lock)
            self.assertFalse(self.target.exists())
            time.sleep(1.6)  # Disposable orphan exits naturally, never publishes.
            with self.fake_nfs():
                self.assertFalse(self.ensure()[0].cache_hit)
            self.assertEqual(len(list((self.root / 'parts').glob('*.part'))), 1)
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=3)

    def test_close_while_copy_active_fences_publication_and_reaps(self):
        def sleeper(request, output, lock):
            return subprocess.Popen([sys.executable, "-c", "import time;time.sleep(30)"],
                stdout=output, pass_fds=(request["output_fd"], lock))
        with self.fake_nfs(), patch.object(self.fetcher, "_spawn", side_effect=sleeper):
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(self.ensure)
                deadline = time.monotonic() + 2
                while not self.fetcher.pending() and time.monotonic() < deadline:
                    time.sleep(0.01)
                self.assertTrue(self.fetcher.pending())
                self.fetcher.close()
                with self.assertRaises(CacheError):
                    future.result(timeout=3)
        self.assertEqual(self.fetcher.pending(), [])
        self.assert_no_partial_or_final()

    def test_fake_uncancellable_timeout_retains_capacity_and_late_success_never_publishes(self):
        class Blocked:
            pid = 999999
            returncode = None
            def wait(inner, timeout): raise subprocess.TimeoutExpired("fixture", timeout)
            def poll(inner): return inner.returncode
            def kill(inner): pass
        blocked = Blocked()
        with patch.object(self.fetcher, "_spawn", return_value=blocked) as spawned:
            with self.assertRaises(CacheFetchTimeout):
                self.ensure()
            self.assertEqual(len(self.fetcher.pending()), 1)
            self.assertEqual(len(list((self.root / "parts").glob("*.part"))), 1)
            with self.assertRaises(CacheFetchBusy):
                self.ensure()
            self.assertEqual(spawned.call_count, 1)
            # Another handle/process cannot bypass retained root-level capacity.
            other = self.fetcher_handle(lock_timeout=0.02)
            with self.assertRaises(CacheFetchBusy):
                self.ensure(handle=other)
            self.assertEqual(len(self.fetcher.close()), 1)
            # Model a complete, correct late result from an uncancellable read.
            owned = next(iter(self.fetcher._owned.values()))
            os.write(owned["fd"], self.data)
            os.fchmod(owned["fd"], 0o400)
            spawned.call_args[0][1].write(json.dumps({"status":"ready", "sha256":self.sha,
                                                    "size_bytes":len(self.data)}).encode())
            blocked.returncode = 0
            self.assertEqual(len(self.fetcher.reconcile()), 1)
        self.assertEqual(self.fetcher.pending(), [])
        self.assert_no_partial_or_final()

    def test_real_sleep_helper_timeout_is_killed_reaped_and_never_published(self):
        fast = self.fetcher_handle(io_timeout=0.03)
        def sleeper(request, output, lock):
            return subprocess.Popen([sys.executable, "-c", "import time;time.sleep(30)"],
                stdout=output, pass_fds=(request["output_fd"], lock))
        with patch.object(fast, "_spawn", side_effect=sleeper):
            started = time.monotonic()
            with self.assertRaises(CacheFetchTimeout):
                self.ensure(handle=fast)
            self.assertLess(time.monotonic() - started, 1)
        deadline = time.monotonic() + 2
        while fast.pending() and time.monotonic() < deadline:
            fast.reconcile()
            time.sleep(0.01)
        self.assertEqual(fast.pending(), [])
        self.assert_no_partial_or_final()

    def test_copy_helper_wrong_or_malformed_result_never_publishes(self):
        for result in ("not json", "{}", "x" * 4097,
                       json.dumps({"status": "ready", "sha256": "a" * 64, "size_bytes": len(self.data)}),
                       json.dumps({"status": "io_error", "errno": True})):
            def malformed(request, output, lock):
                return subprocess.Popen([sys.executable, "-c", f"print({result!r})"], stdout=output)
            with self.subTest(result=result[:50]), patch.object(self.fetcher, "_spawn", side_effect=malformed):
                with self.assertRaises(CacheError):
                    self.ensure()
                self.assert_no_partial_or_final()

    def test_parent_rehashes_part_before_publication_not_just_helper_claim(self):
        def liar(request, output, lock):
            os.write(request["output_fd"], b"X" * len(self.data))
            os.fchmod(request["output_fd"], 0o400)
            result = {"status": "ready", "sha256": self.sha, "size_bytes": len(self.data)}
            return subprocess.Popen([sys.executable, "-c", f"print({json.dumps(result)!r})"], stdout=output)
        with patch.object(self.fetcher, "_spawn", side_effect=liar), self.assertRaises(ValueError):
            self.ensure()
        self.assert_no_partial_or_final()

    def test_post_rename_failure_is_reconciled_by_verified_hit_not_second_transfer(self):
        original = cache_fetch._rename_noreplace
        def committed_then_failed(*args):
            original(*args)
            raise OSError(errno.EIO, "modeled post-rename local failure")
        with self.fake_nfs() as spawned, patch.object(cache_fetch, "_rename_noreplace", side_effect=committed_then_failed):
            with self.assertRaises(OSError):
                self.ensure()
        self.assertEqual(spawned.call_count, 1)
        self.assertEqual(self.target.read_bytes(), self.data)
        with self.fake_nfs() as spawned:
            self.assertTrue(self.ensure()[0].cache_hit)
        self.assertEqual(spawned.call_count, 0)

    def test_closed_fetcher_has_no_new_transfers(self):
        self.fetcher.close()
        with self.fake_nfs() as spawned, self.assertRaises(CacheError):
            self.ensure()
        self.assertEqual(spawned.call_count, 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
