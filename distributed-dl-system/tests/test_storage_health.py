"""Pi-less mount identity, integrity, errno and owned-process deadline checks."""

from contextlib import contextmanager
import errno
import hashlib
import io
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
from hybrid_storage.backends import _directory
from hybrid_storage.health import HealthSupervisor, ProbeRequest, verify_request
from hybrid_storage.inputs import LocalObjectStorageBackend, NFSObjectStorageBackend, object_key
from hybrid_storage.mount_identity import fd_mount_id, require_local_fd
from hybrid_storage.nfs_mount import MountNotReady, require_input_mount

SOURCE = "192.0.2.1:/srv/ml-shared/objects"
ROOT_ROW = "1 0 0:1 / / rw - ext4 /dev/fake rw\n"


class HealthTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="storage-health-")
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name)
        self.root = self.base / "inputs"
        self.root.mkdir()
        self.data = b"verified health fixture\n"
        self.sha = hashlib.sha256(self.data).hexdigest()
        self.key = object_key(self.sha)
        self.file = self.root / self.key
        self.file.parent.mkdir(parents=True)
        self.file.write_bytes(self.data)
        self.backend = NFSObjectStorageBackend(self.root, source=SOURCE)

    def row(self, *, source=SOURCE, vfs="ro,nosuid,nodev,noexec", options="rw,vers=4.1,hard,proto=tcp,sec=sys", kind="nfs4"):
        return ROOT_ROW + f"42 1 0:2 / {self.root} {vfs} - {kind} {source} {options}\n"

    def request(self, role="nfs", **changes):
        values = dict(role=role, root=str(self.root), sha256=self.sha,
                      size_bytes=len(self.data), source=SOURCE if role == "nfs" else None)
        return ProbeRequest(**dict(values, **changes))

    @contextmanager
    def fake_mount(self, text=None, fd_identity=42):
        text = self.row() if text is None else text
        def check(source, *, target):
            return require_input_mount(source, text, target=target)
        with patch("hybrid_storage.inputs.require_input_mount", side_effect=check), patch("hybrid_storage.inputs.fd_mount_id", return_value=fd_identity):
            yield

    def test_configured_target_and_readonly_vfs_over_rw_superblock_are_valid(self):
        admitted = require_input_mount(SOURCE, self.row(), target=self.root)
        self.assertEqual(admitted["mount_id"], 42)
        with self.fake_mount():
            self.assertTrue(self.backend.artifact_exists(self.key))
            with self.backend.open_artifact(self.key) as stream:
                self.assertEqual(stream.read(), self.data)

    def test_missing_expected_source_refuses_all_reads_before_lookup(self):
        backend = NFSObjectStorageBackend(self.root)
        with patch("hybrid_storage.backends.os.open", side_effect=AssertionError("No input lookup")):
            for method in (backend.artifact_exists, backend.resolve_path):
                with self.assertRaises(MountNotReady):
                    method(self.key)
            with self.assertRaises(MountNotReady), backend.open_artifact(self.key):
                self.fail("Unadmitted read")

    def test_actual_ordinary_directory_is_not_nfs_even_with_valid_object(self):
        with self.assertRaises(MountNotReady):
            self.backend.artifact_exists(self.key)
        result = verify_request(self.request())
        self.assertEqual(result, {"ready": False, "status": "mount_unavailable"})

    def test_wrong_missing_autofs_and_unsafe_mounts_refuse_before_input_io(self):
        rows = [ROOT_ROW, ROOT_ROW + f"41 1 0:2 / {self.root} rw - autofs systemd-1 rw\n",
                self.row(source="192.0.2.2:/srv/ml-shared/objects"), self.row(kind="ext4"),
                self.row(vfs="rw,nosuid,nodev,noexec"), self.row(vfs="ro,nodev,noexec"),
                self.row(options="ro,vers=4.1,soft,proto=tcp,sec=sys"),
                self.row(options="ro,vers=4.2,hard,proto=tcp,sec=sys"),
                self.row(options="ro,vers=4.1,hard,proto=udp,sec=sys")]
        for row in rows:
            with self.subTest(row=row), self.fake_mount(row), patch("hybrid_storage.backends.os.open", side_effect=AssertionError("No file lookup")):
                with self.assertRaises(MountNotReady):
                    self.backend.artifact_exists(self.key)

    def test_opened_descriptor_from_underlying_or_different_mount_is_rejected(self):
        with self.fake_mount(fd_identity=43), self.assertRaises(MountNotReady), self.backend.open_artifact(self.key):
            self.fail("Mountpoint race admitted underlying file")

    def test_mount_replacement_before_or_after_reader_is_not_healthy(self):
        for sequence in ([42, 43], [42, 42, 43]):
            with self.subTest(sequence=sequence), patch("hybrid_storage.inputs.require_input_mount", side_effect=[{"mount_id": identity} for identity in sequence]), patch("hybrid_storage.inputs.fd_mount_id", return_value=42):
                self.assertEqual(verify_request(self.request())["status"], "mount_unavailable")

    def test_unmount_during_missing_lookup_is_not_reported_as_object_miss(self):
        self.file.unlink()
        with patch("hybrid_storage.inputs.require_input_mount", side_effect=[{"mount_id": 42}, MountNotReady("unmounted")]):
            with self.assertRaises(MountNotReady):
                self.backend.artifact_exists(self.key)

    def test_missing_object_on_admitted_mount_has_explicit_status(self):
        self.file.unlink()
        with self.fake_mount():
            self.assertFalse(self.backend.artifact_exists(self.key))
            self.assertEqual(verify_request(self.request()), {"ready": False, "status": "source_missing", "errno": errno.ENOENT})

    def test_complete_authorized_read_verifies_size_and_digest(self):
        with self.fake_mount():
            result = verify_request(self.request())
        self.assertEqual(result, {"ready": True, "status": "ready", "bytes": len(self.data), "sha256": self.sha})

    def test_same_size_corruption_truncation_and_growth_are_never_ready(self):
        for data in (b"x" * len(self.data), self.data[:-1], self.data + b"x"):
            self.file.write_bytes(data)
            with self.subTest(data=data), self.fake_mount():
                self.assertEqual(verify_request(self.request()), {"ready": False, "status": "corrupt"})

    def test_mutation_of_metadata_during_read_is_rejected(self):
        original = self.backend._backend.open_artifact
        @contextmanager
        def changing(key):
            with original(key) as stream:
                class Changed:
                    def fileno(inner): return stream.fileno()
                    def read(inner, count):
                        data = stream.read(count)
                        info = self.file.stat()
                        os.utime(self.file, ns=(info.st_atime_ns, info.st_mtime_ns + 1000000))
                        return data
                yield Changed()
        with self.fake_mount(), patch("hybrid_storage.backends.NFSStorageBackend.open_artifact", side_effect=changing):
            self.assertEqual(verify_request(self.request())["status"], "corrupt")

    def test_empty_authorized_object_is_valid(self):
        sha = hashlib.sha256(b"").hexdigest()
        p = self.root / object_key(sha)
        p.parent.mkdir(parents=True)
        p.write_bytes(b"")
        with self.fake_mount():
            self.assertTrue(verify_request(self.request(sha256=sha, size_bytes=0))["ready"])

    def test_permission_outage_stale_and_full_disk_errors_are_preserved(self):
        for code, status in ((errno.EACCES, "permission_denied"), (errno.EPERM, "permission_denied"),
                             (errno.EIO, "io_error"), (errno.ETIMEDOUT, "io_error"),
                             (errno.ESTALE, "io_error"), (errno.ENOSPC, "io_error"), (errno.EROFS, "io_error")):
            with self.subTest(errno=code), self.fake_mount(), patch("hybrid_storage.backends.os.open", side_effect=OSError(code, "simulated")):
                self.assertEqual(verify_request(self.request()), {"ready": False, "status": status, "errno": code})

    def test_links_and_special_files_are_not_healthy(self):
        self.file.unlink()
        self.file.symlink_to(self.base / "outside")
        with self.fake_mount():
            self.assertFalse(verify_request(self.request())["ready"])
        self.file.unlink()
        os.mkfifo(self.file)
        with self.fake_mount():
            self.assertEqual(verify_request(self.request())["status"], "invalid_input")

    def test_nfs_publication_never_touches_unmounted_writable_directory(self):
        stream = io.BytesIO(self.data)
        before = self.file.read_bytes()
        with patch("hybrid_storage.backends.os.open", side_effect=AssertionError("No writes/lookups")):
            with self.assertRaises(PermissionError) as raised:
                self.backend.publish_artifact(self.key, stream, expected_sha256=self.sha, expected_size=len(self.data))
        self.assertEqual(raised.exception.errno, errno.EROFS)
        self.assertEqual(stream.tell(), 0)
        self.assertEqual(self.file.read_bytes(), before)

    def test_local_publisher_checks_actual_parent_and_staging_before_bytes(self):
        self.file.unlink()
        staging = self.base / "private"
        staging.mkdir(mode=0o700)
        publisher = LocalObjectStorageBackend(self.root, staging_root=staging)
        for results in ([OSError(errno.EXDEV, "remote parent")], [{}, OSError(errno.EXDEV, "remote staging")]):
            stream = io.BytesIO(self.data)
            with self.subTest(results=results), patch("hybrid_storage.inputs.require_local_fd", side_effect=results):
                with self.assertRaises(OSError) as raised:
                    publisher.publish_artifact(self.key, stream, expected_sha256=self.sha, expected_size=len(self.data))
            self.assertEqual(raised.exception.errno, errno.EXDEV)
            self.assertEqual(stream.tell(), 0)
            self.assertFalse(self.file.exists())
            self.assertEqual(list(staging.iterdir()), [])

    def test_descriptor_metadata_is_required_and_actual_local_fs_is_supported(self):
        for text in ("", "mnt_id: x\n", "mnt_id: 1\nmnt_id: 2\n"):
            with self.subTest(text=text), patch.object(Path, "read_text", return_value=text), self.assertRaises(MountNotReady):
                fd_mount_id(9)
        with _directory(self.root) as fd:
            self.assertIsInstance(fd_mount_id(fd), int)
            self.assertIn(require_local_fd(fd)["fstype"], ("ext4", "xfs", "btrfs"))
        for kind in ("nfs4", "cifs", "fuse.remote", "tmpfs", "unknown"):
            with self.subTest(kind=kind), patch("hybrid_storage.mount_identity.fd_mount_id", return_value=1), patch.object(Path, "read_text", return_value=ROOT_ROW.replace("ext4", kind)):
                with self.assertRaises(OSError) as raised:
                    require_local_fd(9)
                self.assertEqual(raised.exception.errno, errno.EXDEV)

    def test_invalid_targets_sources_and_probe_requests_are_rejected(self):
        for target in ("/", "relative", "/tmp/../inputs", "/tmp//inputs"):
            with self.subTest(target=target), self.assertRaises(MountNotReady):
                require_input_mount(SOURCE, self.row(), target=target)
        for changes in ({"role": "unknown"}, {"source": None}, {"source": "server:/etc"},
                        {"root": "../inputs"}, {"root": "/"}, {"size_bytes": True}, {"sha256": "bad"}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                self.request(**changes)

    def supervisor(self):
        supervisor = HealthSupervisor(spool_dir=self.base)
        self.addCleanup(supervisor.close)
        return supervisor

    def test_real_child_local_verification_and_real_negative_nfs_admission(self):
        supervisor = self.supervisor()
        local = supervisor.check(self.request("local"), timeout=3)
        self.assertTrue(local["ready"])
        self.assertFalse(local["pending"])
        remote = supervisor.check(self.request(), timeout=3)
        self.assertEqual(remote["status"], "mount_unavailable")
        self.assertEqual(supervisor.pending(), [])

    def test_pending_uncancellable_child_is_retained_and_late_success_stays_failed(self):
        supervisor = self.supervisor()
        class Blocked:
            pid = 999999
            returncode = None
            def wait(inner, timeout): raise subprocess.TimeoutExpired("test reader", timeout)
            def poll(inner): return inner.returncode
            def terminate(inner): pass  # models an uninterruptible kernel wait
        blocked = Blocked()
        with patch.object(supervisor, "_spawn", return_value=blocked) as spawn:
            start = time.monotonic()
            result = supervisor.check(self.request(), timeout=0.05)
            self.assertLess(time.monotonic() - start, 0.5)
            self.assertEqual(result["status"], "timeout")
            self.assertTrue(result["pending"])
            self.assertEqual(supervisor.check(self.request())["status"], "busy")
            self.assertEqual(spawn.call_count, 1)
            self.assertEqual(len(supervisor.close()), 1)
            self.assertEqual(supervisor.check(self.request())["status"], "closed")
            owned = next(iter(supervisor._owned.values()))
            owned["output"].write(json.dumps({"ready": True, "status": "ready", "bytes": len(self.data), "sha256": self.sha}).encode())
            blocked.returncode = 0
            reaped = supervisor.reconcile()
            self.assertEqual(reaped[0]["status"], "timeout")
            self.assertFalse(reaped[0]["ready"])
            self.assertTrue(owned["output"].closed)
            self.assertEqual(supervisor.pending(), [])

    def test_real_disposable_sleep_child_timeout_is_reaped(self):
        supervisor = self.supervisor()
        def sleeper(request, output):
            return subprocess.Popen([sys.executable, "-I", "-B", "-c", "import time;time.sleep(30)"], stdout=output, stderr=subprocess.DEVNULL, stdin=subprocess.DEVNULL)
        with patch.object(supervisor, "_spawn", side_effect=sleeper):
            result = supervisor.check(self.request(), timeout=0.05)
        self.assertEqual(result["status"], "timeout")
        deadline = time.monotonic() + 3
        while supervisor.pending() and time.monotonic() < deadline:
            supervisor.reconcile()
            time.sleep(0.01)
        self.assertEqual(supervisor.pending(), [])

    def test_malformed_failed_or_wrong_identity_child_cannot_report_ready(self):
        supervisor = self.supervisor()
        cases = [(0, b"bad"), (1, b"{}"), (0, b"x" * 4097),
                 (0, json.dumps({"ready": True, "status": "mount_unavailable"}).encode()),
                 (0, json.dumps({"ready": True, "status": "ready", "bytes": 1, "sha256": "0" * 64}).encode())]
        for rc, raw in cases:
            class Finished:
                pid = 999998
                returncode = rc
                def wait(inner, timeout): return inner.returncode
                def poll(inner): return inner.returncode
            def spawn(request, output):
                output.write(raw)
                return Finished()
            with self.subTest(rc=rc, raw=raw[:50]), patch.object(supervisor, "_spawn", side_effect=spawn):
                self.assertEqual(supervisor.check(self.request())["status"], "probe_failed")

    def test_bad_deadlines_reject_before_spawning(self):
        supervisor = self.supervisor()
        for timeout in (0, -1, True, "5", float("inf"), float("nan"), 301):
            with self.subTest(timeout=timeout), patch.object(supervisor, "_spawn", side_effect=AssertionError("No child")), self.assertRaises(ValueError):
                supervisor.check(self.request(), timeout=timeout)

    def test_completion_observed_after_deadline_does_not_report_ready(self):
        supervisor = self.supervisor()
        class Late:
            pid = 999997
            returncode = 0
            def wait(inner, timeout): time.sleep(0.03)
            def poll(inner): return 0
        def spawn(request, output):
            output.write(json.dumps({"ready": True, "status": "ready", "bytes": len(self.data), "sha256": self.sha}).encode())
            return Late()
        with patch.object(supervisor, "_spawn", side_effect=spawn):
            self.assertEqual(supervisor.check(self.request(), timeout=0.01)["status"], "timeout")

    def test_missing_kernel_metadata_is_not_an_object_miss(self):
        with patch.object(Path, "read_text", side_effect=FileNotFoundError(errno.ENOENT, "proc missing")):
            with self.assertRaises(MountNotReady):
                self.backend.artifact_exists(self.key)

    def test_spawn_failure_releases_local_spool_and_keeps_no_owned_child(self):
        supervisor = self.supervisor()
        outputs = []
        def fail(request, output):
            outputs.append(output)
            raise OSError(errno.EAGAIN, "process capacity")
        with patch.object(supervisor, "_spawn", side_effect=fail), self.assertRaises(OSError):
            supervisor.check(self.request())
        self.assertTrue(outputs[0].closed)
        self.assertEqual(supervisor.pending(), [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
