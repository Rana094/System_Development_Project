"""5.3 disposable private-copy/lifecycle acceptance; no live agent/NFS imports."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
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
import uuid

import psutil

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from agent.cache_storage import local_cache, private_workspaces
from agent import execution_lease as leases
from hybrid_storage import workspaces
from hybrid_storage.dataset_manifests import DatasetEntry, DatasetManifest
from hybrid_storage.input_manifests import SourceFile, SourceManifest, encoded
from hybrid_storage.inputs import object_key
from hybrid_storage.worker_cache import CacheMiss
from hybrid_storage.workspaces import (AttemptIdentity, OUTCOMES, WorkspaceCancelled, WorkspaceError,
    WorkspaceBusy, WorkspaceLaunchFailed, WorkspaceLeaseExpired, WorkspacePlan)

TRAINING = b"""import csv,json,pathlib,sys
rows=list(csv.DictReader(pathlib.Path(sys.argv[1]).open()))
result={'rows':len(rows),'sum':sum(int(row['x']) for row in rows)}
pathlib.Path(sys.argv[2],'result.json').write_text(json.dumps(result))
pathlib.Path(sys.argv[3],'candidate.json').write_text(json.dumps(result))
print(json.dumps(result))
"""
ROWS = b"x,y\n1,2\n3,4\n"

PROCESS_SETUP = """
import json,os,sys
from pathlib import Path
sys.path.insert(0,sys.argv[1])
from agent.cache_storage import local_cache,private_workspaces
from hybrid_storage.dataset_manifests import DatasetManifest,DatasetEntry
from hybrid_storage.input_manifests import SourceManifest,SourceFile
from hybrid_storage.workspaces import AttemptIdentity,WorkspacePlan
doc=json.loads(sys.argv[4]);src=doc['source'];data=doc['dataset']
source=SourceManifest(tuple(SourceFile(**v) for v in src['files']),tuple(src['directories']))
dataset=None if data is None else DatasetManifest(data['dataset_id'],data['version'],data['owner'],
    tuple(DatasetEntry(**v) for v in data['entries']),data['previous_manifest_sha256'])
plan=WorkspacePlan(AttemptIdentity(**doc['identity']),doc['input_manifest_sha256'],source,dataset)
cache=local_cache(Path(sys.argv[2]),input_mount=Path(sys.argv[5]))
manager=private_workspaces(cache,Path(sys.argv[3]),max_input_bytes=10000000,reserve_free_bytes=0)
manager.initialize()
"""


class PrivateWorkspaceTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="private-workspace-5-3-")
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name)
        self.cache_root, self.root = self.base / "cache", self.base / "private"
        self.mount = self.base / "not-contacted-nfs"
        self.cache_root.mkdir(mode=0o700)
        self.root.mkdir(mode=0o700)
        self.cache = local_cache(self.cache_root, input_mount=self.mount)
        self.cache.initialize()
        self.manager = self.handle()
        self.manager.initialize()
        self.training_sha = self.install(TRAINING)
        self.rows_sha = self.install(ROWS)
        self.empty_sha = self.install(b"")
        source = SourceManifest((SourceFile("train.py", self.training_sha, len(TRAINING), True),
                                 SourceFile("requirements.txt", self.empty_sha, 0)), ("empty",))
        data = DatasetManifest("ds_" + "a" * 32, 1, "alice",
                              (DatasetEntry("shard", "directory"),
                               DatasetEntry("shard/rows.csv", "file", self.rows_sha, len(ROWS))))
        self.plan = WorkspacePlan(AttemptIdentity("job1", "job1:0"),
                                 hashlib.sha256(b"exact-controller-job-input-binding").hexdigest(), source, data)

    def handle(self, **limits):
        return private_workspaces(self.cache, self.root,
            **dict({"max_input_bytes": 10000000, "reserve_free_bytes": 0}, **limits))

    def install(self, data):
        sha = hashlib.sha256(data).hexdigest()
        target = self.cache_root / "objects" / object_key(sha)
        target.parent.parent.mkdir(mode=0o700, exist_ok=True)
        target.parent.mkdir(mode=0o700, exist_ok=True)
        target.write_bytes(data)
        target.chmod(0o400)
        return sha

    def prepare(self, plan=None, manager=None, authorize=None):
        return (manager or self.manager).prepare(plan or self.plan, authorize=authorize or (lambda plan: True))

    def finish(self, plan=None, outcome="COMPLETED", proof=True):
        return self.manager.finish(plan or self.plan, outcome, quiescent=lambda identity: proof)

    def pins(self):
        return [(row.sha256, ref) for row in self.cache.inventory() for ref in row.pinned_references]

    def control(self, plan=None):
        return self.root / "state" / ((plan or self.plan).identity.key + ".json")

    def process(self, suffix, plan=None):
        return subprocess.run([sys.executable, "-B", "-c", PROCESS_SETUP + suffix,
            str(ROOT), str(self.cache_root), str(self.root), json.dumps((plan or self.plan).document()),
            str(self.mount)], capture_output=True, text=True, timeout=10)

    def test_factory_construction_and_import_are_inactive(self):
        missing = self.base / "missing"
        with patch.object(workspaces, "_directory", side_effect=AssertionError("no I/O")):
            handle = private_workspaces(self.cache, missing, max_input_bytes=1)
        self.assertEqual(handle.root, missing)
        self.assertFalse(missing.exists())
        result = self.process("assert 'job_store' not in sys.modules; assert 'agent.agent' not in sys.modules\n")
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_configuration_requires_separate_provisioned_protected_local_root(self):
        for root in (self.cache_root, self.cache_root / "child", self.base, self.mount):
            with self.subTest(root=root), self.assertRaises(ValueError):
                private_workspaces(self.cache, root, max_input_bytes=1)
        self.root.chmod(0o755)
        with self.assertRaises(PermissionError):
            self.manager.initialize()
        self.root.chmod(0o700)
        with patch.object(workspaces, "_protected", side_effect=OSError(errno.EXDEV, "remote root")):
            with self.assertRaises(OSError):
                self.manager.initialize()
        missing = self.base / "missing"
        with self.assertRaises(FileNotFoundError):
            private_workspaces(self.cache, missing, max_input_bytes=1).initialize()
        self.assertFalse(missing.exists())

    def test_typed_identity_and_plan_reject_invalid_paths_revisions_and_bounds(self):
        for changes in ({"job_id":"../bad"}, {"attempt_token":"other:0"}, {"attempt_token":"job1/0"},
                        {"stage_revision":0}, {"rank":True}, {"rank":-1}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                replace(self.plan.identity, **changes)
        for changes in ({"source":{}}, {"dataset":{}}, {"input_manifest_sha256":"bad"}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                replace(self.plan, **changes)
        for limits in ({"max_input_bytes":True}, {"reserve_free_bytes":-1}, {"lock_timeout":float("nan")}):
            with self.subTest(limits=limits), self.assertRaises(ValueError):
                self.handle(**limits)

    def test_private_copies_preserve_bytes_modes_empty_dirs_and_separate_candidates(self):
        receipt = self.prepare()
        self.assertEqual((receipt.source / "train.py").read_bytes(), TRAINING)
        self.assertEqual((receipt.data / "shard/rows.csv").read_bytes(), ROWS)
        self.assertTrue((receipt.source / "empty").is_dir())
        for file, sha, mode in ((receipt.source / "train.py", self.training_sha, 0o500),
                                (receipt.data / "shard/rows.csv", self.rows_sha, 0o400)):
            cached = self.cache_root / "objects" / object_key(sha)
            self.assertNotEqual((file.stat().st_dev, file.stat().st_ino),
                                (cached.stat().st_dev, cached.stat().st_ino))
            self.assertEqual(file.stat().st_nlink, 1)
            self.assertEqual(file.stat().st_mode & 0o777, mode)
        self.assertTrue(receipt.outputs.is_dir() and receipt.checkpoints.is_dir())
        self.assertFalse(receipt.root.is_relative_to(self.cache_root))
        self.assertEqual(self.manager.status(self.plan)["phase"], "READY")
        self.assertEqual(len(self.pins()), 3)

    def test_known_offline_training_uses_workspace_and_keeps_outputs_after_unpin(self):
        with self.manager.attempt(self.plan, authorize=lambda plan: True, quiescent=lambda identity: True) as receipt:
            self.manager.mark_started(self.plan, authorize=lambda plan: True)
            self.assertEqual(len(self.pins()), 3)
            result = subprocess.run([sys.executable, str(receipt.source / "train.py"),
                str(receipt.data / "shard/rows.csv"), str(receipt.outputs), str(receipt.checkpoints)],
                cwd=receipt.source, capture_output=True, text=True, timeout=5)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(json.loads(result.stdout), {"rows":2,"sum":4})
        self.assertEqual(self.pins(), [])
        self.assertEqual(json.loads((receipt.outputs / "result.json").read_text()), {"rows":2,"sum":4})
        self.assertTrue((receipt.checkpoints / "candidate.json").is_file())
        self.assertEqual(self.manager.status(self.plan)["outcome"], "COMPLETED")
        self.assertEqual((self.cache_root / "objects" / object_key(self.rows_sha)).read_bytes(), ROWS)

    def test_fake_nfs_fetch_to_private_workspace_runs_known_offline_program(self):
        from agent.cache_storage import nfs_cache_fetcher
        nfs, cache_root, private_root = (self.base / name for name in ("fake-nfs", "fetched-cache", "fetched-private"))
        for root in (nfs, cache_root, private_root): root.mkdir(mode=0o700)
        for file in self.plan.inputs():
            target = nfs / object_key(file.sha256)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes((self.cache_root / "objects" / object_key(file.sha256)).read_bytes())
        cache = local_cache(cache_root, input_mount=nfs)
        fetch = nfs_cache_fetcher(cache, source="192.0.2.10:/srv/ml-shared/objects", quota_bytes=10000000,
                                  reserve_free_bytes=0, io_timeout=3)
        self.addCleanup(fetch.close)
        fetch.initialize()
        helper = """
import json,sys
from unittest.mock import patch
sys.path.insert(0,sys.argv[1])
from hybrid_storage.cache_copy_worker import copy_request
with patch('hybrid_storage.inputs.require_input_mount',return_value={'mount_id':42}), \
     patch('hybrid_storage.inputs.fd_mount_id',return_value=42):
    print(json.dumps(copy_request(**json.loads(sys.argv[2]))),flush=True)
"""
        def spawn(request, output, lock):
            return subprocess.Popen([sys.executable,"-I","-B","-c",helper,str(ROOT),json.dumps(request)],
                stdout=output, stderr=subprocess.DEVNULL, pass_fds=(request['output_fd'],lock))
        with patch.object(fetch, "_spawn", side_effect=spawn):
            self.assertEqual(len(fetch.ensure(self.plan.inputs(), authorize=lambda inputs: inputs == self.plan.inputs())), 3)
        manager = private_workspaces(cache, private_root, max_input_bytes=10000000, reserve_free_bytes=0)
        manager.initialize()
        with manager.attempt(self.plan, authorize=lambda plan: True, quiescent=lambda identity: True) as receipt:
            manager.mark_started(self.plan, authorize=lambda plan: True)
            result = subprocess.run([sys.executable,str(receipt.source/'train.py'),str(receipt.data/'shard/rows.csv'),
                str(receipt.outputs),str(receipt.checkpoints)], capture_output=True, text=True, timeout=5)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(json.loads(result.stdout), {'rows':2,'sum':4})
        self.assertTrue(all(not record.pinned_references for record in cache.inventory()))

    def test_existing_lease_watchdog_stops_real_group_before_expiry_pin_release(self):
        guardian, children = None, []
        def stopped(identity):
            if guardian is None: return True
            if guardian.poll() is None: return False
            for child in children:
                try:
                    if child.is_running() and child.status() not in (psutil.STATUS_ZOMBIE,psutil.STATUS_DEAD):
                        return False
                except psutil.NoSuchProcess: pass
            return True
        try:
            with self.assertRaises(WorkspaceLeaseExpired):
                with self.manager.attempt(self.plan, authorize=lambda plan: True, quiescent=stopped):
                    self.manager.mark_started(self.plan, authorize=lambda plan: True)
                    now = time.time()
                    lease = dict(job_id='job1',attempt_token='job1:0',node_id='test-node',generation=1,
                                 lease_id=uuid.uuid4().hex,issued_at=now,expires_at=now+1)
                    worker_root = self.base / 'disposable-guardian'
                    leases.accept(worker_root,lease,'job1','job1:0','test-node')
                    guardian = subprocess.Popen([sys.executable,str(ROOT/'agent/lease_runner.py'),
                        '--lease-path',str(leases.lease_path(worker_root,lease)),'--',sys.executable,'-c',
                        'import time;time.sleep(30)'], start_new_session=True,
                        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    deadline = time.monotonic()+0.7
                    while not children and guardian.poll() is None and time.monotonic()<deadline:
                        children = psutil.Process(guardian.pid).children(recursive=True)
                        time.sleep(0.01)
                    self.assertTrue(children)
                    pending = self.manager.finish(self.plan,'LEASE_EXPIRED',quiescent=stopped)
                    self.assertEqual(pending['phase'],'ABORTING')
                    self.assertEqual(len(self.pins()),3)
                    self.assertEqual(guardian.wait(timeout=6),-9)
                    deadline = time.monotonic()+1
                    while not stopped(self.plan.identity) and time.monotonic()<deadline: time.sleep(0.01)
                    self.assertTrue(stopped(self.plan.identity))
                    raise WorkspaceLeaseExpired('disposable existing watchdog expired')
            self.assertEqual(self.pins(),[])
            self.assertEqual(self.manager.status(self.plan)['outcome'],'LEASE_EXPIRED')
        finally:
            if guardian is not None and guardian.poll() is None:
                os.killpg(guardian.pid,9)
                guardian.wait(timeout=3)

    def test_mutating_one_private_copy_cannot_change_cache_or_another_attempt(self):
        first = self.prepare()
        other = replace(self.plan, identity=AttemptIdentity("job1", "job1:1"))
        second = self.prepare(other)
        file = first.data / "shard/rows.csv"
        file.chmod(0o600)
        file.write_bytes(b"private edits")
        self.assertEqual((second.data / "shard/rows.csv").read_bytes(), ROWS)
        self.assertEqual((self.cache_root / "objects" / object_key(self.rows_sha)).read_bytes(), ROWS)
        self.finish(outcome="FAILED")
        self.assertEqual({ref for sha, ref in self.pins()}, {("job1",self.manager.pin_holder(other))})
        self.finish(other)

    def test_two_workspace_roots_sharing_cache_have_independent_pin_holders(self):
        self.prepare()
        other_root = self.base / 'other-private-root'
        other_root.mkdir(mode=0o700)
        other = private_workspaces(self.cache,other_root,max_input_bytes=10000000,reserve_free_bytes=0)
        other.initialize()
        other.prepare(self.plan,authorize=lambda plan:True)
        self.assertEqual(len(self.pins()),6)
        self.finish()
        self.assertEqual(len(self.pins()),3)
        other.mark_started(self.plan,authorize=lambda plan:True)
        other.finish(self.plan,'COMPLETED',quiescent=lambda identity:True)
        self.assertEqual(self.pins(),[])

    def test_existing_control_cannot_be_rebound_to_a_different_cache_root(self):
        self.prepare()
        other_root = self.base/'other-cache'
        other_root.mkdir(mode=0o700)
        other_cache = local_cache(other_root,input_mount=self.mount)
        other_cache.initialize()
        other = private_workspaces(other_cache,self.root,max_input_bytes=10000000,reserve_free_bytes=0)
        before = self.control().read_bytes()
        with self.assertRaises(WorkspaceError): other.prepare(self.plan,authorize=lambda plan:True)
        with self.assertRaises(WorkspaceError): other.finish(self.plan,'FAILED',quiescent=lambda identity:True)
        self.assertEqual(self.control().read_bytes(),before)
        self.assertEqual(len(self.pins()),3)

    def test_identical_ready_replay_reverifies_without_copy_or_duplicate_pins(self):
        first = self.prepare()
        inode = (first.source / "train.py").stat().st_ino
        with patch.object(self.manager, "_copy_file", side_effect=AssertionError("no copy")):
            second = self.prepare()
        self.assertEqual(first, second)
        self.assertEqual((second.source / "train.py").stat().st_ino, inode)
        self.assertEqual(len(self.pins()), 3)

    def test_plan_rebinding_is_rejected_without_releasing_original_pins(self):
        self.prepare()
        changed = replace(self.plan, input_manifest_sha256="a" * 64)
        before = self.control().read_bytes()
        with self.assertRaises(WorkspaceError):
            self.prepare(changed)
        with self.assertRaises(WorkspaceError):
            self.finish(changed)
        self.assertEqual(self.control().read_bytes(), before)
        self.assertEqual(len(self.pins()), 3)

    def test_cancel_before_staging_is_durable_tombstone_against_delayed_prepare(self):
        record = self.finish(outcome="CANCELLED")
        self.assertTrue(record["pins_released"])
        with self.assertRaises(WorkspaceBusy):
            self.prepare()
        self.assertEqual(self.pins(), [])
        self.assertEqual(list((self.root / "attempts").iterdir()), [])

    def test_every_explicit_terminal_reason_releases_only_its_exact_attempt(self):
        for index, outcome in enumerate(sorted(OUTCOMES)):
            plan = replace(self.plan, identity=AttemptIdentity("job1", f"job1:{index}"))
            with self.subTest(outcome=outcome):
                receipt = self.prepare(plan)
                self.manager.mark_started(plan, authorize=lambda plan: True)
                finished = self.finish(plan, outcome)
                self.assertEqual(finished["phase"], "TERMINAL")
                self.assertEqual(finished["outcome"], outcome)
                self.assertEqual(self.pins(), [])
                self.assertTrue(receipt.root.is_dir())
                with self.assertRaises(WorkspaceBusy):
                    self.prepare(plan)

    def test_managed_context_maps_failure_cancel_expiry_launch_and_interrupt_to_cleanup(self):
        for index, (error, outcome) in enumerate(((RuntimeError,"FAILED"), (WorkspaceCancelled,"CANCELLED"),
                (WorkspaceLeaseExpired,"LEASE_EXPIRED"), (WorkspaceLaunchFailed,"LAUNCH_FAILED"),
                (KeyboardInterrupt,"FAILED"), (SystemExit,"FAILED"))):
            plan = replace(self.plan, identity=AttemptIdentity("job1", f"job1:{index}"))
            with self.subTest(error=error), self.assertRaises(error):
                with self.manager.attempt(plan, authorize=lambda plan: True, quiescent=lambda identity: True):
                    raise error("fixture")
            self.assertEqual(self.manager.status(plan)["outcome"], outcome)
            self.assertEqual(self.pins(), [])

    def test_active_descendants_keep_pins_until_quiescence_is_proven(self):
        self.prepare()
        self.manager.mark_started(self.plan, authorize=lambda plan: True)
        record = self.finish(outcome="CANCELLED", proof=False)
        self.assertEqual(record["phase"], "ABORTING")
        self.assertEqual(len(self.pins()), 3)
        with self.assertRaises(WorkspaceBusy):
            self.manager.mark_started(self.plan, authorize=lambda plan: True)
        finished = self.finish(outcome="COMPLETED", proof=True)
        self.assertEqual(finished["outcome"], "CANCELLED")
        self.assertEqual(self.pins(), [])

    def test_managed_success_without_quiescence_fails_closed_with_pins_retained(self):
        with self.assertRaises(WorkspaceBusy):
            with self.manager.attempt(self.plan, authorize=lambda plan: True, quiescent=lambda identity: False):
                pass
        self.assertEqual(self.manager.status(self.plan)["phase"], "ABORTING")
        self.assertEqual(len(self.pins()), 3)
        self.finish()

    def test_quiescence_requires_explicit_true_not_truthy_or_missing(self):
        self.prepare()
        for proof in (1, None, False):
            self.assertFalse(self.finish(proof=proof)["pins_released"])
            self.assertEqual(len(self.pins()), 3)
        self.finish()

    def test_cache_unpin_error_leaves_terminal_tombstone_and_retry_finishes_cleanup(self):
        self.prepare()
        with patch.object(self.cache, "unpin", side_effect=OSError(errno.EIO, "local metadata error")):
            with self.assertRaises(OSError):
                self.finish(outcome="FAILED")
        record = self.manager.status(self.plan)
        self.assertEqual(record["phase"], "TERMINAL")
        self.assertFalse(record["pins_released"])
        self.assertEqual(len(self.pins()), 3)
        self.assertTrue(self.finish(outcome="COMPLETED")["pins_released"])
        self.assertEqual(self.manager.status(self.plan)["outcome"], "FAILED")
        self.assertEqual(self.pins(), [])

    def test_failure_after_unpin_before_marker_is_safely_idempotent(self):
        self.prepare()
        original = self.manager._save
        def fail(state, plan, record, **options):
            if record["pins_released"]:
                raise OSError(errno.EIO, "modeled final marker fsync failure")
            return original(state, plan, record, **options)
        with patch.object(self.manager, "_save", side_effect=fail), self.assertRaises(OSError):
            self.finish()
        self.assertEqual(self.pins(), [])
        self.assertFalse(self.manager.status(self.plan)["pins_released"])
        self.assertTrue(self.finish()["pins_released"])

    def test_failed_partial_pinning_releases_only_new_holder(self):
        self.prepare()
        other = replace(self.plan, identity=AttemptIdentity("job1", "job1:1"))
        (self.cache_root / "objects" / object_key(self.rows_sha)).unlink()
        with self.assertRaises(CacheMiss):
            self.prepare(other)
        self.assertEqual(self.manager.status(other)["outcome"], "STAGING_FAILED")
        self.assertEqual({ref for sha, ref in self.pins()}, {("job1",self.manager.pin_holder(self.plan))})

    def test_enospc_during_copy_never_yields_ready_and_releases_pins(self):
        original = workspaces.os.fdopen
        def opened(fd, mode):
            stream = original(fd, mode)
            class Writer:
                def __enter__(self): return self
                def __exit__(self, *args): stream.close()
                def __getattr__(self, name): return getattr(stream, name)
                def write(self, chunk): raise OSError(errno.ENOSPC, "modeled full workspace disk")
            return Writer() if mode == "w+b" else stream
        with patch.object(workspaces.os, "fdopen", side_effect=opened), self.assertRaises(OSError):
            self.prepare()
        self.assertEqual(self.manager.status(self.plan)["outcome"], "STAGING_FAILED")
        self.assertEqual(self.pins(), [])
        self.assertFalse((self.root / "attempts" / self.plan.identity.key).exists())
        self.assertTrue(list((self.root / "attempts").glob(".building-*")))

    def test_wrong_destination_bytes_are_readback_rejected_before_ready(self):
        original = workspaces.os.fdopen
        def opened(fd, mode):
            stream = original(fd,mode)
            class Writer:
                def __enter__(self): return self
                def __exit__(self,*args): stream.close()
                def __getattr__(self,name): return getattr(stream,name)
                def write(self,chunk): return stream.write(b'X'*len(chunk))
            return Writer() if mode=='w+b' else stream
        with patch.object(workspaces.os,'fdopen',side_effect=opened), self.assertRaises(ValueError):
            self.prepare()
        self.assertEqual(self.pins(),[])
        self.assertEqual(self.manager.status(self.plan)['outcome'],'STAGING_FAILED')
        self.assertFalse((self.root/'attempts'/self.plan.identity.key).exists())

    def test_authority_revocation_during_copy_aborts_without_ready_or_pins(self):
        allowed = True
        original = self.manager._copy_file
        def revoked(*args):
            nonlocal allowed
            allowed = False
            return original(*args)
        with patch.object(self.manager, "_copy_file", side_effect=revoked), self.assertRaises(PermissionError):
            self.prepare(authorize=lambda plan: allowed)
        self.assertEqual(self.pins(), [])
        self.assertEqual(self.manager.status(self.plan)["outcome"], "STAGING_FAILED")

    def test_unauthorized_initial_request_performs_no_state_or_pin_work(self):
        with self.assertRaises(PermissionError):
            self.prepare(authorize=lambda plan: False)
        self.assertFalse(self.control().exists())
        self.assertEqual(self.pins(), [])

    def test_private_input_tamper_before_launch_is_detected_and_context_unpins(self):
        with self.assertRaises(WorkspaceError):
            with self.manager.attempt(self.plan, authorize=lambda plan: True, quiescent=lambda identity: True) as receipt:
                target = receipt.source / "train.py"
                target.chmod(0o600)
                target.write_bytes(b"changed")
                self.manager.mark_started(self.plan, authorize=lambda plan: True)
        self.assertEqual(self.pins(), [])
        self.assertEqual(self.manager.status(self.plan)["outcome"], "FAILED")

    def test_private_symlink_or_hardlink_is_rejected_without_following_it(self):
        for index, kind in enumerate(("symlink", "hardlink")):
            plan = replace(self.plan, identity=AttemptIdentity("job1", f"job1:{index}"))
            receipt = self.prepare(plan)
            private = receipt.data / "shard/rows.csv"
            private.unlink()
            cached = self.cache_root / "objects" / object_key(self.rows_sha)
            if kind == "symlink": private.symlink_to(cached)
            else: os.link(cached, private)
            with self.subTest(kind=kind), self.assertRaises((OSError, WorkspaceError)):
                self.manager.mark_started(plan, authorize=lambda plan: True)
            self.finish(plan, "FAILED")
            private.unlink()  # Only the fixture alias; leave original cache intact.

    def test_unexpected_private_input_on_ready_replay_aborts_and_keeps_for_review(self):
        receipt = self.prepare()
        extra = receipt.source / "unexpected.txt"
        extra.write_bytes(b"retain")
        with self.assertRaises(WorkspaceError):
            self.prepare()
        self.assertEqual(self.pins(), [])
        self.assertEqual(extra.read_bytes(), b"retain")

    def test_space_admission_counts_duplicate_private_copies_not_unique_cache_bytes(self):
        repeated = replace(self.plan.source, files=self.plan.source.files + (
            SourceFile("alias.csv", self.rows_sha, len(ROWS)),))
        plan = replace(self.plan, source=repeated)
        handle = self.handle(max_input_bytes=self.plan.input_bytes)
        with self.assertRaises(OSError) as error:
            self.prepare(plan, handle)
        self.assertEqual(error.exception.errno, errno.ENOSPC)
        self.assertEqual(self.pins(), [])

    def test_insufficient_available_disk_aborts_before_pin_or_copy(self):
        values = os.statvfs_result((4096,4096,100,10,0,100,10,10,0,255))
        with patch.object(workspaces.os, "fstatvfs", return_value=values), self.assertRaises(OSError):
            self.prepare()
        self.assertEqual(self.pins(), [])
        self.assertEqual(self.manager.status(self.plan)["outcome"], "STAGING_FAILED")

    def test_no_dataset_and_empty_source_dirs_are_preserved_without_fabricated_files(self):
        plan = replace(self.plan, dataset=None)
        receipt = self.prepare(plan)
        self.assertIsNone(receipt.data)
        self.assertTrue((receipt.source / "empty").is_dir())
        self.assertEqual(list((receipt.root / "inputs/data").iterdir()), [])
        self.finish(plan)

    def test_rank_and_revision_get_separate_paths_and_exact_pins(self):
        receipts = []
        plans = [self.plan, replace(self.plan, identity=replace(self.plan.identity, rank=1)),
                 replace(self.plan, identity=replace(self.plan.identity, stage_revision=2))]
        for plan in plans: receipts.append(self.prepare(plan))
        self.assertEqual(len({receipt.root for receipt in receipts}), 3)
        self.assertEqual(len(self.pins()), 9)
        self.finish(plans[0], "SUPERSEDED")
        self.assertEqual(len(self.pins()), 6)

    def test_independent_concurrent_handles_prepare_one_private_workspace(self):
        second = self.handle()
        with patch.object(self.manager, "_copy_file", wraps=self.manager._copy_file) as first_copy, \
                patch.object(second, "_copy_file", wraps=second._copy_file) as second_copy:
            with ThreadPoolExecutor(max_workers=2) as executor:
                a = executor.submit(self.prepare)
                b = executor.submit(self.prepare, manager=second)
                self.assertEqual(a.result(timeout=10), b.result(timeout=10))
        self.assertEqual(first_copy.call_count + second_copy.call_count, 3)
        self.assertEqual(len(self.pins()), 3)

    def test_process_crash_after_pins_retains_control_until_explicit_reconciliation(self):
        result = self.process("manager._build=lambda *args: os._exit(17)\nmanager.prepare(plan,authorize=lambda plan:True)\n")
        self.assertEqual(result.returncode, 17, result.stderr)
        self.assertEqual(self.manager.status(self.plan)["phase"], "MATERIALIZING")
        self.assertEqual(len(self.pins()), 3)
        with self.assertRaises(WorkspaceBusy):
            self.prepare()
        self.assertFalse(self.finish(outcome="RESTART_ABORTED", proof=False)["pins_released"])
        self.assertTrue(self.finish(outcome="RESTART_ABORTED", proof=True)["pins_released"])
        self.assertEqual(self.pins(), [])

    def test_ready_workspace_and_pins_survive_fresh_process_without_automatic_clear(self):
        receipt = self.prepare()
        result = self.process("r=manager.prepare(plan,authorize=lambda plan:True);print(str(r.root))\n")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), str(receipt.root))
        self.assertEqual(len(self.pins()), 3)

    def test_busy_exact_attempt_lock_never_releases_existing_pins(self):
        self.prepare()
        path = self.root / "state" / (self.plan.identity.key + ".lock")
        fd = os.open(path, os.O_RDWR)
        self.addCleanup(os.close, fd)
        workspaces.fcntl.flock(fd, workspaces.fcntl.LOCK_EX)
        with patch.object(workspaces.time, "monotonic", side_effect=(0,10)), self.assertRaises(WorkspaceBusy):
            self.finish()
        workspaces.fcntl.flock(fd, workspaces.fcntl.LOCK_UN)
        self.assertEqual(len(self.pins()), 3)

    def test_corrupt_control_cannot_release_pins_or_escape_paths(self):
        self.prepare()
        record = json.loads(self.control().read_bytes())
        record["work_name"] = "../../escape"
        self.control().write_bytes(encoded(record))
        with self.assertRaises(WorkspaceError): self.manager.status(self.plan)
        with self.assertRaises(WorkspaceError): self.finish()
        self.assertEqual(len(self.pins()), 3)

    def test_control_symlink_is_refused_without_modifying_target(self):
        retained = self.base / "retain"
        retained.write_bytes(b"retain")
        self.control().symlink_to(retained)
        with self.assertRaises(OSError): self.prepare()
        self.assertEqual(retained.read_bytes(), b"retain")
        self.assertEqual(self.pins(), [])

    def test_existing_unowned_attempt_directory_is_never_overwritten_or_deleted(self):
        existing = self.root / "attempts" / self.plan.identity.key
        existing.mkdir(mode=0o700)
        retained = existing / "retain"
        retained.write_bytes(b"retain")
        with self.assertRaises(FileExistsError): self.prepare()
        self.assertEqual(retained.read_bytes(), b"retain")
        self.assertEqual(self.pins(), [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
