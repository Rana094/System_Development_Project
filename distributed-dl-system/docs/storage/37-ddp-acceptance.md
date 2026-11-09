# Phase 9.4 — local DDP fault matrix and physical acceptance gate

9 November 2026, Asia/Dhaka. **Local matrix PASS: 173 tests, 14 files.
Overall Phase 9 acceptance NOT PASSED: physical all-three-Pi validation is pending.**
Only 9.4 was performed. See [evidence](37-ddp-acceptance-evidence.json) and
[9.3 boundary](36-ddp-checkpoint-publication.md). STOP after this report.

## 1. Behavior, repository preservation and exact changes

Before edits, 301 inventoried files and Git status were saved under
`/tmp/hybrid-9-4/before`, `before.json`, `status-before.txt`. Git root is the
application's parent `/home/rana/Projects/System Project/System_Development_Project`,
branch `main`, HEAD `de297dc7d99eb80e52498145fe05b033e5afdaad`. Existing uncommitted
9.1–9.3 work is preserved. No stash/reset/commit, student data move or live DB
import/write was performed.

| File | Change |
|---|---|
| `controller/job_store.py::mark_completed` | After exact hybrid final-checkpoint/all-rank quiescence validation, mark only the current DDP group's rank rows and distributed-attempt phase COMPLETED in the existing job-completion transaction. |
| `tests/test_ddp_acceptance.py` | Six new local fault/recovery/negative cases, including native Gloo and independent-controller SQLite races. |
| `tests/test_hybrid_checkpoint_ddp_resume.py` | Parameterize existing fixture cardinality (default three), pause markers and rank-indexed state checks; keep existing three-rank case and tolerances. |
| `tests/test_hybrid_checkpoint_recovery.py` | Optional test-only `ddp(workers=3)` forwards the existing fixture worker count; previous 9.2/9.3 edits preserved. |
| `tests/README.md` | New matrix command and hardware boundary. |
| `docs/storage/37-ddp-acceptance.md`, `37-ddp-acceptance-evidence.json` | Actual outcomes, readiness, safe future hardware plan and rollback. |
| `docs/storage/implementation-progress.md` | Append-only 9.4 local/hardware status. |

The startup-crash/retry test exposed a pre-existing hybrid history bug: the job
and group could complete while current `job_ranks` still said RUNNING. The eight-line
production fix completes those rows and the group atomically with the job/history,
lease revocation and reservation release. Earlier retries retain STOPPED/FAILED
history. It does not alter eligibility, fairness, reservations, cancellation,
world-size rules, checkpoint writer/format, publication protocol, schema or flags.
Existing legacy and single-worker completion branches retain their behavior.

All native faults target only child PIDs created and retained by the test. Other
ranks are explicitly reaped before fixture containment callbacks return true.
The outer agent launcher/isolation, NFS facts, signed Flask transport and result
collection remain fixtures. Native training, Gloo collectives, SDK save/load,
Torch state and process death are real; this is not physical Pi/NFS isolation proof.

## 2. Required matrix and evidence levels

| Requirement | Local evidence |
|---|---|
| Two-rank DDP | New native committed full-group crash/resume plus existing native legacy/group tests. |
| Three-rank DDP | Existing real three-rank hybrid crash/resume and new native startup/ACK crash scenarios. |
| Missing data | Existing `test_ddp_staging.py::test_one_missing_object_aborts_whole_group_without_running`: no START/Popen, all-rank cleanup. |
| Rank crash before training | New native rank-one pause before process-group creation, owned SIGKILL, no rank reaches `last-step`; group retry completes on the same fixed world. |
| Rank crash during checkpoint | New rank-one pause during step-two ACK; SIGKILL preserves step-one latest and controller COMMITTED head, then resumes all ranks from that valid cut. |
| Partial checkpoint rejected | Existing 9.3 member/hash/ACK/fence tests and `test_hybrid_checkpoint_faults.py::test_incomplete_new_ddp_bundle_never_displaces_last_recoverable_commit`. |
| Full-group recovery | New native two/three-rank state comparisons, warm input reuse, all-rank restore ACK before RUNNING, one final authoritative commit/results completion. |
| Stale rank fenced | Existing stale-DDP-result test; new ACK-crash case rejects old `note_exit` success and old lease result publication; committed head retained. |
| Unsupported world-size change | New actual two-rank SDK refuses a real three-rank archive before training or resume ACK; controller rejects changed immutable worker count before new reservation/start. No elastic resharding. |
| Competing recovery | Two independent SQLite coordinator connections race a complete three-rank retry: one group reservation/stage, three starts, one restore-confirmation event. |
| Other students/jobs safe | Every new native scenario preserves a Bob-owned HELD job's complete DB row, input/result file hashes and event count; collection targets only the faulted job. |
| Completion metadata consistency | Current ranks COMPLETED, prior retry STOPPED, group/job complete and reservations released under the existing fence. |

The private `.ddp-stage-*` step-two proposal can be structurally complete and
contain proposed ACK metadata while the real ACK collective is unfinished. It is
not an SDK-committed save and is never exposed by the trusted latest-only route.
The test verifies that distinction; it does not claim structural validation alone
proves actual collective participation or honest student training.

## 3. Tests, exact commands and development failures

Baseline before changes: **31 existing tests PASS**, 16 staging + 15 publication,
two files, zero failures/errors/skips:

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_ddp_staging.py test_ddp_checkpoint_publication.py
```

Executed native/regression commands (application root):

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_ddp_acceptance.py test_hybrid_checkpoint_ddp_resume.py test_group_checkpoint.py test_group_recovery.py test_distributed_dispatch.py
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_ddp_staging.py test_ddp_checkpoint_publication.py test_hybrid_checkpoint_faults.py test_hybrid_checkpoint_recovery.py test_staging_recovery.py test_storage_config.py test_queue_controls.py test_resource_allocation.py test_execution_ownership.py
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_ddp_acceptance.py
git diff --check
```

Final per-file results: **173 distinct tests PASS across 14 files**, zero
remaining failures/errors/skips. Counts are not summed across repeated runs:

| File | Latest PASS count |
|---|---:|
| `test_ddp_acceptance.py` | 6 |
| `test_hybrid_checkpoint_ddp_resume.py` | 1 |
| `test_group_checkpoint.py` | 8 |
| `test_group_recovery.py` | 7 |
| `test_distributed_dispatch.py` | 7 |
| `test_ddp_staging.py` | 16 |
| `test_ddp_checkpoint_publication.py` | 15 |
| `test_hybrid_checkpoint_faults.py` | 16 |
| `test_hybrid_checkpoint_recovery.py` | 18 |
| `test_staging_recovery.py` | 34 |
| `test_storage_config.py` | 17 |
| `test_queue_controls.py` | 10 |
| `test_resource_allocation.py` | 6 |
| `test_execution_ownership.py` | 12 |

The nine-file regression command passed 144 tests, 9/9 files. The final six-case
command passed 6/6, 1/1 file. The four existing native files passed 23 tests in the
combined run. Whitespace check passed. Native two-rank resumed/control differences
were empty. The new three-rank ACK-crash resume differed only in optimizer
`state.1.exp_avg`, maximum absolute `8.881784197001252e-16` per rank. The existing
three-rank cut/resume retained its prior `1.862645149230957e-09` maximum delta in
`state.4.exp_avg_sq`. All final floating model/optimizer comparisons use unchanged
`atol=1e-8, rtol=1e-7`; other state/progress/RNG assertions remain exact.

The initial
combined native command retained one failing new-test file; its four existing
files passed. The final targeted command replaces only that file's earlier result,
without recounting passing cases or hiding development failures. Native commands
use reviewed localhost sockets. Regression processes use isolated source copies,
temporary DB/files and mocked cluster calls. No benchmark is fabricated.

Development history: the first new matrix run found the real rank-history bug
and a fixture selecting two ranks while intending to generate a three-rank source.
After fixing both, a cleanup edit removed `subprocess`, still needed for the new
negative process test's stderr handle. That import was restored and the affected
six-case file rerun. Earlier logs remain under `/tmp/hybrid-9-4/` and are hashed in
the evidence. Existing assertions/tolerances were not weakened.

## 4. Actual read-only cluster readiness

At `2026-11-09T08:49:51.185509+00:00` the bounded GET/SSH probe read the three
configured hosts. Addresses come from `config/nodes.json`, not guesses. It used
strict existing SSH host keys, BatchMode, no connection persistence/local command,
timeouts and `python3 -B`; no remote files or services were changed.

| Node | Actual result |
|---|---|
| pi01 — `100.101.90.46` | GET health/resources timed out; SSH exit 255, connection timeout. Power state, LAN address, deployed runtime and mount readiness unknown. |
| pi02 — `100.91.135.18` | Matching identity, IDLE, active `dl-node-agent.service`, aarch64. Service root `/home/pi/distributed-dl-system`; User=pi, PrivateMounts=no. |
| pi03 — `100.64.202.71` | Matching identity, IDLE, same observed service root/account/namespace properties. |

On both reachable service roots, `agent/agent.py` differs from this checkout;
the existing SDK/validator hashes match, but `agent/staging.py`,
`agent/checkpoint_service.py`, `hybrid_storage/staging_protocol.py` and
`hybrid_storage/worker_cache.py` are absent. This does not establish a hybrid
runtime elsewhere; none was identified or enabled. Health reports agent Python
3.11.16; the SSH system interpreter reports 3.11.2. Prepared worker PyTorch and
environment compatibility are not inferred from either Python version.

The only observed active NFS filesystem on each is the legacy RW
`192.168.10.150:/srv/nfs/cifar10_data` at `/mnt/nfs_share`, NFS4.2/hard/AUTH_SYS.
A follow-up read-only unit query found **loaded active/waiting**
`srv-mlnfs-inputs.automount`, and loaded but inactive/dead
`srv-mlnfs-inputs.mount` and `ml-nfs-ready.service`, Result=success. This is not
missing automount configuration and not proof of currently mounted RO input
availability. No path access was used to activate it; no read/write artifact probe,
mount/unmount, service restart or live rank failure was attempted.

Readiness transcripts, script source, commands and SHA-256 values are embedded
in the evidence. Raw probe files are `/tmp/hybrid-9-4/readiness.py`,
`readiness.json`, `nfs-unit-readiness.json`. These are observations at probe time,
not a reservation or claim that the workers remain idle.

## 5. Reviewable physical-test prerequisites and safe plan — not applied

The user's standing rule requires explicit approval before Pi deployment or live
exports/fstab/firewall/SSH/systemd changes. This subphase does not reinterpret the
earlier Phase 2 two-node exception as all-three-node Phase 9 acceptance. Do not
substitute pi02/pi03-only runs, reboot a node, relax SSH keys or enable a shared RW
trainer mount to manufacture a PASS.

1. Restore pi01 reachability and verify its real identity/LAN address/runtime.
   Recheck all three nodes, clock bounds, actual interfaces and idle reservations.
   Keep admissions/active student jobs safe using existing authorized queue controls;
   obtain an agreed disposable test window before live fault injection.
2. Review an exact source/hash deployment for **all three**. The existing
   `tools/cluster_setup.py::bundle` packages agent/training/config, not the new
   `hybrid_storage` package; it cannot be assumed to deploy this feature. A separately
   reviewed deployment bundle/bootstrap must include dependencies and all required
   modules, protected LOCAL cache/workspace/checkpoint-spool roots, provisioned
   per-worker credentials and reviewed OS/network isolation/termination/quiescence
   callbacks. No same-UID isolation fallback is permitted. This is a prerequisite,
   not a deployment script supplied or executed in this validation subphase.
3. Review current controller export/client plans and verified addresses, retaining
   restricted RO inputs/root_squash and controller-LOCAL SQLite/checkpoint storage.
   Extend to pi01 explicitly if needed. Back up relevant config and source before
   an approved change; preserve legacy mounts/data and student work. Prepared native
   environments/weights/wheels must be available without Internet runtime transfers.
4. With reviewed bootstrap and all three nodes ready, submit only uniquely named
   disposable authorized jobs through existing APIs: native two-rank run plus
   unrelated work on the third, then three ranks, full immutable inputs per rank.
   For the primary classifier choose an explicit compatible batch size (e.g. 96
   for a new three-rank job), never mutate an existing job's saved world/batch.
5. Capture manifests/source hashes, selected workers/ranks, lease generations,
   STAGING_READY evidence, verified bytes, committed save ID/head and baseline
   metrics. Inject missing data/startup/ACK faults only into positively identified
   test jobs/PIDs. Verify the old committed cut, whole-group stop/fence/re-stage,
   all-rank SDK resume and final state/results; retain other-job hashes/status.
   Test stale ranks and unsupported world changes explicitly. Record actual timings
   without asserting a speedup or reusing local observations as hardware evidence.
6. Roll back test enrollment/bootstrap/source/config using reviewed backups only
   after all test descendants are quiescent or safely fenced. Restore only changed
   test/service configuration; retain jobs, checkpoints, DB/history and unknown files.
   No `rm -rf`, wildcard process kill, production unmount or power-cycle is part of
   this plan. Approve exact commands/targets before applying a live deployment.

Manual read-only checks (no service or mount activation):

```bash
for node in pi01 pi02 pi03; do
  ssh -T -o BatchMode=yes -o ConnectTimeout=5 -o StrictHostKeyChecking=yes \
    -o UpdateHostKeys=no -o ControlMaster=no -o ControlPath=none \
    -o PermitLocalCommand=no "$node" \
    'hostname; systemctl show dl-node-agent.service --no-pager -p ActiveState -p WorkingDirectory -p User -p PrivateMounts; findmnt -t nfs,nfs4 -o SOURCE,TARGET,FSTYPE,OPTIONS'
done
```

Expect the observations above only if external state remains unchanged. An active
automount is not permission to start training. Do not add sudo or deploy commands
to force these probes to succeed. No administrator or auto-review rejection occurred.

## 6. Rollback, security/config and acceptance

No API, DB schema/migration, checkpoint format, transport, credential, scheduler
policy, configuration key or legacy default changes. SQLite remains local, NFS
inputs read-only under trusted staging, and trainers receive no new NFS RW access.
Only authenticated fenced hybrid DDP completion updates current terminal rank/group
history; queued/running/earlier attempts and other jobs are preserved.

Source rollback is scoped to this turn, preserving all prior 9.1–9.3 edits. Keep
later edits and quiesce/hold enrolled hybrid work before switching deployed code.
From the Git root, after reviewing `/tmp/hybrid-9-4/9.4.patch`:

```bash
git apply --reverse --check /tmp/hybrid-9-4/9.4.patch
git apply --reverse /tmp/hybrid-9-4/9.4.patch
```

Only the reverse-check is run here. No schema downgrade, mount/service rollback
or data/cache/checkpoint deletion is needed. Reversion restores the old terminal
rank-history bug for subsequent hybrid completions. All historical data remains.

| Acceptance criterion | Status |
|---|---|
| Requested local two/three-rank fault, partial/stale/world-size and recovery matrix | PASS |
| Existing DDP, cache/publication/recovery, queue/resource/lease/OFF regressions | PASS |
| Current completion history consistent; prior attempts and other held student job preserved | PASS |
| Prior changes/data preserved; no unintended remote/live mutation | PASS |
| Physical pi01/pi02/pi03 run plus full-group valid-checkpoint recovery | NOT PASSED / readiness blocked |
| Overall Phase 9 acceptance | NOT PASSED / pending physical validation |

**The mandatory physical
three-Pi run/recovery criterion is NOT PASSED**, due to pi01 reachability and the
unreviewed/missing deployed hybrid runtime/containment prerequisites. Local faults
do not prove real NFS isolation, network/power durability, Pi memory/throughput or
deployment readiness. The central controller, LOCAL SQLite and authoritative
checkpoint disk remain a **single point of failure**. **STOP at 9.4; do not claim
Phase 9 complete.**
