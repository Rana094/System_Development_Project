# Phase 5.3 — private copies and durable attempt pins

8 November 2026, Asia/Dhaka. **5.3 local component acceptance PASS; STOP before
5.4.** Accepted [5.2](20-cache-fetch.md) and 5.1 were still uncommitted above
`da85a0c`. Earlier changes and legacy launcher/lease sources were saved under
`/tmp/hybrid-storage-5-3-before`; no stash, reset or commit. No production setup,
service, mount, remote or Pi operation occurred.

## 1. Behavior and exact files

| File | Change in 5.3 |
|---|---|
| `hybrid_storage/workspaces.py` | Typed exact plans, private full copies, durable local control, authorization checks, pin lifecycle/reconciliation. |
| `agent/cache_storage.py` | Append inactive `private_workspaces` factory; retain earlier factories. |
| `tests/test_private_workspaces.py` | 39 disposable local checks, workload/pipeline integration, process crash/restart and existing lease-watchdog expiry. |
| `docs/storage/21-private-workspaces.md`, `21-private-workspaces-evidence.json` | Contracts, actual results, commands, preservation and rollback. |
| `hybrid_storage/README.md`, `tests/README.md`, `docs/README.md`, `docs/storage/implementation-progress.md` | Navigation and precise completion boundary; earlier content retained. |

The operator provisions a service-owned 0700 local root disjoint from cache/NFS.
All paths use no-follow directory/file descriptors and supported local filesystem
admission (ext4/xfs/btrfs). Construction is inactive; explicit initialization adds:

```text
<private-root>/
  state/<identity-sha>.lock
  state/<identity-sha>.json            # durable protected control record
  attempts/.building-<sha>-<random>/   # unpublished, retained on error/crash
  attempts/<identity-sha>/
    inputs/source/                    # independent private files
    inputs/data/                      # exact declared dataset, if any
    outputs/                          # mutable result candidates
    checkpoints/                      # mutable checkpoint candidates
```

Plans reuse `SourceManifest`, `DatasetManifest` and `InputFile`. They bind the
existing `job_id:retry` token, explicit rank/stage revision, exact controller
input-manifest SHA and canonical source/dataset manifests. The trusted authority
callback must validate current owner/job/grant and each rank's actual inputs;
typed plans/digests are not authorization. No shard inference or environment
installation. Use 5.2 first for misses; this materializer consumes local cache.

Persist MATERIALIZING intent before pin acquisition. Check private-copy byte and
free-space limits, counting every logical copy, including identical-byte aliases.
Fully verify/pin each unique digest. Stream-copy into a new local inode, verify
destination size/SHA by readback, check source stability, apply source executable
metadata (0500 versus 0400), fsync and rename without replacement. Preserve empty
directories. Verify the complete exact input tree and publish the whole attempt
directory before persisting READY. Partial/corrupt/extra inputs cannot launch.
Existing unexpected directories remain untouched.

**Full byte copies are the supported fallback.** No hardlinks, reflink dependency,
bind mounts, sudo or chown. Copies work across supported local filesystems and
cost additional disk/I/O. Cache link counts stay one; changing a private copy
cannot change cached bytes or another attempt. Outputs/checkpoint candidates are
separate from input cache and still require existing trusted validation/publication.

This allocation is not a complete tenant sandbox. Current legacy training shares
the service UID, so hybrid execution remains disabled. A future launcher needs
separate OS credentials and protected filesystem/process/network boundaries,
exposing only its exact attempt and bounded candidate areas. Cache/NFS/control
roots must remain inaccessible. Ownership handoff and namespace isolation are
not installed or claimed. Tests run only known trusted fixture programs.

## 2. API, persistence, configuration and terminal handling

```python
from agent.cache_storage import private_workspaces
from hybrid_storage.workspaces import AttemptIdentity, WorkspacePlan

manager = private_workspaces(cache, operator_private_root,
    max_input_bytes=operator_private_input_limit,
    reserve_free_bytes=64 * 1024 * 1024, lock_timeout=2)
# Future trusted isolated service only, not activated here:
# manager.initialize()
# plan = WorkspacePlan(AttemptIdentity(job_id, attempt_token, rank, stage_revision),
#                      exact_job_inputs_sha, authorized_source, authorized_dataset)
# with manager.attempt(plan, authorize=current_exact_authority,
#                      quiescent=exact_guardian_and_descendants_stopped) as receipt:
#     manager.mark_started(plan, authorize=current_exact_authority)
#     existing_fenced_launcher_and_monitor(receipt)
```

`prepare` returns an immutable diagnostic receipt with private paths. Identical
READY replay rechecks inputs/pins without recopying or duplicate references.
`mark_started` requires READY, intact private inputs, exact pins and current
authority; it does not launch or itself grant execution permission. Existing
leases/guardians remain authoritative. STARTED/interrupted/terminal replay fails;
reconciliation or a new currently authorized stage revision is required.

Versioned bounded canonical control JSON follows the existing worker durable
record convention (fsync/atomic replacement), with per-identity cross-process
locks. Records bind the plan hash/body, identity, cache/workspace roots and pin
holder. Unknown/malformed records, path escapes and plan/root rebinding fail
closed. Pin holders derive from the workspace root, cache root and exact job/
attempt/rank/revision, so two roots sharing a cache cannot release each other's
pins. Controller/cache SQLite schemas remain unchanged. Durable intent precedes
pin acquisition; reboot/process restart never silently clears pins.

Local control phases MATERIALIZING, READY, STARTED, ABORTING and TERMINAL do not
change scheduler/job states. All managed terminal paths converge on `finish`:
COMPLETED, FAILED, CANCELLED, LEASE_EXPIRED, LAUNCH_FAILED, STAGING_FAILED,
SUPERSEDED and RESTART_ABORTED. First recorded terminal reason wins.

- Synchronous materialization failures unwind their copy operation, persist
  failure and release only their exact holder. Partial trees remain for review.
- Managed `attempt` maps completion, cancellation, expiry, launch failure,
  exceptions and interrupts to cleanup; the caller owns launch/monitoring.
- `finish` persists ABORTING, then requires explicit `True` proof that the exact
  materializer, guardian, trainer and descendants stopped. False/absent/truthy
  nonboolean proof retains pins. Cleanup does not require an unexpired grant.
- Persist TERMINAL before unpin; persist `pins_released=true` afterward. Failure
  retains a recoverable tombstone/marker; exact retries are idempotent even after
  unpin but before the final marker. No runtime files/outputs/checkpoints deleted.

These lifecycle hooks are not wired into legacy HTTP start/watch/stop because
that launcher cannot safely execute hybrid jobs yet. Future runtime wiring must
provide exact lease/process ownership proof, not merely a stop-request ACK.
Current-authority checks must also enforce superseding revisions and worker/
controller epochs; this subphase does not install that authenticated service.

No global flag/default, environment variable, HTTP route, DB migration, upload,
fairness/reservation or checkpoint-format change. Actual config stays **legacy /
false / false / resource** and cache-enabled execution remains rejected.

## 3. Actual tests and evidence

Baseline: **404 passed, zero failures/skips, 20/20 files**. Final selected isolated
suite: **443 passed, zero failures/skips, 21/21 files**, including **39 new cases**.
Exact final command from the application root:

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_private_workspaces.py test_cache_fetch.py test_worker_cache.py test_legacy_registration.py test_storage_registry.py test_dataset_manifests.py test_dataset_validation.py test_artifact_registration.py test_storage_readers.py test_storage_selection.py test_storage_config.py test_job_submission.py test_job_storage.py test_student_portal.py test_queue_controls.py test_resource_allocation.py test_storage_inputs.py test_storage_health.py test_storage_backends.py test_nfs_client_setup.py test_nfs_server_setup.py
```

Coverage: inode independence/mutation, modes/empty dirs, exact manifests/ranks/
revisions/root holders, duplicate-copy disk admission, ENOSPC/readback corruption,
paths/control records, revocation, all terminal outcomes, quiescence, unpin/marker
retry, concurrent handles and real process crash/restart. A known offline stdlib
program reads private CSV and writes separate result/checkpoint candidates,
returning `{"rows":2,"sum":4}` directly and through fake-NFS/5.2/5.3 integration.

An actual existing `agent/lease_runner.py` guardian runs a disposable sleeping
child in its own process group. Pins remain while the group is live; real expiry
kills it before quiescence permits release. This is a local watchdog test, not
production training/DDP or recoverable checkpoint acceptance.

Initial 34 and expanded 37 cases passed. A new two-root regression then failed
(3 references versus 6), exposing a new-code holder collision. Root-scoped
holders and persisted root bindings fixed it; the final 39 cases include both
two-root and cache-rebinding guards. No baseline failure was observed. The
[evidence](21-private-workspaces-evidence.json) retains failure and final output.

No live NFS/Pi/SSH, privileged isolation, actual machine reboot, hardware power
loss, PyTorch/DDP or throughput benchmark was run. Positive NFS facts are mocked
only in the pipeline; local copy/fsync/rename, SQLite pins, processes, workload
and watchdog behavior are actual. This is not the full repository test suite.

## 4. Safe manual verification

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_private_workspaces.py
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python -m hybrid_storage.config
git diff --check
```

Expected: **39 tests pass**, legacy config with both flags false/resource policy,
and no whitespace errors. All data/processes are disposable; no sudo or Internet.
Do not initialize the sample against live worker roots.

## 5. Rollback and residual risks

The **5.3-only** `/tmp/hybrid-storage-5-3.patch` is based on saved uncommitted
5.1/5.2 contents. Review it from the parent Git root:

```bash
git apply --reverse --check /tmp/hybrid-storage-5-3.patch
# Only after review and confirming no later overlapping edits:
# git apply --reverse /tmp/hybrid-storage-5-3.patch
```

Reverse-check passed and preserves earlier storage work. Nothing was deployed,
so no host rollback is needed. For future live use, stop admission, prove exact
producer/guardian/descendant quiescence and finish pending cleanup before removal;
retain all control records, pins, copies, outputs/checkpoints, cache and fetch
parts/quarantine. No GC, destructive sweep or runtime-data deletion is provided.

Full copies cost disk/I/O. Admission is observational, not an OS quota against
other writers; output/checkpoint capacity enforcement remains future executor
work. Local I/O is synchronous and must stay off lease/API-critical threads.
An unwritable/full state filesystem can delay durable cleanup: retain pins and
retry after recovery. Candidate checkpoints still require trusted publication.
Controller remains a **single point of failure**. Future cluster validation
requires pi01/pi02/pi03 unless expressly excepted; prior pi01 deferral remains
explicit and establishes no new live or three-node acceptance.

## 6. Acceptance boundary

**5.3 private-copy/local lifecycle PASS. STOP before 5.4.** Usable verified private
inputs have independent inodes and separate outputs/checkpoints. Pins survive
staged/running/crashed attempts and are released on managed terminal paths only
after quiescence, with durable retry markers. Tenant isolation and live-launch
wiring remain gated. Detailed metrics, consolidated Phase 5 acceptance and live
validation remain pending; no next subphase was implemented.
