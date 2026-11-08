# Phase 5.2 — authorized missing-only NFS cache fetch

8 November 2026, Asia/Dhaka. **5.2 local/fake acceptance PASS; STOP before 5.3.**
The accepted boundary was [5.1](19-worker-cache.md), present as uncommitted
changes above `da85a0c`. Those changes were saved byte-for-byte under
`/tmp/hybrid-storage-5-2-before` before edits; none were stashed, reset or committed.
No production cache, service, config, database, mount or Pi was changed.

## 1. Behavior and exact files

| File | Change in this subphase |
|---|---|
| `hybrid_storage/cache_fetch.py` | Explicit manifest-input validation/authorization seam, missing-only verification, disk admission, cross-process transfer slot, retries, publication and retained helper ownership. |
| `hybrid_storage/cache_copy_worker.py` | Fixed trusted helper: guarded NFS reads into an inherited private local FD, size/SHA verification, readback and fsync. No publication/SQLite code path. |
| `agent/cache_storage.py` | Append inactive `nfs_cache_fetcher` factory; retain 5.1 factory unchanged. |
| `tests/test_cache_fetch.py` | 33 new disposable unit/local integration checks; real helper processes with fake NFS mount metadata. |
| `docs/storage/20-cache-fetch.md`, `20-cache-fetch-evidence.json` | Current contracts, raw evidence, commands, preservation and rollback. |
| `hybrid_storage/README.md`, `tests/README.md`, `docs/README.md`, `docs/storage/implementation-progress.md` | Navigation and explicit completion boundary; prior progress retained. |

The trusted caller supplies `InputFile` entries derived from its exact authorized
job manifest. Logical paths are not used as filesystem destinations. Duplicate
digests fetch once, conflicting sizes/duplicate logical paths fail, and input
lists are bounded to 10000 entries. A current local `authorize(inputs)` callback
must explicitly return `True`; it runs before cache lookup, each transfer,
publication and success return. Raw digests, filenames and parsed manifests do
not grant access. There is no student endpoint or new network transfer protocol.

For each unique digest:

1. Acquire the local cache's cross-process fetch slot and fully verify its local
   replica using 5.1 `open_verified`; a hit performs **no NFS open/read/probe**.
2. For a miss, reject unresolved pins and check quota/free space before transfer.
3. Create a unique service-owned `parts/<sha>.<random>.part` with O_EXCL/no-follow.
   A fixed helper opens only the existing guarded read-only NFS adapter, streams
   at most 1 MiB per read into the inherited part FD, validates exact size/SHA and
   stable source identity, rechecks the mount on exit, readback-verifies, sets
   mode 0400 and fsyncs. It does not accept a destination pathname.
4. The parent waits within the configured NFS-helper deadline, reaps it, validates
   its result, rehashes/fsyncs the same local part FD and rechecks authorization.
5. Under a short local cache transaction, recheck pins and publish using Linux
   `renameat2(RENAME_NOREPLACE)`. Fsync directories, then record the full verified
   replica through 5.1. No partial file is advertised as complete.

If an unpinned regular read-only cache replica has bad size/hash, preserve it
under `quarantine/<sha>.<random>` before installing verified replacement bytes.
No corrupt replica is overwritten/deleted, and no student job, source, checkpoint
or authoritative NFS input is moved. Writable, linked, special-file or permission
failures are errors, not repair permissions. Pinned missing/corrupt replicas
require explicit future attempt reconciliation; repair never clears their pins.

## 2. API, metadata, config and security

Explicit trusted-service example (not run on any live node):

```python
from agent.cache_storage import nfs_cache_fetcher

fetch = nfs_cache_fetcher(
    cache,                              # operator-provisioned 5.1 handle
    source=operator_expected_nfs_source, # verified host:/restricted/export
    quota_bytes=operator_cache_quota,
    reserve_free_bytes=64 * 1024 * 1024,
    attempts=3,
    io_timeout=10,
    lock_timeout=2,
    retry_delay=0.1,
)
# Future trusted isolated service, not the legacy HTTP/training process:
# fetch.initialize()
# outcomes = fetch.ensure(exact_authorized_inputs, authorize=recheck_current_authority)
# Keep fetch alive and reconcile pending() until empty, including after close().
```

Construction is side-effect free. Explicit initialization adds private `parts/`,
`quarantine/` and `state/fetch.lock`; cache objects and SQLite schema remain 5.1
v1. No migration, new table, global flag, environment override, HTTP route,
controller grant protocol, startup hook or deployment bundle change. Existing
controller-local registry/job DB, job states, queue/fairness/reservations,
leases, checkpoints, upload and recovery APIs remain unchanged. Actual defaults
remain **legacy / false / false / resource**, and cache-enabled runtime remains
rejected. Existing same-UID training is not a safe cache/NFS isolation boundary.

`FetchResult(sha256, size_bytes, cache_hit, attempts)` describes each unique
digest. It is a component outcome, not a launch receipt or network metric.
Attempt count is zero on a fully verified initial hit. Detailed byte/timing
telemetry and workload-level cold/warm performance acceptance remain **5.4**.

The callback must derive authority from authenticated current job/owner/attempt
state locally, including cancellation/expiry. Example tests use a True callback
for synthetic public inputs; production must not do that. This seam does not
implement a signed grant format, identity provisioning, stage-revision fencing,
trainer sandbox or execution permission. Those runtime integrations remain gated.

### Quota, concurrency and retry policy

One active NFS producer per cache root, across **all threads/processes**, is the
initial conservative I/O bound. There is no pool or queued backlog. Same-digest
waiters reverify after obtaining the slot and reuse the first completed object.
Lock waits are bounded; contention returns `CacheFetchBusy` without starting I/O.
Each Pi has its own cache/slot; this does not serialize the entire cluster.

Quota conservatively sums logical bytes in `objects/`, `parts/` and `quarantine/`,
including retained crash files. Scanning is streaming, no-follow, depth-bounded
and limited to 100000 entries. The slot serializes admission with publication,
preventing competing fetch processes from overcommitting this quota. Admission
requires `used + expected_size <= quota_bytes` and `statvfs` service-available
bytes covering expected size plus the configured reserve. It is not an OS quota
or a reservation against unrelated processes. Metadata/workspace/output space
is outside the object quota; filesystem free space still accounts for it.

Retry only returned transient I/O errnos (EIO, ESTALE, ETIMEDOUT, EAGAIN,
ECONNRESET, ENETUNREACH, EHOSTUNREACH), at most 1–5 attempts with a finite delay
and fresh part each time. Recheck authority and disk admission before retrying.
Missing source, permission, corruption, mount-admission failure, ENOSPC/EDQUOT,
unsafe paths and cancellation fail closed. A helper wait timeout does **not**
automatically launch another producer. Local publication failures are returned
without replaying remote transfer; if a complete final object became visible,
the next invocation re-verifies/fsyncs it and uses a hit.

### Timeouts, crash ownership and limits

The helper deadline bounds NFS-child waiting, not all local work. Hashing, SQLite,
fsync and accounting remain synchronous local I/O; do not run this on Flask,
scheduler or lease-critical threads. On timeout/interruption request SIGKILL,
retain the owned process, all relevant FDs, part and fetch slot, and fail the
caller. `pending/reconcile/close` never accept late readiness. Cleanup removes
only this call's O_EXCL-created part after reaping, with inode identity checking.

The helper inherits the fetch-lock FD as well as its part FD. If the parent
crashes, a surviving helper still holds the global slot until it exits. Its
fixed code has no final-name publication/metadata operation, so it cannot later
commit an object on behalf of the dead parent. This is a trusted-code control,
not an OS privilege sandbox against compromise of the service UID. Restart can
fetch only once that slot is released. Old parts/quarantine remain accounted,
never swept automatically. There is no durable PID/session adoption service;
later live integration still requires durable service/attempt reconciliation.

## 3. Actual tests and results

Pre-change selected baseline: **371 passed, zero failures/skips, 19/19 files**.
Final combined selection: **404 passed, zero failures/skips, 20/20 files**,
including **33 new fetch tests**. Exact command from the application root:

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_cache_fetch.py test_worker_cache.py test_legacy_registration.py test_storage_registry.py test_dataset_manifests.py test_dataset_validation.py test_artifact_registration.py test_storage_readers.py test_storage_selection.py test_storage_config.py test_job_submission.py test_job_storage.py test_student_portal.py test_queue_controls.py test_resource_allocation.py test_storage_inputs.py test_storage_health.py test_storage_backends.py test_nfs_client_setup.py test_nfs_server_setup.py
```

Real disposable files/processes demonstrate cold publication, unchanged warm
repeat with zero NFS helper calls, missing-only/duplicate inputs, local
corrupt-refetch/quarantine, pinned refusal, empty/bounded-large inputs,
interrupted read/retry, actual copy-write ENOSPC injection, quota/free reserve,
source corruption, mount replacement, authorization revocation, helper/parent
crashes, concurrent-process dedup/quota, timeout/close fencing and post-rename
reconciliation. Fake uncancellable I/O retains ownership and discards even a
complete/correct late part. Actual sleep helpers are killed/reaped. A fresh real
helper rejects an ordinary local directory as NFS.

Initial 24 tests passed with fixture-only unclosed stderr-pipe warnings; stderr
handling was aligned with production DEVNULL. An expanded helper-fixture quoting
error caused a SyntaxError before tests ran, was corrected, then 31 tests passed.
Two additional coverage cases bring the final suite to 33. No selected baseline
failure occurred; the development error was not attributed to old code.
[Evidence](20-cache-fetch-evidence.json) retains all output and commands.

No real NFS/Pi/SSH, network interruption, all-three-Pi validation, machine reboot,
hardware power loss, actual training/DDP, full repository suite or throughput
benchmark was run. Mounted-NFS facts are mocked for positive copy tests; actual
local I/O/rename/process behavior is exercised. No production registration or
schema initialization occurred. Central-controller SPOF remains unchanged.

## 4. Safe manual verification

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_cache_fetch.py test_worker_cache.py
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python -m hybrid_storage.config
git diff --check
```

Expected: **66 tests pass** in two files; config reports legacy execution with
false cache/staging flags and resource policy; no whitespace errors. The same
two files are included in the recorded combined run. No sudo, Internet, live
agent import or production cache-root provisioning is needed.

## 5. Rollback and residual risks

The **5.2-only** patch is `/tmp/hybrid-storage-5-2.patch`, generated against the
saved uncommitted 5.1 contents. Review it from the parent Git root:

```bash
git apply --reverse --check /tmp/hybrid-storage-5-2.patch
# Only after review and checking that later work does not overlap:
# git apply --reverse /tmp/hybrid-storage-5-2.patch
```

The reverse-check passed. It restores the prior 5.1 contents and removes only
new 5.2 source/docs/tests; it does not reverse the entire dirty checkout. No
host rollback is needed because nothing was enabled/deployed. Before any future
live rollback, stop admission, close/reconcile owned helpers until none remain,
and preserve the cache DB, all pins, parts, quarantine, jobs and checkpoints.
Never delete/reset those roots or unmount a pending producer to roll back code.

Retained crash/quarantine files consume quota and require later reviewed
reconciliation/GC. A pinned corrupt replica cannot be silently repaired. The
cache slot intentionally limits throughput to one fetch per worker root; no
benchmark is fabricated. Local storage can still block, and a hard NFS task may
retain capacity after SIGKILL. Exact authenticated grants, trainer isolation,
durable service ownership and private attempt workspaces remain runtime gates.
Future live cluster validation requires pi01/pi02/pi03 unless explicitly excepted;
the earlier pi01 deferral remains explicit and is not a new acceptance waiver.

## 6. Acceptance boundary

**5.2 local/fake acceptance: PASS. STOP before 5.3.** Missing-only guarded NFS
copy, quota/free admission, unique private parts, bounded I/O/producer count,
size/SHA/fsync/atomic publication, transient retry and duplicate prevention are
implemented and tested locally. No incomplete/late part is accepted. The whole
Phase 5 PASS remains **pending**: private usable workspaces, terminal-path pin
lifecycle, full byte/timing instrumentation and live validation are not claimed.
