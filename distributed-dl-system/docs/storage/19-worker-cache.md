# Phase 5.1 — persistent local worker cache inventory

8 November 2026, Asia/Dhaka. Only **5.1** is implemented. Last accepted code
boundary: [4.4 / Phase 4 local acceptance](18-legacy-registration-and-phase4.md),
committed in `da85a0c`. The checkout was clean at the start. No live cache, service,
configuration, infrastructure or production DB was initialized or changed.

## 1. Behavior and files

| File | Change |
|---|---|
| `hybrid_storage/worker_cache.py` | Explicit worker-local inventory, verification, persistent exact-attempt pins and disk accounting. |
| `agent/cache_storage.py` | Inactive trusted-worker factory; no legacy-launcher call site or import side effects. |
| `tests/test_worker_cache.py` | 33 disposable local tests, including actual independent processes and interrupted SQLite transaction recovery. |
| `docs/storage/19-worker-cache.md`, `19-worker-cache-evidence.json` | Contracts, evidence, commands, limitations and rollback. |
| `hybrid_storage/README.md`, `tests/README.md`, `docs/README.md`, `docs/storage/implementation-progress.md` | Navigation and current completion boundary. |

The operator selects an explicit **local disk** cache root, outside the NFS
input path. The root must already exist, service-owned and mode 0700. Construction
performs no I/O. Explicit `initialize()` creates only protected cache children:

```text
<configured-local-cache>/
  objects/sha256/<first-two>/<64-lowercase-hex-digest>
  state/cache.sqlite3
  state/cache.lock
  state/cache.sqlite3-journal   # only while SQLite needs its local journal
```

Opened directory/file descriptors must attest ext4, xfs or btrfs. Remote, FUSE,
overlay, tmpfs and unknown filesystems fail closed; there is no bypass switch.
Cache, state and object directories are private and same-filesystem. Root/NFS
path overlap, symlinks, writable objects, special files, hardlinked objects,
unprotected metadata and unexpected/future DB schemas are rejected. Source
ancestry must be service-protected; administrators and same-UID malicious
processes are outside this component's protection.

Existing complete replicas are enrolled **only after** full bounded SHA-256,
exact-size and stable-FD-identity verification. Filename existence, arbitrary
untracked files and `.part` files do not establish a hit. No directory-wide scan
or adoption, copying, publication, rename, deletion or repair is implemented.
Tests explicitly manufacture complete replicas; production fetching is **5.2**.

## 2. API, metadata, configuration and security

The trusted-worker seam accepts operator configuration, for example:

```python
from pathlib import Path
from agent.cache_storage import local_cache

cache = local_cache(
    Path('/srv/ml-worker-cache'),       # proposed local disk, not provisioned here
    input_mount=Path('/srv/mlnfs/inputs'),
    verify_after_seconds=300,
)
# Future trusted service, after provisioning/isolation/authorization:
# cache.initialize()
```

This is an API example, not a live deployment instruction. No new environment
variables, transport protocol, HTTP route or global flag was introduced.
`config/storage.json` stays **legacy / false / false / resource**. The existing
controller gate still rejects cache-enabled runtime. The legacy agent launcher,
existing environment cache, upload, jobs, scheduling, leases, recovery and
checkpoint interfaces are unchanged. No agent module with runtime mkdir or DB
startup side effects is imported by the new seam.

| Operation | Contract |
|---|---|
| `initialize()` | Explicit idempotent worker-local v1 metadata setup and SQLite integrity check; refuse incompatible metadata. |
| `verify(sha, expected_size, force=True)` | Verify/enroll a complete local replica; return frozen `CacheRecord`, without marking it used. Missing raises `CacheMiss`; bad size/hash/identity raises `CacheCorrupt` (a `CacheMiss`). |
| `verify(..., force=False)` | Advisory optimization only: reuse a recent verified observation if the FD fingerprint still matches. Expiry, changed inode/size/mtime/ctime or a future verification timestamp forces hashing. |
| `open_verified(sha, expected_size)` | **Always** fully hash the opened read-only FD and rewind that same FD before yielding; update last use. No stale-age opt-out. |
| `pin(..., job_id, attempt_id)` | Fully verify before idempotently persisting an exact job/attempt/digest reference; update last use. |
| `unpin(job_id=..., attempt_id=...)` | Remove only matching attempt references; no file deletion or automatic eviction. |
| `inventory()` | Frozen persisted observations/pins; advisory, not fresh byte integrity or authorization. |
| `space()` | Current `statvfs` total/free/service-available bytes plus an advisory verified-size sum; no quota, admission reservation or physical usage estimate. |

The dedicated local SQLite v1 schema consists of `cache_meta`, `cache_objects`,
`cache_pins` and `cache_pins_digest`. It is unrelated to controller `jobs.db` and
its registry schema. Each object records size, `verified|missing|corrupt`, last
successful verification/use timestamps and an FD fingerprint. These are **cache
states**, not new scheduler/job states. Failed validation invalidates the current
observation but retains the historical last-success timestamp and all old pins.
Permission and I/O errors propagate rather than pretending to be successful hits.
Conflicting expected sizes for an already recorded digest are rejected, never
silently rebound; expected identities must come from trusted authorized manifests.

Each operation uses a dedicated connection, FULL synchronization, foreign keys,
transaction and an OS advisory lock shared across processes. Lock acquisition
has a ten-second retry bound; slow local filesystem I/O itself is synchronous
and not cancellable by that timeout. Keep this work off Flask/lease-critical
threads. Hash buffers are at most 1 MiB. No long-lived DB handle is needed.

Bytes, observations and exact pins survive process restart; SQLite rolls back
uncommitted changes. No automatic pin clearing on reboot: later staging/recovery
must reconcile exact attempts/leases conservatively. Full stage-time hashing
is mandatory even after restart. Tests use fresh Python processes and an abrupt
`os._exit` during a transaction, **not actual machine reboot or power loss**.

A digest/manifest constructor/pin ID is not a permission grant. Only a future
authenticated trusted stager may select these APIs after authorizing exact
inputs and attempt ownership. Existing same-UID training cannot safely access
the cache: privilege/filesystem/network isolation remains a runtime gate. The
component does not expose the NFS mount, create writable cache hardlinks, change
training identity or fetch from the Internet. Terminal-path pin lifecycle,
private workspaces and outputs/checkpoint separation are **5.3**.

## 3. Actual tests and evidence

Pre-change isolated baseline: **338 passed, zero failures/skips, 18/18 files**.
New cache tests: **33 passed, zero failures/skips**. Combined final selection:
**371 passed, zero failures/skips, 19/19 files**. No selected pre-existing or
new-code failure was observed. The initial 30-test run passed; three additional
process/transaction/timeout tests also passed with the final schema constraints.

Exact combined command, from the application root:

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_worker_cache.py test_legacy_registration.py test_storage_registry.py test_dataset_manifests.py test_dataset_validation.py test_artifact_registration.py test_storage_readers.py test_storage_selection.py test_storage_config.py test_job_submission.py test_job_storage.py test_student_portal.py test_queue_controls.py test_resource_allocation.py test_storage_inputs.py test_storage_health.py test_storage_backends.py test_nfs_client_setup.py test_nfs_server_setup.py
```

This is a selected fake/local suite, not the full repository suite. Tests cover
stage-time/fresh/stale/clock-rollback verification, wrong size/hash, partial and
missing files, object mutation during hashing, bounded large-file reads, zero
bytes, no-follow protections, remote-admission refusal, state/DB protection,
schema mismatch, I/O invalidation, separate-process persistence and concurrency,
exact pin isolation, conservative invalidation, abrupt transaction interruption,
metadata lock timeout, current disk accounting and unchanged runtime gates.

[Evidence](19-worker-cache-evidence.json) retains exact commands, raw output,
counts and before/after preservation hashes. No NFS/Pi test, live fault,
throughput benchmark, actual training/DDP recovery, machine reboot, or hardware
power loss was performed. NFS fetching, network interruption, ENOSPC publication,
warm zero-transfer acceptance and rollback during transfer belong to **5.2/5.4**.

## 4. Safe manual verification

From the application root, using only a temporary source/data copy:

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_worker_cache.py
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python -m hybrid_storage.config
git diff --check
```

Expected: **33 tests pass**, default report says `runtime=legacy` with both flags
false and resource policy, and no diff whitespace errors. No administrative
permissions, production root creation or live agent import are required.

## 5. Rollback and residual risks

Scoped source/docs diff: `/tmp/hybrid-storage-5-1.patch`. From the **parent Git
root**, review it and run `git apply --reverse --check` before choosing to reverse:

```bash
git apply --reverse --check /tmp/hybrid-storage-5-1.patch
# Only after review and checking that no later edits overlap:
# git apply --reverse /tmp/hybrid-storage-5-1.patch
```

No service/host rollback is needed: nothing was deployed or enabled. Never
delete/recreate an existing worker cache DB, erase pins, overwrite checkpoints,
or reset user changes to roll back code. If a later service uses the API, first
stop admission and reconcile attempt ownership before removing its integration;
retain all cache bytes and metadata. No live schema downgrade is provided.

Disk accounting is observational; quota/admission/duplicate-fetch controls are
still 5.2. No eviction, GC, repair, cache-generation controller reporting,
private workspace, automatic terminal cleanup, scheduling integration or trainer
isolation is enabled. Hashing costs local I/O; a recent inventory row alone must
never authorize staging. A compromised service UID/kernel can defeat these
protections. The controller remains a **single point of failure**. Future
cluster validation requires pi01/pi02/pi03 unless explicitly excepted; the prior
Phase 2 pi01 deferral is preserved and grants no new cluster acceptance here.

## 6. Acceptance boundary

**5.1 local code acceptance: PASS. STOP before 5.2.** Explicit configuration,
local-only protected SHA layout, verified-object metadata, last use/verification,
persistent exact pins, current free disk and mandatory stage-time/stale hashing
are implemented and tested. Restart persistence is demonstrated with local
processes; actual Pi reboot remains untested. The overall Phase 5 PASS criteria
(missing-only NFS transfer, usable private workspace, refetch and warm zero
transfer) remain **pending**. No next subphase was implemented.
