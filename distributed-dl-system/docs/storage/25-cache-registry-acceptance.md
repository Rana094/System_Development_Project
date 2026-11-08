# Phase 6.4 — local cache-registry fault/concurrency acceptance

8 November 2026, Asia/Dhaka. **6.4 local acceptance only; no deployment.**
This completes the Phase 6 local schema/authentication/coverage acceptance
boundary when the recorded checks pass. It does not enable cache execution or
locality scheduling. The controller remains a **single point of failure**.

## 1. Behavior and exact changed files

Last accepted subphase: 6.3 local coverage API. Preserve the existing uncommitted/
untracked 6.1–6.3 changes and their historical reports/evidence. Implement only
6.4, not scheduler integration, a new transport, OS isolation or deployment.

| File | Change |
|---|---|
| `tests/test_cache_registry_acceptance.py` | Sixteen consolidated fault/concurrency tests and the exact real-file 0/2/502 MB fixture. |
| `docs/storage/25-cache-registry-acceptance.md`, `25-cache-registry-acceptance-evidence.json` | Acceptance matrix, exact commands/results, byte/digest evidence, preservation and rollback. |
| `hybrid_storage/README.md`, `tests/README.md`, `docs/README.md`, `docs/storage/implementation-progress.md` | Navigation and completion boundary. |

**No runtime source changes were needed.** All earlier runtime sources and tests
are retained byte-for-byte. No live DB, job, checkpoint, artifact, credentials,
service, worker, network, export, mount or `/etc` configuration was modified.
Only generated disposable local fixture stores are created/removed by tests.
There is no `rm -rf` command or cleanup of user runtime data.

## 2. API, DB, configuration and security

No API, DDL, checksum, migration, auth protocol, feature flag/default, job/checkpoint
state, scheduler rule, resource eligibility, queue fairness, reservation,
concurrency limit, cancellation or execution lease implementation changed.
SQLite remains controller-local. Actual settings remain **legacy / cache false /
staging false / resource**. No live schema migration or key provisioning occurred.

Tests use the actual registrar, manifest/ACL registry, worker cache verifier,
authenticated reporter/ingestion and coverage API. Existing Flask-agent and
registration/heartbeat integration are also exercised by the retained 6.2 suite.
Worker identities pi01/pi02/pi03 in the exact fixture refer to three independent
protected **local test caches**, not connections to physical Pis. Credential
material is deterministic test-only data in disposable processes/stores.

The execution-lease test deliberately keeps verified input-cache presence
independent from execution authority: an expired lease fails the existing
`assert_owned` check even while cache inventory remains fresh. Coverage cannot
grant a reservation or override an execution lease. Reusable immutable inputs
need not be discarded when a job lease expires.

## 3. Test matrix and exact MB fixture

| Requirement | Actual local verification |
|---|---|
| Initial/full inventory | Empty authenticated FULL snapshot; missing objects contribute zero hits; later ADD/REMOVE/FULL reports reconcile. |
| Corrupt/missing/deleted file | Real disposable file mutation/deletion; verifier rejects it, report removes it, coverage counts a miss; controlled fixture repair re-adds it. |
| Restarted worker | A separate Python worker process initializes a newer durable cache epoch, signs a new FULL, then exits abruptly with `os._exit(17)`; cache bytes/identity persist, next reporter advances epoch and acquires released OS lock. |
| Expired lease | Existing worker execution-lease accept/assert path, then an expired wall-clock check; no workload launch or reservation created. |
| Stale heartbeat/inventory | TTL expiry and reconciliation reject retained VERIFIED history; signed unchanged reports do not extend inventory TTL. |
| Duplicate dataset entries | Two logical paths reference the 500 MB object; unique required total is still 502 MB. |
| Out-of-order/replayed inventory | Duplicate responses are no-ops; stale FULL and signed sequence jump cannot revive or advance a replaced session. |
| Offline while response is in flight | Pending response after offline notification is rejected; only a new full handshake restores eligibility. |
| Missing DB entry | Delete only a disposable cache observation; coverage becomes a miss; periodic full restores it without deleting event history. |
| SQLite concurrent writes | Independent child process holds a real SQLite `BEGIN IMMEDIATE` write lock; inventory waits, then both the unrelated write and one inventory event commit correctly. Concurrent worker reports/duplicates commit exactly one event per worker/sequence. |
| SQLite busy | Real competing write lock plus short test-only SQLite timeout; failure invalidates live admission, then reconnect/full recovers after rollback. |
| Crash during transaction | Child updates inventory/session inside an uncommitted transaction and exits with `os._exit(23)`; SQLite recovery retains the prior committed dump/coverage, integrity and FKs. |
| Disk-full publication failure | Inject SQLite error on the second entry of an existing-session FULL refresh; cursor, tombstones and events roll back together; exact retry succeeds. No real disk is filled. |

### Exact fixture: decimal MB, real verified local files

**MB = 1,000,000 bytes.** Stream-publish a real 500,000,000-byte object and a real
2,000,000-byte object through the existing SHA-256 registrar. Use bounded 1 MiB
source/copy buffers, fsync and separate private cache inodes. No sparse files,
truncation-only stand-ins, hardlinks, invented DB presence or scaled-down size.
Cache objects are fully hashed locally and again for the FULL report.

| Local fixture identity | Present verified bytes | Required unique bytes | Missing bytes | Missing MB |
|---|---:|---:|---:|---:|
| pi01 | 502,000,000 | 502,000,000 | 0 | 0 |
| pi02 | 500,000,000 | 502,000,000 | 2,000,000 | 2 |
| pi03 | 0 | 502,000,000 | 502,000,000 | 502 |

All three have fresh authenticated inventory before calculation; an empty cache
can be eligible with zero hit bytes. After known pi01 offline notification and
pi02/pi03 heartbeat expiry, all are ineligible and contribute zero trusted hits.
The emitted `EXACT_MB_FIXTURE` JSON retains actual artifact hashes/sizes and
coverage results in the raw evidence. This is correctness evidence, **not an NFS
throughput benchmark** or a live three-Pi validation.

The test needs about **1,504,000,000 bytes** of temporary object storage plus
metadata/reserve (controller 502 MB, pi01 502 MB, pi02 500 MB). It checks free
space and explicitly skips with a reason if insufficient; a skip cannot count
as this fixture's PASS. TemporaryDirectory cleanup touches only fixture-owned
data. No Internet downloads or network listeners are needed.

### Commands and recorded results

Baseline before edits:

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_cache_registry_schema.py test_cache_inventory.py test_cache_coverage.py
```

Initial new suite:

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_cache_registry_acceptance.py
```

Final selected regression:

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_cache_registry_acceptance.py test_cache_coverage.py test_cache_inventory.py test_cache_registry_schema.py test_private_workspaces.py test_cache_fetch.py test_worker_cache.py test_legacy_registration.py test_storage_registry.py test_dataset_manifests.py test_dataset_validation.py test_artifact_registration.py test_storage_readers.py test_storage_selection.py test_storage_config.py test_job_submission.py test_job_storage.py test_student_portal.py test_queue_controls.py test_resource_allocation.py test_storage_inputs.py test_storage_health.py test_storage_backends.py test_nfs_client_setup.py test_nfs_server_setup.py test_cluster_setup.py test_execution_ownership.py
```

[Evidence](25-cache-registry-acceptance-evidence.json) contains exact commands,
stdout/stderr/counts, fixture JSON, file hashes, current config and rollback scope.
Baseline: **91 passed, 3/3 files, zero failures/skips**, in 26.1 seconds. Initial
new acceptance: **16 passed, zero failures/skips**, in 9.7 seconds. Final selected
regression: **570 passed, 27/27 files, zero failures/skips**, in 115.7 seconds.
The exact MB case ran successfully both times, without scaling or skipping. No
new failure or runtime correction was needed in this subphase.

These are selected tests, not a whole-repository run. The unchanged real localhost
DDP test previously hit sandbox socket EPERM, reproduced on pre-6.3 code; see
[the 6.3 evidence](24-cache-coverage.md). It is not rerun or silently relabeled as
a skip here, because this phase does not change that test or DDP runtime code.
No new claim of unrestricted PyTorch/DDP training is made.

## 4. Safe manual verification

From the application root, with at least the documented free local temporary disk:

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_cache_registry_acceptance.py test_cache_registry_schema.py test_cache_inventory.py test_cache_coverage.py
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python -m hybrid_storage.config
git diff --check
```

Expected: **107 tests pass** across four files, including an `EXACT_MB_FIXTURE`
line with missing bytes `[0, 2000000, 502000000]`; no skip of the exact fixture;
config legacy/cache false/staging false/resource; no whitespace errors.
Tests never contact configured worker IPs or run deployment/system commands.

## 5. Rollback and residual risks

Scoped patch: `/tmp/hybrid-storage-6-4.patch`, relative to the saved starting
contents including all earlier uncommitted work. From the parent Git root:

```bash
git apply --reverse --check /tmp/hybrid-storage-6-4.patch
# Only after reviewing and checking for subsequent overlapping edits:
# git apply --reverse /tmp/hybrid-storage-6-4.patch
```

No live migration/deployment occurred, so there is no host/DB rollback. Reversing
only this patch removes this turn's new tests/report/navigation; retain all jobs,
checkpoints, artifacts, DB/WAL, inventory history and cache identities/pins. Do not
delete data, reset epochs, drop tables or restore stale databases.

Inventory remains advisory; stage-time path/size/hash verification is mandatory.
Expired execution ownership remains fenced independently. Synthetic disconnects,
SQLite error injection and process aborts do not establish hardware power-loss,
real network/NFS outage or full-disk behavior. The exact fixture is local I/O and
is not a benchmark. Socket per-read timeouts and OS filesystem stalls retain the
limits documented in 6.2; no new cancellation/OS isolation implementation exists.

Production activation remains blocked on reviewed trainer/service identity and
filesystem/network isolation, credential provisioning and broader legacy endpoint
authentication. Locality scheduler integration is not part of Phase 6. All-three-Pi
deployment validation is still required unless explicitly excepted; no new pi01
waiver or physical cluster pass is claimed. Dedicated 5.4 metrics acceptance is
still unfinished; this phase does not silently mark it complete.

## 6. Acceptance boundary

The exact fixture passed with zero skips, stale/offline exclusion, the full
local fault/concurrency matrix and unchanged legacy regressions. Therefore
**6.4 / Phase 6 local registry acceptance PASS**. No live/production or whole-suite
PASS is implied. **STOP.** No later phase is implemented automatically.
