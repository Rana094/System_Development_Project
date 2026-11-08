# Phase 6.3 — authorized dataset/job/rank cache coverage

8 November 2026, Asia/Dhaka. This subphase adds a controller-side Python API;
no deployment or scheduler enablement. The controller remains a **single point
of failure**. Stop before the consolidated 6.4 acceptance suite.

## 1. Behavior and exact changed files

Starting checkout: uncommitted/untracked 6.1 and 6.2 implementation and reports.
Last recorded acceptance: 6.2 local integration. Preserve those changes, legacy
fallback and the unfinished dedicated 5.4 acceptance boundary.

| File | Change |
|---|---|
| `hybrid_storage/cache_coverage.py` | `CoverageAPI`, immutable `CacheCoverage` result, bounded `RankRequirements`, exact digest deduplication and authenticated snapshot lookup. |
| `hybrid_storage/registry.py` | Extract existing job-input validation into `_job_inputs`; public `get_job_inputs` keeps its behavior while coverage reuses the same checks in one read transaction. |
| `hybrid_storage/cache_inventory.py` | Remember the actual local DB path at explicit startup; optional exact session filter on the existing eligibility predicate. Existing callers retain their default behavior. |
| `tests/test_cache_coverage.py` | Disposable local manifest/SQLite/cache/authentication integration, arithmetic, ACL and per-rank tests. |
| `docs/storage/24-cache-coverage.md`, `24-cache-coverage-evidence.json` | API contract, exact commands/raw results, preservation and rollback. |
| `hybrid_storage/README.md`, `tests/README.md`, `docs/README.md`, `docs/storage/implementation-progress.md` | Navigation and subphase boundary. |

No live DB connection/import/migration, object scan/copy, NFS mount, Pi/SSH,
credential provisioning, service or `/etc` action occurred. Existing config,
schemas/checksums, worker transfers, job states/checkpoints, scheduler/resources,
fairness, reservations, concurrency, cancellation and execution leases remain
unchanged. This adds no inventory HTTP route or student-facing cache listing.

## 2. API, DB, config and security contract

Construct `CoverageAPI(registry, inventory)` using the existing `StorageRegistry`
and the current `CacheInventory` owner. Construction does no I/O. The caller
authenticates the actor, owns a dedicated controller-LOCAL FULL/FK connection
for the registry, and must not share that SQLite connection between request
threads. The inventory owner retains its singleton lock/lifecycle from 6.2.

```python
from hybrid_storage.cache_coverage import CoverageAPI, RankRequirements

api = CoverageAPI(registry, inventory)
dataset = api.for_dataset(actor, worker_id, dataset_id, version)
job = api.for_job(actor, worker_id, job_id)
rank = api.for_job(actor, worker_id, job_id, rank=0)

# Only a trusted whole-file planner may supply this explicit declaration.
# Use a current owner-authorized immutable binding, not student-supplied hashes.
requirements = RankRequirements(
    rank=0, world_size=3, job_binding_sha256=binding.sha256,
    dataset_hashes=(authorized_data_file.sha256,),
)
selected_rank = api.for_job(actor, worker_id, job_id, rank=0,
                           requirements=requirements)
document = selected_rank.document()
```

No sample above opens an authority store or enables runtime execution. Supply
already explicitly configured/migrated handles; do not use the real job DB as a
verification fixture. No schema or flag was added. Actual config remains
**legacy / cache false / staging false / resource**. Existing controller execution
gates remain in place.

### Authorized required objects

`for_dataset` requires an exact dataset ID and positive integer version. It
uses the current existing dataset ACL, immutable version/manifest binding,
canonical manifest/hash/normalized-entry checks and predecessor validation.
It never substitutes the mutable head or infers ownership from a digest.

`for_job` requires the existing job owner and an exact immutable job-input
binding. Reuse checks for owner, source grant, legacy snapshot pin, execution
specification, requirements identity and current dataset ACL/version/hash.
Requirements are already a source file, so they are counted once. All source
files and pinned dataset files are included; directories carry zero bytes.
Matching content across paths, source/data or requirements is counted once by
SHA-256, regardless of logical filename or executable mode.

Unregistered legacy jobs raise `CoverageUnavailable`; their existing
`get_job_inputs` still returns `None`, and existing legacy execution remains
available. Coverage never registers, migrates, moves or rewrites them. Missing or
corrupt required authority raises a validation/registry error rather than
inventing an empty target. Unknown/unauthorized jobs or datasets deny access
before looking up inventory. ACL revocation is checked on every query.

### Rank semantics

The pinned execution declaration supplies world size. Rank must be a real
integer in that declaration. Default coverage includes all source and all
dataset files on **every** rank, matching current trainers: `DistributedSampler`
selects samples but does not shard stored files or reduce input transfer bytes.

An optional immutable `RankRequirements` must match the exact job binding hash,
rank and world size. Its bounded tuple of dataset digests is deduplicated and
checked against the pinned authorized dataset. All common source/requirements
are always included; unknown or foreign hashes are rejected. Empty data selection
is explicit. Generated-data jobs have no dataset objects. Declarations do not
change execution, create sample-level cache entries, grant staging access or
claim that current trainers can run on arbitrary dataset subsets. A future
trusted planner must supply real pre-sharded whole-file requirements.

Results are per worker/per rank. Future group cost must count a copy separately
on each different worker; do not globally deduplicate across workers or add
locality ratios. No group reservation, staging grant or DDP launch change exists.

### Result fields and arithmetic

| Field | Meaning |
|---|---|
| `worker_id`, `target_kind/id/version` | Requested worker and exact dataset version or job. |
| `input_manifest_sha256` | Dataset manifest digest, or the immutable job-input binding digest for a job. |
| `rank`, `world_size` | Explicit rank context, otherwise `None`. |
| `required_objects`, `required_bytes` | Number and byte sum of unique required file digests. |
| `verified_cached_bytes` | Required bytes reported VERIFIED by the exact current eligible session. |
| `missing_bytes` | `required_bytes - verified_cached_bytes`. |
| `locality_ratio` | Cached/required bytes as a float; byte counts remain exact integers. |
| `missing_hashes` | Sorted unique required digests not currently counted as hits. |
| `inventory_eligible`, `inventory_status` | Advisory auth/session/freshness outcome, not resource/job-lease eligibility. |
| `inventory_age_ns` | Nonnegative controller receipt age, or `None` if absent/future/untrustworthy. |
| `inventory_session_id/sequence`, `computed_at_ns` | Diagnostic snapshot identity/cursor and controller computation time. |

For eligible zero-byte targets, ratio is 1.0 by byte convention. Missing empty
objects still appear in `missing_hashes`; zero missing bytes do not prove all
objects exist. Ineligible inventory always contributes zero hit bytes and ratio
0.0, including an empty target. Required totals exceeding signed-64-bit bounds
are rejected; Python integers are used for sums with no MB rounding.

### Snapshot and freshness boundary

Hold the inventory owner's lock and one read transaction on the same actual
controller-local DB for authorization, required identities, session and entries.
Cross-DB handles are rejected. Reuse 6.2's existing eligibility query with the
**exact live in-memory session ID**, cursor and controller epoch, not historical
VERIFIED rows or merely any ACTIVE row. Require current enrollment/principal,
unretired matching cache epoch, fresh full inventory and live signed heartbeat.
Each counted entry must have exact authorized size, VERIFIED/full verified bytes,
a verification timestamp and a sequence within the session cursor.

Absent inventory, unavailable/closed owner, unenrolled worker, expired heartbeat/
inventory, revoked principal, mismatched live context, invalid/future namespace
or failed offline persistence produces all misses, not optimistic reuse. Schema
errors confined to advisory inventory return `invalid_inventory`; they trigger
no repair. Failure to read the authority DB itself propagates: no result is
claimed when required inputs cannot be authorized/resolved. Age may describe
retained historical observations even when they are explicitly ineligible.

An authorized SQLite snapshot may finish while a later transaction revokes an
ACL; the next query sees the revocation. A result is not an authorization grant
for later staging. Reauthorize/recheck the exact attempt before use. Queries do
not contact workers, open object bytes, renew inventory or execution leases,
reconcile snapshots, or modify any rows. Cache reports are advisory: a worker
can lose a file immediately afterward; stage-time size/hash verification remains
mandatory. No current-file existence guarantee or scheduler admission is inferred.

## 3. Tests and actual evidence

[Evidence](24-cache-coverage-evidence.json) records exact commands, raw results,
per-file counts, preserved source/config/DB hashes and the scoped rollback diff.
Selected baseline: **123 tests passed, 4/4 files**. First coverage selection:
**25 passed**. Expanded focused selection: **154 passed, 5/5 files**, including
31 new coverage tests. No failure occurred in those selections.

Final broader command below ran **561 tests: 560 passed, one error, zero skips**;
**26/27 files passed** in 105.0 seconds. The original 26-file coverage/storage/
agent/queue/lease/deployment selection contributes **554 passing tests**, including
all 31 new cases. The additional `test_distributed_dispatch.py` has six passing
cases and one environment error: `test_real_two_process_ddp` cannot create its
localhost socket (`PermissionError: [Errno 1] Operation not permitted`).

Re-running that unchanged file in a disposable copy with both modified runtime
files restored to their saved **pre-6.3** contents gives the identical socket
error (six pass, one error). This reproduces an existing sandbox limitation,
not a new coverage failure. The real DDP workload never launched in that test;
no unrestricted/local-DDP or hardware pass is claimed. The test was not changed,
silenced or relabeled as a skip. No permission escalation was attempted.

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_cache_coverage.py test_cache_inventory.py test_cache_registry_schema.py test_private_workspaces.py test_cache_fetch.py test_worker_cache.py test_legacy_registration.py test_storage_registry.py test_dataset_manifests.py test_dataset_validation.py test_artifact_registration.py test_storage_readers.py test_storage_selection.py test_storage_config.py test_job_submission.py test_job_storage.py test_student_portal.py test_queue_controls.py test_resource_allocation.py test_storage_inputs.py test_storage_health.py test_storage_backends.py test_nfs_client_setup.py test_nfs_server_setup.py test_cluster_setup.py test_execution_ownership.py test_distributed_dispatch.py
```

For the entirely passing 26-file selection, omit only the final
`test_distributed_dispatch.py` argument; all its other file results are retained
individually in the broader-run evidence. Baseline-reproduction orchestration and
exact stdout/stderr are retained in the evidence too.

The three-worker integration fixture publishes and hashes **real 500-byte and
2-byte local objects**, authenticates reports for pi01/pi02/pi03 identities and
checks exactly 0, 2 and 502 **bytes** missing. This is deliberately small and
Pi-less. It is **not** the Phase 6.4 0/2/502 **MB** acceptance fixture, a live
three-Pi validation, a benchmark or fabricated large-file/cache-presence claim.
The near-64-bit test is pure arithmetic only, not large published files.

Other cases cover duplicated paths and source/data digests, warm/cold/unknown
workers, empty and zero-byte objects, immutable result serialization, authority
and ACL failures/revocation, exact versions after head update, legacy fallback,
source/requirements/job binding, generated data, default DDP versus explicit
whole-file rank requirements, stale/TTL/clock/offline state, corrupted/missing
observations, cross-DB/current-context guards, read-only behavior and a concurrent
ACL write during an authorized disposable WAL snapshot. No full repository suite,
live worker/PyTorch/NFS fault, power recovery or performance measurement is claimed.

## 4. Safe manual verification

From the application root:

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_cache_coverage.py
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python -m hybrid_storage.config
git diff --check
```

Expected: **31 coverage tests pass**; config reports legacy/cache false/staging
false/resource; no whitespace errors. All stores, keys and caches are disposable
fixtures. No real worker address is contacted, no migration touches the live DB,
and no service is started/stopped.

## 5. Rollback and residual risks

`/tmp/hybrid-storage-6-3.patch` is scoped against the saved starting contents,
including earlier uncommitted 6.1/6.2 work. From the parent Git root:

```bash
git apply --reverse --check /tmp/hybrid-storage-6-3.patch
# Only after review and confirming no later overlapping changes:
# git apply --reverse /tmp/hybrid-storage-6-3.patch
```

No host/DB rollback is required: no live migration or deployment occurred. Keep
all schema/history, cache epochs/pins, artifacts, jobs, checkpoints and newer DB
state. Do not reset epochs, delete data, drop tables or restore stale backups.
Future code rollback disables coverage consumers and leaves legacy scheduling
and storage flags unchanged.

Coverage is advisory and metadata-only. Current shared-UID training remains
unsafe for service keys/cache; OS isolation and legacy control/checkpoint endpoint
authentication remain production gates from 6.2. The API is for trusted controller
callers, not a student-supplied rank-plan or arbitrary inventory-listing endpoint.
No measured bandwidth policy or live deployment is enabled. Production validation
still requires all three Pis unless explicitly excepted; earlier pi01 deferral
is not a new waiver. Dedicated 5.4 acceptance remains unfinished.

## 6. Acceptance boundary

**6.3 local coverage API acceptance PASS.** Broader regression remains limited by
the reproduced pre-existing localhost-socket error; it is not an all-green suite.
Coverage API, exact unique-byte calculations, current ACLs/immutable bindings,
live-session freshness and honest per-rank whole-file requirements are implemented
and locally tested. **STOP before 6.4.** The consolidated Phase 6 fault/concurrency
matrix and exact MB fixture remain pending; no overall Phase 6 PASS is claimed.
