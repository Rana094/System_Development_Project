# Phase 11.3 — dry-run-first local cache maintenance

9 November 2026. **11.3 only.** Last evidenced implementation boundary: 11.2
local PASS, with hardware/production acceptance still pending. `main` HEAD
`7edef1ccd8e3264d9716a3482cbd214188d009b6` is unchanged. Prior uncommitted changes
were preserved; 283 source/config/document files were snapshotted before edits.

## Behavior, files and configuration

Explicit idle maintenance can now plan and remove old unpinned local replicas.
**Nothing automatically evicts on import, startup, dispatch, admission failure or
a timer.** A locally issued immutable plan is required, and even `apply(plan)`
remains a dry run unless the trusted caller explicitly supplies `dry_run=False`.
There is no student/HTTP cleanup endpoint or remote filesystem command.

| File | Change |
|---|---|
| `hybrid_storage/cache_eviction.py` | Typed bounded LRU plan, explicit checked apply and nondestructive crash reconciliation |
| `hybrid_storage/worker_cache.py` | Cross-process shared/exclusive activity lock, protected verified-reader lifetime, genuine read-only SQLite sessions for preview |
| `hybrid_storage/cache_fetch.py` | Guard complete authorized fetch calls and retain existing quota/accounting; maintenance refuses nested mounts |
| `hybrid_storage/workspaces.py` | Guard materialization while creating durable pins |
| `agent/staging.py` | Guard the entire fetch-to-workspace-pin interval on the actual signed staging path |
| `agent/cache_storage.py` | Inactive `cache_maintenance(fetcher, ...)` trusted factory |
| `hybrid_storage/inventory_reporter.py` | Local `request_full()` requests a fresh signed full inventory after changes |
| `tests/test_cache_eviction.py` | Local ownership, preview, LRU, pins, race, crash, error, restaging and signed inventory checks |
| `hybrid_storage/README.md`, `tests/README.md`, `docs/storage/implementation-progress.md` | Usage and appended progress |
| This report and `44-cache-eviction-evidence.json` | Exact results, preservation, limitations and rollback |

Cache limits reuse existing validated trusted-worker settings, avoiding a second
inconsistent quota or new global flags:

| Maintenance term | Existing setting / semantics |
|---|---|
| `max_cache_bytes` | `CacheFetcher.quota_bytes`: logical bytes in objects, parts and quarantine; no duplicate alias objects |
| `reserve_free_bytes` | Same fetcher's existing service-available free-disk reserve |
| `incoming_bytes` | Explicit bounded prospective missing cache bytes, supplied by the trusted caller; not an untrusted path/manifest request |
| `max_candidates` | Default 1024, allowed 1–4096; insufficient bounded plans cannot be applied |
| Reviewed plans | At most eight in-process immutable plans; never persisted or replayed after restart |

Active `storage.json`, worker/NFS/lease/queue settings and resource/locality/
bandwidth policy code remain unchanged. Ordinary startup and production isolation
requirements remain gated. Maintenance is optional and is not required for job
execution or production stability. Admission still fails safely when space is
insufficient; it never silently evicts or changes transport.

## Dry run, LRU and ownership

`plan(incoming_bytes=0)` opens controller-independent worker cache metadata in
SQLite `mode=ro`, with `query_only=ON`. It does not touch use/verification times,
create files, hash/fetch content, update inventory or unlink anything. Existing
locks must already have been provisioned by explicit normal cache/fetcher
initialization. Tests compare all local cache file contents before/after preview.
OS access times may change as with ordinary read-only inspection.

The plan reports total logical usage, actual service-available disk space, limits,
pin reference/object counts, skipped changed/missing candidates, selected digest
identities and estimated allocated bytes. LRU order is `(last_used_ns or 0,
sha256)`, so never-used entries are oldest and exact ties are deterministic.
Existing verified reads/pins persist last use. Inventory scans/reconciliation do
not mark cold objects recently used.

Only registered `verified` objects whose current size/inode fingerprint matches
the verified record qualify. Missing/changed replicas are skipped. Read-only
regular files, service ownership, no symlinks/hardlinks, descriptor-relative SHA
paths, local supported filesystem and exact mount identity are mandatory.
Candidates must remain on the root's mount, including bind-mount identity checks.
No arbitrary path from a plan is used for unlink: the component uses only its
trusted cache root and validated `objects/sha256/<prefix>/<digest>` basename.

The cache must be a dedicated service-protected local tree, separate from NFS,
workspaces and checkpoints. Supply `protected_roots` for existing workspace/
checkpoint trees; overlapping trees are rejected. Unknown objects/files are never
adopted for deletion. Parts/quarantine count toward the quota but remain untouched,
as do SQLite, job data, workspaces, outputs, checkpoints and authoritative inputs.
Directory pruning and recursive deletion are not implemented. Hardware/kernel or
malicious same-UID/admin writers are outside the existing trust boundary.
On a co-located controller, include its authoritative roots in `protected_roots`
as well; their paths cannot be inferred from a worker NFS mount. A dedicated cache
root and correctly declared protected roots are trusted configuration requirements,
not a promise to recognize arbitrary misconfigured data trees automatically.

Planning estimates both logical quota and allocated-block relief. Sparse/reflink
storage and other consumers can make physical relief differ. Apply remeasures
actual usage/free space and reports `insufficient_actual_space` rather than
claiming success or expanding the reviewed victim set. No-pressure plans select
nothing; insufficient plans cannot cause a partial clearance.

## Protect staging, running jobs and readers

The new private `state/activity.lock` is an additive lock file, created by normal
explicit initialization. Worker SQLite remains schema v1; no DB migration or pin
schema change. Cache readers/fetchers/materializers take shared OS locks;
maintenance takes a nonblocking exclusive lock, then the existing fetch-slot
lock, then metadata locks. Shared admission waits at most two seconds if explicit
maintenance is active; lease/authorization checks still run before publication.

The actual agent holds a shared guard across `ensure()` **and** workspace
preparation, closing the previous fetched-but-not-yet-pinned interval. Fetch and
workspace APIs also guard their own operations; other trusted callers composing
them must similarly hold `cache.activity()` across their gap. A bare successful
fetch is not a durable workspace pin or launch grant.

Any persistent cache pin blocks **all** eviction, including unrelated unpinned
objects. This intentionally conservative rule provides an idle-only policy.
READY, running and unproven-terminal attempts retain their existing exact-attempt
pins. Only the existing terminal/quiescence lifecycle may release them; neither
maintenance nor restart reconciliation clears pins or guesses from lease age.
An opened verified reader holds its shared guard until its FD is closed.

The existing inherited fetch-slot flock also excludes eviction while an orphaned
or timed-out NFS helper retains ownership, even after its parent/shared lock has
died. An expired helper cannot publish late or have its part cleaned by eviction.
All trusted users of a shared cache must run the guarded implementation before
any eviction is enabled; do not mix old unguarded cache readers with maintenance.
No worker upgrade/restart was performed here.

## Checked deletion and crash reconciliation

A real apply consumes only that instance's issued plan. It reacquires locks,
rechecks root identity, quota/reserve, total usage, exact LRU candidates and pins,
and refuses stale, altered, foreign or pre-restart plans. Pressure changes that
change the required victims require a new review. It never adds an unreviewed
victim. Each candidate is checked again under metadata serialization.

For each removed object, the worker commits `state=missing`, clears verification/
fingerprint, then unlinks only the same checked descriptor-relative directory
entry and fsyncs its parent. The opened inode is kept through that check and
closed before measuring relief. A DB commit/full-disk failure prevents unlink.
Permission/unlink/fsync failures propagate; already completed work is not falsely
reported as rolled back. Optional inventory notification happens after all locks
are released, even on partial failure, and its failure cannot undo a deletion.

Explicit `reconcile()` does **no deletion or unpinning**. After normal cache
initialization/recovery, it checks only registered rows, fully hashes retained
objects, marks missing/corrupt observations, and retains all pin references:

* Crash after committed missing intent but before unlink: the retained file is
  reverified and can be used again; deletion is not replayed.
* Crash after unlink: the absent replica remains missing and needs an authorized
  refetch; no stale verified hit survives the next scan.
* A corrupt/unsafe retained replica is marked corrupt, never deleted by reconcile.
  Unknown files are untouched, and exact-attempt pins remain conservative.

No extra deletion journal/schema is needed because existing persisted missing
observations fail closed, and restart never resumes destructive intent. Tests kill
real disposable child processes at both boundaries. They do not simulate kernel
power loss; fsync/durability still depends on the local filesystem and hardware.
Reconciliation may hash many bytes and requires an operator-arranged idle interval.
It is not installed on a latency-sensitive heartbeat or startup automatically.

## Registry, API and security

Pass an initialized reporter's `request_full` as `notify`. After apply/reconcile,
that local callback requests FULL on the next fresh authenticated challenge.
Cached/lost-response replies retain their original body/sequence. Without the
callback, the existing periodic scan emits `CACHE_REMOVED` or reconciles FULL.
Tests exercise both actual signed paths against disposable controller SQLite.

There is an advisory delay until the next successful heartbeat; outage/replay
rules still apply. This is not an immediate controller acknowledgment. Registry
coverage never grants staging authority, and stage-time verification still handles
missing bytes even before the registry learns about eviction. Callback failure
returns an explicit reconciliation-needed status; periodic FULL remains available.
No new HTTP route, credential, inventory wire version or transport is introduced.

Controller SQLite stays local, and worker metadata stays on its protected local
cache disk. NFS remains a narrowly scoped read-only input view with root_squash;
AUTH_SYS is not per-student authorization. Student code receives private local
inputs, with no arbitrary NFS/cache access, and checkpoints retain their trusted
publisher. The central controller/LOCAL SQLite/authoritative storage is still a
**single point of failure**.

## Trusted usage and safe manual verification

Only within a separately reviewed trusted idle-maintenance bootstrap:

```python
from agent.cache_storage import cache_maintenance

# fetcher is already explicitly initialized and uses reviewed quota/reserve.
maintenance = cache_maintenance(
    fetcher, protected_roots=(workspaces.root, configured_checkpoint_root,
                              *configured_authoritative_roots),
    notify=reporter.request_full,
)
preview = maintenance.plan(incoming_bytes=authorized_missing_bytes)
print(preview.document())
# maintenance.apply(preview) remains dry-run-only.
# Only after concrete review in that same trusted component:
# result = maintenance.apply(preview, dry_run=False)
```

Do not copy the explicit apply call into job dispatch or student request handlers.
`reconcile()` is a separate trusted, nondeleting idle operation after a crash.
This subphase does not configure a worker daemon or run the example on live data.

From the application root, safe local checks:

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_cache_eviction.py
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python -m hybrid_storage.config
git diff --check
git status --short
```

Expect the new tests to pass, active flags `legacy`/OFF/`resource`, no service
start, and only intended working-tree changes. No mount, SSH, `/etc`, firewall,
systemd, live DB or worker cache change is necessary for these checks.

## Actual tests and acceptance

Pre-edit baseline: 143 PASS across worker-cache, cache-fetch, workspace and signed
inventory suites. Initial activity-guard regression: 105 PASS across three files.
Initial new suite: 19 PASS; expanded broad run passed 22 new checks and all 17
selected files. Broad command:

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_cache_eviction.py test_worker_cache.py test_cache_fetch.py test_private_workspaces.py test_cache_inventory.py test_bandwidth_policy.py test_staging_dispatch.py test_staging_recovery.py test_staging_portal.py test_ddp_staging.py test_scheduler_policy.py test_scheduler_metrics.py test_scheduler_locality.py test_queue_controls.py test_storage_config.py test_hybrid_checkpoint_recovery.py test_cache_coverage.py
```

The final new suite additionally checks authorized refetch after eviction. Final
counts/log hashes/latest-per-file results are recorded in the evidence companion.
Latest aggregate: **396 distinct tests PASS across 17 files, zero failures/errors/
skips**. The final 23-test new suite passed; after a final cleanup-guard refinement,
the new suite and cache-fetch suite passed together (56 tests). Reruns are not
counted twice.

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_cache_eviction.py test_cache_fetch.py
```

Tests cover unchanged file hashes on preview, deterministic LRU/limits/reserve,
pinned READY/running/unproven terminal states, fetch-to-pin gap, retained readers,
unreaped producers, unsafe/linked entries, root replacement, stale/foreign plans,
full-disk metadata failure, unlink/notification failure, real process crashes,
nondestructive reconciliation, untouched authoritative/checkpoint/unknown files,
actual signed FULL and CACHE_REMOVED convergence, and standard authorized restage.

Selected regressions include legacy/OFF/resource exact behavior, locality/bandwidth,
queue/quotas, signed single and 2/3-rank staging/cancellation/recovery, owner-safe
portal views, coverage and genuine CPU saved-state checkpoint resume. NFS facts,
trainer/containment and resource telemetry in cluster fixtures are mocked. No live
NFS outage, hardware throughput, physical all-three-Pi run or deployment was done.
Prior Phase 9 physical acceptance and the historical loaded-run HTTP409 limitation
remain pending; neither is claimed repaired by this subphase.

Local acceptance requires safe dry-run-first LRU, stable quotas/reserve, no pinned
or authoritative deletions, guarded staging/readers, no crash deletion replay,
registry convergence and unchanged selected workflows. See evidence for PASS/FAIL.
Phase 11 is not complete: 11.4 combined efficiency/fault acceptance remains.
**11.3 local PASS.** Physical acceptance is not claimed.
**STOP before 11.4.**

## Rollback and residual risks

Stop invoking explicit maintenance; there is no background task to stop. Existing
resource policy rollback remains `runtime.set_scheduler_policy('resource')`.
GC does not change that policy or running jobs. Deliberately evicted replicas are
not undeleted by code rollback: ordinary authorized staging refetches immutable
inputs from the unchanged authoritative source when available. Do not reset local
metadata, pins, jobs or checkpoints. Do not remove/replace lock files while any
process may hold them; the additive lock is harmless to older cache schema v1.
Disable maintenance before deploying older/unguarded readers in a reviewed rollout.

Retain `/tmp/hybrid-11-3/11.3.patch` and the pre-edit snapshot before temporary
evidence expires. At the Git root, check before intentionally reverting only this
subphase:

```bash
git apply --reverse --check /tmp/hybrid-11-3/11.3.patch
# Apply only if source rollback is intended and the check succeeds:
git apply --reverse /tmp/hybrid-11-3/11.3.patch
```

No rollback, reset, stash, commit or deployment was performed. Source fingerprints
preserve prior work. Residual limits: stale pins block cleanup until real lifecycle
quiescence proof; dense/continuous jobs may never offer an idle window; large scans
can delay new stagers; SQLite unable to commit on a totally full disk fails closed;
parts/quarantine/unknown files may prevent meeting quota; allocated-byte relief and
concurrent external disk consumption are uncertain. Eviction may reduce offline
warm-cache reuse; no NFS health/speed probe is added. Dedicated roots, protected
service identity, upgraded cooperating clients and reviewed isolation remain
prerequisites. All-three-Pi physical validation is still required.
