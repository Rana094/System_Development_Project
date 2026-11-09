# Phase 11.4 — combined efficiency and fault acceptance

9 November 2026. **11.4 only; local PASS.** The last evidenced boundary was
[11.3 local PASS](44-cache-eviction.md). This turn adds acceptance tests and
records results; it changes no runtime implementation. Optional efficiency is
not a prerequisite for production stability. Physical deployment/acceptance is
still pending; this report does not approve enabling hybrid storage on workers.

## Files, compatibility and security

Branch `main`, HEAD `7edef1ccd8e3264d9716a3482cbd214188d009b6`. Existing
uncommitted Phase 10/11 work was preserved. Before editing, 254 selected
source/config/document files were snapshotted under `/tmp/hybrid-11-4/before`.
Only the two original documentation files below were appended; the other 252
snapshotted files remain byte-identical. No stash, commit or reset was performed.

| File changed in this turn | Behavior |
|---|---|
| `tests/test_storage_efficiency_acceptance.py` | Ten combined acceptance tests using existing fixtures and actual storage/control interfaces |
| `tests/README.md` | Append safe isolated test command and scope |
| `docs/storage/implementation-progress.md` | Append 11.4 result and remaining physical gates |
| `docs/storage/45-efficiency-acceptance.md` | This report |
| `docs/storage/45-efficiency-acceptance-evidence.json` | Per-file results, log/source hashes, preservation and rollback evidence |

**API, DB, config and security changes: none.** No route, migration, protocol,
credential, lease, checkpoint format, scheduler rule or deployment file changed.
Controller SQLite remains LOCAL. Active flags remain `legacy`, cache OFF,
storage-aware staging OFF, `resource`. The inactive bandwidth sample still
validates only as an explicitly constructed trusted staging component; ordinary
startup remains gated. NFS inputs retain the existing read-only topology and
authorization requirements. No live NFS/share/mount/export/firewall/systemd,
SSH, service, job, DB, checkpoint or cache cleanup was performed.

## Acceptance matrix

| Requirement | Evidence and result |
|---|---|
| Slower/faster links and bandwidth drop | Three real disposable worker caches, immutable source/data binding, signed inventory exchanges, real registry/coverage and selectors. Controlled rates 1/1000/10 B/s initially select pi02. One slow observation leaves the three-sample rolling median unchanged; two slow observations change selection to pi03; three fast observations restore pi02. Rates are synthetic arithmetic fixtures, not measured Pi speed. |
| Transparent cost | Each predicted worker cost equals unique missing bytes / reported median rate + unique missing object count × measured residual. Assert sample count, scenario lower/upper bounds and unknown cluster concurrency. Dataset aliases count once. Logs retain exclusions and size/contention uncertainty. No total-training-time prediction. |
| Old/no observations | Repeated fresh signed inventories cannot rejuvenate old rates. Old/empty samples fall back to original resource order; fresh inventories still permit locality independently. Offline inventory loses predictive certainty. Excluded nodes stay excluded. |
| Concurrent staging | Actual signed three-rank stage/start orchestration, bounded to two concurrent stage RPCs, with event/barrier synchronization. Every rank stages before the first START. Attempting maintenance at each real fetch-to-pin gap raises `CacheActivityBusy`; running pins block eviction under pressure. Trainer/isolation and NFS mount facts are mocked. |
| Full disk/capacity and pins | Combined test refuses a new copy at an actual configured quota while retaining a pinned input, no partial files and unchanged authoritative bytes. Existing rerun fetch/eviction tests inject ENOSPC, metadata-full, reserve shortage and failed unlink; no physical disk is filled. Reconciliation preserves pin counts. |
| Eviction crash and reconciliation | Existing tests kill actual child processes before and after unlink. New combined test exits after unlink before notification, shows advisory stale registry data, initializes a new local cache handle, reconciles without deletion replay, sends signed inventory removal, rejects unauthorized refetch, then performs one authorized missing copy and a zero-copy warm repeat. Signed registry returns to VERIFIED. Both authoritative source views remain unchanged. This is process-crash coverage, not hardware power-loss proof. |
| Fairness and reservations | Reuse actual queue/SQLite/owner-turn fixtures with real `JobBandwidth`, synthetic coverage/rates and mocked launch. Resource and repeated bandwidth runs have identical eight-round service traces; aged cold FIFO jobs start in rounds 0/1 despite warm arrivals. Infeasible head does not block another owner. Cancellation retains quota until confirmed release. DDP consumes one owner slot and all exclusive workers, never a partial group. Tests assert selected preference really used bandwidth rather than silently falling back. |
| Rollback | Resource/locality selection remains independent of optional rates. Switching an actual running three-rank coordinator back to resource leaves the DB and all three reservations unchanged. Source rollback was checked and applied only to a disposable copy; prior source bytes restored exactly. |
| Existing workflows | Selected resource OFF frozen decisions, locality exact-MB coverage, flags, authenticated inventory, private workspaces, queue, stage/cancel/recovery/portal and checkpoint regressions all pass. CPU saved-state resume checks model/optimizer/scheduler/RNG; this is not a physical cluster DDP claim. |

The queue helper's historical output label `LOCALITY_QUEUE` is reused by the new
bandwidth subclass; the actual preference is `JobBandwidth` and assertions verify
it. Its mocked coverage/rates do not establish hardware throughput or production
queue performance. Real signed-rate coverage is tested separately in the same file.

## Commands and actual results

All test commands ran from the application root with Python 3.12.3. The runner's
`--isolated` option copies source into temporary directories and excludes live
databases/runtime roots. No dependency installation or Internet transfer occurred.

Pre-edit baseline:

```bash
PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_bandwidth_policy.py test_cache_eviction.py
```

**45 PASS / two files.** Log: `/tmp/hybrid-11-4/baseline-tests.log`.

New suite development:

```bash
PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_storage_efficiency_acceptance.py
```

The initial run had **one failure and three errors in new test fixtures**: the
fixture created an intermediate cache SHA directory with mode 0755 instead of
0700, and a pin assertion omitted prospective incoming pressure on an exactly
full quota. Corrected fixture permissions and supplied the intended incoming
bytes. Existing fail-closed behavior was correct; no runtime fix was needed.
Logs `new-tests.log` and `new-tests-rerun.log` preserve both results. Corrected
standalone rerun: **10 PASS**, then all ten passed again in the broad run.
These failures are new fixture mistakes, not pre-existing production failures.

Final selected regression command:

```bash
PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_storage_efficiency_acceptance.py test_transfer_metrics.py test_bandwidth_policy.py test_cache_eviction.py test_worker_cache.py test_cache_fetch.py test_private_workspaces.py test_cache_inventory.py test_cache_coverage.py test_scheduler_metrics.py test_scheduler_locality.py test_scheduler_locality_acceptance.py test_scheduler_policy.py test_queue_controls.py test_storage_config.py test_staging_dispatch.py test_ddp_staging.py test_staging_recovery.py test_staging_portal.py test_hybrid_checkpoint_recovery.py
```

**428 distinct tests PASS / 20 files; zero failures, errors or skips.** Reruns are
not double-counted. Log: `/tmp/hybrid-11-4/final-tests.log`; SHA-256 and exact
per-file counts are in the evidence companion. This is a selected regression
run, not every repository test. Exact-MB acceptance used actual local bytes and
did not skip; it requires about 1.638 GB disposable free space.

## Safe manual verification

From the application root:

```bash
PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_storage_efficiency_acceptance.py
.controller-venv/bin/python -B -m hybrid_storage.config
.controller-venv/bin/python -B -m hybrid_storage.config --runtime staging --config config/storage-examples/nfs-bandwidth.json
git diff --check
git status --short
```

Expect ten passing tests; active settings `legacy`/OFF/`resource`; sample settings
`nfs`/ON/`bandwidth` with `automatic_startup: false`; no whitespace errors. Git
status includes the preserved earlier changes plus these five intended files.
The config commands validate only, with no service startup or DB migration.

Live link degradation, unmount/disconnect, full-disk filling and physical power
recovery are **not** manual steps authorized by this subphase. Any future physical
validation needs a reviewed isolated test plan covering **pi01, pi02 and pi03**;
the earlier Phase 2 two-Pi exception does not waive that requirement.

## Rollback and limitations

This turn has no deployed/runtime activation to undo. To undo only 11.4 source
changes, preserve any subsequent edits and inspect the scoped patch first:

```bash
# Run from the Git root: /home/rana/Projects/System Project/System_Development_Project
git apply --reverse --check /tmp/hybrid-11-4/11.4.patch
# Only if the check passes and no later edits should be retained in these hunks:
git apply --reverse /tmp/hybrid-11-4/11.4.patch
```

The first command passed against this workspace; reverse check/apply also passed
on a disposable copy, restoring the two original files byte-for-byte and removing
only the three newly added source/report files. **No rollback was applied here.**
The patch is relative to the pre-11.4 snapshot, not HEAD, so earlier uncommitted
Phase 10/11 changes remain. `/tmp` evidence/patch files are ephemeral; retain them
if needed before cleaning temporary files. Do not use reset/clean or touch DBs.

Operational rollback remains explicit `set_scheduler_policy('resource')` in a
reviewed trusted coordinator and omission of idle-maintenance calls. It affects
future placement, not running attempts. `locality` also remains usable without
rate samples. Nothing automatically changes active config. Evicted replicas can
be fetched again only through existing authorized staging; source rollback does
not undelete cache data or revoke durable pins.

Residual limitations from 11.2/11.3 remain: medians lag rapid changes; per-object
residuals depend on size; shared-NFS/cross-job contention is unknown; estimates
exclude training, workspace and checkpoint work. Disk/quota admission is checked
again during staging. All cache clients must cooperate with the activity lock;
dedicated protected roots and correct cohosted authoritative/workspace/checkpoint
root declarations remain prerequisites. Durable pins/orphaned helpers can block
maintenance indefinitely; never clear them by guessing. The registry remains
advisory until successful signed reconciliation. Real hardware durability, Pi
throughput and all-three-Pi DDP recovery are untested here. Historical loaded-run
cold-stage HTTP409 remains unresolved; the selected suites passed this turn.
The central controller/LOCAL SQLite/authoritative store remains a **single point
of failure**.

**11.4 and Phase 11 local implementation/selected acceptance: PASS. Production,
physical all-three-Pi and outstanding Phase 9 acceptance: NOT PASSED / not run.
STOP. No next phase implemented.**
