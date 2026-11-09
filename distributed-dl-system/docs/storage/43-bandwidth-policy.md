# Phase 11.2 — optional measured input-copy cost

9 November 2026. **11.2 only.** The prior boundary is 11.1 local PASS,
evidenced by its report/progress and actual code. `main` HEAD remains
`7edef1ccd8e3264d9716a3482cbd214188d009b6`; prior uncommitted work is preserved.
No live deployment or policy enablement. Phase 9/all-three-Pi physical acceptance
and a reviewed production bootstrap remain pending.

## Implemented behavior and changed interfaces

The existing `scheduler_policy=bandwidth` value is now accepted by the explicit
trusted staging/checkpoint coordinator, with resource as the unchanged default.
Ordinary portal/dispatcher startup still cannot provision hybrid execution from
JSON flags. Resource and locality remain usable without any throughput collector.

| Files | Change |
|---|---|
| `hybrid_storage/bandwidth.py` | Volatile authenticated-observation store and job-bound cost preference |
| `hybrid_storage/transfer_metrics.py` | Separate predictive summary from decomposed copy samples; retains the inclusive 11.1 snapshot |
| `hybrid_storage/cache_copy_worker.py`, `hybrid_storage/cache_fetch.py` | Measure helper copy interval; parent verifies it is within total elapsed time and independently verifies bytes before sampling |
| `hybrid_storage/inventory_reporter.py`, `hybrid_storage/inventory_protocol.py` | Explicit optional report v2, signed on the existing inventory channel; default v1 unchanged |
| `hybrid_storage/cache_inventory.py` | Accept optional measurements only after existing auth/session/sequence/DB validation; current read-only rate/disk facts |
| `hybrid_storage/locality.py` | Retains exact authorized coverage rows only for the bandwidth subclass; locality selection/output unchanged |
| `hybrid_storage/config.py`, `hybrid_storage/scheduling.py` | Trusted component opt-in, existing selector seam and bounded explainable choice logs |
| `controller/staging_dispatch.py` | Pass actual bounded staging parallelism to the policy; checkpoint coordinator inherits it |
| `tests/test_bandwidth_policy.py` | New measurement, auth, policy and actual local signed staging tests |
| `tests/test_scheduler_policy.py` | Two old bandwidth-unavailable assertions updated for the newly supported component policy; resource/locality assertions preserved |
| `config/storage-examples/nfs-bandwidth.json`, `config/storage-examples/README.md`, `config/README.md` | Inactive exact sample and reviewed-bootstrap requirements |
| `hybrid_storage/README.md`, `tests/README.md`, `docs/storage/implementation-progress.md` | Usage, tests and appended progress |
| This report and `43-bandwidth-policy-evidence.json` | Evidence, limitations and scoped rollback |

The single-worker and DDP selector algorithms are reused unchanged. The preference
is a typed `JobLocality` subclass passed through their existing optional seam.
CPU/RAM profiles stay primary, then estimated copy seconds break equivalent
resource ties. Full compatible DDP groups and existing full-input-per-rank plans
are required. There is no invented sample/shard cache or world-size change.
Queue fairness/aging, quotas, reservations, cancellation, authorization, leases,
stale-attempt fencing and the all-rank READY barrier retain their existing code.

## Measured inputs and estimate scope

11.1's inclusive rate already includes helper startup/parent verification. Adding
another overhead to that rate would double count. New samples decompose total
attempt time into a helper interval and measured residual. The helper interval
covers opening/validating NFS inputs, streamed reads/hash/local writes, helper
readback verification and fsync. Residual is total minus helper interval, including
private-part setup, process startup/wait, parent hash/readback and bookkeeping.
Clock failures or invalid intervals lose optional calibration, not copy safety.
Older helpers/inclusive-only observations never acquire an invented decomposition.

For each resource-eligible candidate, exact authorized unique missing hashes and
bytes come from current coverage. At least three decomposed successful nonempty
copies must be recent. Use the unweighted rolling median rate and median observed
per-object residual:

```
estimated_missing_copy_seconds = missing_unique_bytes / median_copy_bytes_per_second
                              + missing_unique_objects * median_residual_seconds
```

A digest referenced by multiple manifest paths counts once per worker; the same
required digest on two DDP workers incurs two copies. Missing empty objects count
as objects and still require calibration. Fully warm inputs have zero missing-copy
cost, which does not mean instantaneous staging. Failed copies, retry delays,
cache-hit verification, private workspace copying, checkpoint restore, queue wait
and training are excluded. **No predicted total completion time is provided.**

The residual is measured, but assuming a fixed cost per object is a model
assumption: parent verification also depends on size. Logs expose sample payload
range, average missing-object size and extrapolation. An average within the range
does not prove the full object-size distribution is represented. Min/max observed
rates and overhead produce optimistic/pessimistic scenario values, enveloped with
the point estimate. These are not confidence intervals, guarantees, failure-cost
models or calibrated hardware benchmarks. NFS/client page caches affect rates;
no iperf, cache dropping, probes or continuous speed tests run.

Disk facts must match the same live authenticated coverage session/sequence.
The conservative logical space input is missing cache bytes plus the full private
workspace input bytes. Logs include reported disk availability and explicitly
state that quota headroom, workspace limits, metadata/reserves and other consumers
remain unverified. Insufficient reported disk or any unknown comparison fact
restores the original resource choice; this advisory policy does not replace
stage-time quota/free-space/hash/lease admission or assert placement capacity.

## DDP and concurrent transfer uncertainty

The coordinator passes its actual `stage_parallelism` (default 3, supported 1–32).
Per-worker copy producers retain cache-root flock serialization. For a candidate
DDP group, simulate rank-order greedy assignment to that bounded number of slots.
The point estimate applies a conservative multiplier equal to the group's maximum
active slots to each worker copy cost; the optimistic scenario assumes no such
slowdown. This is an explicit saturated-shared-server scenario, **not measured
aggregate NFS capacity**. Cluster-wide active transfers and cross-job contention
remain unknown. Other bottlenecks can make actual time exceed the scenario range.

Group choice is a bounded heuristic: preserve the original winning ordered
CPU/RAM profile, sort within existing compatible buckets by that profile then
worker cost/ID, and compare complete equivalent groups by modeled wave makespan.
It does not enumerate all group permutations or optimize whole-queue completion.
Existing inventory/worker-order tie behavior survives fallback. No infeasible,
reserved, excluded, incompatible or lease-incapable worker is rescued by a rate.

## Authentication, freshness, config and security

Trusted worker bootstrap explicitly shares `TransferMetrics(credential.worker)`
between its fetcher and `InventoryReporter(transfer_metrics=...)`. The constructor
rejects a different worker identity. Without that argument the reporter emits
unchanged inventory v1. With it, report v2 adds only `transfer`, which contains a
bounded numeric summary and generation timestamp. The existing endpoint, worker
credentials, request nonce/session, controller epoch, report HMAC and body bounds
are reused. No artifact transfer/download API or new endpoint is introduced.

Controller bootstrap opts into
`CacheInventory(..., transfer_observations=TransferObservations())` and supplies
its same-registry `CoverageAPI` to the coordinator. Summary ingestion happens
only after the existing exact outstanding-challenge authentication, enrollment,
cache boot/generation/sequence and transactional inventory checks. A duplicate
accepted reply refreshes neither observation age nor disk/inventory timestamps.
Malformed/missing advisory telemetry clears old predictive certainty while valid
inventory remains usable. Invalid auth still fails the entire report.

Source monotonic sample age, generation time/transit, receiver monotonic elapsed
time and the existing 30-second permitted clock-skew bound are combined
conservatively. Default maximum predictive age is 900s, including the skew
allowance; it is not 900s plus allowance. Default minimum count is 3. No samples,
insufficient samples, expired metrics or stale/offline inventory mean unknown,
not zero bandwidth. Session/sequence changes must match fresh coverage; offline,
controller restart and close discard observations. No predictive rows are
persisted, so restart requires new authenticated reporting. Reset collectors when
changing NFS source/local-device configuration; no heterogeneous-path model exists.

Old workers remain accepted with unknown rates/resource fallback. Older
controllers reject opt-in report v2: upgrade reviewed controller code before
explicitly enabling reporter collection. Disabling reporter collection restores
v1. No current worker service or active config was changed in this subphase.

No DB migration, training/checkpoint format, job state, upload/student route,
staging receipt, signed student-visible metrics, execution protocol, credentials,
exports or service configuration changed. SQLite stays controller-local. Inputs
remain narrowly exported read-only, with root_squash; training receives local
private inputs and trusted checkpoint publication remains mandatory. AUTH_SYS
is not a student security boundary. The central controller/LOCAL SQLite/
authoritative storage remains a **single point of failure**.

## Explainability and operational rollback

Existing protected `SCHEDULER_CHOICE` logs/history now include a bandwidth section
only for requested bandwidth policy: cost formula/scope/exclusions, measured rate,
residual overhead, age/count, payload range, missing bytes/object count, disk
status, scenario range, selected group estimate and explicit contention model.
No filenames, hash lists, NFS paths or credentials enter these records or any new
student endpoint. Predicted worker detail is capped at 32 with truncation marked;
existing node detail/history/128KiB record bounds and fail-soft logging remain.
Resource with no explicit logger still delegates directly, with no inventory/rate
query or automatic tracing. Locality does not query throughput.

Immediate existing trusted-controller rollback:

```python
runtime.set_scheduler_policy('resource')
# Optional: stop explicitly requested resource diagnostic logging.
runtime.scheduler.choice_logger = None
```

This affects future selection only and synchronizes lifecycle settings without
DB, input, checkpoint, pin, lease, reservation or running-job mutation. Rate
collection can be omitted at the next separately approved bootstrap. Optional
bandwidth estimates are not a prerequisite for production stability.

## Commands, tests and manual checks

Run from the application root; isolated copies exclude live DB/runtime roots:

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_transfer_metrics.py test_cache_inventory.py test_scheduler_locality.py test_scheduler_policy.py test_storage_config.py
```

Pre-edit baseline: **102 PASS in five files**. Initial new suite: 18 PASS; expanded
focused suites passed before broad regression. The broad command was:

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_bandwidth_policy.py test_transfer_metrics.py test_cache_fetch.py test_cache_inventory.py test_cache_coverage.py test_scheduler_metrics.py test_scheduler_locality.py test_scheduler_policy.py test_storage_config.py test_queue_controls.py test_resource_allocation.py test_staging_dispatch.py test_staging_portal.py test_ddp_staging.py test_hybrid_checkpoint_recovery.py
```

It passed all 15 files. After final range/validation/log-bound refinements, rerun:

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_bandwidth_policy.py test_cache_inventory.py test_scheduler_locality.py
```

Latest-per-file results, exact counts and log hashes are in the evidence companion.
**290 distinct tests PASS across 15 files, zero failures/errors/skips.** Reruns
are counted once; the final targeted command passed 73 tests in three files.
Tests include real helper/local copy intervals with mocked mount facts, synthetic
known-rate arithmetic/outliers/decomposition, actual signed Flask inventory v1/v2,
forgery/replay/duplicate/freshness/offline/restart handling, 2/3-rank policy models,
actual signed single/three-rank local staging and barriers with mocked trainers,
capacity/unknown fallback, resource rollback, bounded logs and unchanged selected
resource/locality/queue/coverage regressions. The existing CPU checkpoint test
resumes saved model/optimizer/scheduler/RNG state. Synthetic calibration in policy
fixtures is explicitly synthetic; no hardware throughput was fabricated.

Safe manual commands (read-only validation, not enablement):

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_bandwidth_policy.py
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python -m hybrid_storage.config --runtime staging --config config/storage-examples/nfs-bandwidth.json
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python -m hybrid_storage.config
git diff --check
git status --short
```

Expect the new suite to pass, component sample `bandwidth` with
`automatic_startup=false`, active default `legacy`/flags OFF/`resource`, and only
intended working-tree changes. The sample without `--runtime staging` must reject
ordinary startup. No service should be started to perform these checks.

No live Pi/SSH/NFS, hardware throughput, real concurrent-server calibration,
production training-time model or native network DDP validation was run. The
historical 10.4 loaded-run staging HTTP409 remains an unresolved limitation, not
claimed repaired by these tests. All-three-Pi deployment/validation remains
required; the old Phase 2 two-Pi exception does not waive that gate.

## Source rollback, residual risks and acceptance

The pre-edit 278-file snapshot and an 11.2-only patch live under
`/tmp/hybrid-11-2/`; retain them before temporary evidence expires. At the Git root,
inspect and check before intentionally reverting this subphase:

```bash
git apply --reverse --check /tmp/hybrid-11-2/11.2.patch
# Apply only if rollback is intended and no later edits would be lost:
git apply --reverse /tmp/hybrid-11-2/11.2.patch
```

Only reverse-check validation was performed; no rollback, commit, reset, stash or
service deployment was performed. This scoped patch preserves all earlier user
changes. Before deploying older controller code, disable opt-in v2 reporter
collection as part of a separately reviewed rollback; do not change live services
for this local implementation exercise.

Residual risks: success-only/size-dependent observations, unmeasured cross-job
contention and local disk/quota limits, wall-clock skew tolerance, greedy group
heuristics, volatile telemetry and slow log/DB I/O can reduce estimate quality.
The modeled scenario range is not an upper bound on outages or real completion.
Rates may stop being available during long warm-cache periods; resource fallback
is intentional. No automatic performance monitor or rollback trigger is added.

**11.2 local PASS.** Acceptance requires transparent measured inputs/uncertainty, authenticated
freshness, opt-in policy, unchanged hard/fair/lease gates and safe resource/locality
fallback, backed by local tests. See evidence for PASS/FAIL. Phase 11 remains
incomplete: 11.3 eviction and 11.4 combined efficiency/fault acceptance are not
implemented. **STOP before 11.3.**
