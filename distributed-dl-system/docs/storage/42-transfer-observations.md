# Phase 11.1 — measured NFS-to-local copy observations

9 November 2026. Scope: **11.1 only**, optional worker-local instrumentation.
The last implementation boundary evidenced by the repository was 10.4 local
PASS. Physical Phase 9/all-three-Pi acceptance remains pending. Existing Phase
10 changes are uncommitted and were preserved. Branch `main`, initial HEAD
`7edef1ccd8e3264d9716a3482cbd214188d009b6`.

## Behavior and exact files

| File | Change |
|---|---|
| `hybrid_storage/transfer_metrics.py` | New explicitly constructed, worker-bound bounded rolling statistics |
| `hybrid_storage/cache_copy_worker.py` | Successful private-FD copy reports its actual streaming byte count |
| `hybrid_storage/cache_fetch.py` | Optional collector receives successful copy bytes/time after independent parent verification |
| `agent/cache_storage.py` | Documents existing factory's optional collector argument |
| `tests/test_transfer_metrics.py` | Seventeen deterministic statistics and fake-NFS/local-copy checks |
| `tests/test_staging_portal.py` | Corrects an existing concurrent-readiness observation race at the committed acknowledgment boundary |
| `hybrid_storage/README.md`, `tests/README.md` | Usage and safe verification |
| `docs/storage/implementation-progress.md` | Appended subphase record |
| This file and `42-transfer-observations-evidence.json` | Scope, evidence, limitations and rollback |

`CacheFetcher(..., transfer_metrics=None)` keeps collection disabled by default.
The existing worker factory forwards the optional argument. No imports initialize
storage or start a service. No probes or background measurement loop are added.

The helper reports `transferred_bytes=total` only after streaming, checking the
authorized size/SHA-256 and source identity, rechecking the read-only mount,
readback verification and local fsync. The parent still independently verifies
the read-only private part. A rate sample requires that verification to succeed
and the helper byte count to exactly match the authorized size. Missing or
malformed optional byte telemetry loses the sample, not compatibility with an
older trusted helper. It cannot bypass hash, mount or lease gates.

Elapsed monotonic nanoseconds run from entry into the copy attempt through
parent verification and existing copy-counter accounting. This includes private
part setup, helper startup/wait, NFS reads, streaming hashes, local writes,
readback verification and fsync. It excludes cache-slot admission/wait, failed
attempts/retry delays, subsequent publication, workspace copies and training.
This is **effective application copy throughput**, not a raw link or wire rate.
NFS client/server page caches may accelerate reads; no cache-dropping or iperf
measurement is performed. A verified copy may remain an observation if a later
authority check cancels publication; observation is never stage readiness.

Warm cache hits and duplicate manifest paths produce no additional transfer
sample. Empty objects remain valid inputs but are excluded from rates. Failed,
corrupt, interrupted or timed-out copies produce no successful sample. Existing
copy attempt/failure/timeout counters remain separate and unchanged: successful
rates alone do not model outage/retry cost or failure probability.

## Statistics, age and concurrency

`TransferMetrics(configured_worker_id, max_samples=64, max_age_seconds=900)`
is explicitly owned by the trusted worker bootstrap. Worker IDs use the existing
inventory identity validator. Boundaries: 1–4096 samples and a positive finite
age at most one day. Memory is bounded; no SQLite or files are written. Samples
reset on process restart. Expired entries remain within the count-bounded ring
but are excluded from current statistics.

`snapshot()` returns a fresh numeric/constant-string document with worker ID,
recent/retained/lifetime sample counts, newest/oldest recent age, last observation
time/age, bytes and duration totals, median bytes/s, effective MiB/s, median
absolute deviation, minimum and maximum. `MiB = 1,048,576 bytes` and each rate is
`payload_bytes * 1e9 / duration_ns`. The median is per-copy, unweighted: sizes
and verification overhead affect samples. MAD describes observed variability;
it is not a predictive confidence interval. No minimum sample count is asserted
to be sufficient for scheduling.

Monotonic time governs freshness; wall time labels observations only. No samples
or stale samples return a null rate, never zero or invented throughput. A
backward monotonic clock excludes affected observations. Clocks/observer errors
are fail-soft at the fetch boundary; authorization and copy failures still
propagate normally.

Existing cache-root flock serialization limits one producer per cache root,
including independent handles/processes. `per_cache_root_copy_limit=1` describes
that invariant. `cluster_concurrent_transfers=null` explicitly states that
cross-root/worker NFS contention is unobserved. Fixed overhead remains null;
11.2 must measure/model it instead of treating this byte-dependent duration as a
constant overhead. No total training completion prediction is made.

## API, database, configuration and security

Only the internal helper readiness dictionary gains an additive integer field;
existing parent consumers accept it. The collector and snapshot are trusted
in-process interfaces. HTTP staging receipts, signed numeric metrics schema,
heartbeat/inventory protocol, website routes, jobs and SQLite schemas remain
unchanged. This subphase does not transport rates to the controller or make
them authoritative. That requires a reviewed freshness/authentication contract
in 11.2. No database migration is needed.

No new settings or policy activation: active `storage.json` stays legacy/cache
OFF/staging OFF/resource. Resource/locality selection and rollback remain as in
10.4; bandwidth remains runtime-unavailable. Production stability never depends
on collecting samples. Trusted bootstrap/isolation remains required before
enabling hybrid execution; no bootstrap/deployment is fabricated here.

The snapshot includes no filenames, artifact hashes, NFS addresses/paths,
credentials, owner or job identifiers. No new student endpoint is exposed. NFS
remains a narrowly exported read-only input source, trusted services alone
publish checkpoints, and SQLite remains controller-local. Cache/workspace pin,
lease and fence logic is unchanged. No cache eviction is implemented in 11.1.
Central controller, LOCAL SQLite and authoritative storage remain a **single
point of failure**.

## Trusted usage and manual verification

Within an already reviewed worker bootstrap, with existing operator configuration
and authorized staging/containment (this snippet performs no initialization):

```python
from hybrid_storage.transfer_metrics import TransferMetrics
from agent.cache_storage import nfs_cache_fetcher

observations = TransferMetrics(configured_worker_id)
fetcher = nfs_cache_fetcher(cache, source=configured_nfs_source,
                          quota_bytes=configured_cache_quota,
                          transfer_metrics=observations)
# Existing service performs authorized ensure(), never arbitrary digest fetches.
summary = observations.snapshot()
```

Before a successful nonempty copy: sample_count=0 and rate=null. After a cold
copy: one sample with actual bytes, positive duration and numeric rate. Repeating
unchanged inputs: no new sample. After max_age_seconds without new transfers:
status=stale, sample_count=0, rate=null. These are expected behaviors, not claims
that any live Pi has produced observations.

Safe local commands from the application root:

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_transfer_metrics.py test_cache_fetch.py test_worker_cache.py
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python -m hybrid_storage.config
git diff --check
git status --short
```

Expect 83 tests PASS in the first command, legacy/OFF/resource from config
validation, and only the intended working-tree changes. Test NFS mount identity
is mocked around actual local file streaming; no hosts are contacted. No test
changes `/etc`, mounts/exports, services or live data.

## Test results

Baseline: 66 PASS in `test_cache_fetch.py` and `test_worker_cache.py` before code
edits. Initial new suite: 14 PASS; extended suite adds advisory-clock failure,
forged helper bytes rejected by parent verification, and timed-out helper cleanup.
The first attempted baseline command named a nonexistent efficiency test; the
runner rejected it before executing tests. The corrected baseline passed. No
runtime workaround or weakened guard was used.

Broad command, executed serially within an isolated source copy:

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_transfer_metrics.py test_cache_fetch.py test_worker_cache.py test_private_workspaces.py test_staging_dispatch.py test_staging_portal.py test_ddp_staging.py test_scheduler_metrics.py test_scheduler_locality.py test_scheduler_policy.py test_storage_config.py test_queue_controls.py
```

See the companion evidence JSON for final per-file counts, status and log hashes.
This initial broad run passed 11/12 files; its existing portal test expected one
rank already READY when a different rank started its concurrent stage RPC. It
instead observed zero READY, which is valid before any acknowledgment. The exact
failure reproduced on the saved pre-11.1 runtime and original test in a separate
isolated copy (`pre-edit-portal-race.log`). The test now observes each committed
controller `record_ready` acknowledgment and verifies 1/2/3 READY and measured
ranks, rather than assuming serial rank copies. No runtime ordering, lease,
timeout, API or launch guard was changed. The full portal suite and final new
suite were rerun separately; see the evidence for latest results.

Latest-per-file aggregate: **247 distinct tests PASS across 12 files, zero
remaining failures/errors/skips**. Repeated runs are not counted twice. This
includes frozen OFF/resource metrics, configured resource/locality/rollback,
queue controls, actual local cache/workspace copies, signed mocked single/DDP
staging and owner-safe portal views. It does not include a new live hardware or
native PyTorch DDP run.

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_staging_portal.py
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_transfer_metrics.py
```

The new tests check known 8/10/12/10/1000 MiB/s synthetic observations (median
10, MAD 2), count/age bounds, no/stale samples, wall-clock changes, thread safety,
actual cold/warm/deduplicated local copies, successful retry, mount/quota/corrupt
failure, cancelled publication, optional telemetry failure, old/helper-metadata
compatibility, serialized concurrent copies and timeout cleanup. Synthetic rates
verify arithmetic only. Local copy timings are not reported as Pi benchmarks.

The prior 10.4 concurrent three-rank cold-stage HTTP409 remains an unresolved
historical loaded-run limitation, followed by passing isolated/serial recovery
tests. This instrumentation does not establish its cause or repair it. No
hardware, real NFS, all-three-Pi contention, training throughput or production
latency validation is claimed. Physical acceptance remains pending.

## Rollback and acceptance

Operational rollback: omit `transfer_metrics` on the next reviewed construction
or set an idle retained trusted fetcher's `transfer_metrics=None` in its owner
thread. No running job needs cancellation and no data/metadata needs deletion.
Existing resource policy rollback remains
`runtime.set_scheduler_policy('resource')`. Do not deploy or restart services
merely to run these examples.

Source rollback is a **scoped 11.1 patch**, including files already modified in
10.4. Save `/tmp/hybrid-11-1/11.1.patch` before ephemeral evidence expires. From
the Git root, inspect/check before applying:

```bash
git apply --reverse --check /tmp/hybrid-11-1/11.1.patch
# Only if reverting this subphase is intended and the check succeeds:
git apply --reverse /tmp/hybrid-11-1/11.1.patch
```

No rollback was applied. Do not use `git restore` or reset across earlier user
changes. Fingerprints and the pre-edit source snapshot are retained under
`/tmp/hybrid-11-1/`; intended documentation appends preserve their old prefixes.

**11.1 local PASS.** Acceptance: verified-copy byte/time observations, robust rolling per-worker
statistics with age/count, no active probes, optional/no policy dependency, and
resource/locality regressions are assessed in the evidence companion. Remaining
11.2 bandwidth prediction/transport, 11.3 eviction and 11.4 combined acceptance
are **not implemented**. STOP after 11.1.
