# Phase 9.2 — bounded staging of the actual reserved DDP group

9 November 2026, Asia/Dhaka. Implements **9.2 only** following the
[9.1 code audit](34-ddp-audit.md). Local verification is recorded below and in
[machine-readable evidence](35-ddp-staging-evidence.json). No deployment or
physical pi01/pi02/pi03 acceptance is claimed. STOP before 9.3.

## 1. Behavior and exact files

The existing resource selector, whole-group reservation, prepared environment,
one-rank-per-worker Gloo launcher and checkpoint format are retained. For an
explicitly installed hybrid coordinator, multiple reserved workers now stage
concurrently, with at most `stage_parallelism` outstanding staging calls. The
default is three; the trusted operator can choose an integer from 1 through 32.
This limits concurrent calls, not the job's supported world size. A one-worker
job keeps its synchronous staging call.

Every rank receives its full authorized immutable source and dataset manifests,
including requirements and every dataset file. The current trainer reads the
entire dataset before applying DistributedSampler and evaluates it in full.
There is no declared file-shard contract. No sample-level cache, rank-index file
filter or new ownership grant is inferred. Warm objects are still verified by
the existing worker cache/workspace code; only missing digests read NFS.

| File | Change |
|---|---|
| `controller/staging_dispatch.py` | Constructor limit; `_stage_request` checks exact reserved rank/lease; `_record_staged` verifies exact receipts/metrics; `_stage_parallel` schedules bounded RPCs and the all-rank barrier; `_cleanup_staging_group` reuses signed cancellation and existing reconciliation; `dispatch` aborts a DDP group on restore/start failure. |
| `controller/checkpoint_recovery.py` | Forwards the optional concurrency limit to its existing parent; checkpoint writer, reader, publication and recovery format unchanged. |
| `tests/test_ddp_staging.py` | Sixteen targeted local concurrency/authority/fault tests. |
| `tests/test_staging_dispatch.py` | Disposable two/three-worker and dataset-ACL fixtures; agent-specific watcher mocks leave executor threads real; staging failure now expects release after every quiescence proof. |
| `tests/test_staging_recovery.py` | Uses the same narrow watcher mock on a restarted fixture agent; independent migration writers remain real threads. |
| `tests/test_hybrid_checkpoint_recovery.py` | Narrows replacement-agent watcher mock; DDP restore failure expects safe whole-group cleanup before release. |
| `tests/README.md` | Reproducible local test command and scope. |
| `docs/storage/35-ddp-staging.md`, `35-ddp-staging-evidence.json` | This subphase report/evidence. |
| `docs/storage/implementation-progress.md` | Appended 9.2 record, preserving prior progress including uncommitted 9.1. |

Starting Git root is the application directory's parent
`/home/rana/Projects/System Project/System_Development_Project`, branch `main`,
HEAD `de297dc7d99eb80e52498145fe05b033e5afdaad`. Starting modifications were the
9.1 progress addition plus its two untracked audit documents. A 295-file snapshot
at `/tmp/hybrid-9-2/before` and `before.json` preserves this turn's baseline.
No stash, reset, commit, live DB import, job-data move or deletion occurred.

## 2. Authority, interfaces and failure semantics

The coordinator's owning thread computes plans, validates placement, persists
fresh bounded leases, verifies replies and updates local SQLite. Pool threads
only perform existing signed HTTP staging RPCs to distinct workers. No registry
connection crosses into the pool, no `check_same_thread=False` workaround and no
transaction remains open while waiting for worker I/O. The supplied HTTP
callable must support concurrent calls to distinct nodes; `requests.post` creates
independent requests. Do not supply a mutable shared Session without an explicit
thread-safety strategy. Each worker retains its existing single cache-producer
lock, quotas, free-space checks, hash verification and private workspace rules.

Replies are stored by logical rank, never completion order. Both a complete,
authenticated receipt and successful trusted `record_ready` are required for
each rank. The final `require_ready` checks the whole group's original attempt,
ACL/input identity, rank set, reservation fingerprints and all live lease
identities/deadlines. The existing `start` checks that complete group again
before **each** launch. Side-table phase `READY` is the existing equivalent of
all ranks STAGING_READY; jobs remain SCHEDULED until the existing running/resume
barrier. No new job state or launch protocol is introduced.

The existing launcher sends rank start requests sequentially after this barrier;
the supported trainer then joins its normal Gloo process-group rendezvous. This
is not a claim of simultaneous OS process creation or a new distributed commit
protocol for arbitrary student programs.

On any detected DDP staging failure, no more ranks are admitted, pending unsent
futures are cancelled, and the controller persists group ABORTING/STOPPING and
blocks all attempt lease renewals. Signed cancellation targets **every reserved
rank**, including ranks whose stage request was never delivered. Worker journals
install durable tombstones, interrupt active copies and refuse delayed stage or
start replays. Running RPCs are joined before a second cleanup pass; late READY
replies are discarded. Cancelling a Python future alone never proves remote
quiescence. Exceptions do not release a worker slot directly.

Existing cleanup releases the group only after every member has authenticated
TERMINAL + pins-released proof or its persisted greatest possibly delivered
lease deadline plus skew/stop margin has passed. Offline or uncertain workers
keep the **whole** reservation set until that condition holds. Changed reservation
fingerprints remain held for reconciliation/review. Cleanup errors leave durable
ABORTING state for `tick()`. Owner cancellation ends CANCELLED; otherwise the
existing retry budget determines QUEUED or FAILED. No per-rank retry is invented.
The same path handles a failed checkpoint restore or partial rank launch: live
descendants retain pins/reservations until the normal containment proof/fence.

Network routes, payload schemas, HMAC credential domains, database schema and
storage JSON keys do not change. Constructor keyword `stage_parallelism=3` is
optional on both `StagingDispatch` and `CheckpointDispatch`; it is trusted
bootstrap configuration, not a student-controlled job field. All shipped flags
remain `legacy`, cache OFF, staging OFF, resource policy. Normal legacy dispatch
and existing in-flight legacy jobs never enter this code path.

Inputs still come from the narrowly scoped read-only NFS view into protected
local cache/private copies. Student code receives local staged paths, no NFS or
cache privilege, no additional HTTP download API and no arbitrary NFS RW access.
No infrastructure, credentials, systemd, exports/fstab, world-size support,
checkpoint format or scheduler eligibility/fairness rule was changed.

## 3. Verification and evidence

Before changes, isolated existing staging/cleanup tests passed: **59 tests**
(25 dispatch + 34 recovery), two files, zero failures/errors/skips:

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_staging_dispatch.py test_staging_recovery.py
```

Final verification commands (from the application root):

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_ddp_staging.py test_staging_dispatch.py test_staging_recovery.py test_hybrid_checkpoint_recovery.py
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_distributed_dispatch.py test_group_checkpoint.py test_group_recovery.py test_hybrid_checkpoint_ddp_resume.py test_storage_config.py
git diff --check
```

Final result: **133 tests PASS**, nine files, zero failures/errors/skips:

| Final test file | PASS |
|---|---:|
| `test_ddp_staging.py` | 16 |
| `test_staging_dispatch.py` | 25 |
| `test_staging_recovery.py` | 34 |
| `test_hybrid_checkpoint_recovery.py` | 18 |
| `test_distributed_dispatch.py` | 7 |
| `test_group_checkpoint.py` | 8 |
| `test_group_recovery.py` | 7 |
| `test_hybrid_checkpoint_ddp_resume.py` | 1 |
| `test_storage_config.py` | 17 |

The first command passed 93 tests, 4/4 files; the second passed 40 tests, 5/5
files. Whitespace check passed. The native three-rank run met the existing
`atol=1e-8, rtol=1e-7` final floating-state tolerance: the only reported difference
was optimizer `state.4.exp_avg_sq`, maximum absolute delta
`1.862645149230957e-09` on each rank; two uninterrupted controls matched exactly.
The test also verifies saved-state restoration, RNG/progress and final commit.
These are localhost correctness measurements, not storage performance benchmarks.

Logs and hashes are recorded in the evidence; raw logs are under
`/tmp/hybrid-9-2/`. The second command requires reviewed localhost
socket access for local CPU Gloo; it does not access physical workers, install
packages or modify host services. Test runner copies exclude live DB/runtime data.

Sixteen new cases cover full file sets, two/three-worker overlap, out-of-order
rank binding, limits 1/2/3 and invalid limits, default SQLite thread protection,
all-ready/expired-peer lease checks, authorization revocation, changed reservation
identity, cancellation, missing objects, wrong signed receipts, lost READY replies,
in-flight private-copy failure, offline expiry/reconnect and partial launch failure.
Barriers/events prove concurrency; there are no timing or throughput assertions.

Development runs exposed two invalid new test fixtures (impossible negative lease
TTL and deleting immutable source ownership) and a replacement-agent watcher that
the old global Thread mock had hidden. Fixed fixtures use consistent expired TTL,
the actual dataset-reader revocation API and agent-specific watcher mocks for all
disposable agents. These were not pre-existing baseline failures. Earlier failing
logs are retained alongside the final evidence instead of being rewritten.

## 4. Manual checks and expected results

Run the two isolated commands above. Expect all selected tests to pass, no skips,
no launch before group READY, zero starts on staging failure, and all reservations
held while any live/uncertain member lacks cleanup proof. The offline fixture then
advances only its disposable lease clocks and checks release/reconnect cleanup.
The native three-rank test performs real SDK save/crash/resume on localhost,
using mocked outer launcher/isolation and fake NFS facts. It is not a Pi test.

```bash
git diff --check
git diff -- controller/staging_dispatch.py controller/checkpoint_recovery.py
cat config/storage.json
```

Expect a clean whitespace check, changes confined to the documented interfaces,
and the unchanged legacy/OFF/resource configuration. Do not enable hybrid runtime
or run SSH/deployment/failure tools as part of this local check.

## 5. Rollback and residual limits

To diagnose concurrency using a separately reviewed trusted bootstrap, choose
`stage_parallelism=1`; this retains the new all-group safety/cleanup behavior.
It does not enable the hybrid component or apply a live service change.

For source rollback, preserve later edits and deliberately quiesce/hold hybrid
work under the existing lifecycle before switching code. The scoped patch
`/tmp/hybrid-9-2/9.2.patch` is relative to the Git root and excludes earlier 9.1
changes. After inspecting it, from that Git root:

```bash
git apply --reverse --check /tmp/hybrid-9-2/9.2.patch
git apply --reverse /tmp/hybrid-9-2/9.2.patch
```

Only the reverse-check is executed in this subphase. No DB migration rollback,
table drop, cache/object/job/checkpoint removal, mount operation or service restart
is necessary. Keep additive metadata for existing hybrid jobs. Keep new enrollment
legacy/OFF unless separately tested and explicitly enabled.

The bound is per coordinator/group, not a calibrated cluster-wide bandwidth budget.
Several controller processes would need separate deployment-level coordination;
existing single-dispatcher ownership still applies. Request timeouts remain the
existing bounded connect/read timeouts, not a new absolute wall-clock guarantee
against a malicious drip-feeding peer or uninterruptible kernel I/O. Long staging
still fails closed at existing lease deadlines; there is no silent lease revival
or direct-NFS fallback. Cleanup can retain reservations during an outage, which is
intentional safe behavior. No measured NFS speedup, memory budget or live mount
timeout/isolation claim is made. The central controller, local SQLite and
authoritative checkpoint disk remain a **single point of failure**.

## 6. Acceptance and boundary

| 9.2 local criterion | Result |
|---|---|
| Real full-file inputs for each reserved rank; no inferred sampler shards | PASS |
| Bounded parallel stage calls, ordered rank binding and DB thread ownership | PASS |
| All ranks READY and valid group/attempt/reservation/ACL before launch | PASS |
| Partial failure aborts entire group; uncertain slots/pins held until safe | PASS |
| Missing data, cancellation, stale authority/replays, reconnect, partial launch | PASS |
| Existing legacy, coordinated checkpoint/recovery and default-OFF regressions | PASS |
| Preserve pre-existing 9.1 changes and runtime data; no live deployment | PASS |

Physical all-three-Pi validation, production isolation/NFS contention and the
Phase 9.4 fault matrix remain pending. No 9.3 checkpoint-publication redesign or
format migration was implemented. **STOP at 9.2.**
