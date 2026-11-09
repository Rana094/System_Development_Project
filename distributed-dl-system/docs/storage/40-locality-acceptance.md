# Phase 10.3 — deterministic locality and fairness acceptance

9 November 2026. **10.3 local PASS; STOP before 10.4.** Added acceptance tests,
not a runtime scheduler change or deployment. Exact results and fingerprints are
in [40-locality-acceptance-evidence.json](40-locality-acceptance-evidence.json).
Configuration activation, production choice logs and broader workload/policy
simulations remain 10.4. Mandatory physical pi01/pi02/pi03 acceptance is pending.

## 1. Baseline, preservation and changed files

Application root: `/home/rana/Projects/System Project/System_Development_Project/distributed-dl-system`.
Git root is its parent, branch `main`, HEAD
`7edef1ccd8e3264d9716a3482cbd214188d009b6`. At start, fifteen tracked/untracked
paths contained the prior 10.1/10.2 work. The user accepted continuing from 10.2;
no new commit or unrecorded phase completion was inferred. All 313 inventoried
files were snapshotted before changes under `/tmp/hybrid-10-3/before`, with
hashes/status/HEAD in that directory's parent. No user changes were stashed,
reset, committed or discarded.

| Exact file relative to application root | Change |
|---|---|
| `tests/test_scheduler_locality_acceptance.py` | Five new tests: one real-byte placement matrix and four queue/reservation simulations |
| `tests/README.md` | Test command, disk requirement and scope |
| `docs/storage/40-locality-acceptance.md` | This report |
| `docs/storage/40-locality-acceptance-evidence.json` | Actual results, commands, hashes and preservation evidence |
| `docs/storage/implementation-progress.md` | Append-only acceptance record |

**No runtime code was changed.** All controller/agent/hybrid-storage source,
configuration, previous tests and earlier audit/architecture/reports remain
byte-for-byte as they were at this turn's start. The two existing documentation
files retain their complete starting contents as prefixes. The scoped diff is
`/tmp/hybrid-10-3/10.3.patch`, relative to the parent Git root.

No API, DB schema/migration, flags/defaults, job state, artifact/checkpoint format,
queue/owner policy, transport, auth, lease, reservation or infrastructure changes.
Legacy fallback and `scheduler_policy=resource` remain defaults. The explicit
10.2 `locality=` selector API remains available; normal production dispatchers
are not activated for locality by this test subphase.

## 2. Exact-byte placement acceptance

The test publishes **actual 500,000,000-byte and 2,000,000-byte objects** through
the real streaming SHA-256 registry. One logical alias repeats the large digest
in the immutable dataset manifest. Source/requirements bytes are 72 unique bytes
and are physically cached on all three simulated workers, so their contribution
does not distort the requested missing-data fixture. Full job inputs are
**502,000,072 bytes per worker**, not an invented 502-byte substitute.

| Simulated worker | Verified data bytes | Verified source bytes | Missing bytes |
|---|---:|---:|---:|
| pi01 | 502,000,000 | 72 | 0 |
| pi02 | 500,000,000 | 72 | 2,000,000 |
| pi03 | 0 | 72 | 502,000,000 |

MB means **1,000,000 bytes**. Real separate local cache inodes are copied in bounded
chunks, fsynced, protected and SHA-verified. Worker cache scanners and authenticated
FULL challenge/response inventories run against controller-local SQLite. No sparse
files, writable hardlinks, synthetic cache-presence rows or mocked byte totals
stand in for the large artifacts. The three identities are local test fixtures,
not physical Pis. Fake prepared manifest wrappers are pinned through the real
`bind_job` validator; resource telemetry, prepared-environment compatibility and
ownership capability are mocked. No upload pipeline, trusted sandbox, actual
training, mount or power recovery is claimed by this placement matrix.

The exact-byte matrix verifies:

1. All six inventory permutations choose pi01 with equal CPU/RAM. Resource-only
   calls retain the original first-inventory choice. Both two- and three-rank
   locality groups are deterministic across the same six permutations. Selection
   leaves the entire disposable registry DB dump unchanged.
2. Insufficient CPU cores or RAM excludes warm pi01 before locality queries;
   pi02 is chosen, and pi01 is absent from the compared eligible coverage rows.
3. Two ranks choose pi01/pi02 with missing bytes `(0, 2,000,000)` and total
   **2,000,000**. Three ranks choose pi01/pi02/pi03 with
   `(0, 2,000,000, 502,000,000)` and total **504,000,000**. Duplicate logical
   paths count one digest per worker; the same small digest missing on two
   workers incurs two transfers. Every rank requires the full source/dataset;
   no DistributedSampler shard locality is fabricated.
4. CPU/RAM feasibility is checked **per rank**, not by pooling cluster capacity.
   When pi01 cannot fit, a three-rank group is impossible despite capacity on
   its peers; a valid two-rank pi02/pi03 group reports 504 MB missing. Reserved
   pi01 or unsupported ownership capability also prevents the full three-rank
   group. No partial group is launched or reserved by these selector calls.
5. Advancing the controlled clock past heartbeat freshness restores resource-only
   single/group ordering. Stale missing bytes and aggregates become **unknown**
   (`None`), retaining age/status instead of unsupported warm/cold certainty.
6. After real signed refreshes, removing pi01's current inventory eligibility
   likewise restores resource-only choice with unknown totals. Deleting only its
   disposable verified observation for the large digest cannot retain that hit:
   pi01 reports 500 MB missing, pi02 wins, and a subsequent authenticated FULL
   verification restores pi01's zero-missing choice.

The case allocates about **1.504 GB plus source/metadata** under an owned temporary
directory. It explicitly skips if less than 1.638 GB is available; there is no
scaled substitute or false PASS. This run had sufficient local disk and **no
skips**. Temporary test data is cleaned by the owning fixture only.

## 3. Fairness, starvation scope and reservation simulations

Four small simulations execute the existing `dispatcher.dispatch_queued_jobs`,
`job_store.fair_queue`, owner policy, atomic `mark_scheduled`, DB reinitialization,
reservation and terminal/control transitions in disposable controller-local SQLite.
Their transport/launch, resource snapshots, cache facts and ownership capabilities
are fixtures. Custom jobs use the existing **legacy** execution binding so the
simulation does not bypass or activate NFS production runtime gates. They call
the real explicit locality selectors after the normal fair queue order, not a
new scheduler. They do not prove authenticated NFS job execution or live leases.

- **Continuous warm arrivals:** two old cold jobs compete with a warm owner for
  eight controlled arrival rounds. Cold jobs miss 502 MB on every eligible worker;
  warm jobs miss zero. The existing one-active-per-owner test policy, aging and
  FIFO remain in force. Cold jobs start in rounds **0 and 1**, in FIFO order.
  Ten jobs receive service, and one warm job remains queued. Complete dispatch
  and attempt traces match resource-only execution and two independent locality
  runs, including DB reinitialization preserving owner service history.
- **Infeasible head:** an old cold job requesting an unavailable GPU stays queued
  and receives no reservation, while another owner's feasible warm job starts in
  the same pass. Resource and locality attempt/start traces agree. No guarantee
  is made for permanently infeasible jobs.
- **Uncertain/cancelling quota:** a SCHEDULED cold job keeps its owner's active
  slot across DB reinitialization and cancellation intent. A second cold job
  cannot bypass the quota even with a free worker; other-owner work can start.
  Only the simulation's confirmed cancellation permits the next cold placement.
- **DDP gang allocation:** an old cold two-rank job consumes one owner active slot
  and two exclusive worker reservations. A younger warm two-rank job cannot be
  partially reserved on the remaining worker, but a third owner's single-worker
  job can use it. The cold owner's next job remains quota-blocked. Real rank rows
  and unique reservations match these outcomes.

These establish bounded **no starvation caused by locality** in the tested feasible
workloads; they are not a proof of unconditional progress under permanent resource
shortage, outages, nonterminating jobs or arbitrary arrivals. CPU/RAM/GPU filters,
fair owner turns and reservations remain authoritative. Disk-space/cache quotas
and private-workspace capacity still belong to trusted stage-time admission;
missing bytes do not predict workspace/checkpoint disk needs. No speculative disk
filter, silent direct-NFS fallback or throughput model was added.

## 4. Commands and actual results

Environment: existing `.controller-venv`, Python 3.12.3, PyTorch 2.14.1,
Flask 3.1.3, requests 2.34.2, numpy 2.5.3. No package installation/download.
All commands ran from the application root. The isolated runner excludes live
DB/runtime roots, copies source to temporary storage and suppresses bytecode writes.

Baseline before edits:

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_scheduler_locality.py test_scheduler_metrics.py test_cache_coverage.py test_queue_controls.py test_resource_allocation.py test_storage_config.py
```

**87 PASS, six files**, `/tmp/hybrid-10-3/baseline-tests.log`; no pre-existing
failures observed in these selected suites.

Final regression commands:

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_scheduler_locality_acceptance.py test_scheduler_locality.py test_scheduler_metrics.py test_cache_coverage.py test_queue_controls.py test_resource_allocation.py test_storage_config.py test_ddp_staging.py
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_distributed_dispatch.py
```

**108 + 7 = 115 distinct tests PASS, nine files, zero final failures/errors/
skips.** Logs `/tmp/hybrid-10-3/final-tests.log` and `ddp-tests.log`. The existing
native two-rank CPU Gloo regression used sandbox escalation for localhost sockets
only. Existing mocked two/three-rank staging/lease/cancellation tests and the
25 frozen resource-only decision scenarios also passed. No physical Pi, live NFS,
network interruption, power cycle, service operation or throughput test was run.

Development evidence: the first new large-cache fixture used recursive `mkdir`
whose intermediate algorithm directory inherited 0755; the existing cache correctly
rejected it. Fixed the fixture to explicitly create each owned cache component
0700. Initial log `/tmp/hybrid-10-3/acceptance-initial.log`: four queue tests PASS,
one new setup ERROR. The corrected file passed all five tests in
`acceptance-second.log`, then was strengthened with all DDP inventory permutations
and passed in the final full run. No runtime permissions, validation or security
checks were weakened, and no production defect required a code fix.

## 5. Manual verification and rollback

Safe exact acceptance rerun (requires the local temporary disk space above):

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_scheduler_locality_acceptance.py
git diff --check
git status --short
```

Expect five PASS with no skip, `LOCALITY_EXACT_MB` showing 0/2,000,000/502,000,000
missing bytes and 2,000,000/504,000,000 DDP totals, and `LOCALITY_QUEUE` showing
cold rounds `[0,1]` and equal repeated resource/locality traces. Whitespace checks
should be clean. Status should contain preserved 10.1/10.2 work plus only the five
files listed for this turn, assuming no subsequent user edits. Do not start a
second dispatcher or enable live locality solely to repeat this test.

From the parent Git root, review the scoped diff and preserve later edits before
source rollback:

```bash
git apply --reverse --check /tmp/hybrid-10-3/10.3.patch
git apply --reverse /tmp/hybrid-10-3/10.3.patch
```

Only the reverse **check** was executed. Rollback restores just this turn's test/
documentation changes while retaining all earlier runtime work. No schema rollback,
DB restore, cache/job/checkpoint deletion, mount or service action is required.
Never force the patch over later overlapping edits. `/tmp` artifacts are temporary;
retain reviewed copies if longer evidence/rollback retention is required.

## 6. Acceptance and residual risks

| 10.3 criterion | Result |
|---|---|
| Actual verified 0/2/502 MB fixture; equal resources choose pi01 | PASS |
| Warm pi01 with insufficient CPU excluded; other hard limits preserved | PASS |
| Stale/missing inventory provides no unsupported certainty | PASS |
| Two/three-rank exact byte sums, unique digests and per-rank resource capacity | PASS |
| Feasible cold jobs retain fair turns/FIFO under continued warm arrivals | PASS — bounded controlled simulation |
| Quotas, uncertain/cancel reservations and atomic complete DDP allocation | PASS |
| Prior changes preserved; runtime/config/security behavior untouched | PASS |
| Broader policy simulation/logging/flag wiring | Deferred to 10.4 |
| Physical pi01/pi02/pi03 NFS/training/recovery acceptance | NOT RUN; still required |

Inventory remains advisory; actual staging must reverify authorized bytes, local
space and fresh attempt/group leases. Local file/auth/SQLite tests do not establish
Pi throughput, real NFS identity/isolation, network/power durability or production
training containment. Physical readiness was not re-probed; Phase 9's three-Pi gate
remains pending, and the prior two-Pi exception does not waive it. The controller,
LOCAL SQLite and authoritative storage remain a **single point of failure**.
**10.3 local PASS; STOP before 10.4.**
