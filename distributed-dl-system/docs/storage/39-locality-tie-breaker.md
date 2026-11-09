# Phase 10.2 — verified locality as a resource tie breaker

9 November 2026. **10.2 local PASS; STOP before 10.3.** The implementation adds
explicit programmatic opt-in to the existing selectors. It does not activate a
production policy, alter fairness or replace the scheduler. Configuration wiring,
operator choice logs and workload simulation remain 10.4; the dedicated exact-MB
and starvation matrix remains 10.3. Evidence:
[39-locality-tie-breaker-evidence.json](39-locality-tie-breaker-evidence.json).

## 1. Starting state and files

Application root: `/home/rana/Projects/System Project/System_Development_Project/distributed-dl-system`.
Git root is its parent, branch `main`, HEAD
`7edef1ccd8e3264d9716a3482cbd214188d009b6`. The nine uncommitted 10.1 files were
present at start and preserved. The immediately preceding accepted local boundary
is the 10.1 report/user continuation, not a guessed new commit. A pre-edit snapshot
of 309 inventoried files, including untracked 10.1 files, lives in
`/tmp/hybrid-10-2/before`; hashes/status/HEAD are retained alongside it.
No reset, stash, commit or live deployment was performed.

| Exact file relative to application root | This turn's change |
|---|---|
| `hybrid_storage/locality.py` | New job-bound preference and immutable freshness/choice snapshots |
| `hybrid_storage/cache_coverage.py` | Batch coverage in one locked read transaction; existing single-query API preserved |
| `controller/scheduler.py` | Optional keyword-only `locality=None`; tie breaking after original filters/resource sort |
| `controller/distributed_dispatch.py` | Optional keyword-only `locality=None`; equivalent complete-group/member tie breaking |
| `controller/scheduler_metrics.py` | Effective policy field defaults to resource; locality observations do not falsely report resource |
| `tests/test_scheduler_locality.py` | Thirteen unit/integration tests |
| `hybrid_storage/README.md` | Storage-folder component entry |
| `tests/README.md` | Safe verification commands |
| `docs/storage/39-locality-tie-breaker.md` | This report |
| `docs/storage/39-locality-tie-breaker-evidence.json` | Commands, hashes, outcomes and scoped preservation evidence |
| `docs/storage/implementation-progress.md` | Append-only progress |

The scoped diff is `/tmp/hybrid-10-2/10.2.patch`, relative to the parent Git root.
It excludes the earlier 10.1 changes from its rollback scope.

## 2. Selection and fallback behavior

Existing resource probes, skip ordering, eligibility, returned object identity,
CPU/RAM primary ordering and OFF stdout are unchanged. With `locality=None`,
selectors do not import the locality component or read cache inventory. The
existing resource path remains the default everywhere, including normal legacy
dispatch and the explicitly constructed hybrid staging coordinator.

Trusted controller callers can construct
`JobLocality(coverage_api, authenticated_actor, job_id, input_manifest_sha256)`
after existing authorization, supplying the exact immutable binding already
validated for that job, and pass it as `locality=` to either selector. The DDP
selector also checks the preference job ID against its actual job argument.
No student preference, arbitrary path, unsigned inventory or mutable filename is
accepted as a locality fact. Construct/use on the owning controller thread; the
preference and SQLite connection are not shared across staging pool threads.
Existing callers are intentionally not changed to auto-construct preferences:
10.4 must wire the reviewed policy and trusted inventory owner explicitly.

### Single worker

All workers passing existing allowed/reserved/excluded/reachable/IDLE/resource
filters get full immutable job-input coverage. After the original stable CPU
ascending/RAM descending sort, only candidates with **exactly the winning CPU
usage and available RAM** can compete on missing bytes. Lower verified missing
bytes wins; equal missing bytes retains the existing inventory order. A warmer
worker cannot beat a better CPU/RAM primary score or enter through a hard filter.

If any eligible worker's inventory is unknown, stale, untrusted or inconsistent,
the entire placement uses the exact resource baseline. Even a noncompetitive
worker with unknown cache triggers this conservative fallback. It may forgo
locality opportunities; it never pretends unknown inventory is a cold, verified
cache. An unavailable inventory is not itself permission to mark an otherwise
resource-eligible worker offline or bypass its final admission/lease checks.

### DDP groups and ranks

The existing prepared/compatible/reservation/resource/ownership/resume filters
build the same buckets. Only workers in complete compatible buckets are placement
candidates; an incomplete bucket cannot contribute a rank or force a registry read.
Full coverage is read for all workers in complete buckets, not only the original
chosen workers. This permits replacing a cold boundary-tied member with a warmer
member without enumerating worker combinations.

First calculate the **unchanged** resource-only winning group. Preserve its
ordered `(CPU usage, negative available RAM)` profile. Within each compatible
bucket, sort by CPU, negative RAM, verified missing bytes, node ID, then take
exactly the original requested worker count. Compare only complete groups whose
ordered CPU/RAM profile matches the original winner; choose minimum summed missing
bytes. Equal byte totals retain the original bucket encounter order. Resource
shape differences, even equal aggregate CPU, do not become a reason to switch
groups for cache coverage. Within equal resource/byte ties, node-ID rank order is
unchanged. This is deliberately stricter than a general weighted cost scheduler.

Current `StagingDispatch.plans` uses the whole source/requirements/dataset on every
rank. Coverage is therefore read with a validated rank-zero/world-size declaration
for each eligible worker and reused as the **identical full required set** for
any rank assignment. Returned snapshots report actual selected rank order and
one missing-byte value per rank; total staging bytes sums these values. A digest
used by multiple logical paths on one rank counts once; staging the same digest
onto two workers counts twice. There is no sample-level cache, partitioned-shard
plan, world-size change or invented per-rank file set. A future trainer with
different rank input plans must extend this contract before using this policy.

If any complete-bucket candidate lacks current verified coverage, or the binding,
world size, target or byte facts disagree, return the unchanged resource winning
group and original rank order. The algorithm uses per-bucket sorts, not factorial
group/rank searches; no new thread or NFS operation is started by selection.

## 3. Registry consistency, diagnostics and security

`CoverageAPI.for_job_workers(actor, worker_ids, job_id, *, rank=None)` accepts a
tuple of 1–4096 unique validated worker IDs. It holds the existing inventory lock
and one registry read transaction for all queries. Refactored `_for_job` reuses
the same existing ACL, immutable binding/source/dataset, artifact authority,
unique-digest accounting, live inventory owner/session/sequence/TTL/heartbeat
validation as `for_job`. No connection, enrollment, migration or lazy registration
is created. Rows are a consistent SQLite snapshot; inventory lock prevents current
session/cursor mutation during the batch. Each result retains its computation
time and inventory age; this is not a future freshness or execution-lease promise.

Every comparison requires current eligible inventory for all compared workers,
matching job/binding/world size and sane complete byte accounting. DB lock/error,
missing authority, ACL denial, malformed or absent coverage and inventory
unavailability fall back to resource choice. These are **advisory placement
failures only**: the existing dispatch authorization and trusted staging must
still reject unauthorized input access. A selected node grants no right to stage
or run; no upload, reservation, lease, cancel or checkpoint gate is removed.

`JobLocality.snapshot()` returns an immutable `LocalityDecision` for the latest
selection and resets on reuse, including empty/no-complete-group selection. It
exposes effective policy/reason, selected IDs, per-rank missing bytes and each
worker's inventory eligibility/status/age/session/sequence/computation time,
required bytes and verified cached/missing bytes. Stale/unknown worker missing
bytes are **None**, not fabricated zero or full cold totals; aggregate bytes are
unknown on fallback. When batch authority cannot be read, no partial new freshness
snapshot is emitted. `document()` adds requested policy and total missing bytes;
it contains no filenames, raw NFS paths, missing-hash lists or credentials.

Existing optional metrics now report the effective policy (`locality` when a
verified comparison is applied, `resource` on fallback or no single-worker tie).
Resource-only documents retain their existing fields/values. These are trusted
operator interfaces; no student API/route or persistent choice-log sink was added.

No DB/schema/migration, job/FSM, format, upload/checkpoint transport, configuration
file/key/default or infrastructure/security setting changed. Existing config
parses locality but startup/coordinator gates continue to block automatic
activation. `scheduler_policy=resource` is unchanged; do not edit it to deploy
this subphase. SQLite stays controller-LOCAL. NFS input exports remain restricted
RO/root_squash, training sees private local inputs, and checkpoints retain trusted
authenticated publication. There is no new Internet-dependent worker transfer.

Queue fairness, owner quotas, aging and `mark_scheduled` atomic reservations are
untouched. Leases remain authoritative; DDP filters capability before placement,
while single-worker dispatch retains its existing post-selection capability gate.
Local cache free-space/quota/admission checks remain at trusted staging. Missing
input bytes do not estimate private-workspace/checkpoint disk needs, and this
subphase does not add a speculative capacity filter or direct-NFS fallback.

## 4. Actual validation

Existing `.controller-venv`, Python 3.12.3, PyTorch 2.14.1, Flask 3.1.3,
requests 2.34.2, numpy 2.5.3. No package installation or worker runtime downloads.
Commands ran from the application root with disposable source/DB/cache copies.

Before changes:

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_scheduler_metrics.py test_cache_coverage.py test_resource_allocation.py test_queue_controls.py test_storage_config.py
```

**74 PASS, five files**; `/tmp/hybrid-10-2/baseline-tests.log`.

Final commands:

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_scheduler_locality.py test_scheduler_metrics.py test_cache_coverage.py test_resource_allocation.py test_queue_controls.py test_storage_config.py
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_distributed_dispatch.py
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_cache_inventory.py test_ddp_staging.py
```

**87 + 7 + 54 = 148 distinct tests PASS, nine files, zero final failures/errors/
skips.** Logs: `/tmp/hybrid-10-2/final-tests.log`, `ddp-tests.log`,
`inventory-staging-tests.log` in that same directory. The native existing 2-rank
CPU Gloo suite used sandbox escalation for localhost rendezvous sockets only;
no Pi was contacted. Existing staged-DDP tests exercise mocked two/three-rank
group barriers, cancellation and leases; this is not three-Pi validation.

Thirteen new tests cover exact ties and primary-score/hard-filter preservation,
partial stale knowledge, DB/ACL/binding/malformed-data fallback, stable ties and
empty reuse, boundary group members, compatible group resource profiles, incomplete
groups/lease rejection, honest effective metrics policy, real authenticated FULL
inventory from warm/empty protected local caches and actual SQLite-backed placement,
stale/unauthorized/incorrect-binding knowledge, batch single-transaction tracing,
unchanged existing single-query results, invalid batch IDs and no DB mutation.
The new file was strengthened with DDP metadata-error/world/job mismatch fallback
and real registered three-rank full-input requirement equivalence, then rerun:
**13 PASS** in `/tmp/hybrid-10-2/locality-final.log` (included once in 148).
The existing 25 frozen pre-10.1 scenarios still match default-OFF outputs, object
identity, stdout and probe order exactly. Registry and authenticated inventory
regressions passed after the read-helper refactor.

Initial new-suite run had three setup errors because its new second-worker fixture
used the wrong key-ID header name (`X-DL-Inventory-Key`). Corrected the fixture to
the existing `X-DL-Worker-Key`; all thirteen tests then passed. No runtime protocol
changed. Initial log `/tmp/hybrid-10-2/locality-initial.log`. No pre-existing
baseline failures were observed in the selected suites. No benchmark or speedup
is claimed; batching currently repeats manifest validation per candidate.

## 5. Manual checks, rollback and acceptance

Safe repeat local verification, from application root:

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_scheduler_locality.py test_scheduler_metrics.py test_resource_allocation.py test_queue_controls.py test_storage_config.py
git diff --check
git status --short
```

Expect **56 PASS, five files**, no whitespace errors and the intended 10.2 changes
plus preserved 10.1 work. No service, live DB, NFS mount or worker access is needed.
Never start a second dispatcher solely to test selection. The full commands above
provide coverage/heartbeat/staging/native-DDP verification when appropriate.

Review the scoped patch, preserve later edits, and run from the parent Git root:

```bash
git apply --reverse --check /tmp/hybrid-10-2/10.2.patch
git apply --reverse /tmp/hybrid-10-2/10.2.patch
```

Only the reverse **check** was executed. Reversal removes only this turn's additions
and modifications, restoring the preserved 10.1 contents, including its untracked
metrics source. It does not reset Git, drop tables, downgrade schemas, restore an
old DB, unmount storage or delete jobs/checkpoints/cache/data. Overlapping later
source edits require manual review, not a forced application. `/tmp` evidence and
patch are session-local; retain copies if longer retention is needed. Omitting
`locality=` immediately restores resource selection for explicit API callers.

| Subphase acceptance | Result |
|---|---|
| Real registry coverage, exact unique inputs, current authorized inventory facts | PASS locally |
| Secondary locality only; resource feasibility/primary scores remain authoritative | PASS |
| Complete compatible DDP group, resource profile and original world size preserved | PASS |
| Unknown/stale/failed metadata returns resource baseline with honest freshness | PASS |
| Legacy/OFF choices, queue/quota/reservation/lease/staging regressions | PASS |
| Previous changes/data preserved; no live mutation | PASS |
| Exact 0/2/502 MB and starvation matrix | Deferred to 10.3 |
| Config activation, explainable production logs and workload simulation | Deferred to 10.4 |
| Physical all-three-Pi validation / deployment | NOT RUN; still required |

Residual risks: authenticated inventory is advisory and may become stale or cache
bytes may change after selection; staging must reverify hashes, capacity, ACL and
fresh leases. Conservative fallback may miss opportunities, exact telemetry ties
may be uncommon, and inventory-read/manifest-validation overhead is unbenchmarked.
No cache-capacity/NFS-throughput benefit is established. Current bootstrap/deployed
hybrid containment readiness remains a production prerequisite. No new hardware
probe was run; Phase 9's prior three-Pi physical gate remains pending, and the
earlier two-Pi exception does not waive it. Controller/LOCAL SQLite/authoritative
storage remains a **single point of failure**. **10.2 local PASS; STOP before 10.3.**
