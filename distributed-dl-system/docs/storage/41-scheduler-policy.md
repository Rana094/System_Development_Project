# Phase 10.4 — configured resource/locality policy and choice logs

9 November 2026. **10.4 local PASS; STOP.** Existing resource selectors, fair
queue, reservations and launch/recovery paths are extended, not replaced. The
active installation remains legacy/resource; no live enable or deployment was
performed. Physical pi01/pi02/pi03 validation and Phase 9's hardware gate remain
pending. [Evidence](41-scheduler-policy-evidence.json) records exact commands,
source fingerprints, test history, simulations and rollback.

Locality is an opt-in greedy tie breaker, **not a whole-queue byte optimizer**.
A synthetic counterexample increases staged bytes from 2 to 100; immediate
resource rollback restores 2 without reordering the jobs. This is a modeled
limitation, not a benchmark or a reason to claim a measured speedup.

## 1. Starting state and files

Application root: `/home/rana/Projects/System Project/System_Development_Project/distributed-dl-system`.
Git root is its parent, branch `main`, HEAD
`7edef1ccd8e3264d9716a3482cbd214188d009b6`. Eighteen prior modified/untracked
paths contained 10.1–10.3 work at start. Latest accepted local boundary is 10.3
plus the explicit user continuation, not a guessed new commit. Before edits,
316 inventoried files were copied to `/tmp/hybrid-10-4/before`; hashes/status/HEAD
are retained alongside it. Prior edits are preserved; no stash/reset/commit.

| Exact file relative to application root | Change |
|---|---|
| `hybrid_storage/scheduling.py` | New validated policy bridge, bounded operator records and immediate resource fallback |
| `hybrid_storage/config.py` | Explicit trusted-staging validator and read-only `--runtime staging` CLI mode |
| `controller/staging_dispatch.py` | Use configured policy with actual authorized plans; retain existing selectors/reservation/staging/leases |
| `controller/checkpoint_recovery.py` | Forward optional same-registry coverage/logger to the existing coordinator |
| `controller/scheduler_metrics.py` | Opt-in bounded filtered IDs/candidate resource details; old documents unchanged by default |
| `controller/scheduler.py` | Supply IDs/resource values to an already-active diagnostic observer |
| `controller/distributed_dispatch.py` | Same bounded diagnostics for existing group filters/candidates |
| `config/storage-examples/nfs-resource.json` | Exact trusted-component resource settings; inactive example |
| `config/storage-examples/nfs-locality.json` | Exact trusted-component locality settings; inactive example |
| `tests/test_scheduler_policy.py` | Seventeen config/selection/logging/simulation/actual local dispatch/resume tests |
| `config/storage-examples/README.md` | Component validation, logging and policy rollback instructions |
| `config/README.md` | Index the new inactive samples |
| `hybrid_storage/README.md` | Document the storage-folder policy component |
| `tests/README.md` | Safe repeat verification and limits |
| `docs/storage/41-scheduler-policy.md` | This report |
| `docs/storage/41-scheduler-policy-evidence.json` | Commands, results, hashes and preservation evidence |
| `docs/storage/implementation-progress.md` | Append-only progress record |

Scoped source diff: `/tmp/hybrid-10-4/10.4.patch`, relative to parent Git root,
against this turn's starting contents, including previously untracked metrics code.
`config/storage.json`, node inventory, queue policy, leases configuration and
NFS proposals remain byte-for-byte unchanged.

## 2. Policy configuration and lifecycle wiring

`require_staging_config(StorageConfig)` admits only explicit NFS/cache/staging
with `scheduler_policy=resource|locality`. Existing schema recognition of
`bandwidth` is retained for compatibility, but execution rejects it: no measured
throughput model is available. Missing/default config stays legacy/cache OFF/
staging OFF/resource. Unknown/invalid keys and unsafe flag combinations retain
the existing strict checks.

The existing `StagingDispatch` constructor now accepts optional `coverage` and
`choice_logger`; existing callers/signatures remain compatible. Coverage must be
a real `CoverageAPI` associated with the **same `StorageRegistry` instance**.
Missing coverage is allowed and uses resource fallback. Constructors create no
DB connection, migration, enrollment, transport, mount or service.

On each queued NFS job, the existing coordinator still reloads the job, checks
watchdog enablement, validates authorization and all immutable workspace plans,
and resolves its existing checkpoint source. It then passes those actual plans
and the original selector arguments through `SchedulingPolicy.select`. Resource
without an explicit diagnostic callback invokes the selector directly, with no
inventory reads, observer clock, automatic choice trace or added stdout.

Locality builds `JobLocality` from the job's authorized owner/ID and exact plan
binding. Plans must preserve the current full-input-per-rank contract; unsupported
divergent rank plans use resource fallback, not fabricated shards. The existing
single/group selectors retain 10.2 resource-equivalence rules, full compatible
groups, world size, freshness/unique-byte accounting and conservative fallback.
After selection, the original atomic quotas/reservations, leases, staged receipts,
all-rank barrier, launcher, cancellation/reconciliation and checkpoint resume
continue unchanged. `CheckpointDispatch` uses this same bridge; it keeps its
trusted publisher, collector, restorer and source/version/SDK acknowledgment gates.

Legacy-bound jobs continue through their original dispatch branch even when an
explicit NFS coordinator exists. No job is reinterpreted, lazily copied, rebound
or migrated by a policy change. No new job/student policy input is accepted.

### Ordinary startup stays protected

The general `dispatcher.main`/portal bootstrap still rejects executable cache/
locality flags: four JSON fields cannot provision protected LOCAL roots, credentials,
schema, NFS readiness, process/network isolation or containment callbacks. This
subphase exposes the flag to the **already existing explicit trusted coordinator**;
it does not fabricate or install a production bootstrap. Configure that reviewed
bootstrap with these typed settings and its authenticated services. Do not copy
samples into active `storage.json` expecting ordinary startup to bypass the gate.

Safe validation from the application root:

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python -m hybrid_storage.config --runtime staging --config config/storage-examples/nfs-resource.json
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python -m hybrid_storage.config --runtime staging --config config/storage-examples/nfs-locality.json
```

Expect `runtime=trusted-staging-component`, the sample policy and
`automatic_startup=false`. This validates settings only, without SQLite imports,
runtime construction or service start. Default CLI behavior/output remains unchanged.
The JSON samples contain only the four existing keys and no secrets.

## 3. Explainable operator choice records

Locality emits `SCHEDULER_CHOICE` JSON at INFO on Python logger
`distributed_ml.scheduler`. The trusted bootstrap explicitly configures its
existing protected logging handler; none is installed globally here. A preferably
nonblocking handler avoids selection stalls. Optional `choice_logger(document)`
routes a copied record to an existing sink and also enables resource-policy
diagnostics. With no handler routing INFO, operators can still read bounded
`runtime.scheduler.history()`; no claim is made that a logfile was provisioned.

Records describe **selection proposals**, before reservation/launch. They contain:

- job ID, selector, requested/effective policy, reason and selected worker IDs;
- hard-filter IDs/reasons/counts, eligible candidate CPU/RAM, group counts and
  explicit resource-equivalent-only scope;
- known per-worker missing/verified/required bytes, inventory eligibility/status,
  age/session/sequence/computation time and per-rank/aggregate missing bytes;
- selection duration and diagnostic availability/truncation indicators.

Resource mode does not query cache just to explain a choice; missing bytes remain
unknown. Stale/absent/error coverage produces resource policy with unknown totals
and an explicit fallback reason. Empty/failed placements are distinguished from
selected proposals. A choice is not evidence of a successful reservation, staged
workspace or running trainer. Existing single-worker post-selection ownership
capability admission remains authoritative; DDP ownership filtering remains prior
to group selection.

Default history holds 128 records (trusted `SchedulingPolicy` caller can bound it
1–4096). Records cap details at 256 nodes and 128 KiB; truncation is explicit.
Invalid/oversized records are dropped rather than exposing unsafe IDs or changing
selection. `recorded`/`dropped` expose retention loss. History returns fresh JSON
copies. Existing `SchedulerMetrics` documents omit the new detail fields unless
`trace_nodes=True`; resource defaults retain prior values/shape. Clock/serialization/
sink failures cannot replace choices; missing diagnostic traces are marked as
unavailable. Original selector exceptions propagate without a false success event.

No filenames, missing-hash lists, raw NFS paths, IPs or credentials are included.
These are trusted operator records, not a student route. The history is volatile
and bounded, not a durable audit log. Selector timing includes resource/coverage
queries but excludes record formatting/sink time; it is not end-to-end job delay,
staging time or NFS bandwidth.

## 4. Simulation and regression evidence

Existing environment: Python 3.12.3, torch 2.14.1, Flask 3.1.3, requests 2.34.2,
numpy 2.5.3 in `.controller-venv`. No package installation or runtime download.

**Policy matrix:** seed 104, 72 cases, repeated twice, **432 selector calls**.
Single/two/three-rank placements cover fresh/stale/absent/error metadata and equal
resources, CPU/RAM ordering, insufficient capacity, reservations and incompatible
groups. Selected resource profiles and feasibility match the original baseline;
unknown metadata returns that exact baseline. Resource rollback matches exactly
and repeated traces agree. Where facts are current, the immediate chosen eligible
set has no more missing bytes than the baseline set for that same decision.

**Greedy workload regression:** two equivalent workers, two ordered jobs. First
job misses `(2,0)` bytes on `(pi01,pi02)`; second misses `(100,0)`. Resource takes
pi01 then pi02 for total **2**; greedy locality takes pi02 then pi01 for **100**.
Switching the same policy object to resource restores **2**, with the same job
order. This synthetic cost model proves no universal aggregate-byte improvement.
No measured throughput, Pi timings or speedup is claimed. Keep resource when
representative observed job delay/staging bytes regress; do not silently add a
new global optimizer, bandwidth weights or fairness override.

Existing 10.3 continuous-warm-arrival/fairness simulations and exact actual
0/2/502 MB cache fixture passed again, including 2 MB/504 MB DDP totals and zero
skips. New actual local HTTP/SQLite/cache/workspace tests exercise configured
single dispatch and a warm pi03 becoming logical rank zero in a three-rank job,
while all three STAGING_READY replies precede any start. Checkpoint dispatch with
locality configured but unavailable inventory follows the existing committed
restore/SDK acknowledgment path. Mount facts, resource/capability, isolation,
trainer process and some workload inventory facts are fixtures; authenticated
inventories, actual storage registration, private copies and lifecycle DB/control
APIs run real code. No physical cluster execution is claimed.

### Exact commands and latest outcomes

Baseline before edits:

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_scheduler_locality.py test_scheduler_metrics.py test_queue_controls.py test_resource_allocation.py test_storage_config.py test_staging_dispatch.py
```

**81 PASS, six files**; `/tmp/hybrid-10-4/baseline-tests.log`.

Main regression run:

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_scheduler_policy.py test_scheduler_metrics.py test_scheduler_locality.py test_scheduler_locality_acceptance.py test_cache_coverage.py test_queue_controls.py test_resource_allocation.py test_storage_config.py test_staging_dispatch.py test_ddp_staging.py
```

**149 PASS, ten files** at that source boundary (new policy suite then had 16
tests); `/tmp/hybrid-10-4/final-tests.log`.

Recovery/native regression and justified reruns:

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_hybrid_checkpoint_recovery.py test_hybrid_checkpoint_faults.py test_distributed_dispatch.py
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_hybrid_checkpoint_recovery.py test_scheduler_policy.py
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_scheduler_policy.py
```

The first ran concurrently with the main run: checkpoint faults **16 PASS** and
native CPU DDP **7 PASS**, but the recovery file had **one ERROR out of 18** (HTTP
409 during three-rank cold staging). The same case then passed individually on
both saved pre-edit and current code (one test each), and the full recovery file
passed **18/18** when rerun serially. No runtime/timeout/lease check was changed
to clear it. Original failure cause was **not established**; do not label it a
proven pre-existing defect or conceal it as an all-green initial run. It remains
a concurrency/test reliability limitation needing care under loaded runners.

Serial second command: **18 + 17 PASS**. Final third command, after diagnostic/
rollback-consistency refinements: **17 PASS**. Latest-per-file aggregate is
**191 distinct tests PASS, 13 files, zero remaining failures/errors/skips**;
repeated tests are counted once. Logs: `recovery-ddp-tests.log`,
`recovery-policy-final.log`, `policy-final.log` under `/tmp/hybrid-10-4`.
The native CPU DDP run used sandbox escalation for localhost rendezvous only;
no Pi or live service was accessed. The pre-edit/current one-case reproduction
program and logs are `probe-recovery.py`, `recovery-probe-before.log` and
`recovery-probe-current.log` in the same directory. All test DBs/files were disposable.

New-test development history also records an initial incorrect path expectation
and an unmocked DDP inventory alias that directed fixture HTTP to the wrong local
test client, causing one assertion failure and one authentication error. Corrected
the fixtures to use the existing protocol constants and the copied test inventory;
no protocol/auth rule was changed. Initial log `policy-initial.log`; corrected
15-test run `policy-second.log`, subsequently extended to seventeen tests.

## 5. Immediate fallback, manual verification and source rollback

In an **already running reviewed trusted coordinator**, on its controller thread:

```python
runtime.set_scheduler_policy('resource')
```

This updates the next selection and the lifecycle's typed settings snapshot;
it does not cancel/migrate running jobs or mutate their bindings, manifests,
checkpoints, cache pins, reservations, lease IDs or DB rows. Tests compare DB dumps
before/after the switch. With no explicit logger, it stops inventory reads and
automatic tracing immediately. Persist the resource sample for the next reviewed
bootstrap; the in-memory switch does not rewrite config. No remote policy endpoint
or automatic metric-triggered switch was introduced. For an explicitly installed
diagnostic callback, setting `runtime.scheduler.choice_logger = None` also restores
the unobserved baseline. Do not start another controller to execute these snippets.

Safe manual repeat from the application root:

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_scheduler_policy.py test_queue_controls.py test_storage_config.py
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python -m hybrid_storage.config
git diff --check
git status --short
```

Expect **44 PASS**, active config still reporting legacy/cache false/staging false/
resource, no whitespace errors, and only prior work plus the listed 10.4 files.
The policy tests use small inputs; the separate exact-MB acceptance rerun requires
the disk space documented in 10.3. Repeat heavy native/large-file suites serially
when investigating the recorded concurrent rejection.

From the parent Git root, review `/tmp/hybrid-10-4/10.4.patch` and retain later edits:

```bash
git apply --reverse --check /tmp/hybrid-10-4/10.4.patch
git apply --reverse /tmp/hybrid-10-4/10.4.patch
```

Only the reverse check was executed. Source reversion restores just this turn,
including previous untracked 10.1 metrics contents. No schema downgrade, DB restore,
cache/input/checkpoint deletion, service or mount operation is needed. Disable
explicit locality use before reverting its API; never force over later changes.
`/tmp` evidence/patch is temporary; retain reviewed copies for longer retention.

## 6. Security, compatibility and acceptance

No schema/migration, job/FSM, file format, credential, student route, upload or
checkpoint transport changes. Controller SQLite remains LOCAL; read-only restricted
NFS inputs/root_squash, protected cache/private workspace, trusted authenticated
checkpoint publication and staging authorization remain required. Training gains
no NFS access or new network privileges. Resource eligibility, fair queue, quotas,
atomic complete-group reservations, cancellation and lease fencing remain hard
constraints. No live config/data/services/SSH/firewall/exports/fstab changes,
deployment, hardware faults, Internet-dependent transfer or Git commit occurred.

| Criterion | Result |
|---|---|
| Existing resource/OFF choices, stdout and APIs retain previous behavior | PASS — frozen cases/config/queue regressions |
| Resource/locality flag reaches existing trusted single/DDP/checkpoint coordinator | PASS locally |
| Filtered nodes, bytes, freshness, policy/reason exposed safely and bounded | PASS |
| Inventory/logger failure does not bypass hard rules or disable resource fallback | PASS |
| Deterministic policy matrix, queue regression and rapid resource restoration | PASS |
| Universal workload byte/time improvement | NOT GUARANTEED — explicit greedy counterexample |
| Latest selected local regressions | PASS — 191 distinct tests; initial concurrent rejection documented |
| Live locality enable / all-three-Pi acceptance | NOT RUN / not enabled; still required |

Residual risks include advisory inventory changing after selection, unbenchmarked
inventory/logging overhead, exact telemetry ties being uncommon, dropped volatile
records, the unresolved loaded-run staging rejection, and absent production
bootstrap/isolation/hardware readiness. Stage-time hashes, disk quota and fresh
leases remain necessary. Controller/LOCAL SQLite/authoritative storage remains
a **single point of failure**. **Phase 10 local implementation/validation PASS;
production/physical cluster acceptance is not claimed. STOP at 10.4.**
