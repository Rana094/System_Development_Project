# Hybrid storage evaluation methodology

Phase **13.1**: reproducible local runner and raw-result foundation. Status of
13.2 controlled policy comparisons,13.3 fault injection and13.4 full measurement/
charts: **not implemented or run by this subphase**. No comparative performance
claim is made here. This document will be extended one approved subphase at a time.

## What runs now

`tools/benchmarks/storage_evaluation.py` plans by default. Explicit `--run`
executes only trusted, unchanged repository trainers on the local CPU:

| Workload | Exact existing trainer | Input and execution |
|---|---|---|
| Regression | `examples/checkpoint_project/train.py` | `x,y` CSV; deterministic seeded MLP, Adam, scheduler, local SDK checkpoint;2 steps by default. |
| CNN | `examples/classification_project/train.py --task cnn` |64-pixel/label CSV, existing1×8×8 CNN;2 steps by default. No torchvision/data download. |
| DDP | `examples/ddp_project/train.py` | `x1,x2,label` CSV, existing DistributedSampler/Gloo/DDP;1 epoch by default,2 or3 actual local processes. |

These are existing training applications, not timing stubs or independent fake
rank jobs. Each DDP rank reads the complete generated dataset and the trainer's
DistributedSampler selects its logical samples. Existing trainer verifies exact
parameter synchronization across ranks. The runner also requires every rank's
summary/runtime, positive training-step count, correct rank/world size and equal
valid final-model SHA-256 before reporting COMPLETED. DDP global batch size is
fixed at96, divisible by both2 and3. Automatic restart and recovery are disabled
for this local DDP smoke mode; single-worker trainers use their unchanged local
SDK save format. These archives are **not controller COMMITTED checkpoints**.

No controller bootstrap imports, SQLite opens, dispatcher, website jobs, agent
HTTP/SSH, worker deployment, package install/download, NFS mounting, speed tests,
cache drops or fault injection. Source is copied into a new private report root,
not moved. No existing dataset/job/checkpoint/output is overwritten. A repeated
experiment ID fails even after a failed/partial run.

The local wrapper explicitly removes inherited production lease/resume/worker
credentials and DDP environment, hides GPUs, uses1 PyTorch/OMP/MKL thread per
rank and supplies only local synthetic input/output/checkpoint paths. This is a
trusted benchmark, not a sandbox or test of production lease/security authority.
Rendezvous endpoint is127.0.0.1:0 with Gloo on loopback; PyTorch chooses an unused
port. Launcher argv and actual rank rendezvous settings are retained. Timeout
terminates the runner-owned process group, not any service or student process.
No environment dump is saved.

## Dataset recipe and versioning

`tools/benchmarks/evaluation/datasets.py`, generator **sha256-counter-csv-v1**:
SHA-256 of the ASCII string `sha256-counter-csv-v1:<seed>:<zero-based-row>` supplies
fixed integer features, formatted with three decimal places and LF newlines.
Regression uses bounded x and y=2x+1; DDP labels the sign of x1+x2; CNN has64
bounded deterministic pixel features and labels cycling0,1,2. The code and recipe
are included in the hashed source snapshot. No global RNG or Internet dataset.

Defaults: seed123, row counts64 and1024 for each task. `--rows` accepts1–8 unique
sizes, each1–1,000,000 rows. Exact **bytes are measured**, not inferred from rows
or advertised as MiB. Each dataset has evaluation namespace ID
`eval-<task>-s<seed>-r<rows>`, recipe version1, SHA-256, actual byte size, kind,
rows and seed. This version is **not** a controller dataset-registry allocation.
The digest pins the bytes. Changed seed/size creates another ID/digest; recipe
changes require a generator-version change. Generation streams rows with bounded
memory, enforces `--max-dataset-bytes` (default64MiB per file; explicit bound≤1GiB),
fsyncs and publishes without replacing a target. Failed partial files are not
published. Read-only synthetic files are verified before/after training.

Creating and hashing these files warms Linux pages. The page-cache state is
**uncontrolled**, not cold; the runner never writes drop_caches. Application
cache/staging are **not used**, which is different from a cold application-cache
trial. Observed NFS kernel mounts are metadata only, not I/O availability tests.
This is not configurationA, B, C, D or E. No claim that NFS has no cache.

## Reproduce

From the application root, with the existing offline CPU-capable torch/numpy
Python environment. Default plan is read-only and does not create reports:

```bash
PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/benchmarks/storage_evaluation.py --experiment-id review-plan
```

Create a **new** small local smoke experiment, never reuse an earlier ID:

```bash
PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/benchmarks/storage_evaluation.py --run --experiment-id eval-local-smoke-001 --rows 12 24 --workloads regression cnn ddp --world-size 3
```

Expected: six completed trials, actual CPU model training, one private new
`reports/eval-local-smoke-001/`; two actual Gloo groups of3 local ranks.
This does not represent three physical Pis. Local DDP needs socket permission;
restricted environments should grant only this local command rather than
silently substituting mocks or relabeling a skipped DDP trial successful.

Two-rank example with a fresh ID:

```bash
PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/benchmarks/storage_evaluation.py --run --experiment-id eval-local-two-001 --rows 12 24 --workloads ddp --world-size 2
```

`--steps`, `--epochs`, `--seed`, `--timeout-seconds`, `--max-dataset-bytes` and
`--output-root` are explicit bounds recorded in metadata. Larger inputs require
a deliberate resource review: these existing trainers parse the full CSV into
memory. The generator's bounded memory does not make the trainer streaming.
Use a LOCAL ext4/xfs/btrfs report root; symlink components, traversal, unknown/
remote/ephemeral filesystems and existing experiment IDs are refused. Missing
report ancestors are created without changing permissions on existing directories;
new experiment roots are0700 and result files0600. No rm/cleanup option.

## Record layout, identities and units

```text
reports/<experiment-id>/
  experiment.json               initial metadata/status RUNNING
  completion.json               final status, counts, finish time (when available)
  trials.csv                    one measured record per attempted trial
  trials.jsonl                  same measurements plus full per-rank raw JSON
  source/                       byte-identical frozen runner/trainers/requirements/SDK
  datasets/<task>-<rows>.csv     generated immutable input
  datasets/<task>-<rows>.csv.json
  trial-0001/
    invocation.json             exact argv, trial identity, workload identity
    launcher.log                process/torchrun output
    result.json                 measured result, including failure/timeout
    ranks/rank-0/
      runtime.json              rank/world/host/version/status/rendezvous data
      stdout.log, stderr.log
      output/summary.json, model.pt
      checkpoints/              unchanged SDK archive for single-worker workloads
```

CSV references the experiment metadata through `experiment_id`; join there for
worker/config/NFS/cache observations. `schema_version=1` identifies the record
contract. All `*_ns` timestamps are integer UTC epoch nanoseconds; elapsed/wrapper
durations use monotonic nanoseconds. `dataset_bytes` is bytes. `training_loop_seconds`
is the **maximum measured rank loop duration** from existing trainer summaries;
per-rank values and the trainer's timing-scope description remain raw. Do not sum
parallel ranks or equate the loop with process launch-to-exit. No derived speedup,
throughput, completion-time prediction or confidence interval is emitted.

`elapsed_ns` spans input recheck, local child launch/exit, raw JSON collection and
post-run input verification. It includes interpreter/torch startup, excludes
source snapshot/dataset generation, and is not website queue latency or staging.
Per-rank wrapper time includes application execution and imports. Raw loop timing
in regression/CNN includes checkpoint saves; DDP restart mode has no SDK save.

`workload_sha256` hashes canonical task/argv/world size/thread settings and source
hashes, including generator/runner/wrapper, unchanged trainer/requirements and SDK.
It excludes experiment ID, absolute report paths and observation times.
`input_binding_sha256` additionally pins the exact evaluation dataset manifest.
Python/package versions, platform/machine/host, affinity count, available physical
memory, Git HEAD/dirty status and exact source hashes are stored. Runtime rank
JSON also retains actual torch/numpy versions; package-distribution metadata can
omit a torch local build suffix while runtime `torch.__version__` includes it.
A source edit during snapshot aborts; this never edits the repository source.

Configured pi01/pi02/pi03 addresses are copied from actual `config/nodes.json`.
Their **observed Pi host and scheduler eligibility are null**, with a not-probed
reason. Local host is measured separately; local ranks are never renamed Pis.
`scheduler_mode=not_exercised` and `comparison_config=null` prevent configuration
intent being mislabeled observed scheduling. Existing `config/storage.json` is
captured, not changed. NFS mount metadata (including options/source/version when
present) comes from `/proc/self/mountinfo` in the local namespace only. Empty
mount list means none observed locally, not that every Pi lacks NFS.

Missing NFS wire bytes, cache hit ratio, first-step latency, staging duration,
checkpoint save/restore timing, recovery cost, server bandwidth, worker eligibility
and scheduler choice remain explicitly listed as **missing**, never invented zero.
Raw files may support later instrumentation, but parsing a log line is not used
here to fabricate timestamps for events that were never recorded.

Failed launches, nonzero exit, timeout, missing/corrupt/inconsistent rank results
remain FAILED/TIMEOUT with raw logs and null unknown timings. CLI exits nonzero.
`completion.json` is final when present; initial `experiment.json` intentionally
stays RUNNING. A crash with no completion is incomplete, not a successful trial.
CSV/JSONL flush/fsync after every recorded attempt. If storage itself is full or
the process is killed, retain partial evidence and treat it incomplete; no claim
of crash-proof logging. Generated directories are Git-ignored but retained on disk.
Archive/share reviewed raw evidence explicitly; no deletion/deployment is implied.

## Current validation and follow-on boundaries

13.1 tests use disposable LOCAL directories: deterministic bytes/hash/size,
changed seed/size, no replacement, interrupted/oversize generation, safe source
snapshot and report paths, no inherited production environment, timeout cleanup,
failed/null result retention, invalid rank/hash checks, JSON numeric validation,
read-only planning and actual regression/CNN training. Exact commands/results,
source hashes and raw measured runs are in `53-evaluation-runner-evidence.json`.
No production jobs/DB/accounts/services/mounts/Pis were changed.

13.2 must add the controlled A/B/C/D matrix, repeat/randomization policy and
page-cache accounting without treating this local-smoke mode as a baseline.
Direct-NFS training needs a reviewed authorized disposable input view and network
boundary; never expose the broad shared input mount to arbitrary student code.
13.3 requires separately approved isolated fault plans and safe return paths.
13.4 must add measured missing metrics and generate charts only from retained
records. Prior pi01 deferral is retained; any physical validation must state its
approved worker scope and cannot claim all-three-Pi success from local ranks.
The central controller/SQLite/authoritative storage remain a single point of failure.

## 13.1 implementation and actual acceptance

Implemented files: `tools/benchmarks/storage_evaluation.py`;
`tools/benchmarks/evaluation/{__init__,datasets,runner,rank_launcher}.py`;
`tests/test_storage_evaluation.py`; `reports/{README.md,.gitignore}`;
this methodology and `docs/storage/53-evaluation-runner-evidence.json`.
Append-only updates: `tests/README.md`, `tools/README.md` and
`docs/storage/implementation-progress.md`. No production code, workload source,
API, DB schema/migration, authorization, storage flag or active policy changed.
The working tree began clean on main at
`f3b371beff5c5f0414bf2acc3513a063fc118d04`; no stash/reset/commit performed.

Before edits:36/36 tests PASS in config/events suites. First new suite:16/16 PASS.
Final new suite:18/18 PASS; together with config17 and events19, **54/54 PASS**
in3 files, zero failures/errors/skips. Exact safe command:

```bash
PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_storage_evaluation.py test_storage_config.py test_storage_events.py
```

Actual retained, measured smoke experiments on this laptop (x86_64, CPU-only):

| Experiment directory under reports/ | Result | Actual groups |
|---|---|---|
| `eval-13-1-local-three-rank` |6/6 completed | regression/CNN each at12 and24 rows, DDP at12 and24 rows with3 local ranks |
| `eval-13-1-local-two-rank` |2/2 completed | DDP at12 and24 rows with2 local ranks |

These were serial experiments, one trial per listed task/size/world-size case;
**not replicated comparative samples**. Dataset byte sizes were regression165/318,
CNN5,140/9,772, DDP198/376 for12/24 rows. Each DDP group's actual final-model
hashes agree. Source and dataset bytes were independently rehashed, and every
CSV field checked against its JSONL record. All resulting CSV/JSON, exact per-rank
summaries/logs and source snapshots remain in the named directories. Raw timings
are evidence of these small executions, not a speedup/variability estimate.
No charts, confidence intervals or fabricated NFS counters.

Both exact retained-run commands, local socket approval scope, hashes, raw logs,
all actual measured row values and units are recorded in the evidence JSON.
Temporary earlier regression/CNN rehearsal was4/4 completed at
`/tmp/hybrid-13-1/runs/initial-local` using an earlier runner snapshot; it is not
merged with the final eight retained cases or relabeled the final source version.
No test failure was hidden or live hardware run claimed.

**13.1 PASS for local runner reproducibility, raw records and honest scope.**
The whole Phase13 evaluation is **not complete**. Worker eligibility and physical
Pi/NFS policy measurements remain unmeasured;13.2–13.4 are pending. No active
student workload/service/storage/account was changed. **STOP before13.2.**

## Rollback

The13.1-only source diff is `/tmp/hybrid-13-1/13.1.patch`, based on the clean
pre-turn snapshot, with source preservation/rollback checks in
`/tmp/hybrid-13-1/source-verification.json`. From the application root:

```bash
git apply --reverse --check /tmp/hybrid-13-1/13.1.patch
```

Review before applying `git apply --reverse /tmp/hybrid-13-1/13.1.patch`.
This reverts source/docs only; retained experiment directories are outside the
patch and **are not deleted**. Reverting reports/.gitignore makes those directories
visible as untracked artifacts; preserve them. No DB/data/mount/service rollback
is required, and no automatic cleanup of runs is provided. Later source changes
may cause reverse-check conflicts; do not overwrite them with reset/restore.

## Phase 13.2 — controlled A/B/C/D protocol

The13.1 sections above are historical local-smoke scope. The new explicit
`--matrix` path adds the four controlled configurations without changing the
old smoke command or production flags:

| Configuration | Application input path | Persistent reuse | Selection |
|---|---|---|---|
| A | Known trusted trainer and dataset read directly from input CAS | Not applicable | Resource |
| B | Verified fetch plus private local workspace, fresh cache directory every attempt | None | Resource |
| C | Verified fetch plus private local workspace | Same protected cache across first/repeat | Resource |
| D | Same as C | Same as C | Resource feasibility/ties plus existing locality component |

B uses the real cache stager as a disposable one-shot implementation, retaining
old directories rather than deleting files. It cannot reuse the previous trial's
cache. C/D first attempts begin with empty caches; the immediately following
repeat uses the same pool. A's repeat is **not called a warm application cache**;
B remains `fresh_no_reuse` in both phases. Optional bandwidth E is not implemented.
These are evaluation configurations, not invalid new combinations of production
storage flags; production still prohibits general direct-NFS student execution.

New code: `tools/benchmarks/evaluation/matrix.py`, `matrix_context.py`, and
`tests/test_storage_matrix.py`. The existing CLI gains explicit matrix/preparation
options; the rank wrapper gains a private per-rank input map so DDP actually reads
each selected slot's own workspace. Existing example trainers are unchanged.

The context uses actual `StorageRegistry`, non-destructive migrations in a NEW
private LOCAL SQLite file, `CacheInventory`, `InventoryReporter` request-bound
HMAC, `CoverageAPI`, `CacheFetcher`, `WorkerCache`, and `WorkspaceManager`.
The database contains only evaluation-owned synthetic input records. Secrets
stay in memory; no production credential or DB is imported. New worker IDs are
`slot-00..02`, never Pi names. Successful terminal paths unpin through existing
workspace cleanup; uncertain failed execution retains pins rather than inventing
quiescence. Cache/source/workspace object bytes are actually hashed and verified.
No writable hardlinks to the persistent cache reach training.

Single-worker selection reuses the actual resource selector and `JobLocality`
on a **declared fixed equal resource profile**, with real signed cache inventory.
DDP uses the existing group locality tie-breaker over a precompatible fixed group,
not the live group dispatcher/reservation/lease path. These logical slots share
one physical host; none is live Pi resource telemetry. This controls the component
comparison but **does not measure distributed controller placement, fairness,
inter-Pi DDP or production lease behavior**. Existing regression tests separately
cover resource hard filters and locality safety. Full cluster evaluation remains
pending; do not turn component results into a claimed deployment advantage.

### Randomization, controls and page cache

`--repeats` accepts2–30 independent blocks; default3. For each workload/size,
each block includes all four configurations and both first/repeat phases.
`--order-seed` (default1729) determines recorded randomized case/block order and
configuration-pair order. First must precede repeat inside a pair; warm-before-cold
is not randomized because it violates the cache-state dependency. Save the full
trial order before execution. Reusing a seed reproduces order, not measured time.

Each configuration has an isolated new pool with identical initial state,
unchanged source/requirements/SDK and fixed world size, training arguments,
generated dataset digest/version/ID, interpreter/packages and hardware within a
run. C/D share only their own first/repeat pool, never another policy's cache.
The source/byte identities are fixed independently of random registry IDs in each
private clone. Candidate resources/compatibility profile is the same in every
cell. Context initialization, source registration, signed inventory refresh and
input integrity prechecks occur outside timed trials. Logical worker selection,
staging, actual child execution/result collection and successful terminal cleanup
are included in `total_ns`; `selection_ns`, `staging_ns`, `process_ns` remain
separate. Bootstrap metadata retains the source snapshot and exact argv.

Linux page cache and NFS client/server caching are **uncontrolled**, not absent.
The same immutable input objects are preverified before each trial, which likely
warms kernel/NFS pages even when application cache is cold. No drop_caches,
remount or disruptive cache eviction is attempted. These trials assess application
reuse **under that declared condition**, not cold-disk/cold-server I/O. Record
actual NFS mount options in native mode; mount identity does not prove current I/O
availability. Accepted cache payload bytes are not NFS wire traffic/retransmits.
A's cache-hit/transfer counters stay null because no application stager measures
them. Its staging time is zero because that operation is deliberately absent.

### Results and uncertainty

Matrix records use schema2 and retain raw per-trial/per-rank JSON plus CSV,
config, repeat block/order, actual byte counters and timings. `summary.json`
reports attempted/completed/failed counts, raw complete samples, mean, median,
minimum, maximum and sample standard deviation for each workload/size/world/
config/phase. With n>=3, a fixed-seed1,000-resample percentile bootstrap gives a
**descriptive within-run interval for the mean**. With n<3, interval is null;
with n<2, standard deviation is null; no completed samples means null mean.
Failures are retained and prevent an all-trials-complete claim. The summaries
condition timings on completed trials and never hide their failure denominator.
Different scopes, workload hashes or dataset identities cannot be pooled.

Three small repetitions are not enough for a general cluster-performance claim.
Cache phases, common-host interference and pair order are dependencies; these
intervals are not independent physical-cluster uncertainty or causal speedup.
No charts, speedups or claimed total-completion predictions are generated here.
13.4 must add missing wire/first-step/checkpoint/recovery/server metrics and a
larger controlled physical evaluation before reporting broader conclusions.

### Explicit local rehearsal

```bash
PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/benchmarks/storage_evaluation.py --matrix --source-mode local-rehearsal --experiment-id review-matrix --rows 12
```

This prints a read-only24-pairs/72-trials plan for all3 workloads at3 blocks.
For a new actual local rehearsal, add `--run` and use a fresh ID. Local rehearsal
uses real source files, trainer processes, CAS/cache/workspace bytes, SQLite and
signed inventory, but its copy subprocess explicitly substitutes **kernel NFS
mount facts** around the existing trusted copy helper. Therefore every record and
summary is labeled **`local_component_rehearsal`**. This is a Pi-less functional
validation, never an NFS/Pi result. Native mode cannot silently use that helper.

### Native NFS gate and preparation for review

The `nfs` source mode requires an explicit actual root/source, exact existing
read-only NFS mount identity and `--trusted-direct-inputs` for baseline A. Only
hashed known repository trainers/synthetic manifests are allowed; no arbitrary
student project, submitted path or HTTP route can turn on direct NFS. Native
scope is **`single_host_nfs_component_comparison`**, still not live distributed
scheduler placement. Source and requirements are identical across all policies;
all DDP ranks need their actual full dataset. A genuinely missing/mis-mounted
input share is refused **before creating an experiment**, with no local fallback.

Read-only native plan using the existing configured controller address:

```bash
PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/benchmarks/storage_evaluation.py --matrix --experiment-id native-review --input-root /srv/mlnfs/inputs --nfs-source 192.168.10.214:/srv/ml-shared/objects --rows 12
```

The laptop returned `not_ready`; an actual native attempt refused with
`MountNotReady: Exact input NFS mount absent or shadowed...`, creating no report.
This is an infrastructure admission refusal, **not a measured failed NFS trial**.
No NFS timing is fabricated from it.

`--prepare-inputs` creates only a NEW local public-input review bundle:

```bash
PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/benchmarks/storage_evaluation.py --matrix --source-mode local-rehearsal --experiment-id public-input-review --rows 12 --prepare-inputs
```

Retained `reports/eval-13-2-public-inputs/input-bundle.json` lists exactly8 immutable
source/requirements/synthetic objects and their SHA/size/key; its objects and
frozen source exist locally. Status is **PREPARED_NOT_PUBLISHED**. No live export,
controller authoritative CAS or worker was written by preparation.

Read-only SSH readiness checks on pi02/pi03 observed both IDLE, existing NFS4.1
RO input mounts from192.168.10.214, Python3.11.2 and the existing `.venv` with
Torch2.14.1/numpy2.4.6. Agent PIDs946/902 were merely read. The unrelated legacy RW
`/mnt/nfs_share` was also observed; it is not used or changed by these modes.
These are point-in-time metadata/telemetry, not I/O/eligibility or evaluation
results. pi01 remains deferred by the user's prior instruction.

Further native trials require reviewed public-input publication under the
controller's current storage-service identity (the authoritative directory is
owned by nobody:nogroup), a new private source/result tree on each approved Pi,
and explicit approval for that deployment. Never change existing CAS permissions,
exports, fstab, firewall, agents or storage flags to make the benchmark pass.
A controller/worker hard NFS outage can leave a kernel-blocked reader; do not
claim userspace timeout is a hard kernel I/O deadline or blindly release pins.
The live-cluster executor and actual physical A/B/C/D placement comparison are
still absent; single-host component tests must not be relabeled cluster results.

### 13.2 measured status and approved preparation — 10 November 2026

**Local protocol/QA: PASS. Native A/B/C/D comparisons: PENDING an explicitly
confirmed idle window; 13.2 is not fully accepted. STOP before13.3.**

The user approved staging/publication on pi02/pi03 only, with pi01 deferred and
training explicitly waiting for an idle window. The reviewed immutable plan is
`reports/eval-13-2-live-review/plan.json` (SHA256
`72dff687bdc9ea9ce76d8bb339703afaa91f9698800f330445c7b8cd6c677082`).
Its historical REVIEW_ONLY status is retained; subsequent receipts record actual
effects. Both Pis now have69 verified read-only source files under
`/var/tmp/dl-eval13-76f05cf7d7dc/source`; no agent installation/restart occurred.
The user published exactly8 approved TEST objects with the existing atomic
no-replace ArtifactRegistrar. Independent controller checks verified SHA/size,
read-only file mode and service ownership; privileged **read-only** SSH checks on
both Pis verified all8 through the existing NFS4.1 RO mounts. Agent PIDs946/902
remained active/unchanged. No Pi training, failure injection or timing trial ran.

An initial publisher helper incorrectly inferred nobody:nogroup from sandbox
overflow IDs65534. The actual host identity, verified outside the sandbox and
against `config/nfs-server.json`, is **ml-publisher125:125**. The failed assertion
preceded writes. The corrected helper rejects remapped user namespaces, validates
the configured identity and root-owned setup marker, and drops only its own
process to125:125. Only its NEW private scratch directory was chowned; no existing
CAS/export/permission/account was changed. The reviewed plan/archive remain
unchanged. Keep the erroneous identity observation as historical evidence, not
an instruction to change infrastructure ownership.

The Pis' `/srv/mlnfs` parent is intentionally root-owned0700. Ordinary SSH user
`pi` cannot read the share; the read verifier used existing sudo privileges for
read-only access. Do not chmod it to make a benchmark pass. Future native trials
must explicitly review the trusted execution identity and production admission
guard in the idle-window plan. No arbitrary student code gains direct NFS access.

Retained verification and measured records:

| Evidence | Actual result |
|---|---|
| `reports/eval-13-2-validation/final-tests.log` |92 tests PASS in5 isolated files; no errors/failures/skips |
| `reports/eval-13-2-controlled-local/` |72/72 completed real CPU component trials:18 each A/B/C/D,3 workloads,3 blocks, first/repeat |
| `reports/eval-13-2-controlled-local/summary.json` |24 groups,3 completed samples each; descriptive variability only |
| `reports/eval-13-2-validation/artifact-verification.json` |CSV/JSON equality,67 frozen/live source hashes,24 actual3-rank Gloo groups synchronized, warm C/D zero new payload, all144 retained13.1 raw files unchanged |
| `reports/eval-13-2-live-review/pi02-staging.json`, `pi03-staging.json` |69 staged files per Pi; exact archive hash and unchanged agent PID |
| `reports/eval-13-2-live-review/pi02-privileged-read-verification.json`, `pi03-privileged-read-verification.json` |69 source hashes and8 NFS object hashes verified per Pi; NFS4.1 identity |
| `reports/eval-13-2-validation/publication-verified.json` |All8 controller objects match SHA/size/read-only mode/host owner125:125 |

Exact final local test command:

```bash
PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_storage_matrix.py test_storage_evaluation.py test_scheduler_locality.py test_cache_fetch.py test_storage_config.py
```

Counts: matrix11, original evaluation18, scheduler locality13, cache fetch33,
storage configuration17. No pre-existing failure was observed in the selected
baseline. A new test's incorrect expected list/dict comparison was fixed during
development. A new manifest hashing mismatch was also fixed before the final
run. Neither is presented as a pre-existing repository failure.

Exact retained real local matrix command (already executed; do not reuse its ID):

```bash
PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/benchmarks/storage_evaluation.py --matrix --source-mode local-rehearsal --experiment-id eval-13-2-controlled-local --rows 12 --repeats 3 --workloads regression cnn ddp --world-size 3 --run
```

These72 trials use a single small dataset size per workload, uncontrolled warm
kernel pages, fixed logical resource profiles and one laptop. Earlier13.1 records
exercise two sizes; they are separate cohorts, not additional matrix repetitions.
No physical Pi placement, inter-Pi DDP, large-data performance, fairness under live
load, wire-byte count, checkpoint/recovery or speedup result follows from this.

Manual non-training verification from the application directory:

```bash
python3 -B reports/eval-13-2-validation/verify_artifacts.py
python3 -B reports/eval-13-2-live-review/verify_readonly.py --privileged-read
```

Expect PASS for72 local records and unchanged prior raw files; remote receipts
should show69 source files and8 VERIFIED objects on each Pi. The second command
uses SSH/read-only sudo and needs network permission. It starts no trainer.
`publication-verified.json` numeric ownership checks must be run in the host
identity namespace, not through a remapping sandbox. Do not rerun staging against
its existing directory or the one-shot publisher against its existing scratch.

### 13.2 compatibility, source diff and rollback

No production API, route, job/checkpoint format, migration or configuration changed.
New registry/cache schemas exist only in NEW private experiment-local SQLite DBs.
Production defaults remain legacy/OFF/resource. Existing trainers are unchanged;
the evaluation rank wrapper accepts an experiment-private per-rank script/data
mapping. The original13.1 CLI path remains available and covered by18 tests.

Relative to the preserved13.1 work, the source changes are the evaluation CLI and
rank wrapper, new matrix/context helpers and11-test suite, plus methodology,
tools/test guides, progress and54 evidence. The exact scoped diff is
`reports/eval-13-2-validation/13.2.patch`; machine details are in
`docs/storage/54-policy-matrix-evidence.json`. Earlier transient `/tmp` logs were
lost across turns. Final checks were repeated into reports; the five edited
originals were reconstructed byte-exact using retained13.1 source snapshots and
document-prefix hashes. Other baseline files came from the preserved worktree.
This reconstruction is disclosed; no unavailable log is claimed retained.

First preview rollback from the application directory:

```bash
git -C .. apply --reverse --check distributed-dl-system/reports/eval-13-2-validation/13.2.patch
```

Only after reviewing later edits, `git -C .. apply --reverse
distributed-dl-system/reports/eval-13-2-validation/13.2.patch` reverses13.2 source
changes while preserving13.1. Reverse application was exercised only in a
disposable copy. Preserve all reports, caches and authoritative TEST objects;
source rollback requires no live service/configuration action and deletes no
runtime data. Leave staged Pi bundles inert. Any later cleanup must explicitly
check references and receive its own reviewed scope; no rm-rf command is supplied.
Central controller/NFS remains a single point of failure.

## Phase 13.3 — reproducible disposable failure campaign

`tools/benchmarks/storage_fault_evaluation.py` and
`tools/benchmarks/evaluation/faults.py` / `fault_worker.py` add a separate, explicit
local campaign. They preserve the13.1/13.2 entry point, source hashes, staged Pi
bundle and every prior experiment. **No13.2 Pi idle window was confirmed.**
The13.3 request authorizes this local work; it does not override the explicit
hold on live Pi training or approve shared-service disruptions.

### Cases and truthful scope

| Case ID | Actual operation and acceptance |
|---|---|
| `agent_process_down` | NEW private copy of the actual Flask agent; loopback ephemeral port exposes only GET /health through a restrictive WSGI wrapper. SIGKILL only its owned child PID; observe failed probe; start replacement and require healthy identity. No job runs on this agent. |
| `interrupted_staging` | Real production cache/copy helper reads3 bytes, then injected ESTALE. Mock only kernel NFS identity. Retry admission verifies no final object exists; second copy must hash/size verify and leave no .part accepted. |
| `cache_corruption` | Fill NEW unpinned local replica, corrupt it, force verification, refetch and preserve corrupt bytes in quarantine. Authoritative source unchanged; subsequent ensure must be a verified hit. |
| `worker_lease_outage` | Existing signed fixture models HTTP timeout/clock expiry, exercises actual SQLite reservations and late cleanup; no actual Pi/agent process is stopped in this case. |
| `partial_checkpoint` | Existing fixture interrupts checkpoint response body, reopens real DB, and verifies only prior COMMITTED head can be selected. |
| `publisher_process_crash` | Existing fixture SIGKILLs its own publisher after immutable rename, reopens DB and refuses orphan promotion. |
| `nfs_outage_model` | Existing fixture injects ENETUNREACH at copy spawn. Warm recovery performs no source copy; cold replacement fails closed. This is a modeled transport error, not measured NFS server failure. |
| `ddp_rank_failure` | Existing real3-process CPU/Gloo fixture SIGKILLs one owned rank after a complete save, reaps group, stages/restores the committed cut and compares genuine resumed state to baseline. |
| `ddp_checkpoint_failure` | Existing real3-process fixture SIGKILLs rank during checkpoint acknowledgment, rejects incomplete cut and recovers full group from previous commit; foreign student fixture remains unchanged. |
| `stale_rank_fencing` | Existing signed fixture replays an expired attempt's checkpoint/result and verifies rejection without changing committed head. |

DDP outer launcher/isolation and NFS mount facts remain mocked. Native children
perform real CPU training/checkpoint save/load; FP32 tensor comparison uses the
existing **rtol1e-7, atol1e-8**, with exact non-floating state comparisons. These
are local regression experiments, never proof of deployed tenant isolation or
physical Pi full-group recovery. Reused tests and their assertions are retained
unchanged; no second checkpoint format or synthetic successful result is added.

The runner snapshots bounded known first-party sources, selected example inputs,
and public nodes/storage/lease-timing policy only. It rejects extra lease-policy
fields rather than copying future credentials. No production DB, per-attempt
lease authority, inventory secret, jobs, outputs or cache is copied. Each case
gets a NEW private writable execution copy separate from the frozen snapshot;
fixture import-time DB/log creation therefore stays outside the installation.
Child environments drop production runtime roots/resume/lease/auth variables.
Known fixture key strings are synthetic fixture credentials, not live keys.

The worker keeps allowlisted subprocesses in its owned case process group by
overriding child Popen session creation **only inside the disposable worker**.
Timeout/exit cleanup signals that group; it never searches for or kills a live
worker by name. Selected helper/rank bodies do not call setsid/setpgid themselves.
Tests exercise timeout descendant cleanup and preserve a separate sentinel
process. This harness containment is not a production process-isolation change.

### Measurements and missing values

`faults.jsonl`, `faults.csv` and each `<case>/result.json` preserve every outcome,
exit code, scope and monotonic integer nanoseconds. Epoch timestamps in experiment
metadata are UTC nanoseconds. Case logs, command invocation, source hashes and
private execution trees remain under a unique experiment ID; reruns never replace
existing IDs. Failures and timeout/missing/invalid receipts cannot count as PASS.

| Field | Exact measurement |
|---|---|
| `detection_ns` agent | Before SIGKILL to first failed loopback health probe. |
| `restart_ns` agent | Replacement private agent spawn to verified healthy response; not time to resume training. |
| `detection_ns` corruption | After replica corruption to forced verifier rejection. |
| `staging_ns` corruption | Repair ensure call including verification/refetch/publication; initial fill excluded. |
| `detection_ns` interrupted copy | Helper's injected read-error timestamp to retry-spawn timestamp, including scheduling/backoff. |
| `staging_ns` interrupted copy | Full ensure duration, including failed attempt and retry; overlaps detection and must not be added to it. |
| `harness_elapsed_ns` | Entire case child run/setup/assertions/cleanup; not failure detection or recovery latency. |
| `recovery_ns` | Null in this version; no instrumented end-to-end recovery boundary exists. |

Reused checkpoint/DDP fixture phase latencies are **null**, with explicit
`metric_scope` and `phase_latencies_missing`. Their elapsed test times must never
be substituted for detection/staging/restart/recovery. One successful injection
per case is functional acceptance, not a latency distribution or reliability
probability. No speedup, physical throughput, server bandwidth or chart is inferred.

### Reproduce safely

Read-only plan (creates no experiment):

```bash
PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/benchmarks/storage_fault_evaluation.py --experiment-id review-faults
```

Run locally with a fresh unique ID and permission for local sockets/CPU processes:

```bash
PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/benchmarks/storage_fault_evaluation.py --run --experiment-id my-local-faults
```

`--cases cache_corruption interrupted_staging` restricts scope. CLI choices are
fixed; there is no arbitrary command, host, PID, privileged deployment or physical
disconnect option. A selected source/fixture failure stays FAILED in raw results.
Default per-case timeout120s can be bounded to5..300s. This timeout is not a promise
to interrupt kernel-blocked NFS on a physical machine; this runner mounts no NFS.

### Physical disruptions deliberately NOT_RUN

`summary.json` and plans always list three deferrals: physical network disconnect,
physical shared NFS/controller outage, and live Pi agent/rank failures. No SSH,
service stop/restart, cable unplug, firewall action, mount/export/fstab edit,
package install or live Pi job occurred in13.3. pi01 remains deferred by user;
there is no all-three-Pi acceptance claim. Prior publication/staging approval
does not authorize these disruptions, and13.2 physical comparisons remain pending.

A later physical run needs a user-confirmed exclusive idle window, fresh actual
pi02/pi03 readiness, disposable owner-authorized jobs, reviewed admission and
resource reservations, exact target process identity, bounded rollback and proof
that unrelated students are unaffected. A network test additionally requires
explicit unplug/disconnect approval and an independently tested out-of-band
return path/local operator. Do not use the same link for the only recovery path.
Never stop this shared NFS server to simulate outage without proven isolation;
use a separately provisioned disposable server/network instead. No destructive
command is offered for the live server. The central controller remains a single
point of failure; application cache does not make it highly available.

### 13.3 observed results — 10 November 2026

**Local disposable campaign:10/10 PASS. Physical disruptions: NOT_RUN.**
Retained final cohort: `reports/eval-13-3-local-faults-v2/`, command:

```bash
PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/benchmarks/storage_fault_evaluation.py --run --experiment-id eval-13-3-local-faults-v2
```

Observed single-run phase values (nanoseconds, not latency distributions):

| Case | Detection ns | Staging ns | Agent restart ns | End-to-end recovery ns |
|---|---:|---:|---:|---|
| Agent process down |5,560,631 |not applicable |358,046,270 |unmeasured |
| Interrupted staging |139,936,045 |515,940,180 |not applicable |unmeasured |
| Corrupt replica |17,837,236 |223,133,755 |not applicable |unmeasured |

All other case phase fields remain null; raw per-case harness durations are
available but are not substituted here. Instrumentation/polling and local host
load affect these observations. The local model does not measure cable loss,
real NFS timeout detection, physical worker restart or physical cluster recovery.

Safe baseline command, run before new source edits:

```bash
PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_cache_fetch.py test_hybrid_checkpoint_faults.py
```

Both files PASS (33+16=49 tests). Later original regression selections PASS:
matrix11, evaluation18 and config17; final new runner suite9 PASS. Thus104
distinct top-level tests across6 files passed in their recorded runs. The new
fixture-adapter test failed in the initial broader run and was rerun successfully
after correcting setup; the failed overall log remains, and is not relabeled a
fully passing run. Original files were not changed to make these tests pass.

Development records are retained: `eval-13-3-initial-cache` FAILED at a newly
introduced string-literal syntax error; first `eval-13-3-local-faults` preserved
3 successful probes and7 fixture import failures. Import-order correction exposed
the omitted public timing-policy file; both harness issues were corrected before
the v2 cohort. These failures predate neither13.3 nor the original code baseline.
They remain separate cohorts and are not pooled into10/10 final acceptance.

`reports/eval-13-3-validation/artifact-verification.json` verifies208 frozen/live
source hashes, raw CSV/JSON equality,10 outcome/receipt pairs, integer/unknown
phase semantics, and all1,110 retained prior raw records (144 from13.1,966 from
13.2) unchanged. Existing13.2 source/archive publication evidence remains intact.
No live hardware was probed or mutated by13.3, and there are no new benchmarks
for the Pis. The full methodology source diff and machine-readable commands,
hashes and outcomes are in `55-fault-evaluation-evidence.json`.

### 13.3 interfaces, manual checks and rollback

New files: `tools/benchmarks/storage_fault_evaluation.py`,
`tools/benchmarks/evaluation/faults.py`, `fault_worker.py`,
`tests/test_storage_fault_evaluation.py`, and
`docs/storage/55-fault-evaluation-evidence.json`. Only methodology, progress and
tools/test guides are appended. No existing production/13.1/13.2 Python, API,
route, schema, configuration, checkpoint format or training code changes.
Legacy/OFF/resource defaults and controller-local SQLite remain intact. Schemas
in disposable cases are exercised through current code against NEW private DBs.

Manual checks (application directory; no faults or training):

```bash
PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/benchmarks/storage_fault_evaluation.py --experiment-id review-faults
python3 -B reports/eval-13-3-validation/verify_artifacts.py
git -C .. apply --reverse --check distributed-dl-system/reports/eval-13-3-validation/13.3.patch
```

Expect PLAN_ONLY with explicit physical NOT_RUN entries, retained-record hash
verification PASS, and a clean reverse-check if no later source edits conflict.
After reviewing later changes, rollback code with `git -C .. apply --reverse
distributed-dl-system/reports/eval-13-3-validation/13.3.patch`. Reverse application
is tested only in a disposable copy. Preserve every reports directory and all
13.2 staged Pi bundles/authoritative TEST objects; no running service, config or
data rollback is needed. Do not delete those objects or caches as source rollback.

**13.3 PASS for the documented local fault campaign; physical fault acceptance
remains pending its safety/idle-window prerequisites. Phase13 is incomplete.
STOP before13.4.**

## Phase 13.4 — offline measurements, provenance and charts

**Reporting acceptance: PASS under the collected local scope. Full physical
Phase13 evaluation is still incomplete.** This subphase reads retained records;
it starts no training/fault jobs and does not claim new Pi/NFS measurements.
Native13.2 comparisons and physical13.3 faults still require their previously
documented idle-window/approval/isolation prerequisites; pi01 remains deferred.

### Implementation and trust boundary

`tools/benchmarks/storage_evaluation_report.py` plans/validates read-only by
default. Explicit `--write` creates a NEW private report directory. Helpers in
`tools/benchmarks/evaluation/report_records.py`, `reporting.py` and
`report_charts.py` isolate bounded readers, metric normalization/summary and
optional Matplotlib/Agg output. No existing runner, worker, controller, trainer,
checkpoint format, production DB/schema/API or config is modified. Legacy/OFF/
resource defaults and controller-local SQLite remain unchanged.

Readers accept explicit local directories only. They reject traversal, unsafe
source/record paths, symlinks/FIFOs in consumed files, duplicate JSON keys/record
IDs, nonfinite/negative/bool numeric measurements, unsupported/mixed schemas and
metadata identity conflicts. Bounded source SHA-256 snapshots, per-rank runtime/
summary files, per-trial/case JSON and CSV/JSONL must agree. Sources are hashed as
bytes, never imported/executed; no pickle/torch checkpoint is loaded and no DB
is opened. Provenance captures the exact consumed files, hashes and reporting
source version. Historical input code need not equal today's code to be valid.

Reports preserve experiment/scope/workload/source/input hash, dataset version,
row count, world size, policy and cache phase. Groups never pool different
experiments or immutable identities. Summaries retain attempted/successful/failed
counts, all successful samples, mean/median/min/max and sample standard deviation;
n<2 means null stdev. No general confidence, speedup or reliability probability
is inferred from the small/dependent local trials. Failed development cohorts
remain visible, not blended into final acceptance cohorts.

### Observed coverage and limits

Final bundle: `reports/eval-13-4-collected-v2/` — **101 records,6 experiments,
53 separate summary groups,5 figures exported as PNG+SVG,264 traceable plotted
points**. Selected experiments are13.1 two/three-rank smoke,13.2 local matrix,
13.3 final local campaign and both retained13.3 failed development cohorts.
An earlier report before layout/schema tightening remains separately retained as
`eval-13-4-collected`; these are two postprocessing versions, not extra trials.

| Metric | Collected observation | Interpretation |
|---|---|---|
| Training time |80 records |Maximum per-rank recorded training-loop seconds, including any checkpoint work inside that loop; not sum of ranks or process launch duration. |
| Completion time |80 records |Measured harness wall time from original records, includes staging/launch/collection according to input scope. |
| Staging time |76 records |72 matrix records (A explicitly has no staging operation) and4 measured fault repair/copy probes; different boundaries remain separate. |
| Copied payload/cache hit bytes and ratio |54 records |B/C/D application counters. Ratio=hit/(hit+accepted copied bytes), including required source/requirements/data copies per worker. A remains unknown. |
| Failure detection / agent restart |6 /2 records |Recorded local probe boundaries only; whole-case time is not recovery latency. |
| Retained disk/model/checkpoint bytes |6 experiment snapshots |Current stat/st_blocks observation of retained files, deduplicated by device/inode. Not historical peak, live cache capacity, authoritative storage or server disk use. |
| Time to first step |Unmeasured |Existing log lines have no usable timestamped first-step event; do not subtract unrelated startup timestamps. |
| NFS wire bytes / server bandwidth |Unmeasured |No client/server/interface counters. Accepted copied payload is not wire traffic or retransmissions; Linux/NFS caching exists. |
| Checkpoint save / restore durations |Unmeasured |Existing archives/state-equivalence checks do not supply isolated timers. File timestamps and sizes cannot reconstruct durations. |
| End-to-end recovery overhead |Unmeasured |No timestamped recovered-training boundary; checkpoint tests' whole-case duration is not substituted. |

Disk observation is not an atomic filesystem snapshot; links/special entries are
skipped and counted, hardlinked regular inodes count once. Allocated bytes use
st_blocks×512. Root-only live NFS paths and authoritative storage are never
walked. Reads/hash validation can warm kernel pages, but no new training timing
is collected during reporting. No cache drops, remounts or counter reset occurs.

`metrics.json` / `metrics.csv` contain normalized per-record base units;
`coverage.csv` explains every measured/derived/null value. `summary.json` retains
the53 groups. `provenance.json` records input file hashes, original metadata,
current retained-disk observations and reporting source hashes. `files.json`
catalogs output SHA/size (excluding its own self-referential digest).

Figures are `collected-outcomes`, matrix `*-timing` / `*-cache`, and separate
fault `*-fault-phases`. Matrix figures show raw dots and descriptive means for
each workload/world/cache phase; A's missing payload observations are N/A,
never plotted as zero. Fault figures omit unmeasured phases and state that
phase boundaries differ/overlap. `chart-data.json` points name the exact input
experiment/record/metric/base-unit value; outcome counts retain failed records.
All figures are local-scope labeled. No graph presents physical Pi placement,
cross-Pi DDP, real NFS throughput or pending recovery measurements.

### Exact commands and optional plotting environment

Read-only validation with the unchanged controller Python (no plotting dependency):

```bash
PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/benchmarks/storage_evaluation_report.py --inputs reports/eval-13-2-controlled-local --report-id review-report
```

Optional plotting packages were installed **only into NEW controller reporting
venv** `reports/eval-13-4-plot-env`. This one-time setup downloaded controller-side
dependencies; neither the report runtime nor any worker downloads packages.
Existing `.controller-venv` and Pi environments were not modified. Exact resolved
pins are saved in `tools/benchmarks/evaluation/reporting-requirements.txt`
(observed Matplotlib3.11.2, plotting NumPy2.5.4). Original training versions remain
separate in input metadata (e.g. recorded training NumPy2.5.3); no training was
re-executed with plotting dependencies.

For a new controller-only plotting environment, provision those pins using your
reviewed package source/offline wheelhouse. No install is done by the reporting
script; requesting unavailable charts refuses before creating output. Matplotlib
uses Agg and a private temporary font/cache directory. CPU training/runtime
dependencies are not prerequisites for basic postprocessing.

Exact final report command (already run; use a new report ID to reproduce):

```bash
PYTHONDONTWRITEBYTECODE=1 reports/eval-13-4-plot-env/bin/python tools/benchmarks/storage_evaluation_report.py --inputs reports/eval-13-1-local-three-rank reports/eval-13-1-local-two-rank reports/eval-13-2-controlled-local reports/eval-13-3-local-faults-v2 reports/eval-13-3-initial-cache reports/eval-13-3-local-faults --write --charts --report-id eval-13-4-collected-v2
```

Without `--charts`, standard-library postprocessing still writes raw metrics,
summary, provenance and Markdown. Never substitute generated/example data for
missing experimental records; malformed records fail validation instead.

### Tests, verification, files and rollback

Safe pre-edit baseline: matrix11 +config17 =28 tests PASS. New reporter12 and
actual optional chart integration2 also PASS: **42 distinct tests across4 files**,
no failures/errors/skips in these selected runs. Commands/logs:

```bash
PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_storage_matrix.py test_storage_config.py
PYTHONDONTWRITEBYTECODE=1 reports/eval-13-4-plot-env/bin/python tools/run_tests.py --isolated test_storage_evaluation_report.py test_storage_report_charts.py
```

Logs live under `reports/eval-13-4-validation/`. The new suite covers strict
provenance/schema/path handling, numerical units, explicit missing values,
cohort/failure separation, hardlink-aware disk accounting, read-only/no-replace
behavior and exact plotted-data traceability. PNG/SVG integration passed; actual
timing/cache/fault/outcome images were visually inspected, and final cache-axis
headroom/shorter labels checked. A draft report remains intact rather than
overwritten during layout refinement. No new hardware benchmark ran.

Manual read-only verification:

```bash
python3 -B reports/eval-13-4-validation/verify_report.py
git -C .. apply --reverse --check distributed-dl-system/reports/eval-13-4-validation/13.4.patch
```

Expect verified input/output/source hashes,101 normalized records,53 cohorts and
264 exact plotted points; reverse-check should pass if later edits do not conflict.
Machine evidence and exact changed/new filenames are in
`docs/storage/56-evaluation-report-evidence.json`; scoped source diff is
`reports/eval-13-4-validation/13.4.patch`. Only four documentation files are
appended; all existing production and13.1/13.2/13.3 Python/config remain intact.
New files are the standalone report CLI, three report helpers, optional pinned
plot requirements, two test files and56 evidence.

After reviewing later edits, source rollback is `git -C .. apply --reverse
distributed-dl-system/reports/eval-13-4-validation/13.4.patch`. Reverse application
is verified only in a disposable copy. Keep all source experiment directories,
report outputs, plotting environment and prior Pi TEST objects/bundles; source
rollback needs no live config/service action and must not delete these records.

**13.4 offline reporting PASS. Requested uncollected metrics remain explicit gaps;
full Phase13 physical evaluation is NOT complete. STOP; no next phase, live enable
or deployment. Central server remains a single point of failure.**
