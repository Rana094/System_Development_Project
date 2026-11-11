# Tools

Run these commands from the repository root using `.controller-venv/bin/python`.
For example:

```bash
.controller-venv/bin/python tools/cluster_setup.py --help
.controller-venv/bin/python tools/demos/prepare_environments.py --help
```

## Setup and local checks

| Script | Purpose |
|---|---|
| [cluster_setup.py](cluster_setup.py) | Inspect inventory/worker health, install services and register workers |
| [run_tests.py](run_tests.py) | Separate test processes; `--isolated` also copies source and excludes live DB/runtime roots |

For local regression suites, prefer a temporary source copy so import-time
controller migrations cannot touch the live database:

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_storage_selection.py test_storage_config.py test_job_submission.py
```

See the [3.3 evidence and full test selection](../docs/storage/13-storage-selection.md).
The old runner invocation remains supported. Isolation covers test state; select
only the reviewed tests rather than using it to run a live demo.

## Live demonstrations

These scripts interact with the configured cluster and create real jobs. Follow
the linked guide for worker availability and dispatcher requirements.

| Script | Purpose | Guide |
|---|---|---|
| [demos/prepare_environments.py](demos/prepare_environments.py) | Submission, staging, environment verification and cache reuse | [Environments](../docs/environments.md) |
| [demos/concurrent_jobs.py](demos/concurrent_jobs.py) | Concurrent training and capacity queuing | [Scheduling](../docs/scheduling.md) |
| [demos/fair_queue.py](demos/fair_queue.py) | Owner fairness, holds and cancellation | [Queue controls](../docs/queue-controls.md) |
| [demos/checkpoint_recovery.py](demos/checkpoint_recovery.py) | Submitted trainer recovery after process failure or optional reboot | [Checkpoint integration](../docs/checkpoint-integration.md) |
| [demos/reboot_recovery.py](demos/reboot_recovery.py) | Reboot a worker running the built-in PyTorch workload and retain evidence | [Benchmarks and recovery](../docs/benchmarks.md) |

## Benchmarks

| Script | Purpose |
|---|---|
| [benchmarks/benchmark_training.py](benchmarks/benchmark_training.py) | Compare individual and two-worker DDP timing |
| [benchmarks/run_reference.py](benchmarks/run_reference.py) | Run the bundled reference workload matrix or full training |
| [benchmarks/verify_reference.py](benchmarks/verify_reference.py) | Verify a completed reference run, hashes and model metrics |

See the [benchmark guide](../docs/benchmarks.md). The optional `friend-resnet18`
timing mode expects the external comparison project at the path in the script;
reference verification also checks original source paths from `provenance.json`.
These external files are optional for normal portal and custom-job use.

## Completed-job comparisons

| Script | Purpose |
|---|---|
| [comparisons/compare_regression_jobs.py](comparisons/compare_regression_jobs.py) | Compare regression predictions, MSE and exact model/optimizer state |
| [comparisons/compare_training_jobs.py](comparisons/compare_training_jobs.py) | Compare regression/classification results and full DDP rank checkpoints |

Both comparison commands take `BASELINE_JOB_ID RECOVERED_JOB_ID`.
Old script names are listed in the [cleanup review](../docs/repository-cleanup.md).

## NFS controller preparation

[nfs_server_setup.py](nfs_server_setup.py) defaults to a read-only controller
plan using [nfs-server.json](../config/nfs-server.json):

```bash
env PYTHONDONTWRITEBYTECODE=1 python3 tools/nfs_server_setup.py --dry-run
```

Exit 2 means unresolved prerequisites or unsafe existing state; do not bypass
them. Explicitly approved `--apply` requires administrator privileges and
the current plan digest, backs up configuration first and stages configuration
only. Package installation, service actions and export activation are separate.
See the [2.2 setup/rollback guide](../docs/storage/05-nfs-server-setup.md) before use.

## NFS client preparation

[nfs_client_setup.py](nfs_client_setup.py) previews configured deployment nodes
offline (all three if unspecified; currently pi02/pi03 by user instruction):

```bash
env PYTHONDONTWRITEBYTECODE=1 python3 tools/nfs_client_setup.py --dry-run
```

Expected exit 2 indicates unresolved local prerequisites, not an applied
configuration. `--node piNN --inspect-local` is read-only on that
worker. Package provisioning and config staging require explicit
approval and a reviewed digest; they never edit fstab or activate the new units.
Read the [client setup/rollback guide](../docs/storage/06-nfs-client-setup.md).

The approved Phase 2 deployment and isolated outage validation have now passed
on pi02/pi03; pi01 remains deferred. See [actual results and rollback paths](../docs/storage/10-phase2-completion.md).
The setup commands still default to planning. Do not blindly reapply a client
plan while its real mount is active. Application storage flags remain legacy/off.
The administrator-only [fault fixture](../hybrid_storage/nfs_fault_probe.py)
defaults to a nonmutating plan; live use requires the reviewed isolated driver
and public TEST artifacts recorded in the evidence.

## Phase 11 validation on approved workers

[`validate_phase11.py`](validate_phase11.py) defaults to a nonmutating plan:

```bash
.controller-venv/bin/python -B tools/validate_phase11.py --nodes pi02 pi03
```

Its explicit administrator `--run` mode uploads verified source into NEW private
fixture roots, activates only the existing RO input readiness, and tests real
cache/workspace/inventory operations with public authorized hashes. It preserves
production agents/config and launches no untrusted trainer. The user approved
pi02/pi03 validation and deferred pi01. Read the [actual completion report](../docs/storage/48-phase11-live-acceptance.md)
before using `--run`: it records required public fixture publication, private
state, exact effects, all results and rollback. Do not reuse existing output
directories or treat this test bootstrap as a production sandbox/DDP launcher.

## Offline observability overhead

[`benchmarks/observability_overhead.py`](benchmarks/observability_overhead.py)
compares an empty call, disabled structured events and enabled JSONL emission to
memory, with bounded iterations/repeats and exact emitted-record validation:

```bash
PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/benchmarks/observability_overhead.py --iterations 5000 --repeats 5
```

JSON on stdout includes nanoseconds per call, every sample, platform and limits.
No controller imports, DB, NFS, Internet or worker operations. This does not
measure disk/network handlers or estimate total training overhead. See
[Phase 12.4 acceptance](../docs/storage/52-observability-acceptance.md).

## Reproducible evaluation — Phase 13.1

[`benchmarks/storage_evaluation.py`](benchmarks/storage_evaluation.py) prints a
read-only plan by default; explicit `--run` executes unchanged regression/CNN/DDP
trainers locally, saving immutable source/input identities and raw CSV/JSON/logs
under a new private `reports/<experiment-id>/`. No dispatcher, production DB,
worker HTTP/SSH, install, Internet transfer, mount operation or service action.

```bash
PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/benchmarks/storage_evaluation.py --experiment-id review-plan
```

Read [the methodology](../docs/storage/13-experiments.md) before an actual run.
Local smoke measurements are not A/B/C/D comparisons or physical Pi results;
unprobed worker eligibility/NFS/cache/scheduler metrics remain unknown.

The explicit `--matrix` path adds randomized repeated A/B/C/D component trials.
Use `--source-mode local-rehearsal` for Pi-less functional validation; every such
result is labeled rehearsal. Native NFS mode requires an existing verified RO
mount and explicit trusted-input acknowledgment; it has no fake/local fallback.
`--prepare-inputs` only builds a fresh local public-input review bundle and never
publishes it. Read the [13.2 methodology/security/uncertainty limits](../docs/storage/13-experiments.md)
before running. These modes do not activate or measure the live cluster scheduler.

## Disposable failure evaluation — Phase 13.3

[`benchmarks/storage_fault_evaluation.py`](benchmarks/storage_fault_evaluation.py)
prints a read-only allowlisted plan. Explicit `--run` creates NEW local private
source/fixture trees and retains raw case CSV/JSON/logs. It can kill only its own
disposable health agent, publisher or CPU/Gloo ranks; cache corruption and copy
interruption target new replicas. It never SSHs, stops live agents/NFS, changes
mounts or disconnects an interface. Real network/server/Pi disruption is explicitly
NOT_RUN. The earlier Pi idle-window restriction still applies.

```bash
PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/benchmarks/storage_fault_evaluation.py --experiment-id review-faults
```

Read [13.3 scope, commands and missing timings](../docs/storage/13-experiments.md)
before adding `--run`. Keep per-case detection, staging and restart values separate;
whole-test duration is not recovery latency. Local mocked NFS errors and real
local DDP failures are never physical Pi/NFS results.

## Collected evaluation reports — Phase 13.4

[`benchmarks/storage_evaluation_report.py`](benchmarks/storage_evaluation_report.py)
validates existing experiment directories read-only by default. `--write` creates
a NEW report with per-record CSV/JSON, coverage, cohort-safe summary, input/source
hash provenance and a retained-disk observation. It starts no jobs or faults and
never loads tensor/checkpoint payloads or opens a database. Optional `--charts`
uses a separate controller-side Matplotlib environment; it never installs anything
at runtime. Pinned plotting-only dependencies are under
[`evaluation/reporting-requirements.txt`](benchmarks/evaluation/reporting-requirements.txt).

```bash
PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/benchmarks/storage_evaluation_report.py --inputs reports/eval-13-2-controlled-local --report-id review-report
```

See [13.4 reproduction, actual charts and measurement gaps](../docs/storage/13-experiments.md).
NFS wire bytes, first-step/checkpoint timings and full recovery latency cannot be
reconstructed from old payload counters or whole-test durations.

## Hybrid delivery preparation (14.2, offline only)

`hybrid_deploy.py plan --output <new-absolute-local-directory>` prepares a private
all-three-Pi source/config review, unique backup namespaces and migration/restart/
rollback plan; `verify --output ...` checks it without SSH or DB/service changes.
`apply --output ...` always returns REFUSED/exit3; production integration and
readiness gates remain unresolved. Do not use the legacy cluster installer as a
hybrid deploy shortcut. Exact commands/results: [14.2 playbook](../docs/storage/58-deployment-playbook.md).

## Readiness checks and new private DB recovery points

`storage_readiness.py backup-db --source <absolute-local-db> --output <NEW-private-directory>`
backs up a read-only LOCAL SQLite source through SQLite and verifies a separate
restored copy; never restores/migrates live state. Outputs contain private data.
`print-worker-program` emits the stdlib-only read probe for explicitly approved
sudo checks, without remote file installation or service/config changes.
See [scope, exact commands, results and remaining blockers](../docs/storage/60-readiness-closeout.md).

## Reviewed source installation toolkit — Phase 14.2 continuation

`hybrid_install.py inspect-program --host controller|pi01|pi02|pi03` prints a
standalone read-only inventory program. `prepare --inventory <absolute-json>
--output <new-absolute-directory>` builds all four private host kits; `verify
--output ...` checks hashes/drift. Neither deploys or opens the live SQLite DB.
Host `run.py dry-run|install|migrate|rollback` has separate explicit guards and
approval requirements. The shipped approval example is invalid by design.
No package download, sudo, SSH mutation or service restart occurs in preparation.
Exact unit/config changes are zero and legacy defaults stay active.

See [61 installation tooling](../docs/storage/61-installation-tooling.md) before
any use. The original `hybrid_deploy.py apply` refusal remains compatible; this
new workflow does not imply hybrid activation or completed14.3 acceptance.
