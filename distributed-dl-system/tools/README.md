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
