# Project map

Use this page to find the code responsible for a feature. For usage instructions,
start at the [documentation index](README.md). Paths below are relative to the
repository root; this is a navigation guide for the current layout.

## Essential entrypoints

| Task | File |
|---|---|
| Browser interface | [`controller/student_portal.py`](../controller/student_portal.py) |
| Schedule and recover jobs | [`controller/dispatcher.py`](../controller/dispatcher.py) |
| Run a worker service | [`agent/agent.py`](../agent/agent.py) |
| Submit a job from the terminal | [`controller/submit_job.py`](../controller/submit_job.py) |
| Inspect job history | [`controller/job_status.py`](../controller/job_status.py) |
| Install/inspect the cluster | [`tools/cluster_setup.py`](../tools/cluster_setup.py) |

## Controller files

The portal, dispatcher and terminal commands share these modules. Keep their
paths stable: installed services, imports and deployment tools reference them.
`controller.py` is an optional read-only terminal dashboard, not the dispatcher.
`migrate_pytorch_jobs.py` is a compatibility command for older databases.

| File | Responsibility |
|---|---|
| [checkpoint_store.py](../controller/checkpoint_store.py) | Atomic controller storage for the legacy JSON dummy-job checkpoints |
| [controller.py](../controller/controller.py) | Read-only cluster dashboard; scheduling and recovery run separately in dispatcher.py |
| [custom_checkpoints.py](../controller/custom_checkpoints.py) | Central SDK checkpoint synchronization and validated restore transfers |
| [dispatcher.py](../controller/dispatcher.py) | Controller loop: place queued work, reconcile starts, collect checkpoints, and recover failed attempts |
| [distributed_dispatch.py](../controller/distributed_dispatch.py) | Persist and reconcile a complete CPU DDP worker group as one scheduled job |
| [execution_leases.py](../controller/execution_leases.py) | Controller-issued generations, renewal uncertainty and publication fencing |
| [group_checkpoints.py](../controller/group_checkpoints.py) | Validate and atomically retain complete DDP checkpoint bundles on the controller |
| [job_artifacts.py](../controller/job_artifacts.py) | Stage worker outputs and publish only while their execution still owns the job |
| [job_spec.py](../controller/job_spec.py) | Version-one student submissions: validate declarations without running their code |
| [job_status.py](../controller/job_status.py) | Display persistent job state and the history of worker attempts |
| [job_storage.py](../controller/job_storage.py) | Publish self-contained, fingerprinted student inputs on the controller filesystem |
| [job_store.py](../controller/job_store.py) | SQLite job and attempt state. Each transition commits its related updates together |
| [job_timing.py](../controller/job_timing.py) | Save controller wall times separately from trainer-reported monotonic durations |
| [migrate_pytorch_jobs.py](../controller/migrate_pytorch_jobs.py) | Idempotent migration for databases created before PyTorch job metadata was added |
| [prepare_job.py](../controller/prepare_job.py) | Prepare staged projects on selected workers; no resource reservation or dispatch occurs |
| [queue_ctl.py](../controller/queue_ctl.py) | Inspect student queues and persist owner-scoped hold, release and cancellation requests |
| [queue_job.py](../controller/queue_job.py) | Queue a prepared single-worker or CPU DDP submission for resource-aware dispatch |
| [queue_policy.py](../controller/queue_policy.py) | Configurable student limits and persistent fair ordering of runnable queue entries |
| [recovery_metrics.py](../controller/recovery_metrics.py) | Append timestamped recovery events for later measurement and troubleshooting |
| [recovery_report.py](../controller/recovery_report.py) | Compute recovery timings around the latest explicitly recorded failure injection |
| [scheduler.py](../controller/scheduler.py) | Choose an eligible idle worker by lowest CPU usage, then greatest available RAM |
| [stage_job.py](../controller/stage_job.py) | Stage received submissions or audit a committed input snapshot after a restart |
| [student_portal.py](../controller/student_portal.py) | Student web interface over the existing submission, queue and recovery services |
| [submit_job.py](../controller/submit_job.py) | Validate command-line training options and atomically publish complete jobs to the queue |

The browser assets are [`index.html`](../controller/portal_static/index.html)
(page structure), [`portal.css`](../controller/portal_static/portal.css) (styles),
and [`portal.js`](../controller/portal_static/portal.js) (requests and interactions)
under `controller/portal_static/`.

## Worker files

| File | Responsibility |
|---|---|
| [agent.py](../agent/agent.py) | Worker HTTP API: report resources, launch one process at a time, and transfer checkpoints |
| [checkpoint_protocol.py](../agent/checkpoint_protocol.py) | Tensor-library-independent validation for the versioned checkpoint transport |
| [custom_runner.py](../agent/custom_runner.py) | Verify a prepared submission and execute its argv in the job's native environment |
| [dl_checkpoint.py](../agent/dl_checkpoint.py) | Cooperative PyTorch checkpoint saving for single workers and coordinated DDP groups |
| [environment_worker.py](../agent/environment_worker.py) | Worker-local input caches and Python environments, independent of the job launcher |
| [execution_lease.py](../agent/execution_lease.py) | Durable worker lease identity and bounded local execution deadlines |
| [lease_runner.py](../agent/lease_runner.py) | Fail-stop supervisor independent of Flask and the student's checkpoint calls |

## Examples, configuration, tools and tests

Each guide below lists the files in its area and explains when to use them.

| Folder | Contents | File guide |
|---|---|---|
| `config/` | Worker inventory, queue/lease policies, submission schema and service dependencies | [Configuration files](../config/README.md) |
| `examples/` | Eight job manifests, five training projects with requirements, and three sample datasets | [Example catalog](../examples/README.md) |
| `tools/` | Cluster setup, regression runner, live demos, benchmarks and result comparisons | [Tool catalog](../tools/README.md) |
| `tests/` | Fourteen regression scripts for controller, worker, portal and recovery behavior | [Test catalog](../tests/README.md) |
| `training/` | Built-in dummy/PyTorch jobs, evaluation and benchmark adapter | [Workload catalog](../training/README.md) |
| `training/reference/` | Eleven unchanged comparison scripts, their explanations and source hashes | [Reference catalog](../training/reference/README.md) |
| `docs/` | Installation, usage, feature workflows, architecture and this file map | [Documentation index](README.md) |

The root [`README.md`](../README.md) is the startup/navigation entry page.
[`.gitignore`](../.gitignore) separates generated state from source.

## Runtime and machine-local files

These locations are used by the existing installation and excluded from Git.
A file being generated does not mean it is safe to erase during operation.

| Location | Contents | Treatment |
|---|---|---|
| `.controller-venv/` | Installed Python interpreter links and dependencies | Recreate from requirements when needed; keep the current environment for services |
| `controller/jobs.db`, `jobs.db-wal`, `jobs.db-shm` | Durable jobs, attempts, reservations and SQLite journal state | Preserve job history; back up consistently before maintenance |
| `controller/.dispatcher.lock`, other `*.lock` | Process/operation coordination | Owned by running processes |
| `config/nodes.lock` | Atomic worker inventory registration lock | Managed by setup tooling |
| `storage/jobs/` | Managed project/data snapshots, environments, logs and results | Preserve submitted jobs and outputs |
| `storage/portal/` | Login records and session secret | Preserve accounts and protect access |
| `storage/benchmarks/` | Locally generated timing runs | Experiment results, separate from source |
| `storage/deployment/` | Previous controller service definitions | Operational rollback data |
| `checkpoints/` | Central built-in/dummy recovery checkpoints | Preserve recoverable jobs |
| `logs/` | Local service/deployment logs | Operational output |
| `worker-storage/` on workers | Prepared environment caches, execution state and deployment backups | Worker-managed runtime output |
| `__pycache__/`, `*.pyc` | Compiled Python bytecode | Regenerable cache |
| `~/distributed-dl-private/` | External comparison results, historical reports and private notes | Outside this repository |

No application modules were established as unused. The [cleanup review](repository-cleanup.md)
explains which files were removed from Git tracking, which tools moved, and why
optional components remain available.
