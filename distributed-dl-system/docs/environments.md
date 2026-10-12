# Execution environments — step 3

A staged job can now be prepared on selected workers. Each worker caches the
verified source and dataset, builds a Python virtual environment, installs the
project's requirements, checks dependency consistency and records its installed
versions. A successful job becomes `READY` when enough compatible candidate
workers have been prepared. Preparation does not reserve resources or start
training. Single-worker scheduling and dispatch use the separate [step 4 commands](scheduling.md).

## One-command terminal demonstration

From the controller PC, copy and run:

```bash
cd "/home/rana/Projects/System Project/System_Development_Project/distributed-dl-system"
PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/check_step3.py \
  --nodes pi01 --profile pytorch-cpu
```

This submits two real demonstration jobs and checks steps 1–3. It prints their
job IDs, verifies their source/dataset snapshots, prepares environments on pi01,
imports PyTorch in those environments and checks that the second job reuses the
source, dataset and environment caches. The first preparation may download
packages; subsequent preparations should be faster.

Expected concluding output:

```text
pi01: READY | Python 3.11.16 | aarch64
torch=2.14.1+cpu | No broken requirements found.
cache hits: {'source_cached': True, 'data_cached': True, 'environment_cached': True}
PASS: submission, storage, worker environments, imports and cache reuse verified.
```

Versions can differ if the package index or configured interpreter changes.
The compatibility and dependency checks must still pass.

To exercise distributed-job preparation on two Pis:

```bash
PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/check_step3.py \
  --nodes pi01 pi02 --profile pytorch-cpu
```

The distributed job stays `STAGED` after preparing only one compatible candidate
and becomes `READY` after the required second candidate is prepared. This command
prepares both candidates; it does not start DDP training.

## Individual commands

Submit and capture the job ID without needing jq:

```bash
JOB_ID=$(.controller-venv/bin/python controller/submit_job.py \
  --spec examples/job.single.json --owner student-2026 \
  | .controller-venv/bin/python -c 'import json,sys; print(json.load(sys.stdin)["job_id"])')
echo "$JOB_ID"
```

Check the managed input snapshot:

```bash
.controller-venv/bin/python controller/stage_job.py --verify "$JOB_ID"
```

Prepare the environment on pi01:

```bash
.controller-venv/bin/python controller/prepare_job.py \
  --job-id "$JOB_ID" --nodes pi01 --profile pytorch-cpu --summary
```

Verify in a fresh worker process and import the expected training library:

```bash
.controller-venv/bin/python controller/prepare_job.py \
  --job-id "$JOB_ID" --nodes pi01 --verify --import-module torch --summary
.controller-venv/bin/python controller/job_status.py "$JOB_ID"
```

Run preparation again to inspect cache reuse:

```bash
.controller-venv/bin/python controller/prepare_job.py \
  --job-id "$JOB_ID" --nodes pi01 --profile pytorch-cpu --summary
```

A good result includes `status: READY`, `prepared: true`, the native Python version
and architecture, and cache-hit flags. Verification prints `verified: true`, the
PyTorch import version and a successful pip dependency check. `job_status.py`
shows prepared worker IDs and their environment interpreter paths.

For a distributed submission, use `examples/job.distributed.json` and prepare
`--nodes pi01 pi02`. You can prepare pi03 as another compatible candidate too;
none of these candidates is reserved until scheduling is implemented.

## Profiles

| Profile | Behaviour |
|---|---|
| `isolated` | Default. New venv using the selected worker interpreter; installs requirements from the normal pip index without inheriting baseline packages. |
| `pytorch-cpu` | Isolated venv using the official PyTorch CPU wheel index. Use for the Pi example and CPU-only PyTorch jobs. GPU-requesting jobs are rejected for this profile. Dependencies must be available from that index, or the requirements file must configure their source. |
| `worker-base` | New venv with the worker interpreter's existing package directories added as a baseline. Job-specific installations live in the new venv; preparation never installs into or uninstalls from the baseline. Baseline package versions participate in the cache identity and pip checks must pass. |

The existing Pi baseline contains an NVIDIA SBSA wheel whose platform tag is
rejected by pip on aarch64. Therefore `worker-base` cannot currently pass the
Pi consistency check. The tested `pytorch-cpu` profile avoids that package in a
separate environment; the original training/agent environment is not modified.
A lab operator can use `worker-base` with a clean curated GPU baseline on lab PCs.

Pin dependency versions in student requirements for reproducible builds. Local
wheels and nested dependency files can live inside the project. Pip gets a
separate, stable dependency-source copy, so build backends cannot mutate the
verified source cache. The complete project version participates in the cache
key, conservatively rebuilding when project inputs change.

Virtual environments separate dependency installations. They do not isolate
student code from the worker filesystem, network or other processes. OS/container
isolation and authenticated student access are required before opening execution
to untrusted university submissions.

## Worker and controller records

Default worker layout under `~/distributed-dl-system/worker-storage/`:

```text
inputs/source/<version>/content/   Verified, reusable project snapshot
inputs/data/<version>/content/     Verified, reusable dataset snapshot
environments/<key>/                Job venv, dependency-source, ready marker
pip-cache/                         Pip download/wheel cache
jobs/<job-id>/                      Working source copy, outputs and preparation records
```

The environment key includes interpreter version, ABI, architecture, platform,
profile and source/dependency identity. ARM and x86 environments are built
separately. Environment directories are built at their final prefix because
venv launch scripts embed absolute paths. They become reusable only after a
successful `ready.json` marker is published. A failed build has no ready marker;
a retry rebuilds that incomplete environment under its build lock.

`jobs/<job-id>/prepared.json` records the resolved packages, Python path, source
and dataset versions and locations. `preparation.log` contains initial build and
pip output. The controller stores the verified record in `job_environments`.
A distributed job needs the requested count of candidate records with matching
Python major/minor version, architecture, profile and effective training package
versions; pip/setuptools/wheel versions are excluded from that compatibility
comparison. Failed verification removes the stale candidate and recalculates
whether the job remains `READY`.

Remote preparation uses the existing SSH aliases and rsync. No agent service
restart is needed. Optional fields in each configured node are `worker_root`
(default `distributed-dl-system`, relative to its SSH home) and `worker_python`
(default `.venv/bin/python`, relative to that root). Configure native interpreters
and suitable baseline environments when moving to the lab PCs.

A controller-side check can use `--nodes local`; its worker storage is under
`storage/local-worker` or `DL_LOCAL_WORKER_ROOT`. The local candidate is a testing
facility and is not a Pi worker. Future scheduling must consider registered
worker capacity and verify candidates again before launching jobs.

## Offline automated checks

```bash
PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tests/test_job_environments.py
PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tests/test_job_submission.py
PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tests/test_job_storage.py
```

Environment tests create actual temporary venvs and install locally generated
wheels. They verify that dependency versions 1.0 and 2.0 coexist without
interference, source/data/environment cache reuse, fresh-process verification,
incompatible Python rejection, failed-build retry, package/input corruption
detection and distributed readiness. They use isolated databases and do not
contact the Pis or change their installed packages.
