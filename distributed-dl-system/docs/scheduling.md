# Step 4: resource allocation and concurrent training

Step 4 runs independent student submissions concurrently across workers. Each
worker has one training slot. With three available Pis, three jobs can run and
additional jobs wait in `QUEUED`. A submitted job uses the source snapshot,
dataset cache and native Python environment prepared in steps 1–3.

## Run the complete demonstration

Run this from the controller computer, with all three Pis powered on. Stop a
manually running dispatcher with Ctrl+C first; the demonstration holds the same
controller lock and runs the production dispatch loop itself. Existing active
jobs must finish before the demonstration starts.

```bash
cd '/home/rana/Projects/System Project/System_Development_Project/distributed-dl-system'
.controller-venv/bin/python -u tools/check_step4.py --nodes pi01 pi02 pi03
```

The tool submits four jobs with distinct student IDs, prepares their environments on all three workers,
queues them and observes three jobs `RUNNING` on distinct Pis while the fourth
remains `QUEUED`. Each trainer performs forward propagation, backpropagation and
Adam updates continuously for 30 seconds. The duration makes simultaneous
execution easy to observe; the workload does not sleep to simulate training.
The fourth job starts automatically when a slot becomes available.

Expect these messages, with your own job IDs and node order:

```text
CAPACITY VERIFIED: 3 distinct workers RUNNING; extra job QUEUED.
VERIFIED job-...: ... optimizer steps; model and logs in .../storage/jobs/job-...
Actual training overlap across initial workers: ... seconds.
PASS: concurrent real MLP training, capacity queue, automatic next dispatch, collected artifacts and released reservations.
```

Environment preparation can take several minutes the first time. The next jobs
reuse the same dependency and dataset caches. The tool exits after all jobs
finish; it leaves no background dispatcher. If interrupted, jobs remain tracked;
start the ordinary dispatcher below to continue monitoring them.

In a second terminal, inspect job states while the demonstration runs:

```bash
cd '/home/rana/Projects/System Project/System_Development_Project/distributed-dl-system'
watch -n 2 '.controller-venv/bin/python controller/job_status.py'
```

Inspect worker slots and reservations independently:

```bash
.controller-venv/bin/python - <<'PY'
import sys
sys.path.insert(0, 'controller')
from scheduler import load_nodes, get_resources
from job_store import get_reservations
for node in load_nodes():
    r = get_resources(node)
    print(node['id'], r['node_state'], r['slots'], r['allocation'])
print('Controller reservations:', get_reservations())
PY
```

During training, each busy worker reports `available: 0` and its requested CPU,
RAM and GPU allocation. After successful collection, the controller releases
the reservation and can place another job. After the demonstration, reservations
are empty and all worker slots are idle.

## Submit and run your own single-worker job

Start the controller in terminal 1:

```bash
cd '/home/rana/Projects/System Project/System_Development_Project/distributed-dl-system'
.controller-venv/bin/python -u controller/dispatcher.py
```

In terminal 2, follow the four explicit stages:

```bash
cd '/home/rana/Projects/System Project/System_Development_Project/distributed-dl-system'
.controller-venv/bin/python controller/submit_job.py \
  --spec examples/job.scheduling.json --owner student01

# Replace this with the job_id printed by submission.
JOB_ID='job-YOUR-ID'

.controller-venv/bin/python controller/prepare_job.py \
  --job-id "$JOB_ID" --nodes pi01 pi02 pi03 --profile pytorch-cpu
.controller-venv/bin/python controller/queue_job.py --job-id "$JOB_ID"
.controller-venv/bin/python controller/job_status.py "$JOB_ID"
```

Only prepared candidates can receive the job. Preparing it on all three workers
allows placement on any of them; preparing on one constrains it to that worker.
`READY` means prepared, and does not itself enqueue a job. Ordinary non-PyTorch
projects can use the `isolated` preparation profile.

The submitted entrypoint is an argv list executed without a shell from the job's
working source directory. `python` and `python3` use the prepared interpreter.
The executor supplies:

| Environment variable | Purpose |
|---|---|
| `DL_JOB_ID` | Submission identity |
| `DL_DATASET_PATH` | Worker dataset file/directory; absent for generated data |
| `DL_SOURCE_DIR` | Job working source directory |
| `DL_OUTPUT_DIR`, `DL_RESULTS_DIR` | Write final models and other outputs here |
| `DL_CHECKPOINT_DIR` | Worker checkpoint directory; use the [step 6 SDK](checkpoint-integration.md) for recovery |

Inspect collected outputs after `COMPLETED`:

```bash
cat "storage/jobs/$JOB_ID/results/summary.json"
cat "storage/jobs/$JOB_ID/logs/attempt-0.log"
ls -lh "storage/jobs/$JOB_ID/results/model.pt"
```

For any submitted script, stdout/stderr are collected into `logs/attempt-N.log`
and regular files written to `DL_OUTPUT_DIR` are collected into `results/`.
The scheduler does not require a specific model, metric or output filename.
A zero process exit and successful collection determine `COMPLETED`.

## What each block does

- `queue_job.py` verifies central inputs and queues only a READY, single-worker
  submission using restart recovery. Distributed jobs remain gated until DDP dispatch is implemented. Step 6 adds
  checkpoint-enabled single-worker jobs.
- `scheduler.py` checks requested CPU cores, available RAM and CUDA GPU count;
  filters busy, reserved, unreachable and unprepared workers; then ranks eligible
  workers by CPU load and RAM. It scans submissions in FIFO order and can place
  a later fitting job when an earlier request cannot currently fit. Step 5 adds [owner fairness, quotas and queue controls](queue-controls.md).
- `job_store.py` reserves an exclusive worker slot and marks `SCHEDULED` in one
  SQLite transaction. Reservations survive controller restart and uncertain HTTP
  starts. Definite rejection, completion or observed attempt failure releases
  them in the state-transition transaction.
- `agent.py` checks the prepared snapshot and rechecks admission under its slot
  lock, then starts one tracked process group. Attempt tokens make repeated
  requests idempotent. `/resources` reports slot and allocation information.
- `custom_runner.py` revalidates the native environment and input cache before
  training, applies CPU affinity and tensor-library thread counts, and executes
  the submitted argv. CUDA visibility matches the GPU request.
- `job_artifacts.py` collects regular result files and logs before completion.
  Collection failure retains the job and reservation for another polling cycle.
- `check_step4.py` exercises the public submission/preparation/queue commands and
  production dispatcher, then checks actual overlapping training intervals,
  changed model parameters, model hashes, finite model tensors, Adam step counters,
  collected logs and released slots.

RAM is an admission request, not an enforced process memory limit. Only one job
runs per worker, even when spare cores remain. GPU discovery uses `nvidia-smi`;
GPU execution has not been tested on this CPU-only Pi cluster. Lab GPU machines
must prepare their own compatible environments before being added to inventory.

This milestone verifies concurrent independent training and capacity queuing.
Custom jobs retain their reservations during lost contact until the worker
confirms an outcome; partition fencing is a later milestone. The existing bundled
PyTorch checkpoint/reboot recovery path remains available. Submitted PyTorch recovery now uses the [step 6 SDK](checkpoint-integration.md);
coordinated multi-worker DDP execution is step 7.
