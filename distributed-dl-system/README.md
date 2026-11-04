# Distributed deep-learning system

The controller queues jobs, chooses idle workers using CPU load and available RAM,
and stores checkpoints centrally. Node Agents launch the training processes and
report their state. A failed PyTorch attempt can continue on another worker using
the controller's last valid model and optimizer checkpoint.

Start with the [website initialization, usage and training integration guide](docs/student-interface.md).
It covers startup, every website option, custom uploads, and an interactive
walkthrough of scheduling, DDP and checkpoint recovery. For machine installation
and migration, use the [deployment guide](docs/deployment.md).

## Run the website locally

On the existing controller laptop, open a terminal and run:

```bash
cd "/home/rana/Projects/System Project/System_Development_Project/distributed-dl-system"
systemctl --user start dl-dispatcher dl-student-portal
systemctl --user is-active dl-dispatcher dl-student-portal
```

Expect two `active` lines. Open **http://127.0.0.1:8080/** in your browser and
sign in using your existing account. The services keep running after you close
the terminal. The portal displays the website; the dispatcher schedules and
monitors training. Workers must be online and configured to execute jobs.

For remote access on the configured Tailscale network, open
**http://100.73.90.91:8080/**. See [Tailscale setup and checks](docs/tailscale.md).

For a fresh checkout without installed services, follow the
[manual local-server instructions](docs/student-interface.md#run-the-local-server-manually-without-installed-services).
They cover Python setup, dependencies, creating a login, starting both processes,
checking the server and stopping it. Replace the project path when using another PC.

## Source layout

| Folder | Purpose |
|---|---|
| `agent/` | Worker HTTP API, resource monitoring, process lifecycle and checkpoint transfer |
| `controller/` | Job database, submission, scheduler, dispatcher and recovery measurements |
| `training/` | Dummy workload, recoverable PyTorch MLP, evaluation and benchmark adapter |
| `training/reference/` | Unmodified reference workloads copied from the comparison project, with source hashes |
| `tests/` | Isolated regression checks; they do not use the live job database |
| `tools/` | Reproducible live-recovery and comparison runners |
| `config/` | Worker addresses and ports |
| `storage/jobs/` | Controller-owned student inputs, manifests and separate runtime folders; ignored by Git |
| `checkpoints/`, `logs/` | Local runtime data; retained for recovery and job history |

Private notes, historical reports, backups and new experiment results live under
`~/distributed-dl-private/`, outside this repository. The virtual environment and
`controller/jobs.db` are operational data and must be retained.

## How the main code blocks work

1. **Submission — `controller/submit_job.py`, `job_store.py`.** Validate the
   request and insert its type, configuration and QUEUED state in one SQLite
   transaction. This prevents the dispatcher seeing a partly defined job.
2. **Placement — `controller/scheduler.py`.** Filter unavailable, busy and
   temporarily unhealthy workers. Among eligible workers, prefer lower CPU
   usage, using available RAM as the tie-breaker. Comparison jobs can constrain
   eligible nodes to keep hardware consistent with the reference experiment.
3. **Dispatch — `controller/dispatcher.py`.** Persist SCHEDULED before contacting
   a worker. An attempt token makes repeated start requests identifiable. If a
   start response is lost, reconcile with that worker before launching again.
4. **Execution — `agent/agent.py`.** Validate supported workloads and keep a single
   worker slot under a lock. Launch a separate process group, track its exit,
   expose logs/results through job state, and transfer checkpoint bytes.
5. **Learning — `training/pytorch_job.py`.** Regenerate deterministic data, build
   an MLP, run forward propagation, compute cross-entropy, backpropagate and
   update Adam. Save model/optimizer state and the next unprocessed batch using
   atomic file replacement. On resume, skip only already-processed batches.
6. **Recovery — dispatcher and trainer.** Sync checkpoints to the controller,
   detect repeated lost contact, record the failed attempt, and temporarily
   exclude that worker. Upload the checkpoint before starting the replacement.
   Record RESUMED only when that process confirms both state dictionaries loaded.
   Require a valid final controller checkpoint before marking PyTorch COMPLETED.
7. **Measurements — `recovery_metrics.py`, `recovery_report.py`.** Append events
   and calculate timing differences. Use the latest central checkpoint before
   the injected failure, rather than a checkpoint from the replacement worker.
8. **Reference comparisons — `training/benchmark_job.py`.** Run fixed, bundled
   scripts using their original model/dataset settings. Keep reference compute
   time separate from controller wall time. These jobs do not checkpoint or
   retry; two-node DDP comparison is not fault-tolerant DDP.

## Normal use

Step 11 adds the [student browser interface](docs/student-interface.md): upload
code and data, automatically prepare and queue single-worker or DDP training,
follow live logs and recovery, cancel jobs, and download results.


General student submission **steps 1–4** are implemented: validate a JSON project
definition and persist its owner, execution mode, resource requests, dataset,
environment and recovery declaration, then publish controller-owned source and
dataset snapshots with file hashes and content versions. Successful submissions
become `STAGED`; preparing enough compatible worker environments makes them `READY`.
Step 4 explicitly queues READY single-worker jobs and dispatches them using
resource requests and persistent exclusive worker reservations. See
[the concurrent training guide](docs/scheduling.md).
See [the submission guide](docs/submission.md) and [storage guide](docs/storage.md).

Run the complete terminal demonstration yourself:

```bash
PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/check_step3.py --nodes pi01 --profile pytorch-cpu
```

It submits two jobs, verifies managed inputs, prepares and checks the worker's
Python environment, and demonstrates cache reuse. See [step 3 commands and expected output](docs/environments.md).

```bash
.controller-venv/bin/python controller/submit_job.py --spec examples/job.single.json --validate-only
.controller-venv/bin/python controller/submit_job.py --spec examples/job.single.json --owner student-2026
```

Run commands from this directory:

```bash
source .controller-venv/bin/activate
python -u controller/dispatcher.py
```

In another terminal:

```bash
source .controller-venv/bin/activate
python controller/submit_job.py --type pytorch --epochs 60 --checkpoint-every-seconds 5 --batch-delay 0.05
python controller/job_status.py
```

Only run one dispatcher. The controller enforces this with a lock.

## Tests

```bash
PYTHONDONTWRITEBYTECODE=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 \
  .controller-venv/bin/python tests/test_checkpoint_recovery.py
```

Checks include exact model/Adam equivalence after interrupted training, queue
transactions, uncertain starts, checkpoint validation, restore acknowledgment,
benchmark dispatch, placement constraints and dummy compatibility.

## Deploy controller-owned code

Deploy while workers are idle:

```bash
for node in pi01 pi02 pi03; do
    rsync -av --exclude=__pycache__ agent/ "$node:~/distributed-dl-system/agent/"
    rsync -av --exclude=__pycache__ training/ "$node:~/distributed-dl-system/training/"
    ssh "$node" 'sudo -n systemctl restart dl-node-agent && systemctl is-active dl-node-agent'
done
```

## Live checks and comparisons

The reboot runner intentionally reboots a selected training worker:

```bash
.controller-venv/bin/python -u tools/run_reboot_demo.py
```

With all workers idle and no dispatcher running, compare the reference workloads:

```bash
.controller-venv/bin/python -u tools/run_comparison.py
```

The runner starts and stops its own dispatcher, constrains benchmarks to
pi02/pi03, and writes private results outside the repository. `--models` selects
any of `equation logistic numpy_mlp resnet18 tiny_cnn`. Images and pretrained
weights must be cached before measuring the image workloads. CIFAR-10 is stored
at `/tmp/cifar10_data`, matching the reference; it may need preparing after reboot.

Model accuracy, single-run compute speed and scheduler/recovery behavior are
separate measurements. Historical numbers from a different environment are
reference observations, not a controlled proof that one scheduler is faster.

## Full reference training and inference

The benchmark variants above differ from the friend's full training workers.
Run all five original training workflows on two workers with:

```bash
.controller-venv/bin/python -u tools/run_comparison.py \
  --models equation logistic_train numpy_mlp_train resnet18_train tiny_cnn_train \
  --world-sizes 2 --output-dir "$HOME/distributed-dl-private/full-training"
```

This uses the original worker code and epoch counts. Logistic regression and
the NumPy MLP export independent rank models; the runner averages their parameters
using the same arithmetic as the original master scripts. ResNet-18 trains on
all 50,000 CIFAR-10 images for one epoch, and the CNN trains for five epochs with
the original augmentation. Both image workers synchronize gradients using DDP.
Their original inference scripts then evaluate the exported model on 10,000
test images. Model files are copied to the private run directory and checked
against the worker's SHA-256 digest.

Your controller replaces the friend's Slurm dispatch/master launch commands.
The original source folder is not edited. Fixed output paths expected by the
image workers are temporarily reserved; existing weights at those paths are
restored afterward. Full-run wall time includes inference and artifact
validation, so it must not be compared with training-only benchmark timers.
The `benchmark` job type hosts these reference workloads with zero retries;
it does not add checkpoint recovery to the friend's workers.

Validate a completed full-training run and generate private notes with:

```bash
.controller-venv/bin/python tools/verify_reference_training.py \
  "$HOME/distributed-dl-private/full-training/<run-directory>"
```

This checks completion and artifact hashes, verifies unchanged reference sources,
and evaluates the averaged logistic/MLP models on independent synthetic examples.
Successful execution and good model accuracy are reported separately.

## Step 4: concurrent student jobs

Single-worker submissions can now be explicitly queued after environment
preparation. Resource requests and durable worker reservations control placement.
Run the four-job real PyTorch demonstration and inspect its outputs using
[the Step 4 terminal guide](docs/scheduling.md).

## Step 5: fair queues and student controls

Students have configurable active and pending limits. Fair owner turns and queue
aging share workers; hold, release and durable cancellation control submissions.
See [the Step 5 terminal guide](docs/queue-controls.md) for the real training
demonstration, owner commands and lab policy settings.

## Step 6: checkpoint recovery for submitted PyTorch code

The cooperative `dl_checkpoint` SDK saves model, optimizer, scheduler, random
state and application progress. Validated central checkpoints can resume a
single-worker submission on another compatible prepared worker. See the
[Step 6 integration and real reboot test guide](docs/checkpoint-integration.md).

For a command-by-command live workflow that submits regression jobs, shows the
queue and worker status, reboots the active Pi, and compares uninterrupted and
recovered results, see the [integration instructions](docs/integration-instructions.md).

The [training examples guide](docs/training-examples.md) submits regression,
binary classification and CNN jobs concurrently and explains their recovery and
result comparison commands.

## Step 7: coordinated CPU DDP jobs

The controller reserves a complete compatible worker group, starts one PyTorch
rank per Pi, and reconciles every rank before completing, cancelling or retrying
the job. Independent jobs can use remaining workers. See the
[distributed training instructions](docs/distributed-training.md) for submission,
status, results and the current restart-recovery behavior.


## Step 8: consistent DDP checkpoints

All ranks save a common training boundary with their complete local states.
The worker publishes only after every rank acknowledges the checkpoint; the
controller validates and retains the complete archive. See the
[Step 8 commands and SDK integration](docs/distributed-training.md#step-8-consistent-ddp-checkpoints).
Automatic group checkpoint restore is available in Step 9 below.


## Step 9: coordinated DDP checkpoint resume

A failed DDP group resumes from the same validated checkpoint on every logical
rank after all old processes are stopped. Restored model, optimizer, scheduler,
random state and application progress are confirmed collectively before the
controller marks the replacement attempt running. Follow the
[Step 9 terminal workflow](docs/distributed-training.md#step-9-coordinated-ddp-checkpoint-recovery)
to reboot an active Pi and compare its result with uninterrupted training.


## Step 10: execution ownership and network partitions

Worker leases and durable generations fence expired attempts. An independent
watchdog terminates their process groups, and collectors reject stale checkpoints
and results before publication. Both single-worker and DDP jobs can recover while
the failed worker remains disconnected. See the
[Step 10 terminal instructions](docs/execution-ownership.md).

## Step 12: installation and lab migration

The [deployment and full-system inspection guide](docs/deployment.md) covers
automated worker setup/registration, persistent controller services, real training
submission, reboot recovery and gradual migration from Pis to Linux lab PCs.
Run `python3 tools/cluster_setup.py check` to inspect registered workers.
