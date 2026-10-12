# Distributed deep-learning system

The controller queues jobs, chooses idle workers using CPU load and available RAM,
and stores checkpoints centrally. Node Agents launch the training processes and
report their state. A failed PyTorch attempt can continue on another worker using
the controller's last valid model and optimizer checkpoint.

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
