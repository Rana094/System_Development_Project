# Step 7: schedule one DDP job across multiple Pis

The scheduler supports both independent single-worker jobs and one CPU PyTorch
DDP job spanning several workers. DDP ranks train the same model on separate
data shards and synchronize gradients through Gloo.

## 1. Submit and prepare

From the controller project directory:

```bash
.controller-venv/bin/python controller/submit_job.py --spec examples/job.distributed.json --owner student01
```

Copy the printed job ID:

```bash
JOB_ID='job-ID-PRINTED-ABOVE'
.controller-venv/bin/python controller/prepare_job.py --job-id "$JOB_ID" --nodes pi01 pi02 pi03 --profile pytorch-cpu
.controller-venv/bin/python controller/queue_job.py --job-id "$JOB_ID"
```

Preparing three candidates allows the scheduler to select any two compatible,
available Pis. It reserves both together; when only one is free, the job waits
without occupying that Pi. Another job that fits can still be dispatched.

## 2. Run and watch

On the installed cluster, start the dispatcher service if needed:

```bash
systemctl --user start dl-dispatcher
```

In your submission terminal:

```bash
.controller-venv/bin/python controller/job_status.py "$JOB_ID"
.controller-venv/bin/python controller/queue_ctl.py list --owner student01
```

The dispatcher prints `DDP_RESERVED`, `DDP_RUNNING` and `DDP_COMPLETED`. Job status
shows each rank's worker, status and attempt. The `Node` field is the rank-zero
coordinator; the `DDP ranks` list is the full worker group.

The third Pi can run an independent single-worker job at the same time. A DDP
job counts as one active job toward the student's quota, while reserving one
worker slot per rank.

## 3. Inspect the results

After completion of the original attempt:

```bash
cat "storage/jobs/$JOB_ID/results/rank-0/summary.json"
cat "storage/jobs/$JOB_ID/results/rank-1/summary.json"
ls -lh "storage/jobs/$JOB_ID/results/rank-0/model.pt"
cat "storage/jobs/$JOB_ID/logs/attempt-0-rank-0.log"
```

Both rank summaries should show `parameters_synchronized: true`, equal final
model hashes and a final hash different from the initial hash. The example runs
40 epochs on the submitted binary-classification CSV. With two ranks, its first
epoch indices show separate 128-example shards; each rank performs 160 optimizer
updates. Accuracy is measured on the training data.

## Change the worker count

Edit `execution.workers` in `examples/job.distributed.json` before submitting a
new job. The trainer's global batch size defaults to 64 and must be divisible by
the worker count; for three workers, add `--global-batch-size`, `96` to the
entrypoint arguments. Prepare at least that many compatible workers. Each rank receives the
declared `resources_per_worker`; a three-worker job needs three available Pis.
PyTorch's sampler pads uneven shards when the dataset size is not divisible by
the worker count.

## Failure and cancellation

Step 7 uses `recovery.mode: restart`. If one rank fails, the controller stops the
whole group, confirms every rank has ended, and restarts the complete job within
its retry budget. With execution leases enabled, reservations remain held until
termination is confirmed or the bounded lease/fencing wait finishes. A restart starts training at epoch
zero. Consistent group checkpoints are available in Step 8 below; automatic
checkpoint resume is available in Step 9 below.
Single-worker checkpoint recovery continues to work as before.

Cancel the entire group with:

```bash
.controller-venv/bin/python controller/queue_ctl.py cancel --owner student01 --job-id "$JOB_ID"
```

The worker environment supplies `RANK`, `WORLD_SIZE`, `LOCAL_RANK=0`,
`MASTER_ADDR`, `MASTER_PORT` and `DL_DDP_BACKEND=gloo`. One process runs per Pi.
The trainer must initialize a process group, use DDP, shard its input and ensure
all ranks execute matching collective operations. Step 7 is verified on CPU;
GPU/NCCL execution is not enabled yet.

The launch and data-sharding pattern follows the
[official PyTorch DDP tutorial](https://docs.pytorch.org/tutorials/intermediate/ddp_tutorial.html).

## Compare individual and DDP training time

The same `ddp_project/train.py` also supports ordinary single-worker execution.
Finish existing jobs and stop the dispatcher, then run:

```bash
.controller-venv/bin/python -u tools/benchmark_training.py --nodes pi02 pi03 --epochs 1000 --repeats 3
```

The benchmark uses one core on pi02 for individual training and one core on each
Pi for DDP. Both modes process the same dataset for the same epochs and optimizer
updates with global batch size 64. Artificial delays are disabled. It prepares
environments before measurement, runs trials sequentially in alternating order,
and saves median training-loop and total scheduler times, speedup, percent time
change, and per-trial evidence under `storage/benchmarks/<run-time>/`.

After the command finishes, restart your normal dispatcher when needed.

To reproduce the friend's actual ResNet DDP benchmark settings:

```bash
.controller-venv/bin/python -u tools/benchmark_training.py --workload friend-resnet18 --nodes pi02 pi03 --epochs 1 --samples 1000 --repeats 3
```

This copies the original benchmark unchanged into the run's managed project,
prepares CPU PyTorch/TorchVision environments and uses the Step 7 group scheduler.
Before running, both workers need the existing CIFAR-10 cache at
`/tmp/cifar10_data` and the ImageNet ResNet-18 weights in their Torch Hub cache.
They are present on pi02/pi03 after the verified run. Four CPU cores per worker
and batch 32 per worker match the friend's convention. The global batch doubles
under two-worker DDP, so its optimizer update count differs from single-worker
training. The friend's 2× logistic benchmark uses independent workers without
gradient synchronization; it is a different method from ResNet DDP.


## Step 8: consistent DDP checkpoints

Submit the checkpoint-enabled version of the same real classifier:

```bash
.controller-venv/bin/python controller/submit_job.py --spec examples/job.distributed-checkpoint.json --owner student01
```

Copy the printed ID, then prepare and queue it:

```bash
JOB_ID='job-ID-PRINTED-ABOVE'
.controller-venv/bin/python controller/prepare_job.py --job-id "$JOB_ID" --nodes pi02 pi03 --profile pytorch-cpu
.controller-venv/bin/python controller/queue_job.py --job-id "$JOB_ID"
```

Use the installed dispatcher service; do not also start a manual dispatcher:

```bash
systemctl --user start dl-dispatcher
```

Watch the job from the submission terminal:

```bash
.controller-venv/bin/python controller/job_status.py "$JOB_ID"
```

The dispatcher prints `DDP_CHECKPOINT` with increasing step numbers and
`acknowledged=2/2`. Job status shows the group checkpoint ID, acknowledgments,
step and SHA-256. The example performs 160 optimizer updates per rank. At
completion, expect step 160, `Checkpoint complete: True`, and both ranks
completed with synchronized parameters.

Inspect the saved archive and rank results:

```bash
unzip -l "storage/jobs/$JOB_ID/checkpoints/latest.ddpckpt"
cat "storage/jobs/$JOB_ID/results/rank-0/summary.json"
cat "storage/jobs/$JOB_ID/results/rank-1/summary.json"
```

The archive contains `metadata.json`, `rank-0.pt` and `rank-1.pt`. Each rank's
payload includes its full model (including buffers), optimizer, learning-rate
scheduler, progress, CPU Torch/Python/NumPy random state and application extras.
This example also records sampler settings and its independent DataLoader
generator state. The controller validates identities, archive contents, hashes,
worker count and all-rank acknowledgments before replacing the central copy.
A checkpoint-enabled job requires a valid final checkpoint to become completed.

### Add checkpoint saving to another DDP trainer

Include `torch` and `numpy` in the submitted requirements; PyTorch object
collectives require NumPy. Initialize the Gloo process group first, then construct
the SDK manager:

```python
from dl_checkpoint import DistributedCheckpointManager
checkpoints = DistributedCheckpointManager()
```

After the optimizer and learning-rate scheduler update, every rank must call:

```python
checkpoints.save(
    model, optimizer, scheduler=scheduler,
    global_step=steps,
    progress={"next_epoch": next_epoch, "next_batch": next_batch},
    extra_state={"loader_rng": loader_generator.get_state()},
)
```

At the final training boundary, call it on every rank with `completed=True` and
`force=True`. The manager uses rank zero's interval decision, collects complete
snapshots, checks agreement on parameters/optimizer/scheduler, and obtains an
acknowledgment from every rank before publishing. Buffers and random states can
differ by rank and are preserved separately. Calls must occur in the same order
on all ranks; a rank-zero-only save would leave other ranks waiting. Custom
samplers, generators and application state must be supplied in `extra_state`.
The complete integration is in `examples/ddp_project/train.py`.

Step 8 supports CPU/Gloo with a fixed worker count. Rank zero temporarily holds
all serialized rank states; the combined payload limit is 10 GiB, and the model
must fit the workers' memory. Automatic restore and coordinated retry from this
archive are implemented in Step 9 below. The current checkpoint example enables
two retries and Dropout to exercise random-state restoration. Set `max_retries: 0`
when you want a failed group to stop without resuming. Step 7's restart mode and
Step 6's single-worker checkpoint resume remain available.

Run the checkpoint protocol and failure checks without the Pis:

```bash
.controller-venv/bin/python tests/test_group_checkpoint.py -v
```


## Step 9: coordinated DDP checkpoint recovery

The controller stops the failed group, confirms every old rank has ended, and
reserves a complete compatible replacement group. It transfers one frozen
checkpoint to every rank before launching any rank. Each rank loads its own
state and participates in collective restore confirmation. The job becomes
running only after both ranks confirm the expected checkpoint, data progress
and new attempt identity. The logical rank count remains fixed; physical Pis
can change between attempts.

Whole-Pi reboot recovery has been verified on pi03 and pi02. Pi02's truncated
Python executables and Torch/NumPy libraries were repaired and checked against
package file hashes. Prepare all three compatible candidates below so the
scheduler can choose two available workers. To reproduce the pi02-specific run,
use `--nodes pi01 pi02` for both submissions and reboot pi02 after checking that
it is one of the active ranks.

### 1. Start the dispatcher

On the installed cluster, use the persistent service:

```bash
systemctl --user start dl-dispatcher
journalctl --user -u dl-dispatcher -f
```

### 2. Train the uninterrupted baseline

In terminal B:

```bash
.controller-venv/bin/python controller/submit_job.py --spec examples/job.distributed-checkpoint.json --owner student01
BASELINE_ID='job-ID-PRINTED-ABOVE'
.controller-venv/bin/python controller/prepare_job.py --job-id "$BASELINE_ID" --nodes pi01 pi02 pi03 --profile pytorch-cpu
.controller-venv/bin/python controller/prepare_job.py --job-id "$BASELINE_ID" --nodes pi01 pi02 pi03 --verify --import-module torch --import-module numpy --summary
.controller-venv/bin/python controller/queue_job.py --job-id "$BASELINE_ID"
.controller-venv/bin/python controller/job_status.py "$BASELINE_ID"
```

Repeat the status command until `COMPLETED`. This is a real binary classifier
with hidden layers, Dropout, Adam, a learning-rate scheduler and CSV input.
The example performs 160 optimizer updates per rank. The delay between batches
makes its checkpoint progress easy to observe.

### 3. Submit the same training again

```bash
.controller-venv/bin/python controller/submit_job.py --spec examples/job.distributed-checkpoint.json --owner student01
RECOVERY_ID='job-ID-PRINTED-ABOVE'
.controller-venv/bin/python controller/prepare_job.py --job-id "$RECOVERY_ID" --nodes pi01 pi02 pi03 --profile pytorch-cpu
.controller-venv/bin/python controller/prepare_job.py --job-id "$RECOVERY_ID" --nodes pi01 pi02 pi03 --verify --import-module torch --import-module numpy --summary
.controller-venv/bin/python controller/queue_job.py --job-id "$RECOVERY_ID"
.controller-venv/bin/python controller/job_status.py "$RECOVERY_ID"
```

Wait until `Checkpoint step` is greater than zero and acknowledgments are `2 / 2`.
Read the current attempt's `DDP ranks` rows to identify its active Pis. In
terminal C, replace the value below with one of those active workers, preferably
rank zero to test loss of the checkpoint coordinator:

```bash
NODE_TO_REBOOT='pi01'
ssh "$NODE_TO_REBOOT" 'sudo -n reboot'
```

The SSH connection may close during reboot. Keep the dispatcher running. Repeat:

```bash
.controller-venv/bin/python controller/job_status.py "$RECOVERY_ID"
```

The controller holds old reservations until termination is confirmed or execution
leases fence the old attempts, stops remaining peers and retries the group.
With enough compatible candidates, the failed Pi need not return. If only two
candidates exist for two ranks, it must wait for two available workers.
Terminal A prints `DDP_RESTORE_PREPARED`,
`DDP_RESUMED` and `DDP_COMPLETED`; status displays a recovery line with the
restored step, source attempt and rank confirmation count. The source checkpoint
might be newer than the one you last inspected if another complete save occurred
before reboot. The new attempt resumes from that common boundary and replays any
updates after it. You do not resubmit the failed job.

### 4. Compare final training state

After both jobs are completed:

```bash
.controller-venv/bin/python tools/compare_training_jobs.py "$BASELINE_ID" "$RECOVERY_ID"
cat "storage/jobs/$RECOVERY_ID/results/rank-0/summary.json"
cat "storage/jobs/$RECOVERY_ID/results/rank-1/summary.json"
```

Expect `recovered_resumed_from` greater than zero,
`exact_model_optimizer_scheduler_match: true`,
`exact_rank_checkpoint_states_match: true` and `pass: true`. The rank-state
comparison covers model buffers, optimizer, scheduler, Torch/Python/NumPy RNG,
application generator and sampler state, and final data progress. It checks
matching source, dataset, environment, worker count and entrypoint first.
This proves restoration preserved the training trajectory; model accuracy is
separate from scheduling and recovery correctness.

### Integrate restore into another trainer

Every rank constructs its model, optimizer, scheduler and data loader before
calling the SDK. Supply a callback that restores custom generator/sampler state
and validates the saved application cursor:

```python
def restore_application(extra, progress):
    loader_generator.set_state(extra["loader_rng"])
    sampler.set_epoch(progress["next_epoch"])

restored = checkpoints.load(
    model, optimizer, scheduler=scheduler,
    restore_extra=restore_application,
)
steps = restored["global_step"] if restored else 0
next_epoch = restored["progress"]["next_epoch"] if restored else 0
next_batch = restored["progress"]["next_batch"] if restored else 0
```

All ranks call `load`, including the initial attempt, and start their loops at
the returned cursor. The SDK restores global RNG after the callback and requires
all ranks to agree before acknowledging restoration. Nonempty `extra_state`
requires a callback. Recreating a mid-epoch DataLoader iterator can consume an
additional generator seed; the supplied trainer preserves its saved generator
state while recreating the iterator. Its input is a deterministic TensorDataset
with `num_workers=0`. Other loaders, stochastic transforms and prefetch workers
need an application-specific replay or state strategy; arbitrary unmodified
training scripts do not gain exact resume automatically.

Recovery currently supports CPU/Gloo with a fixed world size and compatible
prepared environments. A known missing/corrupt central checkpoint fails the job
rather than silently starting from zero. Failure before the first committed
checkpoint can only restart from the beginning. With [Step 10 execution leases](execution-ownership.md), an unreachable worker
retains its reservation until termination is confirmed or the bounded lease wait
finishes; the old producer is fenced before replacement. The example allows two retries. GPU/NCCL and
changing world size during restore are not implemented.

Run the coordinated restore tests locally:

```bash
.controller-venv/bin/python tests/test_group_recovery.py -v
```
