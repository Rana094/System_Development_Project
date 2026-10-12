# Step 6: checkpoints in submitted training code

Single-worker PyTorch submissions can now use `recovery.mode: checkpoint`.
The provided `dl_checkpoint` SDK saves model parameters, optimizer state,
optional learning-rate scheduler state, CPU/CUDA/Python/global NumPy RNG state,
and the application's next training position. The controller verifies the
archive, keeps a durable central copy and restores it onto a compatible prepared
worker when an attempt fails.

The training script must cooperate by calling the SDK at safe points in its
loop. Code using restart recovery continues to run without SDK calls. The
system cannot infer training state from an arbitrary program's memory.

## Run the real training and recovery demonstration

Start with three powered-on, idle Pis. Stop a manually running dispatcher with
Ctrl+C; this tool holds its lock and runs the production controller functions.

```bash
cd '/home/rana/Projects/System Project/System_Development_Project/distributed-dl-system'
.controller-venv/bin/python -u tools/check_step6.py --nodes pi01 pi02 pi03 --failure process
```

The tool submits two copies of the same stochastic MLP. It completes one as an
uninterrupted baseline. For the other, it waits for a controller-owned checkpoint,
kills the training process group, observes the failure, and resumes on another
Pi. It compares the final model tensors, complete Adam state and learning-rate
scheduler state exactly against the baseline. Random batching and dropout make
missing RNG restoration observable. Both jobs perform 6,000 real optimizer
updates; the sample includes a 3 ms delay per batch to leave time to inject the
failure and inspect progress.

To reboot the entire active Pi during training:

```bash
.controller-venv/bin/python -u tools/check_step6.py --nodes pi01 pi02 pi03 --failure reboot
```

This command actually reboots the worker chosen for the interrupted job. It
checks that its boot ID changes and that recovery runs on another Pi. It requires
the existing passwordless SSH and reboot permission on the Pi aliases. Use it
when no unrelated work is running on those Pis. This proves a software reboot;
it does not simulate abrupt loss of electrical power.

Expect messages like:

```text
FAILURE INJECTED: reboot on pi02; central checkpoint step=...
[RESUMED] job-... confirmed on pi03 at step ...
WHOLE-PI REBOOT VERIFIED: boot ID changed; node rejoined; recovery used a different worker.
RESUME VERIFIED: step=... on pi03; model, Adam, scheduler and RNG restored.
EXACT EQUIVALENCE VERIFIED: final model tensors, Adam state and scheduler match uninterrupted training.
PASS: submitted-code checkpoint recovery, real training, different-worker resume, final central checkpoint and exact baseline equivalence.
```

First-time native environment preparation can take several minutes. Runtime
outputs remain in each job's `storage/jobs/<job-id>/` directory. The tool prints
the recovered job's workspace path and exits without a background dispatcher.
If interrupted, start the ordinary dispatcher to continue monitoring tracked jobs.

Watch the queue in another terminal:

```bash
cd '/home/rana/Projects/System Project/System_Development_Project/distributed-dl-system'
watch -n 2 '.controller-venv/bin/python controller/queue_ctl.py list --owner step6-demo'
```

Inspect a job's attempts and checkpoint metadata:

```bash
JOB_ID='job-YOUR-ID'
.controller-venv/bin/python controller/job_status.py "$JOB_ID"
cat "storage/jobs/$JOB_ID/results/summary.json"
ls -lh "storage/jobs/$JOB_ID/checkpoints/latest.dlckpt"
cat "storage/jobs/$JOB_ID/logs/attempt-1.log"
```

## Integrate your own PyTorch training script

The executor provides `dl_checkpoint` on the submitted interpreter's module path.
PyTorch remains a dependency of the student's prepared environment. The complete
runnable example is `examples/checkpoint_project/train.py` and its manifest is
`examples/job.checkpoint.json`.

After creating your model and optimizer, use this pattern:

```python
from dl_checkpoint import CheckpointManager

checkpoints = CheckpointManager()
restored = checkpoints.load(model, optimizer, scheduler=lr_scheduler)
next_step = restored["progress"]["next_step"] if restored else 0

for step in range(next_step, total_steps):
    optimizer.zero_grad()
    loss = loss_function(model(inputs_for_step(step)), targets_for_step(step))
    loss.backward()
    optimizer.step()
    lr_scheduler.step()

    # Capture a consistent state after the update; resume starts at the next step.
    checkpoints.save(
        model, optimizer, scheduler=lr_scheduler,
        global_step=step + 1, progress={"next_step": step + 1},
    )

# Required for checkpoint-enabled completion, including when the loaded checkpoint
# already contains all planned updates.
checkpoints.save(
    model, optimizer, scheduler=lr_scheduler,
    global_step=total_steps, progress={"next_step": total_steps},
    completed=True, force=True,
)
```

Omit `scheduler` on both save and load when you do not use one. `save` returns
`True` when it publishes a checkpoint and `False` when the configured interval
has not elapsed. `force=True` and `completed=True` bypass the interval.

Progress is application-defined JSON: use `next_epoch`, `next_batch`, sample
positions or other fields your loop needs. It must describe the next unprocessed
work. The SDK does not guess how your DataLoader or sampler advances. Use
`extra_state={...}` for tensor and builtin state such as an independent
`torch.Generator.get_state()` or gradient scaler state, and restore that state
from the dictionary returned by `load` before continuing training. Objects,
functions and arbitrary pickle classes are unsupported.

The save boundary must be consistent. Gradient accumulation, custom samplers,
background data-worker RNGs, external data streams and distributed ranks need
application-specific state or a suitable epoch/batch boundary. Global RNG
restoration does not capture DataLoader prefetch queues. Use one manager in one
training process and avoid concurrent save calls.

Declare checkpoint recovery in the submission manifest:

```json
"recovery": {
  "mode": "checkpoint",
  "interval_seconds": 5,
  "max_retries": 2
}
```

Submit, prepare and enqueue through the normal commands:

```bash
.controller-venv/bin/python controller/submit_job.py \
  --spec examples/job.checkpoint.json --owner student01
JOB_ID='job-ID-PRINTED-BY-SUBMISSION'
.controller-venv/bin/python controller/prepare_job.py \
  --job-id "$JOB_ID" --nodes pi01 pi02 pi03 --profile pytorch-cpu
.controller-venv/bin/python controller/queue_job.py --job-id "$JOB_ID"
.controller-venv/bin/python -u controller/dispatcher.py
```

Save final model/results into `DL_OUTPUT_DIR` as before. A checkpoint-enabled
script that exits successfully without an explicit final SDK checkpoint is not
marked `COMPLETED`; it fails according to the configured retry budget after a
short final-transfer grace period.

## How the recovery blocks work

- `agent/dl_checkpoint.py` captures and loads model, optimizer, scheduler, RNG
  and application progress. It publishes a ZIP_STORED archive with metadata and
  tensor-safe PyTorch state through atomic replacement and filesystem sync.
  Interrupted private save files are cleaned on the next manager initialization.
- `agent/checkpoint_protocol.py` verifies the format, job/input/environment
  identity, attempt, progress and state hash using the standard library. It
  rejects duplicate/unexpected ZIP entries, compressed entries and oversized
  archives; state payloads are limited to 10 GiB.
- `agent/custom_runner.py` supplies trusted job identity, interval, checkpoint
  paths and expected resume digest. The SDK loads using `weights_only=True` and
  acknowledges restoration after states and RNGs are loaded.
- `agent/agent.py` transfers a checkpoint only onto an idle worker, validates
  incoming bytes before replacing its resume file, and exposes the trainer's
  acknowledgment only for the matching launcher and attempt.
- `controller/custom_checkpoints.py` validates received archives, rejects stale
  attempts and regressing progress, atomically publishes the central copy and
  uploads it to a replacement worker. A bad transfer preserves the previous copy.
- `controller/dispatcher.py` selects candidates with compatible prepared
  environments, restores before launching, confirms restoration from the trainer
  and requires a final checkpoint before completion. Failed-attempt logs are
  collected when available. Fair owner quotas and cancellation still apply.

The new executor variables include `DL_ATTEMPT_TOKEN`, `DL_MANIFEST_SHA256`,
`DL_COMPATIBILITY_KEY`, `DL_CHECKPOINT_INTERVAL_SECONDS`, `DL_RESUME_CHECKPOINT`,
`DL_RESUME_SHA256` and `DL_RESUME_STATUS_PATH`. Applications should use the SDK
instead of constructing or overriding them.

During an unreachable-worker interval, custom jobs retain their reservations.
After a rebooted agent returns and confirms the old process is gone, the
controller can retry on another worker. This avoids treating lost network
contact alone as permission to run a second copy; network-partition fencing is
still a later milestone. Unsaved updates after the last central checkpoint may
be replayed. The Pi demonstration verifies CPU recovery; CUDA restoration logic
has not been exercised on GPU hardware. DDP dispatch and coordinated group
checkpoints remain later steps.

Run isolated SDK and transport checks without the Pis:

```bash
.controller-venv/bin/python -m unittest discover -s tests -p 'test_custom_checkpoint.py' -v
```
