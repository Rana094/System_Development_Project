# Run multiple training examples

These are three real PyTorch workloads with checkpoint recovery:

| Job definition | Training task | Submitted dataset |
|---|---|---|
| `examples/job.integration.json` | MLP regression: predict a numeric value | Four CSV examples of `y = 2x + 1` |
| `examples/job.binary.json` | MLP binary classification: inside or outside a circle | 256 two-dimensional points |
| `examples/job.cnn.json` | CNN image classification: vertical, horizontal or diagonal lines | 180 synthetic 8 × 8 grayscale images stored as CSV pixels |

The classification metrics measure the training dataset, not an independent test
set. These small datasets keep the demonstration practical on the Pis. Each
model performs 800 optimizer updates. The 80 ms delay between updates gives you
time to inspect jobs and reboot a worker.

## 1. Submit the jobs

From the project directory:

```bash
.controller-venv/bin/python controller/submit_job.py --spec examples/job.integration.json --owner student01
.controller-venv/bin/python controller/submit_job.py --spec examples/job.binary.json --owner student01
.controller-venv/bin/python controller/submit_job.py --spec examples/job.cnn.json --owner student01
```

Copy the three printed IDs:

```bash
REGRESSION_ID='job-REGRESSION-ID'
BINARY_ID='job-BINARY-ID'
CNN_ID='job-CNN-ID'
```

## 2. Prepare the environments

```bash
.controller-venv/bin/python controller/prepare_job.py --job-id "$REGRESSION_ID" --nodes pi01 pi02 pi03 --profile pytorch-cpu
.controller-venv/bin/python controller/prepare_job.py --job-id "$BINARY_ID" --nodes pi01 pi02 pi03 --profile pytorch-cpu
.controller-venv/bin/python controller/prepare_job.py --job-id "$CNN_ID" --nodes pi01 pi02 pi03 --profile pytorch-cpu
```

Preparation happens before queueing; it copies inputs and installs dependencies.
Cached environments are reused on subsequent submissions of the same project.

## 3. Queue all three

```bash
.controller-venv/bin/python controller/queue_job.py --job-id "$REGRESSION_ID"
.controller-venv/bin/python controller/queue_job.py --job-id "$BINARY_ID"
.controller-venv/bin/python controller/queue_job.py --job-id "$CNN_ID"
```

In a separate terminal, start the dispatcher if it is not already running:

```bash
.controller-venv/bin/python -u controller/dispatcher.py
```

The configured per-student active limit is now three. With all three Pis idle,
compatible and prepared, the three independent jobs can run concurrently, one
on each Pi. When workers are occupied or resource requirements cannot be met,
remaining jobs wait in the queue. A single job still uses one Pi.

## 4. Watch and inspect

```bash
watch -n 2 '.controller-venv/bin/python controller/queue_ctl.py list --owner student01'
```

Press Ctrl+C to stop watching, then inspect individual jobs:

```bash
.controller-venv/bin/python controller/job_status.py "$BINARY_ID"
.controller-venv/bin/python controller/job_status.py "$CNN_ID"
```

To show recovery, wait for a running job to have a central `Checkpoint step`
greater than zero. Reboot only its active Pi, as described in
[the integration instructions](integration-instructions.md). Keep the dispatcher
running. If the other workers are busy, recovery can wait for a compatible worker
to become available.

## 5. Read the results

Once each job is completed:

```bash
cat "storage/jobs/$REGRESSION_ID/results/summary.json"
cat "storage/jobs/$BINARY_ID/results/summary.json"
cat "storage/jobs/$CNN_ID/results/summary.json"
```

Classification results include cross-entropy loss, training accuracy, predictions
and the update from which training resumed. Every example exports `model.pt`
containing model, Adam optimizer and learning-rate scheduler state.

For an uninterrupted-versus-recovered comparison, submit the same job definition
twice. Leave one run uninterrupted and reboot the worker running the other.
After both complete:

```bash
.controller-venv/bin/python tools/comparisons/compare_training_jobs.py "$BASELINE_ID" "$RECOVERY_ID"
```

Both runs must use the same task and inputs. Expected output includes exact
model/optimizer/scheduler equality and equal metrics and predictions.
