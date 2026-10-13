# Integration instructions: training and checkpoint recovery

Run two identical regression jobs: finish the first normally, then reboot the Pi
running the second. Compare their final results. Each job trains for 800 updates
and takes roughly one minute after preparation.

## 1. Open the project

Use three powered-on, idle Pis. Stop an existing dispatcher with Ctrl+C before
starting this walkthrough.

In terminal 1:

```bash
cd '/home/rana/Projects/System Project/System_Development_Project/distributed-dl-system'
.controller-venv/bin/python controller/job_status.py
```

## 2. Submit two jobs

Run this command twice. Copy the first and second printed job IDs:

```bash
.controller-venv/bin/python controller/submit_job.py --spec examples/job.integration.json --owner student01
```

Keep these variables in terminal 1, replacing the example IDs:

```bash
BASELINE_ID='job-FIRST-ID'
RECOVERY_ID='job-SECOND-ID'
```

## 3. Prepare and queue both jobs

Preparation copies the submitted code and dataset and prepares the Python
requirements on each Pi. First-time preparation can take several minutes.

```bash
.controller-venv/bin/python controller/prepare_job.py --job-id "$BASELINE_ID" --nodes pi01 pi02 pi03 --profile pytorch-cpu
.controller-venv/bin/python controller/prepare_job.py --job-id "$RECOVERY_ID" --nodes pi01 pi02 pi03 --profile pytorch-cpu
.controller-venv/bin/python controller/queue_job.py --job-id "$BASELINE_ID"
.controller-venv/bin/python controller/queue_job.py --job-id "$RECOVERY_ID"
.controller-venv/bin/python controller/queue_ctl.py list --owner student01
```

## 4. Start the dashboard and scheduler

In terminal 2, open the project directory and run the live cluster dashboard:

```bash
cd '/home/rana/Projects/System Project/System_Development_Project/distributed-dl-system'
.controller-venv/bin/python controller/controller.py
```

In terminal 3, open the project directory and start the dispatcher:

```bash
cd '/home/rana/Projects/System Project/System_Development_Project/distributed-dl-system'
.controller-venv/bin/python -u controller/dispatcher.py
```

In terminal 1, watch the queue:

```bash
watch -n 2 '.controller-venv/bin/python controller/queue_ctl.py list --owner student01'
```

The default owner limit allows one active job. The first job runs while the
second waits. After the first completes, the scheduler starts the second.
Press Ctrl+C to stop watching; this leaves the dispatcher running in terminal 3.

## 5. Reboot the Pi running the second job

In terminal 1, inspect the second job:

```bash
.controller-venv/bin/python controller/job_status.py "$RECOVERY_ID"
```

Wait until it shows `RUNNING` and a `Checkpoint step` greater than zero. Read its
`Node` field. Reboot that Pi using its alias; replace `piNN` with the active node:

```bash
ssh piNN 'sudo -n reboot'
```

Leave the dispatcher running. The dashboard shows the Pi disconnect and return.
Once its agent confirms the old attempt is gone, the scheduler restores the
central checkpoint on another compatible Pi. An SSH disconnection during the
reboot is expected.

## 6. Check recovery

Repeat this command until the second job shows `COMPLETED`:

```bash
.controller-venv/bin/python controller/job_status.py "$RECOVERY_ID"
```

Its attempts should show a failed attempt and a completed attempt on another Pi.
Check the results and resume log:

```bash
cat "storage/jobs/$BASELINE_ID/results/summary.json"
cat "storage/jobs/$RECOVERY_ID/results/summary.json"
rg 'TRAINING_START|TRAINING_COMPLETE' "storage/jobs/$RECOVERY_ID/logs/attempt-1.log"
```

The baseline has `resumed_from: 0`. The recovered job has `resumed_from` greater
than zero, and its log reports `model/Adam/scheduler/RNG restored=True`.
Log suffixes use the retry count: `attempt-0.log` is the original run and
`attempt-1.log` is the first retry.

## 7. Compare the results

After both jobs complete:

```bash
.controller-venv/bin/python tools/compare_regression_jobs.py "$BASELINE_ID" "$RECOVERY_ID"
```

Expected result: `pass: true`, identical model/optimizer/scheduler state, equal
regression MSE, and equal predictions.

This walkthrough demonstrates single-worker CPU training and checkpoint recovery
after a software reboot. Multi-worker training and recovery are later steps.
