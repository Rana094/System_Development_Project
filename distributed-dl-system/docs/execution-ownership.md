# Step 10: leases and execution ownership

The controller now fences unreachable attempts before replacing them. A separate
worker guardian stops expired execution, even if Flask cannot respond. The
controller rejects old checkpoint producers, delayed starts/renewals and stale
result transfers. This applies to both single-worker and DDP submissions.

## What changes for students

Continue using the existing job definitions and checkpoint SDK. Students do not
choose lease IDs or generations. The controller supplies them during dispatch.
The SDK checks ownership before publishing, and DDP archives include the producer
fence for every rank. A replacement still restores the last centrally committed
checkpoint and uses the same training cursor.

## 1. Run the updated dispatcher

From the controller project directory, stop an older dispatcher with Ctrl+C and
start the updated one in terminal A:

```bash
.controller-venv/bin/python -u controller/dispatcher.py
```

Lease settings are in `config/execution_leases.json`:

```json
{
  "enabled": true,
  "duration_seconds": 30,
  "renew_every_seconds": 5
}
```

The duration must be 10–60 seconds, with renewal less than half that duration.
Normal execution continues while leases are renewed. A lost renewal reply blocks
further renewal for the attempt: the controller remembers the greatest deadline
that might have reached the worker. It never assumes a lost reply means the
worker did not accept the renewal.

## 2. Submit and prepare a job

Use the single-worker regression job for the clearest expiration demonstration:

```bash
.controller-venv/bin/python controller/submit_job.py --spec examples/job.integration.json --owner student01
JOB_ID='job-ID-PRINTED-ABOVE'
.controller-venv/bin/python controller/prepare_job.py --job-id "$JOB_ID" --nodes pi01 pi02 pi03 --profile pytorch-cpu
.controller-venv/bin/python controller/queue_job.py --job-id "$JOB_ID"
.controller-venv/bin/python controller/job_status.py "$JOB_ID"
```

Wait for a nonzero central checkpoint step. The `Node` field identifies the
training Pi. Status now shows `Execution ownership`: attempt, worker generation,
lease deadline, blocked renewal and fencing state.

Run the same job uninterrupted first if you want a baseline comparison. Keep its
ID in `BASELINE_ID`; keep the interrupted job's ID in `RECOVERY_ID`.

## 3. Disconnect the active worker

Disconnect that Pi's Ethernet cable while training. Keep the controller and the
other Pis connected. Do not power the Pi off: this test shows that a worker
which loses controller contact can still stop its own expired training attempt.

Repeat in terminal B:

```bash
.controller-venv/bin/python controller/job_status.py "$RECOVERY_ID"
```

Expect blocked renewal, a retained old reservation while the lease might still
be valid, then fencing/retry after expiration plus a five-second safety margin.
Polling, checkpoint transfer and environment checks add to that time. The job
resumes on another compatible Pi without waiting for the isolated Pi to return.
For DDP, the controller stops/fences the entire old group and allocates a complete
replacement group. It needs two compatible reachable workers for a two-rank job.

Reconnect the cable after the replacement is running. The old worker rejoins
with an idle slot. Its expired lease cannot be renewed or replayed into another
execution. Its old checkpoint and results cannot replace the new attempt's data.

The automated verification used a narrower fault: it dropped only controller
requests to the worker's API port 5000. SSH and peer training connections remained
available. This let the old learner keep computing until ownership expired,
while its termination and rejected replay requests could be inspected directly.
Temporary rules were restored after verification.

## 4. Compare completion

After both single-worker jobs are completed:

```bash
.controller-venv/bin/python tools/compare_training_jobs.py "$BASELINE_ID" "$RECOVERY_ID"
```

Expect a nonzero recovered start step, exact model/optimizer/scheduler agreement
and `pass: true`. See [the DDP submission and comparison commands](distributed-training.md#step-9-coordinated-ddp-checkpoint-recovery)
for the group version. For an easy DDP disconnect demonstration, use 80 epochs
in the job definition so the isolated training cannot finish before lease expiry.

## How ownership is protected

- `controller/execution_leases.py` issues a unique lease ID and a monotonically
  increasing generation for each worker allocation. Attempt tokens identify the
  job retry; generations are per worker, so values on different workers need not
  be equal or increase relative to each other.
- `agent/execution_lease.py` persists the worker's highest accepted generation,
  exact owner and deadline. Equal generations can only renew the same live
  owner; expired generations cannot be revived. Deadlines use both wall time
  and Linux BOOTTIME, which includes suspend.
- `agent/lease_runner.py` checks ownership every 0.2 seconds and kills the managed
  process group on expiry. The guardian is independent of HTTP and SDK calls.
  Its PID/start identity is durable; restarting the API reaps a surviving
  guardian before advertising an idle slot.
- The checkpoint SDK checks its lease before publication. Central collectors
  check producer fences and current attempt ownership before replacing files.
- Results are copied into a private staging directory and checked again before
  publication, so a transfer begun by an old owner cannot overwrite a new result.

Workers must advertise protocol 1 and have synchronized clocks within two
seconds to receive protected submissions. The controller waits for the deadline
plus two seconds of clock tolerance and three seconds of termination grace.
Within a controller boot, the wait also uses BOOTTIME to avoid early replacement
following a wall-clock jump. After a controller-machine reboot, an unreachable
old lease gets a conservative maximum-lease quarantine of 65 seconds from
controller startup; an explicit termination confirmation can resolve it sooner.

These checks target trusted Linux workers and training descendants that remain
in the managed process group. They do not provide authenticated student login
or a hostile-code sandbox. Preserve `controller/jobs.db` and each worker's
`worker-storage/execution-authority.json`; their generations are operational
state. Keep worker services and clocks functioning for the bounded lease wait.

## Local checks

```bash
.controller-venv/bin/python tests/test_execution_ownership.py -v
.controller-venv/bin/python -m py_compile agent/execution_lease.py agent/lease_runner.py controller/execution_leases.py
```

The tests exercise autonomous descendant termination, durable restart fencing,
clock bounds, uncertain renewal, original failure reason preservation, and stale
checkpoint/result rejection. Live Pi evidence is retained in each verification
job's `results/ownership-verification.json` and the controller log
`logs/step10-network-verification.log`.
