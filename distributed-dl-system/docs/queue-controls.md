# Step 5: fair student queues and job controls

The scheduler shares workers between students. By default, a student can have
one active job and twenty pending submissions. An active job includes an
uncertain `SCHEDULED` start and a `CANCELLING` attempt, so lost network contact
cannot bypass the quota. Received, staged, prepared, queued and held submissions
all count toward the pending limit. Completed, failed and cancelled jobs remain
in history and do not use a pending slot.

## Run the complete live demonstration

On the controller, with all three Pis powered on and idle:

```bash
cd '/home/rana/Projects/System Project/System_Development_Project/distributed-dl-system'
.controller-venv/bin/python -u tools/check_step5.py --nodes pi01 pi02 pi03
```

Stop a manually running dispatcher with Ctrl+C before starting this tool. The
tool holds the normal dispatcher lock and calls the production dispatch loop;
it does not leave a background controller running. Existing active jobs must
finish first. The demo uses the default one-active-job limit for its student IDs.

It submits six jobs: Alice's three jobs, Bob's job, Carol's job and Dave's held
job. All executable jobs use the real 30-second PyTorch MLP from step 4. The
script proves that:

1. Alice, Bob and Carol each get a worker, even though Alice submitted three
   jobs before the others. Alice's remaining jobs wait for her active quota.
2. A held job does not run. Hold and release work, and a mismatched owner is
   rejected by the control command.
3. Alice's running job performs a real optimizer step before cancellation.
   Its reservation stays held until the worker confirms exit.
4. Alice's second job starts after cancellation; her third still waits for her
   quota, then starts when her second completes.
5. A replay of Alice's cancelled start request is rejected by the worker.
6. Dave's held job is cancelled without starting a process. Four other jobs
   complete, with saved models, finite tensors and matching Adam step counters.

Expect these messages:

```text
CONTROLS VERIFIED: hold, release, hold again; wrong owner rejected.
FAIRNESS VERIFIED: alice, bob and carol each have one worker; alice backlog waits.
CANCEL REQUESTED: running allocation retained until worker confirms exit; held job cancelled immediately.
REPLACEMENT VERIFIED: alice-1 CANCELLED; alice-2 RUNNING; alice-3 still waits for its owner quota.
TRAINING VERIFIED alice-2: ... Adam steps; ...
PASS: fair owner turns, active quota, hold/release, owner checks, real running cancellation, next-job dispatch and completed model artifacts.
```

The first environment preparation can take several minutes. Subsequent jobs
reuse native environments and dataset caches. If the demo is interrupted, run
the ordinary dispatcher to monitor its tracked jobs and finish cancellation.

Watch the queue in a second terminal:

```bash
cd '/home/rana/Projects/System Project/System_Development_Project/distributed-dl-system'
watch -n 2 '.controller-venv/bin/python controller/queue_ctl.py list'
```

## Use the controls yourself

Submission, environment preparation and enqueue commands are unchanged; follow
[the step 4 guide](scheduling.md). Start the ordinary dispatcher in one terminal:

```bash
cd '/home/rana/Projects/System Project/System_Development_Project/distributed-dl-system'
.controller-venv/bin/python -u controller/dispatcher.py
```

To practice hold/release on a first job, enqueue it before starting the
dispatcher, hold it, then start the dispatcher. It stays held until release.

In another terminal, replace the owner and job ID with your submission's values:

```bash
cd '/home/rana/Projects/System Project/System_Development_Project/distributed-dl-system'
OWNER='student01'
JOB_ID='job-YOUR-ID'

# Inspect this student's unfinished submissions and queue explanations.
.controller-venv/bin/python controller/queue_ctl.py list --owner "$OWNER"

# Include completed and cancelled submissions.
.controller-venv/bin/python controller/queue_ctl.py list --owner "$OWNER" --history

# Hold only a QUEUED job; this does not suspend an active training process.
.controller-venv/bin/python controller/queue_ctl.py hold --owner "$OWNER" --job-id "$JOB_ID"

# Release a HELD job while retaining its original queue timestamp.
.controller-venv/bin/python controller/queue_ctl.py release --owner "$OWNER" --job-id "$JOB_ID"

# Cancel a pending job immediately, or request a tracked stop for an active job.
.controller-venv/bin/python controller/queue_ctl.py cancel --owner "$OWNER" --job-id "$JOB_ID"

.controller-venv/bin/python controller/job_status.py "$JOB_ID"
```

A running cancellation first reports `CANCELLING`. The dispatcher requests
SIGTERM from the original worker, checks its attempt token and confirms exit
before marking `CANCELLED` and releasing the reservation. Stops are retried if
contact is lost. After ten seconds, the dispatcher can request SIGKILL. The
worker also cleans up remaining members of the launcher's process group before
advertising an idle slot, including ordinary children that ignore SIGTERM.

Cancellation intent is stored in SQLite and survives controller restart. A late
start acknowledgment, completion poll or failure retry cannot overwrite it.
Workers persist cancellation records and check them again under the launch lock.
If an uncertain start is missing, the dispatcher installs this record before
releasing its reservation. Delayed start requests and replays stay blocked across
agent restart.

Confirmed stopped attempts have their available logs and partial result files
collected on a best-effort basis. Cancellation does not restore a custom model;
submitted PyTorch checkpoint recovery uses the [step 6 SDK](checkpoint-integration.md).

## Configure limits for the lab

The controller reads `config/queue_policy.json` each scheduling pass. It defaults
to:

```json
{
  "max_active_per_owner": 1,
  "max_pending_per_owner": 20,
  "aging_seconds": 300,
  "owners": {}
}
```

An administrator can allow a particular student two active jobs:

```json
"owners": {
  "student01": {"max_active_per_owner": 2, "max_pending_per_owner": 30}
}
```

Insert that object in the complete policy file. Limits must be positive integers;
malformed policy is rejected. View the effective policy:

```bash
.controller-venv/bin/python controller/queue_ctl.py policy
.controller-venv/bin/python controller/queue_ctl.py policy --owner student01
```

Lowering an active limit affects future placement; it does not kill existing
training. Lowering the pending limit affects new submissions. Limit checks and
job insertion or worker reservation share a write transaction, preventing
concurrent requests from exceeding the configured quota.

## Fair scheduling and code blocks

`queue_policy.py` groups queue entries by owner. It gives each eligible owner a
turn, favors owners with fewer active jobs, and uses persisted last-dispatch time
so repeatedly submitting jobs cannot reset service history. Within an owner's
turn, jobs are considered in enqueue order. After the aging threshold, older
waiting owners are promoted ahead of newer waiting owners. Jobs that cannot fit
currently available workers stay queued while other fitting work can run. Aging
cannot supply unavailable hardware or preempt an already running job.

`job_store.py` persists service history, quota checks, hold/release transitions
and cancellation intent. `dispatcher.py` recomputes fair turns after each
placement, reconciles stop requests and retains reservations during uncertainty.
`queue_ctl.py` provides JSON queue inspection, policy inspection and owner-scoped
controls. `agent.py` validates attempt tokens for controller stop requests and
cleans up tracked process groups. `test_queue_controls.py` tests quotas,
concurrent submissions, ordering across restart, races, offline cancellation and
real cleanup of a child that ignores termination.

Existing bundled benchmark and checkpoint-recovery jobs keep their legacy
concurrency for compatibility. These student limits apply to general custom
submissions. The step 4 capacity demonstration now submits jobs with distinct
student IDs so it also works with the default per-student limit.

The prototype CLI trusts the supplied owner label; comparing it with the stored
owner prevents accidental cross-owner commands but does not authenticate a
student. An authenticated portal or OS identity mapping is needed before exposing
these controls directly to students. Policy editing belongs on the trusted
controller host. Process-group cleanup is not an OS sandbox for hostile code.

Run the isolated queue-control checks without accessing the Pis:

```bash
.controller-venv/bin/python -m unittest discover -s tests -p 'test_queue_controls.py' -v
```

The tests use a temporary database and include a real process-group cleanup
check; they do not submit jobs to the live controller database.
