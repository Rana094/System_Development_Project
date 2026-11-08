# Phase 7.3 — durable staging cancellation, expiry and recovery

Date: 9 November 2026, Asia/Dhaka. **Local implementation/acceptance only; no live
activation.** One subphase in this turn. Start: `main`, commit
`68f59e9e72d8ecb2e1b5a98400be2e22051eafc4`. The 14 modified/untracked 7.2 files
were already present. Their exact contents and all inventoried first-party files
were copied to `/tmp/hybrid-7-3/before/` with SHA-256 in `before.json` before edits.
No stash, reset, index staging or commit. The previous delivered scope was 7.2
local restart-mode integration, not production enablement.

## Implemented behavior and files

New files:

* `hybrid_storage/staging_journal.py`: one service-lifetime flock, protected LOCAL
  private directory, bounded canonical v1 JSON, verified identities, unique
  temporary writes, fsync/atomic replacement. Copy intent is durable before
  fetching; possibly-launched intent is durable before Popen. Invalid records or
  conflicting ownership fail closed. READY on disk is never restart permission.
* `hybrid_storage/cleanup_schema.py`: explicit additive controller migration and
  read-only plan; cleanup history retained, schema/checksum mismatch refused.
* `controller/staging_recovery.py`: signed lifecycle queries/renewals/cancellation,
  exact-attempt/group reconciliation, restart handling, delayed offline cleanup
  tracking and atomic controller reservation/history/job closure.
* `tests/test_staging_recovery.py`: new local fault, concurrency and recovery tests.
* This report and `28-staging-recovery-evidence.json`.

Existing files changed relative to the saved **7.2 contents**, not just Git HEAD:

* `agent/staging.py`: durable intent, signed control endpoint, worker watchdog,
  restart aborts, copy interruption, exact completion acceptance, quiescence-only
  terminal cleanup. Cache copy no longer holds the agent slot lock. Cancel can
  persist while a materializer is copying. Short authority updates serialize with
  start/cancel/renew, so old cleanup cannot overwrite a newer generation.
* `agent/agent.py`: record launch after existing Popen/lease attach; watcher accepts
  success only while the exact lease and durable attempt remain valid. Existing
  resource/slot fields report BUSY while staging/cleanup is unresolved, including
  cases with no `current_job`. No new student UI state or metrics page.
* `hybrid_storage/staging_protocol.py`: domain-separated authenticated control
  action on the existing worker HTTP channel; inventory credentials reused.
* `hybrid_storage/cache_fetch.py`: interrupt current owned helpers without closing
  future admission; reap detached producers, prove the cache-wide inherited flock
  is free, then remove only protected managed `.part` scratch names. Unknown files
  remain; symlinks, foreign/linked inodes fail closed.
* `hybrid_storage/workspaces.py`: terminal cleanup of only the recorded unpublished
  `.building-<identity>-<nonce>` tree after quiescence. Preflight the entire tree;
  reject unknown entries, links and unexpected outputs. Every published workspace,
  output and checkpoint remains untouched. No generic recursive deletion command.
* `hybrid_storage/staging_lifecycle.py`: RUNNING publication barrier reuses existing
  manifest/ACL/history/reservation/lease validation for all actual ranks.
* `controller/staging_dispatch.py`: require the explicit cleanup migration, track
  this runtime's active stages, reuse recovery coordinator and signed running
  renewal with persisted possibly-delivered deadlines.
* `controller/execution_leases.py`: default renewal never sends unsigned requests
  for NFS jobs; current result access checks all-rank live ownership. A controller
  SQLite write transaction serializes final NFS result publication with cancel,
  retry and replacement writers. Legacy fencing/renewal remains on its old path.
* `controller/job_artifacts.py`: retain existing SSH/rsync/log flow and formats;
  run final result/log replacement inside the NFS publication barrier. This is
  neither a new artifact transfer API nor NFS writable checkpoint access.
* `controller/job_store.py`: NFS completion requires the authenticated coordinator
  path, all-rank quiescent proofs and current execution fences. Complete the exact
  bound history attempt, revoke renewal and close side state atomically. Legacy
  completion uses its previous semantics. Cleanup closes scheduled cancellations
  as well as running attempts without rewriting previous-attempt history.
* `controller/dispatcher.py`: optional runtime reconciliation for scheduled,
  running and cancelling NFS jobs. Without the runtime, block renewal and withhold
  legacy unsigned reconciliation. A retryable reconciliation error retains state.
* `tests/test_staging_dispatch.py`: explicitly install the disposable migration,
  suppress background threads in fixtures, provide mock containment callbacks and
  run actual 7.3 terminal reconciliation for warm second-job acceptance. No manual
  clearing of `_ready` as a substitute for terminal cleanup.
* `hybrid_storage/README.md` and the appended implementation progress record.

`agent/custom_runner.py`, `controller/storage_selection.py`, the 7.2 report/evidence,
`tests/test_job_environments.py`, configuration and unrelated prior files are
preserved from the starting working tree. The complete scoped diff and preservation
hashes are recorded in evidence.

## API, SQLite and security changes

New signed metadata POST: `/jobs/staging-control`, body
`{action, stage_id, plan, lease}`, action `status|cancel|renew`. Reuse the existing
bounded HMAC protocol/worker credential, with method/path/direction domains,
request freshness/nonce and replies bound to exact request bytes. Reply includes
only stage/plan/fence identity, phase, outcome and `pins_released`; no raw cache,
NFS, output path or credential. No new artifact HTTP download/upload protocol.
An unknown/mismatched status is not completion or a quiescence proof. Cancellation
can install a durable exact-attempt tombstone before the stage request arrives.
Old/expired control identities can cancel/query their own history; they cannot
renew, revive execution, overwrite a new generation or terminate another owner.

Worker journal phases: STAGING → READY → LAUNCHING → RUNNING → ABORTING → TERMINAL.
Failures/cancel/expiry/restart can enter ABORTING earlier. Terminal tombstones and
workspace history are retained; intent is persisted before external side effects.
The two records may disagree after a crash; retries reconcile conservatively.
Neither a vanished PID nor an empty `_ready` dictionary proves safe cleanup.

`StagingService(..., isolate, quiescent=None, terminate=None)` permits staging-only
construction. **Launch refuses missing containment callbacks.** The reviewed
`terminate(identity, record)` must be idempotent and stop the exact isolated
attempt, including escaped descendants; `quiescent(identity, record)` must prove
the whole containment is stopped. Both receive the exact identity/lease record.
A known live tracked process overrides a positive proof. The cache producer must
also have unwound/reaped, with the inherited fetch-slot flock free. A different
worker kernel boot proves old processes are gone. For never-launched records,
durable prelaunch intent plus producer quiescence is sufficient. No unsafe default
callback is provided. Production OS/UID/network containment remains unimplemented
by these adapters and is not established by mocked callbacks in tests.

`install_staging_service(..., watchdog=True)` explicitly starts a 250 ms watchdog;
normal agent bootstrap still does not call it. Tests normally disable autonomous
threads and call `tick`; a dedicated test runs the real watchdog thread. The
watchdog fences expired/cancelled authority and retains BUSY/pins on uncertainty.
Copy deadlines and lease limits remain; no unlimited long-staging renewal promise.
Short signed renewals persist the greatest possibly-delivered controller deadline
before HTTP and never revive expired, blocked, cancelled or wrong-boot authority.

New explicit migration: version 1 of `hybrid_cleanup_migrations` and
`hybrid_cleanup_ranks`, one index and four triggers. Composite FK references the
existing `(stage_id, rank)`. Proof phases are PENDING → QUIESCENT or LEASE_FENCED;
LEASE_FENCED can later become QUIESCENT, never regress. Identity/history cannot be
deleted/rewritten. Existing migration checksums, SQLite `user_version`, job enums,
manifest versions and checkpoint formats do not change. Dedicated LOCAL FULL/FK
connection required; no automatic startup migration, backfill or SQLite on NFS.
Use `cleanup_schema.migration_plan(db)`/`migrate(db)` only in a separately reviewed
operator procedure after existing base/staging migrations. No live DB was opened
or migrated in this turn.

## Failure and terminal paths

* **Cancel during copy:** cancellation tombstone and ABORTING persist while copy
  holds its own materializer lock, not the agent slot lock. Interrupt known helpers;
  authorization checks stop further publication/materialization. The in-flight
  operation must unwind/reap before unpin/scratch cleanup. No late ready/start ACK
  can reopen it. Controller cancellation is also rechecked before READY/RUNNING.
* **Controller restart:** this runtime does not own an old STAGING/READY intent;
  abort it and contact every recorded rank. Already RUNNING groups are queried
  through signed control and require current all-rank fences. No repeated launch
  after an uncertain start. The pre-begin gap is rechecked under a write lock;
  only after issued leases fence (or none were ever issued) can reservations close.
  No stage RPC precedes begin in this API. Prior history rows are not rewritten.
* **Worker restart/crash:** journal lock prevents simultaneous service owners.
  Reconstructed nonterminal records become aborted, never READY launch grants.
  An orphan copy helper may retain its inherited flock; keep pins until it exits.
  Possibly-launched records without a containment proof remain pending. The existing
  lease guardian remains independent of Flask and enforces bounded execution.
* **Worker outage:** block renewal for the entire group. Keep reservations until
  every rank gives a signed terminal/unpinned acknowledgment or satisfies existing
  conservative lease/clock-skew/stop-grace/controller-boot replacement bounds.
  Lease fencing allows replacement but **does not claim remote unpin**. Persist
  LEASE_FENCED; `runtime.tick()` retries old cleanup on return, even after a new job
  attempt exists. Worker BUSY reporting prevents unresolved local cleanup being
  advertised as an empty slot. No shortened lease assumptions or fabricated ACKs.
* **Completion/late results:** accept zero exit only for the durable RUNNING attempt
  before local expiry/cancel. Controller rechecks current retry, owner/ACL, pinned
  inputs/history, all-rank reservations and live leases before publication and
  completion. Final publication holds SQLite's write lock against cancel/retry;
  unreserved/stale/expired/blocked producers are refused. Actual result transfer
  stays in the existing trusted collector. Current-attempt completion closes the
  exact history ID and releases the complete group atomically.
* **Cleanup failures/full disk:** persist intent before release; retain pending
  reservations/pins when proof or release fails. A tombstone with unreleased pins
  retries idempotently. Only managed scratch is removed; complete input objects,
  published workspaces, outputs/checkpoints, journals and unknown files remain.

Normal startup/config still defaults to legacy/OFF/resource. No credential
provisioning, SSH, Pi probe/deploy, sudo, `/etc`, systemd, firewall, export or mount
change. New NFS execution remains restart-mode custom jobs; NFS checkpoint jobs
stay held pending trusted publisher integration. Checkpoint-enabled legacy jobs
keep their existing formats/APIs. Inputs exceeding local capacity still fail
closed with no direct-NFS training fallback. Failed/cancelled NFS logs/results
remain on the worker if strict publication fencing withholds collection; student
staging display/metrics and any separate fenced log display remain 7.4.

## Tests and manual verification

Exact commands, per-run counts/log hashes, source hashes and differences are in
[28-staging-recovery-evidence.json](28-staging-recovery-evidence.json). Baseline
before edits: **67 pass, 4/4 files, zero failures/skips**. The broad 13-file run
passed **240 methods, zero failures/skips**. Later focused/final results are
**89 pass in six files**, then **64 pass in three files**, and final expanded
**34/34 recovery tests pass**, all with zero failures/skips. These snapshots
include progressively added checks; repeated test methods are not added together.

Coverage includes copy cancellation with actual pins; pre-delivery cancel;
expiry with no controller request and real watchdog; controller/worker restart;
offline fencing with deferred remote unpin; current signed renewal; all-three
logical-rank cancellation; missing/partial migration; interrupted/concurrent FULL
SQLite migration; corruption, symlinks and disk-full unpin retry; orphan fetch-lock
ownership; scratch-only cleanup; published output/checkpoint preservation; stale
success; SQLite publication/cancel serialization; exact history closure; replayed
old cleanup and mandatory containment providers.

Tests use real local SQLite/journals, hashes, private copies, Flask test clients,
copy helper subprocesses and selected real threads. A dedicated test launches the
existing lease guardian with a disposable child/grandchild in a new process group,
then proves that group stopped before releasing pins. Other trainers/isolation
are mocks. The process-group proof in this disposable test does **not** establish
production tenant isolation against malicious code escaping containment.

Initial harness runs found: the old warm fixture bypassed new durable cleanup;
watchdog pin count reached zero just before journal fsync; a second reconcile had
already reached TERMINAL after fast real guardian shutdown; the shared watcher
mock suppressed a deliberate ThreadPoolExecutor concurrency test. Corrected the
fixtures/assertions, restored actual threads for that test and interrupted only
that hung local test session. No failure was masked or converted to a skip.

Safe reproducible commands from the project directory:

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_staging_recovery.py test_staging_dispatch.py test_execution_ownership.py test_queue_controls.py
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python -m hybrid_storage.config
git diff --check
git status --short
```

Expected: 81 selected methods pass without skips; checker reports legacy/resource
and both new flags false; whitespace check has no output. Review this turn's
**scoped patch** rather than ordinary Git diff alone, which also contains prior
uncommitted 7.2 work. The preserved 7.2 report documents sandbox EPERM in real DDP
socket creation; those known restricted suites were not rerun or claimed passing
here. No hardware, live NFS outage, network throughput, OS isolation or whole-repo
pass is claimed. All pi01/pi02/pi03 validation remains required before deployment;
the earlier pi01 exception is not a new waiver. The controller/NFS server remains
a single point of failure.

## Rollback, limits and acceptance

The scoped review/rollback artifact is `/tmp/hybrid-7-3/7.3.patch`, relative to
the saved starting **working-tree contents**. It retains all earlier 7.2 work.
Read-only applicability check:

```bash
git -C "/home/rana/Projects/System Project/System_Development_Project" apply --reverse --check /tmp/hybrid-7-3/7.3.patch
```

After reviewing current edits, the same command without `--check` reverses only
this turn if no later changes conflict. Do not reset/clean/restore to HEAD: that
would erase prior uncommitted work. No live DB/config rollback is needed. If this
is later deployed, first stop/quiesce exact producers and reconcile pins/leases;
retain cleanup/history tables and all runtime inputs, jobs, results and checkpoints.
Do not drop tables or toggle a flag as a substitute for fencing uncertain owners.

Residual deployment prerequisites: reviewed service/trainer isolation and scoped
termination/quiescence adapters; explicit operator migrations/credential bootstrap;
actual all-three-Pi validation. Very long staging remains lease bounded. Journal,
workspace and cleanup history are retained; bounded history compaction/GC is not
introduced. Strict fencing may choose a retry over a late otherwise successful
result. Dedicated prior 5.4 acceptance is not inferred from these tests.

**7.3 local lifecycle acceptance: PASS; recorded final checks passed. Production
enablement, NFS checkpoint execution and overall Phase 7 acceptance: pending.
STOP before 7.4.**
