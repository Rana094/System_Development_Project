# Phase 8.4 — local checkpoint fault acceptance

9 November 2026, Asia/Dhaka. **8.4 local PASS. STOP.** Production enablement,
physical NFS faults, power recovery and pi01/pi02/pi03 validation remain pending.
This subphase adds tests and documentation only. No runtime/API/schema/config
change was needed. Legacy/OFF/resource defaults and the explicit reviewed
checkpoint bootstrap remain unchanged. The controller, its LOCAL SQLite database
and authoritative checkpoint disk remain a single point of failure; these tests
do not add controller HA or storage replication.

The starting Git root was the application directory's parent, branch `main`, HEAD
`38d31c22bceb990166ef873f2be60b6ded1417f5`. Existing uncommitted 8.1/8.2/8.3 work
was preserved. Before edits, 289 inventoried source/document files and their
SHA-256 values were saved to `/tmp/hybrid-8-4/before` and `before.json`. No stash,
reset, commit, service restart, SSH/Pi access, live DB import/migration or NFS/
exports/fstab/firewall mutation occurred. Faults affect disposable files and
native child processes owned by the tests only.

## Changes and interfaces

| File | Change |
|---|---|
| `tests/test_hybrid_checkpoint_faults.py` | New: 16 coordinator/publication/restore fault cases with real SQLite/files, signed Flask HTTP and a mocked outer training launcher |
| `tests/test_hybrid_checkpoint_ddp_resume.py` | New: one real three-rank CPU Gloo SIGKILL/resume integration case through committed publication and private authenticated restore |
| `tests/README.md` | Append safe test commands, local socket requirements and fault scope |
| `hybrid_storage/README.md` | Append acceptance/report link |
| `docs/storage/implementation-progress.md` | Append 8.4 status and evidence boundaries |
| This report and `33-checkpoint-fault-acceptance-evidence.json` | Commands, actual results, file hashes, acceptance matrix and rollback |

All pre-existing runtime, schema, configuration, workload and test files remain
byte-identical to the start of this turn. Prior README/progress text remains an
unchanged prefix. No endpoint, transport protocol, feature flag, DB column/table,
archive format, SDK restore behavior, access rule or migration checksum changed.
SQLite stays on LOCAL disk; checkpoints remain controller-published immutable
commits, and workers retain read-only NFS input access through trusted staging.

## Fault matrix and observed behavior

Names below refer to `CheckpointFaultTests` in the new fault suite unless another
suite is specified. An injected exception is identified as an injection, not a
claim of an actual cable, disk, host or Pi failure.

| Required case | Evidence | Actual assertion |
|---|---|---|
| Worker crash | New real three-rank test; existing `test_group_checkpoint.py::test_rank_dies_during_ack_previous_bundle_remains` | SIGKILL an owned native training rank. Reap all old ranks before replacement. A complete prior cut survives; an unacknowledged group bundle does not replace it. |
| Worker connection loss | `test_worker_connection_loss_retains_reservation_until_fenced_then_late_result_rejected` | Inject signed-control timeout. Retain reservation before expiry; after simulated elapsed wall/BOOTTIME on controller and worker, fence and requeue. Late success/result publication is rejected. |
| Interrupted checkpoint upload | `test_interrupted_upload_and_real_connection_reopen_keep_only_previous_commit` | Signed GET body yields 80 bytes then disconnects. Candidate remains REJECTED, previous COMMITTED head survives real connection reopen, and recovery freezes that head. |
| Interrupted restore | `test_restore_connection_dies_after_partial_read_no_receipt_or_private_file_accepted`; `test_partial_restore_body_and_no_space_never_publish_private_resume_or_launch` | Inject disconnect after 64 copied bytes, truncated body and ENOSPC admission. No accepted resume file/receipt or training launch. |
| Lost restore/start acknowledgement | `test_lost_restore_reply_and_controller_restart_never_blindly_launches`; `test_lost_start_reply_on_recovery_fences_instead_of_starting_twice` | Real Flask operation succeeds, then transport loses ACK. Reopened controller aborts the ambiguous attempt; it does not replay a launch. |
| Corrupt file | `test_corrupt_committed_final_never_publishes_results_or_completes`; pre-edit recovery suite corrupt-source case | A corrupted committed file cannot seed recovery or satisfy final completion. Three failed final-checkpoint polls enter cleanup/retry without publishing results. |
| Stale execution result | `test_ddp_stale_rank_checkpoint_result_rejected_for_entire_group` and expired-owner test | Previous-attempt DDP archive and result publication cannot advance current head/completion. Whole-group checkpoint identity/fences remain required. |
| Double recovery | `test_two_sqlite_connections_competing_recovery_reserve_and_launch_once` | Two native threads, separate SQLite connections and a selection barrier compete for the same retry. Exactly one reservation, stage attempt and launch; reopened controller records one resume event. |
| Controller process crash | `test_sigkill_controller_publisher_process_after_rename_preserves_head_on_reopen` | A separate native publisher pauses after immutable rename, before COMMITTED/head update. SIGKILL only that child, reopen SQLite; PUBLISHING orphan is not promoted or selected. |
| Controller restart gaps | `test_process_death_after_checkpoint_rename_does_not_promote_orphan_on_restart`; `test_restart_after_sdk_proof_event_before_running_is_idempotent` | Inject hard-crash boundary and reopen actual DB connections. Existing source is retained; already-confirmed restore event is not duplicated when RUNNING is reconciled. |
| Controller host reboot fencing | `test_controller_host_boot_mismatch_never_renews_old_owner_or_reuses_live_slot` | Simulate different boot identity and offline worker. No old-owner renewal; preserve slot through the new-host guard interval, then replace only after safe expiry. No actual host reboot. |
| Missing old checkpoint | `test_missing_old_checkpoint_is_not_hidden_by_newer_incomplete_or_legacy_file` | Unlink only a disposable committed file. Reject recovery before reservation/launch; do not adopt a plausible mutable legacy file or restart from zero. |
| NFS outage | `test_nfs_outage_warm_recovery_uses_zero_missing_bytes_and_cold_replacement_fails_closed` | Warm verified cache causes zero missing-input transfers. Inject ENETUNREACH for cold pi02 fixture: staging fails closed, launches none, retains previous checkpoint. No actual mount/network disruption. |
| Incomplete DDP checkpoint | `test_incomplete_new_ddp_bundle_never_displaces_last_recoverable_commit` | Three-rank bundle with only two acknowledgements is rejected; recovery still selects the previous complete commit. |
| Genuine saved-state resume and final metrics | New real three-rank test; existing single-worker CPU SDK tests | Load actual tensor state through the existing SDK. Only a current-attempt completed COMMITTED final bundle permits collection/completion. One DDP_RESUMED event, final global step 6, three result-collection calls. |

## Genuine three-rank experiment

The new native test uses three distinct disposable worker agents, credentials,
verified caches and private workspaces named pi01/pi02/pi03. Agent HTTP is handled
by Flask test clients and the outer agent launcher/OS-isolation callback is mocked.
The training processes themselves are real CPU PyTorch DDP/Gloo ranks. This is
not a physical cluster or production isolation test.

1. Run two independent uninterrupted six-update groups as baseline/control.
2. Start a leased hybrid job and real three-rank training. Save complete cuts at
   global steps 1 and 2. Pause all ranks after cut 2 and publish its actual archive
   through authenticated worker GET and the controller's COMMITTED transaction.
3. SIGKILL rank 1; kill/reap remaining owned ranks before any replacement. Feed
   failed exits through actual worker lifecycle records and coordinator cleanup.
4. Reopen the controller SQLite connection and registry/publisher/coordinator.
   Reserve a new attempt, revalidate input bindings/fresh leases, stage all three
   private checkpoint copies via authenticated POST. Input cache reuse stages
   zero additional NFS bytes on every rank. Controller stays SCHEDULED awaiting
   SDK proof.
5. New native ranks load those private files with the unchanged SDK, restore
   application generator state and continue from step 3 through step 6. Actual SDK
   ACK files lift the controller restore barrier. Publish final complete bundle,
   prove quiescence and call existing result collection (transport mocked) for
   all three ranks. Only then record COMPLETED.

The native test uses a private local FileStore for rendezvous and
`GLOO_SOCKET_IFNAME=lo`. No production rendezvous protocol is changed. Only
loopback Gloo sockets are needed. The existing two-rank legacy suites use their
existing local TCP rendezvous. All subprocesses are reaped on success/failure;
there is no persistent listener or service installation.

**Numeric criteria:** immediately loaded model/optimizer/scheduler/Torch RNG
state must match the saved cut exactly. Final Torch/Python/NumPy RNG, application
generator, progress, rank identity, counters, scheduler and other non-floating
state must match the uninterrupted baseline exactly. Only final floating tensors
under model/optimizer use `atol=1e-8, rtol=1e-7`, with shape and dtype unchanged.
The same criterion checks the independent uninterrupted control.

The final FileStore run observed one differing FP32 Adam `exp_avg_sq` tensor per
rank, maximum absolute difference `1.862645149230957e-9`. All model tensors and
other checked state were exact in that run; the two uninterrupted runs were exact
to each other. The source load checks were exact. The initial all-bitwise final
comparison failed on this value; it was not reported as a pass. The cause of the
small post-restore floating difference was not isolated, and general bitwise
three-rank reproducibility is not claimed. The measured difference satisfies the
explicit narrow floating criterion. Existing single-worker and two-rank exact
comparison tests remain unchanged and passed. This is a correctness experiment,
not a throughput benchmark or a guarantee for different hardware/workloads.

## Commands and actual results

Run from the application directory. Every command uses the isolated runner,
which excludes live DBs/runtime roots and copies source to a temporary directory.

```bash
# Pre-edit baseline, ordinary sandbox; 25 + 18 = 43 tests.
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_checkpoint_publication.py test_hybrid_checkpoint_recovery.py
# Existing real two-rank local SDK checkpoints/recovery; 8 + 7 = 15 tests.
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_group_checkpoint.py test_group_recovery.py
# Final 16 fault tests + legacy executable script + 17 config + 4 single-worker SDK.
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_hybrid_checkpoint_faults.py test_checkpoint_recovery.py test_storage_config.py test_custom_checkpoint.py
# Final real three-rank saved-state experiment; 1 test.
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_hybrid_checkpoint_ddp_resume.py
git diff --check
```

| Run | Pass/fail/skip | Actual log |
|---|---|---|
| Pre-edit baseline | 43 PASS, 2/2 files; zero unittest errors/failures/skips | `/tmp/hybrid-8-4-baseline.log` |
| Existing real Gloo | 15 PASS, 2/2 files; zero errors/failures/skips | `/tmp/hybrid-8-4-gloo.log` |
| Final faults/legacy/config/single SDK | 37 unittest cases plus legacy executable script PASS, 4/4 files; zero unittest errors/failures/skips | `/tmp/hybrid-8-4-final.log` |
| Final real hybrid Gloo, private FileStore | 1 PASS, 1/1 file; zero errors/failures/skips | `/tmp/hybrid-8-4-hybrid-gloo-filestore.log` |

Final distinct selections: **96 unittest cases across eight files plus the
original executable legacy regression script; 9/9 files PASS.** New tests: 16
fault cases plus one real three-rank case. Repeated development runs are not added.
The baseline had no test failures. Its pre-existing late-bootstrap fixture logs
an expected `/resources` EPERM/500 under the socket-restricted sandbox; legacy
negative tests also print expected corrupt/missing-final messages.

The sandbox denied even loopback socket creation. The actual Gloo runs therefore
used reviewed `require_escalated` local test invocations. This was permission for
disposable local tests, not root/system configuration or Pi deployment. No test
was skipped to claim Gloo acceptance. Initial development failures are retained:
a fault fixture initially advanced only the controller deadline, not the
independent worker clock; that fixture was corrected to model elapsed time on
both. Initial native final-state exact comparison failed as quantified above;
load/RNG/progress remain exact and only final floating comparison has the stated
criterion. See adjacent evidence for commands, log hashes and source hashes.

## Manual verification and expected results

Repeat the commands above. Expected new suites: 16 fault tests and 1 genuine
three-rank test PASS. Native run prints `THREE_RANK_STATE_DIFFERENCES` and
`UNINTERRUPTED_THREE_RANK_DIFFERENCES`; any non-floating/restore-state difference
or floating difference outside the declared tolerance fails acceptance.
The native test kills only child PIDs that it created, in disposable directories.
Allow loopback sockets in the test environment; do not modify firewall, NFS,
SSH, systemd or live worker configuration to make a sandbox test run.

Review `33-checkpoint-fault-acceptance-evidence.json` for exact per-case checks.
`git diff --check` should emit nothing. Runtime/schema/config hashes should equal
the start-of-turn snapshot. For a future physical validation, review a dedicated
plan for pi01, pi02 AND pi03, inventory active jobs, and use only isolated
disposable jobs. No live unmount, cable pull, reboot, service stop, package install
or power fault is authorized/applied by these local tests.

## Rollback, residual risks and acceptance

Only test/docs files changed. The complete turn-only delta is
`/tmp/hybrid-8-4/8.4.patch`. From the Git root:

```bash
git apply --reverse --check /tmp/hybrid-8-4/8.4.patch
# After reviewing subsequent edits and ensuring these tests are not running:
# git apply --reverse /tmp/hybrid-8-4/8.4.patch
```

This retains all prior uncommitted 8.1/8.2/8.3 work. Do not reset to HEAD, delete
other untracked files, or remove DB tables, jobs, checkpoints or cache data.
No live data/schema/service rollback is required because none changed.

| Acceptance | Status |
|---|---|
| Partial/stale/uncommitted checkpoints cannot become recovery source | PASS locally, injected transfers and native publisher SIGKILL |
| Worker/controller failure retains fencing, reservations and idempotent recovery | PASS locally, real SQLite and native process faults with documented transport/clock mocks |
| Genuine model/optimizer/RNG/application-state resume meets stated criteria | PASS locally, existing single/two-rank exact tests and new three-rank criterion |
| Current final checkpoint gates results/completion; legacy/OFF behavior preserved | PASS locally |
| Physical all-three-Pi NFS, host reboot/power and production isolation validation | NOT TESTED; required before production acceptance |

Residual risks remain those of 8.3: central SPOF/local-disk loss, unproven OS and
network containment, HTTP HMAC without encryption, long transfers exceeding lease
budgets, clock/WSGI timeout assumptions and retained private files needing reviewed
GC. Simulated ENETUNREACH is not a measured kernel NFS outage; SIGKILL is not a
physical power-loss durability test. This turn supplies no throughput result,
hardware benchmark or security-isolation proof. **8.4 local PASS; STOP.**
