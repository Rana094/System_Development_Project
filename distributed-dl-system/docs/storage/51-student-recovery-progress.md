# Phase 12.3 — authorized student staging and recovery progress

10 November 2026, Asia/Dhaka. **12.3 only.** Previous completed subphase:
[12.2 admin storage view](50-admin-storage-observability.md), verified from its
report, progress record and actual source. No new website or live deployment.

## Implemented behavior and exact files

Reuse the existing student job list, detail page, input-staging panel and log
selector. A retried hybrid job now shows its current attempt's recovery
milestones: input workspaces acknowledged, checkpoint copies prepared, processes
launched, and training-state restoration confirmed. The existing staging byte/
verification/copy metrics remain intact. No percentage or per-chunk transfer
progress is invented; measurements that are not recorded remain unknown.

| File | Change |
|---|---|
| `controller/recovery_view.py` (new) | `read`, `_phase`, `log_text`: owner-scoped pure SQLite projection of exact current-attempt recovery milestones and safe text. |
| `controller/student_portal.py` | `progress` reads staging/recovery in one read-only snapshot, checks expected job retry/status; existing overview/detail/log routes reuse it. |
| `controller/portal_static/portal.js` | `renderRecovery`, RECOVERING display badge, recovery log option; existing list/detail reuse. Account revision fences overview/detail/log responses; clears old job content/log choices while preserving the original empty-detail prompt. |
| `controller/portal_static/portal.css` | Existing badge style for RECOVERING. |
| `tests/test_recovery_portal.py` (new) | Fifteen real owner/API/SQLite/signed-staging/recovery fixture checks, compatibility and malformed/foreign metadata cases. |
| `tests/test_recovery_portal_ui.cjs` (new) | Actual portal JS rendering, controls, unknown/restart values, escaping and late-response account-change tests. |
| `docs/student-interface.md`, `tests/README.md`, `docs/storage/implementation-progress.md` | Student guidance, exact local checks and progress record. |
| This report and `51-student-recovery-progress-evidence.json` | Scope, source hashes, commands/results, manual checks and rollback. |

Six existing files changed, five new files. Earlier uncommitted phases are
preserved. No checkpoint writer/reader, SDK, scheduler, agent, registry schema,
storage backend, protocol, lease/cancellation rules or feature flags changed.

## API and milestone contract

Existing owner-authenticated `GET /api/overview` job items and
`GET /api/jobs/<job_id>` add **`recovery` only to hybrid job representations**.
It is null on the initial attempt. Legacy responses retain their existing shape
and recovery history. Staging and recovery are read from the same SQLite snapshot;
if the outer job's retry/status changed before that read, both projections are
null for that poll rather than mixing old job metadata with a newer attempt.
The existing refresh loop reads again on the next poll.

Schema-one recovery projection contains only:

- `attempt` (`retry_count + 1`), recovery `mode`, display `phase` and fixed `label`.
- `total_ranks`, acknowledged input/checkpoint-copy/launch/restore counts or null.
- `restore_required`: true for a validated frozen committed source, false for a
  full restart, null when selection cannot be established.
- `source`: a fixed state plus numeric `global_step`, `size_bytes`, `from_attempt`;
  no checkpoint ID, object hash, path, lease ID or credential.
- Per-rank milestone booleans/nulls, capped at 256 listed ranks with a truncation
  flag. Aggregate counts cover the actual declared group.
- Last recorded `updated_at` as Unix seconds and literal
  `observation="controller_records_only"`; this is not a new latency measurement.

Display phases are **not new job FSM states**. The source of truth remains the
existing QUEUED/SCHEDULED/RUNNING/etc. job and side-ledger state. Labels distinguish:

| Observation | Display |
|---|---|
| Retry queued/held | Waiting for replacement workers / recovery on hold. |
| Existing current-attempt inputs not all acknowledged | Preparing recovery inputs. |
| Inputs ready; selected checkpoint copies incomplete | Preparing checkpoint copies. |
| Copy receipts complete; launch acknowledgments incomplete | Starting training processes. |
| Processes launched; SDK restore barrier not confirmed | Waiting for training-state restoration. |
| SDK accepted but job RUNNING transaction not yet observed | Training state restored; waiting for running status. |
| Job RUNNING and exact restore attempt/group confirmed | Training resumed from checkpoint. |
| Job RUNNING after full restart | Training restarted. |
| Cancellation, ABORTING/TERMINAL staging, or known missing/revoked/blocked/wall-expired lease | Stopping recovery and reconciling execution ownership. |
| Failed/cancelled/completed job | Corresponding terminal label; no active copy claim. |
| Missing/inconsistent/foreign/uncommitted source metadata | Unknown/not-recorded details, never inferred checkpoint readiness. |

Source metadata comes from the **frozen current retry's source**, not whatever
newer checkpoint happens to be latest. A source is projected as committed only
when its owner/job, immutable job-input binding, older producing attempt,
single/DDP kind, COMMITTED phase and numeric metadata agree. The UI does not
open/re-hash/unpickle checkpoint files or parse the private source JSON; actual
runtime publication/recovery validation remains unchanged and authoritative.
A committed DB row reports controller metadata, not proof its file is currently
reachable from a worker.

The existing controller commits SDK restoration as a **whole-group barrier**.
Thus a three-rank recovery with two SDK acknowledgments observed remotely still
shows zero **controller-confirmed** restored ranks until all required proofs
are accepted. Checkpoint copy and process launch are shown independently; neither
is labeled resumed. Restart mode and a checkpoint-mode retry before any saved
checkpoint exists both show checkpoint restoration **Not required**. Internal
cold-launch confirmation rows are not misrepresented as restored checkpoint state.

The existing log route supports `GET /api/jobs/<job_id>/logs?view=recovery`.
It returns the same numeric/allowlisted view as plain text (bounded to the
existing 64 KiB response cap), never a service log or filesystem read. Hybrid
initial-attempt/no-record cases return a fixed not-recorded message; legacy
requests for this new log view return 404. The existing staging log remains.
The current recovery log is selected automatically while waiting to recover,
unless the user already chose another log. Cancel/hold/release controls retain
their existing rules.

## Security, DB and configuration

The existing session and `owned()` check run before recovery route access.
`recovery_view.read` checks owner/custom-job/hybrid binding again **before** any
optional checkpoint/restore tables. A different student receives 404 for detail
and progress logs; anonymous requests receive 401. The 12.2 admin role grants no
exception to another student's job ownership. Overview still includes only the
signed-in student's jobs.

New progress payloads contain **no shared paths, filenames, hashes, publication
or stage IDs, worker credentials, other students' identifiers, arbitrary error
strings or exception text**. Labels are fixed and numeric metadata is typed.
Browser interpolation is escaped. No global/admin observations are added to
student job payloads. Existing hybrid sanitization remains in place.

New progress reads use LOCAL SQLite `mode=ro`, a read transaction, 250 ms lock
timeout and a 500 ms SQLite progress budget. No new DB, schema/migration, row
write, event emission, background task or remote call is introduced by polling.
Missing optional recovery tables are handled as not-recorded metadata; old job
files are not adopted/moved. Current attempt/stage/source joins exclude previous
attempt acknowledgments. A known lease cancellation/expiry changes only the
display; the view cannot renew, fence, reserve, launch or cancel anything.

Account revision checks now protect the existing overview, detail and log
responses as well as the prior admin panel. Logout/account entry clears old job
content and log selections. A late successful Alice response cannot repopulate
Bob's DOM, including when a selected identifier is repeated. The original static
empty-detail prompt is restored on account changes.

Active configuration remains **legacy / worker_cache_enabled=false /
storage_aware_staging=false / scheduler_policy=resource**. No production login,
admin grant, running service, SSH, mount/export/fstab/firewall or Pi was changed.
The existing central controller, LOCAL SQLite and authoritative storage remain
a single point of failure.

## Actual validation

Baseline before edits: **52/52 tests across three files PASS**, 115.8 seconds:

```bash
DL_NODE_BINARY=/usr/lib/chatgpt/resources/cua_node/bin/node PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_staging_portal.py test_student_portal.py test_hybrid_checkpoint_recovery.py
```

First focused implementation run: **13/13 tests PASS**, 32.0 seconds. Added a
stale outer-snapshot test before the seven-file regression:

```bash
DL_NODE_BINARY=/usr/lib/chatgpt/resources/cua_node/bin/node PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_recovery_portal.py test_student_portal.py test_staging_portal.py test_hybrid_checkpoint_recovery.py test_admin_storage.py test_staging_dispatch.py test_storage_events.py
```

**7/7 files PASS**, 232.3 seconds. Final edge-case review added explicit
checkpoint-mode cold-retry treatment and its test, then reran:

```bash
DL_NODE_BINARY=/usr/lib/chatgpt/resources/cua_node/bin/node PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_recovery_portal.py test_staging_portal.py
```

Latest focused **32/32 tests in two files PASS**, 85.6 seconds, testing the final
runtime source. The earlier broad source copy predates only that cold-retry
refinement and test; other runtime paths did not change afterward.

| Latest distinct test file | Cases | Result |
|---|---:|---|
| `test_recovery_portal.py` | 15 | PASS |
| `test_student_portal.py` | 17 | PASS |
| `test_staging_portal.py` | 17 | PASS |
| `test_hybrid_checkpoint_recovery.py` | 18 | PASS |
| `test_admin_storage.py` | 16 | PASS |
| `test_staging_dispatch.py` | 25 | PASS |
| `test_storage_events.py` | 19 | PASS |
| **Distinct latest total** | **127** | **Zero failures/errors/skips** |

Do not add repeated baseline/rerun cases to that count. New fixtures exercise
the actual Flask owner checks, real disposable SQLite/immutable publication/
local-cache copies and existing signed worker transports. They cover queue →
input prep → checkpoint prep → launch → SDK confirmation, partial three-rank
copy counts and SDK barrier, pending/recreated controller, cold restart, stale
attempt, expired lease/cancel display, read-only polling, foreign source metadata,
legacy responses, and owner/admin isolation. Existing recovery regressions
include genuine local CPU checkpoint state resume. Trainer/isolation/NFS facts
in the new portal fixtures are mocked; no physical DDP/Pi acceptance is claimed.

The new Python suite executes all three actual-script JS harnesses:
`test_recovery_portal_ui.cjs`, `test_staging_portal_ui.cjs`,
`test_admin_storage_ui.cjs`. No Node/npm install or Internet transfer. Node is
not on this shell PATH; the installed app runtime above is reused. On other
machines set `DL_NODE_BINARY` to an existing Node path or use Node on PATH.

Visually inspected actual current portal rendering/CSS with synthetic own-job
three-rank recovery data in the in-app browser: input/copy counts 3/3 while
restoration remains 0/3, separate RECOVERING badge and recovery log choice.
Screenshots: `/tmp/hybrid-12-3/student-recovery-top.png` and
`/tmp/hybrid-12-3/student-recovery-preview.png`. A full-page screenshot attempt
failed; standard viewport captures succeeded and the milestone table was
inspected after scrolling. This was a static synthetic fixture with live scripts
disabled, **not a live backend/login/Pi measurement**. Approved temporary
localhost static server used 127.0.0.1:39291, then was interrupted/closed (exit130)
and its browser tab closed. No unrelated service/process was stopped. No mobile
or full accessibility acceptance is claimed here.

Logs, source snapshot/hash catalog, screenshots, 12.3-only patch and reverse-
apply evidence are under `/tmp/hybrid-12-3/`, indexed in the companion evidence.
`git diff --check` PASS. Branch remains `main`, HEAD remains
`7edef1ccd8e3264d9716a3482cbd214188d009b6`. Of 315 snapshotted files, six existing
files changed and 309 original files remain byte-identical; five files added.
Documentation additions preserve original contents. No commit/stash/reset.

## Manual verification, rollback and residual risks

Safe local reproduction from app root:

```bash
DL_NODE_BINARY=/usr/lib/chatgpt/resources/cua_node/bin/node PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_recovery_portal.py
```

Expected: **15 PASS**; actual controller/agent/cache work occurs only in disposable
test copies. The same suite runs the new and existing JS harnesses. No listeners,
Pi connections, mounts, live DB changes or training services are required.

After a separately reviewed deployment, sign in to the existing student portal
and select an authorized retried hybrid job. The list/detail show recovery state
and source step, stage counts, and the Recovery progress log option. Copy/launch
alone must not show Training resumed; every required SDK proof must first be
accepted. A full restart says Not required for checkpoint restoration. Sign in
as another student and verify the job is absent; direct detail/progress-log URLs
return404. Do not initiate/disrupt real jobs merely to check this UI; use isolated
fixtures first. No new feature flag or service enablement was applied here.

Source rollback is the **12.3-only** patch against the pre-turn working tree:

```bash
git apply --reverse --check /tmp/hybrid-12-3/12.3.patch
git apply --reverse /tmp/hybrid-12-3/12.3.patch
```

Review it and apply only if the check succeeds. A real reverse application is
verified in a disposable source copy; the actual checkout is never reversed.
Do not restore whole files from HEAD, which would erase previous uncommitted
phases. No DB/schema/runtime data rollback is needed. No files, datasets,
checkpoints or jobs should be deleted. A live service/source rollback still
requires the existing reviewed deployment procedure.

Residual limits: progress is a committed-controller observation, not real-time
worker liveness; transient snapshots may show not-recorded until the next poll;
SDK partial rank acknowledgments are not persisted individually; in-progress
copy bytes/percentages and a durable recovery latency are not available here.
Stage/job timestamps are existing wall-clock records, not a new stopwatch. The
known loaded-run HTTP409 issue remains disclosed, not root-caused. pi01 stays
deferred; no physical-worker validation was needed/run for this UI subphase.
Full Phase12.4 permissions/retention/overhead/stale/timezone/UI acceptance remains
pending, and Phase12 overall is not complete.

**12.3 PASS:** current authorized student staging/recovery progress is shown in
the existing site without private shared-storage metadata or cross-owner data;
legacy behavior and runtime safety rules preserved. **STOP before 12.4.**
