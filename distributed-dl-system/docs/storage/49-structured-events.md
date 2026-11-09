# Phase 12.1 — structured storage events

10 November 2026, Asia/Dhaka. Scope: **12.1 only**, operator instrumentation in
the existing hybrid lifecycle. No admin/student UI, event HTTP endpoint, new
website, persistent event table, migration or live deployment. Phase 12 as a
whole is not complete. Earlier uncommitted Phase 10/11 changes are preserved.

## Contract and units

`hybrid_storage/events.py` defines schema **1** on Python logger
`distributed_ml.storage.events`, level **INFO**. The log message itself is one
compact JSON object, at most **4,096 UTF-8 bytes**. This module installs no handler,
changes no root logging level, and performs no disk/network/SQLite work on import.
The ordinary default logger threshold suppresses INFO. A trusted bootstrap may
enable this logger using existing operator logging; no storage flag is enabled.
Legacy dispatcher/bootstrap behavior and public payloads remain unchanged.

Every event has `schema_version`, `event_type`, `event_id`, `occurred_at_ns`,
`job_id`, and `attempt_id`. Context fields supplied when available:

| Field | Meaning |
|---|---|
| `event_type` | Stable event identifier from the table below; interpreted within schema 1. |
| `event_id` | Random 128-bit lowercase hex observation ID; unique per emission, not an execution token. |
| `occurred_at_ns` | Integer Unix epoch nanoseconds (UTC), captured on the emitting host; never local-time text. |
| `attempt_id` | Existing `job_id:retry_count` identity; not a lease credential or new retry scheme. |
| `worker_id`, `rank` | Existing worker identity and zero-based logical rank; absent/null where not applicable. |
| `stage_id` | Existing controller stage identifier, when known. |
| `manifest_version` | Exact immutable registered **job-input binding SHA-256**, not a mutable filename. |
| `manifest_schema_version` | Current registered-job-inputs schema, 1; distinct from dataset version. |
| `dataset_version` | Pinned positive immutable dataset version, null/absent if unavailable. No dataset name/owner/path. |

All `*_bytes` values are integer **bytes**, not MB/MiB. All duration/latency
`*_ns` values are integer **nanoseconds** from a **monotonic** clock. Numeric
measurements reject booleans, negatives, floats/NaN/infinity and values exceeding
signed 64-bit range. Unknown measurements are **null**, never invented zero.
Consumers must preserve 64-bit integers; do not parse epoch nanoseconds into a
JavaScript Number and expect exact precision. Convert timestamps/durations on a
trusted backend before future browser presentation.

| Stable event type | Milestone and fields |
|---|---|
| `storage.staging.started` | Authenticated worker accepts exact live attempt authority, before input/workspace preparation. |
| `storage.staging.ready` | Existing authority rechecked and private journal READY committed. `cache_hit_bytes`, `transferred_bytes`, `verified_bytes`, `staging_duration_ns`, `verification_duration_ns`, `copy_duration_ns`. |
| `storage.staging.failed` | That preparation operation raised; same numeric measurements for work accepted before failure, plus `error_class` and numeric/null `errno`. Does not claim cleanup/quiescence completed. |
| `storage.nfs.error` | Authorized NFS-to-cache copy path raised; `error_class`, `errno`, one-based `copy_attempt`. This can include local part-file/disk errors and is not proof the NFS server caused it. |
| `storage.nfs.retry` | Existing bounded transient-errno rule elects another copy attempt. Same fields; `copy_attempt` is the failed attempt being retried. No changed retry limits/delay. |
| `storage.scheduler.choice` | Existing selector proposal, before reservations commit. Per selected worker/rank (maximum existing 256-node log bound), or worker/rank null when no candidate. `requested_policy`, effective `scheduler_policy`, allowlisted `reason`, verified `missing_bytes` or null. Existing richer `SCHEDULER_CHOICE` records remain unchanged. |
| `storage.checkpoint.committed` | Verified immutable file and controller metadata/head transaction committed, then existing reader revalidates. `checkpoint_id` is the authoritative **publication ID**, `verified_bytes` is checkpoint file size, `world_size` is expected group size. One event for a complete archive, not fabricated per-rank saves. |
| `storage.recovery.started` | Existing replacement placement has been reserved, before input staging. Per rank, committed source `checkpoint_id`, `world_size`. Cold launches emit no recovery event. |
| `storage.recovery.confirmed` | Existing SDK model/optimizer/RNG and DDP acknowledgments accepted and confirmation transaction committed. Per rank, same checkpoint ID/world size and `recovery_latency_ns`. A copy/start HTTP acknowledgment alone cannot emit it. |

For successful staging, each input digest is counted **once** even when multiple
logical paths reference it: hit bytes + transferred bytes = verified bytes =
unique required bytes. `transferred_bytes` counts fully verified payload newly
accepted into the local cache, **not wire traffic**, retransmissions, partial
failed reads, workspace copies or checkpoint transfer. Failed staging reports
the same accepted-object counters up to failure; partially read/discarded bytes
are not measurable by these counters. Re-verification may hash the same bytes
several times; `verified_bytes` is not the sum of hash passes. Verification and
copy duration spans may overlap and must not be added as disjoint phases.

Staging duration covers trusted fetch, workspace preparation, final authority
checks and READY journal commit (or exception), excluding RPC transport and queue
wait. Recovery latency covers **reserved replacement dispatch through accepted
SDK restore acknowledgment**, including input/checkpoint staging and launcher
restore. It excludes failure detection, prior queue wait and total job downtime.
This attempt-wide latency is repeated per rank: do not sum it across ranks.
Recovery start clocks are controller-instance local, capped at 128 attempts.
Restart, observer disabled at start or capacity eviction makes latency **null**.
This avoids subtracting clocks across boots or fabricating an estimate.

## Security, delivery and compatibility

The field allowlist rejects the **entire** invalid event, including arbitrary
tags/messages, usernames/owners, filenames, shared paths, IPs, credentials and
lease IDs. Error strings, tracebacks, `OSError.filename` and exception arguments
are never serialized. Error classes are the closed set `timeout`,
`mount_unavailable`, `integrity`, `capacity`, `denied_or_fenced`, `os_error`,
`other`. Scheduler reasons are a closed allowlist of current code constants.
Job/binding IDs are still private operator metadata: these logs are not student
payloads or authorization APIs. A new future reason requires a contract update,
otherwise that event is dropped without changing scheduling.

Events are **best effort**, not a durable outbox or source of execution authority.
Logger/clock/encoding errors cannot authorize or veto jobs/checkpoints. Disabled
logging avoids event clocks, encoding and IDs. There are no per-chunk events or
new per-event DB writes; existing metrics schemas and signed RPC bodies stay
unchanged. Worker copy attribution is scoped with ContextVar inside each actual
staging call, resets on errors, and cannot bleed across concurrent rank threads.

Checkpoint idempotent replay and redundant distributed-save transports emit no
second commit event. A successful repeated READY RPC is a new observation with a
new `event_id`: use `(job_id, attempt_id, worker_id, rank, stage_id)` to identify
the logical stage and choose an observation for experiment accounting. Never sum
READY retry observations as new required bytes. Deduplicate a replicated log by
`event_id`; correlate commits by `checkpoint_id`. Process crashes can lose an
event after a real commit; absence of an event proves nothing about DB authority.
Wall timestamps across machines can be skewed/out of order. State, manifest,
lease and checkpoint validation remain the authority, not event arrival order.

Use a reviewed bounded, nonblocking operator handler at deployment. A custom
synchronous handler can still block despite exception containment; no throughput
or instrumentation-overhead benchmark is claimed here. Log rotation/retention
and persistent UI aggregation/access tests are later subphases. The existing
LOCAL recovery_events table is neither replaced nor expanded by this work.

## Source integration and manual verification

| File | Change |
|---|---|
| `hybrid_storage/events.py` | Contract/validation, JSON emission, staging attribution, bounded recovery clocks. |
| `agent/staging.py` | Observe existing authenticated staging and report READY only after commit. |
| `hybrid_storage/cache_fetch.py` | Observe copy errors/retry decisions; preserve existing retry rules. |
| `hybrid_storage/scheduling.py` | Emit small common events beside existing choice records; resource default gains no inventory/tracing queries. |
| `hybrid_storage/checkpoint_publication.py` | Emit after actual immutable checkpoint/metadata commit. |
| `controller/staging_dispatch.py` | No-op recovery observation hook after reserved placement. |
| `controller/checkpoint_recovery.py` | Per-rank recovery observations after reservation/confirmed SDK restore. |
| `tests/test_storage_events.py` | Contract and real isolated lifecycle regression fixtures. |
| `hybrid_storage/README.md`, `tests/README.md`, `docs/storage/implementation-progress.md` | Documentation links and subphase record. |
| This report and `49-structured-events-evidence.json` | Scope, actual validation, source/test evidence and rollback. |

From the application root, safe local verification:

```bash
PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_storage_events.py
```

Expected: all event tests pass; temporary copied DBs/cache/workspaces only. Tests
capture the **real JSON logging messages** from signed Flask staging, controller
publication and SDK acknowledgment fixtures; no live listener/mount/SSH/Pi is
needed. Existing test isolation callbacks/trainer launch are mocked, so this is
not new evidence of hostile-code containment or physical DDP acceptance.

For a synthetic, local JSONL contract smoke check (not measured workload data):

```bash
PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python - <<'PY'
import logging
from hybrid_storage.events import LOGGER, emit
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(message)s'))
LOGGER.addHandler(handler)
LOGGER.setLevel(logging.INFO)
LOGGER.propagate = False
emit('storage.staging.started', job_id='synthetic-smoke',
     attempt_id='synthetic-smoke:0', worker_id='pi02', rank=0)
PY
```

Expected: one JSON object on stderr with schema 1, a fresh 32-hex event ID and
UTC epoch-nanosecond timestamp; no files or DB touched. An experiment reader can
`json.loads` each message, filter by event type/job/attempt/rank and apply the
deduplication rules above. This command enables logging in that process only;
it does not enable/deploy the hybrid runtime.

## Rollback and limits

Immediate instrumentation rollback: trusted bootstrap sets
`logging.getLogger('distributed_ml.storage.events').setLevel(logging.WARNING)`.
No jobs, manifests, inventories, DB migrations, services or NFS mounts to undo.
For source rollback, the evidence records a **12.1-only** patch based on the
pre-turn working tree, including prior uncommitted work. Review it, run
`git apply --reverse --check /tmp/hybrid-12-1/12.1.patch`, then (only while the
check succeeds) `git apply --reverse /tmp/hybrid-12-1/12.1.patch` from the application
root. A disposable-copy reverse application is checked before reporting PASS.
Do not restore whole files from HEAD: that would erase earlier Phase 10/11 work.

No live system/service/network/config/SQLite modification, Pi contact or deploy.
The prior pi01 deferral is preserved, not treated as new hardware acceptance.
Phase 12.2/12.3 UI and 12.4 full retention/permissions/overhead/stale-event tests
remain pending. The controller, LOCAL SQLite and authoritative storage remain a
single point of failure. The historical loaded-run staging HTTP409 issue from
Phase 10.4 remains disclosed; this subphase does not claim to root-cause it.

Actual test results and final source checks are recorded below and in the
machine-readable evidence companion.

## Actual verification and acceptance

Baseline, before edits: **85/85 tests, four files PASS**, 122.5 seconds:

```bash
PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_staging_dispatch.py test_scheduler_policy.py test_checkpoint_publication.py test_hybrid_checkpoint_recovery.py
```

Regression run: **8/8 files PASS**, 267.0 seconds:

```bash
PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_storage_events.py test_staging_dispatch.py test_staging_portal.py test_staging_recovery.py test_cache_fetch.py test_scheduler_policy.py test_checkpoint_publication.py test_hybrid_checkpoint_recovery.py
```

After adding failed-stage accepted-byte reporting and corruption classification,
reran the final runtime paths: **43/43 tests, two files PASS**, 61.9 seconds:

```bash
PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_storage_events.py test_staging_dispatch.py
```

Then added the explicit all-rank SDK event-barrier test: latest event suite
**19/19 PASS**, 23.3 seconds, using the single-file manual command above.

| Latest distinct test file | Tests | Result |
|---|---:|---|
| `test_storage_events.py` | 19 | PASS |
| `test_staging_dispatch.py` | 25 | PASS |
| `test_staging_portal.py` | 17 | PASS |
| `test_staging_recovery.py` | 34 | PASS |
| `test_cache_fetch.py` | 33 | PASS |
| `test_scheduler_policy.py` | 17 | PASS |
| `test_checkpoint_publication.py` | 25 | PASS |
| `test_hybrid_checkpoint_recovery.py` | 18 | PASS |
| **Distinct latest total** | **188** | **Zero remaining failures/errors/skips** |

Do not add baseline/repeated test counts to that total. The regression source
copy predates the final two telemetry refinements; the later event/staging runs
validate those exact runtime edits. The last test-only addition changes no
runtime source. Existing portal tests include multi-owner/anonymous rejection
and no foreign data/path leakage, but broader Phase 12 UI/retention/access work
is still pending. Existing recovery tests include genuine local CPU state resume;
the new event-specific launch fixtures use mocks. No live Pi test was run.

Initial new-test attempts had six fixture import errors, then one incorrect
assertion that a failed dispatch returned false instead of raising existing HTTP
409. Fixed the test harness/import order and expected exception; no runtime
failure semantics were changed. One attempted test command used a nonexistent
`test_storage_metrics.py` name; the runner rejected it before running tests.
These are recorded development errors, not pre-existing failures or hidden PASSes.

Evidence directory: `/tmp/hybrid-12-1/`, with baseline snapshot/hash catalog,
all attempt logs, final test logs, synthetic JSONL smoke record, final scoped
patch and disposable reverse-apply verification. The evidence JSON lists exact
filenames and hashes. `git diff --check` PASS. Branch stays `main`, HEAD stays
`7edef1ccd8e3264d9716a3482cbd214188d009b6`; no commit/stash/reset. Original
303-file source snapshot retained. Only the nine listed pre-existing files
changed; all other original source/config/docs remain byte-identical. Active
`config/storage.json` is unchanged: legacy / false / false / resource.

**12.1 PASS**: requested metrics have stable event IDs/units and actual trusted
lifecycle producers; JSONL is readable by experiment scripts; validation and
failure isolation are tested; legacy flags and existing routes/formats remain.
**STOP before 12.2.** Phase 12 overall acceptance is not yet claimed.
