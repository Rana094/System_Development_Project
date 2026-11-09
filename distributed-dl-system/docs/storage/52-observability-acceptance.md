# Phase 12.4 — observability acceptance

10 November 2026, Asia/Dhaka. Scope: **12.4 only**, completing the repository's
Phase 12 observability work. Earlier 12.1–12.3 completion is recorded in reports
49–51 and implementation-progress.md. This is isolated local acceptance, not a
production rollout or renewed physical cluster acceptance.

## Behavior and files

The new acceptance suite found two defects in the pre-turn source and reproduced
them before fixing them:

* SQLite REAL affinity permits text in `event_time`. Admin transfer history
  previously passed that text through as `recorded_at`. It now skips nonnumeric,
  nonfinite, negative and beyond-year-9999 timestamps; zero remains valid. An
  injected `/srv/other-student/secret` timestamp no longer enters the aggregate
  response. This test changes only a disposable fixture DB, not a real record.
* Browser timestamps previously treated zero as absent and coerced booleans or
  numeric strings into dates. The shared formatter now accepts finite numeric
  dates only, handles zero, rejects invalid dates and includes the browser's
  local timezone abbreviation/offset. The repeated New York DST hour is distinct.

| File | Change in this subphase |
|---|---|
| `controller/admin_storage_view.py` | Validate historical transfer timestamps before serialization. |
| `controller/portal_static/portal.js` | Strict timestamp formatting with explicit local timezone. |
| `tests/test_observability_acceptance.py` | Fourteen acceptance checks using actual isolated Flask, SQLite, HMAC inventory, logger and portal JS. |
| `tests/test_observability_ui.cjs` | Actual formatter acceptance in UTC, Dhaka and New York. |
| `tools/benchmarks/observability_overhead.py` | Offline, bounded event microbenchmark, JSON stdout; no controller imports. |
| `tests/README.md`, `tools/README.md`, `docs/student-interface.md` | Verification/tool instructions and timestamp behavior. |
| `docs/storage/implementation-progress.md` | Append-only subphase record. |
| This report and `52-observability-acceptance-evidence.json` | Results, provenance, limits and rollback. |

There are **no API routes, DB schema/migrations, transport, auth grants, storage
flags or scheduler/recovery behavior changes**. The existing admin response now
omits malformed historical measurements. Stored UTC times, event schema 1,
integer byte/nanosecond units and monotonic durations are unchanged. Defaults stay
legacy / cache off / storage-aware staging off / resource policy.

## Acceptance evidence

The before-edit baseline was **50/50 tests PASS across three files**: events19,
admin16, recovery15. The first new suite run was **10 pass, two failures**: strict
UI timestamp handling and malformed admin-history timestamp. These are gaps
reproduced in pre-turn code, not newly introduced failures. Both were fixed;
the next run was **12/12 PASS**. Two additional acceptance cases then checked the
real query budget and the benchmark CLI before the final regression run.

The final regression command and per-file counts are in the accompanying evidence
JSON and `/tmp/hybrid-12-4/final-tests.log`. It covers the new acceptance suite,
all Phase12 suites, legacy student UI, staging UI/dispatch, checkpoint recovery,
cache inventory/coverage, OFF config behavior, queue fairness and resources.
Only temporary source copies/databases are used. Fake NFS facts, sandbox hooks
and launchers are explicit fixture substitutions; CPU checkpoint resume coverage
is inherited from the existing recovery suite. No physical training is claimed.

| Requirement | Evidence and result |
|---|---|
| Permissions/multiple students | Alice, Bob, each with/without local admin grant; own detail/progress logs200, foreign404, anonymous401; aggregate storage403 until grant. Revocation is immediate; a cookie `is_admin` field does not grant access. `no-store` checked. Existing suites test insecure/symlinked grants, escaping and control ownership. |
| Read-only polling / DB growth | **150 successful HTTP reads plus30 emitted events** leave the actual fixture DB bytes identical: **106,496 before and after**. No new persistence table or per-event insert. |
| Unchanged inventory growth | **100 signed heartbeats** keep sessions/entries/registry-events at **1/1/1** and SQLite pages **86 → 86**. Diagnostic snapshot **249 bytes**; no abandoned publication tmp files. Heartbeats still update existing rows. |
| Bounded views / snapshots | Existing admin cap:256 workers,20 current dataset heads,10 transfer rows per worker,300 candidate events. New1,001 duplicate stage-event fixture yields one observation, bounded response and no poll write. Existing tests cover20-dataset truncation/256-worker snapshot. Snapshot reads capped256KiB; oversized/malformed/insecure files become Unknown. |
| Query budget | Actual recursive SQLite query is interrupted by the existing500ms portal progress handler; generic503, no raw error text or DB mutation. The test allows5s wall time for host variance. Existing connection lock timeout is250ms. |
| Logging retention | Disposable standard-library rotating JSONL handler,2KiB/file plus2 backups,100 real emissions: exactly3 files, total≤6KiB, every retained line valid schema1 JSON. It is not installed on running services. |
| Stale / replayed events | Duplicate signed response cannot rewrite/refresh the snapshot; old response after newer report rejected.16s offline heartbeat age hides coverage/NFS certainty.±31s health clock skew becomes Unknown; controller clock rollback cannot establish freshness. Existing tests cover sequence/session tampering, worker spoofing and stale recovery attempts. |
| Timestamps / timezone | Exact64-bit epoch nanoseconds preserved in Python under UTC/Dhaka/New York. Actual JS runs in all3 zones; zero valid, missing/boolean/text/nonfinite/out-of-range rejected, DST repeated local hour includes distinct zone/offset. Monotonic duration tests remain passing. |
| Experiment usability | Real emitted schema1 JSONL parsed with integer byte/ns fidelity. Replicated event IDs and repeated logical READY observations deduplicated independently, without ordering authority by wall clock. Known502MiB fixture remains526,385,152 bytes and2,000,000,001ns exactly. |
| UI regressions | All existing staging/admin/recovery JS harnesses run against actual portal.js: permissions, escaping, controls, truthful unknown/restart values and late account-switch responses. No CSS/layout change, browser screenshot or mobile/accessibility acceptance is claimed this turn. |

“Bounded DB growth” here means **the added observability does not append rows on
polls/emissions/unchanged heartbeats**. Existing legitimate FULL inventory,
cache-change and job/recovery history is retained and can grow with workload and
reconciliation. This subphase neither deletes authoritative/audit records nor
claims the entire application DB has a fixed size. Candidate-event limits bound
returned/projected records, not total SQLite pages scanned; the query deadline
also applies. Disk quota/archival policy for old application history remains an
operator concern outside the new instrumentation.

## Measured overhead

Actual controller-host Python3.12.3 microbenchmark,5 repeats of5,000 calls per
mode, using a representative fully populated staging-ready record:

| Mode | Median per synchronous call |
|---|---:|
| Empty Python call | 0.027µs |
| Disabled event call, including call arguments | 0.721µs |
| Enabled validation/ID/clock/JSON/logging to in-memory JSONL | 28.771µs |

**25,000 emitted JSONL records validated**. All raw timing samples, runtime and
platform are in the evidence JSON. This is a synthetic local microbenchmark,
not percentage training overhead, a Pi benchmark, NFS throughput or disk/network
handler latency. No arbitrary performance threshold was used. Existing tests
also prove disabled logging avoids event clocks/encoding and failing loggers
cannot veto actual staging/checkpoint commits. A slow synchronous operator
handler can still block; runtime deployment must choose its handler deliberately.

## Manual verification

Run from the application root; no services or remote workers required:

```bash
DL_NODE_BINARY=/usr/lib/chatgpt/resources/cua_node/bin/node PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_observability_acceptance.py
PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/benchmarks/observability_overhead.py --iterations 5000 --repeats 5
TZ=Asia/Dhaka /usr/lib/chatgpt/resources/cua_node/bin/node tests/test_observability_ui.cjs
```

Expected:14 tests PASS with growth evidence, JSON benchmark with25,000 validated
records and transparent samples, then timestamp/DST formatter PASS. Node path is
this machine's already-installed runtime; use your installed Node elsewhere.
These commands do not import the real controller DB outside the isolated runner.
Full final regression command is recorded verbatim in the evidence JSON.

Experiment scripts should follow report49's contract: preserve integers; use
`event_id` for replicated lines, `(job_id,attempt_id,worker_id,rank,stage_id)` for
repeated READY observations, and `checkpoint_id` for publications. Do not sum
attempt-wide recovery latency across ranks, use timestamps as lease authority,
or treat missing metrics as zero. Logs contain private operator job identifiers
and are never served to students. Unknown source/path/error text is rejected.

For later approved deployment, bounded logging can use a private LOCAL handler
with `RotatingFileHandler(path, maxBytes=4*1024*1024, backupCount=3)` and message-only
JSON formatting; the existing event bound is4096 bytes, well below that file
size. This is an operator example, **not applied configuration**. Review service
identity, directory permissions, event volume and blocking behavior first; do not
write logs to NFS or to a student-visible path. No production logging-retention
or nonblocking-handler claim is made by the temporary rotation test.

## Rollback and remaining limits

Source-only scoped diff: `/tmp/hybrid-12-4/12.4.patch`, against the exact pre-turn
working tree including previous uncommitted work. From the application root:

```bash
git apply --reverse --check /tmp/hybrid-12-4/12.4.patch
```

After reviewing that diff, `git apply --reverse /tmp/hybrid-12-4/12.4.patch` reverts
only12.4. No data/schema rollback required. Do not restore whole files from HEAD;
that would discard earlier uncommitted phases. The accompanying source verification
records the reverse check and actual reverse application in a disposable copy,
with all320 original source hashes restored and newly added files absent.

No commit, stash, reset, production account/role/DB/data change, live service,
mount/export/fstab/firewall/SSH change or worker contact. pi01 deferral remains;
this phase makes no new all-three-Pi claim. Existing runtime security prerequisites
and explicit deployment approval still apply. The central controller/LOCAL DB
and authoritative storage remain a single point of failure. Historical loaded
Phase10.4 HTTP409 is not diagnosed or relabeled resolved by these local checks.


## Verdict

**PASS:243/243 distinct tests in13 files; zero failures/errors/skips.** The new14
acceptance tests and existing regressions meet Phase12's local observability
criteria: metrics consumable by experiment scripts and access restrictions hold.
Repository Phase12 work is complete under the scope and limits above. **STOP**;
no following phase, production enablement or worker deployment performed.
