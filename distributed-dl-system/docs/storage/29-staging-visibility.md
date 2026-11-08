# Phase 7.4 — owner-safe staging visibility

Date: 9 November 2026, Asia/Dhaka. **Local implementation and mock acceptance;
no live activation.** Starting branch `main`, HEAD
`68f59e9e72d8ecb2e1b5a98400be2e22051eafc4`. The delivered 7.3 report/progress
and actual code are the previous local boundary. The existing uncommitted 7.2/7.3
work was preserved. Before edits, saved 266 inventoried files and Git status in
`/tmp/hybrid-7-4/before/`, `before.json` and `status-before.txt`. No stash, reset,
commit, live DB migration, service operation, SSH, mount, export or Pi action.

## Behavior and exact files

Modified relative to that saved working tree:

* `agent/staging.py`: add numeric measurements to the existing signed successful
  stage reply; old top-level byte counters and receipt format remain intact.
* `controller/staging_dispatch.py`: validate measurements after authenticated
  receipt verification; hit plus staged bytes must match unique required digests.
* `hybrid_storage/staging_lifecycle.py`: optional `metrics=None` argument to
  `record_ready`; append one `INPUTS_STAGED` event with first READY in the same
  transaction. Repeated READY cannot replace the original cold measurement.
* `hybrid_storage/metrics.py`: thread-scoped observations retain cumulative
  counters while excluding concurrent inventory verification from request times.
* `controller/student_portal.py`: owner-scoped staging projection in existing
  detail/overview responses and a controller-generated staging log. Sanitize
  hybrid service errors/history and omit input locations/internal lease IDs.
* `controller/portal_static/portal.js`, `portal.css`: STAGING display badge, Input
  preparation panel, rank readiness, reused/staged bytes, measured rank count,
  verification/copy durations and selectable Input staging log. Existing Cancel
  control remains. No new scheduler state or percentage complete is invented.
* `docs/student-interface.md`: explain the display and measurement limits.
* `hybrid_storage/README.md`, `docs/storage/implementation-progress.md`: record
  this subphase and its remaining production gates.

New:

* `hybrid_storage/staging_metrics.py`: v1 strict numeric allowlist, nonnegative
  signed-64-bit bounds; reject booleans, floats, unknown keys and worker text.
* `controller/staging_view.py`: read-only owner/current-attempt projection and
  fixed-text log formatting. Unknown measurements remain null, not zero.
* `tests/test_staging_portal.py`: local signed stage/cache/workspace/portal tests.
* `tests/test_staging_portal_ui.cjs`: dependency-free actual JavaScript execution
  with mocked DOM/fetch, including existing legacy display and controls.
* This report and `29-staging-visibility-evidence.json`.

## API, DB, configuration and security

No schema, migration checksum, `user_version`, job state, checkpoint format,
upload transport or production config changed. SQLite stays controller-local.
Existing `recovery_events` is reused; if absent, staging remains compatible and
measurements are unavailable. Dashboard reads never install optional tables.
Defaults remain `legacy`, cache false, staging false, `resource`.

Existing signed `/jobs/stage` replies gain optional `metrics` schema_version 1
with `cache_hit_bytes`, `staged_bytes`, `workspace_copy_bytes`, `verification_ns`,
`copy_ns`, `copy_attempts`, `copy_failures`, `copy_timeouts`, `cache_corruptions`,
`workspace_failures`. Missing/malformed telemetry is discarded; it does not replace
or bypass authenticated receipts, authorization, reservations or all-rank leases.
No new download API or worker telemetry endpoint was added.

For pinned NFS jobs only, `GET /api/overview` job entries and
`GET /api/jobs/<job_id>` gain `staging` (null until a current attempt exists):
`schema_version`, `attempt`, `phase`, `label`, `ready_ranks`, `total_ranks`,
`measured_ranks`, `updated_at`, `metrics`, `ranks`. Rank entries contain only
`rank`, `ready`, `metrics`. Staging state is historical acknowledgement, never
permission to run. The stored scheduler state remains SCHEDULED during staging.

`GET /api/jobs/<job_id>/logs?view=staging` returns the usual `{text, source}` with
fixed controller text and numeric measurements, capped at 64 Ki characters.
The UI selects this view while awaiting staged inputs; ordinary live logs remain
available separately. SCHEDULED/CANCELLING hybrid default log reads use this
controller view, avoiding premature remote training-log access. A legacy job's
staging-log request is 404. Other owners get 404, anonymous users 401, before any
staging lookup. Read-side SQL checks owner and pinned backend again.

Hybrid detail omits controller input/checkpoint locations, raw job_config and
internal lease credentials, substitutes upload labels for source/data paths,
and projects service-event details through a numeric allowlist. Error messages
become fixed operator-action text; raw exceptions stay in trusted operator logs.
Legacy responses retain their existing representation. Student training output
is still untrusted text belonging to that owner, not a general secret-redaction
service. Browser text is escaped; no other student's event/file listing is joined.

## Measurement semantics and compatibility

Measurements describe a successful, acknowledged staging RPC. Lost replies do
not count as READY or recorded measurements. First READY and its event commit
atomically. Worker reboot cannot reconstruct measurements whose reply was lost;
there is no fabricated history. Current retry is selected explicitly; old attempt
events remain in owner-scoped history without entering new attempt totals.

Each digest counts once per rank for cache-hit/staged bytes. Private workspace
copies count every logical file path. DDP totals sum actual per-rank copies, so
common whole-dataset inputs appear on multiple workers. Partial totals cover only
measured ranks. Zero means measured zero; null means not recorded. Verification
and copy durations overlap, and include existing parent-side hashing/fsync/waits;
they are not wire throughput, child helper CPU, training time or progress bars.

Normal startup still does **not** instantiate the explicit staging runtime.
The existing config validator rejects enabling the worker cache in ordinary
controller startup. Only the reviewed test composition enables the new flow;
this subphase does not remove that production gate. Restart-mode custom jobs are
the supported staged path; NFS checkpoint jobs remain held. Reviewed production
OS/network isolation, termination/quiescence providers and trusted checkpoint
publisher integration remain pending. Controller/NFS remains a single point of
failure. Phase 5.4's historical dedicated acceptance is not retroactively claimed.

## Actual tests and evidence

All Python commands below use disposable source copies and temporary DBs/roots.
No Internet transfer, listener, live training or live NFS was used. Mock NFS facts
allow real hash/copy/fsync helpers; trainer, prepared environment and containment
are mocked. Three simulated worker apps/ranks are not physical Pi validation.

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_student_portal.py test_staging_dispatch.py
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_staging_dispatch.py test_staging_recovery.py test_staging_lifecycle.py test_storage_config.py test_storage_selection.py test_queue_controls.py test_execution_ownership.py test_job_environments.py
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_staging_portal.py test_student_portal.py test_cache_fetch.py test_private_workspaces.py
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_staging_portal.py test_staging_dispatch.py test_worker_cache.py
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_staging_portal.py
git diff --check
```

Exact counts, logs, hashes, final new-suite result, initial corrected integration
failures, JavaScript execution and scoped source preservation are in the adjacent
evidence JSON. Counts from repeated/expanded test runs are **not added**. The
eight-file regression snapshot preceded request-scoped timing hardening; final
portal/cache/workspace/dispatch/worker-cache runs cover that hardening. Existing
real-DDP socket tests previously failed under sandbox socket EPERM; they were not
rerun or claimed passing. No physical benchmark or production browser layout/
accessibility acceptance is claimed.

| Selection snapshot | Result (zero failures/skips) | Log under `/tmp/` |
|---|---|---|
| Pre-edit portal + dispatch | 42 tests, 2/2 files PASS | `hybrid-7-4-baseline.log` |
| Regression, eight files | 145 tests, 8/8 PASS | `hybrid-7-4-regression.log` |
| Request timing + portal/cache/workspace | 102 tests, 4/4 PASS | `hybrid-7-4-final-ui-cache.log` |
| Expanded portal + dispatch + worker cache | 73 tests, 3/3 PASS | `hybrid-7-4-final-attempts.log` |
| Final expanded new suite | 17 tests, 1/1 PASS | `hybrid-7-4-final-portal.log` |
| Actual JavaScript in mocked DOM | PASS | `hybrid-7-4-ui.log` |

The first implementation run failed (12 new tests: one failure/eight errors,
plus a legacy portal import failure). It caught incorrect backend lookup against
`jobs` instead of the actual `job_storage_bindings`, import ordering under the
legacy test entry point, and an incorrect DDP fixture patch target. These were
introduced here and corrected; the same checks then passed. They are not claimed
as pre-existing failures or converted to skips.

## Manual verification and rollback

From the application root, run the last Python command above: expect all new
tests to pass, including multipart website upload -> held receipt -> explicit
trusted registration/preparation -> signed local staging -> mocked launcher ->
authenticated completion -> owner UI. The warm follow-up test refuses any NFS
fetch and expects staged_bytes=0. Cancellation during copy must leave no launcher
call. The existing portal suite must keep legacy submissions/controls/results
working. Other-owner detail/log requests must remain 404.

With Node.js available, run `node tests/test_staging_portal_ui.cjs`; expect its
PASS message. This machine has no shell `node`; the same exported harness was
executed using the available Node REPL tool (exact invocation in evidence).

On a separately approved, isolated deployment with reviewed bootstrap, select
your own hybrid job in the existing portal. During SCHEDULED, expect STAGING and
ready ranks 0/N progressing only on acknowledgements; measured values appear as
ranks finish, with “Not recorded” elsewhere. Choose Input staging in Log source;
no raw input mount path, worker credential or other student's filename should
appear in service summaries. Select Live training after launch. A warm new job
should show measured zero staged bytes. Sign in as another owner: the job should
be absent and direct requests 404. These browser/hardware steps were not run.
Do not enable flags or start services solely to perform this manual check.

Review the exact subphase diff saved as `/tmp/hybrid-7-4/7.4.patch`; unlike
`git diff HEAD`, it excludes pre-existing 7.2/7.3 modifications. From the Git root:

```bash
git apply --reverse --check /tmp/hybrid-7-4/7.4.patch
# Only if rolling back this source subphase, after preserving later edits:
git apply --reverse /tmp/hybrid-7-4/7.4.patch
```

No rollback was applied. Reverse only this reviewed source patch, never a
repository-wide reset/clean. Keep all DBs, cache objects, jobs, results,
checkpoints, mounts, credentials and services. No data/schema rollback is needed;
old code ignores the extra INPUTS_STAGED events and optional reply fields.
The /tmp snapshot/patch is session-local, so preserve it outside temporary storage
before relying on it later. No commit was created.

**7.4 local acceptance: PASS. STOP.** Phase 7
local mock lifecycle/UI acceptance is covered; production activation and validation
on **pi01, pi02 and pi03** remain pending. No previous two-Pi exception authorizes
a new live test or waives all-three-Pi acceptance.
