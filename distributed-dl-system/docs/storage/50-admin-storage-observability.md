# Phase 12.2 — admin storage panel in the existing portal

10 November 2026, Asia/Dhaka. **12.2 only.** Last completed subphase was
[12.1](49-structured-events.md); the user requested 12.2. No new website,
deployment, production admin grant, live worker probe or storage enablement.

## Implemented behavior and source changes

Repository inspection found the existing student Flask portal and a console
cluster dashboard, but **no existing web admin role**. Added a locally granted
admin role and a storage section to that same portal. The existing job pages,
student ownership checks, scheduler, staging barriers and checkpoint formats
remain authoritative and unchanged. Being an admin for storage does not grant
access to another student's job details, logs, results or controls.

The panel lists configured workers, including pi01/pi02/pi03 with the current
inventory. It shows reported verified cache bytes, local disk total/available
capacity, dataset coverage, inventory age/last-seen, NFS mount identity and recent
successful staging transfers. Fields without trustworthy observations show
**Unknown**, not zero or healthy. The existing inventory does not report the
cache's configured quota, so `cache_limit_bytes` remains null; disk capacity is
labeled separately and never substituted as a quota.

| File | Exact integration |
|---|---|
| `controller/portal_admin.py` (new) | Private local admin allowlist, CLI grant/revoke helpers, fresh fail-closed role checks. |
| `controller/admin_storage_view.py` (new) | Read-only, bounded registry/coverage/transfer projection without other students' identifiers. |
| `controller/student_portal.py` | Reuse sessions; add `is_admin` session/login metadata, guarded `GET /api/admin/storage`, local role CLI switches. |
| `controller/portal_static/index.html` | One initially hidden admin section in the existing workspace. |
| `controller/portal_static/portal.js` | Existing refresh loop renders admin-only aggregate table/history, escapes values, clears data on logout/account switch, fences late responses. |
| `controller/portal_static/portal.css` | Two small styles using existing panel/table components. |
| `hybrid_storage/admin_observations.py` (new) | Optional kernel-only NFS identity probe and bounded atomic private LOCAL observation snapshot. |
| `hybrid_storage/inventory_protocol.py` | Accept explicit opt-in inventory v3 with `transfer` and advisory `nfs`; v1/v2 bodies remain unchanged. |
| `hybrid_storage/inventory_reporter.py` | Optional `nfs_health=` callback, using the same request-bound per-worker HMAC. Omission preserves v1/v2 behavior. |
| `hybrid_storage/cache_inventory.py` | Optional `admin_observations=` writer, invoked only after successful authenticated/current-session DB acceptance; diagnostic failures do not fail inventory. |
| `tests/test_admin_storage.py` (new) | Sixteen actual Flask/SQLite/signed-worker/JS integration checks in disposable fixtures. |
| `tests/test_admin_storage_ui.cjs` (new) | Actual portal-script DOM/fetch harness: values, permissions, escaping, logout race and revoked access. |
| `docs/student-interface.md`, `hybrid_storage/README.md`, `tests/README.md`, `docs/storage/implementation-progress.md` | Operator usage, component/test documentation and progress. |
| This report and `50-admin-storage-observability-evidence.json` | Exact commands, results, source hashes, limitations and rollback. |

## API, authentication, DB, configuration and security

`GET /api/admin/storage` requires the existing authenticated session **and** a
current entry in protected `storage/portal/admins.json`. Anonymous returns 401;
ordinary students return 403 before opening the diagnostic DB. A forged
`is_admin` session field or login field is not a grant. No account, including an
account named `admin`, receives implicit privileges. Revocation is checked on
every request. Malformed, linked, public/group-accessible or unsupported remote
allowlist files deny access. Grant/revoke use the existing local account lock,
0600 temporary file, fsync and atomic replacement; no browser role-assignment API.
Existing logins remain valid without any users.json format migration.

`/api/session` and successful `/api/login` add boolean `is_admin`; all existing
fields remain. Admin API output contains numeric aggregates and configured
worker IDs, **no usernames, job IDs, dataset IDs/names, filenames, hashes, lease
tokens, credentials, IPs or shared paths**. Existing student routes continue to
enforce ownership even for admin accounts. Responses retain no-store/CSP
headers. UI is hidden by default and cleared on logout/account switch; pending
admin responses cannot populate a later student's DOM. A revoked/failed poll
replaces previous panel data with a generic unavailable message.

Every admin poll uses a controller-LOCAL **mode=ro SQLite connection**, a single
read transaction, 250 ms lock timeout and a 500 ms SQLite progress budget. There
are **no schema migrations, new DB tables or per-poll DB writes**. Missing optional
tables are normal for legacy installations and yield unknown values. Query
failure yields a fixed 503 message, not filesystem/SQL error details. There is
no remote HTTP/SSH/NFS data read during dashboard polling.

Coverage sums each unique digest once **per current dataset head**, for up to
20 dataset heads in deterministic dataset-ID order; the response and UI flag a
truncated window. Separate datasets sharing bytes each contribute to this
logical coverage denominator; it is not physical storage usage. A missing
zero-byte object prevents a version from being marked complete. Stale/offline/
revoked inventory yields null coverage/cache totals, never unsupported cache
certainty. Historical disk capacity is labeled last reported. Freshness requires
active/enrolled/session/identity/full-snapshot agreement, unexpired registry TTL
and both persisted heartbeat timestamps within a conservative **15 seconds**.
This is an operator observation, **not scheduler eligibility**: the portal
cannot reproduce the separate inventory owner's in-memory epoch/context proof.

Transfer history reuses existing controller `INPUTS_STAGED` events and exact
stage/rank/job/attempt joins. It exposes at most 10 records per worker from a
300-event window, each JSON payload capped at 16 KiB; duplicate stage/rank
observations are suppressed. Only validated numeric staging metrics appear.
Warm successful stages legitimately report zero transferred bytes. Durations
are seconds derived from nanoseconds; copy/verification spans overlap. Payload
bytes are not NIC traffic. No new transfer log/table or speed test is introduced.

Active storage flags stay **legacy / false / false / resource**. No existing
bootstrap automatically opts into new telemetry or admin grants. NFS AUTH_SYS
and cache inventory remain advisory, not a student authorization boundary. The
controller, LOCAL SQLite and authoritative store remain a single point of failure.

## Optional signed NFS observations and compatibility

The existing inventory did not report worker NFS health. Added a narrowly scoped
optional **inventory v3**, on the **same existing HMAC-authenticated HTTP route**,
not a new transport. Its extra `nfs` object is exactly:

```json
{"schema_version":1,"status":"verified_readonly","observed_at_ns":1794268800000000000}
```

Other accepted status: `unavailable`. Malformed/missing telemetry becomes null;
no exception string/source/path is transmitted. An operator may supply
`NFSHealthProbe(expected_source, target=expected_mountpoint)` to the trusted worker
`InventoryReporter(..., nfs_health=probe)`. It reads **kernel mount metadata only**
through the existing strict `require_input_mount` guard; it checks expected NFS
identity/read-only options without triggering automount or opening NFS files.
It does **not** prove network/server I/O availability, and the panel says
**I/O availability: untested** even for a verified read-only mount.

In the already reviewed trusted controller bootstrap, opt in with
`CacheInventory(..., admin_observations=AdminObservations(state_root))`, where
`state_root` is the same private, service-owned LOCAL parent of the actual jobs
and cache-registry DB. This is a constructor sample, not a new ready-to-deploy
bootstrap. Existing credentials, connection factory and worker isolation remain
required; none is created or exposed by the panel.

After HMAC/worker/sequence/session checks and DB commit, the controller writes
`cache-admin-observations.json` beside that DB: at most 256 workers, 256 KiB,
0600, unique temporary file/fsync/atomic replace on the same LOCAL filesystem.
No SQLite-on-NFS or append-only telemetry growth. Startup clears the volatile
snapshot; diagnostic write errors do not prevent inventory acceptance. Duplicate
or tampered responses cannot refresh it. Portal health requires the snapshot's
session ID, sequence and controller receive time to match the **current DB
session/heartbeat**, plus fresh valid probe time within 30 seconds. Unavailable,
stale, mismatched, missing or insecure snapshots cannot display healthy status.

Upgrade controller/parser **before** opting workers into v3. Old controllers
supporting only v1/v2 reject v3; therefore the callback remains omitted by
default. Existing v1/v2 reporters/transfer collectors still work with the new
controller, with NFS health unknown. No live worker was upgraded or enabled here.

## Actual tests and evidence

Baseline before edits: **72/72 tests across three files PASS**, 50.0 seconds:

```bash
PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_student_portal.py test_staging_portal.py test_cache_inventory.py
```

Final regression: **155/155 distinct tests across seven files PASS**, zero
failures/errors/skips, 115.8 seconds:

```bash
DL_NODE_BINARY=/usr/lib/chatgpt/resources/cua_node/bin/node PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_admin_storage.py test_student_portal.py test_staging_portal.py test_cache_inventory.py test_cache_coverage.py test_transfer_metrics.py test_storage_events.py
```

| File | Cases | Result |
|---|---:|---|
| `test_admin_storage.py` | 16 | PASS |
| `test_student_portal.py` | 17 | PASS |
| `test_staging_portal.py` | 17 | PASS |
| `test_cache_inventory.py` | 38 | PASS |
| `test_cache_coverage.py` | 31 | PASS |
| `test_transfer_metrics.py` | 17 | PASS |
| `test_storage_events.py` | 19 | PASS |

The sixteen new checks include actual private-file roles and immediate revoke,
401/403 before data access, multi-owner aggregate redaction, readonly legacy
polls, signed full v3/disk/cache/NFS reporting, alias/zero-byte dataset coverage,
stale/offline/replayed/spoofed sessions, malformed/absent v1/v2-compatible health,
failed snapshot writes, bounded dataset/snapshot windows, genuine existing
staging history through signed Flask fixtures, and actual portal JavaScript.
Isolation/trainer/NFS facts in those fixtures are mocked; no physical Pi result
or production hostile-code sandbox acceptance is claimed.

Separate unchanged student UI harness also PASS:

```bash
/usr/lib/chatgpt/resources/cua_node/bin/node tests/test_staging_portal_ui.cjs
```

Node was absent from shell PATH on the initial new-test run (one test error);
used the existing bundled Node executable above and reran successfully. No
package was installed. On other machines use `DL_NODE_BINARY=/absolute/path/to/node`
or make Node available on PATH; it is a development-only UI test dependency.

Visually checked the actual panel HTML produced by the current rendering
function with current portal CSS in the in-app browser, including expansion of
transfer history. **All displayed preview data are synthetic**, with unknown,
fresh and stale examples for all three worker IDs. Saved
`/tmp/hybrid-12-2/admin-preview.png`. This preview disabled live scripts and used
only a temporary localhost static fixture, not a production login/backend or
live worker data. Initial sandbox localhost bind failed; the approved temporary
fixture server then ran on 127.0.0.1:40393 and was stopped (exit 130), and its tab
closed. No unrelated service/port/process was stopped. No browser/dependency
download. Mobile/accessibility acceptance is not claimed.

Raw logs, baseline snapshot/hash catalog, source-only patch, screenshots and
reverse-apply evidence are under `/tmp/hybrid-12-2/` and indexed by the companion
evidence JSON. `git diff --check` and scoped reverse-apply checks PASS. Original
308-file snapshot retained; only the listed eleven original files changed,
with seven new files. Prior uncommitted phases are preserved. No commit/stash/
reset or production service/database/data/mount/config changes.

## Manual verification and rollback

After normal reviewed deployment of this source, the local operator can grant
an **existing login** access (replace `operator_login` with its actual ID):

```bash
.controller-venv/bin/python controller/student_portal.py --grant-admin operator_login
```

No grant was applied to production during this subphase. Reload/sign in to the
existing portal: the admin sees the Cluster storage panel with all configured
workers; ordinary students do not request the admin endpoint. Missing registry/
health instrumentation shows Unknown. For confirmed observations, use only the
reviewed opt-in trusted constructors described above; do not deploy or change
services/mounts until separately approved. The old HTTP upload/checkpoint flow
and storage flags need no change for this panel.

Immediate access rollback, using the same local operator context:

```bash
.controller-venv/bin/python controller/student_portal.py --revoke-admin operator_login
```

Next API access is 403 and cannot serve new data. Disable optional diagnostic
writing by omitting `admin_observations=` and optional worker v3 by omitting
`nfs_health=` at the next **reviewed** bootstrap. Do not delete DBs, authoritative
objects, checkpoints, cache objects or user files. For source rollback, review
the 12.2-only patch based on the **pre-turn working tree**, then from app root:

```bash
git apply --reverse --check /tmp/hybrid-12-2/12.2.patch
git apply --reverse /tmp/hybrid-12-2/12.2.patch
```

Apply only if the check succeeds. Actual reverse application was verified in a
disposable source copy, never the working checkout. Do not restore whole files
from HEAD: they contain earlier uncommitted phases. Protected diagnostic/grant
files belong only to an explicitly enabled future/local test setup; this turn
created them only in disposable fixtures, so production needs no data rollback.

Residual limits: conservative wall-clock diagnostic freshness is not controller
epoch/lease proof; cache quota and NFS I/O reachability remain unreported; head/
history windows are intentionally bounded; optional snapshot writes do fsync at
authenticated inventory frequency and are not overhead-benchmarked here. Full
Phase 12.4 retention/concurrency/overhead/timezone/stale-UI acceptance is pending.
No Pi was contacted, and prior pi01 deferral is retained. The historical loaded-
run HTTP409 issue remains disclosed, not root-caused by this subphase.

**12.2 PASS:** existing portal reused, admin-only aggregate storage view backed
by actual registry/staging records and optional signed mount observations,
unknowns truthful, legacy/student behavior preserved. **STOP before 12.3.**
