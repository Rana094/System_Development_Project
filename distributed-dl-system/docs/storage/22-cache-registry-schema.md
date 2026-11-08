# Phase 6.1 — controller-local advisory cache registry schema

8 November 2026, Asia/Dhaka. **6.1 schema/local acceptance PASS; STOP before
6.2.** This does not enable inventory ingestion, locality scheduling or NFS
execution. The controller remains a **single point of failure**.

## 1. Actual baseline, behavior and exact files

The user committed prior cache work in `89e213591969692d5ac700a1ca3585600bb37d56`;
the checkout was clean at this subphase's baseline. Last documented acceptance
was 5.3. The 5.4 metrics edits are in that commit, but its dedicated metric/fault
acceptance tests and completion report were not finished before the user moved
to Phase 6. Phase 5.4/overall Phase 5 acceptance remains pending. This turn
implements **6.1 only**, preserving those committed sources without alteration.

| File | Change |
|---|---|
| `hybrid_storage/cache_registry_schema.py` | Explicit additive plan/apply/require API, versioned checksum, constraints, indexes and history/session guards. |
| `tests/test_cache_registry_schema.py` | 22 safe copied-controller/disposable SQLite tests. |
| `docs/storage/22-cache-registry-schema.md`, `22-cache-registry-schema-evidence.json` | Exact contracts, evidence, auth gap, commands and rollback. |
| `hybrid_storage/README.md`, `tests/README.md`, `docs/README.md`, `docs/storage/implementation-progress.md` | Navigation and implementation boundary. |

No live DB was migrated. No controller module that initializes the production
DB was imported for this work; tests use existing copied-controller fixtures.
No workers/artifacts/inventory are automatically enrolled/backfilled from
`config/nodes.json`, jobs, filenames or reported hashes.

### Schema objects and field ownership

Six new tables, seven indexes (including one unique partial index), twenty
triggers. The existing immutable `storage_artifacts(sha256,size_bytes)` registry
is reused through composite foreign keys, without duplicating artifact authority.

| Table | Purpose |
|---|---|
| `cache_registry_migrations` | Independent migration version/checksum/applied timestamp; does not repurpose `PRAGMA user_version`. |
| `cache_registry_workers` | Stable configured worker ID, controller-owned authentication principal/state, enabled flag, controller last-seen and creation/update times. Defaults disabled/UNENROLLED. No credential secrets. |
| `cache_registry_identities` | Persistent cache ID bound to worker, monotonic epoch high-water and irreversible retirement marker. History retained. |
| `cache_registry_sessions` | Controller-issued session nonce/controller epoch, cache epoch, canonical worker boot UUID, auth binding, sequence/body cursor, full-snapshot flag, inventory receipt/freshness/last-seen times and disk total/free/available bytes. Defaults UNVERIFIED. |
| `cache_registry_entries` | Unique digest per session, exact worker/cache/epoch binding, expected artifact size, VERIFIED/UNVERIFIED/MISSING/CORRUPT/REMOVED observation, verified bytes, last verification and accepted sequence/update time. |
| `cache_registry_events` | Append-only `(session_id,sequence)` ledger with worker binding, FULL/CACHE_ADDED/CACHE_REMOVED kind, delta base sequence, body SHA and controller receipt time. No payload transport/API yet. |

Timestamps are signed-64-bit integer nanoseconds, separate from existing job
timestamps. Creation/update/receipt/freshness/last-seen times are controller-owned;
`last_verified_at_ns` is an authenticated worker's eventual observation and does
not itself establish freshness. Byte counts are integer bytes. VERIFIED entries
must reference an exact existing artifact size, carry that full verified byte
count and a verification time; other states contribute zero verified bytes.

Indexes cover worker freshness, worker/cache high-water, active/fresh sessions,
worker/session/state, artifact-to-worker lookup and event receipt time. At most
one ACTIVE session exists per worker. Epoch advance/retirement or worker
revocation/disable/principal change invalidates active sessions without erasing
observations. Immutable identity fields, monotonic cursors, same-sequence body/
freshness guards and irreversible STALE/CLOSED sessions provide defense in depth.
Old sessions require a new authenticated handshake, not reactivation.

These are static storage constraints, **not** an ingest protocol or eligibility
API. Runtime code must still authenticate messages, validate exact current
controller/cache/worker sessions and sequence bodies transactionally, enforce
time-based freshness/offline exclusion and reconcile snapshots. A stored ACTIVE
string or claimed verified bytes is not proof of live files or authorization.

## 2. Actual authentication finding and API/config/security changes

The requested authenticated runtime registration/heartbeat path does **not**
currently exist in the inspected code:

- `tools/cluster_setup.py::register` updates operator inventory; installation
  uses SSH, while `probe` checks self-reported node IDs over plain HTTP.
- `controller/controller.py::check_node` polls `/resources` without worker
  authentication. `controller/scheduler.py::get_resources` does the same.
- `controller/dispatcher.py::monitor_running_jobs` polls job HTTP responses and
  checks execution ownership. `agent/agent.py::fence_downloads` uses attempt/
  generation/lease fields for stale-attempt fencing, not cryptographic identity.

SSH operator access, IP/hostname, self-reported node IDs, NFS AUTH_SYS and execution
lease IDs must not be presented as an authenticated inventory channel. New rows
default to disabled/untrusted, and there is no route/API to mark them trusted.
Test fixtures explicitly set synthetic controller-side auth metadata to test
constraints; they do **not** prove real worker authentication.

Before 6.2 accepts any report, establish/review an authenticated worker channel
bound to the existing operator SSH enrollment, current stable `DL_NODE_ID`,
controller-issued session, persistent cache identity/epoch and boot identity.
Reuse deployment/identity conventions and lease fencing; do not invent an HTTP
artifact download API or trust inventory-supplied identity. Credential issuance/
rotation, request authentication, replay handling and reconnect reconciliation
remain 6.2 work; none is provisioned/deployed here.

The only new callable API is explicit `migration_plan(db)`, `migrate(db)` and
`require_schema(db)`. No connection or migration occurs on import/startup. No
HTTP route, heartbeat writer, coverage API, feature flag/default, job state,
checkpoint format or scheduler/resource/fairness/reservation rule changed.
Actual config remains **legacy / false / false / resource**.

## 3. Migration contract and actual tests

Use a dedicated idle controller-LOCAL SQLite connection with foreign keys ON,
synchronous FULL (or EXTRA), no attachments or temporary schema. The actual
opened DB descriptor must attest a supported local filesystem; NFS/unknown
filesystems are refused. The existing artifact registry v1 must already have
been explicitly migrated. This migration never silently applies that prerequisite.
The ordinary job-store connection uses NORMAL and is intentionally not reused.

`migration_plan` reads without modifying data. `migrate` uses BEGIN IMMEDIATE,
validates the prerequisite and current namespace, creates all objects and its
marker in one transaction, and rechecks shape/FKs. Exact repeat is a no-op;
unknown future/checksum/partial/index/trigger shapes fail rather than being
repaired. Existing rows, user_version, jobs, leases and artifacts are retained.

Version: **1**. Exact DDL checksum:
`e697fc1f0fc273053371c4166b0990d6f8ee497aeccf6b6a6ed6f685478d14fc`.

Baseline: **443 passed, zero failures/skips, 21/21 files**. Initial 19 new schema
tests passed. Adding stale-session/future-marker guards yielded 21 passing new
tests and a 464-test combined selection. Final canonical-boot-ID hardening plus
its regression yields **465 passed, zero failures/skips, 22/22 files**, including
**22 new schema cases**. No selected baseline/new-test failure occurred.

Exact final command from the application root:

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_cache_registry_schema.py test_private_workspaces.py test_cache_fetch.py test_worker_cache.py test_legacy_registration.py test_storage_registry.py test_dataset_manifests.py test_dataset_validation.py test_artifact_registration.py test_storage_readers.py test_storage_selection.py test_storage_config.py test_job_submission.py test_job_storage.py test_student_portal.py test_queue_controls.py test_resource_allocation.py test_storage_inputs.py test_storage_health.py test_storage_backends.py test_nfs_client_setup.py test_nfs_server_setup.py
```

Tests cover read-only planning/import, exact repeat, preservation of all prior
table rows and user_version, artifact/worker/session/epoch FKs, size/verified-byte/
disk/time constraints, no automatic enrollment, untrusted defaults, history and
cursor guards, identity/boot validation, invalidation without deletion, rejected
future/partial schemas, interrupted transaction rollback and three independent
concurrent migration connections. Existing registry registration still works.

[Evidence](22-cache-registry-schema-evidence.json) retains exact commands/raw
output and before/after hashes. No live migrations, SSH/Pi/NFS checks, credentials,
authentication integration, hardware reboot/power loss, coverage/locality API or
502 MB fixture calculation is claimed. This is not the full repository suite.

## 4. Safe manual verification

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_cache_registry_schema.py
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python -m hybrid_storage.config
git diff --check
```

Expected: **22 tests pass**, config reports legacy/off/resource, no whitespace
errors. All migrations target disposable copied-controller DBs. For later live
application, first review the plan and take a consistent SQLite backup including
WAL state; do not copy a running main DB alone. Do not run a migration against
the real job DB simply to check readiness.

## 5. Rollback and residual risks

Source/docs-only patch: `/tmp/hybrid-storage-6-1.patch`. From the parent Git root:

```bash
git apply --reverse --check /tmp/hybrid-storage-6-1.patch
# Only after review and confirming no subsequent overlapping changes:
# git apply --reverse /tmp/hybrid-storage-6-1.patch
```

Reverse-check passed; earlier committed storage/metric edits are retained. No
host/DB rollback is needed because no live schema was applied. After any later
deployment, disable ingestion/locality and preserve all new registry/history
tables, jobs, DB/WAL, leases, artifacts and cache data; do not DROP tables or
restore an old backup over newer jobs to downgrade code. No destructive down
migration/GC exists. Append-only history will need later reviewed retention.

Authentication/ingest/reconciliation remain 6.2; authorized unique-digest coverage
and actual per-rank requirements remain 6.3; comprehensive stale/offline/event/
concurrent-write acceptance and exact pi01=0/pi02=2/pi03=502 MB coverage remain
6.4. Cache inventory is advisory and stage-time verification remains mandatory.
All-three-Pi live validation remains required unless explicitly excepted; prior
pi01 deferral establishes no new waiver or three-node pass.

## 6. Acceptance boundary

**6.1 schema/local acceptance PASS. STOP before 6.2.** No runtime authentication,
inventory ingestion, scheduling enablement or overall Phase 6 PASS is claimed.
The auth gap and unfinished 5.4 acceptance are explicit missing context, not
silently assumed completed features.
