# Phase 4.3 — local registry and immutable job input bindings

8 November 2026, Asia/Dhaka. **4.3 PASS in fake/local integration tests. STOP
before 4.4.** Last accepted subphase was [4.2](16-dataset-manifests.md); the user
requested 4.3. The registry is an explicit trusted-service API, not a new route
or automatic startup migration. **No live DB was migrated.** NFS jobs remain
HELD, flags stay legacy/OFF/resource, and no training/cache/locality execution
is enabled. The central controller remains a **single point of failure**.

## 1. Implemented behavior and exact files

| File | Change |
|---|---|
| `hybrid_storage/input_manifests.py` | Strict immutable source-tree v1 codec retaining executable metadata; immutable registered-job input identity. |
| `hybrid_storage/registry_schema.py` | Explicit migration plan/apply, checksum/shape/FK checks, constraints/indexes/immutability triggers and dedicated-local-connection contract. |
| `hybrid_storage/registry.py` | Verified artifact enrollment/provenance, source manifests, committed dataset versions/current ACLs and immutable job bindings. |
| `tests/test_storage_registry.py` | 38 migration, authorization, concurrency and actual copied-controller snapshot integration tests. |
| `docs/storage/17-registry-and-job-inputs.md`, `17-registry-and-job-inputs-evidence.json` | Contracts, exact DDL/checksum, commands/results, hashes, preservation and rollback. |
| `hybrid_storage/README.md`, `tests/README.md`, `docs/README.md`, `docs/storage/implementation-progress.md` | Navigation, usage and current implementation boundary. |

No existing runtime Python file changed. Existing upload/preparation/scheduling/
recovery/DDP/checkpoint APIs, job states and backend-selection bindings remain
intact. The new API has no live call sites. It never discovers or moves old job
files. Tests explicitly provide streams from disposable, authorized snapshots;
automatic/lazy legacy enrollment remains 4.4.

### Artifact provenance and physical deduplication

`StorageRegistry(db, publisher)` takes an already configured dedicated local
SQLite connection and the existing trusted `ArtifactRegistrar`. Construction
validates an explicitly migrated schema; it does not apply one. Actors must be
the existing authentication layer's stored owner IDs, not a field asserted by
an unauthenticated caller.

`register_artifact(actor, binary_stream, expected_size=..., expected_sha256=...)`
consumes and verifies the complete incoming bytes through 4.1 before transactionally
recording `(digest, size, object_key)` and that actor's enrollment grant. Each
owner must provide authorized bytes, even on a physical dedup hit. There is no
public "grant this digest", arbitrary fetch, path-based enrollment or metadata-only
ownership API. A known digest grants nothing. Artifact size is bounded by the
signed-64-bit registry contract in addition to the registrar's bounded streaming.

Filesystem publication precedes the short DB transaction: a SQL failure can
leave a complete unreferenced object, but no successful ownership record/partial
registry transaction. Retry fully verifies/reuses that object. Never remove it
because the SQL response failed. No GC, quota manager or orphan sweep is added.

### Source and dataset identity

`SourceFile(path, sha256, size_bytes, executable)` and `SourceManifest(files,
directories)` implement the existing draft **source-tree v1 shape** with strict
path/parent/case/Unicode/size/bool validation, defensive tuples, canonical JSON
and bounded decoding. Executable flags are retained. This does not rewrite old
manifest bytes or reinterpret the new dataset-manifest family.

`register_source(actor, manifest)` requires independently enrolled ownership of
every referenced artifact and verifies actual protected local CAS bytes. It then
atomically stores canonical manifest bytes, normalized FK-backed entries and a
manifest ownership grant. Identical source-tree bytes can reuse one manifest
record while each actor has separate provenance. `get_source(actor, digest)`
checks that provenance before returning metadata. Unknown/foreign digests do not
grant access or reveal their content.

`create_dataset(actor, entries, readers=())` creates the 4.2 dataset identity/v1,
checking all input artifacts for owner enrollment and real size/hash before commit.
The canonical bytes, entries, owner/head, first version and current reader list
commit together. Owner/identity are immutable; empty directories and file sizes
are retained. `update_dataset(actor, dataset_id, expected_head_sha256=..., entries=...)`
creates a linked version after verifying owned bytes and comparing the expected
head again under `BEGIN IMMEDIATE`. Two contenders cannot both commit the same
next version; the loser receives `RegistryConflict`. There is no automatic retry
against a different head or silent overwrite of a version.

`set_readers(actor, dataset_id, readers)` loads the current owner and atomically
replaces explicit read-only grants; only the owner can do it. `get_dataset(actor,
dataset_id, exact_version)` loads **current persisted ACLs**, the precise committed
version/digest, canonical bytes and normalized entries in one read transaction.
It verifies owner/ID/version and predecessor identity. Readers cannot create new
datasets by asserting another owner's artifact hashes. Revocation affects future
reads of both old and new versions without editing immutable bytes. Already
delivered copies/running jobs are not retracted by metadata revocation alone.

Only trusted registry records select a manifest; no API accepts a parsed,
client-supplied dataset manifest/ACL as provenance. Manifest hashes/canonical
bytes and normalized entries are revalidated on reads. Byte verification occurs
outside SQLite write transactions, and authority is rechecked inside the commit.
Privileged storage modification/disk failure after a check remains possible;
stage-time byte verification is still mandatory. Metadata lookup is not a staging
receipt or proof of current NFS availability.

### Immutable job inputs

`bind_job(actor, job_id, legacy_manifest_bytes=..., source_sha256=...,
dataset_id=..., dataset_version=...)` is permitted only for an owner-authorized,
explicitly selected **NFS custom job already HELD** by the existing path. It:

1. Checks the owner, custom type, HELD state and existing NFS selection; no job
   status, backend, reservation, attempt or checkpoint field is updated.
2. Verifies raw legacy manifest SHA against `jobs.storage_manifest_sha256`,
   owner/job identity and exact current execution spec. The registered source
   must match every path/hash/size/executable flag and directory in that pinned
   inventory. File ordering is compared canonically without rewriting original
   legacy bytes (legacy traversal order can differ from full-path ordering).
3. Derives the requirements path from the stored job spec, requires it in the
   registered source and pins its actual artifact hash/size. A mutable filename
   or different requirements file cannot substitute for that identity.
4. Resolves the exact committed dataset ID/version/digest under the current ACL
   and matches its paths/hashes/sizes/directories to the pinned legacy data
   inventory. Uploaded data cannot silently become absent/generated data.
   All-null dataset fields are allowed only for the existing generated-data case.
5. Verifies actual CAS bytes, then rechecks all authority/state under a write
   transaction before inserting an immutable binding. This catches ACL revocation
   during verification. An exact eligible retry is idempotent; a different valid
   dataset with identical physical bytes still cannot rebind that job.

`JobInputs` pins source manifest SHA, dataset ID/version/manifest SHA (or null),
requirements path/artifact SHA/size, original legacy manifest SHA, owner/job ID
and a canonical execution-spec SHA. Its own canonical document has distinct
`kind=registered-job-inputs, schema_version=1`; this is not an implicit change to
the historical draft job sidecar, legacy storage v1 or checkpoint formats.

`get_job_inputs(actor, job_id)` verifies owner, binding identity/hash, unchanged
legacy/spec pins, source and requirements identity, and current dataset access.
Dataset head updates cannot reroute a pinned job to a later version. Revocation
denies subsequent access but preserves the job/binding. An old owned job without
a registry binding returns **None**, independently of current flags; its existing
legacy path remains authoritative. These methods do not permit NFS execution.

## 2. Database, API, configuration and security changes

The additive schema remains in the same **controller-local** jobs database. New
objects are:

| Table | Purpose / relational protection |
|---|---|
| `hybrid_schema_migrations` | Version-one checksum marker; unknown version/checksum fails closed. |
| `storage_artifacts` | Digest PK, unique canonical object key and `(digest,size)` identity. |
| `storage_artifact_owners` | `(owner,digest)` enrollment provenance FK; no implicit cross-owner access. |
| `storage_manifests` | Digest PK, kind and bounded private canonical BLOB; not exported on NFS. |
| `storage_manifest_entries` | Manifest/path PK and composite artifact/size FK; files vs directories constrained. |
| `storage_manifest_owners` | Separate provenance for physically shared source metadata. |
| `storage_datasets` | Immutable owner/ID, monotonic head with deferred FK into committed versions. |
| `storage_dataset_versions` | Dataset/version PK, unique manifest FK and composite binding target. |
| `storage_dataset_readers` | Current explicit read-only grants, atomically replaced by owner API. |
| `storage_job_inputs` | Job PK/FK, exact source/data/requirements/legacy/spec identities and binding hash. |

Seven explicit indexes support artifact references/grants, readers and job
source/dataset/requirements lookups. Eighteen triggers protect immutable records,
dataset ownership/head progression and deletion. Referenced jobs/data are RESTRICT;
no cascading input/job/checkpoint cleanup. The existing `job_storage_bindings`
backend table and immutable trigger are unchanged, not repurposed for sidecars.
Full DDL and SHA-256 migration checksum are in the code and evidence.

`migration_plan(db)` reads schema metadata and reports create/unchanged plus exact
statements; it issues no DDL/DML. `migrate(db)` owns a short `BEGIN IMMEDIATE`,
creates approved tables/indexes/triggers and marker transactionally, validates
their exact SQL shape and registry foreign keys, then commits. No executescript
implicit commit, data hashing, downloads, backfill or old-row rewrites occur.
Reapplication is a no-op. Partial/conflicting schema, altered indexes/columns/
triggers, unknown markers and broken registry FKs fail without automatic repair.
Full registry FK scans run at migration/planning and registry construction.
Ordinary API transactions retain FK enforcement and shape/checksum checks without
rescanning every registry row for each file enrollment.
This is the version-one migration, not a destructive generic upgrade/downgrade.

Connections must already be idle, FK=ON and synchronous=FULL/EXTRA, without
attached DBs or temporary shadow schema. Caller transactions are rejected without
commit/rollback. Main DB must be a named regular local file on the existing
ext4/xfs/btrfs allowlist, inspected with no-follow metadata FDs. The operator must
protect the DB/WAL/SHM and path ancestors; path metadata is not access to SQLite's
internal FD and is not safe against privileged path replacement. In-memory DBs
are rejected by this production contract. No journal mode or connection pragma
is silently changed. Existing `job_store.get_connection()` uses NORMAL; use a
dedicated reviewed FULL connection for this subsystem, not an active legacy
transaction. All tests use disposable local FULL connections.

No actual `jobs.db` initializer, schema apply or SQL write was run. No new route,
configuration flag/default, transport, scheduler rule, worker dependency or live
service changed. Runtime stays legacy/OFF/resource. Existing authorization must
authenticate actor IDs before the new Python API is wired. NFS inputs stay
read-only/root_squash and outside training; AUTH_SYS is not tenant authorization.
Checkpoints retain the trusted existing protocol. Registry/ACL metadata is not
published into the NFS object export.

## 3. Tests and actual results

From the application root:

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_storage_registry.py test_dataset_manifests.py test_dataset_validation.py test_artifact_registration.py test_storage_readers.py test_storage_selection.py test_storage_config.py test_job_submission.py test_job_storage.py test_student_portal.py test_queue_controls.py test_resource_allocation.py test_storage_inputs.py test_storage_health.py test_storage_backends.py test_nfs_client_setup.py test_nfs_server_setup.py
env PYTHONDONTWRITEBYTECODE=1 python3 tools/run_tests.py --isolated test_storage_manifest_drafts.py
```

Final main selection: **313 passed, zero failures/skips, 17/17 files** (38 new +
275 existing). New tests separately: **38 passed**. Pre-change baseline:
**275 passed, zero failures/skips, 16/16 files**; no pre-existing selected failure
observed. Historical draft compatibility: **15 passed**, no skips, with the
already available optional system JSON Schema package. No dependency installed.

The tests copy the controller before any `job_store` import; real `stage_job` and
`verify_workspace` operate solely on temporary jobs/DBs. Two owners' staged jobs
register identical bytes, reuse physical objects, and acquire separate immutable
bindings with access denied across owners. Migration preserves an active legacy
job, retry, reservation, attempt, lease/generation and checkpoint-byte fixture.
Concurrent migrators serialize; two version writers yield one new commit and one
conflict. Injected transaction failure leaves a complete unreferenced object and
no partial registry; retry reconciles it. Tests also cover schema rollback,
unknown marker/shape, missing indexes, bad FKs, caller transactions, immutable
records, source codec, corrupt/missing bytes, forged snapshots, requirements/
dataset mismatch, current ACL revocation (including during verification), old
job fallback and unchanged runtime APIs. These are fixture checkpoints, not an
actual training/DDP recovery run.

No new-test failures were observed in the completed runs. Exact commands, output,
SQLite/Python versions, DDL/checksum, source/DB hashes and static checks are in
[evidence](17-registry-and-job-inputs-evidence.json). Tests are source-isolated;
no live DB, NFS/Pi, destructive fault, benchmark or Internet runtime transfer.

## 4. Manual verification / later deployment procedure

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_storage_registry.py
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python -m hybrid_storage.config
```

Expected and observed: 38 tests pass; configuration reports legacy execution,
both new flags false and resource policy. The isolated tests exercise plan/apply/
reapply, old fixtures and exact input binding; no real registry is created.

For a later reviewed live migration: quiesce enrollment/dispatch as appropriate,
take a consistent SQLite backup with referenced input/checkpoint inventory,
check space and protect local DB/WAL/SHM and input roots. Open one dedicated
trusted FULL/FK connection, review `migration_plan`'s statements/checksum, then
apply `migrate` only after explicit deployment approval. Validate FK checks and
unchanged old jobs/checkpoints before any enrollment. Do not copy a loose WAL DB,
put SQLite on NFS, start a second dispatcher or apply this plan as a probe.
Nothing in this subphase grants permission to change `/etc`, services or Pis.

## 5. Preservation, rollback and residual risks

Git root is parent `System_Development_Project`, branch `main`, HEAD
`ddc9a7b4a0a61288462c1999fd8c8ef1a2d01977`. Existing dirty work and all 136
baseline-file contents are preserved except the documented README/progress
additions. Before copies/hashes/logs are in `/tmp/hybrid-storage-4-3/`; initial
status is `/tmp/hybrid-storage-4-3-status-before.txt`. Progress is appended;
existing runtime source, live main DB hash, config, datasets/jobs/checkpoints,
legacy/draft manifest bytes and backend-selection code are unchanged. No
stash/reset/clean/commit or service/infrastructure/remote operation.

Review the turn-only diff `/tmp/hybrid-storage-4-3.patch`. From the parent Git root:

```bash
git apply --reverse --check /tmp/hybrid-storage-4-3.patch
git apply --reverse /tmp/hybrid-storage-4-3.patch
```

Reverse check passed at completion. Preserve later edits/evidence; do not reset
to HEAD or reverse the whole dirty diff. No host/DB rollback is needed here.
After a future migration/enrollment, leave additive schema, bindings and objects
intact; do not drop tables, delete inputs or restore a stale DB over newer jobs.
Before downgrading to binaries that ignore registry/selection gates, drain or
hold/cancel affected jobs with current handlers and preserve recovery metadata.

Residuals: explicit API only, no upload/startup wiring or lazy legacy enrollment
(4.4), GC, quota/space admission, automatic repair, power-loss/hardware validation
or authenticated worker staging. CAS bytes can fail after verification; a row is
not a stage-time receipt. Revoked sharing denies future registry reads but does
not delete jobs or retract delivered private copies. Local synchronous hashing
must stay off lease/scheduler-critical threads. Protected filesystem/DB authority
is required against malicious student code; privileged controller/worker compromise
is outside this trust boundary. All-three-Pi validation remains required unless
expressly excepted; pi01 remains deferred under the earlier pi02/pi03 exception,
and no live cluster readiness is claimed. Controller failure is still a SPOF.

## 6. Acceptance / stop

| 4.3 criterion | Result |
|---|---|
| Exact job source/data/requirements identities, independent of mutable filenames | PASS in copied-controller integration |
| Idempotent additive migration, FKs/indexes and no old-job rewrites | PASS, including concurrent/interrupted/incompatible fixtures |
| Physical dedup without cross-owner authorization | PASS, including two separate owners' staged jobs |
| Legacy/OFF behavior and user-data preservation | PASS in selected regressions; no live migration |

**4.3 PASS in this explicit local integration scope. STOP before 4.4.** Phase 4
overall and production hybrid training are not complete. No next subphase or
deployment implemented.
