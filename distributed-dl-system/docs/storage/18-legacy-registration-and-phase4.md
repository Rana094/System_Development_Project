# Phase 4.4 — explicit legacy registration and Phase 4 local acceptance

8 November 2026, Asia/Dhaka. **4.4 PASS; Phase 4 artifact/manifest/registry local
acceptance PASS. STOP.** Last accepted boundary was [4.3](17-registry-and-job-inputs.md),
followed by the user's 4.4 request. This does not enable hybrid training, add a
student route, apply live schema, or deploy anything. The central controller
remains a **single point of failure**.

## 1. Implemented behavior and exact files

| File | Change |
|---|---|
| `hybrid_storage/legacy_registration.py` | New explicit per-job facade: authorize, validate controlled local snapshot, copy/verify requested inputs into CAS and commit reference-only enrollment. |
| `hybrid_storage/registry.py` | Share snapshot identity checking; add owner-authorized snapshot metadata and atomic/idempotent snapshot enrollment for legacy and HELD NFS jobs. |
| `controller/input_storage.py` | Thin explicit registration seam with a required existing verifier callback; no import of a DB-initializing module. |
| `tests/test_legacy_registration.py` | 25 new compatibility, execution, ownership, failure, retry, path and concurrent enrollment tests. |
| `docs/storage/18-legacy-registration-and-phase4.md`, `18-legacy-registration-evidence.json` | Contracts, actual commands/results, scope, hashes and rollback. |
| `hybrid_storage/README.md`, `tests/README.md`, `docs/README.md`, `docs/storage/implementation-progress.md` | Navigation, usage and completion boundary. |

No existing upload, preparation, scheduling, worker, recovery or checkpoint API
was replaced. The new operation is **reference-only enrollment**: a legacy job
still executes through legacy paths, and an NFS intent stays HELD. A registry
sidecar does not change backend selection, queue status, reservations, attempts,
leases, checkpoint formats or original paths/hashes.

### Explicit trusted-service entrypoint

```python
binding = input_storage.register_legacy_inputs(
    registry, authenticated_owner, job_id,
    managed_root=operator_managed_jobs_root,
    verify_workspace=already_loaded_job_storage.verify_workspace,
)
```

The caller supplies an already migrated registry, dedicated local FULL/FK DB
connection, authenticated owner and controlled local root. These are trusted
controller capabilities, not student-requested paths or ownership assertions.
The existing verifier is passed explicitly; importing this seam never imports
`job_storage`/`job_store` and cannot run their database initializer. No new route,
CLI apply command, automatic scan, startup hook or configuration selection exists.

"Lazy" means **on demand for the one requested job**, not automatic filesystem
loading, bulk migration or worker-cache staging. Other jobs are not inspected.
Old unregistered jobs continue to load and use the original path; enrollment is
optional and never inferred from today's configuration.

### Authorization, source validation and copy

1. `legacy_snapshot_job(actor, job_id)` checks current DB ownership before any
   workspace lookup. Only managed custom snapshots with a known post-staging
   state and DB-pinned legacy manifest are eligible. Explicit legacy and missing
   old backend selections remain legacy; no backend row is inserted or changed.
   NFS-selected jobs must still be HELD. Missing owner/snapshot metadata, RECEIVED
   jobs, built-ins and unknown selections are not guessed or automatically staged.
   Older DBs without managed snapshot columns receive a clear refusal; no schema
   upgrade or owner inference occurs.
2. Derive the workspace as `managed_root/job_id` and require exact agreement with
   the DB's stored path. Reject overlap between managed jobs and CAS/private
   scratch. Traverse roots without symlinks, attest local source descriptors and
   read bounded regular metadata through the existing no-follow reader. Check raw
   manifest SHA against the DB pin and original submission SHA against that
   manifest. No `.resolve()` escape, submitted arbitrary path or original-upload
   fallback is used.
3. Reuse `job_storage.verify_workspace` to validate the original snapshot/spec/
   inventories. Build the strict source/dataset plans before copying; retain
   source executable flags and empty directories. New traversal/case/Unicode/type
   rules can reject an older snapshot for enrollment while leaving it loadable
   in the unchanged legacy lane. No manifest repair or normalization is attempted.
4. Read only declared source/data files using no-follow regular local FDs.
   Enforce expected size/mode and stream into the existing registrar with the
   exact expected digest. Before/after FD signatures detect changes. Bytes are
   copied, not moved or hard-linked from writable legacy data. CAS uses verified
   private scratch, atomic no-replace publication and dedup from 4.1. Checkpoints,
   results, logs and training outputs are never registered as inputs.
5. Re-read job authority, re-run the existing workspace verifier and require
   unchanged pinned raw manifest bytes before final enrollment. Changed source,
   metadata, owner/spec/path or a cancelled NFS intent fails closed. No error
   marks the legacy job failed, changes its queue state or repairs its inputs.

Snapshots and path ancestors must remain controller-protected during the call.
This is not an atomic OS snapshot or a lock against privileged administrators.
The original verifier uses its existing path APIs; untrusted concurrent writers
to those roots are outside the safe enablement contract. Local synchronous I/O
must not block scheduler/lease-critical threads. No network/NFS source transfer
or Internet package download is introduced.

### Atomic metadata and safe retries

`register_snapshot(actor, job_id, legacy_manifest_bytes=..., source=...,
data_entries=...)` requires verified artifact ownership and rechecks current job
authority inside `BEGIN IMMEDIATE`. It stores source metadata, a private dataset
v1 if uploaded data exists, and exact immutable `JobInputs` in one transaction.
Generated inputs have null dataset fields, not a fabricated empty upload.

No new schema is needed: this uses the existing 4.3 tables and immutable sidecar
shape. The 4.3 migration file/checksum is unchanged. Source/data/requirements/
execution-spec/legacy hashes are bound to the original snapshot. The normal
`bind_job` API retains its original NFS-only HELD guard; the separate explicit
snapshot API does not activate NFS for old jobs.

Exact retries reuse the existing binding, dataset ID/version and CAS inodes.
Concurrent requests for one job commit one private dataset/sidecar; the loser
reuses the committed identity, not a new random dataset. Dataset head updates do
not reroute old jobs: retries refer to the pinned version. An existing shared
dataset binding still requires current ACL permission; revoked access cannot be
bypassed by re-enrollment.

Artifact publication/enrollment is incremental before the final transaction. A
failure can leave complete objects and valid owner artifact grants, but no
partial source/dataset/job sidecar transaction. Retry verifies/reuses those
objects. No automatic GC, sweep, deletion, overwrite or corrupt-object repair.
Dedup saves persistent object space; repeated registration still reads/hashes and
uses scratch I/O to verify provenance. It is not the future missing-only cache
transfer path or a scratch quota/admission system.

## 2. API, DB, configuration and security changes

New Python APIs are the controller facade and registry methods described above.
No HTTP/worker endpoint, job state, checkpoint format, source path, flag/default,
database migration or service change. Existing implicit-legacy fallback remains
valid. Explicitly enrolled legacy jobs can return their sidecar from registry
lookups, but existing execution still consults backend selection and uses the
old APIs. A reference does not convert a job's execution mode.

The real DB was not opened through a controller initializer or SQL migration;
its main-file SHA matches the turn baseline. SQLite and ACL/version metadata stay
controller-local. Actual flags remain `legacy / false / false / resource`.
Neither active services nor `/etc`, exports, fstab, firewall, SSH, mounts or Pis
were touched. NFS AUTH_SYS remains outside the per-student authorization boundary;
RO/root_squash exports, authenticated staging and exclusion of NFS/cache from
untrusted training are still required before hybrid execution. Checkpoints retain
the trusted existing publisher flow.

## 3. Actual tests and evidence

Run from the application root:

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_legacy_registration.py test_storage_registry.py test_dataset_manifests.py test_dataset_validation.py test_artifact_registration.py test_storage_readers.py test_storage_selection.py test_storage_config.py test_job_submission.py test_job_storage.py test_student_portal.py test_queue_controls.py test_resource_allocation.py test_storage_inputs.py test_storage_health.py test_storage_backends.py test_nfs_client_setup.py test_nfs_server_setup.py
env PYTHONDONTWRITEBYTECODE=1 python3 tools/run_tests.py --isolated test_storage_manifest_drafts.py
```

Final main selection: **338 passed, zero failures/skips, 18/18 files**. Before
code changes: **313 passed, zero failures/skips, 17/17 files**. New tests separately:
**25 passed**. Historical draft tests: **15 passed**, no skips, using the existing
optional system JSON Schema installation. No dependency installed.

A known offline fixture reads the staged dataset and performs two scalar
gradient updates in a local Python subprocess both before and after enrollment:
exit 0 both times, identical output `{"rows": 1, "weight": 0.38}`. This tests local
legacy workload execution and input loading; it is not Pi/agent execution,
PyTorch/DDP training, checkpoint recovery or a benchmark. Active legacy fixture
job fields, backend selection, input/checkpoint inodes/modes/bytes and original
manifest verification also remain unchanged.

The new tests cover explicit and implicit legacy selection, NFS hold preservation,
two authorized owners sharing physical objects with private access, changed bytes
creating a committed v2 without rerouting the old job, retries/concurrent requests,
SQL rollback after publication, interrupted publication, missing/corrupt files,
mid-copy mutation and authority changes, wrong/overlapping roots, symlinks,
unmanaged/older-schema/ownerless jobs, case conflicts, revoked shares, generated
data, single-job laziness and import-side-effect guards. The full suite includes
4.1 process crashes before/after rename and a simulated SHA collision rejected by
exact content comparison, plus 4.3 interrupted/concurrent/old-fixture migrations.

No baseline failure was observed. While adding the final workload test, some
implicit-backend assertions were accidentally placed in that test; one fixture
assertion failed. It was corrected without weakening checks; corrected focused
and full runs pass. The failure and final raw output are retained in
[evidence](18-legacy-registration-evidence.json), not described as a pre-existing
failure. Initial passing 23/24-case runs are local logs, not extra unique tests.

No live publication, registry migration/enrollment, NFS/Pi operation, destructive
disk/network fault or actual hardware training occurred. Test controllers and
databases are source-isolated; all data mutation/fault fixtures are disposable.
Evidence contains commands/return codes, raw logs, Python/SQLite versions,
source/DB hashes, schema checksum comparison, static checks and rollback scope.

## 4. Manual verification and expected results

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_legacy_registration.py
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python -m hybrid_storage.config
```

Expected and observed: 25 tests pass; config reports legacy runtime, both new
flags false and resource scheduling. The fixture suite executes the existing
workspace verifier and known local workload without touching a real job.
For later real enrollment, use only the explicit Python API example after the
reviewed 4.3 schema/connection/backup procedure. Do not start another controller,
import `job_store` merely to inspect data, flip flags, scan all jobs or migrate
live data as a verification shortcut. No deployment approval is implied here.

## 5. Preservation, rollback and remaining limits

Git root is parent `System_Development_Project`, branch `main`, HEAD
`ddc9a7b4a0a61288462c1999fd8c8ef1a2d01977`. Existing staged/unstaged/untracked
work is preserved; no stash/reset/clean/commit. Baseline hashes/copies of 142 files
and logs are under `/tmp/hybrid-storage-4-4/`; initial status is
`/tmp/hybrid-storage-4-4-status-before.txt`. Only the listed runtime seams and
documentation changed. The 4.3 DDL/checksum, live main DB bytes, actual config,
legacy manifests/data/checkpoints and prior progress are preserved.

Review `/tmp/hybrid-storage-4-4.patch`, containing only this turn's changes.
From the parent Git root:

```bash
git apply --reverse --check /tmp/hybrid-storage-4-4.patch
git apply --reverse /tmp/hybrid-storage-4-4.patch
```

Reverse check passed at completion. Preserve later edits and evidence; do not
reverse the entire dirty diff or reset to HEAD. No host/DB rollback is needed.
After any future actual enrollment, retain objects, grants and sidecars when
removing the helper; existing legacy execution remains unchanged. Never delete
jobs, restore a stale DB over new jobs, rewrite immutable bindings or remove
source/checkpoints to undo registration. Follow 4.3's separate downgrade gates
before older binaries that ignore NFS selection protections.

Residuals: no automatic UI/upload/background wiring, worker staging, cache,
locality policy, quota/space admission, orphan GC, automatic corruption repair
or controller HA. Strict new path/size/schema/locality constraints may reject
some historical snapshots; keep them in the legacy lane, do not silently move,
normalize or infer ownership. Protected local inputs are required; this is not
an atomic filesystem snapshot or hard-NFS timeout mechanism. The central server
is still a SPOF. All-three-Pi future validation remains the default unless
explicitly excepted; pi01 is still deferred under the prior pi02/pi03 exception,
and no cluster acceptance is inferred.

## 6. Phase 4 acceptance and stop

| Requested Phase 4 criterion | Evidence / result |
|---|---|
| Immutable bounded SHA-256 publication and concurrent dedup | PASS: registrar tests, actual local rename/process races/crashes, verified source enrollment. |
| Versioned immutable manifests, safe paths and ownership ACL | PASS: typed/codec/tree/ZIP tests and persisted registry ACL/version checks. |
| Precise source/data/requirements/job identities, idempotent FK/index migration | PASS: exact snapshot comparisons, bindings, existing/active/old/interrupted/concurrent schema fixtures. |
| Two authorized submissions reuse identical physical inputs without sharing access | PASS: two owners' actual staged legacy jobs enroll into shared CAS bytes with separate sidecars/access. |
| Changed bytes create a new digest/version | PASS: registered changed content commits dataset v2; v1 and the old job binding remain intact. |
| Legacy job still loads/executes; explicit nondestructive lazy registration | PASS: existing verifier, known local workload before/after, state/backend/file/checkpoint preservation. |
| Missing/corrupt/collision/crash/path/migration failure safety | PASS in local, injected and disposable process fixtures; not hardware fault testing. |

**4.4 and Phase 4 local code acceptance: PASS. STOP.** Production hybrid execution,
live migration/deployment and all-three-Pi validation remain unclaimed. No next
phase implemented.
