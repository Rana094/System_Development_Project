# Phase 3.1 — strict controller input adapters

7 November 2026. **Subphase 3.1 implemented and locally tested. STOP before 3.2.**
The last completed phase is [Phase 2](10-phase2-completion.md), accepted within
the user's explicit pi02/pi03 scope exception; pi01 remains deferred. No Pi was
contacted during this subphase. Phase 3 as a whole is not complete.

## 1. Implemented behavior and changed files

The existing `StorageBackend` protocol and Phase 1 local/NFS adapters already
provide no-follow regular-file reads and bounded, verified, exclusive local
publication. Their general relative-key contract is used by existing tests and
the public Phase 2 TEST artifacts. Preserve it and add strict input views around
it, rather than changing its meaning or rewriting the legacy controller.

| File | Change |
|---|---|
| `hybrid_storage/inputs.py` | New digest-derived object adapters, validated file references, project-path validation and read-only allowlisted project views. |
| `controller/input_storage.py` | New explicit trusted-controller seam: `local_objects` and `project_inputs`; no DB or runtime selection. |
| `tests/test_storage_inputs.py` | 18 new local tests, including a real legacy staging/object roundtrip in a temporary controller copy. |
| `docs/storage/11-controller-input-adapters.md` | Contracts, test results, manual checks, rollback and limits. |
| `docs/storage/11-controller-input-evidence.json` | Actual test output, baseline comparisons and source hashes. |
| `hybrid_storage/README.md`, `tests/README.md`, `docs/README.md` | Discoverability and safe local verification. |
| `docs/storage/implementation-progress.md` | Append the delivered subphase and remaining gates. |

Repository root is the parent `System_Development_Project`, branch `main`.
The working tree already had extensive staged/unstaged/untracked user changes.
Beginning-of-turn Git status is `/tmp/hybrid-storage-3-1-status-before.txt`;
source/config/document/DB hashes and original edited files are under
`/tmp/hybrid-storage-3-1/`. No stash, reset, clean or commit was performed.

During this turn, an external commit appeared: `ddc9a7b` by rana, “Initialized
Hybrid file system and environment” (23:38:48 +06:00). It recorded the pre-existing
staged deletions; Git now reports main ahead by one. This task did not create,
revert or amend that commit. Consequently the original Git status rows are not
identical. Content-hash comparison still shows only the four intended existing
documentation files changed by this subphase; the live DB and existing storage
code remain identical to the beginning-of-turn bytes.

### Exact interfaces

Import strict components from `hybrid_storage.inputs`; existing package-level
exports remain unchanged, preserving the standalone Phase 2 setup bundles.

| Interface | Contract |
|---|---|
| `object_key(sha256)` | Only 64 lowercase hexadecimal characters; returns `sha256/<first-two>/<full-digest>`. No URI, raw path or alternate spelling. |
| `validate_object_key(key)` | Require exactly that layout, including matching two-character prefix, before filesystem access. |
| `InputFile(path, sha256, size_bytes)` | Frozen expected identity; canonical project path, valid digest, genuine nonnegative integer size. This is not proof of bytes or authentication. |
| `LocalObjectStorageBackend(root, staging_root=None)` | Reuse `LegacyStorageBackend` against trusted operator-configured controller-local roots. Publication requires an explicit protected staging capability and matching key/digest. |
| `NFSObjectStorageBackend(mounted_root)` | Reuse `NFSStorageBackend`; reads only, always rejects publication, including on writable fake directories. Does not mount or attest a mount. |
| `ProjectInputView(backend, files, objects=False)` | Snapshot a trusted per-job file allowlist. For a controlled legacy root, read the exact listed path. With `objects=True`, derive the object's key from its listed digest. |
| `controller.input_storage.local_objects(input_root, staging_root=None)` | Explicit controller-local object factory; never selects a worker NFS publisher from config/job data. |
| `controller.input_storage.project_inputs(managed_source_root, files)` | Read-only view of an already owner-checked, verified legacy source snapshot. |

Strict backends/views satisfy the existing `StorageBackend` protocol. They retain
`artifact_exists`, advisory `resolve_path`, no-follow `open_artifact` and controlled
`publish_artifact`. Actual reads must use `open_artifact`; a returned diagnostic
Path is not a safe-open token or authorization grant.

Project paths reject absolute paths, traversal/dot/empty components, repeated or
trailing slashes, backslashes, colons, controls, DEL and Unicode surrogates. Paths
are not normalized, case-folded or URL-decoded. Valid Unicode names remain valid.
The immutable allowlist rejects duplicate paths, a file used as another file's
parent, and conflicting declared sizes for one digest. Unlisted paths raise
EACCES before touching disk, even if that file or guessed digest actually exists.
Listed missing files retain the existing distinction: existence is false, while
open raises FileNotFoundError. Project views cannot publish.

Publication derives authority only from trusted service configuration: it uses
the controller's local filesystem adapter and private staging, verifies bytes,
sets 0444, and exclusively publishes without replacing existing objects. The
object key must match `expected_sha256` before consuming source bytes. Roots and
`sha256/<prefix>` parents must already be provisioned; constructors never create
directories or move data. Worker publication always fails with EROFS.

## 2. API, DB, configuration and security boundaries

No existing HTTP route, upload handler, storage function, job state, DB schema,
checkpoint/recovery/DDP interface, scheduler or storage flag changed. In-flight
legacy jobs keep their original snapshots and manifests. The current graph is:

```text
existing POST /api/jobs -> student_portal.submit -> job_storage.stage_job
                        -> verify_workspace -> job_store.mark_staged
                                  [unchanged]

future trusted controller caller, after existing owner/manifest verification:
  input_storage.project_inputs -> ProjectInputView -> LegacyStorageBackend
  input_storage.local_objects  -> LocalObjectStorageBackend -> LegacyStorageBackend
  trusted worker read view    -> NFSObjectStorageBackend    -> NFSStorageBackend
```

The new seam is importable but not wired into those routes or selected at runtime.
There is no new digest-fetch endpoint, transport, background job or automatic
registration. The controller factory takes trusted local configuration, not
absolute paths from students. Before constructing a project view, its future
caller must check the authenticated owner/job and call the existing
`job_storage.verify_workspace` against the authoritative DB-bound legacy manifest.
InputFile validation alone does not establish this authority. Do not construct
allowlists from raw request JSON or use knowledge of a digest as permission.

The production roles established in Phase 2 remain controller-local
`/srv/ml-shared/objects` and private `/srv/ml-shared/.publish`, with the read-only
worker view `/srv/mlnfs/inputs`. These are not new runtime configuration fields.
No production data was copied or published here. Existing TEST keys are not
canonical object keys; they remain available through the unchanged older adapter
and are not renamed, overwritten or imported into the strict namespace.

The four flags still read legacy/false/false/resource. The existing startup gate
continues rejecting unsupported hybrid execution. SQLite remains local and was
not imported by the new module or migrated. Live DB bytes and unchanged source
checks are recorded in evidence; test DBs are temporary copies.

**Not yet supplied by 3.1:** mount identity/liveness checks, physical-local-filesystem
attestation of operator roots, supervised NFS I/O deadlines, read-time size/hash
verification, owner authentication service, training namespace/network isolation,
cache/registry integration or route/job opt-in. These are enablement gates; the
NFS wrapper can read a fake ordinary directory in tests and must not be used as
mount admission. Later 3.2 must fail closed on absent/mis-mounted/unhealthy NFS;
3.3 handles runtime selection and old-file references; 3.4 completes the broader
failure/concurrency/config matrix. Central controller SPOF remains unchanged.

## 3. Tests and evidence

Before edits, the existing adapter/config/job-storage selection passed:
**50 tests, 0 failed/skipped** (24 + 17 + 9). No pre-existing failure was observed
in that selection. The new test file then passed all 18 tests. Final command:

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py test_storage_inputs.py test_storage_backends.py test_storage_config.py test_job_storage.py
```

**68 passed, 0 failed, 0 skipped; 4/4 files.** Full final stdout/stderr is in
[evidence](11-controller-input-evidence.json). No NFS, sudo, SSH or remote workers
are required for this selection.

The new tests cover malformed digest/key/prefix rejection before filesystem
access, canonical project paths, unlisted paths before I/O, immutable allowlist
snapshots, duplicate/conflicting references, symlink roots/parents/files,
bounded verified publication/key binding/no overwrite, read-only worker/project
views, empty input and no constructor/import side effects. The local integration
test stages a job through real `stage_job` in a temporary controller copy, checks
its DB-bound manifest, creates an allowlisted source view, explicitly publishes
those tiny test inputs to a temporary object root, reads them through a fake NFS
view and confirms the original manifest bytes and STAGED state are unchanged.
It is not live NFS, automatic migration or an application opt-in test.

Existing flag-off tests compare missing/default config for real portal uploads,
resource placement and dispatcher ordering. Existing general adapters and job
storage tests still pass. No live Pi, mount health/fault, throughput, complete
DDP or production-service test was run in this turn.

## 4. Manual verification and expected results

From the application root, run the 68-test command above. Then:

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python -m hybrid_storage.config
python3 -m json.tool docs/storage/11-controller-input-evidence.json > /dev/null
git diff --check
```

Expected: 4/4 files pass; config prints runtime legacy with both flags false and
resource policy; valid JSON; no whitespace diagnostics. Source examples of the
new seam are in `tests/test_storage_inputs.py`, including the isolated controller
roundtrip. Do not redirect these tests to the real jobs DB or enable hybrid flags.

## 5. Rollback and residual risks

No host rollback is needed: no host configuration, service, database migration,
export, mount or production object changed. Preserve Phase 2's working infrastructure.
The scoped repository patch is `/tmp/hybrid-storage-3-1.patch`. From the parent
Git root, review it and check:

```bash
git apply --reverse --check /tmp/hybrid-storage-3-1.patch
```

Only reverse this turn's edits after checking later work. Earlier user changes
and all jobs/checkpoints stay intact; do not reset or clean the repository. The
new files have no runtime callers, so rollback does not need a service restart.

Residual risks are the explicit 3.2/3.3 enablement gates above. A root path can be
misconfigured by an operator, hashes do not authorize access, NFS hard I/O can
block, and this input view does not reverify bytes on read. Existing trusted
publisher and no-follow parent-ownership prerequisites still apply. Keep hybrid
execution disabled until admission, authenticated staging and isolation are ready.

## 6. Acceptance — STOP

**3.1 PASS:** reused local/NFS adapters with strict digest-derived keys, controlled
allowlisted project paths, controller-local publication capability and read-only
worker access; existing contracts preserved; fake/local integration and flag-off
tests pass. **Phase 3 overall is not complete.** Next is 3.2, not implemented in
this turn. pi01 stays deferred; no revised live cluster validation was claimed.
