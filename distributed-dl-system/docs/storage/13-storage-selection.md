# Phase 3.3 — pinned job storage selection and legacy compatibility

8 November 2026, Asia/Dhaka. **3.3 PASS for submission selection, compatibility
and retained-input scope. STOP before 3.4.** NFS execution is still gated by
authenticated staging, worker isolation and durable process ownership. Selecting
NFS now retains an input snapshot locally and holds the job; it does not launch
training through legacy transfer or claim that NFS inputs have been registered.

Last accepted subphase: [3.2](12-input-health.md). Existing Phase 2 infrastructure
and the user-approved pi02/pi03 exception are unchanged. No node was contacted,
configuration enabled, service restarted, mount changed or live database migrated.
pi01 stays deferred. This is not overall Phase 3 or live cluster acceptance.

## 1. Implemented behavior and files

| File | Change |
|---|---|
| `controller/storage_selection.py` | New additive/idempotent binding schema, immutable selection, implicit-legacy resolution and NFS execution guard. |
| `controller/job_store.py` | Atomically pin new custom jobs; hold NFS jobs after local snapshot commit; guard preparation records, queuing, release, reservation and fair dispatch. |
| `controller/student_portal.py` | Optional form selection, startup default capture, retained HELD NFS receipt, no NFS preparation enqueue; existing auth/CSRF/upload/routes retained. |
| `controller/submit_job.py` | Optional custom-job CLI selection, same pinned/local snapshot flow, explicit held receipt and controller-only compatibility. |
| `controller/prepare_job.py` | Reject NFS intent before worker bootstrap, transfer or verification. |
| `controller/dispatcher.py`, `controller/distributed_dispatch.py` | Reject NFS intent before node/group selection; allow NFS submission default without stopping legacy reconciliation/monitoring. |
| `hybrid_storage/config.py` | Separate controller metadata-mode gate and per-submission selection; cache/locality/bandwidth remain unavailable. |
| `tools/run_tests.py` | Optional `--isolated` source-copy execution, excluding DBs/live runtime roots and clearing storage-root overrides. Existing invocation retained. |
| `tests/test_storage_selection.py` | 21 new migration/pinning/API/CLI/hold/transport/scheduler/compatibility tests. |
| `tests/test_storage_config.py` | Cover permitted held-NFS config and unchanged legacy loop order; unsupported modes still rejected. |
| `tests/test_job_submission.py` | Copy the shared config package into its configured CLI fixture; existing assertions retained. |
| `docs/storage/13-storage-selection.md`, `13-storage-selection-evidence.json` | Exact behavior, results, source hashes, commands, migration plan and rollback. |
| `docs/storage/implementation-progress.md`, `docs/storage/03-configuration.md`, `docs/README.md`, `config/README.md`, `hybrid_storage/README.md`, `tests/README.md`, `tools/README.md` | Current navigation, historical boundary and safe verification. |

Repository Git root is the parent `System_Development_Project`, branch `main`.
Existing dirty work and commit `ddc9a7b` are preserved. No stash/reset/clean/commit.
Before-files/status, test logs and rollback patch are under
`/tmp/hybrid-storage-3-3*`; durable results are in
[evidence](13-storage-selection-evidence.json).

## 2. API, database, config and security

### Pin the selection once

The additive table is in the same controller-local SQLite DB:

```sql
CREATE TABLE job_storage_bindings (
  job_id TEXT PRIMARY KEY,
  binding_version INTEGER NOT NULL CHECK(binding_version=1),
  backend TEXT NOT NULL CHECK(backend IN ('legacy','nfs')),
  selected_at REAL NOT NULL,
  FOREIGN KEY(job_id) REFERENCES jobs(job_id) ON DELETE CASCADE
);
```

An UPDATE trigger rejects changing any binding field. No rebind or metadata-delete
API was added. The foreign-key cascade only preserves existing explicit job-delete
semantics; cancellation does not delete a job, binding or snapshot. `create_schema`
uses IF NOT EXISTS and the existing initializer transaction. It does not backfill
old jobs, rewrite rows, change manifest bytes or move any database to NFS.

New custom-job creation accepts a backwards-compatible keyword-only
`storage_backend="legacy"`. A job and its binding commit together; pinning failure
rolls both back. Trusted callers using the old signature retain legacy behavior.
Dummy/built-in jobs stay legacy. Missing binding means **implicit legacy forever,
independent of current configuration**. Malformed/unknown binding versions fail
closed. Existing statuses, attempts, retry counters, leases, checkpoint formats,
manifest hashes and absolute legacy execution paths are preserved.

No migration was applied to the live DB during development. A schema-only SQLite
URI `mode=ro` query confirmed the binding table/trigger are absent; the main DB
hash matched before/after that query and the beginning of this turn. Tests use
temporary controller copies. The additive schema will be created through the existing
initializer when revised controller code is started after an operator deployment
review; take the project's consistent local SQLite backup before that deployment.
Do not copy a live WAL database as an ad hoc migration or start overlapping old
and new controller authorities.

### Configuration and per-job requests

Actual `config/storage.json` remains unchanged:

```json
{"storage_backend":"legacy","worker_cache_enabled":false,"storage_aware_staging":false,"scheduler_policy":"resource"}
```

The precise opt-in declaration below enables **held NFS submissions only**:

```json
{"storage_backend":"nfs","worker_cache_enabled":false,"storage_aware_staging":true,"scheduler_policy":"resource"}
```

It is not installed/enabled by this turn and does not activate worker NFS staging.
The portal captures its trusted operator settings at startup; configuration edits
apply to new submissions after a reviewed restart, not retroactively. Dispatcher
startup permits this mode so existing legacy leases/recovery/monitoring continue.
New cache and locality/bandwidth settings still fail before controller startup.
`require_legacy_runtime` remains strict for callers requiring legacy-only settings;
`require_controller_config` accepts legacy execution plus held NFS intent.

| Operator default | Per-job request | Result for a new custom job |
|---|---|---|
| legacy/missing config | omitted or legacy | Pin legacy; existing STAGED/preparation flow |
| legacy | nfs | Reject before job insertion; no operator opt-in |
| nfs metadata mode | omitted or nfs | Pin nfs; local snapshot then HELD |
| nfs metadata mode | legacy | Pin legacy; existing flow |
| either | invalid/empty value | Reject; no silent normalization/default |

The existing `POST /api/jobs` multipart route accepts one optional scalar form
field `storage_backend=legacy|nfs`, alongside the existing spec/project/dataset/
profile fields. Selection is not inserted into the version-one job specification,
legacy input manifest, worker payload or checkpoint contract. Students cannot
supply an export, mount root, object path or absolute controller location through
this field. Session owner, CSRF, owner quotas and path/archive checks remain.

Legacy success response stays exactly `job_id` and `status=STAGED`, HTTP202.
NFS success is HTTP202 with `job_id`, `status=HELD`, `storage_backend=nfs` and an
explicit reason. Upload temporaries are retained through the existing trusted
`stage_job` snapshot before cleanup. No portal preparation task is queued. No NFS
object is published and no worker input transfer occurs. Existing owner detail/
download/cancel routes work on that retained snapshot. No web form rewrite or new
student-facing filesystem browser was added.

CLI syntax for a future intentional submission is:

```text
python controller/submit_job.py --spec JOB.json --owner STUDENT --storage-backend nfs
```

This creates a real held job and is **not** a manual verification command to run
against the live DB. The flag is allowed only with `--spec`; explicit NFS requires
operator opt-in. Legacy CLI JSON remains compatible; NFS output includes the held
reason and selection. A controller-only CLI without a config file remains legacy.
A configured installation must include `hybrid_storage`, already required by the
portal/dispatcher; a missing package fails closed instead of ignoring config.

### Hold and execution gates

Local snapshot identity and HELD state commit together for an NFS selection.
Hold counts toward the existing pending-owner quota, consumes no active slot,
creates no rank/attempt/lease, and does not change queue-policy state names.
Owner cancellation remains available. Release is rejected while NFS execution is
unavailable; switching config back to legacy does not reinterpret a held NFS job.

Preparation enqueue/run, worker prepare/verify, environment recording, queuing,
fair selection, atomic reservation, single dispatch and direct DDP dispatch also
guard the persisted binding. Tests force inappropriate STAGED/READY/QUEUED states
and verify refusal before transport/placement. These gates do not replace existing
fairness, resource eligibility, quotas, reservations or lease fencing. Legacy jobs
still use the same code after the binding check. Raw HTTP upload/checkpoint flows
and training APIs are unchanged; no new transport or Internet worker fetch exists.

## Plan for pre-existing file references — no automatic migration

1. Leave all pre-existing jobs implicitly legacy. Keep their original
   `storage_path`, manifest bytes/digest, checkpoints and worker environment keys.
   No scan/copy/move/chmod, new hardlink or backend reassignment occurs here.
2. A future explicit owner-authorized registrar first checks the authoritative
   job and `job_storage.verify_workspace`. Represent an old input locator only
   in controller-private metadata as `(legacy_job_id, legacy_manifest_sha256,
   source_or_data, relative_path)` plus expected byte SHA/size. Derive controlled
   local paths from that verified snapshot, never student-supplied absolute paths.
3. Use the existing controlled project view for owner-authorized local reads.
   Treat any reference as requiring renewed verification; path existence or a
   digest alone is not authorization or proof that old bytes stayed immutable.
4. Later, when a new NFS job actually needs an authorized object, publish only
   required missing bytes through the trusted local object publisher, then commit
   versioned hybrid sidecars/registry bindings. Review that registrar separately.
   Never export the old jobs tree, credentials, SQLite or checkpoints. No wholesale
   copy, implicit legacy symlink into NFS or rewrite of legacy manifests.
5. Reusing inputs for a new job gets a new identity/selection; an old running job
   continues and recovers as legacy. Do not transplant an old job's checkpoint
   into a new identity or convert checkpoint formats as part of input adoption.

No registrar, locator table, object inventory, copy tool or hybrid sidecar writer
was implemented in 3.3. Held NFS snapshots are retained input candidates, not
advertised registered objects. Unblocking them needs authenticated staging,
worker/cache isolation, supervised durable ownership and a reviewed launch barrier.

## 3. Tests and actual results

Baseline snapshot: **109 tests passed**, zero failures/skips, eight selected files.
Initial development runs exposed a configured controller-only CLI fixture that
omitted its shared config package. The fixture now copies the required package;
missing-config compatibility and present-config/missing-package rejection also
have explicit tests. Those failures are retained in evidence, not counted as a
pre-existing failure or a successful run.

Actual final reproducible command:

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_storage_selection.py test_storage_config.py test_job_submission.py test_job_storage.py test_student_portal.py test_queue_controls.py test_resource_allocation.py test_storage_inputs.py test_storage_health.py
```

**130 passed, 0 failed, 0 skipped; 9/9 files**: selection21, config17, submission6,
storage9, portal17, queue10, resources6, input18 and health26.

Tests cover atomic pin/rollback, idempotent additive schema with old rows intact,
unknown binding rejection, immutable updates/duplicate IDs, implicit legacy running
jobs and checkpoint metadata, config flips/restart, NFS local retention/hold/no
worker transfer, owner cancel/release protection, state-tamper dispatch guards,
legacy response shape/preparation, API overrides/operator gate, CLI parsing and
packaging. The existing dispatcher loop order is checked under both legacy
defaults and the NFS submission default. Existing fairness/resource/cancellation,
snapshot/recovery and health tests pass. Full logs and source hashes are durable
in [evidence](13-storage-selection-evidence.json).

`--isolated` copies source/config/test dependencies to a temporary root, excludes
SQLite files and root runtime stores, and clears storage-root/PYTHONPATH overrides.
Existing controller import-time DB initialization is therefore contained. This
is test-state isolation, not a network/security sandbox; the selected tests use
mock cluster traffic and disposable local processes. No live DB-initializing
controller import, SSH/Pi,
NFS deployment/fault, actual training/DDP run, benchmark or new worker service.

## 4. Manual verification

Run the isolated 130-test command above, then:

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python -m hybrid_storage.config
python3 -m json.tool docs/storage/13-storage-selection-evidence.json > /dev/null
git diff --check
```

Expected: 9/9 files pass; actual config reports legacy/false/false/resource; valid
JSON; no whitespace diagnostics. To validate the opt-in declaration without
starting services or opening a DB, put the sample JSON in a private temporary
file and pass `--config <temporary-file>` to the same configuration module.
Expected output: runtime legacy, new_job_default nfs, nfs_execution blocked.
Do not submit a real job or import `job_store` merely to inspect migration state.

## 5. Rollback and residual risks

Current task: no live schema/config/service change, so no host rollback is needed.
Repository patch: `/tmp/hybrid-storage-3-3.patch`. From the parent Git root:

```bash
git apply --reverse --check /tmp/hybrid-storage-3-3.patch
```

Review concurrent work before reversing only this turn. Keep earlier dirty
changes and commit `ddc9a7b`; never reset/clean the tree or restore an old DB over
new jobs. Preserve inputs, checkpoints, objects and all existing NFS infrastructure.

After a future deployment creates bindings, downgrade needs more care: stop new
NFS submissions and cancel/quarantine every nonterminal NFS intent using the
current owner-authorized control path before starting older binaries that ignore
these gates. Keep the additive table/trigger and retained data; do not drop them,
delete job rows or rebind NFS intent to legacy. A reviewed roll-forward is preferred.
Changing only the config default is not a downgrade/release mechanism. Restart
the controller components coherently; mixed old/new authorities are unsupported.

Residuals: this is held submission intent, not runnable NFS jobs. Staging/auth/
tenant isolation, durable pending-process restart handling, manifest/registry
publication and complete worker/DDP readiness remain gated. The input reference
plan is documentation, not an implemented migration. **The central controller
remains a single point of failure.** No all-three-Pi or live validation is claimed.

## 6. Acceptance — STOP

**3.3 PASS:** backward-compatible routes/APIs, explicit config/job selection,
atomic immutable binding, no reinterpretation of old jobs, retained inputs and
no automatic migration/wholesale copy. Safe local regression/integration tests
pass. **NFS execution stays disabled. Phase 3 remains incomplete; next is 3.4.**
