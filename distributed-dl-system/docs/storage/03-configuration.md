# Safe storage configuration — Phase 1.4

Historical Phase 1.4 boundary. [Phase 3.3](13-storage-selection.md) now permits
the limited NFS **held-submission** declaration at controller startup while
keeping execution legacy-only. Its per-job pinning and safety gates supersede
the controller-startup rejection described below; cache/locality remain disabled.

Status: legacy defaults implemented; hybrid execution remains unavailable. This
subphase adds validation and controller startup guards, not NFS deployment,
registry migrations, trusted staging, tenant isolation or a locality algorithm.
See the [architecture](01-hybrid-architecture.md),
[draft lifecycle](02-manifests-and-lifecycle.md) and
[implementation progress](implementation-progress.md).

## File and defaults

The controller reads [config/storage.json](../../config/storage.json), following
the project's existing local JSON configuration style:

```json
{
  "storage_backend": "legacy",
  "worker_cache_enabled": false,
  "storage_aware_staging": false,
  "scheduler_policy": "resource"
}
```

A missing file, `{}`, missing individual fields and the complete defaults above
all produce the same immutable `StorageConfig`. No automatic file/directory/DB
creation or environment-variable override is introduced. Existing
`DL_STORAGE_ROOT` and legacy environment/cache behavior remain unchanged.
Use JSON booleans, not strings `"off"`/`"on"` or numbers 0/1. The older audit's
`worker_cache` draft spelling is not an accepted alias: fail rather than silently
ignoring a possibly enabled setting.

| Setting | Accepted values | Default |
|---|---|---|
| `storage_backend` | `legacy`, `nfs` | `legacy` |
| `worker_cache_enabled` | JSON boolean | `false` |
| `storage_aware_staging` | JSON boolean | `false` |
| `scheduler_policy` | `resource`, `locality`, `bandwidth` | `resource` |

Unknown fields, wrong types/case, whitespace-padded enums, duplicate keys,
nonfinite numbers, malformed/non-UTF-8 JSON, files over 64 KiB, final symlinks and
nonregular files are rejected. A missing file is the only read error that chooses
defaults; permission and I/O errors fail closed. Config parent directories must
be trusted operator-owned local paths; the loader does not attest all ancestor
mounts, ownership or ACLs. Students cannot supply this file through a job upload.

## Combination validation and the runtime gate

Of the 24 enum/boolean combinations, five are coherent declarations:

| Backend | New cache | Storage-aware staging | Policy | Phase 1 runtime |
|---|---|---|---|---|
| legacy | false | false | resource | Supported; existing paths |
| nfs | false | true | resource | Recognized, unavailable |
| nfs | true | true | resource | Recognized, unavailable |
| nfs | true | true | locality | Recognized, unavailable |
| nfs | true | true | bandwidth | Recognized, unavailable |

`nfs` requires storage-aware staging to prevent direct trainer access to a shared
mount. A new cache is only meaningful on that staged path. Locality/bandwidth
require a new verified cache inventory; neither may masquerade as resource policy
when disabled dependencies are missing. NFS staging without a persistent cache
is a valid future resource-policy mode: inputs would still need verified private
local materialization. No bandwidth measurements or cost model are implemented.

`validate_config(document)` checks shape/types/combinations and returns a frozen
dataclass. `load_config(path=None)` reads an explicit operator file, defaulting to
the repository-local file, and applies that validation. Neither selects a backend,
starts a worker or modifies metadata.

`require_legacy_runtime(settings)` permits only the exact default configuration
in this phase. Every coherent non-default declaration raises `StorageConfigError`
with an explicit unavailable-runtime explanation. This is deliberately separate
from recognizing the planned enum values. Do not remove/bypass the gate to try
NFS: authenticated control, private workspaces, registry/migrations, supervised
staging, checkpoint commit/read guards and measured scheduler work remain pending.
The new flags cannot silently activate an incomplete path or fall back under a
requested unsupported mode.

## Where settings are consumed

- [dispatcher.main](../../controller/dispatcher.py) checks before acquiring its
  dispatcher lock, initializing its loop, starting lease renewals or dispatching.
- [student_portal.create_app/main](../../controller/student_portal.py) checks before
  creating factory authentication storage; the server CLI also checks before its
  storage/lock/bootstrap steps. Existing `--create-user` remains an account-management
  utility and does not start training services.
- [hybrid_storage.config](../../hybrid_storage/config.py) offers a standalone
  read-only preflight command. It imports no DB-initializing controller module.

These are startup checks, not hot reload. A config edit does not alter running
attempts or restart a service. No service was restarted during development. The
existing controller modules still have historical import-time DB initialization;
this subphase does not refactor that behavior. Use the standalone checker rather
than importing dispatcher/portal merely to inspect configuration.

Direct legacy submission/preparation/maintenance utilities and the agent retain
their existing paths. They do not interpret the new flags or claim to run hybrid
jobs. No worker deployment bundle, worker endpoint, HTTP payload, authorization,
job state, checkpoint format, dataset path or SQLite schema was changed. Future
hybrid runtime integration must propagate frozen per-job settings through those
paths and preserve existing bindings on global config changes. There are no
enrolled hybrid jobs produced by Phase 1.

## Actual local validation and its limits

17 new tests check defaults/partial settings, all 24 combinations, strict types,
malformed files, CLI results and isolated service gates. For missing config, `{}`
and explicit OFF, tests exercise the real portal upload/staging path and compare
owner/status, immutable source/data identities, bytes and mutable-directory layout.
They also check unchanged CPU/RAM/resource/reservation selection and dispatcher
reconcile/monitor/dispatch order. Unsupported mode stops before lock/renewal or
factory auth creation. No real worker request is needed.

The 79 safe legacy methods selected during Phase 0 were rerun separately on:

1. A hash-verified pre-1.4 source-only baseline, without the new flags/guards.
2. The candidate code with the shipped explicit all-OFF file.
3. The candidate code with the file absent.

Each variant passed 79/79 across 12 files, including portal/ownership, queue/quota/
reservation, single/DDP dispatch/checkpoint mocks and storage checks. All three
copies exclude real DB/jobs/checkpoints/logs/venvs, use isolated temporary caches
and preserve the Phase 0 exclusions. Guard executables reject ssh/rsync/sudo/
mount/exportfs/systemctl/reboot/shutdown/umount. No packages are installed and
`PIP_NO_INDEX=1` prevents runtime dependency downloads in those checks.

The 24 existing adapter tests and 15 draft-schema tests also passed. Final
verification totals **293 passing method executions** (237 comparisons + 17
config + 24 adapters + 15 schemas), **135 distinct methods**, zero failures and
zero framework skips. Twenty-five expensive/process-killing/multiprocess methods
and the standalone training-equivalence script remain deliberately deferred, not
counted as passes or framework skips. This is regression evidence for the tested
legacy paths, not a proof over every possible runtime state.

Read [03-config-validation-evidence.json](03-config-validation-evidence.json) for
actual commands, per-file results, source hashes and the exact audited selection.
Live NFS, Pi/runtime-version validation, failover experiments and performance remain
untested. All-three-Pi validation (pi01/pi02/pi03) still requires a reviewed plan
and explicit deployment approval. The central server remains a single point of
failure; adding flags does not change that availability boundary.

## Safe manual checks and rollback

From the application root, without starting services:

```bash
env PYTHONDONTWRITEBYTECODE=1 python3 -m hybrid_storage.config
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py test_storage_config.py test_storage_backends.py
env PYTHONDONTWRITEBYTECODE=1 python3 tests/test_storage_manifest_drafts.py
git diff --check
```

Expect checker exit 0 with `runtime=legacy` and all four default values, 41 tests
across the first two files, 15 schema/fixture tests without skips under the
documented system Python, and no whitespace diagnostics. `--config <local-file>`
can inspect a disposable config copy; coherent NFS declarations exit 2 with an
unavailable-runtime error, and malformed declarations also exit 2. No live config
edit is needed for verification.

Rollback only this subphase: restore its guarded controller/doc edits from the
pre-1.4 contents, remove its five newly added files listed in the progress record,
and retain all earlier/user changes. The file can also remain with all-off defaults.
No data/DB migration rollback is required. Do not use a repository-wide reset or
delete caches, jobs, checkpoints or data. Keep any eventual service rollout or
revert separate and explicitly approved; no such rollout occurred here.

**1.4 acceptance: PASS for strict defaults/validation, legacy regression checks and
no live deployment. STOP at the Phase 1 boundary.** Later implementation phases
need their own scope; this file does not authorize deploying or enabling NFS.
