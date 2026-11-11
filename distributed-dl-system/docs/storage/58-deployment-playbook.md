# Phase 14.2 — all-three-Pi delivery preparation

**Historical initial preparation below.** The [14.2 continuation](61-installation-tooling.md)
now supplies the explicit host installer, migration executor, rollback and final
139-test/dry-run evidence. The old `hybrid_deploy.py apply` refusal remains intact;
new tooling uses `hybrid_install.py` and protected per-host `run.py`. Active units
and configs are preserved; hybrid activation and14.3 remain blocked by readiness.

**Dry-run preparation PASS; deployment remains BLOCKED.** This subphase adds an
offline delivery planner, verified source archives, proposed unique snapshot
namespaces, migration order, systemd stop/start plan and rollback runbook for the
controller and **pi01, pi02, pi03**. It does not implement a deployment executor.
There are no reviewed production bootstrap/unit/config deltas yet. Even an edited
`apply_ready=true` cannot make this tool apply anything. Do not treat this plan as
a runnable fully integrated release or request approval to execute an incomplete
bootstrap. Stop before 14.3.

The [14.1 audit](57-production-readiness.md) is the evidence source, not a fresh
host check: pi01 was online but its NFS path was unavailable; agent dependencies,
containment and application-state backups remained unverified. The central
controller/local SQLite/NFS server remains a single point of failure. Prior
Phase 13 physical evaluation gaps remain pending.

## Files and scope

| File | Purpose |
|---|---|
| `hybrid_storage/delivery_plan.py` | Offline all-three-node preparation, strict local verification, fail-closed apply refusal |
| `tools/hybrid_deploy.py` | Discoverable CLI wrapper; no SSH/install/service execution |
| `tests/test_delivery_plan.py` | 16 Pi-less safety/integration tests, including actual legacy fixture backup and migrations |
| `hybrid_storage/README.md`, `tools/README.md`, `implementation-progress.md` | Command and scope appends |
| This document and `58-deployment-preparation-evidence.json` | Exact results, source/artifact provenance and rollback |

Starting branch `main`, HEAD `7e88b8414b00d3169916e70495ce14d86f24e05b`.
The previous 14.1 progress append and its two untracked audit files were present
at entry and preserved. 399 original nonignored files were snapshotted; only
three intended documentation appends change existing files. No existing Python,
API, schema, database, job/checkpoint, dependency or active config was changed.
No sudo, remote contact/write, service restart, mount/export/firewall change,
Internet transfer, live job, live migration, backup or restore occurred. Database
work in tests uses newly created disposable fixture databases only.

## Commands actually run and outputs

Run from the application root. `plan` is **always dry-run preparation** and only
writes when the explicit new local output directory is supplied. It never invokes
an installer. The actual final directory is the absolute `review-v2` path below:

```bash
PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/hybrid_deploy.py plan \
  --output '/home/rana/Projects/System Project/System_Development_Project/distributed-dl-system/reports/phase14-2-delivery/review-v2'

PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/hybrid_deploy.py verify \
  --output '/home/rana/Projects/System Project/System_Development_Project/distributed-dl-system/reports/phase14-2-delivery/review-v2'
```

Expected: `verified=true`, all three workers selected, `apply_ready=false`,
`executor_implemented=false`, `live_readiness_verified=false`, blockers B1–B10.
Verification is about local package consistency, not trusted authorization or
current production readiness. The actual completed result:

- Release: `release-2d924028652737d1019dd1d7`.
- Snapshot namespace: `snap-20261110T082453Z-78a0011db2562841`.
- Plan SHA-256: `382724cc15cce5bebd6e8390a82deb561160a3a28264816ff4d894fd770f0094`.
- Controller archive: 119 members, 281,351 bytes, SHA-256
  `d7cc9f39d3dcf4a76ef9622d75e24d6727d5bb5cdb1765f71af3e40f76cee468`.
- Worker archive: 81 members, 185,040 bytes, SHA-256
  `ec87a0c0271b145fc9dd5097af61c16db3d62e014241fff49e8bdda12f0752be`.

A second `plan` against the complete output verified and reused **every byte,
mode and mtime**, including the same snapshot identity. A different output gets
a fresh UTC/random snapshot identity; source-derived release identity is stable
for identical catalogs. Concurrent preparation may safely refuse an incomplete
output; retry after completion verifies it. Partial, changed or unexpected output
is never repaired or overwritten: retain it, investigate and choose a NEW output.

The earlier `review/` package is retained as development evidence. It predates
the bounded archive-decompression refinement and is not current; verifying it
against current source refuses drift. It was never uploaded or applied.

The public CLI also has an intentional negative path:
`tools/hybrid_deploy.py apply --output <absolute-review-directory>` returns
**exit 3, status REFUSED** before reading any package. This is tested in isolated
subprocesses, including tampered plans and missing output. It is not a deployment
command waiting for a yes/no flag; no apply executor is supplied in 14.2.

## Review package and integrity rules

The private mode-0700 output contains mode-0600 files:

```text
review-v2/
  plan.json                       all host operations/gates as DATA
  RUNBOOK.md                      ordered human review and rollback
  storage-current.json            current legacy/OFF/resource proposal to preserve
  storage-opt-in.example.json     inactive NFS/cache/staging/resource example
  nodes-current.json              public inventory snapshot; not installed
  delivery.json                   completion marker and exact payload hashes
  source/
    review.json                   original catalog, all workers and archive hashes
    controller-source.tar.gz
    worker-source.tar.gz
```

Source packaging reuses `hybrid_storage/deployment_review.py`; it includes hybrid
modules that the old legacy installer omits. Only allowlisted source, static UI,
runtime requirements and inactive storage examples enter source archives. User
inputs, jobs, SQLite, checkpoints, credentials, venvs/wheels and cache contents are
excluded. The two public config snapshots outside archives are review inputs,
not an instruction to overwrite worker-local node config.

The plan fingerprints all six active public config files (storage, nodes, NFS
server/clients, leases and queue policy), the 14.1 evidence, planner and wrapper,
role source catalogs and migration sources. It preserves exact raw config bytes,
not only parsed settings. Changed source/config/audit requires new review. A
relocated checkout must obtain/review an audit for that actual application root;
it cannot silently claim another path's evidence. Worker SSH alias/endpoint/user/
port must match the audit and current inventory. HostKeyAlias is explicitly not
assumed to be a current LAN address; UID 1002 on pi03 is retained.

No-follow directory/file reads reject symlinked ancestors, linked/special files
and unsafe outputs; input reads and source sizes are bounded. Archive verification
streams hashes, bounds decompression before tar extended-header allocation,
checks member names/type/size/content against the current source allowlist,
rejects traversal, links, duplicates and missing entries, and never extracts.
Duplicate JSON keys, nonfinite values, added files, altered plans and mismatched
completion markers fail verification. Source is re-read before writing the final
marker. Source/output parents must remain trusted against same-UID/admin changes;
SHA-256 is not an approval signature or a malicious-admin trust boundary.

## Proposed deployment, snapshots and service ordering

**The following is reviewed plan DATA for a future executor, not actions run here.**
The actual machine-readable `plan.json` and generated `RUNBOOK.md` contain exact
canonical per-host roots, unit paths and argv. No SSH command string or shell
script is generated for execution, and no service unit body is invented.

1. Resolve audit B1–B10 and refresh all host checks. Explicitly approve exact
   source, dependencies, protected roots, credentials, OS/network containment,
   NFS topology and concrete bootstrap/unit/config changes. Presence of tools is
   insufficient. pi01 still needs a verified route/export/client plan, not a
   guessed old LAN IP. Never expose input NFS or its network endpoint to training.
2. Use a reviewed admission pause/drain mechanism. Prove no staged/running jobs,
   attempts/ranks, reservations or live leases can race the maintenance window;
   prove all relevant descendants quiescent. A momentary IDLE API response is not
   sufficient and is not maintenance approval. Never force-stop unrelated jobs.
3. Allocate an exclusive per-snapshot journal/backup namespace on **each** host:
   `<application-root>/.hybrid-deploy/snapshots/<snapshot-id>/<host-id>`.
   Back up the exact files to change, original bytes/hashes, mode/UID/GID,
   absent-file list, unit/drop-ins, config, offline runtime identity and service
   enabled/active states. Keep credentials in separate protected recovery storage.
   Existing `deployment-previous` directories are never deleted/reused.
4. After drain proof, stop controller portal, then dispatcher; verify all other
   DB writers are stopped. Capture a new LOCAL SQLite-consistent backup using
   `sqlite3.Connection.backup` with bounded busy handling, preserving WAL state
   through SQLite rather than copying a live `.db` alone. Verify quick/FK checks
   and an isolated restore. Associate it with a consistent manifest/committed
   checkpoint/object-reference recovery point. Estimate backup and rollback disk
   use before any write; free space is not reserved capacity.
5. Transfer the role archive to a private immutable release staging directory
   `<application-root>/.hybrid-deploy/releases/<release-id>/<role>`; verify its
   exact manifest before bounded extraction. Prepare matching pinned offline ARM
   CPU service/training environments separately; no runtime Internet installs or
   silent DDP version/world-size migration. Protect both old and candidate runtime.
6. Rehearse the exact migration sequence below twice on a restored DB copy.
   Approve live DB changes separately, then migrate an idle dedicated LOCAL
   FULL/FK connection. No job-state rewrite, implicit old-data registration,
   partial-schema repair, generic initializer replay or filesystem move.
7. Stop only drained/quiescent agents; journal before each finite source-file
   operation, compare original installed hash/metadata, then atomically publish
   the reviewed source. Preserve unrelated files. Concrete trusted bootstrap,
   unit/drop-in/config deltas are **null/UNRESOLVED** in the plan and prevent apply.
8. Verify/start agents in order **pi01, pi02, pi03**, then portal, then dispatcher;
   restore only previously approved service states. Do not enable lingering,
   new units, mounts or reboot implicitly. Keep admission closed until acceptance.
9. Execute approved disposable all-three 14.3 acceptance. A failed required test
   triggers the reviewed rollback path and keeps admission closed.

Exact service operations are JSON argv arrays, never executed by this tool:
controller `['systemctl','--user','stop|start','dl-student-portal.service']` and
`dl-dispatcher.service`; each Pi
`['sudo','--','systemctl','stop|start','dl-node-agent.service']`.
Each individual start and stop is a separate concrete entry in `plan.json`;
`stop|start` here is explanatory notation, not a literal command. No daemon-reload
or enable action is assumed while desired unit deltas remain unknown. Any later
unit change requires a separately reviewed verify/reload plan and snapshot.

## Migration rehearsal and backwards compatibility

Existing job/attempt/reservation/lease/rank/storage-binding tables must satisfy
actual schema prerequisites. `controller/job_store.py::initialize_database` is
identified but **not automatically called** against production by the planner.
The source-hashed explicit additive sequence is:

1. `hybrid_storage.registry_schema.migrate(db)`
2. `hybrid_storage.cache_registry_schema.migrate(db)`
3. `hybrid_storage.staging_schema.migrate(db)`
4. `hybrid_storage.cleanup_schema.migrate(db)`
5. `hybrid_storage.checkpoint_schema.migrate(db)`
6. `hybrid_storage.restore_schema.migrate(db)`

The new integration test uses the existing actual controller fixture (copied
controller with redirected disposable roots), switches its new DB to WAL, takes
a SQLite backup into a separate new local file, and runs this sequence twice.
Original tables/rows and user_version remain identical, second-pass logical dump
is unchanged, quick_check is `ok`, FK check is empty, and the source fixture is
unchanged. This is **not a restore test of the user's live DB**, existing data
volume, credentials or hardware storage. No migration implementation changed.

Legacy/OFF/resource remains the active config; held NFS submissions and legacy
in-flight jobs keep their existing interpretation. Existing controller guards
still reject automatic cache/locality/bandwidth execution. The example config
is inactive; no flag bypass, alternate HTTP transport or DDP format change.

## Failure rollback plan

The future executor needs an exclusive snapshot lock and durable per-step intent/
completion journal. On retry, skip only exact verified existing source/schema;
never overwrite a backup or replay restarts merely because a journal exists.

On failed acceptance: close admission, fence the new attempts, and prove quiescence
before releasing pins/slots. Retain failure evidence. Stop only affected reviewed
services. For each journaled path, first compare current release hash and metadata;
restore saved prior source/config/unit/runtime only if no third party changed it.
Unexpected drift stops rollback for review. Release-created source files must be
quarantined outside import paths, not removed by recursive cleanup. Preserve all
jobs, caches, artifacts, checkpoints, reports and old backup namespaces.

Keep additive schema only where prior-version compatibility is proved. Do not
blindly downgrade schema or restore an old DB over newer jobs. A necessary DB
restore requires all writers stopped, separate retention of failed current state,
an explicitly approved matching DB/manifest/checkpoint/object recovery point and
an isolated restore proof. Restore prior active/enabled unit states and validate
legacy health before reopening admission. Never restore `/etc/exports`, fstab,
firewall or unrelated CIFAR share automatically; no such changes are proposed by
this package.

## Tests, evidence and preparation rollback

Actual safe runs:

```bash
PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated \
  test_storage_config.py test_storage_selection.py test_deployment_review.py test_nfs_client_setup.py

PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_delivery_plan.py
```

Baseline **72 PASS** (17+21+6+28). Final new suite **16 PASS**, 0 fail/error/skip:
**88 distinct tests in five files**. Retained initial new-suite failures exposed
raw-config drift omission and a relocated test-root mismatch; both corrected.
The final suite also covers decompression limits and concurrent same-output
preparation. No entire-repository/hardware performance/boot/regression claim.
Logs and byte/mtime repeatability evidence are under
`reports/phase14-2-delivery/`; the tracked companion evidence embeds the final
manifest/plan so review facts survive ignored-report cleanup. No benchmark values
or live connectivity claims are fabricated.

Preparation-only source rollback is scoped in
`reports/phase14-2-delivery/14.2.patch`. From the parent git root:

```bash
git apply --reverse --check distributed-dl-system/reports/phase14-2-delivery/14.2.patch
```

After reviewing that this exact patch remains applicable, the same command without
`--check` reverses only 14.2. It preserves the preceding uncommitted 14.1 work.
Actual reverse application is verified **only in a disposable source copy**;
production does not need rollback because nothing was applied. Keep all reports
and data. Source preservation and patch checks are recorded in
`reports/phase14-2-delivery/source-verification.json`.

**14.2 offline preparation PASS; apply-ready installer NOT DELIVERED because
reviewed runtime/bootstrap/unit/config/containment and infrastructure prerequisites
are unresolved. 14.3 BLOCKED; Phase14 incomplete. STOP.**
