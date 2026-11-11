# Hybrid storage implementation progress

Updated: 7 November 2026. This records delivered subphases and their evidence;
it does not imply deployment or overall Phase 1 acceptance.

## Phase 1.1 — architecture delivered

The [architecture document](01-hybrid-architecture.md) was delivered in the prior
turn. The user then explicitly requested completion of 1.2. No runtime changes
were made in 1.1. Later schemas/configuration/deployment are not inferred as done.
The [Phase 0 audit](00-audit.md) remains the historical baseline.

## Phase 1.2 — storage interfaces and adapters delivered

Added files:

- [hybrid_storage/__init__.py](../../hybrid_storage/__init__.py): public exports,
  with no DB, configuration or deployment side effects.
- [hybrid_storage/backends.py](../../hybrid_storage/backends.py): typed
  `StorageBackend` protocol, `LegacyStorageBackend` local adapter and read-only
  `NFSStorageBackend` mounted-files adapter.
- [hybrid_storage/README.md](../../hybrid_storage/README.md): API contract,
  publication capabilities, security prerequisites, limitations and test commands.
- [tests/test_storage_backends.py](../../tests/test_storage_backends.py): isolated
  adapter/security/failure tests with no live NFS.
- This progress record.

The four protocol operations are artifact existence, checked path resolution,
controlled read-only open and controlled publication. Local publication needs an
explicit private staging root and expected size/SHA-256, rejects overwrite and
uses verified atomic publication. NFS publication is always denied. Unsafe paths,
symlinks and nonregular files are rejected; real I/O failures propagate.

No existing application code was edited. Existing upload/preparation/recovery/DDP
and scheduler paths remain in place. No endpoint, transport, checkpoint format,
DB, migration, job state, config file, flag or live infrastructure was changed.
The root/manifest authorization, authenticated control channel and OS isolation
requirements in the architecture still need implementation before enablement.

### Actual validation

Executed on the controller checkout using Python 3.12.3:

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py test_storage_backends.py test_job_storage.py
git diff --check
```

Results: **33 tests passed, 0 failed, 0 skipped** (24 new adapter tests and 9
existing storage tests); **2/2 test files passed**. Existing storage tests import
temporary copies of controller modules and use temporary DBs/inputs. The new
tests do not import the live DB modules. Whitespace check passed.

Coverage includes regular reads, protocol compatibility, malformed keys,
traversal, symlink roots/parents/files, FIFO rejection without blocking, a parent
replacement race, mocked I/O/permission/stale-handle failures, explicit publication
capability, read-only NFS, hidden partial files, size/digest mismatch, bounded
oversize reads, source interruption, simulated disk-full, exclusive publication
races and post-publication directory-fsync failure.

The pre-edit snapshot preserved all 131 inventoried first-party files, including
the audit and 1.1 architecture. Expected Git additions are only the five files
listed above; the pre-existing staged/unstaged/untracked changes are retained.

Not run: full regression suite, real NFS mounts/outages, physical Pi tests,
benchmarks, service operations or Python 3.11 execution. The code targets the
Linux worker environment; runtime acceptance on all worker versions is pending.
Missing/default flags are not implemented/tested until 1.4. These results are not
evidence of production tenant isolation or live storage readiness.

### Review, rollback and remaining work

Manual verification: run the same two commands above; expect 33 passing tests
and no whitespace diagnostics. Review each new file with
`git diff --no-index -- /dev/null <new-file>` (exit 1 denotes an added-file diff).

Rollback: remove only the five 1.2 additions listed above. No application-data or
DB rollback is required; preserve the existing audit, architecture and all prior
user modifications. Do not use repository-wide reset/clean.

Residual risks include blocking NFS I/O, required service authentication and
isolation, trusted-directory provisioning, incomplete controller-failure recovery
design details, and the central server remaining a single point of failure. A
failed final directory fsync can leave a complete file requiring reconciliation;
this is explicit API behavior, not a committed manifest/checkpoint. Future
deployment/validation requires pi01, pi02 and pi03 and separate approval.

**1.2 acceptance: PASS for interfaces and Pi-less adapter tests. STOP.**
Immediately next is 1.3 (schema/migration/generation/attempt/staging/checkpoint
design), only when requested. No 1.3 or 1.4 work is included here.

## Phase 1.3 — manifest and lifecycle drafts delivered

The user requested 1.3 after the 1.2 report. Added:

- [02-manifests-and-lifecycle.md](02-manifests-and-lifecycle.md): versioning,
  semantic validation, additive local SQLite migration strategy, cache identities/
  epochs/inventory sessions, attempt/grant identities, staging/launch transitions,
  checkpoint commit/reconciliation and rollback design grounded in current code.
- [input-tree-v1.schema.json](schemas/input-tree-v1.schema.json) and
  [job-inputs-v1.schema.json](schemas/job-inputs-v1.schema.json): draft JSON Schema
  2020-12 contracts, not runtime validators or changes to the legacy manifest.
- [v1-manifests.json](examples/v1-manifests.json): synthetic byte/hash-consistent
  source, dataset and three-logical-rank job fixture; no actual job/Pi result.
- [test_storage_manifest_drafts.py](../../tests/test_storage_manifest_drafts.py):
  draft-schema, fixture consistency and source-only compatibility checks.

This progress section is the only edit to an existing file. The prior progress
text is preserved verbatim. All other 135 snapshotted files, existing application
code, legacy job states, attempt tokens and manifest/checkpoint formats remain
unchanged. No DB was opened/migrated, no flags introduced, and no services or Pis
contacted. Side tables, grants, ARMED leases, launch barriers, checkpoint journal
and guarded readers described in the draft are future implementation requirements.

Actual validation command, using the already installed system Python 3.12.3 and
`jsonschema` 4.10.3 (absent from the controller venv):

```bash
env PYTHONDONTWRITEBYTECODE=1 python3 tests/test_storage_manifest_drafts.py
git diff --check
```

Result: **15 passed, 0 failed, 0 skipped**; whitespace check passed. Tests cover
schema validity, canonical manifest and artifact hashes, fixture references/rank
coverage, existing source state groups/token behavior, unsafe paths, unsupported
versions/fields/kinds, malformed identity/size/boolean values and declared schema
limitations. They use only local documents and selected pure source ASTs, without
importing live DB modules or resolving remote schema references.

No packages installed or runtime dependencies changed. If the optional test
validator is unavailable, ten schema test methods explicitly skip; five standard-
library fixture/source checks still run. The recorded command above had **no
skips**. Do not interpret schema-shape validation as implemented semantic job
authorization, stage-time verification or migration safety.

Not run in 1.3: previous adapter/storage suites (unchanged runtime; their 33-pass
1.2 result remains historical), migration-twice/crash tests (no migration runner),
runtime lifecycle races, live NFS, all-three-Pi validation or benchmarks. Those
are explicit later acceptance gates, as are default-off behavior tests in 1.4.

Manual review: rerun the commands above and expect 15 tests/zero skips, then inspect
the draft diagrams, relational constraints and crash table. For each new file,
`git diff --no-index -- /dev/null <new-file>` shows its full addition (exit 1 means
differences). Use the pre-1.3 progress snapshot to isolate this appended section;
the progress file was already untracked and ordinary git diff alone omits it.

Rollback: remove only the five new 1.3 files listed above and undo this appended
section. Keep 1.1/1.2 documents/code/tests and all prior user changes. No DB/data/
service rollback is necessary. The central server remains a single point of
failure; authentication, isolation, NFS readiness and all-three-Pi evidence remain
required before eventual enablement.

**1.3 acceptance: PASS for the requested design drafts and local contract tests.
STOP.** Next is 1.4, only when requested; no configuration implementation is
included in 1.3.

## Phase 1.4 — safe flags and legacy compatibility delivered

The user requested 1.4 after the 1.3 report. Added five files:

- [hybrid_storage/config.py](../../hybrid_storage/config.py): immutable typed
  settings, strict JSON/type/combination validation, explicit legacy-only runtime
  gate and a read-only command-line checker.
- [config/storage.json](../../config/storage.json): legacy/resource defaults,
  `worker_cache_enabled=false`, `storage_aware_staging=false`.
- [test_storage_config.py](../../tests/test_storage_config.py): 17 isolated tests,
  covering all 24 combinations and actual default/missing/off controller behavior.
- [03-configuration.md](03-configuration.md): defaults, supported declarations,
  consumption boundaries, manual checks, residual risks and rollback.
- [03-config-validation-evidence.json](03-config-validation-evidence.json): source
  fingerprints, exact safe test selection/deferred cases, commands and results.

Edited six existing files: dispatcher startup, portal factory/server startup,
config README, hybrid package README, tests README and this appended progress
section. Existing pre-1.4 contents are preserved in the local evidence workspace;
the exact diff isolates this phase from prior uncommitted work. All other 135
inventoried files remain unchanged. No stashing/reset/commit occurred.

Missing configuration, `{}`, partial defaults and explicit all-OFF settings use
the same legacy values. Unknown keys (including old `worker_cache`), coerced
booleans, duplicate keys, malformed/oversized/nonregular files and I/O errors fail
closed. Valid NFS/staging/cache/locality/bandwidth declarations are recognized but
the controller startup gate rejects them until the missing runtime/security work
is implemented. No incomplete feature can silently activate or fall back.

The dispatcher checks before lock/renewal/dispatch; the portal checks before
factory auth storage and server bootstrap. Checks are startup-only, not hot reload.
Legacy CLI utilities and worker APIs remain unchanged. There are no new endpoint,
DB/migration, job/rank state, lease, checkpoint format or scheduling changes. New
flags do not disable the existing legacy cache. Historical controller import-time
DB initialization was not refactored; all DB-initializing tests ran in copies.

### Actual tests and evidence

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py test_storage_config.py test_storage_backends.py
env PYTHONDONTWRITEBYTECODE=1 python3 tests/test_storage_manifest_drafts.py
env PYTHONDONTWRITEBYTECODE=1 python3 -m hybrid_storage.config
git diff --check
```

Results: 17 config/startup + 24 adapter + 15 schema tests passed; checker exit 0
reported all legacy defaults; whitespace check passed. The config suite also ran
once more after correcting a test method's descriptive combination count to 24;
its repeated 17 passes are not added to the final totals.

The Phase 0 safe selection was run on three source-only copies, with the original
venv interpreter and no copied DB/data/venv. Commands executed:

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python /tmp/hybrid-storage-1-4-4pir2h_e/baseline/run_selected.py
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python /tmp/hybrid-storage-1-4-4pir2h_e/flags-off/run_selected.py
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python /tmp/hybrid-storage-1-4-4pir2h_e/config-missing/run_selected.py
```

Each passed **79/79 methods in 12 files**, with matching per-file outcomes. The
baseline's 141 source hashes match the pre-edit inventory; candidate production
runtime hashes match the delivered implementation. The runner uses command guards,
offline dependency settings and isolated caches. Evidence includes exact method
selection and command arguments; temporary logs remain at the paths recorded there.

Final counted verification: **293 passing method executions, 135 distinct methods,
0 failures, 0 framework skips**. Twenty-five expensive/process-kill/multiprocess
methods plus the standalone training-equivalence script remain deliberately
deferred. No live NFS, all-three-Pi, Python 3.11 runtime or benchmark validation was
performed. No live DB, job, checkpoint, service, `/etc` file, firewall or Pi changed.

The first config-suite trial also passed but exposed a ResourceWarning from the
existing dispatcher's lifetime lock when the test deliberately interrupts its
infinite loop. The test now retains/closes that temporary handle; final runs have
no such warning. This did not require changing dispatcher lock behavior.

### Manual verification, rollback and acceptance

Run the four safe commands above: expect 41 tests in two files, 15 schema tests
without skips under the documented system Python, checker exit 0 with `runtime`
set to `legacy`, and no whitespace diagnostics. Inspect a disposable config with
`python3 -m hybrid_storage.config --config <copy>`; NFS declarations exit 2 with
an explicit unavailable-runtime error. No live config edit or restart is needed.

Rollback only this subphase's diff: restore the six edited files to their pre-1.4
contents (remove only this appended progress section), and remove the five new
files listed above. The configuration file can instead remain with safe defaults.
Do not reset/clean the repository or touch runtime data. No DB/data migration or
service rollback is needed because nothing was deployed/restarted.

Residual risks remain the central server as a single point of failure, pending
authenticated control and OS isolation, blocking NFS I/O supervision, unimplemented
registry/staging/commit-state integration and incomplete hardware readiness.
Deployment/validation still requires pi01, pi02 and pi03 after separate explicit
approval. The test comparison establishes compatibility for the tested paths,
not every possible runtime state or physical failure.

**1.4 acceptance: PASS. Phase 1 default-off/interface/design scope delivered;
hybrid execution remains disabled. STOP.** No later phase or live deployment is
inferred or implemented.

## Phase 2.1 — read-only topology audit delivered; pi01 deferred

The user requested Phase 2, one subphase per turn. This turn covers **2.1 only**.
Added [04-nfs-topology.md](04-nfs-topology.md) and
[04-nfs-topology-evidence.json](04-nfs-topology-evidence.json); appended this
progress section. All other 145 inventoried files remain unchanged. No runtime
code, API, DB/schema, flags, service or infrastructure configuration was edited.

Fresh read-only observations on 7 November 2026 around 20:18–20:20 Asia/Dhaka:

- Controller: Ubuntu 24.04.4, Wi-Fi 192.168.10.214/24, Tailscale 100.73.90.91,
  wired NIC without carrier/address, about 40.94 GiB free on local ext4. NFS
  server/client packages and server unit absent; no NFS listener/export/root.
- pi02: SSH and both API reads succeeded; LAN 192.168.10.150/24 independently
  verified by identity-matching health response. Debian 12, eth0 100 Mb/s, about
  11.64 GiB free, nfs-common installed, existing NFS server/share present.
- pi03: SSH and both API reads succeeded; LAN 192.168.10.101/24 independently
  verified likewise. Debian 12, eth0 100 Mb/s, about 8.67 GiB free, nfs-common
  installed, existing mount from pi02. User pi has UID/GID 1002, versus pi02 1000.
- pi01: alias/inventory still 100.101.90.46; SSH and API requests timed out;
  local Tailscale status reports offline. The user confirmed "its offline leave
  it for now". Further pi01 work is stopped, with its host details deferred.

Initial sandbox socket/netlink/systemd denials were recorded separately and the
same read-only checks retried with approved network access. SSH required existing
host keys, disabled updates/forwarding/multiplexing/local commands, and sent Python
over stdin with isolated/no-bytecode flags. No remote script file was installed.

pi02's exports config and readable managed table still contain a wildcard/RW/
no_root_squash share. Kernel exports returned headers only, so complete effective
policy is not established. Both reachable agents share the mount namespace where
the old RW NFSv4.2 mount is visible, without the required isolation. These are
unresolved findings; no old share, fstab entry or permissions were changed. The
new `/srv/ml-shared/objects` does not exist on the inspected hosts.

Checks: 3/3 aliases resolved; 2/3 SSH probes succeeded; 4/6 configured API requests
succeeded; both LAN health checks succeeded with matching identity. Read-only
package, NIC/route, local capacity, mount, exports and permission metadata were
collected. The evidence embeds exact scripts/SSH arguments, results/timestamps and
errors. NFS/autofs paths were not traversed or statvfs-ed; mount tables supplied
their metadata. No exportfs invocation, sudo, package download/install, mount,
unmount, writes to NFS, job execution or service mutation occurred.

Local verification commands:

```bash
python3 -m json.tool docs/storage/04-nfs-topology-evidence.json > /dev/null
git diff --check
```

Expect valid JSON and no whitespace errors. Document links, raw identity/address
consistency and preservation of prior content were also checked. Application unit
tests were not rerun for this report-only subphase; no new runtime behavior exists
to test. Manual read-only host commands and their expected observations are in
the report. No throughput, TEST artifact/SHA transfer, RO write-denial, mount
failure or rollback experiment has run.

Rollback: remove only the two new 2.1 report/evidence files and this appended
section. No host/application rollback is needed; all prior code/user changes and
the legacy flags remain intact. Never remove data or reset/clean the checkout.
The central server is still a single point of failure; pi01 details, stable client
addressing, server packages, isolation and later all-three-Pi acceptance remain
pending. The pi01 deferral applies to this inspection, not a claimed passing
three-node deployment.

**2.1: PASS for the authorized reachable-host audit with pi01 explicitly deferred.
Phase 2 overall acceptance: NOT PASSED / not yet deployed or tested. STOP.**
No 2.2 setup script/dry-run or later subphase work was performed.

## Phase 2.2 — controller setup preparation (7 November 2026)

**2.2 preparation: PASS; live applicability: NOT READY. STOP.** Only this subphase
was performed. No server setup was applied, and pi01 was left offline/deferred
without further probes as requested. All-three-Pi deployment acceptance remains
required later. See the [setup/rollback guide](05-nfs-server-setup.md) and
[actual dry-run/test evidence](05-nfs-server-dry-run.json).

### Implemented behavior and exact repository scope

Added six files:

- `hybrid_storage/nfs_server.py`: bounded read-only plan, strict configuration and
  export syntax/policy subset, protected path checks, and future administrator-only
  idempotent staging with backup journal, reviewed digest and cooperative lock.
- `tools/nfs_server_setup.py`: CLI entrypoint, read-only by default.
- `config/nfs-server.json`: separately reviewed infrastructure proposal with the
  2.1 controller/pi02/pi03 LAN addresses; unknown pi01 and publisher IDs are null.
- `tests/test_nfs_server_setup.py`: 19 isolated fake-host tests.
- `docs/storage/05-nfs-server-setup.md`: operation, trust, review and rollback guide.
- `docs/storage/05-nfs-server-dry-run.json`: raw controller plan, checksums, test
  results and before/after host-state observations.

Appended/indexed only five existing files: `config/README.md`,
`hybrid_storage/README.md`, `tools/README.md`, `tests/README.md`, and this progress
file. Prior edits, staged deletions and untracked work were preserved; no stash,
reset, commit or cleanup was used. A scoped exact patch is available at
`/tmp/hybrid-storage-2-2.patch` for review/repository-only rollback.

### Interfaces, configuration, DB and security

No application API, job state, DB schema, checkpoint format or storage feature
flag changed. Existing legacy/default-off behavior remains. The new standalone
CLI offers `--config`, default/explicit `--dry-run`, and future `--apply` with
`--reviewed-plan-sha256`. Apply is configuration staging only: no embedded package
install, service command, exportfs invocation, firewall/fstab/client change or
account creation. Explicit user approval is still required for any live staging.

Only `/srv/ml-shared/objects` is proposed for literal known client hosts with
`ro,sync,root_squash,subtree_check,secure,sec=sys`. The current shared-root subdirectory
layout retains subtree checks. `no_subtree_check,mountpoint` requires a verified
whole dedicated filesystem; that alternative also needs a separately reviewed
atomic publication strategy. Private staging is not exported; SQLite remains
local. No `/`, `/home`, job/checkpoint or DB export is allowed. AUTH_SYS is a host
boundary; per-student authorization and process isolation are still enablement
requirements. The central server remains a single point of failure.

Existing main/drop-in exports are inspected conservatively and backed up before
future changes; unrelated active entries block setup and are never rewritten.
The controller has no existing export config to parse. Preview syntax passed the
managed grammar; native nfs-utils validation is untested because packages are
absent. `exportfs -ra` is activation, not a safe syntax-only dry-run. The guide
links the primary Ubuntu exports/exportfs manuals and explains these limitations.

### Actual tests and evidence

Executed from the application root:

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py test_nfs_server_setup.py test_storage_config.py test_storage_backends.py
env PYTHONDONTWRITEBYTECODE=1 python3 tools/nfs_server_setup.py --dry-run
git diff --check
```

Final result: **60 passed, 0 failed, 0 skipped** across setup (19), configuration
(17) and backends (24); runner reports 3/3 files passed. Tests cover strict host/
export input, backup failure, backup-first/idempotence, concurrent config edits,
stale review digest, unsafe paths/permissions, interrupted setup, mounted-filesystem
checks and unchanged default-off application behavior. Development fixture-umask
and helper-variable errors were corrected and rerun; they are not pre-existing
baseline failures. Full ML/DDP/live hardware tests were not run for this utility.

The initial sandbox denied the local read-only IP query; it was retried with
read-only network visibility and no sudo. Actual controller dry-run **exit 2** is
the expected safe refusal: pi01 address, publisher UID, publisher GID,
`nfs-kernel-server` and `nfs-common` are unresolved. `apply_ready=false`; no Pi
address or identity was invented. Snapshot checks show `/etc/exports`,
`/etc/exports.d`, `/srv/ml-shared`, backup/lock paths and `/etc/fstab` unchanged.
There were no live package/service/export actions, mounts or remote probes.

### Manual verification and rollback

Repeat the dry-run above: expect JSON, exit 2 and the same five blockers unless
the host/config changed. Validate evidence with:

```bash
python3 -m json.tool docs/storage/05-nfs-server-dry-run.json > /dev/null
```

Expect valid JSON, `host_state_unchanged=true`, and the recorded 60-test results.
The guide documents future private backup journals under
`/var/backups/distributed-ml-nfs/<unique-run>/before.json`; no live backup was
created today. Restore only the managed drop-in after matching its installed hash
and preserving concurrent edits. Before any later active rollback, drain the
approved clients and unexport only the exact approved object/client pairs. Never
perform broad unexport, recursive deletion/chown or overwrite unrelated config.
Preserve input/staging data and journals.

For this repository-only work, review the scoped patch and undo only its six new
files/five index-document edits. From the parent Git root, the read-only rollback
check is `git apply --reverse --check /tmp/hybrid-storage-2-2.patch`; applying a
reverse patch later requires rechecking for intervening edits. No host rollback
is needed now. Avoid resetting the dirty checkout.

Residual risks: missing pi01 details, DHCP address stability, no publisher account
selection/provisioning, missing packages/native export checks, the old pi02
wildcard RW share, worker isolation, offline package availability, NFS outages,
controller SPOF and all-three-Pi read/hash/write-denial tests. Phase 2 overall is
not complete. No 2.3 client preparation or 2.4 live application was performed.

## Phase 2.3 — all-three-Pi client preparation (7 November 2026)

**2.3 preparation: PASS; live applicability/overall Phase 2: NOT READY. STOP.**
The user requested 2.3 only. Prepared all three clients without contacting any Pi,
installing packages, using sudo, modifying fstab/services or mounting NFS. pi01
remains offline/deferred as instructed. All-three-Pi deployment acceptance is
still required; no 2.4/2.5 action is included. See the
[client setup/rollback guide](06-nfs-client-setup.md) and
[actual local validation evidence](06-nfs-client-validation.json).

### Behavior and exact file scope

Added seven files:

- `hybrid_storage/nfs_client.py`: default offline three-client preview, optional
  read-only per-worker inspection, and future root-only reviewed package/config
  steps. Conditional package provisioning uses only cached nfs-common/dependencies.
- `hybrid_storage/nfs_mount.py`: standalone mount metadata admission guard.
- `tools/nfs_client_setup.py`: operator entrypoint.
- `config/nfs-clients.json`: explicit pi01/pi02/pi03 proposal, with pi01 null and
  the verified 2.1 controller/pi02/pi03 LAN values.
- `tests/test_nfs_client_setup.py`: 25 isolated fake-client/mount metadata tests.
- `docs/storage/06-nfs-client-setup.md`: topology, boot/trust boundary, operations,
  manual checks, rollback and outstanding live/security gates.
- `docs/storage/06-nfs-client-validation.json`: all three generated plans, exact
  native verification commands/results, test log/checksums and live non-actions.

Only five existing files received insertions: `config/README.md`,
`hybrid_storage/README.md`, `tools/README.md`, `tests/README.md` and this progress
file. Original lines/prior dirty status and the 2.2 server implementation are
preserved. Exact scoped diff: `/tmp/hybrid-storage-2-3.patch`.

### APIs, DB, configuration and security

No application API, schema/migration, job/lease/DDP/checkpoint state or format,
scheduler rule or storage flag changed. Legacy/default-off remains. The standalone
client tool's default/`--dry-run` is offline and always previews all three nodes.
`--node piNN --inspect-local` reads only that local worker. Future approved
`--install-missing-package`/`--apply` require a fresh plan SHA and administrator;
the digest does not grant approval. Package scripts may affect services, so package
provisioning is a distinct reviewed action; there is no package download fallback.
Configuration staging itself runs no service/package/mount commands.

Generated mount/automount/ready units use `/srv/mlnfs/inputs` beneath root-only
0700 `/srv/mlnfs`, source the controller objects export, and propose NFS 4.1/TCP,
RO/nosuid/nodev/noexec/hard/secure-source-port settings. Automount alone is proposed
for boot; mount has no install section, uses nofail/15s mount-command timeout,
and readiness is ordered after/requires the actual mount. The legacy agent stays
independent. Guard checks exact source/path/protocol/options and rejects absent,
ordinary-directory/autofs-only, RW/soft, wrong/bind/shadowed/nested mounts. It
reads kernel metadata without triggering data access. A future stager must use
this gate in its namespace, pin/verify its directory descriptor and independently
authorize and hash every artifact. No readiness check currently launches jobs.

The current agent/training share user/namespace; no malicious-code isolation was
invented. Protected directories do not restrict direct NFS protocol access. Hybrid
enablement remains blocked on distinct execution credentials, endpoint isolation,
supervised hard-mount I/O and runtime integration. Checkpoints stay on the existing
trusted-publication design, and SQLite remains local. The controller is a SPOF.

### Actual tests and manual reproduction

```bash
env PYTHONDONTWRITEBYTECODE=1 python3 tools/nfs_client_setup.py --dry-run
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py test_nfs_client_setup.py test_nfs_server_setup.py test_storage_config.py test_storage_backends.py
env PYTHONDONTWRITEBYTECODE=1 python3 hybrid_storage/nfs_mount.py --source 192.168.10.214:/srv/ml-shared/objects
python3 -m json.tool docs/storage/06-nfs-client-validation.json > /dev/null
git diff --check
```

Results: **85 passed, 0 failed, 0 skipped** (25 client + 19 server + 17 config +
24 backend), four files passed. The new suite passed on its first run; additions
for ancestor overmount/direct boot links and safe query environment were also
rerun. Mock package actions never invoke real apt. All three fake-client staging
runs were tested twice for no-op idempotence, while preserving their old fstab.
Backup-disk failure, partial multifile staging, competing config edit, unsafe
paths/permissions, wrong node, stale digest and no-admin cases fail safely.

Offline preview exits 2 with pi01 and fresh local review/approval requirements,
and includes all three clients. Local guard exits 2/ready=false on the actual
unmounted controller. Generated unit sets for pi01/pi02/pi03 each passed local
`systemd-analyze verify --man=no <mount> <automount> <ready>` with exit 0/no
diagnostics under systemd 255. Standalone isolated copies of the installed guard
also returned the expected refusal. Exact temporary file paths/commands and
generated strings are embedded in evidence; native verification loads no units.
Python 3.11 syntax was parsed locally; execution on Pi Python 3.11/systemd 252
was not performed. Manual reproduction and expected outcomes are in the guide.

No fresh remote probes, native NFS option negotiation, package/root installation,
mount/boot/outage tests, TEST SHA-256/write-denial, DDP/model execution or throughput
benchmark ran. The old pi02 share/fstab/agent and every live host were left alone.

### Rollback and residual risks

No host rollback is necessary today. From the parent Git root, inspect the scoped
diff and run the read-only rollback check:

```bash
git apply --reverse --check /tmp/hybrid-storage-2-3.patch
```

Reverse only the seven additions/five insertions if later desired after checking
for intervening edits. Never reset/clean the dirty repository or delete data.
Future active rollback must fence/drain staging, stop/disable only the new
automount, safely stop its mount, verify current managed-file hashes against the
private backup journal, and restore only those files. Preserve fstab/legacy agent,
the old share, inputs, markers and journals; no forced/lazy/global unmount or
recursive deletion. Package side effects need a separate approved rollback plan.

Remaining gates: pi01 facts/stable addresses, server publisher/packages from 2.2,
offline package cache, new-server NFS 4.1 negotiation, kernel hard-I/O supervision,
old-share/endpoint/process isolation, controller SPOF, and all-three-Pi live
read/hash/RO-write-denial/boot/outage validation. **STOP at 2.3.**

## Phase 2.4 — requested; preflight recorded, application pending

The user requested 2.4. **2.4 is NOT COMPLETED / acceptance NOT PASSED.** The
user explicitly confirmed "Keep pi01 deferred; deployment stays pending". pi01
remains unprobed, and deployment work is deferred until instructed to resume. No server/client deployment or two-Pi substitution was
performed. See [deployment preflight](07-nfs-deployment.md) and
[fresh evidence](07-nfs-deployment-evidence.json).

Added only `docs/storage/07-nfs-deployment.md` and
`docs/storage/07-nfs-deployment-evidence.json`; this appended progress section is
the only existing-file edit. Prior user changes and the 2.2/2.3 implementations
remain intact. No application API, DB/schema, config/flags, job state, lease,
checkpoint/DDP format or security policy changed; legacy/default-off remains.

Fresh read-only controller/SSH/HTTP checks around 21:12–21:13 Asia/Dhaka on
7 November confirmed pi02 and pi03 reachable, with identity-matching API responses,
their original LAN addresses/nfs-common and active idle agents. pi01 was not
contacted. The old pi02 wildcard RW/no_root_squash share and shared agent/training
namespace remain findings, not corrected features. No old mounts were traversed
for data or disrupted.

Commands/results:

```bash
env PYTHONDONTWRITEBYTECODE=1 python3 tools/nfs_server_setup.py --dry-run
env PYTHONDONTWRITEBYTECODE=1 python3 tools/nfs_client_setup.py --dry-run
apt-get --simulate --no-download --no-remove install nfs-kernel-server nfs-common
python3 -m json.tool docs/storage/07-nfs-deployment-evidence.json > /dev/null
git diff --check
```

Controller plan exits 2 / apply_ready=false: pi01 address, publisher UID/GID and
both NFS packages remain missing. Client preview exits 2. The package simulation
exits 100 because needed archives cannot be fetched in no-download mode; no
matching cached NFS provisioning archives were found. These are unresolved
readiness failures, not applied changes. The controller has no new NFS listener,
exports file or server units. Proposed name ml-publisher has no user/group entry;
no account/UID was created or inferred. pi02/pi03 SSH succeeded 2/2 and their
health/resources requests succeeded 4/4. Exact probe commands/results are in
evidence. Local JSON/whitespace/preservation/diff checks passed. No unit suite
rerun was needed for report-only edits; the earlier 85 passes are historical.

A public synthetic 32 MiB TEST artifact was generated only under the temporary
review folder, with SHA-256
`2c473ee33d7dba6b5f1c6ec7589344cb557e4e25fc9b0b8d7018709590400d86`.
It is not published/transferred, and no Pi has read it. All-three-Pi new mounts,
negotiated versions/options, TEST size/hash, EROFS write-denial, throughput and
unexpected-export/exposure validation are **NOT TESTED**. No benchmark result was
invented. No sudo, package install/update/download, account creation, exportfs,
fstab/firewall/systemd mutation, mount/unmount or live job action occurred.

The report lists the concrete remaining server-first, all-three-client execution
sequence. Publisher provisioning, package source/cache, stable addressing/pi01,
new-service listener policy, runtime authorization/isolation and hard-I/O
supervision remain unresolved. The pi01 answer has now been received: keep it
deferred and leave deployment pending. No further deployment actions are taken;
all-three-Pi acceptance remains required. The controller remains a single point of failure.

Manual checks are the commands above with the expected blocked/simulation exit
codes. No host rollback is needed today; undo only the two new docs and this
appended section if desired, using `/tmp/hybrid-storage-2-4.patch` after checking
for later edits. From the parent Git root:

```bash
git apply --reverse --check /tmp/hybrid-storage-2-4.patch
```

Keep all data, DBs, checkpoints, prior modifications and legacy services. Future
live rollback must follow the scoped journals/drain/unmount/export restoration
instructions already documented; no broad resets, deletion or global unexports.
**STOP with 2.4 pending; do not advance to 2.5.**

## Phase 2.4 revision — pi02/pi03 explicitly authorized; controller root blocked

The user superseded the earlier deployment deferral with "no do it without pi1 /
pi2 and p3 are active". This explicitly narrows 2.4 to pi02/pi03. pi01 remains
untouched/excluded; two-node results must not be represented as three-node results.
See [current two-node status/admin handoff](08-nfs-two-node-deployment.md) and
[fresh evidence](08-nfs-two-node-evidence.json). **Live 2.4 remains NOT COMPLETE.**

Added optional `deployment_nodes` to the existing version-one infrastructure
proposals. Omission still selects/requires pi01/pi02/pi03; explicit pi02/pi03
selection is now supported without broadening host rules or accepting unknown
selected addresses. Server rendering/prerequisites/active metadata and client
preview/admission use that selection. Excluded client inspection fails before
host queries. Both proposals select pi02/pi03; pi01's null address is preserved.

Production/helper files changed: `hybrid_storage/nfs_server.py`,
`hybrid_storage/nfs_client.py`, `tools/nfs_client_setup.py`,
`config/nfs-server.json`, `config/nfs-clients.json`,
`tests/test_nfs_server_setup.py`, `tests/test_nfs_client_setup.py`.
Documentation/index edits: `config/README.md`, `hybrid_storage/README.md`,
`tools/README.md`, `docs/storage/07-nfs-deployment.md` (historical scope note),
and this appended progress section. Added only
`docs/storage/08-nfs-two-node-deployment.md` and
`docs/storage/08-nfs-two-node-evidence.json`. All earlier dirty work is preserved.

Actual final local regression command:

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py test_nfs_server_setup.py test_nfs_client_setup.py test_storage_config.py test_storage_backends.py
```

**90 passed, 0 failed/skipped**, four files: server 22, client 27, config 17,
backend 24. New tests cover two-node fake staging/idempotence, omitted-field
compatibility, invalid/duplicate selections, excluded-known-client exports/active
table refusal and excluded-node rejection before inspection. All prior tests pass.
Both generated unit sets passed local native systemd verification; Pi execution
has not occurred. JSON/link/whitespace/preservation/scoped rollback checks passed.

Fresh SSH/HTTP probes confirm pi02/pi03 active with matching API identities; pi01
was not contacted. Controller preflight now has four blockers (publisher UID/GID,
two NFS packages); pi01 is no longer a blocker in this explicit scope. Actual
`sudo -n -l` returned 1 / "sudo: a password is required". This is OS authentication,
not an automatic approval-review rejection. Without credentials, this session
cannot execute root-only controller provisioning/configuration. The guide gives
concrete initial administrator commands; no new scope approval is requested.

No application API/DB/schema/job/lease/checkpoint/DDP/worker inventory or storage
flag changed; legacy/default-off remains. No root package/account/export/service/
fstab/firewall/mount action or TEST transfer/write probe occurred. Live version,
hash/write-denial/throughput/exposure tests are NOT TESTED. Keep the old pi02 share
and legacy agents intact. Controller SPOF, endpoint/process isolation, hard-I/O
supervision, package/UID provisioning and actual root access remain unresolved.

Manual verification and expected results are in the current guide/evidence.
Repository rollback is `/tmp/hybrid-storage-2-4-two-node.patch`, checked with
`git apply --reverse --check /tmp/hybrid-storage-2-4-two-node.patch` from the parent
Git root. Restore only this revision's edits after checking concurrent work; no
host rollback is needed because nothing was deployed. Removing selection restores
the old three-node requirement, not live mount/export state. **STOP at 2.4; no 2.5.**

## Phase 2.5 — requested; safe checks/runbook delivered, live tests blocked

The user requested 2.5. Scope remains explicitly pi02/pi03; pi01 was not contacted.
**2.5 live acceptance NOT PASSED:** fresh observations show the new server/client
mounts are still absent, so no disposable hybrid NFS mount exists to disconnect.
See [outage preflight/procedure](09-nfs-outage-validation.md) and
[fresh evidence](09-nfs-outage-evidence.json). No old mount or service was disturbed.

Added only `docs/storage/09-nfs-outage-validation.md` and
`docs/storage/09-nfs-outage-evidence.json`; appended this progress section.
No production/test source, infrastructure config, application API, DB/schema,
storage flags, scheduler/job/lease/checkpoint/DDP state changed. All previous
dirty work remains intact. Legacy/default-off remains.

Actual read-only checks around 22:17 Asia/Dhaka on 7 November: controller NFS
packages/server units still absent and no observed TCP 2049 listener. Both Pis
responded over SSH and health/resources HTTP with matching identities. Their new
mount/automount/ready units are not found/inactive. The current mount guard ran in
memory under each base Python 3.11.2 and returned ready=false for the absent exact
new NFS mount, without data-path access. Existing `/mnt/nfs_share` RW mounts were
observed only in kernel metadata. Controller `sudo -n -l` still fails with password
required (OS authentication, not automatic approval rejection).

Actual safe local command:

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py test_nfs_client_setup.py test_storage_backends.py
```

**51 passed, 0 failed/skipped** (27 client + 24 backend; two files). Existing
fixtures validate missing-mount/unsafe-option admission, generated ordering,
interruption/disk-full behavior, read-only NFS and simulated EIO/ETIMEDOUT/EACCES/
ESTALE propagation. These tests do not simulate real hard-mount kernel blocking
or prove live disconnect/recovery. No new test implementation or benchmark exists.

The runbook records actual configured timers: mount 15s, ready process 5s,
automount idle 0, hard/timeo=50/retrans=2/retry=0. No actual outage timings were
measured. Primary NFS/systemd references distinguish mount-command/RPC timers
from unbounded hard-read retry behavior. It specifies disposable private-mount
and isolated-transport prerequisites, host/job noninterference checks, pending
producer tracking, restore-transport-first rollback, scoped normal unmount and
journal restoration. No fault fixture was implemented or claimed operational;
the procedure must be concretely reviewed before privileged physical tests.

No mount/unmount, network disconnect/route/firewall change, server/agent restart,
reboot, package/account operation, job action, TEST transfer, global cache drop or
data deletion occurred. All live outage/unmount/recovery rows are NOT RUN.

Manual reproduction/expected results: rerun the 51-test command, then
`python3 -m json.tool docs/storage/09-nfs-outage-evidence.json > /dev/null` and
`git diff --check`; expect passes/valid JSON/no whitespace diagnostics. Existing
safe read-only probe scripts/results are embedded in evidence. No host rollback
is needed; repository rollback is only these two reports and this appended
section, with `/tmp/hybrid-storage-2-5.patch` checked using
`git apply --reverse --check /tmp/hybrid-storage-2-5.patch` from the parent Git root.
Preserve data, checkpoints, earlier modifications and legacy services.

Residual gates: finish 2.4's administrator provisioning/server/client deployment,
then establish the reviewed disposable isolation fixture and measure actual
mount/read/cancel/recovery behavior on both selected workers. Controller SPOF,
hard-I/O supervision and missing authenticated staging/tenant isolation remain.
**STOP: 2.5 NOT COMPLETE; Phase 2 NOT COMPLETE.**

## Phase 2 completion — live 2.4/2.5 passed on approved pi02/pi03 scope

7 November 2026, following the user's “complete phase 2” instruction. The prior
blocked reports above are historical observations. Normal desktop administrator
authentication succeeded for the controller; no sudo policy or SSH setting was
changed. Existing worker `sudo -n` access succeeded. pi01 was neither contacted
nor configured, following the user's explicit two-node exception.

**Phase 2 PASS for pi02/pi03; not an all-three-Pi pass.** See
[actual results/rollback](10-phase2-completion.md) and
[durable commands, sources and evidence](10-phase2-live-evidence.json).

Installed the controller NFS packages and a non-login publisher account (actual
UID/GID125), staged backup-first restricted RO exports, activated LAN-only TCP
NFSv4.1/v4.2, and disabled newly introduced legacy RPC listeners. The explicit
export has only pi02/pi03, ro/sync/root_squash/secure; no home/DB/checkpoint export.
Both Pis received the reviewed new mount/automount/ready units and guard. Their
fstab hashes, old share and original agent PIDs945/917 remained unchanged.

Both workers read and SHA-256-verified a public 32MiB TEST artifact; writes failed
with EROFS/no file created. Actual NFS4.1 read-plus-hash measurements were
3.9740MiB/s (pi02) and 8.7458MiB/s (pi03), including CPU/cache effects. Separate
private pseudo-root probes on both nodes found only the intended navigation
tree and rejected etc/home/setup-marker/private-publication paths with ENOENT.

Added `hybrid_storage/nfs_fault_probe.py` and five Pi-less tests. The administrator
fixture defaults to a nonmutating plan; actual faults ran in private mount/net/
hostname namespaces through a dedicated Unix-gateway/loopback relay. No host
network setting, legacy service or production job was disrupted. Both nodes
rejected unmounted admission, failed a disconnected private mount helper after
about15s, kept a reader pending for about3s, retained readable verified local
input, then completed the full correct hash after relay restoration (pi02
4.6938s, pi03 4.8178s). Both normally unmounted with no pending fixture processes.

Actual development failures are retained: initial relative pkexec copies failed;
pi02's guard falsely rejected Linux's VFS-ro/superblock-rw representation (fixed,
regression-tested and backed up); the initial weaker fault fixture failed its
pending-reader assertion and was normally reconciled before the stronger tests.
An optional additional native repeat/no-op controller audit did not execute after
renewed desktop authentication was pending; no native repeat pass is claimed.

Final safe local command:

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py test_nfs_server_setup.py test_nfs_client_setup.py test_nfs_fault_probe.py test_storage_config.py test_storage_backends.py
```

**96 passed, zero failures/skips**: server22 + client28 + fault5 + config17 +
backend24. Native systemd verification passed on both Pis. The report records
exact tested/untested boundaries, backup paths, manual metadata checks and scoped
rollback. The repository patch is `/tmp/hybrid-storage-phase2-completion.patch`;
check it with `git apply --reverse --check` from the parent Git root before use.

`config/nfs-server.json` now records publisher125/125. The mount guard and client
tests changed for VFS flags. New fixture/tests/report/evidence and current-status
README links were added; earlier dirty work was preserved. No application API,
DB/schema, job/lease/checkpoint/DDP behavior or storage flag changed. The existing
DB hash matches the beginning of this turn and remains local ext4.

Residual limits: only public TEST bytes were exported; full authenticated staging,
supervised hard-I/O, tenant process/network isolation, registry and locality remain
later work. The controller remains a single point of failure. No real reboot,
physical network unplug, production training or pi01 test occurred. The measured
mount failures are private direct helpers, not a real boot/systemd timeout test.
Legacy/off/resource remains active. **STOP after Phase 2; no later phase started.**

## Phase 3.1 — strict input adapters and controller seam

The user requested Phase 3; the one-subphase-per-turn rule remains in force.
**3.1 PASS; STOP before 3.2.** Last completed prior phase is Phase 2 within the
approved pi02/pi03 exception; pi01 stays deferred. No remote node was contacted.
See [3.1 contracts/report](11-controller-input-adapters.md) and
[test/preservation evidence](11-controller-input-evidence.json).

Added `hybrid_storage/inputs.py` with strict SHA-256 object keys, local and NFS
wrappers reusing the existing `StorageBackend` contract, frozen expected file
identities and immutable project-path allowlists. Inputs reject traversal,
noncanonical paths, malformed/mismatched object prefixes and unlisted files before
I/O. Controller publication uses the local adapter/private staging, requires key/
digest agreement and retains verified exclusive publication. Worker/object and
project views cannot write. Existing general-key adapters remain unchanged.

Added `controller/input_storage.py`: explicit trusted factories `local_objects`
and `project_inputs`, without DB imports or route/config selection. Callers must
first bind authenticated owner/job authority and verify the existing legacy
manifest. These are interfaces, not a new digest-fetch endpoint or tenant auth.
No production data registration/copy, job migration, legacy manifest change,
SQLite schema change, API/route rewrite, runtime flag change or service action.

Added `tests/test_storage_inputs.py` (18 tests), reports and current documentation
links. The local integration test stages a real job in a temporary controller
copy, reads its manifest-approved files, explicitly publishes tiny test objects,
reads through a fake NFS view and confirms legacy manifest bytes and STAGED state.
The pre-edit selection passed50 tests. Final command:

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py test_storage_inputs.py test_storage_backends.py test_storage_config.py test_job_storage.py
```

**68 passed, zero failures/skips; 4/4 files**: new18 + existing adapter24 + config17
+ job storage9. Existing tests exercise legacy portal uploads, dispatcher ordering,
resource placement and snapshot recovery with temporary state. No live NFS/Pi,
reboot, full DDP or production-service validation was run. Manual checks and
expected results are in the report. Repository-only rollback patch:
`/tmp/hybrid-storage-3-1.patch`, checked with `git apply --reverse --check` from the
parent Git root; no host rollback is needed. Preserve all earlier dirty changes.

The startup gate still enforces legacy/off/resource. These interfaces do not yet
attest NFS mount identity/availability, physical locality of configured controller
roots, read-time byte integrity or bounded hard-I/O. Runtime opt-in, migration
references, authenticated staging and process/network isolation remain pending.
The central controller remains a single point of failure. **Phase 3 overall is
not complete; immediately next subphase is 3.2. STOP.**

## Phase 3.2 — input health, fail-closed admission and supervised probes

**3.2 PASS in fake/local tests; STOP before 3.3.** See
[contracts/report](12-input-health.md) and [actual evidence](12-input-health-evidence.json).
No remote worker or production service/config/mount was touched. pi01 remains
deferred; there is no new live Pi validation or overall Phase 3 acceptance.

Strict NFS reads require an explicit expected source and exact RO/security mount
metadata. Generalized the existing standalone guard's target without changing
its default. Opened-file mount ID must match admission, with current mount
rechecks before yielding and after successful access. Ordinary directories,
autofs-only triggers, unsafe/mis-mounted/replaced views and missing metadata fail
closed. NFS publication always fails without touching underlying directories.
Strict local object publication now checks the anchored destination/staging FDs
as supported ext4/xfs/btrfs before temporary creation/bytes. The legacy hook is
empty by default; existing general-key adapter behavior remains unchanged.

Added `mount_identity.py`, `health.py`, `health_worker.py` and
`tests/test_storage_health.py`. Health requests bind expected size/SHA to an
authorized input; a read-only child verifies complete bytes and metadata
stability. Missing source, mount, corruption, permission and errno-bearing I/O
failures have explicit results with no fallback. Kernel-cached reads may succeed
offline; this proves current readable bytes, not fresh network/server liveness.

The supervisor returns on its child-wait deadline, requests termination and
retains still-live owned processes. Capacity stays busy until reconciliation;
late successful reads remain failed. Close does not discard pending handles.
This is an in-memory component, not a durable restart-aware service. Future
integration must retain/drain it and establish restart reconciliation. No health
child can publish/checkpoint/update jobs. Raw synchronous NFS I/O remains blocking.

Baseline96 tests passed. Final command:

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py test_storage_health.py test_storage_inputs.py test_nfs_client_setup.py test_nfs_server_setup.py test_storage_backends.py test_storage_config.py test_job_storage.py
```

**144 passed, zero failures/skips; 7/7 files**: health26 + input18 + client28 +
server22 + adapter24 + config17 + storage9. Tests include actual local child
verification, real local-directory negative NFS admission and disposable sleep
termination/reaping. An uncancellable kernel task is modeled only. Existing
fake-NFS tests now explicitly mock source/mount/FD facts. No live NFS/Pi fault,
reboot, throughput, production training, full DDP or parent-crash test occurred.

Source changes are confined to input/health/descriptor/guard components and the
controller factory docstring, tests, reports and README/progress links. No HTTP
API, DB/schema, existing job state, storage flag or lease/checkpoint format change.
The startup gate stays legacy/off/resource. Current report/evidence contains
exact file list, commands, preservation checks and manual expected results.
Repository rollback: review `/tmp/hybrid-storage-3-2.patch`, then
`git apply --reverse --check` from the parent Git root. No host rollback needed.
Preserve earlier modifications and commit `ddc9a7b`.

Residuals: deadline is not kernel cancellation; health is advisory, not staging or
launch authority; ownership is not restart-durable; supported local filesystem
types are intentionally restricted. Controller SPOF, tenant isolation and runtime
integration remain gated. **Next: 3.3 only. STOP.**

## Phase 3.3 — pinned selection, retained NFS intent and legacy compatibility

8 November 2026. **3.3 PASS for selection/compatibility scope; STOP before 3.4.**
See [contracts, reference plan and rollback](13-storage-selection.md) and
[test/preservation evidence](13-storage-selection-evidence.json).

Added stdlib-only `controller/storage_selection.py`: idempotent local SQLite
side-table, versioned immutable selected backend, absent-binding=legacy and NFS
execution guard. `add_job` pins new custom jobs atomically, defaults its old API
to legacy, and never backfills/reinterprets old jobs. Existing input manifests,
execution specs, checkpoint formats and status vocabulary remain unchanged.

The portal/CLI accept an optional selection token outside the job spec. Operator
legacy mode rejects NFS override; limited NFS config selects new held submissions
and still permits explicit legacy jobs. Controller startup accepts this metadata
mode so legacy monitoring/recovery continues. Cache/locality/bandwidth remain
rejected. Actual `config/storage.json` stays legacy/false/false/resource.

NFS uploads use the existing local snapshot flow, then commit HELD with a reason;
no worker preparation is enqueued. Release, preparation/verification, environment
records, queuing, fairness eligibility, reservation and single/DDP dispatch guard
the persisted selection. Cancel remains owner-authorized and retains files.
No NFS object publication, new transfer protocol, automatic old-file migration or
wholesale copy. The old-file locator/explicit adoption plan is documentation;
registrar/sidecars, trusted staging, tenant isolation and durable process ownership
remain gated. These jobs cannot run NFS yet.

`tools/run_tests.py --isolated` now provides reproducible source-copy testing,
excluding live databases/runtime roots and clearing storage-root overrides.
Existing invocation remains compatible. This was needed because older portal/
queue suites initialize SQLite on import before redirecting fixture DBs.

Baseline109 tests passed. Development exposed a configured CLI fixture missing
the shared config package; the bundle now includes it. Explicit absent-config
legacy compatibility and present-config/missing-package rejection are tested.
Earlier failing logs are retained; they are not pre-existing failures.

Actual final command:

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_storage_selection.py test_storage_config.py test_job_submission.py test_job_storage.py test_student_portal.py test_queue_controls.py test_resource_allocation.py test_storage_inputs.py test_storage_health.py
```

**130 passed, zero failures/skips, 9/9 files**: selection21, config17, submission6,
storage9, portal17, queue10, resources6, input18 and health26. Tests cover the
old running job/manifest/checkpoint boundary, atomic rollback/immutability, config
flip/restart, legacy HTTP response/preparation, NFS retained holds and no transport
even with inappropriate states, CLI parsing, and unchanged dispatcher loop order
under both defaults. Existing fairness/resources/cancellation/health tests pass.

No live controller import/migration, DB write, app restart/config activation,
SSH/Pi/NFS action, actual training/DDP run or benchmark. pi01 stays deferred; no
cluster validation is inferred. The live DB, actual config and prior modifications
are preserved; exact source/evidence checks are recorded in the report. Current
read-only SQLite schema inspection confirms the binding table/trigger are absent
from the live WAL DB; no controller module was imported for that probe. The
main-file hash matches before/after the probe and the turn baseline. The current
repository rollback patch is `/tmp/hybrid-storage-3-3.patch`, checked with
`git apply --reverse --check` from the parent Git root. No host rollback needed.
After any later deployment, preserve bindings and cancel/quarantine NFS intent
before downgrading to binaries that ignore the guards; changing config alone
does not rebind jobs. Never restore an old DB over new jobs or delete snapshots.

The central controller remains a single point of failure. Full authenticated
staging/worker isolation, durable health-process restart reconciliation and input
registration remain unavailable. **Phase 3 remains incomplete. Next: 3.4 only.**

## Phase 3.4 — fake/local acceptance completed (8 November 2026)

User requested 3.4 after the accepted 3.3 boundary. Added eight tests in
`tests/test_storage_readers.py` for independent concurrent readers, trusted grant
separation, ready/corrupt/missing outcomes, active-reader mount replacement,
partial-read permission/outage failures, legacy concurrency and shipped config
samples. Added `config/storage-examples/{legacy,nfs-held}.json` and its precise
operator guide. No runtime source, active flags, schema or service changed.

Baseline: **204 passed, zero failures/skips, 12/12 files**. Final isolated
selection: **212 passed, zero failures/skips, 13/13 files**. Eight new tests also
passed separately. The [3.4 report](14-phase3-validation.md) and
[evidence](14-phase3-validation-evidence.json) preserve exact commands/output,
coverage, config validation, runtime/DB hashes, rollback and remaining risks.
Existing modifications and main DB bytes are preserved; no live controller
initialization, SSH/Pi action, deployment or infrastructure mutation occurred.

**3.4 and Phase 3 adapter/fake/local compatibility scope: PASS. STOP.** This
does not enable NFS training. New NFS submissions remain HELD, cache/registry/
locality execution is unavailable, and authenticated staging, training isolation
and durable health-process ownership remain later gates. The central controller
is still a single point of failure. pi01 remains deferred by the earlier explicit
pi02/pi03 exception; no all-three-Pi validation, live fault test, full training/DDP
run or new benchmark is claimed. No next subphase implemented.

## Phase 4.1 — immutable artifact registrar (8 November 2026)

The user supplied Phase 4 after accepted 3.4; only 4.1 is implemented this turn.
New `hybrid_storage/artifacts.py` registers trusted binary streams by computed
SHA-256 into `objects/sha256/<prefix>/<digest>`. Exact size and optional expected
digest are checked with bounded copy/hash/readback. Unique private scratch,
protected local directory FDs, 0444 inodes, fsync and Linux atomic no-replace
rename provide publication. Duplicates are fully verified and byte-compared,
then reuse the existing inode; conflicts raise, never overwrite. The existing
backend publication contract and all runtime call sites remain unchanged.

New `tests/test_artifact_registration.py`: **24 passed** locally, including
thread/process races, process crashes before/after publication, post-rename
fsync failure/retry, corruption/collision simulation, path safety and ENOSPC.
Baseline: **212 passed, zero failures/skips, 13/13 files**. Final selected
isolated regressions: **236 passed, zero failures/skips, 14/14 files**. Initial
new-test fixture permission/keyword-override failures were corrected and retained
in the evidence; no pre-existing selected regression failure was observed.

See [4.1 report](15-artifact-registration.md) and
[evidence](15-artifact-registration-evidence.json) for exact filenames, APIs,
commands/logs, hashes, preservation, manual checks, scoped diff and rollback.
Existing user changes, jobs, main DB bytes and active legacy/OFF/resource config
are preserved. No live service, infrastructure, SSH/Pi, NFS, DB migration or
automatic enrollment action occurred. Private crash orphans are not swept.

**4.1 PASS for trusted local registration. STOP before 4.2.** Dataset manifests/
ACLs, immutable job bindings/migrations and explicit lazy legacy registration
remain 4.2–4.4. No runtime registrar call site, NFS staging, cache or locality
execution is enabled. The controller remains a single point of failure. pi01
remains deferred under the earlier user exception; no all-three-Pi, actual
training/DDP or hardware power-loss validation is claimed. Phase 4 is incomplete.

## Phase 4.2 — immutable dataset manifest and ACL contract (8 November 2026)

User requested 4.2 after accepted 4.1. New `dataset_manifests.py` defines canonical
immutable dataset versions, opaque controller-issued IDs, owner identity, typed
relative file/directory entries and predecessor hashes. Separate trusted current
ACL snapshots enforce private defaults, owner-only writes/reader changes and
explicit read-only sharing. Current ACLs govern access to old versions; no ACL
grant is embedded into immutable manifests. New `dataset_validation.py` performs
read-only no-follow tree inventory/hash and metadata-only ZIP path/type/budget
preflight. No extraction, registration or runtime wiring is introduced.

Added a distinct dataset-manifest v1 schema and canonical synthetic fixture,
preserving historical draft schemas, existing manifests and all runtime APIs.
New tests: **26 manifest/ACL + 13 source/ZIP = 39 passed**. Baseline: **236 passed,
zero failures/skips, 14/14 files**. Final isolated selection: **275 passed, zero
failures/skips, 16/16 files**. Separate system-Python draft compatibility: **15
passed**; new JSON Schema probes: **28 passed**. Review added a missing-manifest
access regression (two initially failing subtests); explicit public type checks
fixed it, and before-fix output is retained as a new-code failure, not baseline.

See [4.2 report](16-dataset-manifests.md) and
[evidence](16-dataset-manifests-evidence.json) for exact APIs/files, schema,
commands/output, security limits, source/DB preservation and rollback diff.
Active flags remain legacy/OFF/resource; existing runtime sources, main DB bytes,
user modifications and prior progress are preserved. No live DB import/migration,
SSH/Pi, NFS, service or infrastructure operation occurred.

**4.2 PASS for typed/canonical manifests, validation and trusted ACL contract.
STOP before 4.3.** Persistent authority, manifest provenance/object enrollment,
unique version/head transactions and job bindings remain 4.3; explicit legacy
enrollment remains 4.4. Parsed documents or stale/client-supplied ACL snapshots
must never authorize production access. Source inspection is not an atomic
snapshot and ZIP preflight is not extraction. The central controller remains a
SPOF; no all-three-Pi, actual training/DDP or new benchmark acceptance is claimed.
pi01 stays deferred under the prior explicit exception. Phase 4 is incomplete.

## Phase 4.3 — local registry and exact job input bindings (8 November 2026)

User requested 4.3 after accepted 4.2. Added `registry_schema.py` for an explicit
version-one additive migration with plan, checksum, exact shape/FK validation,
constraints, indexes and immutable records. Added `registry.py` for verified
artifact ownership, source manifests, persisted dataset versions/current ACLs,
head compare-and-swap and immutable job input binding. `input_manifests.py`
retains source-tree v1 executable metadata and defines exact source/data/
requirements/legacy/spec identities for registered jobs. Physical dedup requires
verified bytes for each owner's enrollment; knowing a digest grants nothing.

Bindings only enroll owner-authorized explicitly selected HELD NFS custom jobs,
and must match the existing DB-pinned legacy snapshot/spec/inventories. Binding
and reads use exact committed dataset versions and current ACLs, including a
recheck after byte verification. Existing backend-selection tables, job states,
leases, checkpoints and runtime APIs are untouched. Old unbound jobs retain the
legacy path. No live migration, startup/upload wiring or lazy enrollment occurred.

New `test_storage_registry.py`: **38 passed**, covering two owners' physical
reuse/separate bindings, actual copied-controller snapshots, ACL revocation,
requirements/source/data conflicts, competing version writers, concurrent and
interrupted migration, incompatible/future schema, FKs/indexes, transaction guards
and active legacy attempts/leases/reservations/checkpoint-byte preservation.
Baseline: **275 passed, zero failures/skips, 16/16 files**. Final main isolated
selection: **313 passed, zero failures/skips, 17/17 files**. Separate historical
draft tests: **15 passed**, no skips. No selected baseline or new-test failure
was observed. No actual training/DDP recovery, Pi/NFS or benchmark run.

See [4.3 report](17-registry-and-job-inputs.md) and
[evidence](17-registry-and-job-inputs-evidence.json) for API/DDL/checksum, exact
files/commands/output, source/DB preservation, manual checks and scoped rollback.
Active config remains legacy/OFF/resource; user changes, previous runtime code,
live main DB bytes and prior progress are preserved. No service, `/etc`, SSH,
mount or remote action was taken. Schema changes exist only in disposable tests.

**4.3 PASS for explicit local registry/binding integration. STOP before 4.4.**
Automatic/lazy legacy enrollment, production wiring, authenticated worker staging,
isolation, cache/locality execution and live deployment remain unavailable.
No destructive downgrade/cleanup exists; later rollback must preserve registry
metadata, immutable inputs and jobs. The controller remains a SPOF; all-three-Pi
future validation remains the default with pi01's earlier exception explicit.
Phase 4 overall is incomplete. No next subphase implemented.

## Phase 4.4 — explicit legacy enrollment and Phase 4 local acceptance (8 November 2026)

User requested 4.4 after accepted 4.3. Added `legacy_registration.py` and a thin
explicit controller seam. It authorizes one committed managed snapshot, requires
the existing verifier callback without importing a DB-initializing module,
validates controlled local paths/pinned metadata, copies only declared inputs
through CAS and commits source/private dataset/exact job references atomically.
Registry snapshot enrollment reuses the existing schema/checksum; no migration
or automatic job scan is added. Exact retries and concurrent requests reuse one
dataset/sidecar. Original files/backend/status/recovery identity are unchanged.

New `test_legacy_registration.py`: **25 passed**, including a known offline
legacy scalar-training workload before/after registration (same output/exit 0),
two owners' physical reuse/private access, dataset v2 with old binding preserved,
active/implicit legacy compatibility, NFS hold preservation, retries, concurrency,
publication/SQL interruption, missing/corrupt/mutated inputs, owner/metadata drift,
path/schema/ACL safety and import guards. An assertion-placement mistake while
adding the final workload case caused one fixture failure; it was corrected and
retained in evidence, not attributed to baseline. Production checks were not
weakened. No selected pre-existing failure was observed.

Baseline: **313 passed, zero failures/skips, 17/17 files**. Final main isolated
selection: **338 passed, zero failures/skips, 18/18 files**. Separate historical
draft validation: **15 passed**, no skips. The complete selection includes prior
bounded publication, collision simulation, actual local process-crash/race,
manifest/path/ACL, registry/version, old migration/lease/checkpoint and flag-off
regressions. No hardware/Pi/NFS or PyTorch/DDP training/recovery run is claimed.

See [4.4 / Phase 4 report](18-legacy-registration-and-phase4.md) and
[evidence](18-legacy-registration-evidence.json) for exact files/APIs, commands,
raw output, source/DB/schema preservation, manual checks, residuals and rollback.
The live main DB, actual legacy/OFF/resource flags, prior user changes, schema v1
checksum and progress prefix are preserved. No live migration, registration,
service, SSH, mount, `/etc` or remote action occurred. Only disposable data was
enrolled/tested; old snapshots and actual checkpoint data were never moved.

**4.4 and Phase 4 artifact/manifest/registry local code acceptance: PASS. STOP.**
No automatic UI/background wiring, authenticated worker staging, training
isolation, persistent worker cache, locality execution, GC/quota or deployment
is enabled. Strict new validation may reject historical inputs for enrollment
without changing their legacy execution lane. The central controller remains a
SPOF; all-three-Pi future validation remains required unless expressly excepted,
and pi01's previous deferral remains explicit. No next phase implemented.

## Phase 5.1 — persistent local worker cache (8 November 2026)

User supplied Phase 5 with one subphase per turn after committed Phase 4
acceptance (`da85a0c`); the checkout was clean. Implemented **5.1 only** in
`hybrid_storage/worker_cache.py` and inactive `agent/cache_storage.py`. Explicit
operator paths select a protected local filesystem, never an NFS cache root.
Initialization creates dedicated worker-local v1 SQLite metadata and private
object directories; no live DB/setup action occurs. Full SHA/size/FD verification
records complete replicas, verification/use times and exact job/attempt pins.
Current free/service-available disk is observable. Stale/changed/future metadata
forces rehashing; `open_verified` always hashes before yielding the same FD.

Missing/corrupt/I/O observations fail closed without deleting data or releasing
old pins. Unpin affects only the exact requested attempt. Pins/bytes persist
across fresh processes, and abrupt uncommitted SQL changes roll back. No automatic
reboot pin clearing, directory-wide adoption, copy/fetch/repair, eviction, HTTP,
lease/lifecycle integration or feature activation. Current same-UID training is
not an isolation boundary; cache-enabled runtime remains rejected. Legacy
execution, scheduler, recovery, checkpoint formats and active config are intact.

Baseline **338 passed, zero failures/skips, 18/18 files**. New cache suite **33
passed**. Final isolated combined selection **371 passed, zero failures/skips,
19/19 files**. No selected pre-existing/new-code failure was observed. Tests
include actual independent process restart/concurrency, abrupt transaction exit,
lock timeout, corruption/staleness, bounded reads, paths/metadata, exact pins and
disk accounting. No machine/Pi reboot, NFS fault, hardware power loss, actual
training/DDP or new benchmark is claimed. Production DB/config hashes match the
turn baseline; no service, SSH/Pi, mount, `/etc` or remote mutation occurred.

See [5.1 report](19-worker-cache.md) and [evidence](19-worker-cache-evidence.json)
for exact files/APIs/commands/output, security gates, manual checks and scoped
rollback. **5.1 local code acceptance PASS. STOP before 5.2.** Overall Phase 5
remains incomplete: missing-only NFS transfer, usable private workspace,
corrupt-refetch and warm zero-transfer acceptance await later subphases. The
controller remains a SPOF; future all-three-Pi validation remains required unless
expressly excepted. pi01's previous deferral remains explicit.

## Phase 5.2 — missing-only guarded NFS cache fetch (8 November 2026)

User requested 5.2 after accepted 5.1. Earlier 5.1 changes were still uncommitted
above `da85a0c`; saved their exact contents before editing. Added `cache_fetch.py`
and fixed `cache_copy_worker.py`, plus an inactive worker factory. Exact trusted
manifest input lists require a current authorization callback. Fully verified
hits never open/probe NFS; unique misses copy through the existing mount-guarded
read-only NFS adapter into unique local parts. Bounded stream/readback hashing,
fsync, parent revalidation and no-replace rename establish complete replicas.

Per-cache OS slots serialize all processes' disk admission and NFS producers.
Quota includes objects, parts and retained quarantine; free-space reserve is
checked before each attempt. Transient returned I/O errors retry within finite
limits; permission, corruption, source absence, mount admission and ENOSPC fail.
Unpinned corrupt bytes are quarantined intact before replacement; existing pins
block repair. No overwrite/delete of user inputs, jobs or checkpoints occurs.

Parent-owned helper deadlines fence late results. Timed-out helpers retain all
relevant FDs, parts and capacity until reconciliation. The helper inherits the
OS slot, so surviving I/O after parent crash still excludes another producer;
fixed helper code never publishes final names or updates SQLite. Old crash files
remain accounted, with no automatic cleanup/adoption service. Runtime stays
legacy/OFF/resource; no schema migration, new HTTP protocol, startup hook,
private workspace, automatic pin lifecycle or scheduler change was added.

Baseline **371 passed, zero failures/skips, 19/19 files**. Final combined
selection **404 passed, zero failures/skips, 20/20 files**, including 33 new
fetch tests. Initial 24 tests passed with test-only stderr pipe warnings; these
were fixed. An expanded test-helper quoting error briefly prevented parsing;
corrected output is retained alongside the failure. Subsequent 31-test run and
final 33-case suite passed. No selected pre-existing failure was observed.
Tests use actual local files, subprocesses, process crash/restart, concurrency,
atomic renames and disk/write/read failure injection, with positive NFS mount
metadata mocked. Real local-directory NFS admission is rejected. No live NFS/Pi,
actual network disruption, machine reboot, training/DDP or benchmark occurred.

See [5.2 report](20-cache-fetch.md) and [evidence](20-cache-fetch-evidence.json)
for exact APIs/files/commands/output, preserved hashes and subphase-only rollback
against the saved 5.1 contents. Live DB/config bytes and earlier 5.1 source/tests
and evidence are preserved. No service, `/etc`, mount, SSH or remote action.
**5.2 local/fake acceptance PASS. STOP before 5.3.** Overall Phase 5 acceptance
remains pending. The controller remains a SPOF; all-three-Pi future validation
remains required unless expressly excepted, with prior pi01 deferral explicit.

## Phase 5.3 — private copies and durable pin lifecycle (8 November 2026)

User requested 5.3 after accepted 5.2. Saved all earlier uncommitted changes and
legacy launcher/lease sources before editing. Added `workspaces.py`, typed exact
plans/identities and an inactive worker factory. Protected local copies preserve
executable metadata/empty directories and separate outputs/checkpoint candidates.
Durable control JSON follows existing worker record conventions; no SQLite
migration, new HTTP protocol, live hook or flag activation. Actual config stays
legacy/OFF/resource and same-UID training remains outside the hybrid gate.

Persist MATERIALIZING intent before unique-digest pins; count every private copy
for disk admission. Bounded copy/readback/hash/fsync/no-replace publication and
complete tree validation precede READY. Roots cannot overlap cache/NFS. Full
copies are the supported fallback; no hardlinks, bind mounts or reflink dependency.
Pin holders/control bind workspace/cache roots and job/attempt/rank/revision.
Reject plan/root rebinding and terminal replay. Managed success/failure/cancel/
expiry/launch/staging/supersession/restart/interrupt paths persist ABORTING, prove
exact producer/guardian/descendant quiescence, persist TERMINAL before unpin, and
then record cleanup completion. Missing proof/failure retains recoverable pins.
No automatic data deletion, GC or reboot pin clearing.

Baseline **404 passed, zero failures/skips, 20/20 files**. Final selection
**443 passed, zero failures/skips, 21/21 files**, including 39 new cases. Initial
34 and expanded 37 passed. A review regression exposed a new-code two-root holder
collision (3 versus 6 references), fixed with root-scoped holders/control bindings;
before-fix failure is retained. No baseline failure was observed. Tests include
inode independence, terminal outcomes/quiescence, ENOSPC/readback, authority and
path guards, concurrent handles, process crash/restart, known private offline
programs, fake-NFS-to-workspace integration and a real disposable existing lease
watchdog stopping its group before unpin.

See [5.3 report](21-private-workspaces.md) and [evidence](21-private-workspaces-evidence.json)
for exact files/APIs/commands/output, preserved hashes and rollback against saved
5.1/5.2 contents. Earlier source/tests/evidence, actual DB/config and legacy
launcher/lease sources are preserved. No service, `/etc`, SSH/Pi, NFS mount,
live setup/migration or remote action. No privileged isolation, hardware reboot/
power loss, PyTorch/DDP or benchmark claim.

**5.3 private-copy/local lifecycle acceptance PASS. STOP before 5.4.** Tenant
isolation, authenticated grant/revision service and live-launch wiring remain
gated. Detailed metrics, consolidated Phase 5 acceptance and all-three-Pi future
validation remain pending; controller SPOF and prior pi01 deferral are explicit.

## Phase 6.1 — explicit controller-local cache registry schema (8 November 2026)

User moved to Phase 6 after requesting a commit message during 5.4. New user
commit `89e2135` contains prior cache work and the metric edits; the checkout was
clean at the 6.1 baseline. Last recorded acceptance remains 5.3: dedicated 5.4
metric/fault acceptance was not completed, and Phase 5 is not silently marked
complete. This turn implements only 6.1 and preserves that committed code.

Added `cache_registry_schema.py`: explicit plan/apply/require migration after
artifact registry v1, with six tables, seven indexes and twenty triggers. Reuse
exact artifact SHA/size FKs; bind worker, cache ID/epoch, boot/controller/session
identity, inventory sequence/body, verification observations, disk capacity and
controller receipt/freshness/last-seen timestamps. Keep high-water/replay history;
default disabled/UNENROLLED/UNVERIFIED. Guard epoch/identity/cursor rollback,
stale-session reactivation and verified-byte mismatches; invalidate sessions on
revocation/retirement without deleting observations. No auto-backfill/enrollment.

Actual audit found operator SSH registration plus unauthenticated HTTP probes/
resource/job polling; no authenticated runtime worker heartbeat path exists to
reuse. Execution lease fencing is not worker authentication. No credentials,
ingest endpoint or auth assertion was invented. 6.2 requires an authenticated
channel bound to operator enrollment and current cache/worker sessions before
accepting reports. Schema constraints alone do not implement protocol/security
or live freshness eligibility.

Migration requires an explicit dedicated controller-local FULL/FK connection,
checks actual local FD identity, uses BEGIN IMMEDIATE and a separate checksum
namespace, preserves user_version/all old rows and refuses unknown shapes.
No live DB migration, startup hook, flag, existing schema/state/checkpoint,
scheduler/resource/fairness rule, live service, `/etc`, mount or SSH/Pi change.

Baseline **443 passed, zero failures/skips, 21/21 files**. Initial new suite 19
passed; expanded 21 plus prior coverage passed 464. Final boot-ID hardening and
its case yield **465 passed, zero failures/skips, 22/22 files**, including 22
new copied-controller/local SQLite cases. No selected failure was observed.
Tests include interrupted rollback, concurrent migrations, prior-row preservation,
FK/index/schema guards and synthetic untrusted/authenticated metadata fixtures;
they do not prove real authentication or runtime inventory updates.

See [6.1 report](22-cache-registry-schema.md) and [evidence](22-cache-registry-schema-evidence.json)
for exact files/API/DDL/checksum, raw commands/output, preserved hashes and scoped
rollback. **6.1 schema/local acceptance PASS. STOP before 6.2.** 6.2–6.4 and the
0/2/502 MB coverage fixture remain unimplemented. Actual config stays legacy/OFF/
resource; controller SPOF, unfinished 5.4 acceptance and all-three-Pi future
validation requirements (with prior pi01 deferral explicit) remain documented.

## Phase 6.2 — authenticated inventory and reconciliation (8 November 2026)

Preserved all eight uncommitted/untracked 6.1 files/edits from the starting
checkout. Implemented only 6.2; dedicated 5.4 acceptance is still unfinished.

Added bounded strict HMAC metadata protocol, protected local credential loader,
explicit persistent worker cache ID/epoch reporter and explicit controller-local
SQLite ingest owner. Actual operator `cluster_setup.register`, Flask agent and
resource heartbeat now have opt-in tested integration seams. No authenticated
runtime channel previously existed; only this metadata endpoint is authenticated,
not legacy launch/resources/checkpoints. No new artifact transfer protocol.

Require operator-issued unique per-worker keys/principals, exact controller
challenge/nonce/session, monotonic cache epoch/cursor, immutable replay history,
transactional FULL/CACHE_ADDED/CACHE_REMOVED and signed unchanged heartbeats.
Duplicates extend no TTL; controller forces periodic full verification. Restart/
reconnect requires a new full snapshot. Authentication/HTTP/verification failure
invalidates live cache admission before attempting durable STALE persistence;
read-only/full/unavailable DB cannot leave old observations eligible.

Schema/checksum unchanged from 6.1; no live DB migration, key provisioning,
config activation, job/checkpoint/lease/resource/fairness/reservation/concurrency
change, service or remote action. Actual configuration stays legacy/OFF/resource.
Same-UID training can steal service keys, so production activation remains gated
on isolated service/trainer identities and reviewed deployment. Component settings
in tests do not remove the portal/dispatcher execution gate.

Selected baseline: 110 tests passed, 4/4 files. Initial 25-case new suite exposed
one retry error (pending session unnecessarily replaced after a lost first
response), fixed and retained in evidence. Expanded 32 passed. Final current
source: **523 passed, zero failures/skips, 25/25 files**, including 38 new inventory
cases. Real Flask local test client plus mocked transport; no hardware or real
network listener claim. Existing execution-ownership/deployment regressions pass.

See [6.2 report](23-authenticated-cache-inventory.md) and [evidence](23-authenticated-cache-inventory-evidence.json)
for exact files/APIs, commands/raw results, preserved hashes and scoped rollback
relative to saved 6.1 contents. **6.2 local component/integration acceptance PASS.
STOP before 6.3.** Coverage/job/dataset/per-rank API and exact 0/2/502 MB fixture
remain pending. All-three-Pi validation is still required for deployment; no new
pi01 waiver or overall Phase 6 pass. Central controller remains a SPOF.

## Phase 6.3 — authorized dataset/job/rank coverage (8 November 2026)

Preserved earlier uncommitted/untracked 6.1/6.2 work. Added controller-side typed
`CoverageAPI`, immutable result and exact-job-bound `RankRequirements`. Reuse
existing owner/ACL/canonical manifest/version/job source/requirements checks in
a single read snapshot. Deduplicate SHA-256 across paths/source/data/requirements;
return exact integer required/verified/missing bytes, missing hashes, byte ratio,
inventory age and diagnostic session/cursor. Inventory auth/freshness uses the
exact live context/session on the same actual controller-local DB. Unknown/
stale/offline/revoked/invalid inventory counts as all misses. Authority failures
raise; unregistered legacy jobs keep their old fallback with no automatic moves
or registration. No object I/O, row writes, schema/config/lease/scheduler changes.

Default DDP coverage includes all source/data on every rank, matching current
DistributedSampler trainers. A trusted explicit whole-file dataset set must match
the immutable job binding/rank/world size; common source is always included.
No fabricated sample-level caching, stage grant, rank launch or group reservation.

Initial baseline: 123 pass, 4/4 files. Initial new 25 pass; expanded focused 154
pass, 5/5 files, including 31 coverage cases. Final broader run: 561 tests, 560
pass/one error/zero skips, 26/27 files. The original 26-file storage/auth/queue/
lease/deployment selection has 554 passing tests. Added DDP file has six pass
and one sandbox `socket.socket()` EPERM error before training launched. Repeated
the unchanged file with saved pre-6.3 runtime sources in a disposable copy and
reproduced the same error. No test or DDP code changed to mask it. No unrestricted
DDP/hardware success claimed. Three-worker fixture checks exact 0/2/502 **bytes**
using real local hashed objects; the requested MB fixture remains 6.4.

See [6.3 report](24-cache-coverage.md) and [evidence](24-cache-coverage-evidence.json)
for exact files/API/commands/results, preservation and scoped rollback against
starting contents. **6.3 local coverage API acceptance PASS; broader regression
has the documented pre-existing sandbox limitation. STOP before 6.4.** Config
stays legacy/OFF/resource. No live DB migration/service/credential/NFS/Pi/SSH or
`/etc` action. All-three-Pi deployment validation, trainer/service isolation,
dedicated 5.4 acceptance and central-controller SPOF remain explicit boundaries.

## Phase 6.4 — local registry fault/concurrency acceptance (8 November 2026)

Preserved earlier uncommitted/untracked 6.1–6.3 work. Added 16 consolidated local
acceptance tests. **No runtime/API/schema/auth/config changes were needed.**

The exact fixture streams real 500,000,000-byte and 2,000,000-byte objects through
the registrar and full cache verification, with separate protected cache inodes.
Three authenticated local pi01/pi02/pi03 identities report respectively
502,000,000 / 500,000,000 / 0 cached bytes and exactly
0 / 2,000,000 / 502,000,000 missing bytes. MB means 1,000,000 bytes. A duplicated
logical path for the large digest does not inflate the 502 MB required total.
Known-offline/stale identities contribute no trusted hits. This is real local
fixture data, not live Pis, NFS throughput or simulated large-file presence.

Matrix covers empty/full/add/remove/reconcile, actual corrupt/deleted replicas,
missing DB observations, duplicates/reordered/late/in-flight reports, stale TTLs,
expired execution leases (coverage does not grant execution), abrupt worker
process exit with durable cache epoch, abrupt uncommitted SQLite writer exit,
real independent-process SQLite contention, concurrent worker/duplicate commits,
busy retry/reconnect and injected second-entry disk-full rollback. Existing rows,
event/cursor consistency, integrity and foreign keys are verified. Tests use only
temporary stores/processes; no live jobs, migrations, data deletion or network.

Baseline: **91 pass, 3/3 files, zero failures/skips**. Initial new suite: **16
pass, zero failures/skips**. Final selected regression: **570 pass, 27/27 files,
zero failures/skips**, 115.7 seconds. Exact MB fixture passed both runs; about
1.504 GB of fixture-only object storage, bounded buffers, explicit free-space
skip guard (no skip occurred). No new-code or selected baseline failure.

See [6.4 report](25-cache-registry-acceptance.md) and [evidence](25-cache-registry-acceptance-evidence.json)
for exact files/commands/results/digests, preserved hashes and scoped rollback.
**6.4 / Phase 6 local registry acceptance PASS. STOP.** Current config remains
legacy/OFF/resource. No scheduler integration, OS isolation, credential deployment,
service/SSH/Pi/NFS or `/etc` change. Live all-three-Pi validation is still required;
no new pi01 waiver or production/whole-repository pass. The prior sandbox socket
EPERM on real DDP remains documented in 6.3, not rerun or hidden here. Dedicated
5.4 acceptance and central-controller SPOF remain explicit outstanding boundaries.

## Phase 7.1 — fenced staging side state / RUNNING barrier (8 November 2026)

Preserved earlier uncommitted/untracked 6.1–6.4 work. Implemented 7.1 only.
Explicit migration adds three tables, two indexes and nine triggers. jobs.status
remains SCHEDULED while per-attempt/rank side phase is STAGING, preserving existing
active quotas, reservations, queue fairness, cancellation and website/API enums.
Bind unchanged job_id:retry token, actual history attempt_id, committed input
binding, full exact rank plans, reservation fingerprints and existing lease
IDs/generations. Require full rank receipts and all current fences for READY.

Trusted begin/record_ready/read barrier recheck owner/current dataset ACL, job
state/retry, committed identities, placement and reservation, lease revocation/
renewal uncertainty, controller boot, wall and BOOTTIME expiry. Exact repeats
are idempotent; different identities/receipts cannot overwrite history. Existing
job_store.mark_running now rejects NFS without readiness and still rejects even
READY NFS through the separate execution gate. No runtime activation. Legacy
transition behavior remains unchanged. Read barrier attests local DB even for
job-store NORMAL connections; migration needs dedicated LOCAL FULL/FK.

Tests explicitly synthesize NFS SCHEDULED/reservation prerequisites on disposable
DBs because normal NFS enqueue/dispatch remains gated. Actual lease/history APIs
and mark_running are exercised; receipt digests are synthetic trusted metadata,
not proof of remote workspace checks. No RPC/launcher/private dataset path,
producer cleanup/reconciliation or UI metrics implemented: those are 7.2–7.4.

Baseline 98 pass, 5/5 files. Initial new 16 pass; expanded focused 41 pass, 3/3
files. Final current source: **590 pass, 28/28 files, zero failures/skips**,
including 20 staging cases. No selected failure. All prior Phase 6 acceptance,
legacy portal/queue/storage and execution ownership regressions pass. Prior
real-DDP sandbox socket limitation remains documented; no hardware claim.

See [7.1 report](26-staging-lifecycle.md) and [evidence](26-staging-lifecycle-evidence.json)
for exact files/schema/API/commands/results, preservation and scoped rollback.
**7.1 local state/barrier acceptance PASS. STOP before 7.2.** No live migration,
config/credential/service/NFS/Pi/SSH or /etc action. Flags remain legacy/OFF/
resource. Trainer isolation, authenticated workspace verification, all-three-Pi
validation, dedicated 5.4 acceptance and controller SPOF remain explicit. No
overall Phase 7 cold/warm/cancel end-to-end acceptance is claimed.

## Phase 7.2 — explicit authenticated staging/dispatch (8–9 November 2026)

Started from clean `main`, commit `68f59e9e72d8ecb2e1b5a98400be2e22051eafc4`;
7.1 was the last delivered subphase in repository evidence. Implemented this
subphase only. No stash/reset/commit or live DB/config/service/Pi/NFS action.

Added bounded, domain-separated request/reply HMAC staging metadata using the
existing per-worker Credential type and HTTP channel. Explicit trusted controller
runtime connects current immutable job/ACL checks, the existing fair queue,
resource selection, quotas, exclusive reservations and leases to actual worker
CacheFetcher/WorkspaceManager operations. Stage every actual rank's whole-file
inputs before any launch; recheck all ranks' fences before each signed start.
Reverify workspace bytes/pins at launch, then use the existing custom runner and
guardian with LOCAL source/data paths. Keep existing interpreter/resource/output/
checkpoint interfaces. Require authenticated ACKs before atomic RUNNING/history
and side STARTED. The signed start also binds the immutable execution-spec digest;
worker metadata drift is rejected. No schema migration or job-state enum change.

Defaults remain legacy/OFF/resource; normal startup does not install/inject the
runtime. A reviewed OS/network isolation adapter is mandatory and has no unsafe
default. Tests mock that adapter and process launch, rather than claiming tenant
isolation. NFS execution is restart-mode only; new NFS checkpoint jobs remain
held pending trusted publisher integration. Existing legacy checkpoint formats/
APIs remain. No direct-NFS training fallback or Internet runtime transfer added.
Long staging is bounded by leases; uncertain dispatch retains reservations and
blocks renewal. Authenticated stop/periodic renewal, terminal pin cleanup and
restart reconciliation remain 7.3; UI remains 7.4. Warm second-job tests explicitly
finish/reset only disposable test ownership, not production terminal handling.

Successful baseline: 64 pass, four files. New final suite: **25 pass, zero
failures/skips**. Final queue/default/barrier selection: **96 pass, six files**
(22 new-suite methods at that snapshot; the final suite adds stale queue and
prepared execution-spec drift and atomic late-bootstrap refusal cases). Broader selection: **372 pass / 14 errors / zero skips, 386
methods, 17/19 files**. All 14 errors occur at sandbox-denied `socket.socket()`
in existing group-checkpoint/recovery tests before training. Reproduced the same
14 errors against pre-change runtime sources restored into a temporary copy;
the other method passes. No test masking, network escalation or hardware success
claim. Real single-worker checkpoint and offline dependency/launcher cases pass. Final
execution-boundary verification after the last runtime change: **36 pass, 3/3
files**, including 24 new cases, offline environment and legacy agent cases;
then 25/25 new cases passed after the final atomic-bootstrap guard.

See [7.2 report](27-staging-dispatch.md) and
[evidence](27-staging-dispatch-evidence.json) for exact files, commands, raw result
references, API/security limits, source preservation and scoped rollback patch.
**7.2 local restart-mode integration PASS; production enablement/unrestricted NFS
checkpoint support NOT PASSED. Phase 7 acceptance remains pending. STOP before
7.3.** All-three-Pi deployment validation, trainer/service isolation, dedicated
5.4 acceptance and central-controller SPOF remain explicit boundaries.

## Phase 7.3 — durable staging cancellation/expiry/recovery (9 November 2026)

Preserved the starting uncommitted/untracked 7.2 work on `main` at `68f59e9`;
saved all 260 inventoried files before edits. Implemented only 7.3. No stash,
reset, commit, live DB/config/credential/service/SSH/NFS/Pi or `/etc` action.

Protected worker intents persist before fetch and possible launch; one journal
owner, durable terminal tombstones, restart aborts and a watchdog prevent forgotten
READY work from starting. Signed control reuses per-worker HMAC credentials for
status/cancel/renew. Copy releases the agent slot lock, so cancellation persists
while a producer is busy. Interrupt helpers and retain pins until materializer,
cache-wide inherited fetch lock and exact training containment are quiescent.
Remove only protected orphan parts and the recorded unpublished building tree;
keep complete objects, published inputs, results/checkpoints and unknown files.
Launch now requires reviewed termination and quiescence callbacks as well as
isolation. Missing proof, live processes, corruption and disk errors fail closed.

Explicit additive LOCAL FULL/FK cleanup migration: two tables, one index, four
triggers; old schemas/checksums, user_version, states and checkpoints unchanged.
Controller restart/outage/cancel reconciliation preserves whole-group reservations
until exact quiescent proofs or existing conservative lease fencing. LEASE_FENCED
does not claim remote unpin; keep tracking/retry old cleanup after reconnection.
Atomically close exact history/current retry, revoke leases and release slots;
preserve prior attempt history in the pre-begin gap. Signed running renewal never
revives expired ownership. NFS result publication serializes with cancellation
under SQLite's write lock and rechecks all-rank live fences/ACLs. Late success,
wrong attempt/worker and unsigned fallback are refused. Default reconciliation
with no runtime blocks hybrid renewal instead of using legacy control endpoints.

Baseline **67 pass, four files**. Broad current-selection snapshot **240 pass,
13/13 files**, zero failures/skips. Later fencing **89 pass, six files**; boundary
**64 pass, three files**; final expanded new suite **34 pass**, zero failures/skips.
Counts across reruns are not added. Fixtures use real local journals/SQLite/hash/
copy helpers and signed Flask clients, plus a real watchdog, concurrent SQLite
writers and one disposable guardian/child/grandchild shutdown test. Other trainers
and containment providers are mocks; no production isolation or hardware claim.
Initial harness timing/assertion issues and an interrupted mocked-thread test are
documented, fixed and not masked. Previously documented real-DDP socket EPERM
suites were not rerun or claimed passing here.

See [7.3 report](28-staging-recovery.md) and
[evidence](28-staging-recovery-evidence.json) for exact files/APIs/migration,
commands/results, preserved sources, risks and rollback scoped against 7.2.
**7.3 local lifecycle acceptance PASS. STOP before 7.4.** Defaults remain
legacy/OFF/resource; production bootstrap/isolation/containment proof, NFS
checkpoint publisher support, all-three-Pi validation and Phase 7 UI/acceptance
remain pending. Long staging stays lease bounded; controller/NFS remains a SPOF.

## Phase 7.4 — owner-safe staging UI/logs delivered locally

9 November 2026. Preserved the starting 7.2/7.3 working tree on `main` at
`68f59e9`, with 266 inventoried files saved before edits. Implemented only 7.4;
no stash/reset/commit, live DB/schema/config/service, SSH, NFS or Pi changes.

Existing portal detail/overview gain owner-scoped current-attempt staging state
for pinned NFS jobs. Input preparation shows acknowledged ready ranks, cache
reuse/staged bytes and verification/copy durations; Input staging is selectable
in the existing log control. Numeric measurements are validated after signed
receipt verification, matched to unique required digest bytes and recorded once
with first READY in the existing event transaction. Concurrent inventory work is
excluded from request timing. No schema, scheduler state, transport or checkpoint
format change. Missing measurements stay null; retries do not inherit old totals.
DDP totals represent actual rank copies, not unique cluster-wide content.

Hybrid service messages omit input paths, raw errors and internal lease IDs;
owner checks precede staging reads, and other users' history is not joined.
Legacy responses/defaults remain unchanged. No metrics can grant execution.

Pre-edit baseline: **42 pass**. Eight-file regression: **145 pass**. Subsequent
portal/cache/workspace selection: **102 pass**; portal/dispatch/worker-cache:
**73 pass**; final expanded new suite: **17 pass**. Counts across overlapping
runs are not added; zero final failures/skips. Actual portal JavaScript passes
a dependency-free mocked DOM/API harness. Multipart upload through held receipt,
explicit trusted staging, mocked launcher/completion and owner UI is covered;
warm/cancel/three-rank partial readiness, ownership, escaping, malformed telemetry,
lost acknowledgements, deduplication and retry isolation are covered. Initial
implementation/test integration failures were corrected and documented.

See [7.4 report](29-staging-visibility.md) and
[evidence](29-staging-visibility-evidence.json) for exact files/commands/results,
security/compatibility limits, manual checks and source rollback scoped against
the preserved 7.3 tree. **7.4 local acceptance PASS; STOP.** Phase 7 local mock
lifecycle/UI acceptance is covered. Production startup remains gated and flags
stay legacy/OFF/resource. Reviewed production isolation/containment, NFS checkpoint
publisher, dedicated historical 5.4 acceptance and validation on pi01/pi02/pi03
remain pending. No browser layout, live hardware or benchmark claim.

## Phase 8.1 — authoritative checkpoint integration audit delivered

9 November 2026. Started on clean `main` at
`38d31c22bceb990166ef873f2be60b6ded1417f5`; saved all 272 inventoried files
before edits. Implemented **audit documentation only** for 8.1. No runtime,
configuration, schema, job, DB, checkpoint, service, SSH/Pi or NFS changes;
no stash/reset/commit.

[The checkpoint audit](30-checkpoint-audit.md) and
[evidence](30-checkpoint-audit-evidence.json) trace dummy JSON, built-in .pt,
custom single-worker v1 and CPU Gloo DDP v2 writers/contents, local temporary
publication, latest selection, retention, controller sync, restore/completion
acknowledgements, leases and actual authentication hooks. Existing central latest
archives are mutable, replaced before separate DB/event updates; no implemented
immutable COMMITTED journal/head exists in the audited code. The single-worker
known-missing-checkpoint restart gap remains; DDP already rejects that case.
Existing checkpoint multipart restore is not HMAC-authenticated and is rejected
by installed hybrid mutating-route hooks. Hybrid checkpoint mode remains gated.
The audit identifies exact 8.2 seams without lifting those gates or rewriting
formats. Private checkpoints must stay outside the NFS input export; original
manifest identity and immutable source/data version must both remain bound.

Safe isolated baseline: **57 tests PASS, 4/4 files, zero failures/errors/skips**
(custom checkpoint 4, execution ownership 12, staging lifecycle 20, storage
selection 21). Existing real stochastic single-worker CPU resume matches model,
Adam, scheduler and Torch RNG exactly; this is not NFS recovery acceptance.
Networked DDP/Gloo suites, live hardware/failure tests, NFS operations and
benchmarks were not run. Manual commands, precise symbols, limitations and
documentation-only rollback are in the report/evidence.

**8.1 audit PASS; STOP before 8.2.** Phase 8 authoritative publication/recovery/
fault testing remains pending. Legacy/OFF/resource defaults, central SPOF,
reviewed production isolation and all-three-Pi validation requirements remain.

## Phase 8.2 — trusted immutable checkpoint publisher delivered locally

9 November 2026. Preserved the uncommitted 8.1 audit/progress on `main` at
`38d31c22bceb990166ef873f2be60b6ded1417f5`; all 274 starting inventoried files
saved before edits. Only new publication modules/tests/docs plus appended progress/
package README; existing runtime files, formats, config and 8.1 report/evidence
are unchanged. No stash/reset/commit, live DB/schema/service/SSH/Pi/NFS changes.

Explicit `CheckpointPublisher`/`LocalCheckpointPublisher` validates original
manifest, immutable input binding, owner ACL, current staged RUNNING attempt,
rank-zero producer, all peer reservations/leases/generations and complete single
v1/DDP v2 archive metadata/hashes. Bounded private attempt-specific copy/fsync ->
verified no-replace immutable snapshot -> atomic SQLite COMMITTED/head/job metadata
transaction. Concurrent writers compare expected head; idempotent committed replies
cannot rewind it. Interrupted/rejected candidates and old committed files are
retained, never silently promoted or overwritten. No automatic old-file adoption.

Explicit additive checkpoint migration: four tables, two indexes, ten triggers,
LOCAL FULL/FK, exact checksum, no user_version or earlier schema changes. Not
applied live. Existing HTTP checkpoint GET gains authentication only when the
reviewed worker service is explicitly installed: fresh per-worker HMAC headers
bind request and immutable snapshot SHA/size; controller streams bounded bytes.
No new download route or trainer NFS RW permission. Unsigned restore POST remains
blocked; this read protocol does not authenticate future restore uploads.

Baseline **16 pass**. Eight-file regression snapshot **148 pass**. Later
portal/legacy/publication boundary selection **60 pass**. Final expanded new suite
**25 pass**, zero failures/errors/skips; overlapping counts are not added. Covers
signed single/DDP existing-route transfer, independent concurrent SQLite writers,
crash/orphan/restart boundary, cancellation/expiry veto, exact-ID idempotency,
path/MAC/body integrity, partial/disconnected transfer, ACL/reservation loss,
disk/root/migration guards and legacy fallback. Initial import/exception-assertion
integration errors corrected without weakening checks/skips. New archive fixtures
use opaque tensor bytes and synthetic valid staging rows; old stochastic CPU SDK
test still proves exact resume. No new-mode resume or live hardware claim.

See [8.2 report](31-checkpoint-publication.md) and
[evidence](31-checkpoint-publication-evidence.json) for exact commands/files/schema,
manual verification, setup samples, limitations and source rollback preserving
8.1. **8.2 local PASS; STOP before 8.3.** Defaults remain legacy/OFF/resource;
production bootstrap/isolation, authenticated restore, committed-source recovery,
final metrics consistency, full 8.4 faults and pi01/pi02/pi03 validation remain
pending. Central server/SQLite remains a SPOF; no live deployment was performed.

## 2026-11-09 — Phase 8.3: committed-source recovery (local PASS)

Explicit `CheckpointDispatch` now freezes and verifies an owned COMMITTED source
against the original immutable job inputs, reuses resource placement/reservations,
stages missing inputs, restores a verified private checkpoint through authenticated
POST on the existing route, and invokes the unchanged SDK resume path. All DDP
ranks stage/restore before any launch; recovered jobs stay SCHEDULED until exact
SDK/process/lease restore proof. Ambiguous starts are fenced, not replayed.
Periodic/final publication uses 8.2; completion requires a verified current-attempt
COMMITTED final checkpoint. Copy/restore/commit events retain existing reporting
fields and numeric-only student progress. No format, shipped flag or default
startup changes; base StagingDispatch stays restart-only. Additive LOCAL restore
migration is explicit and was not applied live.

Baseline: **84 tests PASS, 3/3 files**. Final selections cover **157 distinct
unittest cases across eight files plus the original executable legacy regression
script**, all PASS, no test failures/errors/skips in those selections. Overlapping
runs are not added. New suite: **18 PASS**, including real CPU stochastic SDK
resume through committed publication/authenticated private restore, exact model/
Adam/scheduler/Torch/Python/NumPy RNG equality, replacement pi02 fixture, warm-zero
input staging, three distinct mocked DDP workers, final checkpoint gates,
restart/lease/auth/cancel/version guards. Real DDP/Gloo/Pi execution is not claimed.
Initial new-test fixture/import errors were corrected. The existing legacy test
filename was restored byte-for-byte after a naming collision; new tests have a
separate filename. Prior 8.1/8.2 source/docs are preserved by the scoped delta.

See [report](32-checkpoint-recovery.md) and
[evidence](32-checkpoint-recovery-evidence.json) for files, exact commands/logs,
manual expectations, schema/security details and rollback preserving earlier work.
No SSH, live NFS/Pi/service/config/data changes, commit, stash or reset.
**8.3 local PASS; STOP before 8.4.** Production isolation/bootstrap, full 8.4
fault matrix and pi01/pi02/pi03 physical validation remain pending. Central
controller/SQLite/checkpoint storage remains a SPOF.

## 2026-11-09 — Phase 8.4: local checkpoint fault acceptance (PASS)

Added `test_hybrid_checkpoint_faults.py` (16 cases) and
`test_hybrid_checkpoint_ddp_resume.py` (one real three-rank CPU Gloo case).
No runtime/API/schema/config changes were necessary; all prior code/test bytes
are preserved. Faults cover worker connection loss, partial upload/restore,
corrupt/missing checkpoints, stale results, independent competing recovery
connections, lost restore/start ACKs, controller restart/proof gaps, actual
publisher SIGKILL after rename, reboot fencing and simulated NFS outage/disk full.
No incomplete/stale source is selected; current final commit still gates results.

Genuine three-rank test: two uninterrupted baselines, complete saved cut at step
2, SIGKILL one native rank/reap all old ranks, reopen SQLite, authenticated private
restore on all three disposable agents, unchanged SDK resume through step 6,
all-rank proof and committed final completion. Saved-state load and final RNG/
progress/application state are exact. Final floating model/optimizer tensors use
atol=1e-8, rtol=1e-7; observed final Adam second-moment difference 1.862645149230957e-9.
Existing exact single/two-rank comparisons passed unchanged. Agents/isolation and
result transfer remain mocked; native training and process SIGKILL are real.

Baseline 43 PASS; existing real Gloo 15 PASS; final fault/legacy/config/single SDK
37 unittest cases plus executable legacy script PASS; new native three-rank case
1 PASS. **96 distinct unittest cases across eight files plus the legacy script,
9/9 files PASS**, zero final unittest failures/errors/skips. Overlapping runs are
not added. Socket sandbox restrictions required reviewed local Gloo test
permission. Early clock-fixture and final floating comparison failures are
retained and explained; no production checks or existing assertions were removed.

See [8.4 report](33-checkpoint-fault-acceptance.md) and
[evidence](33-checkpoint-fault-acceptance-evidence.json) for exact files, commands,
matrix, actual measurements, manual verification and turn-only rollback.
No live deployment, Pi/SSH, NFS/service/config/data changes, migrations or commit.
**8.4 local PASS; STOP.** Physical pi01/pi02/pi03 faults, production containment,
real mount outages/power durability and deployment acceptance remain NOT TESTED.
The central controller/local SQLite/checkpoint disk remains a single point of
failure. Legacy/OFF defaults remain unchanged.

## 2026-11-09 — Phase 9.1: actual DDP implementation audit (PASS)

Starting checkout clean on main, HEAD de297dc7d99eb80e52498145fe05b033e5afdaad.
Audited the existing custom and hybrid DDP call graphs, launcher/guardian,
rank/world-size rules, primary trainer/sampler, checkpoint writer/reader and
publication/recovery. Documentation only; no runtime/API/schema/config changes.

Actual mechanism: one logical rank per reserved worker, prepared interpreter
launched by custom_runner (not torchrun), CPU Gloo, fixed world size on recovery.
Rank zero publishes one v2 ZIP archive containing metadata and all rank-N.pt
members; these are not separate rank-owned checkpoint files or PyTorch DCP.
Keep this format and the existing trusted publication/lease fences. Final
rank-zero results/model.pt is separate from the full recoverable checkpoint.

Every primary trainer rank reads the entire CSV, builds the full TensorDataset,
uses DistributedSampler/set_epoch and evaluates all rows. Full immutable source/
dataset objects are required on every rank. Read-only installed-sampler probe:
256 rows, three ranks x 86 indices = 258 including padding, 256 unique rows,
all rank memberships change between epochs. No file-shard contract exists.

Existing staging RPCs are sequential; bounded parallel staging belongs to 9.2.
The all-rank READY/fresh-lease barrier already exists and must be retained.
Do not share the caller-owned SQLite connection across worker threads or release
uncertain reservations on partial failure. Existing admission has no explicit
maximum distributed worker count, while group checkpoint validation caps at 256;
no limit, world-size support or batch setting was changed. Three-rank example
training requires an explicit divisible global batch such as 96 instead of 64.
Rank-zero gather memory and reference-workload Internet/cache assumptions are
recorded as hazards, not claims of new support.

Local verification: 15 existing tests PASS (7 distributed dispatch + 8 checkpoint),
2/2 files, zero failures/errors/skips; real localhost Gloo processes, reviewed
socket permission, no Pi or live NFS. Twenty-two source files parsed; function
anchors/hashes recorded. No new test implementation in this audit subphase.
See [9.1 report](34-ddp-audit.md) and [evidence](34-ddp-audit-evidence.json) for exact
interfaces, commands, findings, unknowns and audit-only rollback.
**9.1 local PASS; STOP before 9.2.** Phase 9.2–9.4 and physical pi01/pi02/pi03
validation remain pending. Default legacy/OFF/resource behavior is unchanged;
controller/local SQLite/checkpoint storage remains a SPOF.

## 2026-11-09 — Phase 9.2: bounded all-rank input staging (local PASS)

Preserved the uncommitted 9.1 audit/progress and all earlier work. Existing
resource/group selection, reservations, quota/fairness, rank/world size, Gloo
launcher and coordinated v2 checkpoint format remain in place. Every worker
stages its entire authorized immutable source/dataset file set; DistributedSampler
does not authorize sample/file subsets.

Explicit hybrid coordinators now support `stage_parallelism=3` (trusted optional
constructor keyword, valid integers 1..32). At most that many stage RPCs are
outstanding, with worker cache producer limits unchanged. Only signed RPCs use
pool threads; local SQLite/fresh leases/receipt validation stay on the owning
thread. Replies are indexed by rank. All READY receipts and current group/attempt,
reservation, ACL and lease fences are checked before any existing launcher call
and rechecked before each rank start. Single-worker staging remains synchronous.

Partial staging, restore or launch failure blocks renewal and aborts the whole
DDP group through existing signed cancellation/durable worker tombstones. In-flight
RPCs are drained, late READY discarded and cleanup retried. Slots/pins are released
only after all-rank quiescence or safe persisted lease fencing; offline/changed
reservations remain held. No new transport, schema, job state, shard protocol,
checkpoint format, world-size rule or silent direct-NFS fallback was introduced.

Before changes, 59 existing staging/recovery tests passed. Final verification:
**133 tests PASS, 9/9 files, zero failures/errors/skips**, including 16 new cases,
existing two-rank Gloo, checkpoint/group recovery, actual localhost three-rank
crash/committed-resume and legacy/OFF config checks. Event/barrier tests prove
concurrency, not throughput. Development fixture errors and their corrections
are recorded. Narrow agent-specific watcher mocks replace the old global Thread
mock, allowing real stage executor and migration threads.

See [9.2 report](35-ddp-staging.md) and [evidence](35-ddp-staging-evidence.json)
for exact files, commands/log hashes, API/security details, scoped diff and rollback.
No SSH, Pi, real NFS, service, exports/fstab/firewall mutation or deployment.
Physical pi01/pi02/pi03 validation remains required; no earlier two-Pi exception
is treated as satisfying Phase 9 acceptance. Default legacy/OFF/resource settings
are unchanged. The central controller/local SQLite/checkpoint disk remains a
single point of failure. **9.2 local PASS; STOP before 9.3.**

## 2026-11-09 — Phase 9.3: canonical complete DDP save publication (local PASS)

Preserved uncommitted 9.1/9.2 changes. Existing rank-zero coordinated v2 archive,
all-rank SDK writer/reader, CPU Gloo/world-size rules, validator, authenticated
worker service/collector, legacy checkpoint path and 9.2 staging are unchanged.
Existing validation already requires every rank member/hash/ACK plus the exact
input/group/environment and all current producer fences.

Closed a publication gap: a fresh transport publication ID could previously
commit the same SDK save again. The trusted local publisher now checks original
job/attempt + checkpoint_id against immutable committed metadata after validation
and again under final SQLite write serialization. Identical bytes return the
canonical committed receipt; redundant transport intents are REJECTED. Conflicting
bytes under the same save ID fail closed. Concurrent collectors produce one
COMMITTED manifest/head revision/event; an old-save replay never rewinds latest.
Current all-rank authority is checked before deduplication. Existing exact-ID
read-only committed ACKs remain compatible. No new schema, migration, route,
transport, tensor deserialization or DCP/version conversion was added.

Two opaque DDP fixture helpers now use distinct save IDs/ACKs for distinct cuts,
matching the unchanged real writer. New tests cover complete members/ACKs, wrong
group/fences, fresh-ID retry, conflicting and concurrent saves, stale attempts,
old replays, progress/completion monotonicity, orphan retention and signed HTTP.
The initial new-test exception expectation error and correction are recorded.

Baseline: 41 existing publication/fault tests PASS before edits. Final:
**107 tests PASS, 8/8 files, zero failures/errors/skips**, including 15 new tests,
native two-rank checkpoint/recovery and real localhost three-rank crash/committed
resume within existing numerical tolerances. No benchmark or physical Pi claim.
See [9.3 report](36-ddp-checkpoint-publication.md) and
[evidence](36-ddp-checkpoint-publication-evidence.json) for exact files, commands,
logs/hashes, compatibility/performance limits, scoped diff and rollback.

No SSH/Pi, live NFS/DB, service, export/fstab/firewall/systemd, deployment, data
deletion, package installation or commit. Legacy/OFF/resource remains the default.
All-three-Pi validation and 9.4 remain pending. Metadata-history scan latency and
retained redundant candidate disk usage are documented tradeoffs. Controller/
LOCAL SQLite/authoritative checkpoint disk remains a single point of failure.
**9.3 local PASS; STOP before 9.4.**

## 2026-11-09 — Phase 9.4: local DDP matrix PASS; physical acceptance pending

Preserved prior uncommitted 9.1–9.3 work. Added six local acceptance cases with
native CPU Gloo: two-rank committed full-group crash/resume; three-rank startup
death/cold retry; rank death during checkpoint ACK and full-group restore from
the prior valid cut; real 3-to-2 archive resume rejection; controller immutable
world-size drift rejection; and independent whole-group recovery competition.
Owned native children are reaped before quiescence proof; a separate student's
HELD job, all input/result file hashes and DB/event state remain unchanged.
Existing suites supply missing-data, partial archive/ACK, stale-rank and legacy
coverage. Outer launcher/isolation/mounts and results transport remain fixtures.

Found/fixed an existing hybrid DDP completion-history bug: ranks could remain
RUNNING after job completion. `job_store.mark_completed` now updates only the
current fenced/quiescent group's rank and group history to COMPLETED in the same
transaction as the job/attempt, lease revocation and slot release. Old retries
retain STOPPED/FAILED history. No format/writer, world-size, API/schema/config,
transport, security/lease rule, policy or legacy-path change was introduced.

Baseline 31 existing tests PASS. Final latest-per-file results: **173 distinct
tests PASS, 14 files, zero remaining failures/errors/skips**, including six new
cases. The initial combined native run's failing new file was corrected/rerun;
its existing native files passed and are not recounted. Development history
records the real history defect plus fixture-cardinality/import mistakes. No
numerical tolerances were weakened. See [9.4 report](37-ddp-acceptance.md) and
[evidence](37-ddp-acceptance-evidence.json) for exact commands/results/diffs,
read-only probes, manual verification, rollout prerequisites and source rollback.

Actual read-only GET/SSH readiness: pi01 timed out at its configured address;
pi02/pi03 matched identity and were IDLE with active legacy agents. Their service
roots lack the hybrid modules; agent source differs, SDK/validator hashes match.
The old RW NFS share is active. New input automount units are loaded/active/waiting,
but the actual input mounts/readiness service are inactive; no automount activation
was attempted. The standard deployment bundle omits hybrid_storage, and reviewed
protected bootstrap/isolation/credentials/all-three-worker readiness are required.

No live job was submitted/killed, no source deployed, no service/mount/export/
fstab/firewall/SSH/systemd changed, no live DB/data mutation or Git commit.
**9.4 local matrix PASS; overall Phase 9 physical acceptance NOT PASSED.**
The earlier two-Pi Phase 2 exception does not satisfy this three-Pi requirement.
Do not claim Phase 9 complete. Controller/LOCAL SQLite/authoritative checkpoint
disk remains a single point of failure. **STOP at 9.4.**

## 2026-11-09 — Phase 10.1: existing resource scheduler metrics

Started from clean `main` at `7edef1ccd8e3264d9716a3482cbd214188d009b6`;
snapshotted 304 inventoried files. Added a bounded, immutable, caller-owned metrics
observer around existing single-worker and DDP group selectors. Default OFF
performs no measurement clock reads or metrics I/O. Enabled records expose duration,
filter/query/eligible/group counts and selected IDs; observer failures do not
replace choices. Public selector/dispatch APIs, sort keys, probe order and stdout
are preserved. No new config, DB/schema, route, lease, storage or policy change.

Traced CPU/RAM/inventory single ties versus CPU/RAM/node-ID DDP ties and first
encountered compatible-group total-CPU ties. Documented persisted owner turns,
aging, atomic quotas/reservations, one quota slot per DDP job, full-worker group
reservations and existing lease gates. Checked-in active quota is three (the
missing-policy fallback is one), pending quota twenty, aging 300 seconds.

Baseline 33 tests PASS. Final **66 distinct tests PASS, six files, zero remaining
failures/errors/skips**: ten new metrics tests (including 25 pre-edit golden
scenarios in three observation modes), resource, queue, config, mocked staging and
existing native two-rank CPU Gloo. A corrected golden-generation dependency and
initial missing-fixture test error are recorded. No benchmark or hardware claim.
See [10.1 report](38-resource-scheduler-metrics.md) and
[evidence](38-resource-scheduler-metrics-evidence.json) for commands, exact files,
source fingerprints, results, scoped diff and rollback.

No commit, service/deploy/export/mount/SSH/firewall/systemd change, Pi contact or
live DB/cache/job/checkpoint mutation. Legacy/OFF/resource fallback is preserved.
Locality computation, activation and simulations belong to 10.2–10.4. Physical
pi01/pi02/pi03 acceptance remains required and Phase 9's hardware gate remains
pending. Controller/LOCAL SQLite/authoritative storage is still a single point
of failure. **10.1 local PASS; STOP before 10.2.**

## 2026-11-09 — Phase 10.2: verified locality tie breaking

Preserved all nine uncommitted 10.1 files at `main` HEAD
`7edef1ccd8e3264d9716a3482cbd214188d009b6`; snapshotted 309 inventoried files.
Added explicit job/owner/immutable-binding-bound `JobLocality` and optional
`locality=` to the existing selectors. Full-input cache coverage for eligible
workers uses one locked SQLite read snapshot and existing authenticated inventory
freshness/sequence/ACL checks. Single placements keep CPU/RAM primary ordering;
DDP keeps the original winning ordered resource profile, compatible whole groups
and world size, then compares summed per-rank verified missing unique bytes.
Equal locality ties retain existing final ordering. Partial/stale/missing/failed
coverage restores the exact resource-only choice without inventing cache facts.

Immutable diagnostics expose age/status/session/sequence, known bytes and effective
policy/fallback reason. Current trainers require full inputs on every rank; no
sample/shard caching, bandwidth model or private-workspace disk prediction was
invented. No new config/schema/routes, runtime policy activation, queue fairness,
quotas, reservations, lease, authorization or staging-security changes. Existing
dispatchers still use resource policy; 10.4 will wire reviewed configuration/logs.

Baseline **74 PASS**. Final **148 distinct tests PASS, nine files, zero remaining
failures/errors/skips**, including thirteen new tests, the frozen resource-only
scenarios, actual signed warm/empty inventory + SQLite selection, coverage/auth,
queue/resource/config and existing native 2-rank CPU Gloo/mocked 3-rank staging.
An initial new-fixture header typo caused three setup errors; it was corrected
to the existing protocol header and all final checks passed. See the
[10.2 report](39-locality-tie-breaker.md) and
[evidence](39-locality-tie-breaker-evidence.json) for exact files, commands/results,
preservation fingerprints, scoped diff, manual verification and rollback.

No commit, SSH/Pi/live NFS/deployment/service/export/mount/firewall/systemd change,
live DB/job/checkpoint/cache mutation or benchmark. All-three-Pi acceptance is
still required; Phase 9 physical acceptance remains pending. The controller,
LOCAL SQLite and authoritative storage remain a single point of failure.
**10.2 local PASS; STOP before 10.3.** Exact-MB/starvation matrix and production
configuration/choice logs/simulations remain separate 10.3/10.4 work.

## 2026-11-09 — Phase 10.3: deterministic locality acceptance

Preserved all fifteen pre-existing 10.1/10.2 status paths on `main` at
`7edef1ccd8e3264d9716a3482cbd214188d009b6`; snapshotted 313 inventoried files.
Added one acceptance file with five tests and documentation only. No controller,
agent, hybrid-storage runtime, config, API, schema, job/FSM, queue, lease, auth,
checkpoint or transport changes were needed.

Actual SHA-published 500 MB + 2 MB objects, a duplicate logical path, common
72-byte source/requirements inputs and three separate protected local caches
produce exact verified misses pi01=0, pi02=2,000,000, pi03=502,000,000. Signed
FULL inventories and actual registry/selectors choose pi01 across all six inventory
orders; per-rank capacity/reservation/ownership exclusions remain hard. Two-rank
missing total is 2 MB; three-rank total is 504 MB. Stale/missing inventory is unknown
and restores resource ordering; a missing verified observation cannot count a hit.
Prepared metadata/resources/capability are fixtures, not live upload or Pi execution.

Controlled queue simulations use existing fair turns and atomic SQLite scheduling:
two cold FIFO jobs start in rounds 0/1 amid eight rounds of warm arrivals. Full
traces match resource-only and repeated locality runs. Infeasible heads do not
block feasible owners; uncertain/cancelling jobs keep quota/reservations; a DDP
job uses one owner slot and all its workers with no partial peer allocation.
This is bounded progress evidence, not a promise for permanently infeasible jobs.

Baseline **87 PASS**. Final **115 distinct tests PASS, nine files, zero remaining
failures/errors/skips**, including existing frozen OFF/resource, coverage, queue,
capacity/config, native two-rank CPU Gloo and mocked two/three-rank staging tests.
An initial new-cache fixture's intermediate-directory mode was correctly rejected;
fixed the fixture to create protected components 0700, retaining the runtime guard.
See [10.3 report](40-locality-acceptance.md) and
[evidence](40-locality-acceptance-evidence.json) for files, commands/results,
scope limits, source fingerprints, scoped diff and rollback.

No commit, deployment, Pi/SSH/live NFS contact, service/export/mount/firewall/systemd
change, live DB/job/checkpoint/cache write, runtime download or benchmark. Legacy/
resource defaults and NFS/cache/checkpoint trust boundaries remain unchanged.
All-three-Pi validation and Phase 9 physical acceptance are still pending; the
controller/LOCAL SQLite/authoritative storage is still a single point of failure.
**10.3 local PASS; STOP before 10.4.** Production policy wiring/logs and broader
workload simulations are not implemented by this acceptance subphase.

## 2026-11-09 — Phase 10.4: configured policy, choice logs and rapid fallback

Preserved eighteen prior 10.1–10.3 status paths at `main` HEAD
`7edef1ccd8e3264d9716a3482cbd214188d009b6`; snapshotted 316 inventoried files.
Wired validated resource/locality settings into the existing explicit trusted
staging/checkpoint coordinators using authorized actual workspace plans and an
optional same-registry CoverageAPI. Bandwidth remains unavailable. Resource with
no explicit logger keeps original selector calls and performs no inventory/trace
work; old legacy-bound jobs retain their existing branch. Ordinary unconfigured
portal/dispatcher startup stays protected; no production bootstrap was fabricated.

Locality now emits bounded operator SCHEDULER_CHOICE records with filtered IDs,
candidate resources, verified/unknown bytes, freshness, policy/reason and explicit
truncation/diagnostic availability. No filenames/NFS paths/IPs/credentials or new
student route. INFO routing belongs to the trusted bootstrap; history remains
available in memory. `set_scheduler_policy('resource')` restores future resource
placements without DB/job/cache/pin/lease mutations. Read-only component config
CLI/examples validate exact flags without starting a service or importing SQLite.

Seeded 72-case matrix repeats 432 selections across single/two/three ranks and
fresh/stale/absent/error/CPU/RAM/reservation/compatibility variants. Actual local
signed single/three-rank staging and committed checkpoint resume pass, with mount/
isolation/training fixtures. A modeled two-job counterexample raises total bytes
2→100 under greedy locality; resource rollback restores 2. No universal bandwidth
benefit is claimed; resource remains active/default. Existing 10.3 fairness and
actual 0/2/502 MB acceptance and hard constraints passed again.

Baseline **81 PASS**. Latest results **191 distinct tests PASS, 13 files, zero
remaining failures/errors/skips**, including seventeen new policy checks and native
CPU DDP. Initial new-fixture path/alias mistakes were corrected. One existing
three-rank recovery case rejected cold staging with HTTP409 during concurrent
heavy runs; it passed alone on saved pre-edit/current source, then full recovery
passed serially. Cause was not established, no guard/timeout was weakened, and
the initial error remains recorded as a loaded-run reliability limitation.
See [10.4 report](41-scheduler-policy.md) and
[evidence](41-scheduler-policy-evidence.json) for exact files, commands/latest-per-
file counts, simulation results, failure history, logging/config examples and
scoped source rollback.

No schema/migration/state/format/auth/transport, active storage.json/nodes/quotas/
leases/NFS config, live DB/data, service/deploy/Pi/SSH/firewall/systemd change,
runtime download, benchmark or Git commit. All-three-Pi physical validation and
Phase 9 hardware acceptance remain pending; central controller/LOCAL SQLite/
authoritative storage is still a single point of failure. **10.4 and Phase 10
local implementation/validation PASS; no live enable or production/physical
acceptance claim. STOP at 10.4.**

## 2026-11-09 — Phase 11.1: optional actual copy observations

Preserved existing Phase 10 modifications at `main` HEAD
`7edef1ccd8e3264d9716a3482cbd214188d009b6`; snapshotted 274 source/config/document
files before edits. Added worker-local `TransferMetrics`, explicitly passed to
the existing cache fetcher, disabled by omission. The existing trusted helper
reports actual streamed bytes; samples require independent parent verification
and positive payload/time. Old or malformed optional helper telemetry only loses
a sample. Hash/mount/authorization/lease/publication gates remain unchanged.

Bounded rolling median effective MiB/s and MAD include sample counts, age and
stale/no-sample unknown values. Time includes helper/setup/verification/fsync,
excludes admission/failed attempts/retry delay/workspace/training; bytes are
application payload, not wire traffic. Warm/empty/failed copies are not successful
rate samples. No active probes, speed tests, persistence, HTTP/registry transport,
fixed-overhead prediction, bandwidth selection or eviction are introduced.
Cross-worker contention remains explicitly unknown. Resource/locality policies,
legacy/OFF defaults, config, schemas and job/checkpoint protocols remain intact.

Baseline **66 PASS**. Latest **247 distinct tests PASS across 12 files, zero
remaining failures/errors/skips**, including seventeen new statistics/copy tests.
An initial portal regression assumed serial DDP stage order; its same failure
reproduced with saved pre-edit runtime/test. Corrected only the test to observe
each committed READY acknowledgment (1/2/3), preserving concurrent runtime and
all launch guards. Full portal and final new suite pass. Initial failure and
pre-edit reproduction are retained; prior 10.4 loaded-run HTTP409 remains an
unresolved historical limitation. See [11.1 report](42-transfer-observations.md)
and [evidence](42-transfer-observations-evidence.json) for exact files, commands,
results, scope, source fingerprints and scoped rollback.

No live Pi/SSH/NFS, service/export/mount/firewall/systemd, live DB/job/cache/data,
checkpoint or deployment change, Internet transfer, benchmark or Git commit.
All-three-Pi/Phase 9 physical acceptance and trusted deployment bootstrap remain
pending. Central controller/LOCAL SQLite/authoritative storage is still a single
point of failure. **11.1 local PASS; Phase 11 incomplete. STOP before 11.2.**

## 2026-11-09 — Phase 11.2: optional measured input-copy cost

Preserved prior uncommitted work at `main` HEAD
`7edef1ccd8e3264d9716a3482cbd214188d009b6`; snapshotted 278 source/config/document
files before edits. Enabled the existing bandwidth enum only for explicitly
constructed trusted coordinators; ordinary unconfigured startup remains gated.
Resource stays the legacy/OFF default, and resource/locality require no rate
collector. No active config or service was changed.

Verified copies now optionally decompose the helper interval and measured
per-object residual without double-counting inclusive 11.1 rates. Shared explicit
worker collectors publish bounded numerical summaries via optional authenticated
inventory report v2 on the existing request-bound HMAC channel. Default reports
remain v1; old workers imply unknown rates. Controller ingestion follows existing
enrollment/session/generation/sequence/transaction checks and keeps rates volatile.
Duplicate replies, stale/offline sessions, restart and missing/malformed telemetry
cannot refresh old predictive certainty. No DB migration or new download API.

Bandwidth preferences use exact authorized missing unique bytes/object counts,
at least three recent decomposed copies, measured median rate/residual and live
disk facts, only among equivalent CPU/RAM profiles and complete compatible groups.
Unknown/insufficient inputs restore resource ordering. Choice logs show age/count,
observed scenario range, size extrapolation, disk/quota limitations and actual
bounded parallelism with an explicit contention assumption. Residual cost is not
proven size-independent; cross-job/server contention remains unknown. Estimates
exclude training, workspace/restore/queue/failure cost and are not total completion
predictions. Existing fairness, reservations, quotas, authorization, leases and
all-rank staging/launch barriers remain authoritative. Immediate resource rollback
preserves running state and performs no DB mutation.

Baseline **102 PASS**. Latest **290 distinct tests PASS across 15 files, zero
failures/errors/skips**, including 22 new measurement/auth/policy/dispatch checks.
The broad 15-file run passed; final targeted validation after range/validation/log
bounds passed 73 tests in three files. Synthetic rate fixtures test arithmetic;
local real copies, signed Flask inventory and single/three-rank staging use mocked
NFS/trainer/isolation. Existing genuine CPU checkpoint resume also passed. No
hardware throughput was invented. Only old bandwidth-unavailable test assertions
were updated for the supported opt-in component. See [11.2 report](43-bandwidth-policy.md)
and [evidence](43-bandwidth-policy-evidence.json) for exact files, commands, model
limitations, protocol compatibility, fingerprints and scoped rollback.

No live Pi/SSH/NFS, deployment, exports/mounts/firewall/systemd/service changes,
live DB/job/checkpoint/cache data mutation, Internet transfer, benchmark or commit.
All-three-Pi/Phase 9 physical acceptance and reviewed protected deployment remain
pending. Prior loaded-run HTTP409 remains a historical unresolved limitation.
Central controller/LOCAL SQLite/authoritative storage is still a single point of
failure. Optional efficiency is not a production-stability prerequisite.
**11.2 local PASS; Phase 11 incomplete. STOP before 11.3.**

## 2026-11-09 — Phase 11.3: explicit dry-run-first idle cache eviction

Preserved earlier uncommitted changes at `main` HEAD
`7edef1ccd8e3264d9716a3482cbd214188d009b6`; snapshotted 283 source/config/document
files. Added inactive trusted `CacheEvictor`/`cache_maintenance` using the existing
fetcher's max-cache quota and free-space reserve. Planning uses read-only SQLite,
bounded deterministic LRU, pin counts, inode fingerprints and local mount checks.
Apply remains dry-run unless explicitly requested on that instance's issued plan;
stale/foreign/insufficient plans cannot delete. No automatic admission/startup GC.

Shared OS activity guards now cover verified readers, complete fetch/materialization
and the actual signed agent fetch-to-pin gap. Any persistent cache pin blocks ALL
eviction, including READY/running/unproven terminal attempts. Existing terminal
quiescence proof alone releases pins. Existing inherited producer locks retain
capacity for expired/orphaned NFS helpers. No pin or lease policy was weakened.

Only unchanged registered local replicas are eligible. Quarantine/parts/unknown
files, private workspaces, output/checkpoints and authoritative inputs are retained.
Dedicated trusted roots and declared protected roots remain required; co-located
controller authoritative roots must be protected explicitly. Missing metadata is
committed before checked unlink/fsync; crash reconciliation verifies actual local
bytes without replaying deletion or dropping pins. Existing signed FULL or
CACHE_REMOVED reports reconcile the advisory controller registry, with explicit
notification/lag status. No schema or wire change; worker DB remains v1 and the
controller DB remains LOCAL. The additive activity lock requires upgraded,
cooperating trusted clients before any maintenance rollout.

Baseline **143 PASS**; early activity-guard regression **105 PASS**. Latest
**396 distinct tests PASS across 17 files, zero failures/errors/skips**, including
23 new safety/integration checks. The broad 17-file run passed; final cleanup
refinement/new restage case passed in a 56-test targeted rerun. Tests use real
local files, real child-process crashes around unlink, signed local Flask inventory
updates, actual staging-gap/READY/running pin protection, simulated disk/unlink/
notification errors and authorized fake-NFS restage. Existing single/DDP recovery,
portal, queue, coverage, resource/locality/bandwidth and CPU saved-state resume
regressions passed. No hardware throughput or physical recovery was fabricated.
See [11.3 report](44-cache-eviction.md) and
[evidence](44-cache-eviction-evidence.json) for exact files, commands, ownership,
source fingerprints, manual previews, residual risks and scoped rollback.

No live cache/data/DB/job/checkpoint cleanup, Pi/SSH/NFS/deployment, exports/mount/
firewall/systemd/service change, Internet transfer, benchmark or commit. Legacy/OFF/
resource defaults and all active config remain unchanged. All-three-Pi/Phase 9
physical acceptance and reviewed isolation/bootstrap remain pending; the historical
loaded-run HTTP409 remains unresolved. Central controller/LOCAL SQLite/authoritative
storage is still a single point of failure. **11.3 local PASS; Phase 11 incomplete.
STOP before 11.4.**

## 2026-11-09 — Phase 11.4: combined efficiency and fault acceptance

Preserved prior uncommitted work at `main` HEAD
`7edef1ccd8e3264d9716a3482cbd214188d009b6`; snapshotted 254 selected source/config/
document files. Added ten combined acceptance tests and appended documentation
only. No runtime, API, DB/schema, wire/auth, security, deployment or config change.
All 252 other snapshotted originals remain byte-identical; the two documentation
prefixes are preserved. No commit, stash or reset.

Actual signed inventories/immutable binding/coverage/selectors verify controlled
slower/faster rates, rolling-median response to a sustained drop, transparent
missing-copy arithmetic and old/no-sample fallback despite fresh inventories.
Actual queue/SQLite tests with modeled rates/launch preserve resource service
traces, cold FIFO under warm arrivals, quotas/cancellation and full exclusive DDP
groups. Actual signed bounded three-rank staging protects each fetch-to-pin gap,
all-ready launch and running pins; resource rollback preserves the DB/group.
Real child-process eviction crash plus local restart converges through signed
inventory and one authorized refetch followed by a zero-copy warm repeat. Pinned
quota refusal and reconciliation retain inputs and authoritative bytes.

Pre-edit baseline **45 PASS**. Initial new fixtures had one failure/three errors
(intermediate directory mode and omitted prospective pressure); corrected tests,
with no runtime fix, then **10 PASS** standalone. Final selected regression:
**428 distinct tests PASS across 20 files, zero failures/errors/skips**, including
transfer, eviction, full-disk/error, cache/registry, resource/locality/bandwidth,
exact-MB locality, queue, stage/cancel/DDP/portal/recovery and genuine CPU saved-state
resume checks. No hardware rates, power-loss or training benchmark was fabricated.
Scoped reverse patch passed workspace check and actual rollback on a disposable
copy; no rollback was applied to this workspace. See [11.4 report](45-efficiency-acceptance.md)
and [evidence](45-efficiency-acceptance-evidence.json) for exact files/commands,
log hashes, initial fixture failures, preservation, rollback and residual risks.

Legacy/OFF/resource defaults, immutable inputs, private workspace/trusted checkpoint
boundaries and LOCAL SQLite remain unchanged. Optional efficiency is not required
for production stability. No live Pi/SSH/NFS, cache cleanup, job/DB/checkpoint/data,
exports/mount/firewall/systemd/service change, deployment or Internet transfer.
All-three-Pi/Phase 9 physical acceptance and reviewed protected bootstrap remain
pending; the Phase 2 two-Pi exception does not waive these. Historical loaded-run
cold-stage HTTP409 remains unresolved. Predictions retain size/contention/age
uncertainty and are not total-completion estimates; maintenance still requires
idle cooperating clients, protected dedicated roots and durable pins. Central
controller/LOCAL SQLite/authoritative storage remains a single point of failure.
**11.4 and Phase 11 local implementation/selected acceptance PASS; production and
all-three-Pi acceptance NOT PASSED / not run. STOP.**

## Phase 11 closeout — implementation complete; live gates refreshed

In response to the request to fully complete Phase 11, consolidated all four
completed subphases in [46-phase11-closeout.md](46-phase11-closeout.md) with
[fresh readiness evidence](46-phase11-closeout-evidence.json). Saved 11.4 test
logs still match their SHA-256 evidence: **428 tests PASS / 20 files**, zero
failures/errors/skips; runtime/config bytes remain unchanged since that passing
snapshot. No redundant test run or runtime change was made for this closeout.

Fresh bounded read-only HTTP/strict-key SSH to all three configured identities:
pi01 still times out; pi02/pi03 match identities, report IDLE and run the legacy
agent from `/home/pi/distributed-dl-system`. Queried hybrid staging, checkpoint,
cache and Phase 11 modules are absent in those active service roots. Both show
the legacy RW NFS4.2 mount; no input path was touched to activate automounts.
Initial sandbox EPERM was resolved by the reviewed read-only network retry; it
is not counted as a device outage. No remote files, jobs, services or mounts changed.

Only this progress append and the new closeout report/evidence change. No API,
DB/schema, config, security, runtime, deployment or commit. Legacy/OFF/resource
remains active; controller SQLite/authoritative storage remains a single point
of failure. The report states the ordered live prerequisites and rollback; no
ready-to-apply deployment bundle/isolation implementation is implied. Requested
pi01 availability/verified address before all-three-Pi validation. All three
remain mandatory, with reviewed concrete artifacts and explicit approval before
deployment/system changes. **Phase 11.1–11.4 local implementation/acceptance
COMPLETE; physical/production acceptance INCOMPLETE. STOP before live mutation.**

## Two-worker continuation — user exception and offline deployment preparation

The user replied to the pi01 availability question: **"fow now continue withour
pi1 / with pi2 and pi3"**. Current continuation therefore targets **pi02/pi03**,
with pi01 deferred; the previous all-three prerequisite does not block this
explicitly authorized two-node scope. No pi01 probe was made in this subphase.
Three-rank physical acceptance is not claimed. Standing review/explicit-approval
requirements for live deployment and system/service/network changes still apply.

Added inactive `hybrid_storage/deployment_review.py`: offline source catalogs and
deterministic private local review archives with exact member/archive hashes,
explicit selected endpoints and `apply_ready=false`. Includes hybrid modules
missing from the unchanged legacy installer bundle. Excludes active config, DBs,
credentials, jobs/caches/checkpoints/venvs; no SSH/extract/install/apply action.
Actual local artifacts have **77 worker files /112 controller files**. Reviewed
both archive hashes and member sets. No archive was uploaded or extracted remotely.

Fresh read-only pi02/pi03 GET/strict-key SSH: both matching ONLINE/IDLE, legacy
active roots still lack hybrid modules, bwrap/unshare/systemd-run/setpriv found,
nft/iptables absent on probe PATH, differing numeric pi UID/GIDs. Namespace/cgroup
facts and free disk recorded; no containment trial, mount activation, sudo,
package/service/firewall/config/job/data change. Tool presence is not containment
proof. Protected OS/network isolation and exact trusted bootstrap remain the next
required preparation artifacts before a concrete live deployment approval.

Baseline **8 PASS**. Selected final **76 PASS across five files**, zero failures/
errors/skips, including six new offline safety/integration tests. Default storage
config stays legacy/OFF/resource; no API/DB/schema/runtime/security or existing
deployment code change, no commit/stash/reset. See [47 preparation report](47-two-worker-deployment-preparation.md)
and [evidence](47-two-worker-deployment-preparation-evidence.json) for exact files,
commands, bundle/probe hashes, next plan and scoped rollback. **PASS: one offline
preparation subphase. STOP before bootstrap/isolation implementation or live apply.**

## 2026-11-10 — Phase 11 full tests and approved pi02/pi03 acceptance

The user explicitly approved completing full Phase 11 and all necessary tests,
retaining the current **pi02/pi03-only exception; pi01 deferred and not contacted**.
Completed the approved scope. Added an explicit trusted
probe/bootstrap, plan-only-default live driver and four local tests. Production
agents, student launcher, storage flags, existing APIs/DBs/schema/leases/checkpoint
formats remain unchanged; the test is not a general production DDP sandbox.

Snapshotted 277 selected source/config/document files before editing. Preserved
all earlier uncommitted work at `main` HEAD
`7edef1ccd8e3264d9716a3482cbd214188d009b6`. Four original documentation files were
appended; other originals unchanged. No commit/stash/reset. Actual publisher UID125
registered three public immutable CAS fixtures (7 MiB total, 0444), after fixing
a pre-publication private-checkout import failure; both attempts are recorded.
Source-only reviewed archive went into new root-private `/var/tmp` trees on the
two Pis. Started only their installed RO NFS readiness; no config/system unit/
export/firewall/SSH edits, package install, production restart or Internet transfer.

**56/56 existing regression files passed** in 788.7 s, including genuine CPU
two-/three-rank DDP and checkpoint crash/resume. Ten schema skips due to absent
venv jsonschema were resolved by rerunning that entire file with installed system
Python/jsonschema: **15/15 PASS**. Four new probe tests passed; latest aggregate
**946 unittest cases plus one standalone checkpoint script PASS across 57 files,
zero remaining failures/errors/skips**, no duplicated reruns. Local PyTorch2.14.1,
Python3.12.3/Flask3.1.3; actual worker probe Python3.11.2.

**Both physical Pis PASS:** NFS4.1 exact RO source, 7,340,032 cold bytes each,
warm repeat0, observed whole-ensure effective rates pi02 1.64518 MiB/s and pi03
1.56435 MiB/s (not wire speed/benchmark claims). Corruption refetch, one-copy
concurrent miss, actual quota refusal, private workspace/pin/reader protection,
distinct UID65534 readonly/private-path/no-network probe passed. Real disposable
eviction-process crash reconciled to missing2,097,152 bytes; actual authenticated
LOCAL controller registry agreed, refetched exactly that amount and warm-staged0.
Offline inventory rejected, signed reconnect restored eligibility. Final RO write
checks returned EROFS30; all three authoritative hashes/modes/UIDs unchanged.
Production agent PIDs stayed pi02=946 and pi03=902, both final IDLE. All owned probe
sessions exited; only new private fixture trees/public immutable objects retained.

Real copy-cost examples also expose uncertainty: prior-sample estimates of
1.08147/1.15789 s exceeded later measured copy intervals of 0.22112/0.22223 s.
Cache/concurrency conditions changed and causes were not isolated. No efficiency
or confidence-bound claim is made; resource remains active, bandwidth optional.

See [48 completion report](48-phase11-live-acceptance.md) and
[evidence](48-phase11-live-acceptance-evidence.json) for exact files, commands,
all test counts, program/bundle/log hashes, publisher setup failure, scope and
scoped source/live rollback. Legacy/OFF/resource defaults remain active. Physical
link/power loss and production hostile-code/DDP enablement are not claimed; pi01
three-rank hardware tests remain deferred. Historical loaded-run HTTP409 remains
disclosed, not root-caused. The central controller/LOCAL SQLite/authoritative
storage remains a single point of failure. **Phase 11 COMPLETE / PASS under the
approved two-worker component-validation scope. STOP before the next phase.**
## 2026-11-10 — Phase 12.1 structured storage events

Implemented **12.1 only**. New `hybrid_storage/events.py` defines schema-one
allowlisted JSON events on INFO logger `distributed_ml.storage.events`, stable
type/observation IDs, UTC epoch nanoseconds, integer bytes and monotonic durations.
Existing trusted staging emits exact manifest/dataset version, unique verified
hit/accepted transfer bytes, durations and failed-stage counters. Bounded copy
retry decisions emit numeric errno/closed error classes without messages/paths.
Existing selectors emit policy/reason proposals without new resource-mode
inventory/tracing queries. Immutable checkpoint events follow actual commit;
per-rank recovery events require existing full SDK restore acknowledgment.
Reserved-dispatch-to-restore latency is process-local, bounded to 128 starts and
null after restart/eviction. No new execution authority or checkpoint format.

All handlers/config/DB schemas/public APIs and signed payloads remain unchanged;
no new persistent event table, UI, worker deployment or Pi contact. Logging is
default-suppressed, with no event clocks/serialization when disabled; errors in
logging cannot veto staging/commit. Protected operator metadata is not exposed
to student routes. Default active storage stays legacy/OFF/resource. Existing
Phase 10/11 work is preserved; no commit/stash/reset/service/network mutation.

Baseline **85 PASS in four files**. Final distinct **188 tests PASS across eight
files**, zero remaining failures/errors/skips; reruns not doubled. Includes real
signed local staging/cold/warm/failure, three-rank attribution, all-rank SDK event
barrier, checkpoint idempotency, restart-null latency, redaction and disabled/
broken logging; existing portal/lease/checkpoint regressions pass. Initial
test-harness import/assertion errors and rejected nonexistent test name are
recorded and corrected, not labeled pre-existing failures. No physical hardware
acceptance or event-overhead benchmark claimed. Broader UI permissions/retention/
stale-event tests remain Phase 12.2–12.4.

See [49 contract and report](49-structured-events.md) and
[machine-readable evidence](49-structured-events-evidence.json) for exact source
files, commands/results, units/deduplication/delivery limits and scoped source
rollback. **12.1 PASS. STOP before 12.2; Phase 12 overall remains incomplete.**

## 2026-11-10 — Phase 12.2 admin storage panel

Implemented **12.2 only**. Inspection found no existing web admin role (the other
cluster dashboard is console-only), so reused the student portal and its login
sessions with a private, LOCAL operator grant/revoke allowlist. No implicit admin
username, public grants or relaxed job ownership. New guarded read-only
`GET /api/admin/storage` reports configured pi01/pi02/pi03 aggregate cache/disk,
current dataset coverage, inventory age, NFS mount identity and bounded successful
staging history. Students/anonymous receive 403/401 before data access; roles are
rechecked each request. UI clears old data and fences late responses on logout.

Reuse existing registry and INPUTS_STAGED metadata, no DB schema/migration/poll
writes or Pi calls. Optional signed inventory v3 reports kernel-only RO mount
identity under the existing request-bound per-worker HMAC; v1/v2 defaults remain
unchanged. Optional controller diagnostic snapshot is private/local/atomic and
bounded to 256 workers/256KiB, session/sequence/receive-time checked. No automatic
runtime opt-in, production admin grant, deployment, service/mount/config change.
Unreported cache quota, stale/absent coverage and NFS observations are Unknown;
mount identity is not I/O reachability or scheduler eligibility.

Baseline 72/72 tests in three files PASS. Final **155/155 distinct tests across
seven files PASS**, no failures/errors/skips; existing student UI harness PASS
separately. Initial Node-not-on-PATH error corrected using the existing bundled
runtime, no installation. Current panel HTML/CSS was visually checked in the
in-app browser with explicitly synthetic three-worker data and expanded transfer
history; saved screenshot, closed tab/stopped temporary localhost fixture server.
No physical worker result is claimed. Prior uncommitted work preserved, legacy/
OFF/resource unchanged; source rollback checked against the 308-file snapshot.

See [50 implementation report](50-admin-storage-observability.md) and
[evidence](50-admin-storage-observability-evidence.json) for exact files, API and
v3 compatibility, local operator commands, tests, limits and scoped rollback.
**12.2 PASS. STOP before 12.3; Phase 12 overall is not complete.**

## 2026-11-10 — Phase 12.3 student staging/recovery progress

Implemented **12.3 only**. Reused the existing job list/detail, staging metrics
and log selector. New owner-scoped `controller/recovery_view.py` projects the
current retry's committed source and exact input/copy/launch/SDK milestones from
LOCAL SQLite only. No file/NFS/worker reads, checkpoint deserialization, new DB
schema/migration or runtime recovery/scheduler changes. Existing hybrid overview/
detail add `recovery`; the existing logs route gains `view=recovery`. Legacy
payloads stay unchanged; initial hybrid attempts have no recovery panel.

A copied checkpoint/process start is not labeled resumed. The existing complete
DDP SDK barrier controls confirmed counts; full restart and checkpoint mode with
no saved source mark restoration Not required, not a fabricated SDK success.
Missing/stale/inconsistent/foreign source metadata remains unknown. Owner checks
precede private side tables, admin role does not bypass job ownership, and no
shared paths/hashes/credentials/other-owner identifiers enter the new progress
payload. Progress/staging share a readonly snapshot; stale outer job retry/status
cannot mix attempts. Browser account revision guards now cover overview/detail/
logs, clear old content/log selection and preserve the static empty-detail hint.

Baseline **52 PASS in three files**. Latest distinct **127 tests PASS across seven
files**, zero failures/errors/skips; repeated runs not doubled. Final focused
15 recovery +17 staging tests validate cold-retry refinement. New suite includes
actual signed local staging/recovery/SQLite/Flask ownership, three-rank SDK
barrier and actual recovery/staging/admin JS. Existing checkpoint tests include
genuine local CPU state resume; new UI fixtures mock NFS/trainer/isolation.
Visually checked current portal HTML/CSS with explicitly synthetic three-rank
progress in the in-app browser. Full-page capture failed; normal top/scrolled
viewport captures succeeded. Saved screenshots, closed tab and stopped approved
temporary localhost static fixture. No physical Pi or production deployment.

Preserved earlier uncommitted work; no commit/stash/reset. Legacy/OFF/resource
configuration unchanged; no production login/role/service/DB/data/network/mount
mutation. Snapshot of315 original source/doc files retained; source-only rollback
verified in a disposable copy. See [51 report](51-student-recovery-progress.md)
and [evidence](51-student-recovery-progress-evidence.json) for exact files, commands,
source hashes, safeguards, limits and rollback. **12.3 PASS. STOP before 12.4;
Phase12 overall remains incomplete.**


## 2026-11-10 — Phase 12.4 observability acceptance

Completed **12.4 only** with fourteen acceptance tests and a bounded offline
JSONL microbenchmark. New tests reproduced two pre-turn gaps, now fixed: admin
history rejects malformed/nonfinite/out-of-range timestamps instead of exposing
arbitrary text, and the existing browser formatter handles epoch zero, rejects
nonnumeric/invalid dates and shows the local timezone (including repeated DST).
No new routes, DB schema/migration, grants, storage flags or runtime control flow.

Baseline **50/50 PASS** in three files. First acceptance run had two reproduced
failures, both corrected; focused12/12 then PASS. Final **243/243 distinct tests
PASS in13 files**, zero failures/errors/skips, including final14 acceptance cases,
all Phase12 suites, existing owner UI, staging/restore, signed inventory/coverage,
OFF config, queue fairness and resources. Actual JS runs in UTC/Dhaka/New York
plus all three existing storage UI harnesses. No new browser/mobile/layout or
physical Pi result is claimed.

Measured150 reads +30 emissions leave disposable SQLite bytes identical
(106,496 before/after);100 signed heartbeats preserve1/1/1 session/entry/event
counts and86 pages, bounded249-byte snapshot. Query interruption, bounded history,
private snapshot failures, log rotation, stale/replayed/clock-skew reports and
integer-safe/deduplicated JSONL experiment consumption pass. Existing real cache
FULL/change and job audit history is retained; the entire historical DB is not
claimed fixed-size. No records are pruned.

Actual synthetic local5x5,000-call benchmark: median0.721us disabled event call,
28.771us enabled JSONL emission to memory;25,000 records validated. This is not
Pi/NFS/disk/network or total training overhead; custom synchronous handlers can
block and deployed log retention still needs reviewed operator configuration.

All prior changes preserved, no stash/reset/commit. LOCAL DB, legacy/OFF/resource
and existing authorization/fencing remain intact. No live services/accounts/data,
mounts/exports/firewall/SSH/Pis touched; pi01 deferral retained. Source snapshot320
files; scoped rollback checked/applied only in a disposable copy. See
[52 report](52-observability-acceptance.md) and
[evidence](52-observability-acceptance-evidence.json) for exact files/diffs, commands,
results, limits and rollback. **12.4 PASS; Phase12 repository observability work
complete under this local scope. STOP; no next phase or live enable.**

## 2026-11-10 — Phase 13.1 reproducible local evaluation runner

Implemented **13.1 only**. New `tools/benchmarks/storage_evaluation.py` and
identifiable `tools/benchmarks/evaluation/` helpers plan read-only by default;
explicit --run snapshots unchanged regression/CNN/DDP example trainers and SDK,
generates versioned deterministic multi-size CSV datasets and executes bounded
local CPU smoke jobs. Records experiment/workload/input hashes, actual sizes,
source/argv/version/host/config observations, configured pi01/pi02/pi03 identities,
local NFS mount options when observed, and raw CSV/JSONL/per-rank JSON/logs.
Unknown Pi eligibility/host, live scheduler choice/NFS metrics stay null/missing;
local ranks are never mislabeled physical Pis. No A/B/C/D/E comparison claim.

New experiment IDs only, private LOCAL roots, no symlink/traversal/remote output,
no source/data replacement, bounded generator/source reads and timeout cleanup of
owned children. Existing trusted trainers/SDK formats unchanged. No production
lease/resume credentials inherited, no controller/DB/dispatcher imports, agent
HTTP/SSH, package install/Internet, services/mounts or worker deployment. Git main
started clean at f3b371beff5c5f0414bf2acc3513a063fc118d04; prior committed work
preserved, no commit/stash/reset and legacy/OFF/resource unchanged.

Baseline36 tests PASS. New initial16 tests PASS; final **54/54 distinct tests PASS
across3 files** (evaluation18, config17, events19), no failures/errors/skips.
Actual regression/CNN CPU training is in the new suite. Separately executed and
retained **8/8 real local trials** under reports/: two sizes per regression/CNN,
plus DDP at both sizes with actual2 and3 Gloo ranks. All rank parameter hashes
synchronized. Explicit local-socket commands approved automatically; no remote Pi
contact. Rehashed retained source/datasets and verified CSV/JSON equality. An
initial4-trial earlier-source rehearsal is kept separately in /tmp, not combined.

Full methodology and exact manual/rollback commands:
[13-experiments.md](13-experiments.md); machine evidence:
[53-evaluation-runner-evidence.json](53-evaluation-runner-evidence.json).
Raw experiments retain independent identities and measured data; generated dirs
are Git-ignored, not deleted. Kernel page cache uncontrolled/likely warmed by
creation/hashing; application cache/staging not used. No Pi/NFS speedup, policy
advantage, confidence interval, fault result or chart invented. pi01 deferral and
central-controller single point of failure remain. **13.1 PASS for local runner;
Phase13 overall incomplete. STOP before13.2.**

## 2026-11-10 — Phase 13.2 controlled matrix and approved preparation

Implemented13.2 only: explicit A direct trusted-input reads, B fresh local staging,
C persistent staging/resource and D persistent staging/locality component modes.
Deterministic randomized paired blocks retain first-before-repeat dependencies;
all cells retain identical workload/input hashes, world size and fixed logical
resource profiles. Real CAS/cache/workspace, signed inventory/coverage and
production selector components are reused in NEW local experiment DBs. Raw
CSV/JSONL, per-rank results/logs and descriptive distributions are retained;
unknown wire/first-step/recovery/live-placement measurements remain missing.
Native NFS mode requires exact RO mount identity and explicit trusted-input
acknowledgment. Explicit local rehearsal substitutes only kernel mount facts and
never claims actual NFS or physical Pi scheduling/DDP results.

Final **92/92 tests PASS across5 files**: matrix11, evaluation18, locality13,
fetch33, config17. Retained actual local matrix: **72/72 completed trials**,
18 each A/B/C/D, regression/CNN/3-rank Gloo,3 repetitions, first/repeat. All24
DDP groups synchronized; warm C/D repeated staging transfers0 new payload bytes.
Rehash verification confirms CSV/JSON equality,67 source hashes and all144
retained13.1 raw files unchanged. This small, single-size-per-workload laptop
rehearsal is functional evidence, not a Pi/NFS performance or speedup claim.

Explicit approval allowed staging/publication only on pi02/pi03; pi01 remains
deferred and training waits for a user-confirmed idle window. Both Pis now have
69 verified source files under `/var/tmp/dl-eval13-76f05cf7d7dc/source`. The user
published8 approved immutable TEST objects; independent controller and NFS4.1 RO
checks verified every SHA/size, host owner125:125 and readonly file mode. Agent
PIDs946/902 remain active/unchanged. No Pi training or measured native trial ran.

An initial helper assertion safely rejected publication because sandbox overflow
UID65534 was mistaken for the real storage owner. Host-namespace/config checks
established ml-publisher125:125; the corrected helper was validated before user
execution. Existing ownership/permissions were never changed. Root-only NFS
parent permissions remain intact; remote verification used read-only sudo.

No production API/DB/config/trainer or job/checkpoint data changed; defaults stay
legacy/OFF/resource. No install/Internet, exports/fstab/firewall/account/service
changes, fault injection, reset/stash/commit. Earlier transient logs disappeared;
fresh final logs and raw evidence are retained under reports, and the scoped
baseline reconstruction is documented. See [methodology](13-experiments.md),
[54 evidence](54-policy-matrix-evidence.json) and scoped reversible diff under
`reports/eval-13-2-validation/13.2.patch`. Source rollback was verified only in a
disposable copy; all raw reports, staged bundles and TEST objects remain intact.

**13.2 local protocol PASS; physical A/B/C/D comparison PENDING. Phase13 remains
incomplete. STOP before13.3 and before any Pi training.** The live physical
controller-placement executor is still absent; even future single-host NFS trials
must retain that limitation. Central server remains a single point of failure.

## 2026-11-10 — Phase 13.3 disposable failure evaluation

Added a separate read-only-by-default failure runner and identifiable helpers in
`tools/benchmarks/evaluation/`. Explicit --run uses NEW private source/fixture
copies and allowlisted local cases only. Real owned Flask health-agent process
crash/restart, interrupted3-byte copy + verified retry, cache corruption + forced
verification/quarantine/refetch, actual publisher SIGKILL after rename, partial
checkpoint rejection, stale fencing and native3-rank CPU/Gloo full-group recovery
reuse existing implementation/tests. Modeled worker/NFS errors remain explicitly
modeled. No alternate checkpoint format, arbitrary command/PID/host or live-fault
CLI option is introduced. Owned case process groups bound timeout cleanup.

Final retained **10/10 local cases PASS** under
`reports/eval-13-3-local-faults-v2/`. Three probes separately measure detection,
copy/repair staging and private agent restart; uninstrumented fixture phase and
end-to-end recovery fields remain null. Whole-test duration is never substituted
for recovery. Real DDP state comparison retains rtol1e-7/atol1e-8 for FP32 and
exact other-state checks. No physical Pi/DDP/NFS availability/latency claim.

Original baseline49 tests PASS (cache33/checkpoint faults16). Original matrix11,
evaluation18 and config17 also PASS; final new suite9 PASS: **104 distinct tests
across6 files** in recorded runs. An initial broader run retained a new adapter
failure; corrected new suite was rerun, without rerunning unchanged passing
originals. New syntax/import-order/missing-public-policy issues and initial
failed cohorts are recorded, never hidden or called pre-existing failures.
Verification:208 source hashes, raw CSV/JSON equality and all1,110 prior raw
records unchanged. No commit/stash/reset; all384 baseline files preserved except
the four intentional documentation appends; scoped rollback is verified only in
a disposable copy. See [methodology](13-experiments.md),
[55 evidence](55-fault-evaluation-evidence.json), and
`reports/eval-13-3-validation/13.3.patch`.

Production Python/API/DB/config/trainers and legacy/OFF/resource are unchanged;
only NEW disposable local DBs/files/processes are exercised. No SSH, live Pi job,
service/account/permission/export/mount/firewall change, Internet transfer or
physical failure injection. Pi01 deferral persists. Physical network disconnect,
shared NFS/controller outage and Pi agent/rank faults are explicitly NOT_RUN:
prior publication/staging approval still requires a confirmed Pi idle window;
disconnect additionally needs explicit approval and a tested independent return
path. Shared-server isolation is unproven and central server remains a SPOF.

**13.3 local fault campaign PASS; physical acceptance pending.13.2 native
comparisons also remain pending. Phase13 incomplete. STOP before13.4.**

## 2026-11-10 — Phase 13.4 collected measurement reports and charts

Added a separate read-only-by-default offline reporter and three identifiable
helpers under `tools/benchmarks/evaluation/`, with optional controller-only pinned
plot dependencies and two test files. Explicit --write creates NEW private output;
no training/fault/Pi job runs. Bounded no-follow readers verify frozen source,
per-rank/per-record and CSV/JSON hashes/identity/schema. No pickle/torch loads,
source execution or DB open. Different experiments/workloads/input versions,
world sizes, scopes and cache phases remain separate; failures stay visible.

Actual final bundle `reports/eval-13-4-collected-v2/`: **101 recorded observations
from6 cohorts,53 summary groups,5 PNG+SVG figures and264 traceable plotted points**.
Includes both retained13.3 development-failure cohorts. Reports measured training/
completion/staging and application payload/cache ratios, selected fault phases,
and current retained-file disk observations. Physical NFS wire bytes/server
bandwidth, first-step timestamps, checkpoint save/restore timers and end-to-end
recovery are explicitly unmeasured; no invented zeros, speedups or physical claims.
Training time is max per-rank loop time; whole process/test time is not recovery.
Disk metadata is current retained-tree use, not historical peak or server storage.

Baseline28 tests PASS (matrix11/config17). New reporter12 +real Matplotlib chart2
PASS: **42 distinct tests in4 files**, no failures/errors/skips in selected runs.
Frozen/leaf/raw numerical provenance, safe paths/schema/duplicates, exact unknown
semantics, failure/cohort separation, hardlinks, no output replacement and chart
point provenance tested. Actual images visually inspected; draft report retained
before final headroom/label/schema refinement, not overwritten. A NEW optional
reporting venv downloaded plotting packages on controller only; existing training
and worker environments remain unchanged. Runtime itself never installs/downloads.

See [full methodology, gaps and commands](13-experiments.md),
[56 evidence](56-evaluation-report-evidence.json), and scoped
`reports/eval-13-4-validation/13.4.patch`. All389 baseline files preserved except
four intentional documentation appends; no existing Python/API/DB/config/trainer
change, stash/reset/commit, live Pi contact, service/export/mount/firewall change.
Legacy/OFF/resource and controller-local SQLite remain intact. Reversible source
rollback is exercised only in a disposable copy; retain all data/reports/TEST
objects. Pi01 remains deferred and prior idle-window/isolation prerequisites hold.

**13.4 local reporting PASS; full Phase13 NOT complete.13.2 native comparisons,
physical13.3 faults and the uncollected13.4 metrics remain pending. STOP; no next
phase or deployment. Central controller/NFS remains a single point of failure.**

## 2026-11-10 — Phase 14.1 all-three-Pi production readiness audit

Read-only audit of controller and **pi01/pi02/pi03** completed. Starting main
HEAD `7e88b8414b00d3169916e70495ce14d86f24e05b` clean;397 nonignored originals
snapshotted. pi01 is online at Tailscale100.101.90.46/LAN192.168.0.107, replacing
historical deferral for this audit only. Its old SSH HostKeyAlias is not its LAN
address. NFS192.168.10.214:2049 times out there; input mount/units absent. pi02/
pi03 retain RO NFS4.1 input mounts, accepted using complete mount-table guards;
ordinary pi TEST reads are denied by root0700 parent. No fresh privileged hash/
write-denial test or effective server export-table verification was possible.

Production remains legacy/OFF/resource. Controller gate rejects cache/locality/
bandwidth startup; main does not construct trusted hybrid dispatch runtime.
All live agent roots have older source and lack queried hybrid modules. No
reviewed production OS/network containment/bootstrap, protected credential/root
setup or all-three DDP dependency group was validated. All agent pip checks fail
on unsupported nvidia-cusparselt-cu13; pi01 torch import timed out twice. pi02/
pi03 tiny isolated CPU/Gloo-import probes pass, not training/DDP acceptance.
Prepared CPU environments have differing versions; existing resolved-package
compatibility keys must remain enforced. No Internet worker installs occurred.

All Pi Ethernet links report100Mbit/s; controller uses Wi-Fi with Ethernet down.
No measured throughput claim. Free space controller40.541GiB/pi015.666GiB/
pi0210.670GiB/pi038.644GiB requires aggregate budgets. SQLite remains LOCAL;
no DB open/migration. Only source-only previous worker snapshots found in queried
locations; root-only NFS backup journals unreadable, consistent application-state
backup/restore and external backups unknown. Unrelated pi02 wildcard RW/
no_root_squash CIFAR export and pi02/pi03 legacy RW mounts require exposure
review without disturbing their users. Central server remains a SPOF.

Selected config17/storage-selection21/deployment-review6/NFS-client28 tests:
**72 PASS,0 fail/error/skip**,4 files. Existing environment failures are explicitly
recorded. All observed controller/agent PIDs unchanged. No sudo, remote writes,
install, service restart, export/mount/firewall change, live training, DDP group,
fault injection or restore. Prior Phase13 physical gaps remain pending.

Added [57 readiness report](57-production-readiness.md) and
[57 evidence](57-production-readiness-evidence.json); this append is the only
existing source change. Preserved396 other originals and all data. Scoped diff,
source-hash verification and disposable-copy reverse-apply checks are retained
under `reports/phase14-1-readiness/`. No stash/reset/commit. No API/DB/config/
runtime change; legacy fallback intact.14.2 must prepare reviewed idempotent
all-three deployment/snapshots/rollback and refuse unresolved prerequisites;
14.3 mutations require explicit approval.14.4 demo/README not started.

**14.1 audit PASS; deployment readiness FAIL/BLOCKED. Phase14 incomplete.
STOP before14.2.**

## 2026-11-10 — Phase 14.2 all-three delivery dry-run preparation

Added `hybrid_storage/delivery_plan.py`, `tools/hybrid_deploy.py` and16 Pi-less
tests. Explicit local plan output holds complete controller/worker source review,
exact source/config/audit hashes, fresh unique snapshot namespaces per controller/
pi01/pi02/pi03, LOCAL DB migration order, concrete service argv/order and rollback
runbook as DATA. No deployment executor: apply always REFUSES/exit3 before reading
a package. All-three requirement cannot be silently reduced; pi03 UID1002 and
SSH HostKeyAlias distinction retained. Actual unit/config/bootstrap deltas and
disk budgets remain null/UNRESOLVED. Defaults and production gates unchanged.

Final `reports/phase14-2-delivery/review-v2/` verifies and repeated preparation
preserves all bytes/modes/mtimes. Release `release-2d924028652737d1019dd1d7`,
snapshot `snap-20261110T082453Z-78a0011db2562841`, plan SHA256
`382724cc15cce5bebd6e8390a82deb561160a3a28264816ff4d894fd770f0094`.
Controller119/worker81 regular source members; no secrets/user data/DB/checkpoints/
environments/wheels. Six public configs and audit are drift checked. Symlinks,
unsafe/resealed archives, decompression bombs, duplicates, concurrent/partial
output and tampered gates tested. Earlier development review retained, never
uploaded/applied. Source digest verifies consistency, not permission or freshness.

Baseline72 PASS plus final new16 PASS = **88 distinct tests in5 files**; no final
fail/error/skip. Initial new13 suite had2 implementation/test failures (raw config
hash omission and relocated CLI fixture-root mismatch), corrected and retained.
Six proposed additive migrations actually run twice on a SQLite-backup/restored
WAL disposable actual-controller fixture: legacy rows/user_version unchanged,
second logical dump unchanged, quick/FK checks clean. Not live restore acceptance.

No sudo, SSH/remote contact or writes, installs/Internet, live DB open/migration/
backup, service restart, mount/export/firewall change, training/fault/DDP/boot
test.14.1 observations are historical inputs, not fresh Pi readiness. All B1–B10
gates remain explicit; B7 packaging/preparation improved, no applying installer.
Central controller/NFS remains a SPOF. Legacy/OFF/resource intact; no existing
Python/API/schema/config changes or automatic data moves/registration.

See [58 playbook](58-deployment-playbook.md) and
[58 evidence](58-deployment-preparation-evidence.json). Preserved399 baseline
files except3 intended documentation appends; previous uncommitted14.1 audit
preserved exactly. New5 files; scoped rollback verified in disposable copy only.
No reset/stash/commit, and no data/report cleanup. Top-level README/demo reserved
for14.4. **14.2 preparation PASS; apply-ready deployment NOT delivered due to
unresolved prerequisites.14.3 BLOCKED; Phase14 incomplete. STOP.**

## 2026-11-10 — Phase 14.3 blocked deployment preflight

Fresh read-only SSH metadata/health probes completed on all3 aliases; hostnames
match and agents ONLINE/IDLE at probe time. pi01 input mount absent and configured
NFS TCP2049 times out. pi02/pi03 retain admitted RO NFS4.1 mounts509/508. All3
service roots still have old agent hash and lack queried staging/checkpoint/
hybrid config modules. No privileged input read, write test or dependency repair.

Actual reviewed14.2 package verifies exit0 but apply_ready=false and executor
absent; apply entrypoint returns REFUSED/exit3 without any deployment operation.
Legacy config validates exit0; controller NFS/cache example rejects exit2;
explicit staging-component validation succeeds with automatic_startup=false.
Current code still lacks trusted production bootstrap and applying executor;
unit/config deltas and disk budgets remain unresolved. User's request to proceed
does not turn a data-only plan into executable deployment. No guard bypassed.

Five fresh local checks and3 SSH probes recorded; prior88 tests not rerun or
claimed as14.3 hardware acceptance. No sudo, remote writes, deploy/restart,
DB read/migration/backup, package install/Internet transfer, mount/export/firewall
change, training/DDP/fault/boot test. Services remain legacy/OFF/resource; live
acceptance NOT_RUN. Dependency/backup/containment and unrelated legacy export
unknowns remain from14.1. Central server remains a SPOF.

See [59 preflight](59-deployment-preflight.md) and
[59 evidence](59-deployment-preflight-evidence.json). Added2 docs plus this append;
404 prior nonignored originals preserved except this intentional append. Prior
14.1/14.2 edits unchanged. Source-only scoped rollback verified in disposable
copy; no runtime rollback needed because deployment never started.

**14.3 BLOCKED/NOT COMPLETE; integrated bootstrap/executor and infrastructure
prerequisites must be resolved and reviewed before apply. STOP before14.4.**

## 2026-11-10 — 14.1 closeout continuation: real DB recovery and protected probes

User asked to finish14.1/14.2 before deployment, kept pi01 on its existing network,
and approved narrowly scoped sudo READ checks. Added explicit LOCAL SQLite backup/
restore verification and fixed-path protected NFS probe modules plus CLI and17
focused tests. No startup/API/config/legacy code change. 14.1 verification first;
no full-phase completion asserted while infrastructure/approval inputs are missing.

Actual live controller DB opened mode=ro, backed up to NEW private directory and
separately restored/verified: both1,601,536B, identical SHA256
`5e9fc64bf5c3f2fac165b7493c36ef2c82436476ad7e03f7bd0976d769572144`.
LOCAL DB-only recovery point, not app-files/checkpoints/off-host/HA. Recovery files
are ignored/private, no row data exposed. Another NEW clone ran six hybrid
migrations twice:150 existing jobs/all13 legacy tables/user_version unchanged,
second fingerprint unchanged, quick_check ok/FK clean. Live DB not migrated/restored.

Approved root probes now verify pi02/pi03 RO NFS4.1, existing public165B TEST hash
and EROFS/errno30 write-open denial with0 bytes written. Root setup-journal names
listed only; no journal-content restore claim. pi01 has no matching mount or
client journal, so TEST operation skipped with no local-directory fallback.
Initial probe rejected kernel/proc/self; fixed to numeric own PID and added real
portable-kernel-path test. Initial failures/program retained. Controller sudo-n
needs password; its corrected read-only command supplied for user's terminal,
output still pending. Approval persists only for protected reads, no setup changes.

Measured tiny Tailscale path checks: controller→pi01 DERP(sin)114/121/117ms;
pi02→pi01 DERP231/105/105ms;pi03→pi01 DERP250/113/113ms;pi02→pi03 directLAN2ms.
No direct pi01 path established in3 tiny tries each. Latency/path observations,
not bandwidth/DDP slowdown benchmark. NFS binds only192.168.10.214; LAN NFS to
pi01 times out, tailscale100.73.90.91:2049 refuses on all3. No Internet-dependent
NFS workaround enabled. Offline all-three restricted-export/client proposals
validated; pi02/pi03 unit bytes unchanged. Bidirectional private no-SNAT route and
router topology unknown. All3 service pip checks still fail on unsupported NVIDIA
package; queried wheel caches absent. No packages/environment changes.

Final105 tests PASS in7 files (prior88 +backup10 +probe7),0 fail/error/skip. Initial
104 local pass preceded real/proc/self failure; regression-fixed105 retained.
No sudo configuration, mounts/exports/network/firewall/service changes, live job,
DDP/fault/boot test, production DB migration/restore, stash/reset/commit or data
cleanup. New ignored DB copies only; controller/NFS remains a SPOF. Prior14.1–14.3
source edits preserved; new modules intentionally make old frozen14.2 catalog
historical, not silently regenerated or deployed.

See [60 closeout](60-readiness-closeout.md) and
[60 evidence](60-readiness-closeout-evidence.json). Scoped source rollback verified
only in disposable copy; keep all private backups/reports. **14.1 readiness still
BLOCKED;14.2 applying installer not completed;14.3 NOT RUN. STOP for protected
controller output and a compatible private pi01 data path.**

## 2026-11-10 — 14.1 controller terminal evidence and network requirement confirmed

User supplied successful approved root controller probe JSON; provenance is
user-terminal paste, not an assistant rerun. Nine local consistency checks PASS:
effective input export only to pi02/pi03, ro/root_squash, managed-file agreement,
comments-only main exports, NFS4.1/4.2 enabled and3/4.0 disabled, expected165B TEST
hash and read-only/no-change probe scope. Controller write-open not attempted,
as expected for trusted local publication; worker EROFS results remain separate.
Root0700 setup-journal name confirmed, not journal-content or restore proof.
Earlier sudo-auth failure remains historical; controller terminal output is no
longer pending. See [updated closeout](60-readiness-closeout.md) and its evidence.

User explicitly chose **local/private-only runtime data transfers**. pi01's
current Internet DERP path cannot be used for dataset/checkpoint/DDP traffic;
no requirement relaxation, relay NFS workaround or deployment approval inferred.
Need verified private bidirectional routing while pi01 stays on its network;
actual topology and router next hops remain unknown. All-three requirement holds.

Updated four documentation/evidence files only; no runtime/API/DB/config/schema
change. Prior105 tests not rerun for this evidence-only update. JSON consistency,
source fingerprint/preservation and scoped reverse-patch checks recorded in
`reports/phase14-1-controller-verification/`; exact narrower rollback patch is
`documentation-update.patch`, tested only in a disposable copy. Reports/backups
retained. No new SSH/sudo, live DB read, deployment, mount/export/route/service
change, package transfer, benchmark or job/DDP test. Legacy/OFF/resource intact.

**14.1 readiness remains BLOCKED** on pi01 private route, compatible offline
dependencies, coordinated files/checkpoint/off-host recovery and trusted runtime
containment/bootstrap. **14.2 applying installer remains unfinished;14.3 NOT RUN.**
Central controller/NFS remains a SPOF. This closes one evidence gap only. STOP.

## 2026-11-10 — Phase14.2 executable source-delivery tooling and dry-run acceptance

Completed the missing explicit host installer, migration executor and guarded
rollback in `deployment_executor.py`, `deployment_migrations.py` and
`deployment_toolkit.py`, with `tools/hybrid_install.py`, three focused test files
and [61 tooling report](61-installation-tooling.md)/evidence. Earlier uncommitted
14.1–14.3 work preserved. Legacy `hybrid_deploy.py apply` still refuses; the new
protected host launcher performs separately approved operations instead.

Fresh non-sudo read-only source/unit/identity inventories succeeded on controller,
pi01,pi02,pi03; no remote file or service changes. Actual UID/GID1002 on pi03 kept.
Reviewed kit has controller124/worker86 source files,396 private regular files,
snapshot `snap-20261110T170756Z-34c52925118fb87e`, KIT SHA256
`cef5f6f443f0a20c3232cc219645fa86395e346fe40c089be99b1b84b31ce293`.
Repeat preparation preserved bytes/modes/mtimes. Packaged controller dry-run passes
with conservative2,695,448,936B budget; worker capacity previews derive only from
fresh read-only inventory and locally verified payloads, not remote execution.
Invalid example approval refuses exit2 before deployment namespace changes.

Explicit unit/config deltas are empty: preserve exact service executable/argv,
enablement, local config/credentials and legacy/OFF/resource. No requirements pip
install or environment replacement. Exact interpreter/destination/stop/start/
install/migrate/rollback argv supplied in OPERATIONS.json. No new flags or API.
Separate ten-minute per-action/host/hash approvals and all-three maintenance/gate
attestations required. Actual stopped-unit/cgroup/source/unit/config guards run;
installer never kills training or starts services. Unique private snapshots,
exclusive lock, preimage backups and fsynced journal precede atomic source writes.
Rollback preserves external edits, restores only reviewed preimages and quarantines
new source; never deletes jobs/cache/checkpoints or restores an old DB automatically.

Controller migration step requires idle jobs/reservations/leases/staging and exact
reviewed migration code; creates verified LOCAL DB recovery copies, rehearses six
additive modules twice on a new restored fixture/copy and checks legacy rows/schema/
user_version/FKs. Interrupted additive migrations can resume without refreshing
original recovery files. Immutable SQLite reading is only for verified closed
backup copies with empty WAL, never live controller state.

Final **139PASS,0fail/error/skip,10files**: previous105 rerun plus20executor,
7migration,7toolkit. Disposable fixtures prove interruption, legacy preservation,
rollback, locks, disk-full/links/traversal, stale/bad approvals and config-off
regressions. Initial fixture-row/SHM-mtime and later exception-expectation failures
retained, corrected. Actual systemd volatile ExecStart fields handled/tested.
Logs, raw inventories, repeatability and scoped rollback evidence under
`reports/phase14-2-completion/`. No sudo, remote install/copy, service operation,
live SQLite connection/backup/migration/restore, training/DDP/NFS fault/boot test.

**14.2 compatible source-delivery tooling/dry-run acceptance PASS.** This is not
a production hybrid bootstrap or containment implementation.14.1 private pi01
data route, compatible offline dependencies, coordinated recovery and trusted
runtime gates remain; pi02 active-unit/PID0 metadata does not imply healthy agent.
All-three requirement and private-only data transfers preserved. Controller/NFS
remains a SPOF. **Hybrid-enabled deployment readiness BLOCKED;14.3 NOT RUN. STOP
before privileged staging, installation or restart.**

## 2026-11-10 — 14.3 explicit two-node scope and fresh readiness checks

User confirmed pi01 at another site and explicitly requested pi02/pi03-only work.
Added `--workers pi02 pi03` support in deployment_toolkit.py; defaults remain all3.
Exact selected hosts required, pi01 payload/operations absent, only pi01-specific
gateB3 deferred. Executor receipts bind selected scope; legacy all_three_workers
receipts accepted only for true three-worker plans. Cross-scope/ambiguous receipts
refused without writes. No live node/config/flag change or Pi01 contact.

Fresh read-only checks: pi02/pi03 expected identities ONLINE/IDLE with one available
slot, controller NFS reachable and full-table RO mount admission PASS. Both service
pip checks still FAIL on unsupported nvidia-cusparselt-cu13 0.8.1. Basic temporary
namespace true-process smoke checks pass, not production containment acceptance.
Controller query_only SQLite aggregate snapshot:0 active jobs/reservations/live
leases/nonterminal staging, not a persistent admission fence. No student identifiers
or secret values published. No sudo, remote writes/packages, service operation,
fault/job/DDP/boot test, live migration or deployment.

Prepared/verified new two-node final-review, all Pi payload verification local only;
controller packaged dry-run passes. Prior reviews retained as dated evidence.
Final49 local tests PASS in4files (20executor+7toolkit+6newscope+16delivery),0 fail/
error/skip. Earlier139 result not relabeled as fresh full-suite/hardware acceptance.
Source rollback verified only in new disposable copy; prior changes preserved.

Actual engineering gap remains: dispatcher main does not construct/pass trusted
runtime; normal worker startup lacks staging/inventory/checkpoint bootstrap and
reviewed isolate/terminate/quiescent providers; controller config still rejects
cache/locality execution. Installer is a legacy-preserving delivery profile, not
the missing runtime. Clean offline candidate envs, protected service/job identities,
network/cgroup isolation, admission/drain and coordinated recovery provisioning
are needed. Recommendation: separate pi02/pi03 pilot before switching student services.
Manual privileged host provisioning will be needed after the concrete runtime plan;
deployment approval alone cannot supply missing code. Central server remains SPOF.

See [62 pair scope and engineering plan](62-two-node-deployment-preflight.md) and
companion evidence for exact files/interfaces, commands, tests and rollback.
**14.3 NOT COMPLETE; no live deployment started. Pi01 is deferred, not blocking
this two-node scope. Remaining blockers are real integration/dependency work. STOP.**

## 2026-11-11 — 14.3 runtime assembly, actual NFS checks and Pi component tests

Continued the authorized pi02/pi03 scope; pi01 was neither contacted nor used as
a blocker. Implemented explicit `hybrid_storage/runtime_bootstrap.py` factories
and wired `controller/dispatcher.py::main(storage_runtime=None)` into the existing
four reconciliation/dispatch seams. Enforce matching configuration/LOCAL DB,
operator enrollment, initialized checkpoint-store identity, protected separate
worker roots, private endpoint configuration, actual RO mount identity and an
explicit launch boundary. Reuse authenticated staging/inventory/checkpoint
services, coverage, transfer observations and admin diagnostics. No new schema,
route or checkpoint format. Default legacy calls/configuration are preserved.
Failed worker setup cannot be retried as legacy; busy shutdown retains ownership.

Added 28 startup tests. Actual broad local run: 250 PASS in 13 files, 0 fail/error/
skip, 306.1s; its startup suite had 21 cases. Expanded final suite: 28 PASS locally
and 28 PASS on each Pi under Python 3.11.16. Repeated runs are not distinct cases.
Final transport review enforces selected polled endpoints and no redirects for
coordinator GET/POST calls; that regression triggered the final local/Pi reruns.
The Pi tests used a hashed 223-file temporary source bundle transferred over LAN,
not installed services or live DBs. Local fixtures simulate NFS/containment and
training; their pass is not a live cold/warm/DDP/recovery acceptance claim.

Fresh privileged read-only checks on both Pis: exact RO NFS 4.1 input mount, known
public TEST 165-byte hash PASS, write-open denied EROFS 30 with 0 bytes written.
Controller and peer routes use eth0. CPU-only environment already on both nodes
passes pip check; Torch 2.15.0+cpu/Gloo/tensor smoke check passes, with missing-NumPy
warning. The old service environment's documented NVIDIA dependency issue was
not repaired. Bubblewrap namespace capability and nftables v1.0.6 are available.

Final test roots retained: pi02 `/tmp/dl-phase14-3-runtime-h6jg_46q`, pi03
`/tmp/dl-phase14-3-runtime-qjoqhskb`; intermediate roots/receipts are retained too.
After tests original agents remain active/IDLE
with unchanged PIDs 946/902. No installed source/config, existing data, packages,
services, exports, firewall, SSH, fstab or live DB/migration was changed. Controller
sudo authentication still requires a terminal password; no password was requested.

**Production gaps remain explicit:** LaunchBoundary is an interface with NO Linux
provider in this delivery; normal agent startup still needs reviewed activation.
Current pi account has NOPASSWD sudo ALL and is shared with training. Portal immutable
intake/offline metadata preparation, protected result collection, durable drain,
clean candidate envs and reviewed identity/root/key provisioning still need work.
New factories alone do not implement these. Existing 14.2 installer is legacy-only;
its older review packages must not be relabeled as an enabled hybrid release.

See [63 runtime assembly](63-runtime-assembly.md), companion evidence and
`reports/phase14-3-runtime/` for exact commands, measured receipts and scoped source
rollback rehearsal. No production switch occurred. Controller remains a SPOF.
**Assembly/local/Pi compatibility and actual NFS checks PASS; overall 14.3
NOT PASSED. Live hybrid inventory/training/cancellation/recovery/DDP/boot tests
remain NOT_RUN. STOP before unsafe activation; do not advance to 14.4.**

## 2026-11-11 — 14.3 live v8 pilot acceptance

**PASS for the operator-approved controller, pi02 and pi03 scope.** Activated
`pilot-20261111-074d980811f0` with offline packages and private-LAN workers.
The live suite passed owner permissions, cold training and result collection,
warm reuse on both workers, cancellation of a running job, single-worker
checkpoint recovery and two-rank DDP group recovery. Both recovered DDP ranks
matched the uninterrupted model hash. Final checks verified active and enabled
units, idle workers, signed inventory coverage and read-only NFS 4.1 access.

The pilot remains active. Legacy state and rollback snapshots were retained.
The three-worker and physical-reboot criteria remain unverified. See the
[v8 deployment report](64-v8-pilot-deployment.md) for measured results, receipts,
local regressions, manual verification commands and rollback. Phase 14.4 has
not started.

## 2026-11-11 — 14.4 supervisor demo and delivery documentation

**PASS for the documentation deliverables and approved two-worker scope.**
The [supervisor runbook](14-supervisor-demo.md) contains a timed live-demo script,
an actual v8 sign-in screenshot, a recorded-evidence route, measured cold/warm
and recovery results, the research contribution, limitations, and optional
Linux lab and Wake-on-LAN plans. The README distinguishes the active pilot from
legacy startup. The project architecture now shows the deployed data and trust
boundaries. The original Phase 1 proposal remains available as design history.

The runbook's read-only preflight passed on the controller, pi02 and pi03. Both
Pis showed active and enabled agents and read-only NFS 4.1. All five receipt
queries reproduced the documented values. The screenshot was captured from the
pilot's LAN sign-in page and inspected. Relative links and `git diff --check`
passed. The optional live submission and full timed presentation were not run
in this documentation step. The original three-Pi acceptance remains unverified
with pi01 deferred. No service, API, configuration, schema or deployment was
changed. To roll back 14.4, reverse only the demo documentation and screenshot
diff. Runtime rollback remains the separate procedure in the
[v8 deployment report](64-v8-pilot-deployment.md#commands-and-rollback).
