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
