# Regression checks

Use a Python environment with `config/runtime-requirements.txt`, `torch` and
`numpy` installed. The existing `.controller-venv/` already contains these packages
on the configured controller. A fresh development environment can be prepared with:

```bash
python3 -m venv .controller-venv
.controller-venv/bin/python -m pip install -r config/runtime-requirements.txt torch numpy
```

From the repository root, run all checks with:

```bash
.controller-venv/bin/python tools/run_tests.py --isolated
```

Run selected files with:

```bash
.controller-venv/bin/python tools/run_tests.py --isolated test_student_portal.py test_cluster_setup.py
```

Each file runs in a separate process. Some suites import controller modules from
temporary copies and modify module search paths, so running every suite in one
`unittest discover` process can mix their isolated environments. The built-in
checkpoint suite is also a standalone assertion script.

| File | Coverage |
|---|---|
| `test_checkpoint_recovery.py` | Built-in trainer equivalence, legacy migration, dispatch and benchmark compatibility |
| `test_cluster_setup.py` | Safe inventory registration, deployment bundles and service generation |
| `test_custom_checkpoint.py` | Submitted trainer state, SDK restore and checkpoint validation |
| `test_distributed_dispatch.py` | Group reservation, rank launch, stop/retry and real two-process training |
| `test_execution_ownership.py` | Lease watchdogs, attempt fencing and stale result rejection |
| `test_group_checkpoint.py` | Consistent DDP checkpoint publication and invalid acknowledgments |
| `test_group_recovery.py` | Exact distributed restore, rank transfer and recovery acknowledgments |
| `test_job_environments.py` | Offline pip installation, cache reuse and environment compatibility |
| `test_job_storage.py` | Input snapshots, archive safety and staging recovery |
| `test_job_submission.py` | Manifest validation, submission transactions and CLI behavior |
| `test_job_timing.py` | Timing reports and atomic persistence |
| `test_queue_controls.py` | Queue fairness, limits, cancellation and process cleanup |
| `test_resource_allocation.py` | CPU/RAM/GPU admission and worker reservations |
| `test_student_portal.py` | Login, uploads, templates, ownership, queue actions and downloads |

These tests use temporary inputs/databases and mocked cluster calls; the DDP
suites launch local training processes and need permission to open loopback
sockets. They do not need the physical Pi cluster. Live cluster demonstrations
are under [tools/demos/](../tools/README.md#live-demonstrations).

## Hybrid storage checks

[test_storage_config.py](test_storage_config.py) checks strict flags and compares
missing/default/off startup, uploads and resource placement using temporary
controller copies. [test_storage_backends.py](test_storage_backends.py) checks
safe local/NFS-adapter reads and publication without a live NFS mount. These two
files can be run directly through the runner without touching the live DB:

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py test_storage_config.py test_storage_backends.py
```

[test_storage_manifest_drafts.py](test_storage_manifest_drafts.py) validates design
fixtures and schemas. Use the documented system Python with its existing optional
`jsonschema` package for zero skips; it is absent from the controller venv:

```bash
env PYTHONDONTWRITEBYTECODE=1 python3 tests/test_storage_manifest_drafts.py
```

The [1.4 configuration guide](../docs/storage/03-configuration.md) records additional
source-only baseline comparisons. Several older tests import DB-initializing
controller modules before redirecting their DB path, so run those comparisons in
a temporary source copy, as recorded in the audit, rather than on live job data.

[test_nfs_server_setup.py](test_nfs_server_setup.py) tests controller setup with
isolated fake-host directories and synthetic network/package facts. It covers
strict export policy, backup-before-change, repeated staging, concurrent edits,
interrupted setup and unsafe filesystem paths. It never invokes sudo, exportfs,
systemd, package installation or remote Pis. Run the three safe local files:

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py test_nfs_server_setup.py test_storage_config.py test_storage_backends.py
```

The [2.2 evidence](../docs/storage/05-nfs-server-dry-run.json) records 60 passing
tests and the separately executed, read-only controller dry-run. Those historical
results predate the [live Phase 2 validation](../docs/storage/10-phase2-completion.md)
on the explicitly selected pi02/pi03; pi01 remains deferred.

[test_nfs_client_setup.py](test_nfs_client_setup.py) adds 25 isolated tests for all
three fake clients, backup/idempotence, conditional mocked offline provisioning,
boot ordering and mount admission. It never invokes real SSH, apt, systemd or
mount operations. Safe combined regression selection:

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py test_nfs_client_setup.py test_nfs_server_setup.py test_storage_config.py test_storage_backends.py
```

The [2.3 evidence](../docs/storage/06-nfs-client-validation.json) records 85 passes
and separate local native verification of the three generated systemd unit sets.
It also records refusal by the mount guard on the unmounted controller. These
results do not establish live Pi mount/boot/outage behavior or tenant isolation.

[test_nfs_fault_probe.py](test_nfs_fault_probe.py) adds five Pi-less tests using
local TCP echo and a temporary Unix socket. No root, NFS, SSH or host network
configuration is required; the test environment must permit loopback sockets.
The mount guard also now covers Linux's read-only VFS mount over an rw NFS
superblock, while still requiring the security flags in the VFS field.

The final Phase 2 local selection passed **96 tests, zero failures/skips**:

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py test_nfs_server_setup.py test_nfs_client_setup.py test_nfs_fault_probe.py test_storage_config.py test_storage_backends.py
```

Counts: server 22, client 28, fault fixture 5, config 17, backends 24.
Actual private-mount/disconnect/recovery tests on pi02/pi03 are separate from
these local tests; see the [Phase 2 report/evidence](../docs/storage/10-phase2-completion.md).
No reboot, production training, global outage or full tenant isolation was tested.

[test_storage_inputs.py](test_storage_inputs.py) adds 18 strict-key/project-view
checks and a local integration test using the real legacy staging code in a
temporary controller copy. NFS is a fake local directory; there is no sudo, SSH,
mounting or live DB access. Run the current Phase 3.1 selection (68 passes):

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py test_storage_inputs.py test_storage_backends.py test_storage_config.py test_job_storage.py
```

See the [3.1 report](../docs/storage/11-controller-input-adapters.md) for exact
scope. These tests do not establish mount health, read-time integrity admission
or application opt-in; those remain later subphases.

[test_storage_health.py](test_storage_health.py) adds 26 fake/local checks for
exact mount/FD admission, size/hash verification, error classification, publication
locality and owned-process deadlines. It uses a real local verification child and
disposable sleep child; uncancellable hard I/O is modeled, not induced. No root,
SSH, mount or live DB access. The current safe selection passes 144 tests:

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py test_storage_health.py test_storage_inputs.py test_nfs_client_setup.py test_nfs_server_setup.py test_storage_backends.py test_storage_config.py test_job_storage.py
```

The [3.2 report](../docs/storage/12-input-health.md) distinguishes component health
from live NFS/server availability and documents pending-process ownership limits.
Local publication/spooling currently supports ext4/xfs/btrfs; unknown or remote
filesystems fail closed. Existing fake NFS input tests explicitly mock mount IDs.

[test_storage_selection.py](test_storage_selection.py) adds 21 temporary-controller
checks for atomic/immutable pinning, additive migration, old-job preservation,
config/API/CLI selection, retained NFS holds and execution gates. Use `--isolated`
for suites that import DB-initializing controller modules before fixture setup;
the runner excludes live DB/runtime roots and clears storage-root overrides.
This isolates test state, not network access; live demos remain separate.

Current Phase 3.3 selection: **130 passed, zero failures/skips, 9 files**:

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_storage_selection.py test_storage_config.py test_job_submission.py test_job_storage.py test_student_portal.py test_queue_controls.py test_resource_allocation.py test_storage_inputs.py test_storage_health.py
```

See the [3.3 report](../docs/storage/13-storage-selection.md). Configured CLI
fixtures include the shared `hybrid_storage` config package; absent-config
controller-only CLI compatibility and missing-package fail-closed behavior have
separate tests. No live NFS/Pi or complete DDP execution is exercised here.

### Phase 3.4 acceptance

[test_storage_readers.py](test_storage_readers.py) adds eight checks for concurrent
independent descriptors, simultaneous project grants, mixed integrity outcomes,
mount replacement, partial-read permission/outage failures, legacy reads without
NFS admission, and the shipped [config samples](../config/storage-examples/README.md).
Mount metadata/FD identities are explicitly mocked; real temporary files supply
the bytes. There is no production mount bypass, live NFS or controller DB import.

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_storage_readers.py test_storage_selection.py test_storage_config.py test_job_submission.py test_job_storage.py test_student_portal.py test_queue_controls.py test_resource_allocation.py test_storage_inputs.py test_storage_health.py test_storage_backends.py test_nfs_client_setup.py test_nfs_server_setup.py
```

The [3.4 report](../docs/storage/14-phase3-validation.md) records actual results,
coverage and remaining gates. Keep `--isolated`: older controller tests initialize
SQLite on import. The source-copy runner is state isolation, not a network
sandbox; this selection was reviewed for fake/local operation. Do not infer full
training, DDP recovery or live all-three-Pi acceptance from it.

### Phase 4.1 artifact registrar

[test_artifact_registration.py](test_artifact_registration.py) adds 24 local tests
for bounded streams, verified deduplication, immutable paths/content, no-replace
rename, thread/process writers, private scratch, error handling and process-crash
reconciliation. It writes only disposable directories; no live NFS, DB or Pi.

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_artifact_registration.py
```

The [4.1 report](../docs/storage/15-artifact-registration.md) records the complete
236-test isolated regression command, outcomes, initial fixture failures and
remaining scope. Registrar tests exercise the actual local Linux rename syscall;
they do not establish hardware power-loss durability or authorized dataset/job
enrollment, which remain later subphases.

### Phase 4.2 dataset contracts

[test_dataset_manifests.py](test_dataset_manifests.py) has 26 canonical identity,
versioning, immutability, ownership/ACL, parser and artifact integration checks.
[test_dataset_validation.py](test_dataset_validation.py) has 13 local inspection,
symlink/race/path and ZIP-slip/metadata checks. No archive extraction, DB import,
live NFS/Pi or enrollment occurs.

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_dataset_manifests.py test_dataset_validation.py
```

The [4.2 report](../docs/storage/16-dataset-manifests.md) records the full 275-test
selection, separate 15-test historical schema run, 28 new-schema checks, access
regression fix and limits. Optional system `jsonschema` validates documentation
offline only; new runtime/tests use stdlib and add no worker dependency. These
components require later trusted registry/version bindings and current ACL lookup
before any production access is enabled.

### Phase 4.3 registry and immutable job inputs

[test_storage_registry.py](test_storage_registry.py) adds 38 isolated migration,
authority, concurrent writer and copied-controller integration checks. It tests
real temporary staged jobs/leases/checkpoint-byte fixtures without contacting
workers or importing a live job database.

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_storage_registry.py
```

The [4.3 report](../docs/storage/17-registry-and-job-inputs.md) records the full
313-test main selection, separate 15-test historical draft validation, exact
DDL/checksum, compatibility checks and rollback. No live migration, NFS execution
or automatic legacy enrollment is implied by these component tests.

### Phase 4.4 explicit legacy enrollment

[test_legacy_registration.py](test_legacy_registration.py) adds 25 local cases
for owner-authorized snapshot registration, two owners' physical dedup/private
access, immutable retry/concurrent sidecars, unchanged legacy execution/files,
version updates and failure/path/import safety. It reuses the reviewed temporary
controller fixture without rerunning its inherited test methods. One known
offline scalar-training fixture runs before and after enrollment; no worker,
NFS/Pi, PyTorch/DDP or live registry action occurs.

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_legacy_registration.py
```

The [4.4/Phase 4 report](../docs/storage/18-legacy-registration-and-phase4.md)
records the full 338-test selection, separate 15 draft tests, actual results,
corrected fixture assertion placement, compatibility limits and rollback.

### Phase 5.1 persistent local worker cache

[test_worker_cache.py](test_worker_cache.py) adds 33 disposable local checks for
verified-object metadata, stale/stage-time hashing, corruption/path protection,
exact pins, process restart, concurrent processes, interrupted SQLite changes,
bounded metadata lock waits and disk accounting. No NFS/Pi or live-agent action.

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_worker_cache.py
```

See [5.1 report](../docs/storage/19-worker-cache.md) for the 371-test combined
selection, exact API and still-disabled staging/cache execution boundary.

### Phase 5.2 missing-only NFS cache copy

[test_cache_fetch.py](test_cache_fetch.py) adds 33 local/fake tests for actual
helper streaming, atomic publication, warm zero-NFS reuse, integrity/refetch,
quota/reserve and ENOSPC, transient interruption, current authorization, concurrent
processes and parent/helper failure ownership. Positive NFS mount facts are
mocked; the real helper rejects an ordinary directory. No live NFS/Pi test.

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_cache_fetch.py test_worker_cache.py
```

Expected: 66 tests pass. [The 5.2 report](../docs/storage/20-cache-fetch.md) records
the 404-test combined selection, corrected fixture error/warnings, exact limits
and the 5.2-only rollback preserving earlier uncommitted 5.1 work.

### Phase 5.3 private workspaces and lifecycle pins

[test_private_workspaces.py](test_private_workspaces.py) adds 39 disposable local
checks for independent full copies, modes/paths, exact pin/root isolation,
terminal cleanup/quiescence, failures, process crash/restart and usable inputs.
Known stdlib programs run directly and through fake-NFS component integration;
an existing lease guardian's real local expiry is tested before unpinning.
No live Pi/NFS, PyTorch/DDP, machine reboot or privileged sandbox is exercised.

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_private_workspaces.py
```

[The 5.3 report](../docs/storage/21-private-workspaces.md) records the 443-test
selection, corrected two-root holder regression and scoped rollback preserving
earlier uncommitted storage work.

### Phase 6.1 local cache registry schema

[test_cache_registry_schema.py](test_cache_registry_schema.py) adds 22 checks
against copied-controller temporary DBs: non-destructive/idempotent/concurrent
migration, rollback, indexes/FKs, untrusted defaults, high-water/session/cursor
guards, disk/size constraints and base registry compatibility. Synthetic SQL auth
metadata tests constraints, not real worker authentication. No live DB/worker use.

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_cache_registry_schema.py
```

See [6.1 evidence and boundary](../docs/storage/22-cache-registry-schema.md) for
the 465-test selected run, the actual runtime-auth gap and pending 5.4 acceptance.

## Phase 6.2 authenticated cache inventory

`test_cache_inventory.py` has 38 disposable local cache/SQLite tests using the
actual Flask agent, registration and heartbeat adapters. Covers signed full/delta/
unchanged reports, startup/reconnect, missing/corrupt objects, duplicate/out-of-order
replay, spoofing/rotation/revocation, freshness, concurrent duplicate writes and
failed offline persistence. No SSH/Pi/NFS/network listener or credentials required.

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_cache_inventory.py test_cluster_setup.py test_cache_registry_schema.py
```

Expected: 68 tests pass. Final broader selection: 523 tests, 25/25 files passed.
See [exact commands, initial new-code failure and limitations](../docs/storage/23-authenticated-cache-inventory.md).

## Phase 6.3 cache coverage

`test_cache_coverage.py`: 31 safe disposable registry/cache/auth integration cases
for deduplication, ACLs, exact job/dataset versions, DDP whole-file requirements,
freshness and read snapshots. Three authenticated fixture workers have exactly
0/2/502 **bytes** missing from actual local objects; this is not the 6.4 MB fixture.

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_cache_coverage.py
```

Expected: 31 pass. Broader run: 554 pass across 26 files, plus six pass/one socket
EPERM error in existing `test_distributed_dispatch.py`. That same localhost-socket
error reproduces with pre-6.3 runtime sources in a disposable copy; test unchanged.
See [exact commands/results and limitations](../docs/storage/24-cache-coverage.md).

## Phase 6.4 registry fault/concurrency acceptance

`test_cache_registry_acceptance.py` adds 16 isolated local cases, including the
exact 0/2/502 MB fixture using actual 500 MB and 2 MB objects. MB = 1,000,000
bytes; the fixture writes about 1.504 GB under a temporary directory and skips
explicitly if its free-disk requirement is unmet (a skip is not acceptance).
No Pi/SSH/NFS/network listener or Internet transfer. Process abort, actual SQLite
write contention and injected SQL disk-full errors affect disposable fixtures only.

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_cache_registry_acceptance.py test_cache_registry_schema.py test_cache_inventory.py test_cache_coverage.py
```

Expected: 107 tests pass with no skips. See [exact fixture, results and residual
limitations](../docs/storage/25-cache-registry-acceptance.md). The previously
reproduced sandbox socket EPERM in the real DDP test remains documented separately.

## Phase 7.1 staging lifecycle metadata

`test_staging_lifecycle.py` has 20 isolated local migration/state/fencing cases.
NFS reservation prerequisites are synthetic fixture rows; actual lease APIs,
job-store RUNNING rejection and unchanged legacy run/completion are exercised.
No hybrid dispatch, real workspace verification, remote staging or training claim.

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_staging_lifecycle.py
```

Expected: 20 pass. Final selected regression: 590 tests, 28/28 files, no failures
or skips. See [exact contracts and limitations](../docs/storage/26-staging-lifecycle.md).

## Phase 8.4 hybrid checkpoint faults and native resume

`test_hybrid_checkpoint_faults.py` adds 16 isolated cases using real local SQLite,
protected files and signed Flask clients. It injects connection loss, partial
transfers, corrupt/missing files, ENOSPC/NFS errors and restart gaps; races two
independent SQLite connections and SIGKILLs only its own disposable publisher
subprocess. The outer training launcher is mocked. No Pi or live mount is touched.

`test_hybrid_checkpoint_ddp_resume.py` adds a real three-rank CPU Gloo experiment:
complete cut -> authenticated COMMITTED publication -> owned rank SIGKILL ->
reopened controller -> authenticated private restore -> native SDK resume ->
verified final commit/completion. Agents/isolation and result transport are
fixtures. Private FileStore rendezvous and loopback Gloo sockets are required;
a socket-restricted sandbox needs reviewed local test permission, not changes to
host firewall/services. All children are reaped. No Internet runtime transfer.

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_hybrid_checkpoint_faults.py
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_hybrid_checkpoint_ddp_resume.py
```

Expected: 16 + 1 PASS, no skips. Saved model/optimizer/scheduler/Torch RNG loads
are exact; final RNG/progress/application state remain exact. Final floating
model/optimizer tensors use atol=1e-8, rtol=1e-7; all other comparisons are exact.
See [8.4 report and measured differences](../docs/storage/33-checkpoint-fault-acceptance.md).
Existing exact single/two-rank tests remain unchanged. Physical pi01/pi02/pi03
validation, real NFS outages and production isolation remain pending.

## Phase 9.2 bounded DDP staging

`test_ddp_staging.py` exercises two/three disposable authenticated agents, real
local cache/private copies and a mocked trainer/isolation. Barriers prove overlap
and the configured limit without timing/throughput assertions. Tests cover full
input plans, rank-bound out-of-order replies, the all-ready/live-lease barrier,
revoked access, changed reservations, cancellation, missing data, in-flight copy
abort, lost READY replies, offline cleanup and partial launch failure. Controller
SQLite keeps its default thread guard; only RPCs run in the staging pool.

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_ddp_staging.py test_staging_dispatch.py test_staging_recovery.py test_hybrid_checkpoint_recovery.py
```

Expected: 16 + 25 + 34 + 18 tests pass. No listener, Pi, mount or service changes.
See [9.2 implementation, verification and rollback](../docs/storage/35-ddp-staging.md).

## Phase 9.3 one committed manifest per distributed save

`test_ddp_checkpoint_publication.py` uses disposable local stores/SQLite and the
existing authenticated checkpoint route. Fifteen tests cover complete rank
members/hashes/ACKs, corrupt/partial/mixed saves, all-rank fences, canonical save
identity under fresh transport IDs, independent concurrent collectors, stale
attempts, progress/completion monotonicity and interrupted orphan retention.
Opaque tensor members test the privileged boundary without deserialization.

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_ddp_checkpoint_publication.py test_checkpoint_publication.py test_hybrid_checkpoint_faults.py test_hybrid_checkpoint_recovery.py
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_group_checkpoint.py test_group_recovery.py test_hybrid_checkpoint_ddp_resume.py test_storage_config.py
```

Expected: 74 + 33 tests pass. Native Gloo regressions need reviewed localhost
socket permission. They preserve the existing writer and v2 format; production
pi01/pi02/pi03 and live NFS validation remain pending. See the
[9.3 report](../docs/storage/36-ddp-checkpoint-publication.md).

## Phase 9.4 DDP fault/acceptance matrix

`test_ddp_acceptance.py` adds six cases: native two-rank committed full-group
resume; three-rank startup death/cold retry; rank death during checkpoint ACK
followed by full-group committed resume; actual 3-to-2 SDK resume rejection;
controller world-size drift rejection; and competing whole-group recovery.
Native cases preserve a separate student's disposable job/files and reap all
owned child processes before asserting quiescence. They verify terminal rank
history as well as jobs, checkpoints, reservations and existing numerical limits.

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_ddp_acceptance.py
```

Expected: six PASS with reviewed localhost socket access. See the
[9.4 report and complete regression commands](../docs/storage/37-ddp-acceptance.md).
Local PASS does not satisfy physical three-Pi acceptance. Read-only readiness
found pi01 unreachable and the reachable agents missing the hybrid modules;
no deployment, live rank kill, service or mount change was performed.

## Phase 10.1 resource scheduler observations

`test_scheduler_metrics.py` checks 25 frozen pre-edit resource decisions for both
single workers and compatible DDP groups, default-OFF/disabled/enabled observation,
probe order, exact ties, bounded metrics, failures and uncertain reservations.

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_scheduler_metrics.py test_resource_allocation.py test_queue_controls.py test_storage_config.py
```

Expected: 43 PASS, no physical worker or NFS access. The observer is OFF by default;
this subphase does not enable locality. See the [10.1 report](../docs/storage/38-resource-scheduler-metrics.md)
for the separate existing native localhost DDP and mocked staging regressions.

## Phase 10.2 verified locality tie breaker

`test_scheduler_locality.py` checks optional placement tie breaking with mocked
resources plus real authenticated inventories, immutable input registration and
disposable SQLite/caches. It verifies resource priority, conservative stale/error
fallback, group resource profiles, per-rank full-input bytes and freshness metadata.

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_scheduler_locality.py test_scheduler_metrics.py test_resource_allocation.py test_queue_controls.py test_storage_config.py
```

Expected: 56 PASS across five files, without Pi/NFS access. Policy activation is
still gated; this adds explicit `locality=` to the existing selectors only.
See the [10.2 report](../docs/storage/39-locality-tie-breaker.md) for the complete
148-test coverage/inventory/staging/native-DDP regression commands and rollback.

## Phase 10.3 exact locality and fair-queue acceptance

`test_scheduler_locality_acceptance.py` adds five tests. The exact decimal-MB
fixture publishes real 500 MB/2 MB artifacts and three signed, separately verified
local cache inventories. It writes about 1.504 GB plus metadata, needs at least
1.638 GB free temporary disk, and skips rather than scaling down if unavailable.
The other four tests simulate fair turns, continued warm arrivals, infeasible
heads, uncertain/cancelling quotas and atomic DDP reservations in disposable SQLite.

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_scheduler_locality_acceptance.py
```

Expected: five PASS, zero skips when sufficient space exists, exact missing bytes
0/2,000,000/502,000,000 and DDP totals 2,000,000/504,000,000. No listener, Pi,
mount or trainer is used by this new file. Runtime policy activation remains off.
See the [10.3 report](../docs/storage/40-locality-acceptance.md) for exact scope,
115-test regression commands, evidence and rollback.

## Phase 10.4 configured policy and choice logs

`test_scheduler_policy.py` adds seventeen checks for resource/locality settings,
choice contents/bounds, stale/error fallback, immediate resource rollback, real
signed single/three-rank staging with fake mounts/launch, and checkpoint resume.
A seeded 72-scenario matrix repeats 432 selector calls. A greedy two-job
counterexample demonstrates that lower bytes for each immediate placement do
not guarantee lower whole-workload bytes; rollback restores resource behavior.

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_scheduler_policy.py test_queue_controls.py test_storage_config.py
```

Expected: 44 PASS, no physical worker access. This file uses only small local
inputs. The separate 10.3 exact-MB suite retains its temporary-disk requirement.
The [10.4 report](../docs/storage/41-scheduler-policy.md) records all 191 distinct
latest passing regression tests, a concurrent-run staging rejection and its
isolated/serial checks, precise config/logging examples and source rollback.

## Phase 11.1 optional transfer observations

`test_transfer_metrics.py` adds seventeen checks for median/MAD units, rolling
count/age, unknown/stale observations, threading, actual verified-copy measurement,
warm-hit exclusion, failure/timeout/hash rejection and fail-soft telemetry. Mount
facts are mocked; copied payloads are real local files. No Pi/network probe or
speed benchmark is run.

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_transfer_metrics.py test_cache_fetch.py test_worker_cache.py
```

Expected: 83 PASS, zero skips, no live data access. Resource/locality remain
unchanged and bandwidth runtime is still unavailable. See the
[11.1 report](../docs/storage/42-transfer-observations.md) and its evidence for
the broader selected staging/queue/policy regressions and scoped rollback.

## Phase 11.2 optional measured-copy policy

`test_bandwidth_policy.py` checks decomposed copy/overhead arithmetic, authenticated
inventory v1/v2, stale/replayed/forged/missing observations, disk uncertainty,
resource-equivalent bandwidth choices, 2/3-rank bounded staging models, signed
single/three-rank local dispatch barriers, log bounds and resource rollback.
Calibration rates in policy fixtures are synthetic, not Pi benchmarks.

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_bandwidth_policy.py
```

Expected: 22 PASS, no hosts contacted. Ordinary startup remains gated; inactive
`nfs-bandwidth.json` validates only with `--runtime staging`. Resource/locality
remain independent of optional rate collection. The [11.2 report](../docs/storage/43-bandwidth-policy.md)
records the selected broader regressions, model limitations and scoped rollback.

## Phase 11.3 dry-run-first cache eviction

`test_cache_eviction.py` checks readonly previews, deterministic bounded LRU,
quota/reserve, pin counts and activity exclusion, actual fetch-to-pin protection,
retained orphan producers, unsafe entries/root replacement, real process crashes,
full-disk/unlink/notification errors, nondeleting reconciliation, untouched
source/checkpoint/unknown files, signed registry FULL/CACHE_REMOVED and authorized
refetch. All eviction/crash tests use disposable local caches, not worker hosts.

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_cache_eviction.py
```

Expected: 23 PASS, no hosts contacted or live data changed. Maintenance is explicit
and disabled by omission; it never bypasses activity/pins or automatically frees
space for a job. See the [11.3 report](../docs/storage/44-cache-eviction.md) for the
selected broader regression commands, ownership/rollout limits and rollback.

## Phase 11.4 combined efficiency acceptance

`test_storage_efficiency_acceptance.py` adds ten combined tests for signed
changing/old/missing rate observations with actual job coverage, bandwidth queue
fairness, bounded three-rank staging and pin exclusion, capacity refusal, process
crash → signed registry reconciliation → authorized missing-only restage, and
resource/locality rollback. Synthetic rates and disk errors are fixtures; these
are not physical Pi throughput or power-loss tests.

```bash
PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_storage_efficiency_acceptance.py
```

Expected: **10 PASS**, no live host/data/service changes. The broader 20-file run
passed **428 distinct tests**, including exact-MB locality and CPU saved-state
resume. It requires about 1.638 GB temporary free space for the separate exact-MB
suite. See the [11.4 report](../docs/storage/45-efficiency-acceptance.md) and its
evidence for commands, initial corrected fixture mistakes, limitations and scoped
rollback. Runtime code/config are unchanged in 11.4; legacy/OFF/resource remains
active. Physical validation on pi01, pi02 and pi03 remains pending.

## Two-worker offline deployment preparation

The user now explicitly defers pi01 and continues with pi02/pi03.
`test_deployment_review.py` adds six offline source-bundle checks: role contents,
node selection, deterministic real member/archive hashes, existing output/source
link safety, excluded runtime/key stores, changed-source abort and no controller/
SQLite import or socket requirement.

```bash
PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_deployment_review.py test_cluster_setup.py test_storage_config.py test_bandwidth_policy.py test_cache_eviction.py
```

Expected: **76 PASS / five files**, no hosts contacted by these tests. The separate
read-only two-host readiness and source artifacts are recorded in the
[preparation report](../docs/storage/47-two-worker-deployment-preparation.md).
An offline review bundle is not an installed or isolated hybrid runtime.

## Phase 11 completed regression and two-Pi acceptance

The explicit full-phase approval was exercised on pi02/pi03, with pi01 deferred.
All 56 pre-existing test scripts passed. Ten schema cases skipped by the venv
were rerun with the installed system Python/jsonschema, all 15 cases in that file
passing. The new `test_phase11_probe.py` adds four local rehearsal/security/default
plan tests. Latest aggregate: **946 unittest cases plus one standalone checkpoint
script PASS, 57 files, zero remaining failures/errors/skips**; reruns not doubled.

```bash
PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated
PYTHONDONTWRITEBYTECODE=1 python3 tools/run_tests.py --isolated test_storage_manifest_drafts.py
PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_phase11_probe.py
```

The full suite needs localhost sockets for native CPU/Gloo tests and sufficient
temporary space for exact-MB acceptance. The first command alone can still skip
schema cases if jsonschema is absent in that venv; the second uses the existing
documented system interpreter, without installing any dependency.

Separate explicitly approved hardware checks passed for real NFS copies, signed
controller registry, warm zero bytes, corruption/concurrency/pins/quota, isolated
UID65534 negative tests, eviction crash/refetch and unchanged production agents.
No physical three-rank or production student-job deployment is claimed. Exact
commands, synthetic-vs-physical scope, logs and rollback are in the
[48 completion report](../docs/storage/48-phase11-live-acceptance.md).
## Structured event tests — Phase 12.1

```bash
PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_storage_events.py
```

Tests the actual JSON log contract, strict field/numeric validation, error
redaction, disabled/failed logging, thread-local attribution, bounded monotonic
recovery clocks, real signed cold/warm/private-workspace staging, failed/fenced
preparation, three-rank mock transports, immutable commit idempotency, scheduler
resource/locality facts and SDK-gated recovery/restart. Disposable copied DBs and
local cache files only; fake NFS/trainer/isolation, no hardware acceptance.
See [12.1 results and scope](../docs/storage/49-structured-events.md).

## Admin storage view — Phase 12.2

`test_admin_storage.py` adds sixteen isolated checks for operator-only roles,
401/403 and immediate revoke, read-only legacy polls, owner-safe aggregates,
signed v3/old inventory, offline/stale/replay handling, unique/zero-byte dataset
coverage, bounded snapshots/windows and genuine existing staging history. It
executes `test_admin_storage_ui.cjs` against the actual portal JavaScript.
Node is a development-only requirement for that harness; no npm packages or
network access are needed. Set `DL_NODE_BINARY` to an installed Node executable
when `node` is not on PATH. The app's already-installed runtime used here:

```bash
DL_NODE_BINARY=/usr/lib/chatgpt/resources/cua_node/bin/node PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_admin_storage.py
/usr/lib/chatgpt/resources/cua_node/bin/node tests/test_staging_portal_ui.cjs
```

On other machines use your actual Node path. Final distinct selected regression:
155 tests across seven files PASS, no failures/errors/skips. Also visually checked
an in-app-browser synthetic panel preview; it is not live Pi acceptance. Exact
commands/results and source rollback are in the [12.2 report](../docs/storage/50-admin-storage-observability.md).

## Student recovery progress — Phase 12.3

```bash
DL_NODE_BINARY=/usr/lib/chatgpt/resources/cua_node/bin/node PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_recovery_portal.py
```

Use your installed Node path if different, or `node` on PATH. Fifteen tests use
actual owner-authorized Flask routes, disposable SQLite and existing signed
staging/checkpoint/recovery fixtures. Checks cover real milestone transitions,
three-rank SDK barrier, cold/full restart, controller restart, expired ownership,
stale attempt/snapshot exclusion, readonly polling, legacy compatibility and
cross-student/admin denial. Runs the actual new recovery JS harness and both
existing staging/admin JS harnesses, including logout-response races/escaping.
No npm, Internet, listener, NFS mount or Pi needed; training/isolation are mocked.

Latest selected regression: **127 distinct tests PASS across seven files**,
zero failures/errors/skips, with reruns not doubled. Synthetic desktop panel
preview also visually checked. Commands, evidence, limits and scoped rollback:
[12.3 report](../docs/storage/51-student-recovery-progress.md).

## Observability acceptance — Phase 12.4

`test_observability_acceptance.py` exercises real isolated Flask/SQLite/signed
inventory fixtures: owner/admin/anonymous permissions, 150 read-only polls,
100 authenticated unchanged heartbeats, bounded history/snapshot/rotation,
query interruption, corrupt/stale/replayed observations, exact JSONL units and
deduplication. It executes the actual portal JS in UTC, Asia/Dhaka and New York
(including repeated DST hour), plus all three existing storage UI harnesses.
No live workers, network listener, system service, mount or production DB needed.

```bash
DL_NODE_BINARY=/usr/lib/chatgpt/resources/cua_node/bin/node PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_observability_acceptance.py
PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/benchmarks/observability_overhead.py
```

The Node path is this machine's existing runtime; use your installed Node path
elsewhere. The second command emits a synthetic local microbenchmark as JSON on
stdout, never imports the controller and enables logging only in that process.
See [12.4 results, scope and rollback](../docs/storage/52-observability-acceptance.md).
