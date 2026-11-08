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
