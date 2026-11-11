# Hybrid storage components

This folder contains the new storage-specific code. Phase 1.2 provides file
interfaces/adapters; Phase 1.4 adds configuration validation and startup guards.
The file adapters are not used by live upload/staging/checkpoint paths. This
package import does not select a runtime backend, initialize SQLite, install a
service or mount NFS. Explicit administrator setup and fault-probe entry points
are separate from runtime execution. Existing uploads, preparation, checkpoints and scheduling retain
their current implementations.

See the [architecture](../docs/storage/01-hybrid-architecture.md) and
[implementation progress](../docs/storage/implementation-progress.md).

## Optional transfer observations — Phase 11.1

[`transfer_metrics.py`](transfer_metrics.py) provides explicitly constructed
`TransferMetrics(configured_worker_id)`, passed through `nfs_cache_fetcher` as
`transfer_metrics=`. It measures actual successful NFS-to-verified-local payload
copies, with bounded rolling median MiB/s, variability, sample count and age.
Warm hits and failed/empty copies are not successful rate samples. No probes,
persistence, HTTP telemetry or scheduling activation are added; omission preserves
the existing path. Cross-worker contention remains unknown, and rates include
verification/fsync/helper overhead rather than measuring wire speed. See the
[11.1 report](../docs/storage/42-transfer-observations.md) for scope and rollback.

## Locality tie breaking — Phase 10.2

[`locality.py`](locality.py) supplies trusted controller `JobLocality` preferences
to the existing single-worker/DDP selectors via optional `locality=`. It reuses
`CoverageAPI.for_job_workers` for a locked read snapshot of authorized immutable
inputs and current authenticated inventories. CPU/RAM feasibility and ordering,
compatible complete groups, quotas, reservations and lease/staging gates remain
authoritative. Unknown or stale inventory restores resource-only choice.

The immutable preference snapshot exposes verified missing bytes, inventory age,
session/sequence and fallback reason without filenames or credentials. Every
current DDP rank requires full inputs; this code invents no sample/shard cache.
Configuration/dispatcher activation remains gated until 10.4. See the
[10.2 report](../docs/storage/39-locality-tie-breaker.md) for API contracts, tests,
limits and source rollback. Legacy and resource defaults remain unchanged.

## Configured scheduler policy — Phase 10.4

[`scheduling.py`](scheduling.py) now wires validated resource/locality settings
through the existing trusted `StagingDispatch` and `CheckpointDispatch` selectors.
It uses the actual authorized workspace plans and optional same-registry
`CoverageAPI`. Locality emits bounded, structured operator choice records including
filtered workers, missing bytes, freshness, requested/effective policy and reason.
Default resource calls retain the original selector path without inventory reads
or automatic tracing. An explicit logger can also trace resource selections.

`runtime.set_scheduler_policy('resource')` restores the original policy for future
placements without modifying running jobs, bindings, cache, leases or SQLite.
Config examples and read-only validation are documented
[here](../config/storage-examples/README.md#trusted-coordinator-policy-settings--phase-104).
Ordinary unconfigured dispatcher/portal startup still cannot bootstrap hybrid
execution; no live config, deployment or service was changed. Bandwidth is now
an optional trusted component policy described below. Locality is a greedy tie breaker and can increase aggregate bytes for
some job sequences; resource stays the active default. See the
[10.4 report](../docs/storage/41-scheduler-policy.md) for simulations, actual test
history, operational logging, limits and rollback.

## Optional bandwidth policy — Phase 11.2

[`bandwidth.py`](bandwidth.py) uses decomposed verified-copy observations from
`TransferMetrics.prediction_snapshot()` and authenticated inventory report v2.
The reporter and controller receiver are separately opt-in; default report v1 and
resource/locality paths remain independent. Stale, absent or incomplete facts
restore resource ordering. No new endpoint, DB table, mount or speed test.

Existing CPU/RAM profiles remain primary for single workers and complete DDP
groups. The estimated missing-copy time adds measured residual per-object overhead
to missing bytes divided by measured copy rate. Logs expose uncertainty, age/count,
disk limits and bounded-parallel group contention assumptions; training completion
is not modeled. Trusted bootstrap instructions and unchanged immediate resource
rollback are in the [config examples](../config/storage-examples/README.md) and
[11.2 report](../docs/storage/43-bandwidth-policy.md). Collection and bandwidth
selection are optional and not prerequisites for production stability.

## Input-file API

`StorageBackend` is a typed, runtime-checkable `typing.Protocol`, exported from
`hybrid_storage` along with `LegacyStorageBackend` and `NFSStorageBackend`.
Implementations use Python's standard library and Linux filesystem operations;
no new transport or third-party dependency is introduced.

| Method | Contract |
|---|---|
| `artifact_exists(key: str) -> bool` | True for an accessible regular file; False for a missing file/parent. Invalid keys, links, special files, permission errors and I/O outages raise rather than masquerading as misses. |
| `resolve_path(key: str) -> Path` | Check the existing file and return its configured path for diagnostics/integration. The returned path can become stale; it is not an authorization grant or a safe-open token. |
| `open_artifact(key: str) -> ContextManager[BinaryIO]` | Open read-only, anchor all path components with no-follow directory descriptors, reject nonregular files, close on context exit. Caller must still verify authorized size/digest at staging time. |
| `publish_artifact(key, source: BinaryIO, *, expected_sha256: str, expected_size: int) -> Path` | Trusted-controller-only capability: bounded copy, verify bytes, flush, publish a new name without overwriting. Read-only adapters reject it. Does not commit a manifest, DB record or recoverable checkpoint. |

Keys must be canonical relative POSIX file paths. Reject absolute paths, `..`,
empty/dot components, backslashes, colons and NULs. All configured roots must be
explicit absolute paths other than `/`; no path component may be a symlink.
Roots and destination parent directories must already exist. Constructors do not
create or inspect filesystem state; filesystem validation happens at access time.

## Local and NFS roles

`LegacyStorageBackend(root)` gives a local read view, including files in an
existing managed workspace. It is not a replacement for `job_storage.stage_job`,
nor does it interpret current manifests. Publication requires the explicitly
configured `staging_root` capability. Use only a dedicated input root for new
publication, never checkpoints or a broad runtime directory.

`LegacyStorageBackend(input_root, staging_root=private_root)` is the trusted
controller's local publication view of its input export. The private staging
directory must be owned by the current service UID, owner-only (0700), outside
the input root, not an ancestor of it, on the same filesystem, and not exported
through another NFS path. Target parents must be provisioned by the trusted
service. SHA-256 must be lowercase hex; expected size must be a nonnegative int.

Publication copies to a new private temporary file, verifies exact size/digest,
sets mode 0444 and fsyncs before creating the final name with an exclusive
same-filesystem link. It then fsyncs the destination directory and removes its
own temporary name. It never links an existing student/cache input directly into
the store. Existing targets, including symlinks, raise `FileExistsError` and are
never replaced. The caller retains ownership of the source stream.

Wrong bytes, oversized inputs and precommit failures leave no published entry;
cleanup touches only the temporary entry created by that call. Process/power loss
can leave private temporary files for later reconciliation. A directory-fsync
failure after final linking can leave a complete file visible despite an exception:
reconcile by expected identity before retrying, never blindly overwrite. Durable
manifest registration and crash recovery are later subphases. Publication is
bytes-only; future workspace materialization applies authorized executable modes.

`NFSStorageBackend(mounted_input_root)` uses the same safe read contract on an
already mounted input export. It **always rejects publication**, even when the
underlying directory is writable. Controller publication goes through its local
adapter, not a worker RW NFS mount. Administrators must verify that the configured
controller export and worker mount represent the intended input store; the adapter
does not infer that mapping, attest read-only mount/export flags or perform mounts.

## Security and availability limits

Call only from trusted services after authorizing a job/owner/attempt's exact
input selection. A relative key or hash is not authorization. These adapters do
not implement student authentication, privilege separation, private workspaces,
cache inventory, lease fencing or filesystem sandboxing. Protected root/parent
ownership and no untrusted writers are prerequisites, including protection from
renames, bind-mount aliases and ACL-based access. Mode 0444 is defense in depth;
it cannot stop a privileged owner from modifying a file. The NFS mount and its
network endpoint must remain inaccessible to student code.

Reads are synchronous filesystem operations. `ETIMEDOUT`, `EIO`, `ESTALE` and
permission errors propagate; no retry, implicit legacy fallback or remote fetch
is attempted. Kernel-blocked NFS I/O requires the supervised staging design
described in the architecture before enabling the new mode. No upload/worker
network protocol or checkpoint format is changed here.

## Safe local verification

From the application root:

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py test_storage_backends.py test_job_storage.py
```

Expected: 24 adapter tests and 9 existing storage tests pass, two files total.
All data is temporary. NFS is represented by a local directory and mocked I/O
failures; these tests do not prove live NFS, authentication or Pi readiness.

## Phase 1.4 configuration boundary

[config.py](config.py) now validates the operator's storage flags. The dispatcher
and student portal validate them at startup; defaults keep the existing paths.
This does not wire the file adapters into those paths. Non-default settings are
recognized but were rejected by the Phase 1.4 runtime gate. Phase 3.3's limited
controller metadata mode is described below; security/staging/registry/scheduler
execution remains gated. See the historical
[configuration guide](../docs/storage/03-configuration.md) for exact defaults,
supported combinations, limitations and safe verification commands.

## Phase 2.2 server preparation

[nfs_server.py](nfs_server.py) prepares a restricted controller NFS plan and an
idempotent, backup-first configuration-staging operation for later approval.
The default/`--dry-run` command is read-only. No caller enables NFS or invokes
package/service/export activation. The original controller plan was blocked on
missing pi01/publisher details and NFS packages; see the
[server setup guide](../docs/storage/05-nfs-server-setup.md). Application storage
flags remain unchanged and legacy-only.

## Phase 2.3 client preparation

[nfs_client.py](nfs_client.py) produces offline plans for all three Pis, with
future conditional offline package provisioning and backup-first idempotent
configuration staging. [nfs_mount.py](nfs_mount.py) is a standalone kernel mount
metadata guard: ordinary directories/autofs-only triggers are not ready inputs.
Generated units keep the legacy agent independent of the on-demand mount.
The planner does not activate units or wire them into live job staging. Authorization,
process/network isolation, supervised I/O and all-three-Pi validation still gate
hybrid enablement. See the [client guide](../docs/storage/06-nfs-client-setup.md).

The user's 2.4 scope revision selects pi02/pi03 through optional infrastructure
`deployment_nodes`; omission still requires all three. Excluded nodes are not
exported, staged or inspected by the client planner. See the
[original two-node deployment status](../docs/storage/08-nfs-two-node-deployment.md).
That authentication blocker was resolved through normal desktop administrator
authentication; [Phase 2 results](../docs/storage/10-phase2-completion.md) record
the actual server/client deployment and isolated outage tests on pi02/pi03.

## Administrator-only fault validation

[nfs_fault_probe.py](nfs_fault_probe.py) is a disposable public-TEST fixture,
never a training/runtime component. Its default invocation prints a plan with
`mutations=false`. Explicit live use requires root, private mount/network/hostname
namespaces and a protected Unix-socket gateway. It interrupts only its own relay,
tracks pending readers/mount producers and restores transport before cleanup.
Keep failed fixtures available for reconciliation; never force-unmount a pending
hard NFS reader or disrupt a host interface. Exact driver sources, failed-attempt
cleanup and both successful live results are retained in the Phase 2 evidence.

The real read-only automount and mount guard are now installed on pi02/pi03.
Only public synthetic artifacts were published. Application flags remain legacy/
off; authenticated staging, supervised I/O and execution/network isolation are
still required before student data can use the new backend.

## Phase 3.1 strict input views

[inputs.py](inputs.py) adds `LocalObjectStorageBackend` and
`NFSObjectStorageBackend` around the existing adapters. They accept only canonical
`sha256/<prefix>/<digest>` keys. `InputFile` and `ProjectInputView` bind approved
relative project paths to expected identities, rejecting unlisted paths before
disk access. Publication remains a trusted controller-local capability; worker
and project views are read-only. Existing general-key adapters remain compatible.

The explicit [controller seam](../controller/input_storage.py) has no runtime
callers, DB side effects or config selection. See the historical
[3.1 contracts, tests and rollback](../docs/storage/11-controller-input-adapters.md)
and the stricter current admission below.

## Phase 3.2 health and mount admission

Strict NFS object reads now require an explicit expected source, exact RO mount
and matching opened-FD mount ID. Ordinary directories and autofs-only triggers
are rejected. Strict local publication attests actual parent/staging FDs as ext4,
xfs or btrfs before writing. Existing general-key legacy contracts remain intact.

[health.py](health.py) adds full expected size/SHA verification and a read-only
child supervisor with bounded waits, retained pending ownership, capacity refusal
and late-result fencing. Raw NFS reads remain synchronous; use the supervisor for
probes. Keep it alive until its owned processes are reaped. No durable daemon
restart journal or runtime service is installed; hybrid flags still stay off.
See [3.2 behavior, tests and limits](../docs/storage/12-input-health.md).

## Phase 3.3 pinned submission selection

The controller now distinguishes a new job's selected backend from executable
storage. Its additive [binding helper](../controller/storage_selection.py) pins
selection in local SQLite; missing old bindings always mean legacy. Optional
config/API/CLI NFS intent retains the existing local upload snapshot and holds
the job. Release, preparation and single/DDP placement stay blocked for NFS.
An explicit legacy job override still uses the existing path. Current config
remains legacy/off/resource; no NFS execution, automatic migration or copy was
enabled. See [3.3 interfaces, reference plan and rollback](../docs/storage/13-storage-selection.md).

## Phase 4.1 explicit artifact registration

[artifacts.py](artifacts.py) adds `ArtifactRegistrar(root, staging_root=private)`
for trusted controller callers. `register(binary_stream, expected_size=exact_size,
expected_sha256=digest_or_none)` returns a frozen `Artifact(sha256, size_bytes,
key, created)`. It computes canonical SHA keys, streams/copies/readback-verifies
with bounded buffers, then atomically renames a private 0444 inode without
replacement using Linux `renameat2(RENAME_NOREPLACE)`. A duplicate is fully
verified and byte-compared before reuse; mismatched content raises
`ArtifactMismatch`, never overwrites. Roots/scratch must be pre-provisioned,
protected, service-owned and on a supported local filesystem. Only derived
SHA directory names are created. No changes to the existing publication API.

This is an unwired component, not an authorization grant or student route.
Dataset ACLs/manifests, job registry and lazy legacy enrollment are later
subphases. No runtime flags or live data change. See [4.1 usage, tests, crash
semantics and rollback](../docs/storage/15-artifact-registration.md).

## Phase 4.2 dataset manifests and access contract

[dataset_manifests.py](dataset_manifests.py) defines frozen `DatasetEntry`,
`DatasetManifest` and current-authority `DatasetACL` snapshots. `create_dataset`
issues an opaque dataset ID and private default ACL; `next_version` requires
the owner and links the prior manifest hash. Strict canonical JSON and SHA-256
identify immutable version bytes. File paths/types/sizes/hashes, parent entries,
case/Unicode conflicts and parser bounds are validated. Read-only shares remain
separate from manifest content so current-ACL revocation applies to old versions.

[dataset_validation.py](dataset_validation.py) supplies read-only `inventory_tree`
with no-follow FDs and file hashing, plus metadata-only `validate_zip_members`.
It never extracts archives or registers/moves legacy data. Source protection and
later actual-byte verification are required; inspection is not an atomic snapshot.

These components have no runtime call sites or DB authority. Retrieve the current
ACL and exact committed version/digest from trusted controller storage before
access; parsed documents, public constructors and known hashes grant nothing.
Persistence, unique version transactions and job bindings are 4.3. See the
[4.2 contract, tests and rollback](../docs/storage/16-dataset-manifests.md).

## Phase 4.3 persisted registry and job inputs

[registry_schema.py](registry_schema.py) provides explicit `migration_plan(db)`
and `migrate(db)` on a dedicated trusted local FULL/FK connection. Migration is
additive, transactional, shape/checksum guarded and never runs on import/startup.
[registry.py](registry.py) provides verified per-owner artifact/source enrollment,
current persisted dataset ACLs, immutable versions with head compare-and-swap,
and exact job source/data/requirements/legacy/spec bindings. A known digest or
parsed client manifest cannot grant access. New byte ownership requires an actual
verified stream; physical dedup does not share ownership.

[input_manifests.py](input_manifests.py) retains source-tree v1 executable metadata
and defines `JobInputs`. Binding requires a DB-pinned matching local snapshot and
an explicitly selected HELD NFS job; it changes no job state or checkpoint format.
Old unbound jobs remain legacy. No live DB was migrated, runtime flag enabled or
upload/staging path wired. See [4.3 API, DDL, tests and rollback](../docs/storage/17-registry-and-job-inputs.md).

## Phase 4.4 explicit legacy registration

[legacy_registration.py](legacy_registration.py) registers only one requested,
owner-authorized, committed local snapshot. The [controller facade](../controller/input_storage.py)
requires the existing `verify_workspace` callback explicitly, so imports never
initialize SQLite. It validates controlled roots and pinned metadata, copies
declared files through verified CAS publication, then atomically commits source,
private dataset and immutable job references. Exact retries/concurrent requests
reuse one sidecar; no existing files are moved, linked, deleted or repaired.

Legacy execution/backend/recovery identity remains unchanged; NFS intents stay
HELD. There is no automatic UI/startup scan or flag activation. The existing 4.3
schema/checksum is unchanged. See [4.4 contracts, tests, rollback and Phase 4
local acceptance](../docs/storage/18-legacy-registration-and-phase4.md).

## Phase 5.1 persistent local worker cache

[worker_cache.py](worker_cache.py) adds explicit protected local-disk metadata,
bounded full-object verification, verification/use times, exact-attempt pins and
current disk accounting. [The worker seam](../agent/cache_storage.py) constructs
an inactive handle without importing the legacy HTTP launcher. Initialization
is explicit; bytes and pins persist across process restart. Stage-time reads
always hash the same opened FD. Inventory is advisory and never grants access.

No fetching, repair, eviction, workspace creation, lifecycle wiring or feature
activation is introduced. Existing same-UID training is not a safe cache trust
boundary; the runtime gate stays closed. See [5.1 contracts, 33 new tests,
limitations and rollback](../docs/storage/19-worker-cache.md).

## Phase 5.2 explicit missing-only NFS cache fetch

[cache_fetch.py](cache_fetch.py) adds exact-input authorization callbacks,
verified-hit reuse, quota/free-space admission, serialized cross-process NFS
fetches and atomic publication. [cache_copy_worker.py](cache_copy_worker.py)
copies only through the guarded read-only NFS adapter into an inherited private
part FD; parent publication requires reaping, verification and renewed authority.
Timeouts retain ownership/capacity, and late readiness never publishes. Unpinned
corruption is preserved in quarantine before repair; pinned replicas fail closed.

The appended worker factory is inactive: no HTTP/API transport, startup wiring,
flag activation or new SQLite schema. Private workspaces and lifecycle wiring
remain 5.3. See [5.2 contracts, evidence and rollback](../docs/storage/20-cache-fetch.md).

## Phase 5.3 private attempt copies and pin lifecycle

[workspaces.py](workspaces.py) builds protected private source/data copies from
verified pinned objects, preserves executable metadata and empty directories,
and separates outputs/checkpoint candidates. Full copies are the supported
fallback; no writable hardlinks, mounts or shared cache inodes. Durable exact
plan/root-bound records and holders survive process restart. Terminal cleanup
requires exact producer/guardian/descendant quiescence and retains recoverable
pins/markers when proof or metadata operations fail. No runtime-data deletion/GC.

The worker factory remains inactive, and same-UID legacy training is not a safe
isolation boundary. Config, checkpoint formats and SQLite schemas are unchanged.
See [5.3 APIs, 39 tests and rollback](../docs/storage/21-private-workspaces.md).

## Phase 6.1 controller-local advisory cache schema

[cache_registry_schema.py](cache_registry_schema.py) adds an explicit transactional
plan/apply/require migration after artifact-registry v1. Six new tables retain
worker auth metadata, cache high-water identities, inventory sessions, verified
entries and replay history, with seven indexes and twenty defensive triggers.
No connection, auto-enrollment, HTTP ingest or scheduler change on import/startup.
New workers/sessions default disabled/untrusted. Actual runtime worker HTTP
authentication is absent; 6.2 must establish it before accepting inventory.

Existing schema/config/source remain unchanged. See [6.1 contracts, actual auth
finding, 22 tests and rollback](../docs/storage/22-cache-registry-schema.md).

## Phase 6.2 authenticated cache inventory

[inventory_protocol.py](inventory_protocol.py), [inventory_reporter.py](inventory_reporter.py)
and [cache_inventory.py](cache_inventory.py) implement explicit per-worker HMAC
metadata reports over the existing HTTP agent: startup/reconnect full snapshots,
add/remove events, unchanged heartbeats, cache epochs, strict sequence/session
fencing, TTL/offline exclusion and controller-requested full reconciliation.
Actual operator registration and resource polling have optional tested adapters.
No auto-enrollment, key provisioning, schema migration or runtime activation.
The default endpoint is disabled; shared-UID legacy training is unsafe for keys,
so OS isolation remains a production enablement prerequisite. Existing execution
gates, config defaults, job leases and scheduling remain unchanged.
See [6.2 report, 38 new tests, evidence and rollback](../docs/storage/23-authenticated-cache-inventory.md).

## Phase 6.3 authorized dataset/job/rank coverage

[cache_coverage.py](cache_coverage.py) exposes controller-side `CoverageAPI` for
exact dataset versions or immutable job bindings. Count each digest once across
paths/source/data/requirements; return exact required/hit/missing bytes, missing
hashes, ratio and inventory age/eligibility. Current ACLs and the exact current
authenticated session are checked in one read snapshot. Missing/stale/offline
inventory contributes zero hits; unregistered legacy jobs remain unchanged and
return an explicit unavailable error to this API.

Each rank defaults to all source/data. Optional trusted whole-file dataset sets
are checked against the pinned job/rank/world size; no sample-level caching or
execution change. Same-UID isolation and runtime gates remain. See [6.3 API, 31
tests, evidence and rollback](../docs/storage/24-cache-coverage.md).

## Phase 6.4 local registry acceptance

`tests/test_cache_registry_acceptance.py` exercises initial/full and incremental
reports, real file corruption/deletion, process restart/SQLite crash, expired
execution ownership, stale/offline and reordered/duplicate reports, missing
observations, write contention and transactional error recovery. Real locally
published/verified files give pi01/pi02/pi03 fixture misses exactly 0/2/502 MB
(decimal MB). These are isolated local caches, not a live Pi cluster.

No runtime/API/schema/config changes. Legacy defaults and execution gates remain.
See [6.4 acceptance, commands, evidence and rollback](../docs/storage/25-cache-registry-acceptance.md).

## Phase 7.1 staging side state and readiness barrier

[staging_schema.py](staging_schema.py) and [staging_lifecycle.py](staging_lifecycle.py)
add explicit local migration and trusted metadata APIs. Keep jobs SCHEDULED
while side phase STAGING binds exact input/attempt/history/rank/reservation/lease
identity. Require all rank receipts and current fences for READY. The actual
job-store NFS RUNNING transition checks readiness and retains the separate
execution gate, so no hybrid job starts in this subphase. Default legacy paths
and website/API states remain unchanged. No automatic migration, RPC or cleanup.
See [7.1 APIs, 20 new tests and rollback](../docs/storage/26-staging-lifecycle.md).

## Phase 7.2 explicit authenticated dispatch/staging integration

`controller/staging_dispatch.py` connects existing resource selection, exclusive
reservations and leases to `agent/staging.py` on the existing HTTP channel.
`hybrid_storage/staging_protocol.py` authenticates bounded metadata with the
existing per-worker credential type and a separate HMAC domain. Artifact bytes
still come only from the read-only NFS backend. Private inputs are reverified at
launch and passed to the existing `custom_runner.py` as local paths.

This is explicitly injected integration, exercised with disposable databases,
fake NFS identity and mocked isolation/process launch. Normal controller/agent
bootstrap remains legacy. Flags alone cannot enable it. A reviewed OS/network
isolation launcher is mandatory; no same-UID fallback is supplied. New NFS
checkpoint jobs remain held pending trusted publisher integration; legacy
checkpoint APIs/formats remain unchanged. Stage duration is bounded by leases.
Terminal pin release, restart reconciliation, authenticated stop/renewal and
production activation remain gated on subsequent lifecycle work. See the
[7.2 report](../docs/storage/27-staging-dispatch.md) for exact scope and evidence.

## Phase 7.3 durable cancellation, expiry and recovery

`staging_journal.py` records worker copy/launch intent and terminal tombstones on
protected LOCAL disk. Signed `/jobs/staging-control` messages cancel, query or
renew the exact stage/plan/lease. A watchdog fences expired attempts. Restarted
workers never restore launch permission from a READY record. `CacheFetcher` can
interrupt producers and reconcile only owned scratch parts after proving the
cache-wide fetch lock is free. Private workspace pins release only after exact
producer/containment quiescence; published inputs/outputs/checkpoints are retained.

`cleanup_schema.py` provides a separate explicit LOCAL FULL/FK migration for
controller acknowledgments. `controller/staging_recovery.py` preserves complete
group reservations until quiescent acknowledgments or conservative lease fencing,
then closes the exact attempt atomically. Lease-fenced offline cleanup remains
tracked for reconciliation when a worker returns. Result collection and completion
recheck current attempt, all-rank leases/reservations and authorization; final
publication serializes with cancellation. Legacy flows remain the default.

Hybrid launch additionally requires reviewed termination and quiescence callbacks;
there is no PID-gone or missing-callback shortcut. Normal startup/config remain
gated; no live migration, credentials or deployment occurs. See the
[7.3 report](../docs/storage/28-staging-recovery.md) for safe local commands,
evidence, limits and rollback. UI work remains 7.4.

### Phase 7.4 student visibility (local acceptance)

The existing portal now projects owner/current-attempt staging side state and
numeric measurements into Input preparation and the Input staging log. Strict
`staging_metrics.py` validation excludes worker paths/text/credentials. The trusted
coordinator records one event with first READY in existing local SQLite;
`metrics.py` request observations exclude concurrent inventory work. Legacy
representations/defaults stay unchanged, and measurements never grant launch
authority. Missing data remains unavailable; DDP totals sum per-rank file copies,
not unique cluster-wide bytes or wire throughput.

See [7.4 report](../docs/storage/29-staging-visibility.md) and adjacent evidence for
files, APIs, safe local commands, results, manual verification and scoped rollback.
Explicit production bootstrap, isolation/containment and trusted NFS checkpoint
publication remain gated; no live deployment or all-three-Pi acceptance is claimed.

### Phase 8.2 trusted checkpoint publication (explicit, inactive)

`checkpoint_publication.py` supplies the typed trusted publication interface and
an explicit private LOCAL store. `checkpoint_schema.py` adds an opt-in FULL/FK
SQLite journal/head migration without changing earlier schemas or user_version.
Candidates are bound to original manifest plus immutable input version and live
all-rank fences; verified archives publish under new no-replace names, then
conditional COMMITTED/head/job metadata updates occur in one SQLite transaction.
Legacy latest files and SDK formats are untouched. Interrupted/rejected candidates
never become head automatically; immutable history is retained.

`checkpoint_transfer.py` and explicit `agent/checkpoint_service.py` reuse the
existing checkpoint GET route with per-worker HMAC request/receipt headers and
bounded streaming. Worker snapshots freeze local candidate bytes outside trainer
directories. No new artifact API, trainer NFS RW access, automatic migration,
default service activation or unsigned restore upload bypass is introduced.
Restore/committed-source scheduling remains 8.3; ordinary checkpoint-mode hybrid
execution stays gated. See [8.2 report](../docs/storage/31-checkpoint-publication.md)
and adjacent evidence for APIs, sample bootstrap, tests, risks and scoped rollback.

## 8.3 explicit committed checkpoint recovery

`controller/checkpoint_recovery.py::CheckpointDispatch` connects the explicit
publisher to existing resource placement, signed staging/control and SDK resume.
`checkpoint_restore.py` and `agent/checkpoint_restore_service.py` authenticate
restore on the existing HTTP route and materialize a verified independent private
checkpoint copy. `restore_schema.py` is an explicit additive LOCAL migration;
no automatic bootstrap/migration or config activation. The base restart-only
coordinator and legacy/default-OFF execution remain unchanged.

Recovery freezes an original-input-bound COMMITTED source, requires fresh leases,
restores all DDP ranks before any launch, waits for existing SDK restore proof,
and requires a current-attempt committed final checkpoint before completion.
See [8.3 report](../docs/storage/32-checkpoint-recovery.md) and its evidence for
actual tests, security boundaries, manual verification and scoped rollback.
Production deployment, full 8.4 fault testing and all-three-Pi validation remain
pending; the controller remains a single point of failure.

## 8.4 local checkpoint fault acceptance

Tests now cover interrupted publication/restore, stale results, competing
recovery, controller restart/SIGKILL, missing/corrupt checkpoints and simulated
NFS outage, plus real three-rank CPU Gloo resume through committed storage and
private worker copies. This subphase changes tests/docs only, with no runtime,
API, schema, config or bootstrap changes. Final selected checks: 96 unittest
cases plus the legacy executable regression script PASS. New cases: 16 fault
checks and one genuine three-rank test, with exact saved-state loading and the
stated final floating tolerance. See [8.4 report](../docs/storage/33-checkpoint-fault-acceptance.md)
and its evidence. Production enablement and physical all-three-Pi validation
remain pending; central controller/storage remains a SPOF.

## Explicit idle cache maintenance — Phase 11.3

[`cache_eviction.py`](cache_eviction.py) provides dry-run-first `CacheEvictor`,
constructed through `agent.cache_storage.cache_maintenance`. It reuses existing
fetcher quota/free-space reserve, plans deterministic LRU victims and requires an
explicit `apply(plan, dry_run=False)` on a locally issued, revalidated plan.
Nothing automatically evicts on startup, dispatch, failure or a timer.

Shared cache activity guards cover readers, fetch/materialization and the actual
agent's fetch-to-pin gap. Any persistent pin blocks all eviction. Only unchanged,
registered local replicas qualify; NFS, parts/quarantine, unknown files, private
workspaces, outputs/checkpoints and DB files are not deleted. Existing schema v1
retains durable missing observations before unlink; crash reconciliation verifies
actual files and never replays deletion or clears pins. The existing signed
inventory path reports removals or a requested full reconciliation.

All users of a shared cache must run guarded code before enabling maintenance.
See the [11.3 report](../docs/storage/44-cache-eviction.md) for exact ownership,
limits, manual preview, actual tests, residual risks and source/data rollback.
No live cleanup or deployment was performed.

## Offline hybrid deployment review

`deployment_review.py` builds source-only controller/worker review archives with
per-file and archive SHA-256 manifests, including `hybrid_storage` omitted by the
legacy installer bundle. It never uploads/extracts/installs or activates services:

```bash
.controller-venv/bin/python -B -m hybrid_storage.deployment_review --nodes pi02 pi03
# Optional new local output directory; existing paths are rejected.
.controller-venv/bin/python -B -m hybrid_storage.deployment_review --nodes pi02 pi03 --output /tmp/hybrid-review-new
```

The user currently defers pi01 and authorizes pi02/pi03 continuation. The manifest
records that scope and always reports `apply_ready=false`. It excludes runtime
data, active operator configs and credential stores; dependencies/isolation and
trusted bootstraps remain separate review gates. See the [preparation report](../docs/storage/47-two-worker-deployment-preparation.md).

## Approved Phase 11 hardware acceptance

`phase11_probe.py` is a trusted test bootstrap, driven by
[`tools/validate_phase11.py`](../tools/validate_phase11.py). Default invocation
prints a plan; explicit `--run` creates new private fixture state only and uses
the existing authenticated inventory protocol. It never replaces the production
agent or enables student execution. Approved pi02/pi03 tests passed with real
RO NFS, zero-byte warm stages, corruption/concurrency/quota/pin/crash/reconciliation
checks and a distinct-UID/no-network negative isolation probe. pi01 is deferred.
All repository tests also passed; see the [completion report](../docs/storage/48-phase11-live-acceptance.md)
for scope, actual timings, source/OS requirements, commands and rollback.
## Structured operator events — Phase 12.1

[`events.py`](events.py) defines version-one, allowlisted JSON events on
`distributed_ml.storage.events` (INFO): exact job/attempt/worker attribution,
immutable input version, staging bytes/durations, NFS copy errors/retries,
selection policy/reason, committed checkpoint publication ID and SDK-confirmed
recovery latency. No handler/DB/schema/config/UI is installed or enabled. Events
are advisory; default logging is suppressed and legacy storage remains unchanged.
See the [contract, units, delivery limits and tests](../docs/storage/49-structured-events.md).

## Admin observations — Phase 12.2

[`admin_observations.py`](admin_observations.py) provides optional kernel-only
`NFSHealthProbe` and `AdminObservations`: accepted signed inventory can publish
one bounded private LOCAL diagnostic snapshot beside its actual SQLite DB.
Explicit `InventoryReporter(..., nfs_health=...)` opts into inventory v3 on the
same existing HMAC channel; omitting it preserves v1/v2. Explicit
`CacheInventory(..., admin_observations=...)` enables the snapshot; omission
writes nothing. No automatic bootstrap, new DB table or scheduler authority.

The existing portal's admin-only aggregate panel uses readonly registry and
staging records, plus session/sequence/heartbeat-matched NFS observations.
Unknown/stale telemetry cannot imply healthy/eligible cache. Mount identity is
not an I/O availability test. Controller/parser must be upgraded before workers
opt into v3. See [the contract, security, tests and rollback](../docs/storage/50-admin-storage-observability.md).

## All-three-Pi delivery preparation — Phase 14.2

`delivery_plan.py` and `tools/hybrid_deploy.py` prepare/verify a private offline
review with complete role source bundles, exact config/audit hashes, unique
snapshot namespaces, additive migration order, service ordering and rollback.
`plan --output <new-absolute-directory>` is always a dry run; repeating a complete
unchanged output verifies/reuses it byte-for-byte. `verify --output ...` performs
local validation only. `apply --output ...` always refuses (exit3); no deployment
executor, network/DB access or service operations. All three Pis are mandatory.
Live bootstrap/containment/NFS/dependency/backup gates remain unresolved, and
legacy/OFF/resource stays active. See [14.2 playbook](../docs/storage/58-deployment-playbook.md).

## Explicit readiness and recovery-point checks

`state_backup.py` creates only a NEW private LOCAL SQLite recovery point and a
separately verified restored copy; source mode=ro, no live restore/migration API.
WAL-aware backup, size/free-space/deadline bounds, all-table content/user_version
comparison and quick/FK checks. It is DB-only, not coordinated app-file recovery
or off-host redundancy. Recovery copies contain private data; keep ignored/private.
`readiness_probe.py` supports approved protected NFS metadata and the existing
public TEST hash/zero-byte write-open-denial check; never changes exports/mounts.
`tools/storage_readiness.py` wraps both and emits a standalone read-only program.
Exact scope/results, supplied controller terminal verification and pi01 DERP path:
[readiness closeout](../docs/storage/60-readiness-closeout.md). These tools do not
enable production hybrid execution or make the old delivery review apply-ready.
The user retained local/private-only runtime data transfers; pi01's current
relayed path does not satisfy that requirement. No Internet data fallback enabled.

## Executable source delivery — Phase 14.2 continuation

`deployment_toolkit.py` / `tools/hybrid_install.py` produce a private all-three-Pi
kit from fresh read-only host inventories. Each host has exact source preimages,
payload, launcher and operation argv. `deployment_executor.py` supplies explicit
approval-bound, locked, journaled source install and rollback; no service stop or
start is automatic. Partial installs resume only verified bytes; rollback restores
backups and quarantines release-created files, preserving jobs/cache/checkpoints.
`deployment_migrations.py` separately backs up/restores/rehearses on LOCAL SQLite
before additive idle-controller migrations; never restores a live DB automatically.

Unit/config changes are explicitly zero: legacy/OFF/resource and exact existing
service definitions remain. Protected roots, quiescent services and later exact
approval are mandatory. This adds delivery tooling, not hybrid bootstrap/isolation
or production enablement. Old `hybrid_deploy.py apply` still refuses; use the new
reviewed workflow rather than bypassing it. Actual scope,139 local tests, all-host
read-only inventory, dry run, limitations and rollback:
[installation tooling](../docs/storage/61-installation-tooling.md).
