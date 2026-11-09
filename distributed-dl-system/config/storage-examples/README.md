# Storage configuration samples

These are exact inputs to the current config validator, with no secrets. They
are examples, not active configuration. Services read `config/storage.json`;
there is no environment override or command to activate a sample here.

From the application directory, validate without starting services or importing
the controller database:

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python -m hybrid_storage.config --config config/storage-examples/legacy.json
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python -m hybrid_storage.config --config config/storage-examples/nfs-held.json
```

| Sample | Meaning and expected output |
|---|---|
| [legacy.json](legacy.json) | Current production execution mode: `runtime=legacy`, both new flags false, resource scheduling. Missing config or `{}` has the same settings. |
| [nfs-held.json](nfs-held.json) | Opt-in new-submission default only: `runtime=legacy`, `new_job_default=nfs`, `nfs_execution=blocked`. New NFS jobs retain a controller-local snapshot and become HELD. Explicit legacy submissions and old implicit-legacy jobs retain existing execution. |

The ordinary portal/dispatcher startup still rejects cache/locality/bandwidth
execution: it cannot provision a trusted runtime, isolation or credentials from
four JSON flags. The held-NFS sample does not register objects, stage inputs,
create a cache, launch a job or test NFS availability. The explicit trusted
component settings below do not remove that startup gate. A reviewed deployment
and bootstrap are required before changing active operator settings.
Changing the default back to legacy does not release or rebind held NFS jobs.

## Trusted coordinator policy settings — Phase 10.4

| Sample | Explicit component behavior |
|---|---|
| [nfs-resource.json](nfs-resource.json) | Trusted NFS/cache/staging coordinator; original resource scheduling, no inventory reads or choice tracing by default. |
| [nfs-locality.json](nfs-locality.json) | Same trusted coordinator and safety gates; verified missing bytes break resource-equivalent ties, otherwise resource fallback. |
| [nfs-bandwidth.json](nfs-bandwidth.json) | Phase 11.2 optional measured input-copy cost breaks resource-equivalent ties; unavailable/stale facts restore resource. Requires the explicit metrics bootstrap below. |

Validate these samples without importing SQLite, starting services or changing
active configuration:

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python -m hybrid_storage.config --runtime staging --config config/storage-examples/nfs-resource.json
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python -m hybrid_storage.config --runtime staging --config config/storage-examples/nfs-locality.json
```

Expect `runtime=trusted-staging-component` and `automatic_startup=false`. An
already-reviewed bootstrap supplies the parsed settings, its exact `StorageRegistry`,
authenticated `StagingClient` and optional same-registry `CoverageAPI` to
`StagingDispatch`/`CheckpointDispatch`. Missing/stale coverage falls back to resource.
Bandwidth is available only to the explicit trusted component as described below. Never copy these samples into active
`storage.json` to bypass runtime provisioning or all-three-Pi acceptance.

Locality emits structured `SCHEDULER_CHOICE` INFO events on Python logger
`distributed_ml.scheduler` and retains bounded operator history at
`runtime.scheduler.history()`. The bootstrap must explicitly route INFO to a
protected, preferably nonblocking logging sink. No global logging handler, logfile,
student route or persistent DB table is installed. An optional `choice_logger`
callable can route records into an existing operator sink and also enables
resource-policy diagnostics. Logs describe selection proposals, not completed
reservations or launches; they include filtered IDs, missing bytes, freshness,
policy/reason and truncation indicators without filenames, NFS paths or secrets.

For immediate rollback in that existing trusted coordinator, call
`runtime.set_scheduler_policy('resource')` before the next dispatch pass, and use
resource settings at its next bootstrap. This changes future placements only;
it does not rebind jobs, move inputs, clear caches or interrupt running attempts.
With no explicit choice logger, resource policy also disables inventory queries
and automatic tracing. Optional diagnostic logging can be disabled separately
by setting `runtime.scheduler.choice_logger = None` on the controller thread.
No network policy-control endpoint or automatic performance trigger is added.

The policy is greedy, not a whole-queue bandwidth optimizer: a controlled two-job
example stages 100 modeled bytes with locality versus 2 with resource. Resource
rollback restores 2. This is a modeled counterexample, not a Pi benchmark; resource
remains the active default. Review [10.4 evidence](../../docs/storage/41-scheduler-policy.md)
before choosing locality for a real workload.

## Optional bandwidth estimates — Phase 11.2

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python -m hybrid_storage.config --runtime staging --config config/storage-examples/nfs-bandwidth.json
```

Expected: validated `scheduler_policy=bandwidth`, `automatic_startup=false`.
Ordinary controller startup with this sample still fails closed. Active
`storage.json` stays unchanged. In a separately reviewed trusted bootstrap:

* Each worker explicitly shares one `TransferMetrics(credential.worker)` between
  its existing `CacheFetcher` and `InventoryReporter(transfer_metrics=...)`.
* Controller `CacheInventory(transfer_observations=TransferObservations())` receives
  bounded summaries over the existing authenticated challenge/report endpoint.
  Pass its same-registry `CoverageAPI` to the coordinator. No new download API.
* Controller code must understand optional inventory report v2 before enabling
  reporter measurements. Default reporters still emit byte-compatible v1; old
  reporters are accepted with unknown throughput and resource fallback. Older
  controllers reject v2, so upgrade code in a reviewed deployment first.
* At least three recent decomposed successful copies are needed. Old inclusive
  11.1 rates do not supply an invented overhead. No calibration speed test runs.
  Constructor defaults: 64 samples, 900s worker age, controller age at most 900s
  plus the conservative age calculation; minimum sample count 3. The controller
  adds the existing 30s allowed clock skew to observed age, not to the TTL.

Choice records include missing unique bytes/object counts, measured rate and
residual per-object overhead, sample age/count, observed range, disk availability,
size extrapolation and group concurrency assumptions. Costs cover missing-input
copy to verified cache, excluding warm verification, workspace materialization,
checkpoint restore, failures, queue wait and training. They do not predict total
completion time. Quota/workspace limits and cross-job NFS contention remain
unknown; actual stage-time admission, verification and leases stay authoritative.

Resource/locality remain independently usable without rate collection. Immediate
rollback remains `runtime.set_scheduler_policy('resource')`; stopping optional
report collection can wait for the next reviewed bootstrap. No deploy, service
restart, schema migration or cache eviction is performed by this subphase. See
[11.2 report](../../docs/storage/43-bandwidth-policy.md) for model assumptions,
authentication/freshness, exact test evidence, residual risks and source rollback.

## Infrastructure and trusted caller settings

These values are separate from the four application flags. Do not add them as
keys to `storage.json`: unknown keys are rejected. Existing server/client
planners read [nfs-server.json](../nfs-server.json) and
[nfs-clients.json](../nfs-clients.json). Those files and the
[Phase 2 report](../../docs/storage/10-phase2-completion.md) record this installation:

| Role | Configured value / requirement |
|---|---|
| Controller/source | `192.168.10.214:/srv/ml-shared/objects` |
| Controller authoritative inputs | `/srv/ml-shared/objects`; trusted local publisher, immutable SHA-256 objects |
| Private publication scratch | `/srv/ml-shared/.publish`; owner-only, same local filesystem; never exported |
| Publication account | Existing configured UID/GID `125:125`; verify on any new host, do not reuse blindly |
| Worker input view | `/srv/mlnfs/inputs`; exact RO NFS4.1/TCP source, hard, `sec=sys`, `nosuid,nodev,noexec` |
| Restricted export clients | pi02 `192.168.10.150`, pi03 `192.168.10.101`; `ro,sync,root_squash,no_subtree_check` |
| pi01 | Deferred by explicit user exception; no verified LAN address in the config |
| SQLite, jobs, checkpoints | Remain controller-local under existing ownership; none exported |

These addresses are recorded configuration, not fresh reachability evidence.
All three Pis remain the default validation requirement; the prior pi02/pi03
exception remains explicit and is not an all-three-Pi pass. No host is contacted
by these samples or their tests. Before deployment on another network, collect
read-only evidence and review restricted host addresses, ownership and mount
configuration; never substitute wildcard clients.

The trusted component seam is `controller.input_storage.local_objects` for
local publication and `NFSObjectStorageBackend` for worker input reads. These
APIs take operator-provisioned paths, not student-requested paths. Only the
strict NFS wrapper admits mount/FD identity; the raw Phase 1 adapter is a
low-level primitive, not a production admission boundary. Authorized manifest
entries still require owner/job checks and size/hash verification at staging.
The exported view must be outside untrusted training processes. AUTH_SYS is
not per-student authorization, and `noexec` does not prevent Python reading a
script. Checkpoints continue through the trusted authenticated publisher path,
never a worker-writable export.

See [Phase 3 validation](../../docs/storage/14-phase3-validation.md) for test
evidence, timeout limits and rollback. This sample changes no `/etc` files,
mounts, services, jobs or databases.
