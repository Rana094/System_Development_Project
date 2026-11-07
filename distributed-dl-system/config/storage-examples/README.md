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

Do not enable cache/locality/bandwidth: the controller rejects these runtime
features even though their names are reserved by the typed schema. The NFS
sample does not register objects, stage inputs, create a cache, launch a job or
test NFS availability. No executable NFS configuration exists yet. A later
reviewed deployment/restart is needed to change active operator settings.
Changing the default back to legacy does not release or rebind held NFS jobs.

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
