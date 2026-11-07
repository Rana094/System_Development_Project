# Configuration

| File | Purpose | Used by |
|---|---|---|
| [nodes.json](nodes.json) | Worker IDs, reachable addresses, ports and optional worker paths | Scheduler, dispatcher, setup and status tools |
| [queue_policy.json](queue_policy.json) | Default/per-owner active and pending limits, queue aging | Queue admission and fair scheduling |
| [execution_leases.json](execution_leases.json) | Enable leases and set duration/renewal interval | Execution ownership and partition recovery |
| [storage.json](storage.json) | Legacy-default flags; optional held NFS submission mode, execution still gated | Dispatcher/portal/CLI; [selection and compatibility](../docs/storage/13-storage-selection.md) |
| [storage-examples/](storage-examples/README.md) | Tested legacy and held-NFS config samples; precise operator paths and limits | Read-only config validation; does not activate settings |
| [nfs-server.json](nfs-server.json) | Restricted pi02/pi03 NFS export; actual controller publisher UID/GID 125; pi01 deferred | Server planner and approved local setup; [live results/rollback](../docs/storage/10-phase2-completion.md) |
| [nfs-clients.json](nfs-clients.json) | Read-only input automount on pi02/pi03; pi01 deferred | Client planner and approved local setup; [live results/rollback](../docs/storage/10-phase2-completion.md) |
| [job.schema.json](job.schema.json) | Published version-one submission schema | Students and external tooling; runtime validation is in `controller/job_spec.py` |
| [runtime-requirements.txt](runtime-requirements.txt) | Flask, requests, psutil and waitress for services | Controller/worker installation |

Submitted projects declare their own training dependencies in their project
`requirements.txt`. Service dependencies do not install every training package.

Worker registration creates `nodes.lock` locally; it is runtime state and ignored
by Git. For changes, use the [deployment guide](../docs/deployment.md),
[queue policy guide](../docs/queue-controls.md#configure-limits-for-the-lab), and
[execution ownership guide](../docs/execution-ownership.md).

NFS server/client proposals now accept optional `deployment_nodes`. Omission keeps
the original all-three-Pi requirement. The user explicitly selected `["pi02",
"pi03"]` for 2.4; only those hosts enter the export/client plan. Keep the two
proposal selections/addresses consistent. This infrastructure selection does not
enable hybrid execution or change the application worker inventory.
