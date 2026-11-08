# Documentation

Start with the [website guide](student-interface.md) to use the system, or the
[project map](project-map.md) to understand the repository.
Commands in these guides run from the repository root unless stated otherwise.

## Learn and run

| Guide | Purpose |
|---|---|
| [Architecture](architecture.md) | Components, job lifecycle and code reading order |
| [Project map](project-map.md) | File-by-file responsibilities and optional components |
| [Website](student-interface.md) | Startup, accounts, dashboard, uploads and walkthroughs |
| [Training examples](training-examples.md) | Run regression, binary classification and CNN jobs |
| [Integration walkthrough](integration-instructions.md) | Submit baseline/recovery jobs and compare results |

## Submission and scheduling

| Guide | Purpose | Original development step |
|---|---|---|
| [Submission](submission.md) | JSON contract and validation | 1 |
| [Storage](storage.md) | Managed input snapshots, hashes and staging | 2 |
| [Environments](environments.md) | Worker Python environments, profiles and caches | 3 |
| [Scheduling](scheduling.md) | Resource requests, reservations and concurrent jobs | 4 |
| [Queue controls](queue-controls.md) | Owner limits, fairness, hold/release and cancellation | 5 |

## Recovery and distributed training

| Guide | Purpose | Original development step |
|---|---|---|
| [Checkpoint integration](checkpoint-integration.md) | Add the checkpoint SDK to single-worker trainers | 6 |
| [Distributed training](distributed-training.md) | DDP groups, consistent checkpoints and coordinated resume | 7–9 |
| [Execution ownership](execution-ownership.md) | Leases, fencing and recovery during network partitions | 10 |

## Operations and evaluation

| Guide | Purpose |
|---|---|
| [Deployment](deployment.md) | Worker/controller installation and lab migration |
| [Tailscale](tailscale.md) | Remote website and worker access |
| [Benchmarks](benchmarks.md) | Reference comparisons, full training and inference |
| [Tools index](../tools/README.md) | Select a live demo or analysis script |
| [Tests](../tests/README.md) | Run isolated regression checks |
| [Hybrid storage progress](storage/implementation-progress.md) | Architecture, phase history and current implementation boundary |
| [NFS infrastructure results](storage/10-phase2-completion.md) | Live pi02/pi03 validation, measurements, evidence and rollback |
| [Controller input adapters](storage/11-controller-input-adapters.md) | Strict object keys, approved project-file views and local tests; runtime still legacy |
| [Input health and admission](storage/12-input-health.md) | Mount/FD identity, verified reads, explicit failures and supervised deadlines |
| [Pinned storage selection](storage/13-storage-selection.md) | Legacy-compatible config/job intent, held NFS submissions and old-file reference plan |
| [Phase 3 validation](storage/14-phase3-validation.md) | Fake/local acceptance, concurrent reads, tested config samples, remaining execution gates |
| [Immutable artifact registration](storage/15-artifact-registration.md) | Bounded SHA-256 publication, atomic rename, verified deduplication and local crash tests |
| [Dataset manifests and ownership](storage/16-dataset-manifests.md) | Canonical immutable versions, current ACL contract, safe tree inspection and ZIP preflight |
| [Registry and exact job inputs](storage/17-registry-and-job-inputs.md) | Local additive migration, verified ownership, current ACLs and immutable source/data/requirements bindings |
| [Legacy registration / Phase 4 acceptance](storage/18-legacy-registration-and-phase4.md) | Explicit nondestructive enrollment, legacy compatibility, local execution evidence and rollback |
| [Persistent local worker cache](storage/19-worker-cache.md) | Verified local observations, restart-persistent pins, disk accounting and disabled runtime boundary |
| [Missing-only NFS cache fetch](storage/20-cache-fetch.md) | Authorized verified copy, quota/admission, helper ownership, atomic publication and local/fake evidence |
| [Private workspaces and pin lifecycle](storage/21-private-workspaces.md) | Independent attempt copies, durable exact pins, terminal quiescence and local workload/watchdog tests |
| [Repository cleanup](repository-cleanup.md) | Review decisions and old-to-new tool paths |

The numbered steps in older guides describe development milestones. Use the
feature names above to navigate the current system.
