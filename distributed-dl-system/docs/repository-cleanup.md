# Repository cleanup review

Reviewed on 7 November 2026. The review inventoried first-party code, tests,
documentation, configuration and example inputs, checked imports and command/path
references, and separated them from installed packages and generated runtime data.

## Decisions

| Category | Finding | Action |
|---|---|---|
| Core controller and worker code | Used by services, commands or tests | Kept existing locations to preserve service/deployment paths |
| Tools | Demos, benchmarks and result analysis were mixed together; numbered names hid their purpose | Grouped by purpose and renamed numbered demonstrations |
| Documentation | The root README mixed startup, internals, historical milestones and long experiment commands | Replaced it with a short entry page; added architecture, benchmark and documentation indexes |
| Local Python environment | 938 tracked files are installed dependencies rather than project source | Removed from Git tracking; retained the environment on disk |
| Job database | One tracked SQLite database contains local job history | Removed from Git tracking; retained `controller/jobs.db` on disk |
| Old checkpoint JSON files | Four tracked checkpoint files are generated recovery data | Removed from Git tracking; retained all four files on disk |
| Example datasets | Three required CSVs existed locally but were hidden by an ignore rule | Added explicit ignore exceptions so the inputs can be committed |
| Caches and worker runtime output | Regenerable Python caches, locks and worker storage do not belong in source | Expanded `.gitignore` and removed eight application bytecode cache directories; job data stays local |

Removing files from Git tracking does not delete the local files. Git's staged
deletions for the environment, database and old checkpoints are intentional.
The three CSV datasets appear as source additions and should be included when
committing this cleanup. Existing Git history is unchanged.

## Optional components retained

No source module was proven unused by this review. A CLI or dynamically launched
script can be useful without being imported by the main application.

| Component | Why it remains |
|---|---|
| `controller/controller.py` | Optional read-only terminal dashboard; the scheduler runs in `dispatcher.py` |
| `controller/migrate_pytorch_jobs.py` | Explicit compatibility migration for older databases, also covered by regression checks |
| `controller/checkpoint_store.py`, `training/demo_job.py` | Supported legacy dummy-job checkpoint flow |
| `training/pytorch_job.py`, `training/evaluate_checkpoint.py` | Built-in recoverable workload and checkpoint evaluation |
| `training/reference/` and `training/benchmark_job.py` | Fixed comparison workloads with verified source hashes |
| Demo tools and example projects | Teaching, integration, recovery and benchmark workflows |

## Tool moves

Commands now use these paths. Imports, repository-root resolution and documentation
references were updated with the moves. Update any personal scripts that use the old paths.

| Previous path | Current path |
|---|---|
| `tools/check_step3.py` | `tools/demos/prepare_environments.py` |
| `tools/check_step4.py` | `tools/demos/concurrent_jobs.py` |
| `tools/check_step5.py` | `tools/demos/fair_queue.py` |
| `tools/check_step6.py` | `tools/demos/checkpoint_recovery.py` |
| `tools/run_reboot_demo.py` | `tools/demos/reboot_recovery.py` |
| `tools/benchmark_training.py` | `tools/benchmarks/benchmark_training.py` |
| `tools/run_comparison.py` | `tools/benchmarks/run_reference.py` |
| `tools/verify_reference_training.py` | `tools/benchmarks/verify_reference.py` |
| `tools/compare_regression_jobs.py` | `tools/comparisons/compare_regression_jobs.py` |
| `tools/compare_training_jobs.py` | `tools/comparisons/compare_training_jobs.py` |

## Keep the structure understandable

- Put live cluster demonstrations under `tools/demos/`, timing/reference runners under `tools/benchmarks/`, and completed-job analysis under `tools/comparisons/`.
- Add feature documentation to `docs/` and link it from `docs/README.md`.
- Keep student-owned example code separate from built-in workloads and unchanged reference snapshots.
- Store job outputs in the existing managed runtime locations; retain data required by accounts, recovery and job history.
- Do not move `agent/`, `controller/`, `training/` or active runtime paths without updating their service and deployment consumers.

## Verification

- All 14 existing regression scripts passed. The three DDP suites required loopback socket access outside the restricted sandbox.
- All 12 tool entrypoints loaded and displayed `--help` when launched from outside the repository.
- All eight example manifests passed validation, including their project and dataset paths.
- All eleven reference Python files still match their recorded SHA-256 hashes.
- Local Markdown links and section anchors resolve; moved tools have no remaining old command references outside the move table.
- The ten moved tools changed only repository-root resolution and renamed sibling imports; core controller and worker modules are unchanged.
- The local environment, database and four previously tracked checkpoints remain on disk.

See the [project map](project-map.md) for current file responsibilities.
