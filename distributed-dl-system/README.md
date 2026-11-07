# Distributed deep-learning system

Submit training through a browser or the command line. The controller prepares
worker environments, schedules single-worker and distributed PyTorch jobs,
collects results, and resumes supported jobs from validated checkpoints after a failure.

## Start here

| I want to… | Read this |
|---|---|
| Start the website and submit training | [Website guide](docs/student-interface.md) |
| Understand the folders and files | [Project map](docs/project-map.md) |
| Understand how the system works | [Architecture](docs/architecture.md) |
| Install or move the system to another machine | [Deployment](docs/deployment.md) |
| Choose a runnable example | [Examples](examples/README.md) |
| Find a demo, benchmark or comparison command | [Tools](tools/README.md) |
| Find a specific feature guide | [Documentation index](docs/README.md) |

## Start the existing installation

From this repository's root directory:

```bash
systemctl --user start dl-dispatcher dl-student-portal
systemctl --user is-active dl-dispatcher dl-student-portal
```

Expect two `active` lines, then open **http://127.0.0.1:8080/** and sign in with
your existing account. Workers must be online to execute training.
The configured Tailscale address is **http://100.73.90.91:8080/**;
see [remote access](docs/tailscale.md).

For a fresh checkout, follow [manual startup](docs/student-interface.md#run-the-local-server-manually-without-installed-services)
or [installation](docs/deployment.md). Only run one dispatcher at a time.

## Folder layout

```text
distributed-dl-system/
├── agent/              Worker API, job execution and checkpoint SDK
├── controller/         Portal, submission, scheduling and recovery
│   └── portal_static/  Browser HTML, CSS and JavaScript
├── config/             Worker inventory, policies and dependencies
├── docs/               Feature guides, architecture and file map
├── examples/           Job manifests, training projects and sample data
├── tests/              Isolated regression checks
├── tools/
│   ├── cluster_setup.py  Installation and cluster inspection
│   ├── run_tests.py      Run all regression scripts separately
│   ├── demos/            Live environment, queue and recovery demonstrations
│   ├── benchmarks/       Timing and unchanged reference workload runners
│   └── comparisons/      Compare completed training results
└── training/           Built-in workloads and reference snapshots
```

Local runtime files also live in `.controller-venv/`, `controller/jobs.db`,
`storage/`, `checkpoints/` and `logs/`. They are excluded from version control;
retain them on an existing installation for accounts, results and recovery.
See [the cleanup review](docs/repository-cleanup.md) for what was moved and why.

## Development checks

```bash
PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py
```

The runner gives each test file its own Python process because tests use isolated
controller copies and module paths. See [test setup and individual checks](tests/README.md).
