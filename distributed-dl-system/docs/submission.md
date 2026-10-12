# Student submission contract — steps 1 and 2

A student describes a training project using version-one JSON. The controller
validates the declaration, assigns a job ID, and stores its owner and normalized
settings atomically in SQLite. Step 2 automatically copies and fingerprints the
inputs, then changes the job from `RECEIVED` to `STAGED`. The dispatcher processes
`QUEUED` jobs. Step 3 prepares worker environments and moves eligible submissions
to `READY`; scheduler integration follows later. See [controller storage](storage.md) for the layout and
restart recovery commands.

## Validate and submit

Run from the project root:

```bash
.controller-venv/bin/python controller/submit_job.py \
  --spec examples/job.single.json --validate-only

.controller-venv/bin/python controller/submit_job.py \
  --spec examples/job.single.json --owner student-2026

.controller-venv/bin/python controller/job_status.py <job-id>
```

For a distributed declaration, use `examples/job.distributed.json`. It requests
two workers. The example project contains a small trainer that accepts executor
rank settings; step 1 does not launch it or establish a DDP group.

Validation-only creates no job. An accepted submission prints a JSON receipt
containing `job_id`, `owner`, `status`, the normalized submission with managed
paths, and storage locations/content versions.
Built-in dummy/PyTorch options remain available, but cannot be mixed with `--spec`.
Their existing execution and recovery paths retain their current behaviour.

## Required fields

| Field | Meaning |
|---|---|
| `schema_version` | Integer `1`; future incompatible formats will use a new version |
| `name` | Human-readable job name, up to 128 characters |
| `project` | Existing local project directory or valid ZIP package |
| `entrypoint` | Argument array such as `["python", "train.py", "--epochs", "10"]`; stored as arguments rather than a shell command string |
| `dataset` | Existing local file or directory; `null` when the trainer generates its data |
| `execution.mode` | `single` or `distributed` |
| `execution.workers` | Exactly `1` in single mode; at least `2` in distributed mode |
| `resources_per_worker.cpu_cores` | Positive integer CPU-core request for each worker |
| `resources_per_worker.memory_gb` | Positive finite memory request for each worker |
| `resources_per_worker.gpus` | Nonnegative integer; use `0` on the Pi testbed |
| `environment.requirements` | Dependency file inside the project, such as `requirements.txt`; it may be empty |
| `recovery.mode` | `restart` or `checkpoint`, declaring the intended failure behaviour |

Optional fields:

- `environment.python`: Python 3 major/minor version such as `3.12`.
- `recovery.max_retries`: integer from 0 through 10; defaults to 2.
- `recovery.interval_seconds`: positive finite period; required for `checkpoint`
  mode and prohibited for `restart` mode.

The machine-readable shape is in `config/job.schema.json`. The controller also
checks local file availability and the presence of the dependency file. Project
and dataset paths are relative to the submission file, not the terminal's working
directory. Accepted records store absolute paths. Dependency paths stay relative
to the project root, including the root of a ZIP package. For a ZIP submission,
place the dependency file at the declared member path; the controller inspects
archive metadata during validation. Staging then extracts safe ZIP members;
neither operation executes student code.

Unknown fields, duplicate JSON keys, malformed JSON, invalid numeric values,
missing inputs, and inconsistent mode/worker settings are rejected. GPU and
worker counts describe a request; Pi capacity checks and resource reservations
are a later scheduling step. The format supports lab-PC requests without a
hard-coded three-worker limit.

## Ownership and durability

Supply `--owner` as a student ID or email. This local CLI records the supplied
identity; student authentication belongs to the later interface step. An owner
cannot be embedded in the training manifest. Existing historical jobs keep a
null owner when the database adds the new column.

The accepted name, paths, command, execution mode, resource requests, environment
and recovery declaration are persisted together with the owner and `RECEIVED`
state. Successful staging commits managed paths and the snapshot manifest digest
with `STAGED`. These remain readable after a controller restart, even if the
original files are removed. Keep original inputs available until staging succeeds.
The original request is retained separately in `submission.json`.

Selecting checkpoint mode records the intended capability. The controller does
not yet verify or provide checkpoint integration for custom projects. Likewise,
submitting distributed mode does not make a single-process script distributed;
the trainer must implement the training-framework integration.

## Verify the submission contract

```bash
PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tests/test_job_submission.py
```

These checks use temporary projects and a temporary database. They test both
execution modes, lab resource declarations, ZIP inputs, generated datasets,
invalid settings, owner validation, persistence across interpreter restarts,
legacy database migration, CLI compatibility, and the dispatch boundary. They
do not contact the Pis or create jobs in the live database.
