# Controller-owned job storage — step 2

Submitting a valid student job now stages its source and dataset automatically.
The controller publishes a verified input snapshot and changes the job from
`RECEIVED` to `STAGED`. It retains the original submission for audit, while the
job's execution settings point to managed copies. No training starts at this
step. [Environment preparation is available in step 3](environments.md);
scheduling and dispatch follow later.

## Submit and inspect

```bash
.controller-venv/bin/python controller/submit_job.py \
  --spec examples/job.single.json --owner student-2026

.controller-venv/bin/python controller/job_status.py <job-id>
.controller-venv/bin/python controller/stage_job.py --verify <job-id>
```

The receipt includes the managed paths, project content version and dataset
content version. Validation-only still creates no job and copies no files.

Stage an older step-one submission, or recover interrupted staging:

```bash
.controller-venv/bin/python controller/stage_job.py --job-id <job-id>
.controller-venv/bin/python controller/stage_job.py --pending
```

`--pending` processes all `RECEIVED` custom submissions. Each error is reported
with its job ID; other submissions can still be staged. Successful jobs remain
outside the dispatch queue. Step 3 prepares their worker environments and changes
them to `READY` when enough compatible candidates are available.

## Layout

Default location: `storage/jobs/<job-id>/` under this project.

```text
<job-id>/
  submission.json   Original normalized request, including original input paths
  manifest.json     Ownership, managed settings, file hashes and content versions
  source/           Project files, ready for later environment preparation
  data/             Dataset snapshot, or empty for generated data
  checkpoints/      Runtime checkpoints, separate from the existing prototype path
  logs/             Runtime logs
  results/          Outputs to return to the student
```

Projects can be directories or ZIP packages. ZIP projects are unpacked into
`source/`. Dataset directories and ZIP packages populate `data/`; a regular
file such as a CSV is stored under `data/<original-filename>`. Generated datasets
(`dataset: null`) keep the execution dataset path and dataset version null.
Source/dataset files are copied, never hard-linked to the original inputs.
Executable bits and empty directories are retained.

To use a larger lab disk, configure the controller environment before submitting
or staging jobs:

```bash
export DL_STORAGE_ROOT=/path/to/controller-data/jobs
```

Keep this root stable while staging is in progress. Already staged jobs remember
their absolute storage location in SQLite, even if the environment changes.
The selected storage root must be outside the source and dataset directories.
This prevents recursively copying the storage area into its own input snapshot.
Runtime storage is ignored by Git; it is not a source-code backup.

## Versions and verification

The manifest lists every immutable source/dataset file with its relative path,
SHA-256 digest, byte size and executable flag, plus empty directory information.
A canonical hash of this inventory forms the content version. Identical dataset
contents and paths yield the same version across jobs; changed content or layout
produces a new version. Each job still receives its own copy. For ZIP uploads,
the original archive digest is also recorded; the temporary archive copy is
removed after extraction.

The database stores the workspace path, manifest digest and staging timestamp.
`--verify` checks that digest, submission ownership and settings, the managed
paths, and every source/dataset file. Changes to `checkpoints/`, `logs/`, and
`results/` are allowed because these are runtime outputs.

After a job reaches `STAGED`, deleting or editing the student's original files
cannot change its managed snapshot. A new controller process can reconstruct
and verify the submission using SQLite and the stored metadata alone. If managed
inputs are corrupted or missing, verification fails; staging does not silently
replace a committed snapshot from potentially changed originals.

## Interrupted staging

1. Insert the complete request as `RECEIVED`.
2. Copy into a hidden temporary workspace while holding a per-job lock.
3. Reject changed inputs, links, special files and unsafe ZIP members.
4. Hash and verify the copied inputs, flush their files/directories, and rename
   the workspace to its final path.
5. Commit `STAGED`, its manifest digest and managed execution paths in SQLite.

An interrupted copy has no published snapshot. A retry removes only that job's
abandoned temporary folders and copies again. If the controller stops after
publication but before the database commit, `--pending` verifies the published
snapshot and commits it without needing the original input files. Concurrent
staging of the same job is rejected. Failed copies retain `RECEIVED` with a
recorded error and do not create training attempts or consume retry counts.

Directory submissions must contain ordinary files/directories. ZIP submissions
must not contain escaping paths, symbolic links, special files or duplicate
normalized names. Extraction never executes student code.

## Checks

```bash
PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tests/test_job_storage.py
PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tests/test_job_submission.py
```

Checks use isolated databases and temporary storage. They cover input removal,
restart reconstruction, interruption before/after publication, locking, changed
inputs, dataset versions, corruption detection, ZIP extraction and compatibility
with the submission contract. Existing checkpoint-recovery regression checks
remain in `tests/test_checkpoint_recovery.py`.

This is local-controller ingestion of submitted inputs. The student web-upload
interface, per-user access controls, storage quotas and training dispatch belong
to subsequent steps. Step 3 now provides worker dataset caches and environments.
