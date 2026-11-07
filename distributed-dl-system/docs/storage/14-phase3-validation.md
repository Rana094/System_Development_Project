# Phase 3.4 — fake/local validation and configuration samples

8 November 2026, Asia/Dhaka. **3.4 PASS for fake/local component and legacy
compatibility acceptance. STOP.** This closes the requested Phase 3 adapter,
admission, compatibility and test scope. It does **not** enable end-to-end NFS
training. NFS submissions remain HELD pending authenticated staging, worker
isolation and durable process ownership. Cache/registry/locality execution is
still unavailable. The central controller remains a **single point of failure**.

The last accepted subphase was [3.3](13-storage-selection.md), evidenced by its
report/progress and the user's request for 3.4. Architecture/audit documents are
historical design/baseline records, not proof of later runtime integration. No
live Pi/NFS probes, deployment, benchmark or training occurred in this turn.
pi01 remains deferred under the explicit pi02/pi03 exception; all-three-Pi
acceptance is not claimed. Any later cluster validation still requires all three
unless the user expressly approves an exception.

## 1. Changes and behavior

| File | Change |
|---|---|
| `tests/test_storage_readers.py` | Eight tests: simultaneous readers and independent offsets/FD closure, independent project grants, mixed verification outcomes, mount replacement, partial-read errors, legacy concurrency and two shipped config examples. |
| `config/storage-examples/legacy.json`, `nfs-held.json` | Exact validated flag declarations; neither is the active `config/storage.json`. |
| `config/storage-examples/README.md` | Actual supported settings, separate trusted paths/source/ownership, existing topology, security and activation limits. |
| `docs/storage/14-phase3-validation.md`, `14-phase3-validation-evidence.json` | Acceptance matrix, commands, raw results, preservation checks and rollback. |
| `docs/storage/implementation-progress.md`, `docs/README.md`, `tests/README.md`, `config/README.md` | Current progress and navigation. |

No runtime Python file needed modification. New tests use ordinary temporary
files for bytes, explicitly fake mount metadata and FD identity, and bounded
thread barriers to keep multiple descriptors open simultaneously. Production
admission has no fake-mount switch. The partial-read tests inject errors only
after a successful first byte and assert descriptor cleanup and failed health.
Parallel project views demonstrate separation of trusted caller grants, not
student authentication. The integrity probe verifies size/hash; opening a file
alone does not verify all its bytes.

Git root is the application's parent `System_Development_Project`, branch
`main`, HEAD `ddc9a7b4a0a61288462c1999fd8c8ef1a2d01977`. Existing staged,
unstaged and untracked changes are preserved; no stash/reset/clean/commit.
Before hashes/copies, status and logs are in `/tmp/hybrid-storage-3-4/` and
`/tmp/hybrid-storage-3-4-status-before.txt`. Durable evidence is linked below.

## 2. API, database, configuration and security

No API, database migration/schema, job state, checkpoint format, scheduler,
dependency or runtime configuration change in 3.4. Controller and
`hybrid_storage` runtime source hashes match the turn baseline. SQLite remains
local; its main-file hash is unchanged. No controller import/initializer or SQL
write ran against the live database. The test runner excludes DB/WAL/runtime
roots and initializes only disposable controller copies.

Actual flags remain `legacy / false / false / resource`. The
[samples](../../config/storage-examples/README.md) are validator inputs, not a
new path-configuration schema. `nfs-held.json` reports `runtime=legacy`,
`new_job_default=nfs`, `nfs_execution=blocked`. It cannot run NFS jobs, silently
fallback to HTTP execution, release existing holds or reinterpret old jobs.
No object registration, wholesale copy or automatic legacy adoption occurred.

The strict NFS wrapper requires exact RO NFS4.1 source/mount/FD identity. It
never writes through a worker mount or treats the empty underlying mountpoint
as storage. Root squash and restricted RO exports remain required. AUTH_SYS is
not tenant authorization; owner-authorized manifests and exclusion of the NFS
view from untrusted training remain enablement prerequisites. Checkpoints retain
the trusted existing publisher flow. No student may supply trusted root/source
paths. Existing legacy permission and transport limitations are not repaired by
these component tests.

## 3. Tests and acceptance evidence

Run from the application root with its existing virtual environment:

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_storage_readers.py test_storage_selection.py test_storage_config.py test_job_submission.py test_job_storage.py test_student_portal.py test_queue_controls.py test_resource_allocation.py test_storage_inputs.py test_storage_health.py test_storage_backends.py test_nfs_client_setup.py test_nfs_server_setup.py
```

**212 passed, zero failures/skips, 13/13 files.** The pre-change selection was
**204 passed, zero failures/skips, 12/12 files**. New tests also passed separately
(8/8). No pre-existing failures were observed in this selection. This is not the
whole repository suite: live, expensive and training/DDP tests were not run.

| File | Tests passed |
|---|---:|
| `test_storage_readers.py` | 8 |
| `test_storage_selection.py` | 21 |
| `test_storage_config.py` | 17 |
| `test_job_submission.py` | 6 |
| `test_job_storage.py` | 9 |
| `test_student_portal.py` | 17 |
| `test_queue_controls.py` | 10 |
| `test_resource_allocation.py` | 6 |
| `test_storage_inputs.py` | 18 |
| `test_storage_health.py` | 26 |
| `test_storage_backends.py` | 24 |
| `test_nfs_client_setup.py` | 28 |
| `test_nfs_server_setup.py` | 22 |

| Requested acceptance | Grounded verification | Result |
|---|---|---|
| Fake mounted directories and unavailable/mis-mounted paths | Health tests use the real mount parser with fake metadata; wrong source/type/options, ordinary dirs, autofs-only, absent metadata, mount races and FD mismatch fail closed. Server/client tests cover restricted export/mount planning with fake hosts. | PASS (fake/local) |
| Unauthorized paths | Input tests reject traversal, symlinks/special files and unlisted grants before I/O; parallel views keep the same project path bound to different approved objects and deny cross-view paths/raw keys. | PASS (trusted-view scope) |
| Permission/errors/corruption | Open-time and partial-read EACCES/EPERM, ESTALE/ETIMEDOUT/EIO; ENOSPC/EROFS, missing files, size/hash/metadata changes, bounded publication and process timeout tests. | PASS (injected errors) |
| Concurrent reads | Six simultaneous independent FDs/offsets; three integrity probes with ready/corrupt/missing results; mount replacement rejects three active readers and closes FDs. | PASS |
| Flag-off/legacy regressions | Missing/empty/OFF settings equivalence, upload bytes and legacy API/preparation, old running state/checkpoint across migration, queue/resource rules; legacy concurrent reads never consult NFS admission. | PASS |
| Precise production config samples without secrets | Both shipped examples are checked through the real CLI in subprocesses; documentation separates four application flags from source/root/ownership and held intent from execution. | PASS |

Raw command arrays, return codes, full test output, Python version, before/after
hashes and static checks are in [evidence](14-phase3-validation-evidence.json).
Static validation includes Python 3.11 AST parsing, JSON parsing, changed Markdown
local links/whitespace, scoped reverse-patch validation and `git diff --check`.
The source-copy runner isolates state, not network; the chosen files were
reviewed for fake/local behavior. No Internet-dependent worker transfer occurs.

## 4. Manual verification without activation

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python -m hybrid_storage.config
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python -m hybrid_storage.config --config config/storage-examples/legacy.json
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python -m hybrid_storage.config --config config/storage-examples/nfs-held.json
```

Expected: the first two return identical legacy settings; the third adds
`new_job_default: nfs` and `nfs_execution: blocked` while `runtime` stays legacy.
All exit 0, do not initialize SQLite or activate settings. Run the isolated test
command above for the full acceptance selection; expect 13/13 files passing.
No root, SSH, mounts or service operations are needed. These commands were run.

## 5. Rollback and residual risks

No host/runtime rollback is needed. A scoped diff containing **only this turn**
is at `/tmp/hybrid-storage-3-4.patch`, generated against before-files rather than
the dirty Git base. From the parent Git root, review/check before applying:

```bash
git apply --reverse --check /tmp/hybrid-storage-3-4.patch
git apply --reverse /tmp/hybrid-storage-3-4.patch
```

The reverse check passed at completion; later edits may invalidate it. Preserve
later user changes and evidence first. This removes only the newly added test,
samples/reports and documentation additions, not jobs/DBs/checkpoints or earlier
phases. Do not reset to HEAD or reverse the entire pre-existing dirty diff.
After a future deployment of 3.3, follow its separate downgrade requirements:
cancel/quarantine NFS intent before older binaries that ignore gates; preserve
bindings, inputs and DB. Merely restoring flags does not rebind a job.

Residuals: kernel hard-NFS reads may outlive a caller deadline; raw adapter reads
are synchronous and must not run on Flask/scheduler/lease threads. Metadata
admission and kernel-cached verified reads do not prove fresh server liveness.
The health supervisor has in-memory ownership; retain/reconcile pending children,
and add durable restart ownership before daemon integration. Publication's
ext4/xfs/btrfs allowlist is conservative type attestation, not proof of physical
locality or power-loss durability. Byte immutability still relies on protected
trusted ownership. No real network outage/full-disk/corruption, all-three-Pi
run, training/DDP recovery, security sandbox or throughput benchmark was tested
here. These are explicit limits, not evidence of production readiness.

## 6. Decision

**3.4 and Phase 3 fake/local adapter/compatibility acceptance: PASS.** Existing
legacy execution remains the default and runtime source is unchanged in 3.4.
**Executable hybrid training / production end-to-end acceptance: NOT CLAIMED.**
Do not enable staging/cache/locality or deploy based on this result. STOP; no
next subphase is implemented.
