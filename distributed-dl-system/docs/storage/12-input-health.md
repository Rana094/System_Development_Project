# Phase 3.2 — input health and fail-closed admission

**3.2 PASS in fake/local tests; STOP before 3.3.** This builds on the accepted
[3.1 interfaces](11-controller-input-adapters.md). Existing routes, in-flight job
behavior and legacy/off/resource flags remain unchanged. No remote worker, NFS
mount, service or infrastructure setting was touched. pi01 remains deferred under
the earlier user exception. Phase 3 as a whole is not complete.

## 1. Implemented behavior and files

| File | Change |
|---|---|
| `hybrid_storage/nfs_mount.py` | Backwards-compatible exact `target` keyword; missing kernel metadata fails admission. Existing source/RO/security/ancestry checks remain. |
| `hybrid_storage/mount_identity.py` | New Linux opened-FD mount IDs and conservative local-filesystem attestation. |
| `hybrid_storage/inputs.py` | Strict NFS source/mount/FD admission and mount rechecks; strict local publication validates actual destination/staging FDs. |
| `hybrid_storage/backends.py` | Empty legacy publication admission hook; only the strict wrapper overrides it. |
| `hybrid_storage/health.py` | Validated requests, size/hash verification, explicit failure statuses and owned-child supervisor. |
| `hybrid_storage/health_worker.py` | Trusted read-only subprocess entrypoint; no jobs/DB/mount/publication operations. |
| `controller/input_storage.py` | Document enforced publication locality; factory signatures unchanged. |
| `tests/test_storage_health.py` | 26 fake/local health, race, error and lifecycle tests. |
| `tests/test_storage_inputs.py` | Earlier fake NFS tests now explicitly mock kernel identities; production has no bypass. |
| `docs/storage/12-input-health.md`, `12-input-health-evidence.json` | Contracts, results, commands and rollback. |
| `hybrid_storage/README.md`, `tests/README.md`, `docs/README.md`, `docs/storage/implementation-progress.md` | Navigation, verification and progress. |

`NFSObjectStorageBackend(root, source="<verified IPv4>:/srv/ml-shared/objects")`
requires an explicit expected source for every read. The one-argument constructor
remains side-effect free, but reads now raise `MountNotReady`. It never guesses a
source or substitutes an ordinary directory. Publication always fails EROFS
without touching the root or consuming source bytes.

The existing guard accepts `target=<exact absolute root>`, retaining
`/srv/mlnfs/inputs` by default. It requires exact source/export, NFS4.1/TCP,
AUTH_SYS, hard/ro/nosuid/nodev/noexec, proper VFS restrictions, unshadowed ancestry
and no nested mounts. Linux's rw superblock is allowed only beneath a properly
restricted RO VFS mount. Ordinary directories, autofs-only triggers, wrong
source/type/options and missing metadata fail closed.

Before yielding a file, the wrapper compares its opened FD's `mnt_id` to the
admitted mount and rechecks current metadata. This catches a mount disappearing
between admission and open, even with valid bytes in the underlying directory.
Successful context completion rechecks again. Missing objects are distinct from
an unmount race; no error selects legacy storage. No input path is created or
used for a write probe. Descriptor identities follow the kernel's documented
[`fdinfo`/`mountinfo` contract](https://www.kernel.org/doc/html/latest/filesystems/proc.html).

Strict controller publication checks the actual anchored destination-parent and
staging FDs before temporary creation, source consumption or publication. Only
visible **ext4, xfs and btrfs** mounts are currently supported. NFS, CIFS, FUSE,
tmpfs and unknown types fail EXDEV. Adding another type requires review/tests.
This is conservative filesystem-type attestation, not proof that a block device
is physically local or power-loss durable. Existing same-filesystem, protected
staging, no-follow, verified bytes, fsync and exclusive publication remain.
The general legacy adapter's default hook is a no-op, preserving its API.

## 2. API, DB, configuration and security

No new HTTP route, schema, job/lease/checkpoint/DDP state, scheduler behavior or
runtime selection. Phase 1 general-key adapters remain low-level interfaces;
new runtime callers must use strict admission. Standalone setup bundles still
import correctly. No installed Pi guard/unit changed in this turn.

New trusted-service API from `hybrid_storage.health`:

```python
request = ProbeRequest(
    role="nfs",
    root="/srv/mlnfs/inputs",
    source="192.168.10.214:/srv/ml-shared/objects",
    sha256=authorized_file.sha256,
    size_bytes=authorized_file.size_bytes,
)
supervisor = HealthSupervisor(spool_dir=protected_local_directory)
result = supervisor.check(request, timeout=5.0)
```

This is an integration example, not a command executed on a Pi or a new config
field. Use one long-lived supervisor per trusted service. Expected identity must
come from the authenticated owner/job's authorized manifest, never raw student
JSON. For controller-local checks use `role="local"` without a source. Requests
reject malformed roots/digests/sources, bool sizes, unknown roles and oversized
payloads. A digest, valid request or healthy result grants no owner/job authority.

`verify_request` is the synchronous child operation: no-follow regular-file open,
exact fstat size, streaming bounded by expected size plus one, SHA-256, and
before/after device/inode/size/mtime/ctime agreement. Ready requires the full
expected bytes and successful mount-context completion. No contents are returned
or workspace created. Future staging must verify the actual bytes it copies;
health is advisory, not a reusable safe-open or launch capability.

Availability here means the authorized bytes are readable and verifiable from
the admitted view now. Kernel-cached reads may succeed with an unreachable server.
This is not a ping, fresh-wire-read guarantee or promise of future availability.
No global cache drop or write probe is used.

| Result | Meaning |
|---|---|
| `ready` | Complete expected read verified in the child before deadline. |
| `mount_unavailable` | Missing/wrong/unsafe/replaced mount or descriptor identity. |
| `source_missing` | ENOENT during an admitted lookup. |
| `permission_denied` | EACCES/EPERM, original errno retained. |
| `corrupt` | Size/hash/metadata-stability mismatch. |
| `io_error` | Other filesystem errors with errno, including EIO, ESTALE, ETIMEDOUT, ENOSPC, EROFS and locality EXDEV. |
| `invalid_input` | Other rejected file shape/type. |
| `timeout` | Caller deadline expired; late success stays failed. |
| `probe_failed` | Child crash, malformed/oversized output or wrong identity. |
| `busy`, `closed`, `probe_reconciled` | No new readiness: owned capacity held, admission closed, or another observer consumed the result. |

Local spool/setup/spawn errors raise rather than fall back. Kernel ETIMEDOUT is
an errno-bearing I/O error; the supervisor deadline is a separate lifecycle result.
The four flags remain legacy/false/false/resource and unsupported hybrid startup
is still rejected. No DB import, migration or production object copy occurred.

### Deadlines and retained ownership

The supervisor starts an isolated Python read-only child with closed inherited
FDs and a local temporary output file. It avoids pipe-draining and unbounded
post-timeout waits. The budget defaults to5s; valid values are finite positive
numbers up to300s. OS process creation, scheduling and local setup are not hard
real-time operations. Completion observed after the deadline is rejected.

On timeout, request termination of only the owned child and return ready=false,
PID/probe ID and pending state. A still-live child remains registered and holds
capacity; new checks return busy. `reconcile()` reaps only owned Popen handles,
closes local output and discards late success. `close()` stops admission and
requests termination but retains live children. Keep the supervisor alive and
reconcile until `pending()` is empty; never drop/recreate it to bypass capacity.
The trusted child cannot publish an object/checkpoint or update a job.

The [Python subprocess timeout contract](https://docs.python.org/3/library/subprocess.html)
does not establish that a child exited. Hard NFS kernel I/O can outlive a signal.
This component bounds the caller's child wait and tracks ownership; it cannot
promise kernel cancellation. Do not call synchronous `verify_request` or raw NFS
reads from Flask, scheduler or lease threads.

Ownership is in memory, **not a durable daemon/restart journal**. No service is
enabled here. Future integration must retain/drain the supervisor and establish
restart reconciliation before production use. A parent crash can leave a read-only
child holding a mount reference; it cannot commit stale data but needs operator
reconciliation. Controller SPOF and tenant process/network isolation remain open.

## 3. Tests and actual results

Pre-edit baseline: **96 passed, zero failed/skipped** in the five existing
input/client/adapter/config/job-storage suites. Full logs and hashes are in
[evidence](12-input-health-evidence.json).

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py test_storage_health.py test_storage_inputs.py test_nfs_client_setup.py test_nfs_server_setup.py test_storage_backends.py test_storage_config.py test_job_storage.py
```

**144 passed, 0 failed, 0 skipped; 7/7 files**: health26 + input18 + client28 +
server22 + adapter24 + config17 + job-storage9. NFS-positive tests explicitly mock
kernel mount/FD identity over temporary files. Production has no fake-mount switch.
After anchoring the local output spool creation through its attested directory
FD, the 26 health tests were rerun and passed. Syntax is checked for Python3.11;
this turn's execution used the controller environment, not physical Pi Python.

New coverage: mandatory source, actual ordinary-directory rejection despite valid
bytes, custom targets, RO VFS/rw superblock, wrong source/type/options, missing
metadata, FD/mount races, missing source, permission/I/O errors, corruption/size/
mutation, symlinks/FIFO, RO enforcement, local publication admission, malformed
requests/results, late completion and spawn cleanup. Real child processes verify
a local object and reject an ordinary directory as NFS. A real disposable sleep
process verifies timeout termination/reaping. A fake uncancellable process models
pending ownership, busy/close/reconcile and fenced late success; **no real
uninterruptible kernel task was created or claimed tested**.

Earlier 3.1 fake-NFS tests now supply an expected source and explicit metadata
mocks while preserving their path/authorization/roundtrip assertions. Existing
flag-off portal/upload/scheduling, legacy snapshots/recovery and fake setup/
idempotence tests pass. The live DB and existing legacy source/flags are compared
with beginning-of-turn hashes in evidence.

No SSH, live NFS/Pi fault, reboot, throughput, production training, full DDP,
actual controller failure or parent-crash recovery was tested. Nothing deployed.

## 4. Manual verification

Run the 144-test command above from the application root, then:

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python -m hybrid_storage.config
python3 -m json.tool docs/storage/12-input-health-evidence.json > /dev/null
git diff --check
```

Expected: 7/7 files pass; legacy/false/false/resource; valid JSON; no whitespace
diagnostics. `tests/test_storage_health.py` contains complete safe local examples.
Do not redirect fixtures to the live DB, alter a mount, enable hybrid flags or
signal an unrelated PID to reproduce them.

## 5. Rollback and residual risks

Repository-only patch: `/tmp/hybrid-storage-3-2.patch`. From the parent Git root,
review and first run:

```bash
git apply --reverse --check /tmp/hybrid-storage-3-2.patch
```

Reverse only this turn after checking concurrent work. Preserve prior changes
and commit `ddc9a7b`. No host rollback or service restart is needed; no runtime
caller was enabled, and tests reaped their real children. Never reset a DB,
unmount a legacy share or delete a job/checkpoint. A future caller with pending
work must reconcile that owned process before retiring its supervisor; restore
transport before normal unmount.

Remaining limits: deadline is not kernel cancellation; raw synchronous NFS reads
may block; health is not staging/launch authority or a verified persistent copy;
pending ownership is not restart-durable; supported local filesystems are
deliberately restricted. Authentication, tenant isolation, runtime selection and
migration remain gated. **The controller is still a single point of failure.**

## 6. Acceptance — STOP

**3.2 PASS for component/fake/local integration scope:** exact mount/FD admission,
availability via verified authorized reads, explicit errors/corruption, supervised
deadlines with pending ownership, RO semantics and no underlying-directory write
fallback. Legacy/default-off tests pass. **Phase 3 remains incomplete; next is
3.3. STOP.**
