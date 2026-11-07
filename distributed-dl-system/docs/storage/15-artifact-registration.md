# Phase 4.1 — immutable content-addressed registration

8 November 2026, Asia/Dhaka. **4.1 PASS for trusted local registration. STOP
before 4.2.** Last accepted boundary was [3.4](14-phase3-validation.md); the user
then supplied Phase 4 with one subphase per turn. Dataset manifests/ACLs (4.2),
job bindings/migrations (4.3) and legacy enrollment (4.4) are not implemented.
This is not overall Phase 4 acceptance or an executable hybrid training system.

## 1. Behavior, interfaces and files

| File | Change |
|---|---|
| `hybrid_storage/artifacts.py` | New explicit trusted-service registrar, immutable result and mismatch error; bounded copy/readback, no-replace atomic rename, deduplication and durability checks. |
| `tests/test_artifact_registration.py` | 24 local tests, including actual concurrent threads/processes and subprocess crashes at publication boundaries. |
| `docs/storage/15-artifact-registration.md`, `15-artifact-registration-evidence.json` | Contracts, commands, raw results, hashes, rollback and limits. |
| `hybrid_storage/README.md`, `tests/README.md`, `docs/README.md`, `docs/storage/implementation-progress.md` | Usage, navigation and current progress. |

New API in `hybrid_storage.artifacts`:

```python
registrar = ArtifactRegistrar(operator_owned_objects_root,
                              staging_root=operator_owned_private_scratch)
artifact = registrar.register(authorized_binary_stream,
                              expected_size=authorized_exact_size,
                              expected_sha256=authorized_digest_or_none)
```

`expected_size` is a genuine nonnegative integer, required as the admission
bound. An optional expected SHA-256 must be exactly 64 lowercase hexadecimal
characters and match the content. Without it the digest is computed from the
stream. The source needs only `read(n)`, not seeking, and remains caller-owned.
Nonbinary/unbounded-returning streams are rejected. Each read/copy/hash operation
uses at most 1 MiB per buffer, independent of file size. This assumes the source
honors `read(n)`; a malicious Python object can allocate internally before return.

The frozen result has `sha256`, `size_bytes`, `key`, `created`. The key is
`sha256/<first-two>/<digest>`; with root `/srv/ml-shared/objects`, the full layout
is `/srv/ml-shared/objects/sha256/<first-two>/<digest>`. `created=False` means a
complete existing object was verified and reused, not merely that a name exists.
`ArtifactMismatch` signals size/digest/content/type instability. Permission and
I/O errors propagate; they do not trigger fallback or return successful metadata.

### Publication and concurrency

1. Validate expected metadata before filesystem access/source consumption. Open
   every root component with no-follow directory descriptors. Require protected
   service-owned root/scratch, supported local mount identity and equal filesystem
   device. Root and scratch are pre-provisioned, absolute, nonoverlapping paths;
   scratch is owner-only 0700, outside all exports. Group/world-writable roots
   fail closed. No constructor/import filesystem activity occurs.
2. Create an O_EXCL/O_NOFOLLOW 0600 scratch file with a random 128-bit name.
   Colliding names are retried at most eight times without touching those files.
   Stream-copy/hash, allowing only one extra byte to detect overflow beyond the
   exact expected size. Flush, rewind and independently verify the copied regular
   file's size/hash and stable inode/size/timestamps. No mutable source hard link.
3. Set the completed inode to 0444 and fsync it. Create only the derived
   `sha256` and prefix directories through anchored FDs, validating local mount
   type, ownership and nonwritable-by-group/others permissions. Fsync their
   parents on creation or retry. No student-chosen path/name enters publication.
4. Call Linux `renameat2(..., RENAME_NOREPLACE)` between the private scratch and
   final directory FDs. This atomically publishes a complete inode, with no
   interval exposing partial bytes and no replacement of an existing object.
   The host's glibc declaration in `/usr/include/stdio.h` and Linux
   `/usr/include/linux/fs.h` define the ABI/flag used. `ctypes` is stdlib; no
   added dependency, shell command or transport. Missing API/kernel/filesystem
   support propagates an error; no unsafe plain-rename/replace fallback.
5. If a competing writer already published that digest, open the existing
   regular, service-owned, read-only file without following links. Recompute its
   size/hash and compare every byte to the incoming verified scratch, checking
   stable metadata. This also rejects a simulated same-digest/different-content
   collision. Never repair, chmod or overwrite a conflicting existing object.
   Fsync the reused object too, so a retry can finish a prior interrupted commit.
6. Fsync final and scratch directories before success; remove only this call's
   owned scratch on errors/dedup, then fsync scratch. Cross-process arbitration
   is the no-replace rename, not an in-memory lock. Six thread callers and three
   independent process callers each yield exactly one creator and one stored
   object. Legacy `publish_artifact` still rejects duplicates as before; its
   existing link-publication contract is unchanged.

A crash before rename can leave an inaccessible private orphan, never a partial
object in the export. There is no orphan sweep/GC here. A crash or fsync failure
after rename may leave a complete object despite an error/no response. Retry the
same authorized content: the registrar verifies bytes, preserves the inode and
repeats durability operations. Never delete the final object to "roll back" a
failed response. Process-crash tests establish this visibility/retry behavior;
they are not power-loss/disk-durability tests.

## 2. API, database, configuration and security boundary

Only the explicit Python component API is new. No HTTP/CLI upload route, worker
protocol, DB migration/table, job/lease/checkpoint state or active setting changes.
No existing runtime Python file was modified. Existing `stage_job`, controller
selection, manifests and all recovery/DDP flows remain intact. The registrar has
no runtime call sites and does not move/copy pre-existing jobs automatically.
SQLite remains local; its main-file hash matches the turn baseline. No live
controller import, schema initialization or DB write was performed.

The API requires an already authorized stream and operator-provisioned roots.
SHA-256 and `created` are not access credentials; no public digest lookup or
cross-owner access is added. Physical reuse here does not implement dataset ACLs.
Those belong to 4.2/4.3 before any student-facing enrollment. Do not expose object
paths, existence results or the NFS view directly to untrusted training code.
NFS exports remain restricted/read-only/root_squash; publication is controller-
local only. AUTH_SYS is not a per-student security boundary. Checkpoints retain
the trusted existing publisher protocol, never a writable input export.

Only service-owned, protected ext4/xfs/btrfs roots are admitted, using the existing
mount/descriptor helpers. Filesystem-type attestation is conservative and does
not prove physical locality, exclusive access by privileged processes or hardware
durability. All trusted path ancestors/ownership provisioning remain operator
responsibilities. Runtime stays `legacy / false / false / resource`; NFS intent
is still HELD. No package install, service restart, `/etc` edit, SSH/Pi/mount
operation or Internet-dependent worker transfer occurred.

## 3. Actual tests and evidence

From the application root:

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_artifact_registration.py test_storage_readers.py test_storage_selection.py test_storage_config.py test_job_submission.py test_job_storage.py test_student_portal.py test_queue_controls.py test_resource_allocation.py test_storage_inputs.py test_storage_health.py test_storage_backends.py test_nfs_client_setup.py test_nfs_server_setup.py
```

Final: **236 passed, zero failures/skips, 14/14 files** (24 new registrar tests +
the prior 212). The pre-change baseline was **212 passed, zero failures/skips,
13/13 files**. New tests also passed separately, 24/24. No baseline failures
were observed. An initial new-test run failed because fixture directories inherited
a group-writable umask and a fixture helper rejected overridden keyword values;
the fixtures were corrected, preserving the strict production permissions.
Initial failure output is retained, not classified as a pre-existing failure.

Coverage includes bounded nonseekable streams, empty objects, computed/expected
SHA, changed content, dedup/inode preservation, simulated hash collision,
truncation/overflow, readback corruption, source EIO, injected write ENOSPC,
unsafe roots/scratch/prefixes, symlinks/FIFO/directory targets, cross-device or
remote admission, scratch-name collision, unsupported rename errors, actual
thread/process races, subprocess exits before/after rename, post-rename fsync
failure and retry, import side effects and unchanged legacy behavior. No blanket
skip hides unavailable rename support on this host: the real syscall executes
in the concurrent and ordinary successful tests.

The source-copy runner excludes live DB/WAL and runtime roots; all new publication
and crash fixtures live in disposable local directories. Existing selections were
reviewed for fake/local operation. No real full disk, power outage, NFS/Pi fault,
actual training/DDP recovery or benchmark was run. Static checks and exact command
arrays, full logs, return codes, Python version and before/after source/DB hashes
are in [evidence](15-artifact-registration-evidence.json).

## 4. Manual verification and expected outcomes

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_artifact_registration.py
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python -m hybrid_storage.config
```

Expected: 24 tests pass; config reports legacy execution, both new flags false,
resource policy. The first command verifies two trusted streams with identical
bytes reuse one inode, changed bytes yield a different digest, conflict rejection,
concurrent writers and crash reconciliation. Both commands were run. For a manual
API experiment, use only newly provisioned disposable 0755 object/0700 scratch
siblings on supported local storage, not actual jobs or `/srv`. Invoke the API
shown above twice with separate `io.BytesIO(b"example")` streams and size 7:
expect `created=True`, then `False`, equal key and one 0444 object. Supplying
`b"changed"` creates a different key. ACL, dataset version and legacy enrollment
verification remain later subphases; this example must not be wired into upload
before those checks exist.

## 5. Preservation, rollback and residual risks

Git root is the application's parent `System_Development_Project`, branch `main`,
HEAD `ddc9a7b4a0a61288462c1999fd8c8ef1a2d01977`. Existing staged/unstaged/untracked
work is preserved; no stash/reset/clean/commit. Before hashes/copies of 124 files
and logs are under `/tmp/hybrid-storage-4-1/`; initial Git status is at
`/tmp/hybrid-storage-4-1-status-before.txt`. Runtime sources, DB bytes and actual
flags are unchanged apart from the newly added, unwired registrar module.

A scoped diff against this turn's before-files is at
`/tmp/hybrid-storage-4-1.patch`. From the parent Git root, review then reverse
only this turn if needed:

```bash
git apply --reverse --check /tmp/hybrid-storage-4-1.patch
git apply --reverse /tmp/hybrid-storage-4-1.patch
```

The check passed at completion. Preserve any later edits; do not reverse the
entire dirty Git diff or reset to HEAD. No service/config/DB rollback is needed.
Removing registrar code must not delete any objects published in later use.
Never restore an old DB over newer jobs or delete existing scratch/data as an
automatic cleanup operation.

Residual risks: registration performs synchronous local I/O; it is not a job
quota system, cancellation service, background publisher or bounded-latency
worker. Concurrent registration needs scratch capacity for each incoming file
even on a dedup hit. A crash can leave private scratch or an unreferenced complete
object; ownership-aware reconciliation/GC is later work. Protected ownership is
the immutability boundary; a privileged process can still alter bytes. No dataset
manifest/version, ACL registry, job FK/index migration or lazy legacy registration
exists yet. The central controller remains a **single point of failure**. All
three Pis remain the default future cluster test requirement; pi01 is still
deferred under the existing explicit exception, and no cluster acceptance is
inferred from local tests.

## 6. Acceptance and stop

| 4.1 criterion | Result |
|---|---|
| Canonical SHA-256 identity and `objects/sha256/<prefix>/<digest>` layout | PASS |
| Bounded stream/hash/copy, unique private scratch, copied-byte verification | PASS |
| Fsync and same-filesystem atomic no-replace rename | PASS on this local Linux host |
| Concurrent writer safety and verified physical deduplication | PASS (threads/processes) |
| Legacy/OFF behavior and user-data preservation | PASS in the selected isolated regressions |

**4.1 PASS. STOP before 4.2.** Overall Phase 4 acceptance awaits manifests/ACLs,
precise job bindings, migrations and explicit legacy enrollment. No next subphase
or live deployment is implemented.
