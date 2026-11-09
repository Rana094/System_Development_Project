# Phase 8.2 — trusted immutable checkpoint publication

9 November 2026. **Local interface/integration acceptance PASS; no deployment or
checkpoint-mode execution enablement. STOP before 8.3.** Started on `main` at
`38d31c22bceb990166ef873f2be60b6ded1417f5`, preserving the uncommitted 8.1 report,
evidence and progress entry. Saved all 274 inventoried starting files and status
under `/tmp/hybrid-8-2/before/`, `before.json`, `status-before.txt` before edits.
No stash/reset/commit, live DB/migration, service, SSH/Pi, mount, export, firewall
or deployment action occurred. [8.1](30-checkpoint-audit.md) is the audited baseline.

## Behavior and exact files

New files:

| File | Interfaces and behavior |
|---|---|
| `hybrid_storage/checkpoint_publication.py` | Typed `CheckpointPublisher` protocol, frozen `CommittedCheckpoint`, explicit `LocalCheckpointPublisher.initialize/publish/committed/head`. No DB opening, automatic migration, launcher, retention or latest-file replacement |
| `hybrid_storage/checkpoint_schema.py` | Explicit `migration_plan/migrate/installed` with exact namespace/checksum verification; controller-local FULL/FK requirements |
| `hybrid_storage/checkpoint_files.py` | Descriptor-anchored private local roots, no-follow regular files, bounded copy/hash/free-space admission, existing archive validator reuse |
| `hybrid_storage/checkpoint_transfer.py` | `CheckpointCollector.sync` uses authenticated headers and bounded streaming on the existing HTTP checkpoint GET route. Reuses operator-provisioned per-worker inventory credentials |
| `agent/checkpoint_service.py` | Explicit idle-bootstrap `CheckpointService` / `install_checkpoint_service`; authenticated frozen LOCAL worker snapshots served by the existing route, single in-flight spool slot |
| `tests/test_checkpoint_publication.py` | 25 local publication/authentication/integrity/concurrency tests |
| This report and `31-checkpoint-publication-evidence.json` | Commands, hashes, results, scope and rollback |

Existing modifications are only appended sections in `hybrid_storage/README.md`
and `docs/storage/implementation-progress.md`. Existing controller/agent runtime
files, SDK/archive formats, checkpoints, config and 8.1 documentation remain
byte-identical to their starting contents. SDK import compatibility follows the
project's existing non-package `checkpoint_protocol`/`execution_lease` convention;
the agent Flask runtime is not imported to validate an archive.

## API and trust boundaries

Nothing installs these services or calls the new collector at normal startup.
No existing legacy sync/restore call is redirected. No config flags were added or
changed: shipped values remain `legacy`, cache false, staging false, `resource`.
The ordinary controller config gate and `StagingDispatch.authorize` checkpoint
mode rejection stay in place until committed-source recovery is integrated.

The explicit controller interface accepts an already authenticated actor/job and
producer, binary stream, server-generated 32-hex `publication_id`, exact previous
head ID (or None), expected SHA-256 and byte count. Students cannot invoke it via
a new route or provide filesystem destinations. It requires immutable registered
NFS inputs, custom checkpoint mode, RUNNING staged attempt/history, exact rank-zero
placement, current source/dataset authorization, reservations, prepared environment
identity and live all-rank wall/BOOTTIME/controller-boot/generation/lease fences.
The journal binds both the unchanged original manifest SHA and immutable input
binding/version. Single v1 and complete CPU Gloo DDP v2 archives remain unchanged.
No privileged publisher deserializes student tensor/pickle state.

The worker service intercepts **existing** `GET /jobs/<job>/custom-checkpoint`
only when explicitly installed. Its authentication hook precedes the old optional
query-param download fence, including when other staging hooks are installed.
Other endpoints retain their behavior. On this explicitly protected checkpoint
route, unsigned downloads and the old unsigned restore POST are rejected. This
GET protocol does **not** authenticate multipart restore; that remains blocked
for 8.3 rather than opening a bypass. Fresh requests bind worker/job/attempt,
generation/lease ID, nonce, timestamp, method and route with domain-separated HMAC.

Request header `X-DL-Checkpoint-Request` and response header
`X-DL-Checkpoint-Receipt` contain bounded base64 canonical JSON (maximum 8192
characters each). `X-DL-Checkpoint-MAC` authenticates them. The signed receipt
binds the complete fresh request plus schema version, whole-archive SHA and size.
The collector rejects redirects/partial status, wrong MAC/context/version, excess
size, truncated or changed body and response replay against a different request.
GET requests have two-second freshness; repeated authorized reads within that
window are not a mutating single-use command. Publication idempotency/CAS, not a
nonce cache, protects metadata transitions. No multi-GiB JSON/base64 body is used.

Worker candidate paths derive only from a validated job ID and prepared job.
Read opens reject symlinks, FIFOs and hardlinks. Copy freezes a regular candidate
through a stable descriptor into an anonymous private local file, checks source
fingerprint before/after copying, expected format/environment/count and current
local lease, then signs its size/digest. No writable cache hardlink or trainer NFS
RW privilege is introduced. One spool lock covers snapshot creation **and** the
active HTTP file lifetime; WSGI file close releases it. Failure paths close the
file and release the slot. A staging service installed in either bootstrap order
must have the same credential and non-overlapping cache/input/workspace roots.

HMAC authenticates, not encrypts, existing HTTP. Production OS/network isolation
must keep credentials, controller store and worker spool inaccessible to student
code; these tests do not supply or prove that containment. Malicious code can
fabricate its own training state; structural integrity does not prove honest ML.

## Publication, schema and failure behavior

Operator-provisioned roots must be private service-owned LOCAL directories. The
controller root cannot overlap the exported input objects or their scratch root.
Worker spool must be outside trainer job directories and, when staging is present,
outside cache/NFS/private-input roots. Symlink paths, permissive roots and remote/
unsupported mount identities fail closed. Root pathname/device/inode is bound to
the database; substituting a fresh empty store on restart is refused.

1. Under a short SQLite write transaction, authorize the complete live attempt,
   compare expected previous head and insert RECEIVING intent. Each random
   publication ID is permanently bound to that attempt, worker, byte hash/size,
   input version, source fences and expected head.
2. Write `pending/<publication_id>/candidate` under a protected attempt-specific
   0700 directory. The row binds that directory to the attempt; the ID is not a
   student path. Read in at most 1 MiB chunks and at most declared size plus one
   byte; check configured maximum, available disk/reserve and exact size/SHA.
   Fsync file, validate the unchanged archive by a controlled descriptor alias,
   recheck the attempt and metadata, record VERIFIED then PUBLISHING.
3. Fchmod 0400/fsync and publish `commits/<publication_id>.dlckpt` using existing
   Linux renameat2(RENAME_NOREPLACE), then fsync both directories. Never replace
   a committed file. Existing latest.dlckpt/latest.ddpckpt files stay untouched.
4. In one short BEGIN IMMEDIATE, recheck input authorization, current attempt,
   all live rank leases/reservations, producer generations and expected old head;
   reject progress/completion regression. Atomically mark COMMITTED, compare-and-
   swap head/revision and update existing jobs checkpoint path/time/size fields.
   No DB transaction spans the network transfer, file hashing or archive parsing.
5. Return a path-free frozen receipt. Exact-ID `committed`/`head` access rehashes
   protected immutable bytes outside the metadata read transaction. Missing,
   writable, linked or corrupt committed files fail closed.

An already COMMITTED matching idempotency key returns the original receipt even
after lease expiry or a newer head; it never advances/rewinds head again. A key
with different bytes/job/owner/worker/expected-head conflicts. Interrupted or
REJECTED IDs cannot be reused; retry a failed transfer using a fresh server key.
Ordinary failures retain REJECTED candidates/orphan snapshots. A simulated hard
crash can leave RECEIVING/VERIFIED/PUBLISHING, but those never become a head and
restart never automatically promotes them. This is deliberate conservative
reconciliation, not automatic salvage or deletion. Two concurrent writers with
the same expected head can produce only one winner. Finalization serializes with
existing cancel/retry writers through SQLite; a late/expired producer cannot
commit. A missing known committed snapshot never silently means start from zero.

The explicit additive migration creates four tables, two indexes, ten triggers:
`hybrid_checkpoint_migrations`, `hybrid_checkpoint_store`,
`hybrid_checkpoint_publications`, `hybrid_checkpoint_heads`. Immutable identity,
metadata/state-transition, no-delete and head-CAS guards retain history. Existing
rows, migration checksums and PRAGMA user_version are preserved. Checksum:
`f6daea358292dacf365eb8f3fd3097b0cac89ef20c95ac787a781ea8f87e5a10`.
It is **not applied to the live DB**, and no constructor auto-migrates it.

## Safe setup samples and remaining integration

These are explicit API/bootstrap samples, **not commands run on this installation**:

```python
# Dedicated controller-local FULL/FK connection and existing StorageRegistry.
from hybrid_storage import checkpoint_schema
from hybrid_storage.checkpoint_publication import LocalCheckpointPublisher
from hybrid_storage.checkpoint_transfer import CheckpointCollector

plan = checkpoint_schema.migration_plan(registry.db)  # Review before application.
# checkpoint_schema.migrate(registry.db)              # Explicit operator action.
# Root must already be privately provisioned on local disk, outside input exports.
publisher = LocalCheckpointPublisher(registry, Path('/var/lib/dl-controller/checkpoint-store'))
# publisher.initialize()                             # Explicit operator action.
collector = CheckpointCollector(publisher, credentials, get=requests.get)
# collector.sync(actor, job_id, reserved_rank_zero_node, current_lease,
#                publication_id=secrets.token_hex(16), expected_head=head_id)
```

Worker equivalent: construct `CheckpointService` with the existing trusted
worker root, its privately loaded per-worker `Credential`, and a separate local
spool such as `/var/lib/dl-node/checkpoint-spool`; register
`install_checkpoint_service(agent_module, service)` during reviewed idle bootstrap.
No directories, keys, mounts or services were created from these samples. The
existing `load_credential(..., expected_worker=...)` private credential loader may
be reused; keys never belong in student specs. No new storage.json fields exist.

Default size bound is 10 GiB plus 16 MiB container allowance; default free reserve
is 256 MiB at each copy side. Both are explicit constructor parameters, not new
feature flags. Stream chunks are bounded to 1 MiB and connect/read timeouts default
to (2,10) seconds. Idle socket timeouts are not a total-transfer deadline. Large
copies require valid lease renewal throughout; without it the final commit is
rejected. At previously reported ~100 Mbps, large checkpoints may outlast a
lease; no throughput benchmark or long-transfer renewal acceptance is claimed.
Production WSGI response timeout/disconnect handling needs deployment review.

8.3 still needs authenticated restore upload/staging, committed-source dispatch/
resume, compatibility mirrors or reader guards, and final metrics/event integration.
Do not run legacy latest-only readers concurrently as authority for this new
interface, or lift the checkpoint gate prematurely. Legacy checkpoint adoption/
automatic conversion/moving is not implemented. Current tests deliberately build
synthetic valid RUNNING checkpoint staging rows because the production dispatcher
still rejects checkpoint mode; they do not claim a runnable NFS recovery job.
Controller/SQLite/authoritative storage remains a single point of failure.

## Actual verification

All Python commands ran from disposable source copies/DBs/roots, no listeners,
hardware or Internet transfers:

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_custom_checkpoint.py test_execution_ownership.py
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_checkpoint_publication.py test_custom_checkpoint.py test_execution_ownership.py test_staging_dispatch.py test_staging_recovery.py test_staging_lifecycle.py test_storage_config.py test_storage_selection.py
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_checkpoint_publication.py test_custom_checkpoint.py test_student_portal.py test_staging_portal.py
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_checkpoint_publication.py
git diff --check
```

| Selection / stage | Actual result | Log under /tmp |
|---|---|---|
| Pre-edit baseline | 16 tests, 2/2 files PASS | hybrid-8-2-baseline.log |
| Eight-file regression snapshot (15-test publication version) | 148 tests, 8/8 PASS | hybrid-8-2-regression.log |
| Concurrent-writer expansion | 18 tests PASS | hybrid-8-2-concurrency.log |
| Later publication/legacy/portal boundaries | 60 tests, 4/4 PASS | hybrid-8-2-final-boundaries.log |
| Expanded publication suite | 24 tests, 1/1 PASS | hybrid-8-2-final-publication.log |
| Final HTTP empty-chunk boundary | 25 tests, 1/1 PASS | hybrid-8-2-final-stream.log |

Zero final failures/errors/skips; overlapping counts are not added. Initial runs
caught a package-import mismatch (agent is a non-package SDK layout in existing
entry points) and three tests asserting ValueError where the intended rejection
was PermissionError. Corrected without weakening rejection or skipping tests.
The final source and log fingerprints are in adjacent evidence JSON.

Coverage includes signed existing-route single/DDP transfer, exact immutable bytes,
atomic head/job metadata, retained history, concurrent independent SQLite writers,
idempotent acknowledgement, file-before-DB crash/restart, cancellation and expiry
before commit, revoked ACL/reservation, corrupt/unsafe/oversized/partial streams,
MAC/context replay, disk admission/error, symlinks, local/private roots, schema
idempotence/user_version preservation and blocked late bootstrap. Archive payloads
in the new suite are intentionally opaque tensor bytes: it validates publication,
not numerical training. The unchanged legacy suite includes real stochastic CPU
resume with exact model/Adam/scheduler/Torch RNG equivalence. No new-mode resume,
8.4 full fault matrix, physical DDP, NFS outage or live hardware claim. Previously
documented real-DDP socket-EPERM suites were not rerun/escalated.

## Manual verification, rollback and residual risks

Run the last isolated command above: expect 25 passing tests and no live DB writes.
Review the schema plan/checksum in a disposable local registry; repeated migrate
must be unchanged and preserve user_version. In the authenticated fixture, unsigned
GET/POST must be rejected; exact signed bytes must produce COMMITTED and an immutable
file; interrupted/expired/losing writers must leave the old head unchanged. Do not
turn on flags, apply a migration to a live DB or install services merely to verify.

The scoped diff `/tmp/hybrid-8-2/8.2.patch` is relative to saved **8.1 working-tree
contents**, not HEAD. From the Git root, verify
`git apply --reverse --check /tmp/hybrid-8-2/8.2.patch`. Only for a reviewed source
rollback after preserving later edits, reverse that patch. No rollback was applied.
Keep all databases/additive tables, jobs, cache data, committed snapshots and
identified candidates; old code ignores the new namespace. No DB drop, restore,
checkpoint deletion, service stop, NFS change or repository reset/clean is required
for this unactivated source rollback. /tmp artifacts are session-local; preserve
the patch/snapshot elsewhere before relying on them later.

Retained rejected/incomplete candidates consume disk until separately reviewed
retention/reconciliation work. Free-space checks are admission checks, not a global
multi-writer quota. Disk-full and uncertain fsync/DB outcomes fail closed and may
retain nonrecoverable orphan files. Production isolation and transfer limits need
review; bootstrap is explicitly inactive. All-three-Pi validation still requires
pi01, pi02 and pi03; no previous two-Pi exception waives it here.

**8.2 local PASS; STOP. 8.3 committed checkpoint recovery and 8.4 full acceptance
remain pending.**
