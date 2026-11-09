# 8.3 — committed checkpoint recovery

Only subphase 8.3. Normal startup and shipped legacy/OFF/resource settings remain
unchanged. This adds an explicitly constructed coordinator; it does not deploy,
provision keys/isolation, migrate a running database or install services. Phase
8.4 and live pi01/pi02/pi03 validation remain pending. The controller, local
SQLite database and authoritative checkpoint store remain a single point of
failure.

## Behavior and interfaces

```mermaid
sequenceDiagram
    participant C as Trusted controller
    participant D as Local SQLite and private commits
    participant W as Reserved trusted worker agents
    participant T as Existing training SDK
    C->>D: Validate owned immutable inputs; freeze committed head per retry
    C->>W: Existing authenticated input staging, fresh leases
    C->>W: Signed restore of exact committed checkpoint to each private workspace
    W-->>C: Verified copy receipts, all ranks
    C->>W: Existing signed launch, after complete staging/restore barrier
    W->>T: Existing resume SHA/SDK; private LOCAL checkpoint
    T-->>W: Existing model/optimizer/RNG restore acknowledgements
    W-->>C: Signed control observation, exact process/attempt/lease
    C->>D: Confirm recovery once; existing RUNNING transition
    C->>D: Trusted publication of current-attempt final checkpoint
    C->>D: Fenced result collection and COMPLETED transition
```

`controller/checkpoint_recovery.py::CheckpointDispatch` extends the existing
`StagingDispatch`. Construction requires the exact `StorageRegistry`, authenticated
staging client, `LocalCheckpointPublisher`, `CheckpointCollector` and new
`CheckpointRestorer`, with the same per-worker credentials and explicit
`StorageConfig('nfs', True, True, 'resource')`. It is never installed automatically.
The base restart-only coordinator still rejects checkpoint jobs. Legacy jobs and
legacy HTTP checkpoint routes keep their behavior when these services are absent.

Before placement, `recovery_source` rechecks owner, source/dataset ACLs, original
manifest and immutable input binding, then verifies the exact COMMITTED head's
bytes. It freezes one source per job/retry. The source must come from an earlier
retry with the original manifest, binding, environment and fixed rank count.
Missing/corrupt known committed files, lost heads and version drift fail closed;
no mutable `latest` adoption, copying of old jobs, or fallback to zero progress.
A job with no known committed checkpoint can start cold, including a retry that
failed before its first commit. Old producer leases need not remain live; new
reservations and all destination leases must be fresh.

Existing resource selection, compatible prepared environments, fair queue,
concurrency accounting, reservations, cancellation and retry cleanup remain in
use. Inputs use existing verified missing-only cache staging. Each destination
receives the complete checkpoint archive required by its existing SDK, not a
fabricated sample-level cache entry. DDP retains the same world size. All ranks
stage inputs and restore files before ANY launch. Launch intent and ACK times are
persisted; ambiguous launches are fenced and cleaned up, never blindly replayed.

`CheckpointRestorer.stage` streams a pinned, rehashed committed descriptor over
raw **POST on the existing `/jobs/<job_id>/custom-checkpoint` route**. Requests
bind worker, job, attempt, lease/generation, stage ID, source metadata/hash/size,
nonce and two-second freshness using method/domain-separated per-worker HMAC.
Replies bind the complete request. Redirects, unsigned multipart restores and
wrong receipts/signatures are rejected on explicitly protected workers. Default
connect/read timeouts remain (2,10) seconds. Metadata headers retain the existing
8192-character cap; overlarge rank metadata fails closed.

`agent/checkpoint_restore_service.py` requires an unlaunched READY stage with the
exact destination fence/binding. It serializes with staging and the checkpoint
transfer slot, checks local free space for two archive copies plus reserve,
streams bounded chunks into an anonymous service-private temporary file, verifies
size/SHA and the unchanged single-v1 or DDP-v2 archive, and checks prepared
manifest/environment/source-attempt/rank count. It never deserializes tensors.
A unique `.part` copy is fsynced and renamed without replacement to the attempt's
private `checkpoints/resume.dlckpt`, mode 0400, single link. A separate protected
service receipt freezes the source. Retries accept only the same verified source;
incomplete receipt writes fail closed. Owned unpublished `.part` names are the
only temporary paths removed by this implementation. Committed checkpoints,
private published files, user outputs and databases are retained.

Launch rechecks receipt, hash, staged plan and lease. `agent/agent.py` uses this
private path and passes a trusted `--staged-resume` switch to `custom_runner.py`.
The existing launcher derives the path from its protected staged input receipt;
no student-selected resume path or cache/NFS root is passed to training.
`DL_RESUME_STATUS_PATH`, output/checkpoint writing locations, SDK formats and
existing resume logic are unchanged.

A signed start ACK proves launch, not restored state. Recovered jobs remain
SCHEDULED with reservations until signed control replies contain the existing
SDK's exact process/attempt/hash/step and model/optimizer/RNG acknowledgements.
DDP additionally reuses `distributed_dispatch.restore_confirmed` for every rank
and its existing all-rank/application-state checks. Missing ACKs remain pending
with bounded lease renewal; failed/completed-without-ACK or 180-second overdue
restores enter fenced cleanup. A restarted controller can reconcile a fully
acknowledged launch without relaunching it. Partial/unknown launch groups abort
conservatively. Cancellation, expired leases and forgotten worker stages cannot
lift the RUNNING gate.

Periodic collection uses the 8.2 authenticated publisher. Identical verified
snapshots do not create another head or measurement. New COMMITTED heads and
existing `jobs.checkpoint_*` metadata plus checkpoint events update in one SQLite
transaction. RESTORE_PREPARED / DDP_RESTORE_PREPARED describe complete copy ACKs;
RESUMED / DDP_RESUMED occur once per retry after SDK proof. Existing step/hash,
retry and DDP rank-count fields are retained for reporting. Student projections
allow numeric progress/completion only; internal paths, credentials and hashes
are not added to the student response.

Final result collection cannot complete a checkpoint job using an old or
non-final checkpoint. The final snapshot must be fully COMMITTED, hash verified,
completed, bound to the original inputs and produced by the current attempt.
The final metadata/confirmed-restore guard is rechecked inside the existing
locked `mark_completed` transaction. Three unsuccessful final-checkpoint polls
enter fenced cleanup/retry. Existing artifact collection and timing reports
remain unchanged; their transport/isolation is not replaced by this subphase.
Legacy latest-only CLI readers are not the authority for hybrid recovery; use
the committed journal and owner-scoped portal checkpoint progress.

## Files and schema

| Files changed in this turn | Purpose |
|---|---|
| `controller/checkpoint_recovery.py` (new) | Frozen source, restore/SDK barrier, periodic/final publication |
| `controller/staging_dispatch.py`, `staging_recovery.py`, `job_store.py` | Small extension hooks, compatible-environment selection and locked final guard |
| `hybrid_storage/checkpoint_restore.py` (new) | Authenticated restore client/protocol on existing route |
| `hybrid_storage/restore_schema.py` (new) | Explicit additive LOCAL migration |
| `hybrid_storage/checkpoint_publication.py`, `checkpoint_transfer.py` | Pinned committed reads, atomic events, unchanged-snapshot deduplication |
| `agent/checkpoint_restore_service.py` (new), `checkpoint_service.py` | Private verified restore and existing-route authentication |
| `agent/agent.py`, `custom_runner.py`, `staging.py` | Private resume input, revalidation, signed exact-process observations |
| `controller/staging_view.py`, `student_portal.py` | Numeric-only checkpoint/recovery progress |
| `tests/test_hybrid_checkpoint_recovery.py` (new) | 18 local recovery/compatibility/security tests, including real CPU SDK resume |
| `hybrid_storage/README.md`, `docs/storage/implementation-progress.md`, this report and evidence | Scope, results and rollback |

Restore migration v1 creates three tables, one index and six triggers:
`hybrid_restore_migrations`, `hybrid_restore_attempts`, `hybrid_restore_ranks`.
It stores frozen source/binding per retry, stage identity, per-rank copy receipt,
launch intent/ACK, restored/confirmed bits and final-checkpoint failure count.
The coordinator requires the prior immutable-input, staging, cleanup and checkpoint
schemas, controller-local SQLite with FULL durability and foreign keys, and the
existing recovery-events schema initialized explicitly. Frozen identity/source and
monotonic launch/restore proof guards retain history. No `user_version`, old
schema checksum, scheduler state, archive format or shipped config is changed.
Checksum: `f9924c3251ec9c94b8bac3a95426acb3e3b8937c6067d3e7b78a00917407a383`.
No migration was applied live.

## Verification

**8.3 local PASS.** Pre-edit baseline: 84 tests, 3/3 files. Final selected
versions: 157 distinct unittest cases across eight files, plus the original
executable legacy recovery script, all PASS. New suite: 18/18 PASS. No unittest
failures, errors or skips in these final selections; repeated runs are not added.

| Exact selection (all commands use the isolated runner below) | Actual result | Log |
|---|---|---|
| Pre-edit publication + staging dispatch + staging recovery | 84 tests, 3/3 PASS | `/tmp/hybrid-8-3-baseline.log` |
| Publication + dispatch + student portal + earlier 17-case new suite | 84 tests, 4/4 PASS | `/tmp/hybrid-8-3-final.log` |
| Legacy executable script + custom checkpoint + staging recovery + staging portal + config | 72 unittest cases plus legacy script, 5/5 PASS | `/tmp/hybrid-8-3-final-compatibility.log` |
| Expanded DDP final-commit/reporting acceptance | 17 tests PASS | `/tmp/hybrid-8-3-acceptance.log` |
| Final missing-worker-service guard + custom checkpoint + staging dispatch | 18 + 4 + 25 = 47 tests, 3/3 PASS | `/tmp/hybrid-8-3-final-guards.log` |

The pre-existing late-bootstrap fixture requests `/resources` only to establish a
served Flask app; in this sandbox psutil's network probe logs an EPERM/500 both
before and after this change. Its late-bootstrap rejection assertion passes.
Legacy negative tests also print expected corrupt-checkpoint and missing-final
messages. Those are not test failures. Early new-test fixture errors (missing
explicit recovery-events initialization, parent-process Torch import while Popen
was mocked, and preparing a replacement after enqueue) were fixed; initial failed
runs are retained in the evidence. Existing tests were not weakened or removed.
A test filename collision was corrected by restoring the original legacy test to
its pre-turn SHA-256 and using `test_hybrid_checkpoint_recovery.py` separately.


Exact commands/results and source hashes are in
`32-checkpoint-recovery-evidence.json`. All tests use temporary source copies,
disposable local DBs and worker roots. NFS facts and process launch/isolation are
mocked for lifecycle tests; no listener, SSH, live mount or Pi is involved.

The new real CPU test trains a stochastic model with Adam and StepLR for 12
updates; saves after update 4, exits after update 6, publishes the saved archive,
restores its committed bytes through the authenticated worker route into a new
private workspace and invokes the existing SDK in a fresh native subprocess.
Model, optimizer, scheduler, Torch RNG, Python RNG and NumPy RNG state equal the
uninterrupted baseline exactly (`torch.equal`, zero tolerance). The outer agent
launcher is mocked. This proves CPU state continuity, not deployed isolation or
genuine multi-process DDP execution. DDP tests use three distinct Flask worker
agents/caches/workspaces/credentials with mocked launch and SDK ACKs.

Repeat safe manual verification from the application directory:

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_hybrid_checkpoint_recovery.py
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_checkpoint_publication.py test_staging_dispatch.py test_student_portal.py
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_checkpoint_recovery.py test_custom_checkpoint.py test_staging_recovery.py test_staging_portal.py test_storage_config.py
git diff --check
```

Expected: 18 new tests PASS; new recovery remains SCHEDULED before SDK proof,
then RUNNING; warm unchanged inputs stage zero bytes; pi02 fixture receives
missing inputs; all three DDP fixtures restore before any launch; incorrect
source/auth/fence/cancel blocks launch; completion requires current final commit.
Legacy/config/portal suites pass, and the original legacy recovery script reports
all checks passed. Static diff check emits nothing.

Explicit bootstrap sample for a future reviewed disposable deployment only:

```python
# Existing LOCAL FULL/FK registry and prior schemas, private roots and keys.
from hybrid_storage import restore_schema
from hybrid_storage.checkpoint_restore import CheckpointRestorer
from checkpoint_recovery import CheckpointDispatch  # existing controller sys.path

plan = restore_schema.migration_plan(registry.db)  # read/review, no auto apply
# restore_schema.migrate(registry.db)              # separate explicit setup
restorer = CheckpointRestorer(publisher, credentials, post=requests.post)
runtime = CheckpointDispatch(registry, staging_client, publisher=publisher,
    collector=collector, restorer=restorer,
    settings=StorageConfig('nfs', True, True, 'resource'))
# Supply runtime to existing trusted portal/dispatch/monitor integration.
# Install CheckpointService + StagingService only at reviewed idle worker bootstrap.
```

Do not substitute this coordinator on a running process or use two coordinators
for the same cluster. No example above was applied to the installed system.

## Rollback and remaining risks

The turn snapshot is `/tmp/hybrid-8-3/before`; `/tmp/hybrid-8-3/8.3.patch`
contains ONLY this turn's source/docs delta, including additions. Review it before
rollback. From the Git root, the read-only check is:

```bash
git apply --reverse --check /tmp/hybrid-8-3/8.3.patch
# Only after checking subsequent user edits and with no active hybrid jobs:
# git apply --reverse /tmp/hybrid-8-3/8.3.patch
```

This restores modified 8.2 files to their pre-8.3 bytes and removes only new 8.3
source/docs files; it retains 8.1/8.2 work. Never use a reset, restore-to-HEAD or
blanket untracked deletion. No DB/table/data/checkpoint rollback is needed here
because live state was untouched. If later deployed, fence and reconcile active
attempts before reverting bootstrap; retain journals, private files and additive
schema/history. Disabling the runtime alone does not safely release reservations.

HMAC authenticates existing HTTP; it does not encrypt data. Trusted OS/network
isolation must prevent student access to credentials, service spool, cache and
NFS mounts. Readonly file mode alone is not a multi-tenant security boundary;
training code owns its private copy. Structural validation/SDK ACKs do not prove
honest student computation. Private-file retention needs a separate reviewed GC
policy. Socket idle timeouts are not total-transfer deadlines; large transfers
can outlast leases and fail closed. The 100 Mbps historical network was not
benchmarked. Clock synchronization and deployment WSGI timeouts remain operational
requirements. No physical crash, power recovery, real NFS outage, or genuine
Gloo multi-rank resume was tested in this turn. All-three-Pi validation remains
required; no earlier pi01 deferral is treated as a waiver.

8.3 acceptance status/results are recorded in the adjacent evidence file.
STOP before 8.4; full Phase 8 and production acceptance remain untested.
