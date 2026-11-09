# Phase 9.3 — one authoritative commit per consistent distributed save

9 November 2026, Asia/Dhaka. Implements **9.3 only**, following the accepted
[9.1 audit](34-ddp-audit.md) and [9.2 staging](35-ddp-staging.md). Verification
results are recorded below and in
[evidence](36-ddp-checkpoint-publication-evidence.json). STOP before 9.4.
No physical pi01/pi02/pi03 test, production enablement or deployment is claimed.

## 1. Implemented behavior and files

The supported DDP mechanism remains one rank per reserved worker, CPU Gloo,
fixed saved world size, and the existing `DistributedCheckpointManager` writer.
Rank zero writes one v2 ZIP_STORED archive containing metadata.json and all
rank-N.pt members after the existing coordinated save/acknowledgement collectives.
This is not PyTorch DCP, independently published per-rank files, or a format
migration. Final model-only results remain separate from recoverable checkpoints.

The privileged publisher already validated complete members, each hash, all
rank acknowledgements, original immutable inputs, prepared environment, group
identity and every current lease fence. The identified gap was save identity:
different controller-generated **transport publication IDs** could previously
commit the same SDK save more than once.

The publisher now recognizes an SDK save by original job + retry/attempt +
checkpoint_id, with group identity and exact full metadata/byte hash/size checked.
An identical already-committed save returns the canonical committed receipt.
Its new redundant transfer intent is REJECTED; head/revision, jobs.checkpoint_path,
commit event and canonical bytes are unchanged. Conflicting bytes under that
checkpoint_id raise RegistryConflict. A genuinely new SDK checkpoint_id remains
a distinct save, even at the same training step; ordinary progress/completion
monotonicity still applies. No world-size support changes are introduced.

| File | Exact change |
|---|---|
| `hybrid_storage/checkpoint_publication.py` | Internal `_CommittedSave`, `_require_new_distributed_save` and `_reject`; save identity checked under SQLite write serialization after validation and again immediately before COMMITTED/head CAS. |
| `tests/test_ddp_checkpoint_publication.py` | Fifteen focused complete-save/publication/concurrency/authority cases. |
| `tests/test_checkpoint_publication.py` | DDP fixture uses different save IDs and matching ACKs for different step/completion cuts, matching the existing real writer's unique IDs. Single-worker fixture unchanged. |
| `tests/test_hybrid_checkpoint_recovery.py` | Same correction for opaque DDP fixture saves, including retry identity; prior 9.2 watcher/cleanup changes preserved. |
| `tests/README.md` | Reproducible test commands and scope. |
| `docs/storage/36-ddp-checkpoint-publication.md`, `36-ddp-checkpoint-publication-evidence.json` | Report and test/preservation evidence. |
| `docs/storage/implementation-progress.md` | Append-only 9.3 ledger entry. |

Repository root is the application's parent,
`/home/rana/Projects/System Project/System_Development_Project`; branch `main`,
HEAD `de297dc7d99eb80e52498145fe05b033e5afdaad`. Starting status contained the
uncommitted 9.1/9.2 work. Before edits, 298 inventoried files and Git status were
saved under `/tmp/hybrid-9-3/before`, `before.json`, `status-before.txt`. Existing
changes are preserved; no stash/reset/commit, job/data relocation or live DB open.

## 2. Commit, API, DB and security boundaries

The authoritative committed manifest is the existing immutable
`hybrid_checkpoint_publications.metadata_json`, together with original input
binding, source fences, hash/size and COMMITTED phase. One logical save has one
canonical committed record and one atomically updated head/event, not N rank
publications. Existing schema and explicit migration checksums are unchanged.
SQLite and immutable checkpoint files remain on protected controller-LOCAL disk,
outside the read-only NFS input export. No new database table, migration, JSON-SQL
extension, transport, credential or endpoint is required.

Existing publication order is preserved:

1. Authorize the live original whole-group attempt and expected current head;
   persist a RECEIVING transport intent.
2. Stream/copy/hash into that intent's private candidate; verify the entire v2
   archive and expected names, sizes, all rank hashes and acknowledgements.
3. In a short write transaction, reauthorize all placements/fences and verify
   environment/group metadata. Check committed save identity before VERIFIED.
4. For a new save, preserve VERIFIED -> PUBLISHING, fsync and no-replace rename
   to a unique controller-owned immutable archive.
5. Under the final write transaction, reauthorize every fence, check save identity
   again, compare expected head and progress, then atomically COMMIT the metadata,
   head/revision, job checkpoint pointer and existing event.

Independent collectors can both validate/rename before one commits. The second
save-identity check serializes that race: identical bytes acknowledge the winner;
conflicting bytes are rejected. Only one record becomes COMMITTED and only one
head revision/event is added. Redundant private candidates or immutable orphans
are retained under the existing review/retention policy, never auto-promoted.
No failed/rejected/incomplete file is selected merely because it looks final.

New save attempts must pass current authorization **before** deduplication. A
revoked peer lease, later retry, missing reservation or changed group cannot use
an old matching UUID to bypass fencing or advance latest. Existing acknowledgments
of an exact previously COMMITTED publication ID remain readable after expiry;
they do not mutate the head. Replaying an older save under a fresh transport ID
can return its old committed receipt while leaving the newer head untouched.
The initial expected-head CAS remains mandatory; a stale expected head can still
require refetch/retry. A redundant transport ID marked REJECTED is not reusable;
use its returned canonical committed ID or a fresh trusted transfer ID.

The existing job/attempt index restricts the lookup to that attempt's committed
DDP history. Rows are read incrementally without a new SQL sort or tensor-state
deserialization. Canonical selection among pre-existing identical duplicates is
deterministic. Conflicting pre-existing reuse fails closed for new publication;
existing records are retained and not rewritten or migrated. Recovery continues
to use the existing verified committed head/record.

The training SDK, checkpoint validator, legacy sync/restore, HTTP service and
collector, launcher and 9.2 staging coordinator are unchanged. The public Python
method signature and frozen receipt type are unchanged; a successful redundant
DDP transfer may now return the original publication ID. This path is exercised
through the existing authenticated GET route. Single-worker publications bypass
the new DDP save lookup. Default configuration remains legacy/cache OFF/staging
OFF/resource. No component is newly installed or enabled at ordinary bootstrap.

Student training processes still receive no arbitrary NFS RW privilege. The
trusted service handles publication and only examines bounded metadata/opaque
state byte hashes. Structural integrity and coordinated SDK ACKs do not prove
that malicious student code performed honest training; OS/network isolation and
credential protection remain deployment prerequisites.

## 3. Tests and actual results

Baseline before edits: **41 tests PASS**, 25 publication + 16 checkpoint faults,
two files, zero failures/errors/skips:

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_checkpoint_publication.py test_hybrid_checkpoint_faults.py
```

Final verification commands, from the application root:

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_ddp_checkpoint_publication.py test_checkpoint_publication.py test_hybrid_checkpoint_faults.py test_hybrid_checkpoint_recovery.py
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_group_checkpoint.py test_group_recovery.py test_hybrid_checkpoint_ddp_resume.py test_storage_config.py
git diff --check
```

Final result: **107 tests PASS, 8/8 files**, zero failures/errors/skips:

| Test file | PASS |
|---|---:|
| `test_ddp_checkpoint_publication.py` | 15 |
| `test_checkpoint_publication.py` | 25 |
| `test_hybrid_checkpoint_faults.py` | 16 |
| `test_hybrid_checkpoint_recovery.py` | 18 |
| `test_group_checkpoint.py` | 8 |
| `test_group_recovery.py` | 7 |
| `test_hybrid_checkpoint_ddp_resume.py` | 1 |
| `test_storage_config.py` | 17 |

The first command passed 74 tests in four files; the second passed 33 tests in
four files. Whitespace check passed. Logs/hashes are preserved in the evidence
and raw logs under `/tmp/hybrid-9-3/`. The native Gloo command uses reviewed localhost socket access,
private rendezvous and owned child processes. No physical worker, live NFS mount,
service or Internet transfer is needed. Test runner source copies exclude live
DB/runtime data. Opaque-state fixtures do not pretend to be genuine tensor
training; existing native SDK suites supply writer/resume regression evidence.

The real three-rank case verified exact saved-state restoration and existing
final floating-state tolerance `atol=1e-8, rtol=1e-7`. Its only reported resumed
delta was optimizer `state.4.exp_avg_sq`, maximum absolute
`1.862645149230957e-09` on each rank; the two uninterrupted controls matched
exactly. No tolerance was changed. This is local correctness evidence, not a
storage/network benchmark or physical Pi result.

New tests verify complete metadata/rank members; missing/extra/duplicate/empty/
corrupt rank members; incomplete/reordered/negative/mixed-save ACKs; wrong world,
group and fences; one canonical fresh-ID retry; conflicting content; old-save
replay; a new UUID at the same step; monotonic completion/progress; independent
SQLite collector races; mid-stream peer revocation; a stale attempt; an interrupted
orphan followed by a fresh valid commit; and authenticated HTTP retry.

An initial new test expected only ValueError for all fence failures; actual
authorization correctly raises PermissionError for mismatched group/fences.
The new test now explicitly accepts those two documented failure classes.
This was a new-test expectation error, not a pre-existing baseline failure.
The earlier failing log is retained. Fixtures now use unique cut IDs so that
legitimate progression no longer fabricates contradictory saves under one ID.

## 4. Manual verification

Repeat the isolated commands above. Expect one COMMITTED row and one revision/
event for identical concurrent saves, rejection of malformed/conflicting saves,
and no movement of the prior head on failure or replay. Expected tests are
15 + 25 + 16 + 18 and 8 + 7 + 1 + 17, respectively. Native suites retain their
existing deterministic/numerical tolerances, rather than weakening comparisons.

```bash
git diff --check
git diff -- hybrid_storage/checkpoint_publication.py
cat config/storage.json
```

Expect a clean whitespace check, only the documented publisher extension, and
unchanged legacy/OFF/resource flags. Do not deploy, restart services or mutate
checkpoints/mounts to perform these local checks. The evidence records hashes of
the unchanged writer, validator, schema, services, launcher/staging and config.

## 5. Rollback and residual risks

Keep later edits and enrolled jobs safe before switching code. After deliberately
quiescing/holding hybrid jobs under the normal lifecycle, inspect this turn's
scoped `/tmp/hybrid-9-3/9.3.patch`. From the Git root:

```bash
git apply --reverse --check /tmp/hybrid-9-3/9.3.patch
git apply --reverse /tmp/hybrid-9-3/9.3.patch
```

Only the reverse-check is executed here. The patch excludes prior 9.1/9.2 edits.
No DB/schema downgrade or checkpoint/cache/job deletion is necessary. Keep
COMMITTED/REJECTED history and orphans; never remove them automatically as rollback.
Keep legacy defaults until separately tested and explicitly enabled. Reverting
the publisher removes the new canonical-save guarantee for subsequent transfers.

The lookup scans the current attempt's committed metadata history twice per new
save; very long histories can increase transaction latency. This is an explicit
compatibility tradeoff avoiding an unreviewed schema/backfill. No performance
benchmark or scaling claim is made. The existing collector's skip-unchanged path
avoids routine redundant transfers. Retries without it can retain extra private
candidates/orphans and consume disk; existing admission/reserve checks remain.
Large rank-zero gather memory, source clock/lease limits, real power durability,
production containment and Pi versions remain as audited. No DCP/version migration
is performed. The central controller/local SQLite/checkpoint disk remains a
**single point of failure**.

## 6. Acceptance boundary

| 9.3 local criterion | Result |
|---|---|
| Preserve writer, coordinated rank-zero v2 format and world-size semantics | PASS; unchanged source hashes and native regressions |
| Expected rank members, hashes, common-save ACKs, current group/fences | PASS; retained validators and targeted malformed-save tests |
| One COMMITTED metadata manifest per consistent SDK save across collectors | PASS; identical retries share canonical receipt, conflicting bytes rejected |
| Stale attempt/rank, incomplete save or old replay cannot advance latest | PASS; head/revision/job pointer/event unchanged on rejection or replay |
| Legacy/single-worker, default-OFF and existing group recovery behavior | PASS |
| Preserve prior changes and data; no live mutation/deploy | PASS |

Physical
all-three-Pi validation and Phase 9.4's full requested fault/world-size matrix
remain pending. **STOP at 9.3.**
