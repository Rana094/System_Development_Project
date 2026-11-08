# Phase 7.2 — authenticated staging and existing launcher integration

Started 8 November; completed local verification 9 November 2026. Starting Git root: the parent `System_Development_Project`;
branch `main`, commit `68f59e9e72d8ecb2e1b5a98400be2e22051eafc4`.
The working tree was clean. Inspected README, audit, architecture, progress,
actual dispatcher/DDP placement, job transitions, leases, worker start,
environment runner, persistent cache and private workspace implementations.
No stash, reset, commit, enrollment, live database access/migration, SSH, service,
mount, `/etc` or Pi operation occurred.

## Behavior and exact files

New runtime files:

* `hybrid_storage/staging_protocol.py`: strict bounded metadata, path/action/worker
  HMAC domains, request nonce/time, signed replies bound to exact request bytes;
  reuses `inventory_protocol.Credential`. No credential creation or new artifact
  download transport. HTTP remains the existing worker control channel.
* `controller/staging_dispatch.py`: explicitly constructed `StagingDispatch`.
  Revalidate current owner/ACL, source, dataset version, requirements, execution
  spec and legacy manifest pins. Reuse resource selection, complete DDP group
  selection, `mark_scheduled`, active quotas, reservations, history and leases.
  Record staging intent, request missing-only staging, verify authenticated exact
  receipts, wait for every rank and recheck every fence before each launch.
* `agent/staging.py`: explicitly installed trusted `StagingService`; stage only
  signed exact plans under current lease/cancellation authority. Reuse actual
  `CacheFetcher.ensure` and `WorkspaceManager.prepare/mark_started`; the latter
  rechecks full bytes, layout, modes, inodes and pins at launch. No path from a
  student's request is used as a workspace/cache/NFS root.

Existing runtime changes:

* `controller/dispatcher.py`: optional keyword-only `storage_runtime` seams in one-job and fair-queue dispatch;
  unbound/legacy jobs retain their existing dispatch path.
* `controller/storage_selection.py`: NFS execution requires the explicitly passed
  trusted runtime and exact current immutable authorization. No JSON grant/token
  can substitute for that Python service object.
* `controller/job_store.py`: opt-in runtime keywords for prepared environment
  recording, fair queueing, reservations, release and RUNNING. Only the original NFS
  infrastructure hold can become READY through trusted environment recording;
  owner-held jobs cannot be implicitly released that way. Existing eligibility,
  compatible prepared worker counts, concurrency and fairness remain. New runtime
  transitions use LOCAL FULL/FK connections; legacy connections remain NORMAL.
  RUNNING also needs all-ready fences plus the coordinator's authenticated launch
  ACK path. Change side phase READY→STARTED atomically with RUNNING/history.
* `agent/agent.py`: verified local input arguments and mandatory operator-provided
  isolation wrapper immediately before existing lease guardian/Popen. No new
  launcher or replacement training protocol. Default agent behavior is unchanged.
* `agent/custom_runner.py`: trusted CLI-only local source/data override; preserves
  controlled interpreter/workspace paths, package verification, argv, resource affinity/thread
  limits, output/checkpoint locations and environment variables. File datasets
  resolve their original logical basename inside the local data tree. Legacy
  invocation still verifies its old snapshots exactly as before.

Tests: `tests/test_staging_dispatch.py` (25 new cases) and
`tests/test_job_environments.py` (extend existing real offline runner case).
Documentation: this report, its evidence JSON, appended progress record and
`hybrid_storage/README.md`. No other files are intended changes.

```mermaid
sequenceDiagram
    participant C as Existing controller dispatcher
    participant DB as Local SQLite / lease authority
    participant A as Trusted worker agent
    participant F as Read-only NFS input backend
    participant W as Verified LOCAL cache / private inputs
    C->>DB: Validate immutable job + ACL, resource eligibility, reserve group
    C->>DB: Persist leases and exact staging plans
    loop Every actual rank (current trainers require whole dataset)
        C->>A: Signed POST /jobs/stage (plan, attempt, lease)
        A->>W: Hash existing objects
        A->>F: Stream only missing objects
        A->>W: Verify / atomic publish / pin / private copies
        A-->>C: Request-bound signed receipt (no raw paths)
        C->>DB: Record exact rank READY; recheck fences
    end
    C->>DB: All-rank barrier + current lease/reservation/ACL checks
    C->>A: Signed existing POST /jobs/start + exact receipt
    A->>W: Reverify private inputs and pins
    A->>A: Required isolated existing custom runner, under lease guardian
    A-->>C: Signed existing start ACK + execution fence
    C->>DB: RUNNING/history + side STARTED in one transaction
```

## API, DB, flags and security boundaries

No schema migration, job enum, checkpoint format, portal upload/HTTP response,
credential file, storage config or NFS export changed. The explicit 4.3/7.1
migrations must already exist; runtime never applies them automatically. Registry
read validators execute against the same local, locked job-store snapshot rather
than borrowing stale metadata from another database connection. No SQLite on NFS.

`StagingClient.call` uses POST on the existing configured worker URL. A request
has version 1, configured worker ID, random nonce, send time and body. The
`X-DL-Staging-MAC` authenticates worker/key ID, method/path, direction and complete
body, with a domain distinct from cache inventory. Reply MAC also includes a
hash of the exact request. Requests/replies are bounded to 1 MiB, duplicate JSON
keys/nonfinite values rejected, freshness bounded to two seconds. Stage body:
`stage_id`, immutable versioned `plan`, existing `lease`. Start body is the
existing custom payload plus `staging_id`, `staging_receipt` and the registered
`execution_spec_sha256`; the worker hashes its prepared spec and rejects drift. No secret or NFS
path is included. This authenticates metadata, not HTTP confidentiality.

Request replay is idempotent only for the exact live attempt; existing durable
lease generations, cancellation tombstones and workspace state fence old owners.
Different rank/plan/stage/receipt/generation cannot replace a pinned identity.
Reply replay under another request nonce fails. Artifact digest knowledge alone
grants no access. Inventory is advisory; cache bytes are always verified at stage
and workspace bytes again at launch. Missing/corrupt files, disk admission, failed
isolation and failed/auth-mismatched acknowledgments cannot enable RUNNING.

The worker service is deliberately **not installed by normal startup**. Install
only in reviewed dedicated hybrid-worker bootstrap, with no live trainer and an
operator-supplied `isolate(command, workspace, data)` boundary. That trusted adapter
must isolate service UID/credentials/cache/other jobs, hide the NFS mount and block
direct NFS network access (including AUTH_SYS bypass), and limit any DDP network
access to approved peers. There is no unsafe same-UID default. Tests supply a
mock and do not establish this OS/network boundary. The entire custom runner,
including dependency probes, must run inside it. Do not treat a callback or argv
alone as evidence of real isolation.

While this service is explicitly installed, unsigned mutating legacy routes are
denied, including stop/lease/checkpoint uploads, so they cannot bypass staging
authentication. Inventory retains its independent HMAC route. Authenticated
stop/periodic renewal/terminal reconciliation are not implemented in 7.2; hence
this bootstrap is not production-ready. Default agents retain those old routes.

All flags remain legacy/OFF/resource. Normal config validation still rejects
cache execution activation; this turn does not remove that safety gate. The new
path is exercised only by explicit trusted service construction and dispatcher
injection. Operator-provisioned compatible environments are required; there is
no package install, Internet transfer or new preparation transport here. Tests
use offline local wheels for the real interpreter test.

Current NFS execution support is **restart-mode custom jobs**, single worker or
whole-data CPU DDP. New NFS checkpoint jobs remain held: authenticated trusted
checkpoint publication/restoration has not been integrated. Existing legacy
checkpoint/recovery flows retain their formats. Single-worker regressions pass;
the group tests have the sandbox socket errors recorded below.
Inputs larger than local capacity fail closed; direct-NFS training is unsupported
and never a silent fallback. Outputs/checkpoints keep their existing local
launcher locations, separate from input cache; no output is published to NFS.

## Leases, failures and explicit 7.3 boundary

Before each signed stage/start, renew the same lease identity through a LOCAL
FULL transaction only if its existing reservation, boot, deadlines and uncertainty
fences are current. Never revive an expired/blocked/revoked lease. Other ranks
must remain live at the final barrier. Copy helpers and private copies recheck
local authority before publication. Long staging remains bounded by the accepted
lease; no claim of indefinitely renewable large-data staging is made.

After uncertain copy/start, keep SCHEDULED/reservations/history and block renewal;
never blindly requeue or start a replacement. A remote launcher may already have
started after a lost HTTP ACK; its existing guardian enforces expiry. Do not run
this path in the normal reconciliation loop until 7.3 integrates it. A service
restart loses its in-memory ready lookup and fails closed; durable workspace pins
remain for reconciliation. One staged identity occupies the service until that
work is implemented. No automatic terminal unpin/temporary-file cleanup or new
retry protocol is claimed here. Warm second-job tests explicitly finish their
disposable first workspace and clear test-only service ownership; this is not
production cleanup evidence. Cancellation between ready/start is tested; full
cancellation during copy/outage/restart acceptance remains 7.3.

The controller/NFS server remains a single point of failure. SHA-256 and HMAC do
not attest an administrator-compromised worker. Production/trainer isolation,
authenticated lifecycle controls and all-three-Pi validation remain outstanding;
the historical pi01 deferral is not new approval to omit it.

## Actual tests and manual verification

Results and exact selected commands are recorded in
[27-staging-dispatch-evidence.json](27-staging-dispatch-evidence.json).
Baseline: **64 passed, four/four files, zero failures/skips** before edits.
Broad selection: **386 methods: 372 pass, 14 errors, zero skips; 17/19 files pass**
in 165.7 seconds. All 14 errors are group-checkpoint/recovery socket EPERM;
the pre-change reproduction has the same 14 errors in 15 methods. The real
single-worker checkpoint suite's four cases pass. Final queue/default/barrier
selection: **96 passed, 6/6 files, zero failures/skips**. Final execution-boundary run: **36 passed, three/three files**, including all
**24 new staging cases**, six offline environment cases and six resource/legacy
agent cases. Python 3.12.3, Flask 3.1.3 and PyTorch 2.14.1 are installed locally;
these versions do not imply real DDP training ran successfully. After the final
atomic bootstrap guard, the new suite expanded to **25 passing cases**; a failed
late Flask setup cannot activate launch hooks without authentication.
Counts across repeated runs are not added together.
Initial test-harness runs exposed import-path setup, an attempted mutation of an
immutable ACL fixture, and a dataset fixture basename mismatch; these were fixed
in tests. The corrected real runner uses the original file basename. No baseline
test failure was found in the successful selection.

The new cases exercise real job-store reservation/history and transitions, local
Flask test clients with request/reply HMAC, actual copy helper subprocesses with
fake mount facts, real SHA-256/cache SQLite/private copies and mocked isolation/
Popen. They check cold/warm bytes, all-three local logical rank barrier, failed
third rank, missing source, ACL revocation, corrupt workspace, no eligible worker,
disk admission, expired lease, lost start reply, receipt mismatch, bounded peer
reply, unsafe paths, authentication/action/body replay, mandatory isolation,
idempotent warm staging and bare RUNNING rejection. The real offline environment
test executes submitted Python with the staged local file dataset and source
even after tampering with the old source; this does not prove OS isolation.

Safe commands from the project directory:

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_staging_dispatch.py test_staging_lifecycle.py test_job_environments.py
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python -m hybrid_storage.config
git diff --check
git diff --stat
git status --short
```

Expected: 51 methods pass in those three files, checker reports legacy/resource
with both flags false, no whitespace diagnostics, and only the files listed above
plus the report/evidence/progress/README edits. Review new untracked files with
`git diff --no-index -- /dev/null <file>` (exit 1 means differences).

No live NFS, physical Pi, mount/outage, service, network throughput or production
isolation claim. The two existing group-checkpoint/recovery suites were attempted in the broader
selection: 14 methods failed at `socket.socket()` with sandbox EPERM before
training. Both unchanged suites were rerun against a temporary source copy with
all edited tracked files restored from the starting Git commit; the same 14
errors reproduced (one remaining method passes). The previously documented DDP
socket restriction also applies here; no physical or real distributed training test
was substituted with a mock and labeled hardware success. No broad whole-repo
pass or dedicated Phase 5.4 acceptance is inferred.

## Rollback and acceptance

The full scoped review diff is `/tmp/hybrid-7-2/7.2.patch` (includes new files).
Read-only rollback applicability check:

```bash
git -C "/home/rana/Projects/System Project/System_Development_Project" apply --reverse --check /tmp/hybrid-7-2/7.2.patch
```

This check passed; it does not apply changes. After review, the same command
without `--check` reverses only this patch if no later edits conflict.

No live config or DB change needs reversal. Keep defaults unchanged and do not
install/inject these explicit components. For code rollback, review/reverse only
this turn's patch relative to `68f59e9`; preserve any subsequent user changes.
The local review patch and its reverse-check command are recorded in evidence.
Do not reset/clean, delete runtime data or drop committed staging/registry tables.
If operators later exercise this path outside tests, first stop/quiesce all exact
producers/trainers and reconcile pinned attempts; toggling a flag is not safe
cleanup of uncertain execution.

**7.2 local restart-mode dispatch/staging integration: PASS when the recorded
selected tests passed. Production enablement and unrestricted NFS checkpoint-job
support: NOT PASSED. Overall Phase 7 acceptance: pending. STOP before 7.3.**
