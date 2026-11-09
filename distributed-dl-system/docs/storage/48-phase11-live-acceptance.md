# Phase 11 completion — approved pi02/pi03 validation

10 November 2026, Asia/Dhaka. **Phase 11.1–11.4 PASS under the approved two-worker
scope.** The user explicitly deferred pi01, then approved completing the full
phase and all necessary tests. That approval supersedes the earlier preparation
stop points for this work. pi01 was not contacted. This is not a three-Pi result.

This report supersedes the pending **Phase 11 validation** status in reports
46/47: all repository regression files and the new probe tests passed, and actual
NFS/cache/inventory/isolation/fault validation passed on pi02 and pi03. Historical
reports retain their original observations. Production student-job execution
remains legacy/OFF/resource; enabling it was not necessary to validate this optional
optimization and is not claimed here. This turn installs no replacement agent or
general-purpose production sandbox/DDP bootstrap.

## 1. Implemented behavior and files

| File | Change in this completion turn |
|---|---|
| `hybrid_storage/phase11_probe.py` | Explicit trusted worker validation bootstrap: new private state only; real NFS copies, warm reuse, corruption repair, concurrent fetch, quota/pins/private workspace, distinct-UID/no-network probe, disposable eviction crash and reconciliation; existing signed inventory replies |
| `tools/validate_phase11.py` | Plan-only default; explicit `--run` for pi02/pi03 source-only private upload, installed RO readiness, local disposable controller registry, actual authenticated inventory/coverage, eviction/refetch/offline/reconnect and unchanged-agent checks |
| `tests/test_phase11_probe.py` | Four safe local rehearsal/input/state/default-plan tests |
| `hybrid_storage/README.md`, `tools/README.md`, `tests/README.md`, `docs/storage/implementation-progress.md` | Appended commands, scope and completion evidence |
| This report and `48-phase11-live-acceptance-evidence.json` | Exact outcomes, commands, hashes, limitations and rollback |

Before editing, 277 selected source/config/document files were snapshotted under
`/tmp/hybrid-phase11-complete/before`. Existing uncommitted changes on `main` HEAD
`7edef1ccd8e3264d9716a3482cbd214188d009b6` were preserved; no stash, reset, commit
or force operation. Only the four original documentation files above are appended;
all other snapshotted originals remain byte-identical.

## 2. API, DB, config and security

No existing production route, storage API, job state, scheduler rule, lease,
checkpoint format, inventory schema/wire format, worker registration, configuration
or deployment command was changed. No new download/HTTP protocol. The probe's
stdin operations are an explicit trusted SSH **test driver**, not a student/API
transport. Inventory messages reuse the actual request-bound worker HMAC protocol
and optional v2 transfer summaries; keys were ephemeral, private to the test
processes and never printed or placed in archives.

Only a new disposable controller fixture DB was created and explicitly migrated:
`/tmp/hybrid-phase11-complete/hardware-run/controller-fixture/registry.db`.
It uses a minimal fixture job table plus real registry/inventory schemas and
`StorageRegistry`, `CacheInventory`, `CoverageAPI` and `TransferObservations`.
Published byte hashes are registered through the real API; no verified cache
presence is fabricated. The controller fixture and worker SQLite files are LOCAL,
not NFS. Existing `controller/jobs.db`, student data/jobs/checkpoints, production
agent source roots and service definitions were not operated on by the tests.
Normal running services can still perform their own unrelated background work;
no blanket live-DB hash-identity claim is made.

Active flags remain:

```json
{"storage_backend":"legacy","worker_cache_enabled":false,"storage_aware_staging":false,"scheduler_policy":"resource"}
```

### Actual approved live actions

* Trusted controller publication created three public deterministic immutable CAS
  objects using `ArtifactRegistrar`, UID/GID **125:125**, mode **0444**. Total
  **7,340,032 bytes**. Existing objects were never overwritten or deleted.
* A hash-reviewed worker source archive (78 members, SHA-256
  `b211e7ccb9ea56685f5b47f0d0322d536c2bbd2b830fad4a1895b5505d6a2540`)
  was copied over existing strict-key SSH into a **new root-owned 0700** tree on
  each selected Pi: `/var/tmp/dl-phase11-9188f95e1b6cae4fc3c11394`.
  Archive member names/types/bytes/hashes were verified; no general extraction
  into existing directories, credentials, venv or runtime data was performed.
* `sudo -n systemctl start ml-nfs-ready.service` activated the **existing installed
  RO mount/readiness configuration** on each worker. No unit/export/fstab/firewall/
  SSH file was edited, no production agent/NFS server restarted, no package installed.
  Readiness found nft/iptables under `/usr/sbin`; their earlier absence from the
  SSH PATH was not a missing-package finding. No firewall command was applied.
* Trusted probes used only their own new caches/private workspaces and public
  authorized fixture hashes. An owned child was intentionally exited after an
  unlink only inside that new cache. No physical disk filling, shared link drop,
  power cycle, production unmount, arbitrary kill or authoritative deletion.
* The validation bootstrap exited after tests. Source, cache/quarantine/workspace
  and controller fixture evidence are retained. Installed RO mounts remain active;
  no potentially shared mount was forcibly/lazily unmounted as cleanup.

The initial publisher wrapper dropped privileges before importing from the private
checkout and failed with `ModuleNotFoundError`, before publishing any file. The
corrected wrapper imported the reviewed registrar first, then dropped to UID125.
It published all three fixtures successfully. Both transcripts/program hashes are
retained. This was a setup failure, not a concealed successful first attempt or a
test failure requiring changed production logic.

### Isolation result and its exact boundary

Both Pis successfully ran the bounded validation child through `setpriv
--no-new-privs` and `bwrap --unshare-all`, new PID/mount/user/network namespaces,
UID/GID **65534**, capabilities dropped, cleared environment, detached stdin and
private proc/dev/tmp. It received a read-only **copy** of the authorized workspace
inputs, never a writable cache hardlink. Actual negative checks passed: cache DB,
NFS and service-source paths hidden; writing inputs failed; controller/NFS network
connection unavailable. The owned child was reaped before releasing test pins.

This validates that fixture's concrete boundary. It is not a general hostile-code
security proof, cgroup resource limiter or peer-network DDP sandbox. It does not
replace the production `StagingService.isolate/terminate/quiescent` providers.
No untrusted student trainer was launched. Three-rank physical DDP and separate
Phase 9 production/deployment acceptance are not claimed by this Phase 11 result.

## 3. Actual tests and evidence

### Full local repository regression

```bash
PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated
```

**56/56 existing files passed**, 788.7 seconds. This includes genuine CPU/Gloo
two-/three-rank crash/resume, model/optimizer/scheduler/RNG checkpoint continuity,
all storage/backend/path/auth/registry/queue/staging/recovery/UI regressions and
Phase 11 cost/eviction tests. Only temporary source/state/owned child processes
were used. Localhost sockets were allowed for the native tests; no Pi accessed
by this suite. The standalone `test_checkpoint_recovery.py` has executable
assertions, not a unittest count; it passed and is counted separately.

The venv run initially skipped ten draft-schema cases because `jsonschema` is
absent there. No dependency was installed. The documented system Python already
has jsonschema **4.10.3**, so the full affected file was rerun:

```bash
PYTHONDONTWRITEBYTECODE=1 python3 tools/run_tests.py --isolated test_storage_manifest_drafts.py
```

**15/15 PASS, zero skips**, replacing that file's earlier skipped result.

New local probe/packaging validation and final probe rerun:

```bash
PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_phase11_probe.py test_deployment_review.py
PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_phase11_probe.py
```

**4 + 6 PASS**, then **4 PASS** after a metadata-label clarification in the driver.
The clarification replaces an ambiguous `production_mutation=false` label with
explicit unchanged-agent/config and requested RO-mount-activation fields; the
actual earlier result's mount commands remain disclosed above. It changes no test
behavior. The exact executed driver source and current source are retained.

Latest distinct aggregate: **946 unittest cases PASS plus one standalone checkpoint
regression script, across 57 files; zero remaining failures/errors/skips**.
Reruns and the schema replacement are not double-counted. The original ten skips,
new-probe development rehearsal and setup import failure remain in the evidence.
Controller Python **3.12.3**, PyTorch **2.14.1**, Flask **3.1.3**; worker probe
system Python **3.11.2**. Existing agent health Python **3.11.16** is a separate
runtime; neither implies the version of a prepared production ML environment.

### Actual pi02/pi03 run

The publisher command used the normal desktop administrative authorization
(`pkexec`) and dropped to the configured publisher account before writing. Worker
commands used existing noninteractive sudo. No credential was requested in chat.
Then:

```bash
PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/validate_phase11.py \
  --nodes pi02 pi03 \
  --publication /tmp/hybrid-phase11-complete/authorized-publication.json \
  --output /tmp/hybrid-phase11-complete/hardware-run --run
```

**PASS on both physical workers.** Exact driver, publication/SSH/activation/probe
source, source archive/member hashes, authenticated results and final inspection
are embedded in the evidence companion. No Internet-dependent worker transfer.

| Actual result | pi02 | pi03 |
|---|---:|---:|
| SSH/control address | `100.91.135.18` | `100.64.202.71` |
| Verified LAN address | `192.168.10.150` | `192.168.10.101` |
| Exact input source | `192.168.10.214:/srv/ml-shared/objects` | Same |
| Real input mount / NFS version | `/srv/mlnfs/inputs` / **4.1** | Same |
| Cold verified input bytes | **7,340,032** | **7,340,032** |
| Cold effective MiB/s, total ensure interval | **1.64518** | **1.56435** |
| Unchanged warm repeat staged bytes | **0** | **0** |
| Corrupt replica | Re-fetched, bad local bytes quarantined | Same |
| Concurrent same-digest fetch | Exactly one miss/copy, one hit | Same |
| Authenticated predictive sample count before eviction | **5** | **5** |
| Quota refusal / incomplete acceptance | EDQUOT before copy / none | Same |
| Private workspace/pins/open-reader exclusion | PASS | PASS |
| Distinct-UID/RO/private-path/network isolation | PASS | PASS |
| Process crash after disposable unlink / nondeleting reconcile | PASS / one missing object | Same |
| Signed controller coverage missing after crash | **2,097,152 bytes** | **2,097,152 bytes** |
| Actual missing-only refetch / next warm repeat | **2,097,152 / 0 bytes** | **2,097,152 / 0 bytes** |
| Offline eligibility rejected / authenticated reconnect | PASS / PASS | PASS / PASS |
| Unique write attempt to verified RO share | **EROFS (30)** | **EROFS (30)** |
| Production agent PID before/after | **946 / 946** | **902 / 902** |
| Final production agent resources | Matching identity, **IDLE** | Matching identity, **IDLE** |

Cold rates are actual application-payload/whole-ensure observations under this
test's page-cache, CPU, fsync/verification and concurrent-worker conditions. They
are not cold-wire/iperf link speeds, speedup claims or total-training predictions.
The authenticated per-copy summaries separately retain measured rate/residual,
age/count and sample-size range used by the existing cost model. Source data,
kernel caches and contention can change predictions. The evidence also computes
the one-missing-object copy estimate from those real facts and records the later
measured copy interval without claiming general predictive accuracy.

| One subsequent missing-object copy | pi02 | pi03 |
|---|---:|---:|
| Predicted seconds from five authenticated prior samples | 1.08147 | 1.15789 |
| Later measured copy-attempt seconds | 0.22112 | 0.22223 |

The point estimates substantially overpredicted these two later copies. Source
page-cache and concurrent-transfer conditions changed during the test; their
individual effects were not isolated. The observed-extrema scenarios are not
confidence bounds (the later values were slightly below their prior lower
scenario). This is evidence of model uncertainty, not proven efficiency benefit.
Resource remains the active policy; bandwidth remains explicitly optional and
requires workload-specific calibration before any production-performance claim.

The three authoritative objects remain UID/GID125, 0444, with unchanged hashes:

| Bytes | SHA-256 |
|---:|---|
| 1,048,576 | `0d93c614948f718cc6b2625aa03f68c587734a33d225aab97ef38c93162401dd` |
| 2,097,152 | `bbbf6edcc8752b336fbd3bfba3b934de82e7d5a2b94b81a352c1f0c6fbe827c6` |
| 4,194,304 | `b2b3c7885719e8c94acf94602f8701f7e4281628dd36db0effe28026a9ea9fde` |

### Acceptance matrix

| Criterion | Result |
|---|---|
| 11.1 real bytes/time, robust rolling statistics, age/count, no active probes | PASS: real two-Pi observations plus complete deterministic statistics/failure tests |
| 11.2 transparent copy-only costs, freshness/capacity/concurrency uncertainty, opt-in bandwidth, hard constraints and rollback | PASS: authenticated physical facts plus real selectors/queue tests; no total-completion model |
| 11.3 explicit dry-run-first bounded LRU, limits/pins, no authoritative deletion, reconciliation | PASS: local full fault matrix and physical private-cache crash/refetch, root/RO/isolation checks |
| 11.4 slow/fast/drop/old/no samples, concurrency/disk-full/pins/crash/fairness/rollback | PASS: all repository cases and actual two-worker cache/inventory faults; ENOSPC/link-drop scenarios simulated, no production disk/link disruption |
| Resource/locality remain independent; legacy/OFF defaults | PASS: all config/selection/queue regressions and unchanged active config |
| Physical three-Pi / native Pi DDP training | NOT RUN: pi01 explicitly deferred; separate Phase 9 gate not claimed |
| Production student-job hybrid enablement | NOT ENABLED; active defaults intentionally preserved |

## 4. Safe verification and rollback

Safe default commands, from the application root:

```bash
.controller-venv/bin/python -B tools/validate_phase11.py --nodes pi02 pi03
.controller-venv/bin/python -B -m hybrid_storage.config
PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_phase11_probe.py
git diff --check
```

Expect plan-only/no upload, legacy/OFF/resource, four PASS, no whitespace errors.
Do not reuse the existing output directory for another live run. `--run` is an
administrator action, not a student flow; it requires the reviewed authorization,
fixed public manifest and NEW private local/remote state. Fixture source/state
must remain inaccessible to untrusted processes. Never use constant test
quiescence callbacks as production termination providers.

Source-only rollback, from the parent Git root after preserving later edits:

```bash
git apply --reverse --check /tmp/hybrid-phase11-complete/completion.patch
# Only after reviewing these exact source/documentation hunks:
git apply --reverse /tmp/hybrid-phase11-complete/completion.patch
```

Reverse check and actual reversal on a disposable source copy passed; nothing
was reverted here. Earlier Phase 10/11 user work is preserved.

Live rollback does **not** require an agent restart or config restore: none was
replaced. All owned validation processes exited; the private root trees and
public immutable CAS fixtures can be retained for audit. Do not recursively
delete unknown files or immutable inputs as cleanup. Existing RO mount activation
is the only service-state action: if restoring its previously inactive state is
later required, first verify no readers/other jobs use it, then perform only the
reviewed normal stop/unmount. Do not force/lazy unmount or stop a shared NFS server.
Keep SQLite, production jobs/checkpoints, durable pins and legacy mounts intact.

Evidence/log/archive paths under `/tmp` are ephemeral; the repository evidence
embeds the critical programs, observations, counts and hashes. It includes the
initial skipped/setup results rather than rewriting history.

## 5. Residual limits and final status

Medians can lag link changes; overhead depends on size; shared NFS/cross-job
contention remains uncertain. Registry facts are advisory and require stage-time
verification; actual quota/free-space/lease gates remain authoritative. Safe
eviction requires dedicated protected roots, cooperating upgraded cache clients
and durable pins. Physical power loss and host-wide NFS disconnect were not
injected; failure paths were tested on disposable fixtures. Historical loaded-run
cold-stage HTTP409 remains disclosed, not root-caused; the full serial suite,
native DDP/recovery and new physical component tests passed this turn.

NFS AUTH_SYS is not a per-student security boundary. The validation sandbox is
not a general production DDP isolation implementation. Production trust/identity/
network containment still requires its own explicit reviewed enablement work;
these test callbacks are not silently installed into the student launcher.
The central controller/LOCAL SQLite/authoritative storage remains a **single
point of failure**.

**Phase 11 COMPLETE / PASS for implementation and all required local tests plus
approved pi02/pi03 component acceptance. pi01 deferred by user exception. No
three-Pi or production student-job deployment claim. STOP before any next phase.**
