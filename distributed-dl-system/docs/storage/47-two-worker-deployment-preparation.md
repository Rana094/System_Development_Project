# Two-worker continuation — offline deployment preparation

Scope authorization: the user answered the pi01 availability question with
**"fow now continue withour pi1 / with pi2 and pi3"**. This supersedes the
all-three-node prerequisite for the current continuation: proceed with pi02 and
pi03, leave pi01 deferred. Earlier closeout/readiness reports remain historical
evidence. A two-worker test will be reported as two-worker acceptance, never as
an executed three-rank run. No pi01 probe was performed in this subphase.

This turn completes **one preparation subphase only**: offline exact source
bundles, selected-worker manifest, tests and a concrete deployment/isolation
review plan. It performs no remote installation or live enablement. The user's
standing rule still requires review and explicit approval before deployment or
system/service/network configuration changes; the worker-count exception does
not remove that rule. No approval is requested for an incomplete bootstrap.

## Implemented files and artifacts

| File | Change |
|---|---|
| `hybrid_storage/deployment_review.py` | Offline catalog/build CLI; complete hybrid Python source in worker/controller review archives; explicit node selection; hash manifest; always `apply_ready=false` |
| `tests/test_deployment_review.py` | Six offline safety/integration tests |
| `hybrid_storage/README.md`, `tests/README.md`, `docs/storage/implementation-progress.md` | Appended command/scope/results |
| This report and `47-two-worker-deployment-preparation-evidence.json` | Actual observations, hashes, plan, tests and rollback |

Current Git branch/HEAD are unchanged (`main`,
`7edef1ccd8e3264d9716a3482cbd214188d009b6`). Earlier uncommitted changes were
preserved. No existing agent, controller, scheduler, cache, checkpoint, auth or
deployment command was changed. API/DB/config/security changes: **none**. Active
`storage.json` remains legacy/OFF/resource; the global inventory still lists all
three historical nodes. This preparation selects only pi02/pi03 in its own
manifest without overwriting that inventory or any operator config.

The old `tools/cluster_setup.py::bundle` still serves the legacy installer and
omits `hybrid_storage`. The new command is a separate review artifact producer,
not an implicit replacement for that installer:

```bash
# Catalog only; stdout, no output files, network or controller/SQLite import.
.controller-venv/bin/python -B -m hybrid_storage.deployment_review --nodes pi02 pi03

# Create only a NEW absolute local directory. Existing destinations are rejected.
.controller-venv/bin/python -B -m hybrid_storage.deployment_review \
  --nodes pi02 pi03 --output /tmp/hybrid-two-worker-review-new
```

Actual executed output directory: `/tmp/hybrid-two-worker-prep/review-bundle`.

| Archive | Source members | SHA-256 |
|---|---:|---|
| `worker-source.tar.gz` | 77 | `d0624591dd661f43d97d4fe4d34701bcf74df38bf39174003a02c92254b6fe2f` |
| `controller-source.tar.gz` | 112 | `3701112497428b64e3eb0c052cd9373d8b210f2d0bdc3182dffdd95f2052d42f` |

`review.json` contains every member's bytes/SHA-256, selected endpoint facts from
`config/nodes.json`, pi01 deferred, required review gates and archive hashes.
The repository evidence embeds that manifest, so the inventory survives `/tmp`
cleanup; archive bytes themselves are temporary local artifacts. Rebuild into
a new directory from matching source if needed. No archive was uploaded.

Only allowlisted Python/static source, runtime requirements text and inactive
legacy/resource/locality/bandwidth examples are included. Live storage config,
nodes config, credentials/key stores, DBs, jobs, artifacts, checkpoints, caches,
logs, venvs, wheels and weights are excluded. Controller source also includes
agent/training source needed by existing preparation paths. These archives are
not a self-contained Python environment or an installed runtime.

The builder rejects symlinked source/ancestors, linked/special/oversized source,
changed bytes between catalog and build, relative/existing output destinations
and duplicate/unknown worker identities. Bounds: 8 MiB per source, 128 MiB total,
2048 members per role. It uses deterministic regular-file tar members, no link
entries or absolute/traversal member names, private new output files, fsync and
a completion manifest written after both archives. A failed build can retain its
own incomplete new directory, with no completion marker; do not deploy it.
Source parents must remain trusted against same-UID/admin replacement. No upload,
extract, apply, package installation, remote execution or automatic activation API.

## Fresh read-only pi02/pi03 observations

Exact timestamp, remote program/commands and replies are embedded in the evidence.
Queries used strict existing SSH host keys, no local SSH command/persistent
connection, bounded GETs/SSH/subprocess timeouts and `python3 -B` on the workers.

| Fact | pi02 | pi03 |
|---|---|---|
| Configured endpoint | `100.91.135.18:5000` | `100.64.202.71:5000` |
| Health/resources identity | Matches; ONLINE/IDLE; one slot available | Matches; ONLINE/IDLE; one slot available |
| Active service | `/home/pi/distributed-dl-system`, User=pi, PrivateMounts=no | Same path/account name/namespace property |
| SSH UID/GID | 1000:1000 | 1002:1002 |
| Kernel / SSH Python | `6.12.25+rpt-rpi-2712` / 3.11.2 | Same |
| Agent health Python | 3.11.16 | 3.11.16 |
| Root filesystem free bytes | 11,483,582,464 | 9,307,320,320 |
| Commands found | bwrap, unshare, systemd-run, setpriv | Same |
| Commands not found on probe PATH | nft, iptables | Same |
| Cgroup controllers reported | cpuset cpu io pids | Same |

Numbers are point-in-time facts, not reserved capacity. Tool presence and userns
limits do not prove namespaces/permissions/network containment actually work.
No user namespace, firewall rule, cgroup, process containment or test job was
created remotely. No sudo command ran. No missing-tool package was installed.

Both active service roots still lack the queried hybrid staging/checkpoint/cache
and Phase 11 files; agent source differs from this checkout. Only the legacy RW
NFS4.2 input share is visible in the mount-table query; the proposed RO automount
was not activated by path access. Current TCP port 5000 is occupied by the legacy
agent; do not launch another server there. No service-root/data/config was changed.

## Concrete next deployment/isolation preparation

The source review is ready; **the runtime deployment is not apply-ready**. The
next separate subphase must prepare these missing artifacts before approval:

1. A worker bootstrap using the actual existing `StagingService`,
   `install_staging_service`, `CheckpointService`, inventory reporter and shared
   `TransferMetrics`. Use exact pi02/pi03 credentials provisioned outside archives,
   protected dedicated local cache/workspace/spool roots and the reviewed RO NFS
   source. No student-supplied root/credential/worker ID. Mount identity and source
   admission still fail closed. Omit automatic eviction.
2. Actual OS **and network** isolation providers for `isolate`, `terminate` and
   `quiescent`, with tests. Tool presence is insufficient. Private mounts must hide
   NFS/service/cache/credential roots; training gets only its authorized private
   workspace and prepared offline runtime. Termination must cover all descendants
   and produce proof before pins/slots release. Single-worker jobs can use no
   network; two-rank DDP needs reviewed peer-only connectivity while blocking
   controller/agent control interfaces. No same-UID or unrestricted-network fallback.
   Missing nft/iptables on this PATH and unproven delegated privileges are open
   design facts; do not guess a firewall implementation or require Internet install.
3. A controller bootstrap on LOCAL SQLite with explicit migrations, same-registry
   `CoverageAPI`, authenticated inventory/transfer observations, `CheckpointDispatch`
   and existing fair queue/lease/recovery APIs. The test inventory/credential set
   contains only pi02/pi03; do not dispatch a third rank or enroll deferred pi01.
   Use distinct disposable state/queue and a separately verified unused listener
   or a reviewed maintenance window; never run two controllers against one live DB.
4. A backed-up, idempotent staged-install plan with exact source/archive hashes,
   destination ownership, offline dependency checks, service definitions and rollback.
   Preserve `/home/pi/distributed-dl-system`, existing agent/service and all jobs,
   DBs/caches/checkpoints. Prefer a separate versioned tree/test service during
   validation; actual names/ports/privileges must be verified before proposing
   concrete system commands. Review/approve those complete artifacts before apply.
5. Approved validation on pi02/pi03: authorized cold/warm single-worker input copies,
   signed summaries, exact policy choices/rollback, two-rank all-ready barrier,
   valid checkpoint/full-group recovery and retained unrelated work. Use only
   disposable identified jobs/artifacts for process faults or capacity fixtures.
   Collect actual bytes/time; do not claim measured link speed from synthetic rates.
   Idle-cache preview first; no active pins or authoritative deletions. Three-rank
   hardware tests stay deferred, with the user's current exception recorded.

This report is the plan for continued preparation, not permission to skip these
artifacts, weaken containment, edit the firewall or restart the production agents.
Central controller/LOCAL SQLite/authoritative storage remains a single point of
failure. No physical Phase 11 execution, Pi throughput benchmark, native two-rank
hybrid run or power/disk-loss test was performed by this preparation subphase.

## Tests, verification and rollback

Baseline, isolated: **8 PASS** (`test_cluster_setup.py`), log `baseline.log`.
Final selected regression:

```bash
PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_deployment_review.py test_cluster_setup.py test_storage_config.py test_bandwidth_policy.py test_cache_eviction.py
```

**76 PASS / five files; zero failures/errors/skips** (6 + 8 + 17 + 22 + 23).
Log `/tmp/hybrid-two-worker-prep/tests.log`; hashes/counts in evidence. New tests
verify reproducible actual archives/member hashes, explicit two-node deferral,
no socket/controller/SQLite dependency, existing-output retention, unsafe-source
rejection, runtime/key-store exclusion and source-change abort without a false
completion marker. Both real local artifacts were additionally checked for member
sets, regular files, archive hashes and explicit `apply_ready=false`.
`git diff --check` and default config validation pass. No live service test ran.

Safe manual commands above should print an offline manifest selecting pi02/pi03,
deferring pi01, `apply_ready=false`; tests should pass. Creating a bundle requires
a new output path and touches only that local output directory. Do not extract or
copy it into a live service root as a verification step.

Scoped source rollback, from the parent Git root after preserving later edits:

```bash
git apply --reverse --check /tmp/hybrid-two-worker-prep/preparation.patch
# After reviewing the scoped patch:
git apply --reverse /tmp/hybrid-two-worker-prep/preparation.patch
```

Reverse check passed; no rollback was applied. This reverses only the new module,
test, report/evidence and three documentation appends, preserving earlier Phase
10/11 work. Local archives may be retained for audit; no remote rollback is needed
because no deployment occurred. Never reset/clean, clear pins, unmount existing
shares, or delete jobs/checkpoints/DBs as rollback.

**PASS: pi02/pi03-only offline deployment preparation. Live deployment/security/
runtime acceptance: pending the concrete bootstrap/isolation artifacts and later
explicit approval. pi01 deferred by the user's current exception. STOP.**
