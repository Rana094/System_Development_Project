# Phase 14.1 closeout work — verified recovery point and protected NFS checks

**Readiness is still blocked; do not mark 14.1 or 14.2 fully complete.** This
continuation finishes specific verifications with actual code and evidence, under
the one-subphase rule, before an applying 14.2 installer or 14.3 deployment.
The user chose to keep pi01 on its current network and approved the narrowly
scoped protected read-only checks. No approval to change NFS, routing, dependencies,
permissions, firewall, SSH or services was inferred. The user supplied the
successful controller terminal probe on 2026-11-10 and explicitly retained
**local/private-only runtime data transfers**. The current pi01 relay path does
not satisfy that requirement. Neither decision authorizes deployment.

## Implemented and actually verified

New files are `hybrid_storage/state_backup.py`, `hybrid_storage/readiness_probe.py`,
`tools/storage_readiness.py`, and two focused test files. These are explicit
operator tools; no import/startup route, website, scheduler, configuration flag,
job state or existing schema/API changed. Existing uncommitted work was preserved.
The wrapper can emit a portable stdlib-only program so approved worker probes
need no remote file installation or package transfer.

The backup helper opens the explicit controller LOCAL SQLite source `mode=ro`,
reads WAL through SQLite (never immutable=1 or a raw live-DB copy), and creates
only a NEW private mode0700 directory with mode0600 database copies and completion
manifest. It verifies local descriptors, disk reserve/byte limits, deadline,
source inode stability, quick/FK checks, every table's typed logical content and
user_version against a separately restored copy. No live restore or migration API.
Incomplete outputs are retained without a marker; existing outputs are never
reused or overwritten. Trusted source/output parents are required against
same-UID/admin replacement. File data/credentials/rows are never printed.

Actual recovery point:
`reports/phase14-1-closeout/db-recovery-01/`.
Both `snapshot.sqlite` and `restored.sqlite` are **1,601,536 bytes**, SHA-256:
`5e9fc64bf5c3f2fac165b7493c36ef2c82436476ad7e03f7bd0976d769572144`.
Full logical content/13 table counts and user_version match. This proves a
**database-only LOCAL recovery point**, not an application-files/checkpoint
recovery point, off-host backup, whole-system disaster recovery or HA.
The DB copies contain private application data and remain ignored/private;
do not add them to Git or publish their contents.

An additional NEW clone of this verified snapshot underwent all six proposed
hybrid migrations twice. All **150 original jobs**, all13 original tables/rows
and user_version were preserved. The second-pass logical fingerprint was
unchanged, quick_check `ok`, FK violations0. The source recovery snapshot remained
byte-identical. This is real existing-state compatibility rehearsal, **not a
production migration**. Exact module results/checksums are retained in
`reports/phase14-1-closeout/migration-rehearsal-result.json`.

## Approved protected worker results

| Host | Input mount | Existing 165-byte TEST SHA-256 | Write-open denial | Setup backup journal |
|---|---|---|---|---|
| pi01 | absent/rejected | NOT_RUN; no ordinary-directory fallback | NOT_RUN | absent |
| pi02 | RO NFS4.1, ID509 | verified | **EROFS (errno30)** | root0700; two journal names listed |
| pi03 | RO NFS4.1, ID508 | verified | **EROFS (errno30)** | root0700; one journal name listed |

Expected digest:
`0cb28871cf66d68026f61a434086adfca958eb18c1353bd76fde4151a687e897`.
The test only attempts O_WRONLY on that exact existing verified public object,
without CREATE/TRUNCATE/APPEND; **zero bytes were written**. A successful open is
closed and reported as failed RO proof; EACCES alone does not count as EROFS.
No arbitrary submitted paths, publication, job output or checkpoint is touched.
Backup journal contents were not read and their restore viability is not claimed.

The first protected worker attempt exposed a checker bug: the no-follow parent
walk rejected kernel `/proc/self`. Fixed to numeric `/proc/<own-pid>/mountinfo`
without relaxing no-follow reads. A new real portable-program test covers that
kernel path. Failed outputs and original program retained; corrected v2 passes
on all three workers within the same already-approved protected-read scope.
The initial controller `sudo -n` returned “a password is required”; that tool
attempt read no protected files. The later successful user-terminal result below
closes this authentication-related evidence gap. Both records are retained.

## Controller protected result supplied by the user

The user ran the approved corrected v2 command as root and pasted its JSON.
`reports/phase14-1-controller-verification/controller-user-terminal.json` retains
the supplied values; `validation.json` records provenance and nine PASS consistency
checks. This was **not independently rerun by the assistant**. The retained
program SHA-256 matches the approved v2 program. The probe reports read-only scope,
no configuration changes, no live restore and zero bytes written.

Effective `/var/lib/nfs/etab` has exactly two entries: `/srv/ml-shared/objects`
for `192.168.10.150` and `192.168.10.101`, both `ro,sync,root_squash,secure,sec=sys`.
The managed exports file agrees; `/etc/exports` contains comments only. No wildcard,
home, jobs DB or RW export appears in this controller effective table. This does
not change the separately documented legacy RW exposure on pi02.

Kernel versions report `-3 +4 -4.0 +4.1 +4.2`: NFS4.1/4.2 enabled; NFS3/4.0
disabled. The existing TEST object is 165 bytes with the expected digest verified.
Controller write-open was **not attempted**; its false RO-denial field is expected
for this role, whose trusted publisher writes locally. Worker EROFS evidence is
separate. Root-owned mode0700 setup journal has one entry name; contents were not
read and restore was not verified. This point-in-time result is not throughput,
all-three connectivity, backup restore or end-to-end production acceptance.

## Actual network findings and bottleneck question

Controller NFS listens only on `192.168.10.214:2049`. `/etc/nfs.conf.d/distributed-ml.conf`
contains that host binding and enables NFS4/TCP, disables NFS3/UDP. All three Pis
get connection-refused for `100.73.90.91:2049`. pi01 times out to the LAN NFS
address; pi02/pi03 reach it. pi01 routes LAN NFS through192.168.0.1 from192.168.0.107;
the controller routes toward pi01 via192.168.10.1. These are observed default
routes, not proof of a private routed path and not proposed router next hops.

Tiny Tailscale path probes (maximum3 pings per path, stop early on direct):

| Path | Path observed | Reported latencies | Replies |
|---|---|---|---:|
| controller → pi01 | DERP(sin), no direct connection | 114,121,117 ms | 3 |
| pi02 → pi01 | DERP(sin), no direct connection | 231,105,105 ms | 3 |
| pi03 → pi01 | DERP(sin), no direct connection | 250,113,113 ms | 3 |
| pi02 → pi03 | direct192.168.10.101:41641 | 2 ms | 1 |

Exit1 on the pi01 ping commands means a direct connection was not established;
replies arrived. These are limited path/latency observations, **not measured
throughput, application RTT distributions, DDP step timings or training slowdown**.
Tailscale documents relay paths as generally slower/lower-throughput than direct
paths: [official connection types](https://tailscale.com/docs/reference/connection-types).
DDP synchronizes during training, so the high-latency pi01 path is a substantial
performance risk. A warm cache removes repeated dataset staging, not gradient
synchronization: [official DDP tutorial](https://docs.pytorch.org/tutorials/intermediate/ddp_tutorial.html).
Actual impact still needs approved isolated workload measurements.

The current dispatcher constructs `master_addr` from selected worker IPs
(`controller/distributed_dispatch.py:141`); configured worker IPs are Tailscale
addresses. Ethernet connectivity does not alone prove a plain-LAN DDP path;
pi02/pi03's observed Tailscale path happens to be direct LAN. No launcher or
advertised address was silently changed.

Because the user kept pi01's current network and runtime transfers must not depend
on the Internet, this work does not enable NFS on the relay as a workaround.
A private/local interconnect is needed before claiming all-three local-NFS
readiness. The user explicitly reaffirmed local/private-only transfers; Internet
Tailscale data/checkpoint/DDP traffic is not an approved fallback. Offline validated proposal
files preserve NFS binding192.168.10.214, add only verified pi01 address192.168.0.107
to the restricted RO/root_squash export, and retain identical pi02/pi03 client
unit bytes. These proposals are **not installed**, have apply_ready=false, and
require a verified bidirectional no-SNAT route. Router topology/next hops remain
unknown; no fabricated route/firewall command or device reconfiguration.
See `reports/phase14-1-closeout/network-review.md` and companion JSON/proposals.

## Remaining gates

All three agent `.venv` pip checks still FAIL on unsupported
`nvidia-cusparselt-cu13 0.8.1`. Queried pip/uv wheel-cache directories are absent;
no offline wheelhouse was fabricated or packages downloaded/installed. The earlier
pi01 Torch import timeout and unverified three-worker CPU dependency group remain.
A clean pinned offline service/training environment needs an exact reviewed plan.

The completed user-terminal check used the following exact corrected command
(retained for reproducibility; **no repeat is currently required**):

```bash
sudo /usr/bin/python3 -I -B '/home/rana/Projects/System Project/System_Development_Project/distributed-dl-system/reports/phase14-1-closeout/probe-program-v2.py' --role controller --privileged --verify-test-input
```

Its supplied JSON covers effective `/var/lib/nfs/etab`, kernel NFS versions,
setup journal names and existing public TEST bytes. Original/corrected approved
program hashes, earlier failure and commands remain under `protected-read-plan*.json`
and the companion evidence. No password was requested, stored or included.

Still unresolved: pi01 NFS/private data path; clean compatible dependencies;
coordinated app-files/checkpoint recovery and off-host backup; production trusted
bootstrap/containment; applying deployment executor and concrete unit/config
changes. 14.2's previous review-v2 is historical: newly added modules intentionally
change catalogs. A later completed installer needs a NEW reviewed package; no
old frozen package, marker or active config is overwritten to disguise drift.
All-three requirement is preserved. Central controller/local DB/NFS remains a SPOF.

## Tests, manual verification and rollback

Final safe local run:

```bash
PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated \
  test_storage_config.py test_storage_selection.py test_deployment_review.py \
  test_nfs_client_setup.py test_delivery_plan.py test_state_backup.py test_readiness_probe.py
```

**105 PASS**: previous88 + backup10 + protected probe7, seven files,
0 final failures/errors/skips. Initial104 passed before the real protected probe
exposed `/proc/self`; the corrected105 run includes its regression test. The
remote failures, sudo-auth failure and corrected results are retained honestly.
No live jobs, DDP group, fault/outage injection, bandwidth test, production DB
migration/restore, package install or service/config/network changes occurred.
Only approved sudo **read** probes ran; no root setup/deploy command.

This controller-evidence update changes documentation only. The nine supplied-JSON
consistency checks pass; source hashes, preservation and scoped rollback checks
are recorded under `reports/phase14-1-controller-verification/`. The prior105
unit tests above were **not rerun** for this update. No additional SSH, sudo,
live DB read, training or hardware test was performed by the assistant.

Manual checks (new output name required for another DB recovery point):

```bash
.controller-venv/bin/python -B tools/storage_readiness.py backup-db \
  --source '/home/rana/Projects/System Project/System_Development_Project/distributed-dl-system/controller/jobs.db' \
  --output '/home/rana/Projects/System Project/System_Development_Project/distributed-dl-system/reports/phase14-1-closeout/db-recovery-NEW'
```

Expected: restored_copy_verified=true and coordinated_filesystem_backup=false.
Full raw records, private recovery copies and code/proposal hashes are under
`reports/phase14-1-closeout/`; tracked companion evidence contains only manifests,
metadata and aggregate/hash results, never DB rows or credentials.

Source-only rollback patch: `reports/phase14-1-closeout/closeout.patch`. From git root:

```bash
git apply --reverse --check distributed-dl-system/reports/phase14-1-closeout/closeout.patch
```

After reviewing exact applicability, omitting --check reverses only this
continuation's source/docs changes; actual reverse application tested only in a
NEW disposable copy. Preserve all private backup/report directories. Existing
406 source files are preserved except intended README/progress appends; no reset,
stash, commit, authoritative-file deletion or overwrite. Nothing deployed requires
a live service rollback. API/config/DB schema unchanged; legacy/OFF/resource intact.

For only this later controller-evidence/network-decision update, use the narrower
`reports/phase14-1-controller-verification/documentation-update.patch` instead of
the earlier closeout patch. From the git root, review applicability with:

```bash
git apply --reverse --check distributed-dl-system/reports/phase14-1-controller-verification/documentation-update.patch
```

Reverse application is tested only in a NEW disposable copy. Preserve all reports
and backups; no live rollback is required. The earlier whole-closeout patch is a
historical artifact and must not be assumed applicable after these doc updates.

**Verification gaps narrowed with actual results;14.1 readiness still BLOCKED,
14.2 applying tooling unfinished,14.3 NOT RUN. Controller protected evidence
accepted with stated provenance; no full-phase PASS claimed. STOP pending a
verified local/private route for pi01 and the other readiness gates above.**
