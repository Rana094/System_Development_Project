# Phase 14.1 — production readiness audit, 2026-11-10

**Audit complete; production hybrid deployment BLOCKED. Phase 14 is not complete.**
All three Pi aliases are now reachable by SSH and the controller can read each
agent's health/resources API. That supersedes the historical pi01 deferral for
this read-only audit. It does not establish a working three-worker hybrid system
or authorize deployment. The current system remains `legacy/OFF/resource`.
The central controller, local SQLite and NFS server remain a single point of
failure; no HA or verified disaster-recovery capability is claimed.

Scope: one subphase, 14.1. No sudo, remote publication/config writes, install,
mount/export/firewall change, service restart, DB open/migration, training job,
DDP group, fault injection, throughput test or restore test. Dependency checks
included bounded isolated imports and a two-element CPU tensor sum; these are
not training or DDP acceptance. Snapshot observations are timestamped in
[57-production-readiness-evidence.json](57-production-readiness-evidence.json).
Raw probes and output remain in `reports/phase14-1-readiness/` (ignored runtime
evidence). Do not infer success from a command's exit status when stderr or the
nested result records a failure.

## Repository and production configuration

Application root:
`/home/rana/Projects/System Project/System_Development_Project/distributed-dl-system`.
Git root is its parent, branch `main`, starting status clean, HEAD
`7e88b8414b00d3169916e70495ce14d86f24e05b` (committed evaluation work).
397 original nonignored files were hashed and copied before changes. No
stash/reset/commit, data move or cleanup was performed.

Actual `config/storage.json`, also validated by the controller config CLI:

```json
{
  "storage_backend": "legacy",
  "worker_cache_enabled": false,
  "storage_aware_staging": false,
  "scheduler_policy": "resource"
}
```

`config/nodes.json` uses the three Tailscale addresses below. NFS server config
still selects **pi02 and pi03 only**, with pi01's NFS client address null;
server address is `192.168.10.214`, publisher UID/GID `125:125`. Worker-local
`config/nodes.json` copies retain the old LAN addresses. They must not override
the controller's current identity record or be mistaken for proof of reachability.
Secrets, private SSH keys and service environment values were not read or copied.
Absence of the two queried worker credential filenames is not a global secret
inventory or an authentication proof.

## Verified host and SSH inventory

`ssh -G` resolved all aliases to user `pi`, port `22`, identity path
`~/.ssh/id_rsa`, `IdentitiesOnly no`. This records a key path, not key material.
All probes used existing host trust with `StrictHostKeyChecking=yes`,
`UpdateHostKeys=no`, batch mode, no connection sharing or local command.

| Alias | SSH HostName / controller API IP | HostKeyAlias (lookup label) | Actual Ethernet IPv4 | OS / kernel | System Python / agent Python | UID:GID |
|---|---|---|---|---|---|---|
| pi01 | 100.101.90.46 | 192.168.10.125 | **192.168.0.107/24** | Debian 13.5 trixie / 6.18.50+rpt-rpi-2712 | 3.13.5 / **3.11.16** | 1000:1000 |
| pi02 | 100.91.135.18 | 192.168.10.150 | 192.168.10.150/24 | Debian 12 bookworm / 6.12.25+rpt-rpi-2712 | 3.11.2 / **3.11.16** | 1000:1000 |
| pi03 | 100.64.202.71 | 192.168.10.101 | 192.168.10.101/24 | Debian 12 bookworm / 6.12.25+rpt-rpi-2712 | 3.11.2 / **3.11.16** | **1002:1002** |

`HostKeyAlias` is a host-key database label, not an observed address; in
particular do not use `192.168.10.125` as pi01's current LAN address. pi01 routes
toward the NFS server via its `192.168.0.1` gateway. Its Tailscale SSH/API reachability
does not imply a route to the controller's NFS LAN address.

Controller: Ubuntu 24.04.4 LTS, x86_64, kernel `7.0.0-42-generic`, Python
3.12.3 (system and `.controller-venv`), user rana `1000:1000`. Wi-Fi `wlo1` is
`192.168.10.214/24`, Tailscale `100.73.90.91`; physical Ethernet `enp4s0` is down
with no carrier. A wired controller path is not presently demonstrated.

All three `eth0` links report **100 Mbit/s, full duplex**. Arithmetic ceiling:
12.5 MB/s = about 11.92 MiB/s before overhead, not an observed transfer rate.
Controller Wi-Fi, shared server bandwidth, Tailscale path and concurrent reads
may add bottlenecks. No speed test or disruptive page-cache manipulation ran.
Actual effective NFS staging throughput must come from measured transfers.

## Services, code drift and ownership

| Host | Unit / scope | Service identity | PID before = after | State |
|---|---|---|---|---|
| controller | dl-dispatcher.service / user | rana | 1662 | active/running, enabled |
| controller | dl-student-portal.service / user | rana | 1663 | active/running, enabled |
| pi01 | dl-node-agent.service / system | pi (1000) | 1036 | active/running, enabled |
| pi02 | dl-node-agent.service / system | pi (1000) | 946 | active/running, enabled |
| pi03 | dl-node-agent.service / system | pi (1002) | 902 | active/running, enabled |

The controller NFS server is active/exited and enabled, consistent with its
service type; MainPID=0 is not evidence that NFS is down. All Pi service roots
are `/home/pi/distributed-dl-system`; interpreters resolve to the managed uv
CPython 3.11.16 installation through `.venv`. User-unit empty `User=` fields
on the controller mean the invoking user's service, not root.

The live Pi roots all have the same older `agent/agent.py`:

- Installed SHA-256 `4d510093da9302348feb132d4bb7ab2870fda0ed1d18692b296cab517fcc429a`, 46,820 bytes.
- Checkout SHA-256 `33395ab48ffe11433c8f91d443df63998bba46d0301ddd0a1c43a5f6585a626a`, 50,886 bytes.
- Queried live `agent/staging.py`, `agent/cache_storage.py`, checkpoint publication/
  restore services and `hybrid_storage/config.py`, `worker_cache.py`,
  `cache_registry_schema.py` are absent on all three. Live `config/storage.json`
  is absent too. Existing isolated evaluation bundles elsewhere do not establish
  installation in a service's working directory.

Source hashes/PIDs do not prove the precise code already loaded in each running
process. No process was restarted to resolve this uncertainty.

Pi units report `PrivateMounts=no`, `NoNewPrivileges=no`, `MemoryMax=infinity`
and `CPUQuotaPerSecUSec=infinity`. Tool presence (`bwrap`, `unshare`, `setpriv`,
network utilities) does not establish privileges or secure containment. Existing
resource admission is not an enforced cgroup memory/CPU limit
(`agent/agent.py`, RAM admission comment around line 840).

## NFS identity, permissions and availability

Controller configuration file `/etc/exports.d/distributed-ml.exports` contains:

```text
/srv/ml-shared/objects 192.168.10.150(ro,sync,root_squash,subtree_check,secure,sec=sys) 192.168.10.101(ro,sync,root_squash,subtree_check,secure,sec=sys)
```

This is the existing reviewed subdirectory topology with `subtree_check`, not a
new proposal to alter it. `/etc/exports` on the controller has only comments.
There is no controller wildcard export in the files read. `exportfs -v` returned
0 but stderr reported permission denied opening `.etab.lock` and stdout was
empty: **the effective export table remains unverified**, not proven empty.
Reading `/proc/fs/nfsd/versions` also returned permission denied.

| Check | pi01 | pi02 | pi03 |
|---|---|---|---|
| nfs-common installed | 1:2.8.3-1 | 1:2.6.2-4+deb12u1 | 1:2.6.2-4+deb12u1 |
| TCP 192.168.10.214:2049 | **timeout** | reachable | reachable |
| Exact `/srv/mlnfs/inputs` | absent | NFS4.1, mount ID 509 | NFS4.1, mount ID 508 |
| Full mount-table admission | **rejected** | accepted | accepted |
| TEST object read as pi | missing path | permission denied | permission denied |
| Mount/automount setup | absent | active, automount enabled | active, automount enabled |

Mounted inputs on pi02/pi03 identify source
`192.168.10.214:/srv/ml-shared/objects`, VFS
`ro,nosuid,nodev,noexec`, NFS4.1/TCP/AUTH_SYS, `hard,timeo=50,retrans=2`, 1 MiB
rsize/wsize. `timeo` is deciseconds; hard mounts may continue retrying and do
not become a bounded 15-second file I/O timeout. The systemd mount unit's
`TimeoutUSec=15s` is its mount operation timeout. NFS superblock options contain
`rw` while the per-mount VFS is `ro`; the existing admission guard explicitly
handles this valid arrangement. Admission was rechecked using the **complete**
`/proc/self/mountinfo`, including ancestor mounts, not the earlier filtered table.

`/srv/mlnfs` is root-owned mode 0700 on pi02/pi03. This explains why ordinary
pi cannot traverse to read the approved 165-byte public TEST object. It also
means the current pi service cannot be assumed to act as the proposed trusted
stager. Do not loosen it globally merely to make a probe pass. The controller
read and SHA-256 of existing TEST digest
`0cb28871cf66d68026f61a434086adfca958eb18c1353bd76fde4151a687e897`
matched. No fresh privileged client hash or write-denial test ran in 14.1;
historical successful checks are not substituted for current acceptance.

`ml-nfs-ready.service` exists on pi02/pi03, static/inactive; its required mount
exists. Current agent unit dependencies do not include this readiness guard.
That preserves legacy boot during an NFS outage, but hybrid staging needs an
explicit fail-closed readiness check in its trusted bootstrap. Boot/reboot and
outage behavior have not been revalidated here.

**Additional existing exposure:** pi02 `/etc/exports` configures
`/srv/nfs/cifar10_data *(rw,sync,no_subtree_check,no_root_squash)`. pi02/pi03
also have the unrelated legacy RW NFS4.2 `/mnt/nfs_share` mount. This is not the
new RO input share, and no writes were attempted. Effective exposure and users
need review; do not remove files, exports or services that may serve other jobs.
NFS AUTH_SYS is not a per-student security boundary. Required containment must
prevent untrusted code reaching either shared mounts or the server over the
network; DDP needs only reviewed peer communication, not general network access.

## Dependency compatibility and offline readiness

Controller `.controller-venv`: Flask 3.1.3, requests 2.34.2, psutil 7.2.2,
waitress 3.0.2, torch distribution 2.14.1, NumPy 2.5.3, pip 26.2.1;
`pip check` PASS. These are installed metadata observations, not a claim of
identical controller and ARM training binaries.

All three agent `.venv`s: Python 3.11.16; Flask/requests/psutil/waitress versions
match the above, torch distribution 2.14.1, NumPy 2.4.6, pip 26.2.1.
**All three `pip check` runs FAIL** with:
`nvidia-cusparselt-cu13 0.8.1 is not supported on this platform`.
pi02/pi03 actually import torch `2.14.1+cu130`, report CUDA build 13.0 and Gloo
available, and return 3.0 from the isolated CPU sum. This does not cure the
unsupported package or prove distributed collectives work.

pi01 torch import/probe timed out at 15 seconds, then a separate 40-second SSH
retry timed out. Cause unknown; no ABI/root-cause assertion is supported.
A later exact-probe-code/process-owner check found no matching leftover probe
process and sent no signals. Agent health remains separate from torch readiness.

Prepared training environment observations (not installed by this audit):

- pi01: queried `worker-storage/environments` absent.
- pi02: two prepared environments, torch `2.15.0+cpu`, no NumPy, pip check PASS.
- pi03: four prepared environments with `2.14.1+cpu` or `2.15.0+cpu`; one has
  NumPy 2.4.6. Three initial prepared-environment pip checks PASS; imports in all
  four report Gloo available and CPU sum 3.0. NumPy-absent imports warn about
  NumPy initialization; actual workload requirements decide usability.
- Identically named environment key `1e878968...` resolves different torch
  versions on pi02 and pi03. All inspected ready markers match their installed
  package probes. `agent/environment_worker.py::verify_environment` compares
  the complete probe and pip check; `prepare` constructs a compatibility key
  including resolved packages (excluding packaging tools), Python major/minor,
  platform and profile. Thus cache-key equality alone is insufficient, and this
  observation does **not** prove compatibility fencing is broken. A matching
  `f3beb767...` CPU environment exists on pi02/pi03; no complete three-node
  compatible group or real DDP run has been verified.

No approved offline wheelhouse, Python/ARM wheel hashes or matching three-worker
training dependency lock was validated. Existing environment provisioning includes
an Internet PyTorch index; do not use it for the new worker runtime path.
A reviewed offline dependency set is required before deployment, with no silent
DDP world-size or checkpoint-format migration.

## Disk, SQLite and restore points

| Host | Local filesystem at project | Available bytes at probe | Approximate GiB |
|---|---|---:|---:|
| controller | ext4 /dev/nvme0n1p4 | 43,530,088,448 | 40.541 |
| pi01 | ext4 /dev/mmcblk0p2 | 6,083,952,640 | 5.666 |
| pi02 | ext4 /dev/mmcblk0p2 | 11,457,048,576 | 10.670 |
| pi03 | ext4 /dev/mmcblk0p2 | 9,281,269,760 | 8.644 |

These are free-space observations, not quotas or permission to consume that space.
Cache/workspace/partial-copy/checkpoint/environment budgets and reserve must fit
**together**. A library's default spool ceiling (for example the approximately
10 GiB checkpoint publisher limit) is not a per-device capacity plan. Do not
choose destructive eviction to meet a deployment requirement.

`controller/jobs.db` is local ext4, 1,601,536 bytes at initial stat, rana-owned,
mode 0644; it was not opened, copied as a live backup or migrated. SQLite must
remain on controller LOCAL disk. Live DB/schema/queue contents are not verified
by a file stat or by the unit tests using disposable databases.

All three Pis retain one `worker-storage/deployment-previous` directory with
source/config/service files. It is a **source-only previous snapshot**, not a
consistent jobs/cache/environment/checkpoint backup or tested restore point.
Queried project backup locations are absent; external backups are unknown.
Controller `/var/backups/distributed-ml-nfs` and pi02/pi03
`/var/backups/distributed-ml-nfs-client` exist root-owned mode 0700; listing is
denied. Their contents, freshness and restore viability remain unverified.
Controller `/srv/ml-shared/.nfs-setup.json` is root-owned mode 0600 and was not
read. Do not represent a setup journal as application-state disaster recovery.

Before approved mutation, 14.2 must plan unique source/config/unit snapshots,
a SQLite-consistent backup (including correct WAL handling), matching immutable
manifest/object/checkpoint metadata and protected credential recovery, plus an
isolated restore check. Never restore over a running DB, job tree or checkpoint.

## Exact code gates and next-plan prerequisites

The source components are substantial and locally tested; production wiring is
still intentionally incomplete. Relevant current interfaces:

| File / interface | Observed gate or hazard |
|---|---|
| hybrid_storage/config.py:113 `require_controller_config` | Rejects cache enabled or non-resource scheduler; held NFS submissions are distinct from NFS execution. |
| hybrid_storage/config.py:126 `require_staging_config` | Validates an explicitly constructed component, does not construct/start it. |
| controller/dispatcher.py:1214 `main` | Calls controller gate and default reconciliation/dispatch loops without a storage runtime. |
| controller/student_portal.py controller-config calls | Startup/submission remains governed by the same gate; preserve legacy routes/authorization. |
| controller/storage_selection.py:49 `require_execution` | Requires explicit trusted runtime for pinned NFS jobs; no silent legacy reinterpretation. |
| agent/agent.py:1380 startup | Existing Flask agent/lease ownership startup; no production cache/staging/checkpoint/inventory bootstrap. |
| agent/staging.py:157 `launch_inputs` | Requires callable reviewed termination/quiescence providers; must remain fail closed. |
| hybrid_storage/deployment_review.py:82 `review` | Offline catalog explicitly returns `apply_ready=false`, not an approved installer. |
| tools/cluster_setup.py:136 `bundle` | Omits hybrid_storage; cannot deliver this checkout as an integrated hybrid agent. |
| tools/cluster_setup.py installer around 192–245 | sudo/service actions, Internet-capable pip install, replacement/deletion of its sole previous snapshot. Do not run for this delivery. |
| agent/environment_worker.py:275,418 | Verify complete prepared environment and use resolved compatibility identity across DDP ranks. |

Deployment blockers B1–B10 and required remedies are recorded in the companion
JSON: production bootstrap/lifecycle integration; enforceable OS/network trust
boundary and authenticated services; pi01 NFS topology/setup; offline compatible
dependencies; aggregate resource budgets; consistent backups and tested restore;
new all-three-node dry-run deployment/rollback; unrelated legacy export exposure;
privileged current NFS and end-to-end acceptance; incomplete physical evaluation.

**Immediately next subphase is 14.2, preparation only.** Its script must refuse
apply while these readiness gates are unresolved, identify required integration
work rather than bypass guards, preserve immutable release identities and legacy
in-flight jobs, and show exact proposed changes before requesting later apply
approval. This audit does not implement that script or authorize mutation.

## Validation, manual checks and rollback

Actual selected safe local run (no new production code):

```bash
PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_storage_config.py test_storage_selection.py test_deployment_review.py test_nfs_client_setup.py
```

**72 PASS, 0 failures/errors/skips, four files:** config17, storage selection21,
deployment review6, NFS client setup28. Log:
`reports/phase14-1-readiness/local-tests.log`. This selected regression run is
not the entire repository suite or a proof of all legacy job workflows.
Existing failing agent pip checks and pi01 timeouts are separate audit findings,
not concealed by unit-test success. All observed service PIDs remained unchanged.

Read-only manual commands, from the application root:

```bash
git status --short
.controller-venv/bin/python -B -m hybrid_storage.config
.controller-venv/bin/python -B -m hybrid_storage.deployment_review --nodes pi01 pi02 pi03
ssh -G pi01
ssh -G pi02
ssh -G pi03
ssh -o BatchMode=yes -o StrictHostKeyChecking=yes -o UpdateHostKeys=no pi01 'hostname; ip -br -4 address; python3 --version; systemctl show dl-node-agent.service -p User -p MainPID -p ActiveState; df -B1 /home/pi/distributed-dl-system; findmnt -t nfs,nfs4'
ssh -o BatchMode=yes -o StrictHostKeyChecking=yes -o UpdateHostKeys=no pi02 'hostname; ip -br -4 address; python3 --version; systemctl show dl-node-agent.service -p User -p MainPID -p ActiveState; df -B1 /home/pi/distributed-dl-system; findmnt -t nfs,nfs4'
ssh -o BatchMode=yes -o StrictHostKeyChecking=yes -o UpdateHostKeys=no pi03 'hostname; ip -br -4 address; python3 --version; systemctl show dl-node-agent.service -p User -p MainPID -p ActiveState; df -B1 /home/pi/distributed-dl-system; findmnt -t nfs,nfs4'
```

Expected point-in-time outcomes: legacy config accepted; source review selects
three workers but `apply_ready=false`; three known aliases; pi01 no input NFS
mount, pi02/pi03 RO 4.1 input mount (plus unrelated legacy RW share). Free space,
addresses and PIDs can change; always compare identity/source/security metadata
again before approval. `findmnt` returning no match on pi01 is an audit finding,
not a reason to create an ordinary directory and launch training through it.
Detailed probe programs, timeouts and exact output hashes are retained with the
evidence; no privileged commands are supplied as implicit apply instructions.

Changes: this report, its evidence JSON, and an append to
`implementation-progress.md` only. **No API, schema, flag, DB, dependency,
authoritative data or service configuration changes.** README and final demo
architecture are reserved for 14.4.

Rollback needs no runtime action. Review the scoped documentation patch
`reports/phase14-1-readiness/14.1.patch`, then from the **git root** check:

```bash
git apply --reverse --check distributed-dl-system/reports/phase14-1-readiness/14.1.patch
```

If these exact documentation edits are still the only edits being reverted,
`git apply --reverse` with that same path reverses just this subphase. Check
again if the files have since changed; do not reset other work. Reverse application
is exercised only in a disposable copy under the reports directory. Keep all
reports, jobs, objects, checkpoints, environments and existing backup directories.
Source preservation and reverse-check evidence is in
`reports/phase14-1-readiness/source-verification.json`.

**14.1 audit PASS (read-only coverage and explicit unknowns). Deployment readiness
FAIL/BLOCKED. Full Phase 14 acceptance NOT RUN/NOT MET: all-three hybrid jobs,
cancellation/recovery/DDP, mount read/write controls, permissions and service boot
still require resolved gates, an approved deployment and isolated acceptance.
STOP before 14.2.**
