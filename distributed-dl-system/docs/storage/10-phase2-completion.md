# Phase 2 completion — approved pi02/pi03 scope

7 November 2026, Asia/Dhaka. **PASS for pi02 and pi03.** The user's explicit
instruction, “no do it without pi1 / pi2 and p3 are active”, overrides the original
three-Pi requirement for this infrastructure validation. pi01 remains deferred;
it was not contacted, configured or exported to. This is not an all-three-Pi pass.

This report supersedes the blocked status in the historical
[2.4 preflight](08-nfs-two-node-deployment.md) and
[2.5 preflight](09-nfs-outage-validation.md). Earlier reports/evidence retain their
original observations. See [captured evidence](10-phase2-live-evidence.json) for
commands, stdout/stderr, executed fixture source, hashes, journals and test log.

Only Phase 2 was completed. The application still uses `storage_backend=legacy`,
`worker_cache_enabled=false`, `storage_aware_staging=false`,
`scheduler_policy=resource`. No cache registry, locality placement or new runtime
staging was enabled. **The controller remains a single point of failure.**

## 1. Implemented behavior and repository changes

The controller now serves a restricted read-only input export. Both selected Pis
have the separate on-demand read-only input mount and metadata admission guard.
Two public synthetic artifacts were published with verified immutable bytes.
Both workers read/hash-verified the baseline, rejected writes, and passed
disposable unmount/mount-failure/disconnect/recovery tests. Existing agents and
the old pi02-hosted share were preserved.

| Repository file | Change in this completion turn |
|---|---|
| `config/nfs-server.json` | Record the actual publisher UID/GID, 125/125; retain explicit pi02/pi03 selection. |
| `hybrid_storage/nfs_mount.py` | Require read-only/security flags in the VFS field; permit Linux's separate rw NFS superblock flag. |
| `tests/test_nfs_client_setup.py` | Regression for read-only VFS over rw superblock and missing/unsafe VFS flags. |
| `hybrid_storage/nfs_fault_probe.py` | New administrator-only public-TEST fault fixture; default invocation is a nonmutating plan. |
| `tests/test_nfs_fault_probe.py` | Five Pi-less relay/Unix-gateway/default-plan/mount-identity tests. |
| `docs/storage/10-phase2-completion.md`, `10-phase2-live-evidence.json` | Actual deployment, measurements, limitations, commands and rollback. |
| `docs/storage/implementation-progress.md` | Append this completion and its exact scope. |
| `docs/storage/08-nfs-two-node-deployment.md`, `09-nfs-outage-validation.md` | Add historical-status pointers without rewriting earlier observations. |
| `docs/README.md`, `config/README.md`, `hybrid_storage/README.md`, `tools/README.md`, `tests/README.md` | Make the current status and administrator/runtime boundary discoverable. |

The Git root is the parent `System_Development_Project`; branch `main` already
contained extensive staged/unstaged/untracked user work. No stash, reset, clean,
commit or service deployment from that unrelated work was performed. The scoped
before/after patch is `/tmp/hybrid-storage-phase2-completion.patch`; see repository
rollback below. Temporary raw evidence is under
`/tmp/hybrid-storage-phase2-live-en1on_ce`; durable evidence is checked into this
documentation tree as a new file, without making a Git commit.

## 2. API, DB, configuration and security

No application API, SQLite schema, job state, scheduling rule, lease, checkpoint
format, DDP path, HTTP upload flow or worker inventory changed. The controller
`jobs.db` SHA-256 matched its beginning-of-turn value. `findmnt -T` confirms the
DB remains on local `/dev/nvme0n1p4`, ext4; it is not on NFS. The 148-file baseline
inventory checks source/config/docs plus this DB, not a full recursive snapshot
of every historical job/checkpoint. No job/checkpoint data operations were issued.

### Actual controller configuration

Controller: `192.168.10.214`; selected clients: pi02 `192.168.10.150`,
pi03 `192.168.10.101`. No addresses were invented. Setup used normal desktop
administrator authentication (`pkexec`), not a sudo-policy change or credentials
in chat. Worker administrator commands used existing passwordless `sudo -n`.

The controller installed `nfs-kernel-server` and `nfs-common`
`1:2.6.4-3ubuntu5.1`, rpcbind `1.2.6-7ubuntu2`, and apt-recorded dependencies.
The actual apt history contains a successful direct install; it does not show an
`apt-get update` fallback. This was administrator bootstrap; no worker runtime
data/dependency transfer depends on the Internet. Both Pis already had nfs-common,
actual version `1:2.6.2-4+deb12u1`; no worker package installation was needed.

Created non-login/no-home `ml-publisher`, actual UID/GID **125/125**:

| Path | Actual ownership/mode or purpose |
|---|---|
| `/srv/ml-shared` | root, 0755; setup marker root 0600 |
| `/srv/ml-shared/objects` | 125:125, 0755; only public TEST objects so far |
| `/srv/ml-shared/.publish` | 125:125, 0700; private, not exported |
| `/etc/exports.d/distributed-ml.exports` | Two explicit object-export clients, no wildcard/subnet |
| `/etc/nfs.conf.d/distributed-ml.conf` | LAN-only TCP, NFSv4.1/v4.2; v3/v4.0/UDP disabled |
| `/etc/systemd/system/nfs-mountd.service.d/distributed-ml.conf` | Disable v2/v3 mount protocol network listeners |

The export contains exactly:

```text
/srv/ml-shared/objects 192.168.10.150(ro,sync,root_squash,subtree_check,secure,sec=sys) 192.168.10.101(ro,sync,root_squash,subtree_check,secure,sec=sys)
```

The reviewed subdirectory layout retains `subtree_check`; the alternative
`no_subtree_check` is not applied blindly to a directory on the controller's
larger root filesystem. Native `exportfs -ra` succeeded. `exportfs -v` and etab
show only these two explicit read-only/root-squashed input exports. No home,
database, checkpoints, `/srv/ml-shared` parent or physical root export was added.

Package startup initially introduced broad RPC listeners. Only the newly added
controller rpcbind service/socket and rpc-statd/notify services were stopped and
masked, and mountd's new drop-in disabled its v2/v3 TCP/UDP listeners. After the
new controller NFS service restart, actual NFS was **TCP 192.168.10.214:2049 only**;
there was no RPC port 111 or mountd/statd network listener. Kernel versions were
`-3 +4 -4.0 +4.1 +4.2`. Final server/mountd state was active; server enabled.
No worker's existing NFS server was restarted or reconfigured. No firewall rule,
SSH setting, fstab entry or existing agent unit was changed.

NFSv4 uses virtual ancestor nodes for path navigation: kernel export metadata
therefore lists `v4root` ancestors `/`, `/srv`, `/srv/ml-shared`. These are not
physical recursive directory exports. Both workers separately mounted the NFSv4
pseudo-root in disposable private mount namespaces: root listed only `srv`;
`etc`, `home`, `srv/ml-shared/.nfs-setup.json` and `.publish` all returned ENOENT.
These checks did not open private file contents. See the captured negative tests.

The protocol configuration follows the native
[nfsd](https://manpages.ubuntu.com/manpages/noble/man8/rpc.nfsd.8.html),
[mountd](https://manpages.ubuntu.com/manpages/noble/man8/rpc.mountd.8.html) and
[nfs.conf](https://manpages.debian.org/bookworm/nfs-common/nfs.conf.5.en.html)
interfaces; actual listeners and successful client operations establish this
host's observed behavior.

### Actual client configuration

Both selected workers received only these new files:

```text
/etc/systemd/system/srv-mlnfs-inputs.mount
/etc/systemd/system/srv-mlnfs-inputs.automount
/etc/systemd/system/ml-nfs-ready.service
/usr/local/libexec/distributed-ml/nfs_mount.py
```

`/srv/mlnfs` and the underlying mountpoint are root-protected 0700 directories.
The on-demand automount is enabled; the real input mount is active. Source:
`192.168.10.214:/srv/ml-shared/objects`, target `/srv/mlnfs/inputs`, actual NFS4.1,
`ro,nosuid,nodev,noexec,hard,proto=tcp,sec=sys,timeo=50,retrans=2`.
Native `systemd-analyze verify --man=no` passed for all three units on each Pi.
The ready oneshot completed successfully and is subsequently inactive by design.
It requires the real mount and checks kernel metadata, never accepting an ordinary
empty directory or autofs trigger as input readiness. It does not attest liveness
or bytes; stage-time verification is still required.

Existing `dl-node-agent` PIDs remained **945 on pi02, 917 on pi03**. Both fstab
hashes matched their original values. The old RW `/mnt/nfs_share` from
`192.168.10.150:/srv/nfs/cifar10_data` remains untouched and is not an approved
new hybrid input/publisher path.

`runuser -u pi -- test -r <TEST-artifact>` failed on both protected mounts.
However the mount remains in the host namespace, and AUTH_SYS/reserved ports
are not student authorization. Full training process/network isolation and
authenticated per-job staging are not implemented here. No student/private data
was published; do not enable hybrid execution before those controls exist.
Publication used the existing trusted local backend after dropping to UID/GID125,
verifying bytes and publishing new 0444 files. No worker RW checkpoint share or
arbitrary training-code publisher was introduced.

## 3. Tests and actual results

### Local, Pi-less regression checks

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py test_nfs_server_setup.py test_nfs_client_setup.py test_nfs_fault_probe.py test_storage_config.py test_storage_backends.py
```

**96 passed, 0 failed, 0 skipped; 5/5 files.** Server 22, client 28, fault fixture
5, config 17, backend 24. Temporary fake-host setup covers backup/idempotence and
failure paths. Relay tests use local TCP/Unix sockets without root or live NFS.
The controller ran Python3.12; the actual guard/fixture also ran on Pi Python3.11.2.
The evidence includes the full test output and executed source hashes. An optional
additional native controller repeat/no-op audit was canceled when renewed desktop
authentication was pending; it did not execute. Do not count it as a native
idempotence pass. Native setup was applied once; fake idempotence tests passed.

After the final explanatory fixture docstring/default-plan wording update, the
five fixture tests were rerun and passed. Python3.11 syntax compatibility, JSON,
documentation links, whitespace and reverse-patch checks passed. All 1,031 prior
Git status rows were preserved; exactly four new paths were added. The progress
log retains its original bytes as a prefix. Current installed configuration bytes
and SHA-256 hashes were also captured read-only for concurrent-edit-safe rollback.

### Live read-only input checks — 2.4

Each artifact is exactly **33,554,432 bytes (32 MiB)**, generated from public
deterministic data. Baseline SHA-256:
`f018ec88fef259becd2616f498a8dfb54dd354b4075c0ed5861597c615c2e62b`.
Outage-object SHA-256:
`e9c13bcb6d0ed0415b5871f2107717352cad504c9282aafd76ff6cff1bf066a0`.
Exact filenames, seeds and publication results are retained in evidence.

| Node | Version | Baseline SHA | Write probe | Read + SHA time | Observed MiB/s |
|---|---|---|---|---:|---:|
| pi02 | 4.1 | Match | EROFS (30), no file created | 8.0523 s | 3.9740 |
| pi03 | 4.1 | Match | EROFS (30), no file created | 3.6589 s | 8.7458 |

These are first observed baseline read-and-hash measurements on these mounts,
including CPU/cache effects; no global cache drop was performed. They are not
raw Ethernet throughput or a representative workload benchmark. No benchmark
was fabricated or extrapolated to the eventual lab cluster.

### Disposable outage/unmount checks — 2.5

The final fixture ran sequentially on each node inside distinct private mount,
network and hostname namespaces. A private loopback relay connects over a
root-private Unix socket to a reserved-source-port host gateway. Only this
fixture's TCP streams were interrupted. No host interface, route, firewall,
shared NFS listener, production job or agent was stopped. A private hostname and
network namespace avoid accidentally reusing the host NFS transport/client view.

The fixture normally unmounted its TEST view, rejected missing-mount admission,
attempted a mount while its relay was disconnected, restored transport, then
paused a new artifact reader after its first 4 KiB. The reader remained pending
during the fault; a verified private local copy remained readable. Restoring the
relay allowed the reader to finish with the exact full-object SHA. Final cleanup
used normal unmounts and stopped both dedicated relay/gateway processes.

| Node | Disconnected mount-helper failure | Reader paused for | Recovery after restore | Full SHA | Cleanup |
|---|---:|---:|---:|---|---|
| pi02 | 15.0054 s; exit 32, connection timeout | 3.0290 s | 4.6938 s | Match | Normal unmount; no pending processes |
| pi03 | 15.0020 s; exit 32, mount syscall failure | 3.0328 s | 4.8178 s | Match | Normal unmount; no pending processes |

Both reports have `complete=true`, `unmounted_admission_rejected=true`,
`local_input_read_during_outage=true`, and `pending_processes=[]`. Final host
guards remained ready; original agents, old mounts, host namespaces and hostnames
were intact. Captured resource samples showed both agents IDLE during observation.
Final process inspection found no remaining fault fixture processes on either Pi.

Configured unit timers: mount 15 s, ready process 5 s, automount idle 0.
The table measures the direct private mount helper, **not** a host systemd timeout
or boot test. No reboot was performed. `hard,timeo=50,retrans=2` does not bound a
later read's total wait; production staging still needs supervised I/O and pending
producer reconciliation. See the primary
[NFS mount](https://manpages.debian.org/bookworm/nfs-common/nfs.5.en.html) and
[systemd mount](https://manpages.debian.org/bookworm/systemd/systemd.mount.5.en.html)
semantics. No hard-reader cancellation guarantee is claimed.

### Failures encountered and corrected

* `pkexec` changed the working directory; initial relative setup-copy commands
  failed. Absolute paths were used before successful provisioning.
* pi02's initial ready service rejected a valid read-only mount because Linux
  reports an rw NFS superblock separately. The guard was corrected, regression
  tested, installed with an old-file hash check/backup, and readiness passed.
* The first mount-namespace-only outage attempt let the reader finish early;
  it **failed** the pending-reader assertion and is not counted as a pass.
  Its owned private mount was normally unmounted, with no pending readers, before
  its dedicated relay was terminated. Stronger network/hostname isolation was
  then added and both nodes passed. The exact reason for the first early read was
  not conclusively measured; transport reuse/cache effects were a possibility.

## 4. Manual verification commands and expected results

Run from the application root. These commands inspect configuration/metadata;
do not repeat live fault injection or reapply active client setup casually.

```bash
cat config/storage.json
python3 -m hybrid_storage.nfs_fault_probe
systemctl is-active nfs-server.service nfs-mountd.service
systemctl is-enabled nfs-server.service
ss -lntup
findmnt -T controller/jobs.db -o TARGET,SOURCE,FSTYPE,OPTIONS
ssh pi02 'findmnt -t nfs,nfs4 -o SOURCE,TARGET,FSTYPE,OPTIONS'
ssh pi03 'findmnt -t nfs,nfs4 -o SOURCE,TARGET,FSTYPE,OPTIONS'
ssh pi02 'sudo -n /usr/bin/python3 -I -B /usr/local/libexec/distributed-ml/nfs_mount.py --source 192.168.10.214:/srv/ml-shared/objects'
ssh pi03 'sudo -n /usr/bin/python3 -I -B /usr/local/libexec/distributed-ml/nfs_mount.py --source 192.168.10.214:/srv/ml-shared/objects'
ssh pi02 'systemctl show dl-node-agent.service --property=MainPID --property=ActiveState; sha256sum /etc/fstab'
ssh pi03 'systemctl show dl-node-agent.service --property=MainPID --property=ActiveState; sha256sum /etc/fstab'
python3 -m json.tool docs/storage/10-phase2-live-evidence.json > /dev/null
git diff --check
```

Expected: flags legacy/off/resource; fixture `mode=plan, mutations=false`; two
active controller units and enabled server; NFS listener only LAN TCP2049;
DB ext4; old share plus new RO NFS4.1 mount; guards `ready=true`; original agent
PIDs active; JSON valid; no whitespace errors. Expected fstab SHA-256:

```text
pi02 29b7314c3007dbb2c0e8c4cbed7ff1fd8fa2a87d7a4277cb62037ee822209e13
pi03 a2f8046258c00d33f7d90e0cbcf1dc21edc3884e6913f034a9814e3dc2d392b3
```

Root-only `sudo exportfs -v`, `sudo cat /proc/fs/nfsd/versions`, and inspection
of the journals below provide further native checks. OS authentication may be
needed again. Do not use `showmount` as the v4-only reachability criterion because
the legacy MOUNT/RPC services are intentionally disabled.

## 5. Rollback and residual risks

No rollback was applied after success. Keep TEST artifacts, publication/account
directories and all backup journals. Root-only backup locations:

| Host / purpose | Journal or backup |
|---|---|
| Controller provisioning / pre-package config | `/var/backups/distributed-ml-phase2/20261107T163149.261551Z` |
| Controller server staging | `/var/backups/distributed-ml-nfs/20261107T163200Z-595b927d8093447da0013bcfc169799a` |
| Controller RPC/drop-in/mask state | `/var/backups/distributed-ml-phase2/network-20261107T163450.302744Z` |
| pi02 client files | `/var/backups/distributed-ml-nfs-client/20261107T163611Z-aa9fb01d3f8d46df9303f4af954c50de` |
| pi02 previous guard | `/var/backups/distributed-ml-nfs-client/guard-fix-e7eaa06c6b904b96a2e9b80d61a6aca2` |
| pi03 client files | `/var/backups/distributed-ml-nfs-client/20261107T163820Z-d07750c147bf42aeaa6d368450dad0c4` |

Before rollback, confirm no future staging process is using the new mount; if a
hard reader is pending, restore its transport and reconcile its attempt before
normal unmount. Do not use lazy/force unmount or stop connectivity underneath a
pending reader. The successful fixtures already unmounted and have no pending
producers; preserve their reports rather than rerunning cleanup against reused PIDs.

1. On **pi02 and pi03 only**, after confirming no new-mount users, disable/stop
   `srv-mlnfs-inputs.automount`, stop `ml-nfs-ready.service`, then stop
   `srv-mlnfs-inputs.mount` and confirm the exact target no longer has NFS mounted.
   Restore only the four new client files from that host's `before.json`, checking
   current hashes against the recorded installed versions before replacing any
   concurrent edit. Previously absent files can be moved to a dated root-only
   rollback archive. Run daemon-reload. Keep fstab, `/mnt/nfs_share`, agent service,
   mountpoint directories, setup metadata and all student data unchanged.
2. Only after both clients are drained, unexport exactly
   `192.168.10.150:/srv/ml-shared/objects` and
   `192.168.10.101:/srv/ml-shared/objects` with `exportfs -u`; archive the owned
   new export drop-in after comparing it with evidence. Preserve unrelated
   exports. Restore journal-owned controller files and original service states
   only after inspecting the provisioning and network journals.
3. If returning to the pre-NFS controller, stop/disable the newly introduced
   `nfs-server.service` before removing its dedicated protocol/mountd drop-ins.
   Only unmask the four RPC units if they are still this run's `/dev/null`
   symlinks and the journal confirms they were previously absent/unmasked.
   Do not start them blindly: the pre-package controller had no NFS service.
   Review package enablement side effects separately. No automatic package purge,
   account removal or recursive directory deletion is part of rollback.
4. Repository-only rollback is independent of host state. From the parent Git
   root, first run
   `git apply --reverse --check /tmp/hybrid-storage-phase2-completion.patch`.
   Apply only after reviewing concurrent work. The patch restores this turn's
   edits, preserving all earlier staged/unstaged/untracked modifications. It does
   not unmount anything, revert packages or restore root configuration journals.

Residual risks: central-server power/disk failure is still a cluster storage SPOF;
hard NFS I/O can wait indefinitely; AUTH_SYS and the retained old RW share are not
tenant isolation; malicious-code sandboxing and endpoint isolation remain open.
No reboot, actual controller failure, physical link unplug, live training failure,
DDP recovery or pi01 validation was attempted. Do not interpret the isolated
three-second transport fault as a proof of those scenarios. Configured boot
independence was checked by unit semantics/native validation, not a real reboot.

## 6. Acceptance decision — STOP

| Phase 2 acceptance criterion | Result |
|---|---|
| Reviewed topology and backup-first server/client setup | PASS; earlier plans plus actual journals/native validation |
| Selected workers read authorized public TEST artifact and verify SHA-256 | PASS on pi02/pi03 |
| Writes to new export fail | PASS, EROFS on pi02/pi03 |
| Actual version/options/measurement and export boundaries documented | PASS |
| Missing mount rejected; disposable disconnect/read recovery and normal cleanup | PASS on pi02/pi03 |
| Safe scoped rollback documented; legacy application preserved | PASS |
| All three Pis | Explicit user exception; pi01 deferred, NOT TESTED |
| Runtime hybrid cache/locality/tenant isolation | Outside Phase 2; remain disabled |

**Phase 2 complete for the explicitly approved two-node scope. STOP.**
