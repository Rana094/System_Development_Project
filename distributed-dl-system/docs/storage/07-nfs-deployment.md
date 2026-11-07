# Phase 2.4 — deployment preflight, application pending

**Superseded scope:** the user subsequently authorized pi02/pi03 only. Current
selection, tests and the controller privilege blocker are recorded in the
[two-node follow-up](08-nfs-two-node-deployment.md). The all-three-Pi/deferral
discussion below is the earlier attempt's historical record.

7 November 2026, Asia/Dhaka. **2.4 NOT COMPLETED: no server/client configuration
applied.** The user requested 2.4, authorizing work on this subphase. This report
records actual readiness checks, not a successful deployment or a request to
reapprove work already authorized. The earlier instruction to leave pi01 offline
still applies. The user explicitly confirmed: "Keep pi01 deferred; deployment
stays pending". Do not probe pi01 or apply deployment changes until instructed
to resume; all-three-Pi acceptance is retained.

See [server preparation](05-nfs-server-setup.md),
[client preparation](06-nfs-client-setup.md), [progress](implementation-progress.md)
and [fresh raw observations/commands](07-nfs-deployment-evidence.json).

## Actual current observations

Fresh read-only checks on 7 November around 21:12–21:13 Asia/Dhaka:

| Host | Observation | Deployment consequence |
|---|---|---|
| Controller | `192.168.10.214` remains assigned; NFS packages/server units absent; no observed TCP 2049 listener or exports config. | Server is not ready to export inputs. |
| pi01 | Not contacted, respecting the explicit deferral. Proposal LAN address remains null. | Cannot configure a verified host allowlist or complete all-three-Pi tests. Current reachability is not established. |
| pi02 | SSH success; health/resources HTTP 200 with matching identity; eth0 `192.168.10.150`; nfs-common installed. | Existing worker remains usable; new client is not deployed. |
| pi03 | SSH success; health/resources HTTP 200 with matching identity; eth0 `192.168.10.101`; nfs-common installed. | Existing worker remains usable; new client is not deployed. |

Both reachable agents were active and reported IDLE at the sample. That is not a
reservation or permission to disrupt their existing services/mounts. Their old
`/mnt/nfs_share` remains RW from pi02, with the prior wildcard/no_root_squash
configuration still observed on pi02. No old share or service was changed.

Controller dry-run returned **exit 2 / apply_ready=false**, with five blockers:
pi01 address, publisher UID, publisher GID, nfs-kernel-server, and nfs-common.
Client offline preview returned **exit 2**, still covering all three nodes.
Both proposal files keep their original null values; no address/UID was invented.

The read-only package simulation:

```bash
apt-get --simulate --no-download --no-remove install nfs-kernel-server nfs-common
```

returned **100** with `Unable to fetch some archives`. No matching NFS/rpcbind/
keyutils package archives were found in the controller's apt archive directory.
The simulation listed additional keyutils, libevent-core-2.1-7t64 and rpcbind
packages; installation/resolution from a reviewed provisioning source is still
needed. Simulation is not an install, update, download or proof of package safety.

The proposed dedicated name `ml-publisher` currently has no passwd/group entry.
A later setup can allocate a non-login system account/group with no home creation,
then record the **actual assigned** UID/GID after checking separation from training
credentials. No account or numeric identity was created/selected today. Do not
substitute rana/pi or guessed IDs simply to make preflight pass.

## Concrete TEST input prepared locally

A new **public synthetic** 32 MiB input was generated only under the temporary
review directory; it contains no student data or credentials:

```text
bytes: 33554432
SHA-256: 2c473ee33d7dba6b5f1c6ec7589344cb557e4e25fc9b0b8d7018709590400d86
key: TEST-phase-2-4-2c473ee33d7dba6b5f1c6ec7589344cb557e4e25fc9b0b8d7018709590400d86.bin
```

Evidence records the deterministic SHAKE-256 generator/seed and exact local path.
This file has **not** been published on the controller's authoritative store,
transferred to a Pi, read through NFS or used for a throughput result.

## Remaining execution sequence within 2.4

1. Resolve pi01 availability while respecting the user's answer. If available,
   inspect it read-only, verify identity/LAN address/packages/mounts and update the
   two proposals from observed values. Revalidate stable addresses for all three.
   Keeping pi01 deferred leaves 2.4 pending; two clients cannot satisfy acceptance.
2. Resolve the controller publisher identity and package prerequisites. Review
   actual package/service effects and listener/interface/protocol policy before
   activation; package scripts can start services. Cached/offline installation is
   not currently viable. Bootstrap package provisioning is separate from worker
   runtime transfers, which must never depend on Internet downloads.
3. Use protected copies of reviewed setup code, fresh root-visible plans and
   journaled backups. Apply the server configuration first. Inspect actual
   managed/kernel exports and listeners; do not activate unrelated exports or
   broaden the allowlist. Any required additional network policy needs a concrete
   reviewed plan before changing firewall/systemd configuration.
4. Publish only the complete verified synthetic TEST input through the trusted
   local publication interface. Keep application flags legacy/off and export no
   real student data while endpoint/process isolation remains incomplete.
5. Inspect/stage/activate the new client configuration on **pi01, pi02 and pi03**,
   preserving each old mount/agent/fstab. Start only the prepared automount and
   demand the mount through its ready guard; verify the exact source/options.
6. On each Pi record `findmnt -t nfs,nfs4`, negotiated version, source and options;
   stream the TEST input and verify its exact size/SHA-256. Measure actual elapsed
   read/hash time and label observed throughput with cache/CPU limitations. Do
   not drop global caches or present link speed as measured throughput.
7. Attempt an exclusive create of a new journaled TEST-only probe name. Require
   **EROFS** to prove client read-only enforcement; EACCES alone is inconclusive.
   An unexpected successful write is a failure: stop, preserve evidence and
   reconcile only that owned test path. Separately verify server-side restricted
   RO/root_squash export policy and no unexpected new exports/listeners.

No steps above were executed as live mutations. All three read/hash/write-denial
rows, negotiated NFS version, measured throughput and exposure acceptance remain
**NOT TESTED**, not zero or passing. Disconnect/unmount fault experiments are 2.5.

## Verification, file scope and rollback

This attempt adds only this report and its evidence JSON, and appends progress.
No runtime code, API, DB/schema, checkpoint/lease/DDP state, config or feature flag
changed. Existing modifications were preserved. No unit suites were rerun because
production code is unchanged; the 2.3 85-pass result remains historical, not a
fresh hardware or deployment result.

Safe local reproduction:

```bash
env PYTHONDONTWRITEBYTECODE=1 python3 tools/nfs_server_setup.py --dry-run
env PYTHONDONTWRITEBYTECODE=1 python3 tools/nfs_client_setup.py --dry-run
python3 -m json.tool docs/storage/07-nfs-deployment-evidence.json > /dev/null
git diff --check
```

The server query needs local interface visibility; expect exit 2 and the recorded
blockers while prerequisites remain unchanged. Client preview also exits 2.
Evidence embeds exact read-only SSH/HTTP commands/probe sources for pi02/pi03;
do not run its historical all-node 2.1 source against deferred pi01.

No host rollback is required. Repository rollback is only the two new report/
evidence files and this attempt's appended progress section. The exact diff is
`/tmp/hybrid-storage-2-4.patch`; check it from the parent Git root with
`git apply --reverse --check /tmp/hybrid-storage-2-4.patch` before any later
reversal. Preserve all prior work; never reset/clean or remove jobs/data.

Future live rollback remains the scoped server/client journal procedure in the
2.2/2.3 guides: drain staging, unmount only the new mounts safely, unexport only
reviewed new pairs, and restore only unchanged managed configuration files.
Preserve existing services/shares, data and backups; no broad unexport, forced/
lazy unmount or recursive deletion. Publisher/package side effects need their
own recorded provisioning rollback. The central server remains a SPOF.

**2.4 acceptance: NOT PASSED — application and all-three-Pi validation pending.
STOP.** No live version/throughput/hash/write-denial/exposure result is claimed;
no 2.5 work was performed.
