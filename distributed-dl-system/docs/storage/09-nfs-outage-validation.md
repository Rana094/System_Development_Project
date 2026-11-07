# Phase 2.5 — outage validation preflight and isolated test procedure

Historical preflight: the prerequisites below were subsequently completed.
See [Phase 2 completion](10-phase2-completion.md) for actual isolated outage tests,
timings, cleanup and limitations on pi02/pi03.

7 November 2026, Asia/Dhaka. Scope remains **pi02/pi03**, following the user's
explicit two-node exception. pi01 was not contacted. **2.5 live acceptance is
NOT PASSED: 2.4's new server/export/client mounts still do not exist.** This report
records safe checks and a future test procedure, not a completed outage exercise.

See [2.4 deployment/admin handoff](08-nfs-two-node-deployment.md),
[fresh evidence](09-nfs-outage-evidence.json), [client setup](06-nfs-client-setup.md)
and [progress](implementation-progress.md).

## Actual prerequisites checked

Read-only probes around 22:17 Asia/Dhaka on 7 November observed:

| Check | Controller | pi02 | pi03 |
|---|---|---|---|
| New NFS server | Packages not installed; server units not found; no observed TCP 2049 listener | Not the proposed new server | Not the proposed new server |
| New client mount/automount/ready units | Not a worker requirement | All three not found/inactive | All three not found/inactive |
| New `/srv/mlnfs/inputs` NFS admission | Not a worker requirement | Guard ready=false | Guard ready=false |
| Worker availability sample | Controller services left running | SSH + health/resources succeeded; agent IDLE | SSH + health/resources succeeded; agent IDLE |

The existing mount on both Pis is still
`192.168.10.150:/srv/nfs/cifar10_data` at `/mnt/nfs_share`, RW/NFS 4.2. It is
unrelated to the new controller export. Do not repurpose, unmount or disconnect it.
An IDLE sample is not an isolated-job reservation or evidence no other user needs
the share. The controller's `sudo -n -l` still returns 1 / password required.
No privileged deployment or fault injection was attempted.

The current standalone mount guard was sent inside the metadata-only probe and
executed in memory under each Pi's base **Python 3.11.2**, without installing a
file. It read `/proc/self/mountinfo` and returned ready=false for the missing exact
mount. This is a real two-node negative admission check; it is not a read of NFS
data, a simulated disconnect, or execution of the whole setup under Python 3.11.

## Actual safe tests

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py test_nfs_client_setup.py test_storage_backends.py
```

**51 passed, 0 failed, 0 skipped**:
27 client tests and 24 adapter tests. The tests use isolated local fixtures/mock
syscalls, not a live NFS mount. Coverage includes missing/local/autofs-only mount
rejection, wrong source/options/overmount checks, boot-independent generated unit
structure, backup failure/interruption handling, NFS write rejection, bounded
publication and propagation of simulated EIO/ETIMEDOUT/EACCES/ESTALE. Those errors
are not silently reported as cache misses. No scheduler/Flask/lease/DB is invoked.

Exact read-only SSH/HTTP commands, probe source and returned metadata are in
evidence. No mount, unmount, disconnect, reboot, service stop/restart, firewall/
route edit, package/account operation, root staging, live job submission/cancel or
global cache drop occurred. Existing agents and shares were left intact.

## Configured timeouts versus measured behavior

These values are read from the current generated client units, not hardware
measurements:

| Setting | Scope | Actual outage measurement |
|---|---|---|
| Mount `TimeoutSec=15s` | systemd mount-command timeout | NOT MEASURED |
| Ready service `TimeoutStartSec=5s` | Metadata guard process after mount dependency | NOT MEASURED |
| Automount `TimeoutIdleSec=0` | Idle unmount disabled in the proposed unit | NOT MEASURED |
| `timeo=50`, `retrans=2`, `retry=0` | Proposed client RPC/helper retry policy | NOT MEASURED |
| `hard` | No promised total read deadline | NOT MEASURED |

`timeo` uses deciseconds, so 50 represents five seconds for the initial response
timeout. With hard mounts, retries may continue; two retransmissions do not imply
an overall ten-second deadline. Changing to soft mounts merely to make a test
finish can compromise integrity. A Python timeout cannot be treated as proof that
kernel I/O is canceled. [Debian NFS client manual](https://manpages.debian.org/bookworm/nfs-common/nfs.5.en.html)

systemd's mount timeout supervises the mount command, not every later file read.
Capture actual mount start/failure times separately from read/cancellation/recovery
times. A blocked kernel task may remain unresolved after a userspace timeout;
keep it tracked and fence late publication. Do not claim reservations or attempts
can be released safely based only on the timer.
[Debian systemd.mount manual](https://manpages.debian.org/bookworm/systemd/systemd.mount.5.en.html)

## Future disposable test procedure — not executed

Prerequisites: complete 2.4 on pi02/pi03, including genuine read/hash/EROFS and
exposure checks; obtain the necessary host administrator execution; record active
jobs and mount/client identities again. Use only public TEST input and fresh
owned temporary workspaces. Do not run these cases on the legacy share or through
student submission APIs while hybrid runtime integration remains disabled.

For each node, first prove the disposable process's mount namespace differs from
the host/agent namespace, with private propagation and no other job processes or
open input descriptors. Any inherited systemd autofs triggers require explicit
isolation: do not access them or invoke host systemctl through a shared bus from
inside the test namespace. Mount ownership/source/options must be revalidated
before an unmount. If a fully isolated fixture cannot be established, stop with
that case BLOCKED; a namespace command alone is not sufficient proof of isolation.
The fixture/privileged fault runner is not implemented or asserted tested here.

| Case | Isolated action | Required observation | Status now |
|---|---|---|---|
| Baseline | Read only the authorized TEST artifact in the disposable fixture | Exact SHA-256/size, valid source/options, EROFS write denial | NOT RUN |
| Mount removed | Close disposable input handles; remove only that fixture's private mount | Fresh guard refuses; no training/input read from ordinary underlying directory; host mount/agent unchanged | NOT RUN |
| Server unavailable at mount | Use a dedicated test transport isolated from host traffic | Record actual timeout/failure; legacy agent stays responsive and no job launches partially staged | NOT RUN |
| Connection loss during read | Drop only the disposable transport/namespace path | Record pending/error state and process/task status; no partial verified input or successful publication | NOT RUN |
| Connectivity restored | Restore only the fixture transport and revalidate identity/bytes | Record actual recovery delay/hash; stale canceled producer cannot publish | NOT RUN |
| Already-local input | A disposable process reads its private copied TEST input | Local bytes remain readable without NFS; this alone does not prove scheduler/lease/cache integration | NOT RUN |

Disconnect testing requires a dedicated test network/transport whose isolation
from shared NFS connections is established first. Never take down eth0/Wi-Fi/
Tailscale, add host-wide packet drops/routes, stop a shared NFS server, or restart
the old pi02 service for this experiment. A mount namespace isolates mount-table
changes, not necessarily network connections. A dedicated network namespace/test
server needs a reviewed concrete fixture before use; no such live fixture exists
in this attempt. Do not invent successful network isolation or timeout numbers.

While each case runs, observe the host agent and the other selected worker from
outside the fixture. Require unchanged existing jobs/leases/share availability.
Stop at the first unexpected effect and preserve evidence. The future runtime's
cache registry, staging cancellation, lease fencing and launch barrier are not
implemented by these infrastructure tests; remain explicit enablement gates.

## Safe rollback and manual verification

No host rollback is required now. For a future approved disposable fault:

1. Restore the isolated transport first, keeping admission/publication fenced.
   Account for blocked processes/open descriptors; do not assume they died.
2. Close/drain only disposable readers, then perform a normal unmount of only the
   revalidated fixture mount in its proven private namespace. A busy/error result
   is a diagnostic condition, not permission for forced/lazy/global unmount.
3. Recheck host/agent mounts, service availability and actual active jobs outside
   the fixture. Preserve logs, TEST workspace and pending-task identity for review.
4. If configuration was deliberately changed in a later reviewed experiment,
   restore only its journaled files after matching installed hashes and checking
   concurrent edits. The scoped 2.2/2.3 journal procedures still apply. No broad
   export reload/unexport, unrelated service restart, config overwrite or `rm -rf`.

This attempt adds only this report and its evidence JSON and appends progress.
No production/test source, config/API/DB/job/checkpoint/lease state, scheduler rule
or default-off flag changed. All prior work was preserved. Review rollback with
`git apply --reverse --check /tmp/hybrid-storage-2-5.patch` from the parent Git
root; reverse only these three documentation changes after checking later edits.

Safe local verification:

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py test_nfs_client_setup.py test_storage_backends.py
python3 -m json.tool docs/storage/09-nfs-outage-evidence.json > /dev/null
git diff --check
```

Expect 51 passes and valid evidence/whitespace. Read-only evidence reproduction
must keep the explicit pi02/pi03 filter and avoid NFS data-path traversal.
Do not use generic mount/unmount/disconnect commands as a substitute for the
missing reviewed disposable fixture. pi01 remains excluded.

**2.5 acceptance: NOT PASSED — live outage/unmount/recovery tests NOT RUN.**
Local/metadata checks passed, but 2.4 deployment and root access are still blocking
prerequisites. Controller SPOF, hard-I/O supervision, authenticated staging,
process/network isolation and later cache/lease integration remain risks. Phase 2
is not complete. **STOP; return to the outstanding 2.4 administrator/deployment
steps before attempting physical 2.5 validation.**
