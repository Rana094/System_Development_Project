# Controller NFS setup preparation — Phase 2.2

Date: 7 November 2026, Asia/Dhaka. **Prepared and fake-tested; no live setup applied.**
Only 2.2 is covered. pi01 remains deferred/offline at the user's request; it was
not probed or powered on again. See [topology](04-nfs-topology.md),
[architecture](01-hybrid-architecture.md), [progress](implementation-progress.md)
and [actual dry-run/test evidence](05-nfs-server-dry-run.json).

## Files and execution boundary

- [hybrid_storage/nfs_server.py](../../hybrid_storage/nfs_server.py): validated
  plan, conservative export syntax/policy checks, read-only prerequisite checks,
  backup-before-change and idempotent configuration staging for future approval.
- [tools/nfs_server_setup.py](../../tools/nfs_server_setup.py): operator entrypoint.
- [config/nfs-server.json](../../config/nfs-server.json): explicit reviewed proposal,
  separate from the application's unchanged default-off `storage.json`.
- [tests/test_nfs_server_setup.py](../../tests/test_nfs_server_setup.py): fake-host
  tests; no sudo, actual exports, service operations or Pi connections.

The command defaults to a **read-only dry-run**, also selectable with `--dry-run`.
It reads NIC addresses, mount metadata, package state and relevant local paths/
export configuration. It does not create locks, directories or backups in dry-run
mode. No controller DB modules are imported and no SQLite file is opened.

Future `--apply --reviewed-plan-sha256 <current-digest>` requires administrator
credentials, the exact current plan digest and zero blockers. It stages files and
directories only. It never installs packages, creates accounts, mounts anything,
invokes exportfs, or starts/enables/reloads/stops a service. A digest binds reviewed
intent/current observations; it is **not user approval**. The agent must still
obtain the explicit approval required for 2.4 before any live staging/activation.

Package provisioning and service/export activation remain separately reviewed
actions. This avoids a package install or service start unexpectedly applying
unreviewed exports. The dry-run prints prospective commands for review only;
there is no subprocess path that executes them from this script.

## Proposed layout and export policy

| Logical path | Proposed access and purpose |
|---|---|
| `/srv/ml-shared` | Admin-owned 0755 parent; never exported. |
| `/srv/ml-shared/objects` | Dedicated trusted publisher UID/GID, 0755; immutable input objects and non-sensitive tree manifests only. The sole export. |
| `/srv/ml-shared/.publish` | Same publisher identity, 0700; private unpublished temporary bytes on the same local filesystem for the current subdirectory layout. Never exported. |
| `/srv/ml-shared/.nfs-setup.json` | Admin-owned 0600 setup ownership marker; never exported. |
| `/etc/exports.d/distributed-ml.exports` | Admin-owned 0644, one managed input-only row. Existing `/etc/exports` is not rewritten. |
| `/var/backups/distributed-ml-nfs/<unique-run>/before.json` | Private backup journal, before staging any config/input directories. |

Do not export `/`, `/home`, the repository, `storage/jobs`, checkpoints, logs,
owner/job authorization manifests, credentials or `jobs.db`. Owner/job sidecars
remain local and go only through the future authenticated stager channel. Input
objects must still be published through the verified 1.2 publication interface;
this setup does not recursively chmod/chown existing files or make arbitrary
existing data immutable. Read-only AUTH_SYS remains a host-level trust mechanism,
not a per-student boundary. Job isolation and NFS endpoint restrictions are still
unimplemented enablement requirements; application flags remain off.

The proposed file has exactly pi01/pi02/pi03 client slots. Only numeric IPv4 host
addresses are accepted, with distinct clients and no wildcard, subnet, hostname,
netgroup or shell text. The two known LAN addresses come from 2.1; pi01 is `null`.
No Tailscale or guessed LAN address is silently substituted. All three must be
specified before future staging. These are DHCP addresses: revalidate/reserve them
before approval. The selected server address must currently belong to this host.

The publisher UID/GID are also `null`: no dedicated account has been selected or
created. Future values must be explicit existing non-root IDs for a reviewed
trusted service, separated from student training credentials. Do not fill them
from the current login user merely to bypass this blocker.

The generated **preview only** currently contains:

```text
# Managed by distributed-dl-system nfs-server v1
/srv/ml-shared/objects 192.168.10.150(ro,sync,root_squash,subtree_check,secure,sec=sys) 192.168.10.101(ro,sync,root_squash,subtree_check,secure,sec=sys)
```

It is incomplete for deployment because pi01 is absent; the script refuses to
apply it. The source config is review material, not live `/etc` configuration.

### Why this layout uses subtree_check

The observed root is a shared ext4 filesystem containing private controller state.
For that subdirectory layout, retain `subtree_check`. The Ubuntu manual describes
its boundary check and recommends considering it for mostly read-only exported
subdirectories. Do not disable it solely for convenience. [Ubuntu exports(5)](https://manpages.ubuntu.com/manpages/noble/man5/exports.5.html)

`layout=dedicated-filesystem` permits the requested `no_subtree_check` only when
kernel mount metadata identifies the exact objects path as a whole filesystem
root on a different device from its parent. A bind-subdirectory, same-device
mount, missing mount or empty ordinary directory fails the check. That mode adds
`mountpoint` so a missing filesystem is not exported as an underlying directory.
No filesystem/bind mount is created here. The default sibling `.publish` would
then be on a different filesystem, incompatible with the 1.2 publisher's atomic
link. A whole-filesystem export also leaves no same-filesystem pathname outside
its tree for private temporary files. A separately reviewed publication strategy
(for example unnamed temporary inodes) would be needed and is not implemented.
Keep the proposed subdirectory layout for the current publisher; dedicated mode
is an infrastructure option, not a ready application publication topology.
[Ubuntu exports(5)](https://manpages.ubuntu.com/manpages/noble/man5/exports.5.html)

## Syntax/policy check and preservation rules

The script checks the current `/etc/exports` and nonhidden `*.exports` drop-ins.
It accepts comments/empty unmanaged files, but refuses any unrelated active line,
unsupported syntax or unsafe ownership. It never "fixes" or erases an unrelated
export. The managed filename must carry the exact ownership marker, export only
the fixed objects path, and use this tool's strict literal-client/option grammar.
Malformed spacing, unknown options, RW, no_root_squash and broad paths fail closed.
Readable `etab` entries must also be restricted to the planned clients/options.

This is a **conservative syntax/policy subset**, not a complete replacement for
nfs-utils parsing. Complex otherwise-valid existing exports require manual review.
On this controller `/etc/exports` and its drop-in directory are absent, so there
is no existing syntax to parse. The preview passes the conservative checker and
unit tests; native exportfs acceptance remains untested because the tools are
absent. The documented exportfs interface has no general harmless syntax-only
dry-run flag. `exportfs -ra` synchronizes real export state and must not be called
as a pretend dry-run. [Ubuntu exportfs(8)](https://manpages.ubuntu.com/manpages/noble/man8/exportfs.8.html)

The future staging path also:

1. Refuses symlinked, untrusted-owner, group/world-writable administrative paths
   and NFS/autofs/unknown-filesystem traversal. The CLI has no fake-root override.
2. Refuses to adopt a nonempty unmarked shared root, unexpected directory modes/
   ownership or changed publisher IDs. It never walks/chowns/chmods input children.
3. Recomputes the plan, checks its reviewed digest, and obtains an admin-only
   no-follow lock at `/etc/.distributed-ml-nfs.lock`; then checks again. The lock
   is created only by future approved staging, not dry-run.
4. Writes/fsyncs a private backup journal containing every inspected export file's
   original text, existence, SHA-256, UID/GID/mode and intended new drop-in digest.
   Existing main exports and other drop-ins are backed up but never modified.
5. Creates only missing managed directories. A root marker precedes child creation
   so interrupted recognized setup can resume; incorrect attributes after a crash
   still require manual reconciliation rather than a recursive repair.
6. Writes a hidden temporary config file, checks the target has not changed, then
   publishes atomically; new targets use exclusive linking. It fsyncs directories.
   Matching repeated configuration returns `no_changes` without new backups or
   service/export operations. Changed client lists get a new backup first.

Freeze manual configuration edits during future staging; its lock coordinates
cooperating setup invocations, not an adversarial administrator. Backups preserve
text and POSIX ownership/mode, not an asserted complete ACL/xattr/SELinux snapshot.
Special host policy requires review. No root/privilege escalation mechanism is
embedded: real apply needs the administrator, and an approved trusted script copy.

## Actual dry-run and tests

Commands executed from the application root:

```bash
env PYTHONDONTWRITEBYTECODE=1 python3 tools/nfs_server_setup.py --dry-run
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py test_nfs_server_setup.py test_storage_config.py test_storage_backends.py
git diff --check
```

The sandbox initially denied the read-only `ip` query. The dry-run was retried
outside that restriction, without `--apply` or sudo. Its expected **exit 2** reports
`apply_ready=false` and five blockers: pi01 address, publisher UID/GID, and missing
`nfs-kernel-server`/`nfs-common`. That is a successful refusal, not a deployed server.
Both known client preview entries passed the conservative syntax checker. Snapshot
checks confirmed `/etc/exports`, its directory, `/srv/ml-shared`, backup/lock paths
and `/etc/fstab` unchanged. No live backup or config was created.

Final tests: **60 passed, 0 failed, 0 skipped** (19 setup, 17 configuration,
24 backend). Fake-host tests exercise backup/idempotence, interruption/resume,
competing edits, backup-disk failure, unknown identities/clients, wrong plan digest,
unsafe exports/permissions/symlinks and dedicated-filesystem checks. No command in
those fixtures reaches a Pi or a real service. Initial development failures from
fixture umask and an extracted-helper variable were fixed; they did not predate
this work. Full model/DDP/hardware tests were not run for this isolated utility.

## Review and rollback plan — not executed

Before later application, obtain the separate approval required by 2.4 after
resolving all blockers, choosing stable addresses/account policy, preparing packages
through an approved source, and reviewing the fresh plan/diff. The printed
`apt-get --no-download install ...` is an offline-cache provisioning option only;
package availability/dependencies are unverified and it is never run by this tool.
Installing packages may itself affect services and needs its own reviewed step.
Native export activation checks and the old pi02 wildcard share remain unresolved.

Configuration staging then requires the exact new digest and explicit `--apply`;
service start/export activation are separate reviewed commands. No firewall,
fstab, client mount or application flag change is bundled. Later package/service
rollback depends on the separately approved provisioning plan, not this script.

If future staging must be undone, stop and review current state first:

- Before activation, compare the managed drop-in with the backup journal's intended
  installed hash. Restore only that drop-in's recorded prior bytes/attributes, or
  remove only that verified config file if it was originally absent. Use atomic
  replacement and fsync; refuse if another administrator changed it meanwhile.
- Do not restore older unrelated `/etc/exports`/drop-ins over later edits: they
  were never changed by this tool. Keep the private backup journals.
- If activated later, first safely drain/unmount the approved client jobs and
  unexport only the exact reviewed client/object pairs. Never use broad unexport-
  all or restart an unrelated shared service as an automatic rollback. Reassess
  other exports before any reread. That is later maintenance, not a 2.2 test.
- Preserve all object/staging directories, ownership marker, input files and jobs.
  Leave the private lock safely in place; do not unlink an active lock or remove
  directories recursively. No `rm -rf`, package purge or automatic data cleanup.

Rollback of **this repository-only subphase** is simply its scoped diff: remove
the six additions listed in progress and undo only its five documentation/index
edits. No host rollback is needed today. The controller remains a single point of
failure; AUTH_SYS isolation, supervised NFS outages, client boot behavior, live
write denial and all-three-Pi validation remain pending.

**2.2 preparation acceptance: PASS — reviewed implementation and fake tests plus
an actual blocked read-only controller plan. Live applicability: NOT READY.
STOP.** No live approval is requested while prerequisites are unresolved, and no
2.3 client setup or 2.4 deployment was performed.
