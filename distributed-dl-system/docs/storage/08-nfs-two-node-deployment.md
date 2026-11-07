# Phase 2.4 — explicit pi02/pi03 deployment scope

Historical preflight: the administrator-authentication blocker below was later
resolved. The [Phase 2 completion report](10-phase2-completion.md) records actual
deployment and validation; retain this report as the earlier observation.

7 November 2026, Asia/Dhaka. The user superseded the earlier pending deployment
decision: **"no do it without pi1 / pi2 and p3 are active"**. Proceed with pi02 and
pi03 only. Do not contact, provision, export to or mount on pi01. This is an
explicit two-node scope exception; any eventual result applies to those two nodes.

**Application status: NOT DEPLOYED — controller administrator authentication
required.** No additional approval of the two-node scope is needed. The concrete
privilege check `sudo -n -l` returned 1, `sudo: a password is required`. This is
the operating system's authentication requirement, not an automatic tool-review
rejection. No alternative privilege route or sudo policy change was attempted.

See [actual evidence](08-nfs-two-node-evidence.json), [progress](implementation-progress.md),
[earlier preflight](07-nfs-deployment.md), [server procedure](05-nfs-server-setup.md)
and [client procedure](06-nfs-client-setup.md).

## Implemented selection and compatibility

The existing version-one server/client proposals accept optional
`deployment_nodes`, a nonempty unique list of known Pi IDs. The two checked-in
proposals now specify `["pi02", "pi03"]`; pi01's address stays null. Omission
preserves the original three-node validation, so an unspecified/missing pi01
still blocks legacy proposal staging. An exclusion must be explicit.

Server rendering, prerequisite checks and active-export-table validation use only
the selected clients. Even a known address for an excluded node is not exported
or accepted in active metadata. Client previews contain only the selected nodes;
attempting to inspect/render/stage an excluded node fails before host queries.
Defaults retain the old three-node preview mode label. Unknown/empty/duplicate/
coerced selections fail closed. Selected hosts still need verified LAN addresses;
there is no wildcard/subnet fallback or removal of identity/path safeguards.

The worker inventory, application APIs, SQLite, checkpoint/lease/DDP states and
storage flags remain unchanged. Hybrid execution stays legacy/default-off. These
changes are confined to infrastructure scope; no training is launched.

## Actual current checks

- Fresh pi02 and pi03 SSH probes succeeded, and both health/resources calls
  returned HTTP 200 with matching node identities. Probe commands/sources and
  current network/mount/package/service metadata are embedded in evidence.
- Current controller server plan exits 2 with **four** blockers: publisher UID,
  publisher GID, nfs-kernel-server, nfs-common. pi01 is no longer a blocker in this
  explicit profile. The only proposed clients are `192.168.10.150` and
  `192.168.10.101`, using the controller's observed `192.168.10.214` LAN address.
- Offline client preview exits 2 because fresh per-worker inspection/application
  is required. Its client set is exactly pi02/pi03. It is not a mounted result.
- Noninteractive controller sudo failed before any privileged action. No package
  install/update/download, account creation, systemd/exports/fstab/firewall edit,
  mount/unmount, TEST publication/transfer, write probe or live job action ran.
- **90 tests passed, 0 failed, 0 skipped:** 22 server + 27 client + 17 config +
  24 backend. Five new methods check explicit partial selection and preserve the
  default missing-pi01 behavior; fake two-node staging/idempotence passed.

Executed local commands:

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py test_nfs_server_setup.py test_nfs_client_setup.py test_storage_config.py test_storage_backends.py
env PYTHONDONTWRITEBYTECODE=1 python3 tools/nfs_server_setup.py --dry-run
env PYTHONDONTWRITEBYTECODE=1 python3 tools/nfs_client_setup.py --dry-run
sudo -n -l
```

The sudo command only requests policy information. Its failure means this session
cannot perform the controller's root-only provisioning, exports or service steps.
Running a sudo authentication command in another terminal does not necessarily
authenticate this session; sudo timestamps may be terminal-scoped. Do not weaken
sudo policy or share a password to work around this.

## Administrator prerequisite handoff

The controller needs an operator to perform the administrator steps in their own
terminal. The two-node deployment request already authorizes the setup; no second
scope confirmation is requested. Actual credentials are not available to this
agent. First provision the controller packages and dedicated publisher identity:

```bash
sudo apt-get --no-remove install nfs-kernel-server nfs-common
getent passwd ml-publisher >/dev/null || sudo adduser --system --group --no-create-home --home /nonexistent --shell /usr/sbin/nologin ml-publisher
getent passwd ml-publisher
getent group ml-publisher
```

These commands have **not been executed** here. apt presents its real package
transaction for operator review and may require a bootstrap network/package
source; the earlier no-download simulation failed because archives were absent.
This administrative provisioning is distinct from worker runtime input/dependency
transfers, which must remain Internet-independent. Package maintainer scripts may
start services: record their effects/listeners and do not claim exposure acceptance
from installation alone. Existing publisher entries require identity/policy review;
do not replace them. The account must remain non-root and separate from training.

Return the actual package/account outcomes; record the assigned UID/GID rather
than guessing them. Server configuration then needs root execution using a
protected copy of the reviewed setup code, a fresh plan and its reviewed SHA.
Do not run `--apply` against the current null publisher IDs or start new client
mounts before the server is validated. Root authentication is also required for
the remaining server config/activation; this handoff does not claim these initial
commands finish deployment or grant this session persistent root access.

After administrator access/provisioning is resolved, continue **2.4**, server first,
then only pi02/pi03: preserve journaled backups and legacy services, verify native
exports/listener exposure, publish only the public synthetic TEST input, demand
the RO mounts through the guard, verify actual findmnt version/options and exact
TEST bytes/hash, and require EROFS on an exclusive TEST write probe. Record measured
read/hash throughput with cache/CPU limitations. The prepared 32 MiB TEST input
from the earlier preflight remains local/unpublished. No real student inputs are
authorized for the incomplete execution-isolation path.

## Manual verification, rollback and acceptance

Repeat the safe local commands above: expect 90 passing methods and the recorded
blocked plans/sudo status until the host changes. Client JSON must list pi02/pi03
only; server preview must contain exactly those two literal addresses. Native
systemd syntax checks for both newly rendered unit sets passed locally; Pi-native
unit/mount execution remains untested. Validate evidence and whitespace with:

```bash
python3 -m json.tool docs/storage/08-nfs-two-node-evidence.json > /dev/null
git diff --check
```

The scoped patch is `/tmp/hybrid-storage-2-4-two-node.patch`. From the parent Git
root, `git apply --reverse --check /tmp/hybrid-storage-2-4-two-node.patch` checks
repository rollback without applying it. Reverse only this revision's file edits
after checking for later changes. Removing `deployment_nodes` restores the original
all-three requirement; it does not unexport/unmount anything on a deployed host.
No host rollback is needed for this session because no live mutation occurred.
Future live rollback follows the existing per-host journals and scoped drain/
unmount/export restore plan; package/account effects need their own recorded
reconciliation. Never reset/clean or remove jobs, checkpoints or unrelated data.

Residual risks remain publisher/package provisioning, root access, DHCP stability,
native export/listener policy, the existing pi02 wildcard RW share, direct NFS/
process isolation, hard-I/O supervision and controller SPOF. Live pi02/pi03 mount,
version/throughput, TEST SHA-256 and RO write-denial are **NOT TESTED**. pi01 is
intentionally excluded, not failed or passed. **2.4 NOT COMPLETE; STOP before 2.5.**
