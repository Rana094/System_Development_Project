# Phase 14.2 continuation — executable delivery tooling and dry run

Date: 10 November 2026 (Asia/Dhaka).

**14.2 source-delivery tooling is implemented and locally tested. No live
deployment or hybrid activation occurred. 14.3 remains blocked by the 14.1
readiness/runtime gates.** The earlier [58 preparation](58-deployment-playbook.md)
is retained as history; its unconditional `hybrid_deploy.py apply` refusal remains
compatible. The new explicit `hybrid_install.py` workflow supplies the missing
installer, migration executor and guarded rollback. It is not an enable-hybrid
switch, a sandbox, or evidence that production hybrid execution is integrated.

## Delivered scope and exact files

| File | Behavior |
|---|---|
| `hybrid_storage/deployment_executor.py` | Read-only inventory and dry run; explicit journaled install/rollback with exact approval, host/file/service checks and exclusive lock |
| `hybrid_storage/deployment_migrations.py` | Separate approved idle-controller DB backup, restored-copy rehearsal twice, additive migration and verification; no live DB restore |
| `hybrid_storage/deployment_toolkit.py` | All-three-Pi inventory validation, protected per-host payloads/launchers, exact operation argv and drift-checked idempotent kit preparation |
| `tools/hybrid_install.py` | Discoverable offline toolkit CLI |
| `tests/test_deployment_executor.py` | 20 disposable-host safety and rollback tests |
| `tests/test_deployment_migrations.py` | 7 actual legacy-fixture migration/backup/interruption tests |
| `tests/test_deployment_toolkit.py` | 7 offline all-three kit, portability, drift and idempotency tests |
| This report and `61-installation-tooling-evidence.json` | Results, hashes, limits, exact review and rollback references |
| `58-deployment-playbook.md`, `implementation-progress.md`, `hybrid_storage/README.md`, `tools/README.md` | Historical clarification, progress and operator discovery |

Existing changes were preserved. No website/API, scheduler, job-state enum,
storage flag default, auth protocol, checkpoint format or worker-launch bootstrap
changed. Unit and active-config deltas are **explicitly empty**, rather than
unresolved fields: preserve the observed service definitions, interpreter paths,
credentials, node config and `legacy/OFF/resource`. An absent worker storage config
continues to mean legacy defaults; the installer does not create one. Runtime
requirements are source only; no installer runs pip or downloads packages.

This is a compatible code/schema delivery profile. Enabling NFS/cache/locality
in production still requires the missing trusted bootstrap, OS/network isolation,
termination/quiescence providers and reviewed configuration in a separate change.
Do not interpret the new executor as resolving those architecture gaps.

## Transaction and security contract

Preparation requires inventories for the controller **and pi01, pi02, pi03**,
at most one hour old. Requested roots/UID/GID/hostnames derive from the audit;
current configured worker identities remain explicit. pi03 UID/GID1002 is
preserved. Inspection reads bounded single-link, no-follow source/unit files,
machine identity, disk metadata and `systemctl show`; it opens no SQLite
connection, changes no remote files and uses no sudo.

Every host plan binds the exact preimages/metadata, desired source hashes,
application root, host/machine identity, snapshot namespace, active storage-config
hash, unit hashes and service launch identity. systemd's changing `ExecStart`
timestamps/PIDs are excluded from launch identity; executable/argv are checked.
Unknown drop-ins fail closed instead of silently omitting them. Unit/enablement
drift requires a new review. Same-UID administrators and trusted parent directories
are part of the trust boundary; this is not protection against a compromised
administrator/kernel. Existing legacy training containment remains a blocker.

An install, migration or rollback requires a **distinct** caller-owned mode0600
operator receipt binding the exact host, action and SHA-256 of `host-plan.json`.
It expires within ten minutes, includes all three workers, private-only runtime
networking and maintenance/quiescence attestation, and references separately
reviewed evidence SHA-256 for every readiness gate. The shipped example has
`operator_approval=false`, zero timestamps and UNRESOLVED evidence: it is not usable
authorization. A receipt is a trusted operator attestation, not a signed approval
or an automated proof that arbitrary referenced evidence resolves a gate. No
approval is generated/inferred by the installer or this subphase.

Before changing source, and again for each file, require the reviewed services
already inactive with PID0 and no descendant processes in their cgroups. The
installer **never stops services or kills a job**. Later approved maintenance must
close admission/drain jobs, ranks, leases and reservations and establish descendant
quiescence first. A momentary IDLE response does not suffice. Controller commands
run as the audited user, never sudo; worker installs require later approved root.
Fresh installation inventories expire after24h. Older rollback snapshots remain
usable with fresh approval and current identity/unit/config/quiescence checks.

Host operation lock and unique snapshots are under:
`<application-root>/.hybrid-deploy/snapshots/<snapshot-id>-<host>/`.
The caller owns mode0700 deployment directories; snapshot files are mode0600.
Save every prior source file, mode/UID/GID, absent-file identities, original
service states, unit bytes and existing storage config before `SNAPSHOT_COMPLETE`.
No source publication occurs before this marker. An incomplete snapshot is retained
and refused, never repaired or overwritten. Source publication uses a unique
same-directory file, fsync, atomic rename and verification, with fsynced intent/
completion journal. New source directories are readable by the service; existing
file ownership/modes are preserved. Exact completed retries do not refresh backups
or restart services. Interrupted installs resume only from old/reviewed bytes;
external edits stop the transaction. Full disks fail without a completion marker.
Unique interrupted source temporary files may remain for review; no recursive
cleanup is provided.

The payload allowlist covers only reviewed role source, static UI, runtime
requirements and inactive examples. It excludes jobs, objects, caches, DB files,
checkpoints, credentials, venvs and active config. Never transfer private DB backup
files or any unrelated report directory to workers. No exports, mounts, routing,
SSH, firewall, fstab or systemd definitions are modified by this installer.

## SQLite step and rollback

`migrate` is controller-only and separately approved, after source installation
while all reviewed services remain stopped. Refuse active jobs, reservations,
unexpired nonrevoked leases or nonterminal staging. Use LOCAL filesystem checks,
bounded SQLite online backup plus separate verified restore, and a NEW rehearsal
copy. Rehearse these six explicit modules twice:

```text
registry_schema -> cache_registry_schema -> staging_schema -> cleanup_schema
-> checkpoint_schema -> restore_schema
```

Verify original table contents/schema and user_version, idempotency, quick_check
and foreign keys before migrating the idle controller using a FULL/FK dedicated
connection. Verify installed and imported migration code against the reviewed
hashes. No generic legacy initializer, backfill, state rewrite or data move runs.
Live SQLite always uses normal locking/WAL handling. `immutable=1` is used **only**
to read the hashed, verified, closed private recovery copy with empty/no WAL;
it never applies to the live DB. This avoids changing recovery SHM sidecars.

A mid-sequence failure may leave compatible additive tables. Retain original
recovery copies and retry the idempotent sequence with new approval after review;
never down-migrate or automatically restore an older DB. This is DB-only recovery,
not coordinated checkpoint/filesystem or off-host disaster recovery. Those remain
14.1 gates; the central controller/local DB/NFS remains a SPOF.

`rollback` preflights the full installed/backup set before changing any source.
It restores saved bytes/metadata only when current bytes match the reviewed
release, or skips an exact original file. It moves release-created files into
the private snapshot's `quarantine/` outside import paths. External edits, corrupted
backups and quarantine collisions stop rollback. Partial rollback retries safely;
completed rollback cannot be installed again under the same transaction. It
does not restore the DB, delete user files, restore unrelated infrastructure,
restart services or change enablement. Keep additive schema only with the tested
legacy compatibility; a necessary live DB restore needs a separate reviewed
matching recovery point and explicit approval.

## Actual all-host review and dry-run results

Local directory (ignored/private):
`reports/phase14-2-completion/review/`.
Snapshot: `snap-20261110T170756Z-34c52925118fb87e`.
`KIT.json` SHA-256:
`cef5f6f443f0a20c3232cc219645fa86395e346fe40c089be99b1b84b31ce293`.
Each host contains `host-plan.json`, `run.py`, `approval.example.json`, and a
private `payload/`; top-level `OPERATIONS.json` contains exact host-local argv.
The complete review has396 regular files. Repeated preparation preserved every
byte, mode and mtime. Verification refuses source drift, extra/missing files,
symlinks, wrong identities and partial output. Select a new output after changes.

Fresh non-sudo read-only inspection succeeded on all four hosts:

| Host | Source files | New worker source files | UID:GID | Observed free bytes | Source-only reserve/budget bytes |
|---|---:|---:|---|---:|---:|
| controller |124|0|1000:1000|43,456,036,864|541,559,144|
| pi01 |86|63|1000:1000|5,537,841,152|538,804,060|
| pi02 |86|63|1000:1000|11,457,773,568|538,804,060|
| pi03 |86|63|1002:1002|9,283,325,952|538,804,060|

Each Pi also has2 existing source files changed in the candidate. Controller
preimages match the development checkout containing this new code. Budgets include
source/rollback temporaries and512MiB reserve; these are point-in-time estimates,
not reserved disk or training/cache quotas. Worker payloads were verified **locally**;
no Pi payload staging or host-side installer dry run was executed. Pi02's unit
reported active but MainPID0; no agent health or boot acceptance is inferred.

The packaged controller `dry-run` succeeded with required free2,695,448,936B,
observed43,462,180,864B, including its conservative DB/rehearsal budget. It performs
OS descriptor/stat checks on the DB, **not a SQLite connection or migration**.
The unapproved example install returned exit2 before creating a deployment
namespace. No root command, remote write, installation, service operation,
production backup/migration or training test ran.

The initial sandbox could not access SSH/user systemd. The same reviewed
read-only inspection succeeded with network/session access; no automatic review
rejection or privileged setup was involved. Raw failures and successful records
are retained. Small Tailscale control-plane inspection is not permission to use
Internet-dependent student data transfers: the user retained private-only runtime
data/checkpoint/DDP traffic. Pi01's verified private path is still missing.

## Commands and later all-three playbook

Actual local commands, from the application root:

```bash
PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/hybrid_install.py prepare \
  --inventory '/home/rana/Projects/System Project/System_Development_Project/distributed-dl-system/reports/phase14-2-completion/final-readonly/inventory.json' \
  --output '/home/rana/Projects/System Project/System_Development_Project/distributed-dl-system/reports/phase14-2-completion/review'

PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/hybrid_install.py verify \
  --output '/home/rana/Projects/System Project/System_Development_Project/distributed-dl-system/reports/phase14-2-completion/review'
```

Expected: `executor_implemented=true`, all three selected,
`VERIFIED_INSTALL_TOOLKIT_NOT_APPLIED`, `apply_ready=false`, no hybrid activation.
The verify command is safe to repeat. Future preparation needs fresh inventories;
the retained package is an exact dated review, not perpetual readiness.

For fresh inspection, `tools/hybrid_install.py inspect-program --host pi01`
prints a self-contained stdlib program; likewise controller/pi02/pi03. Review
its bytes before sending to `python3 -I -B -` using the existing strict SSH alias.
The actual collector/argv are retained in `reports/phase14-2-completion/collect_inventory.py`
and `final-readonly/*-inspection-result.json`; no remote file installation needed.

**Later14.3 only, after readiness and explicit approval of exact hashes:**

1. Resolve pi01 private connectivity, compatible offline dependencies, coordinated
   recovery/containment/bootstrap gates. Fresh all-three identity, NFS and drain
   checks are required. A receipt with arbitrary hashes does not resolve them.
2. Review each exact plan hash, all-three operation order, budgets and unchanged
   unit/config profile. Close admission and drain through a reviewed maintenance
   mechanism, including queued-job eligibility and descendants. Stop portal then
   dispatcher, then drained pi01/pi02/pi03 agents with the exact argv in OPERATIONS.
   The installer cannot establish an admission fence or safely kill training.
3. Transfer **only the host's kit directory** over the existing authenticated
   administrative SSH channel to a NEW private location; never rsync --delete,
   merge/overwrite old reviews or copy jobs/DB/credentials. No Internet package
   retrieval. Keep exact reviewed script/payload hashes and modes. Worker protected
   destinations are `/var/lib/distributed-ml-delivery/<snapshot>/pi01|pi02|pi03`,
   root-owned0700; controller is `<app>/.hybrid-deploy/kits/<snapshot>/controller`,
   audited-user0700. Privileged directory/copy operations themselves require later
   approval. Independently verify the copied hashes using trusted tools **before**
   executing any script as root; the kit cannot attest its own untampered launcher.
4. Run each host's `dry_run_argv`. This verifies staged payload and current preimages
   and disk again. The actual interpreter paths are the controller's
   `.controller-venv/bin/python` and each Pi's `/home/pi/distributed-dl-system/.venv/bin/python`.
   No new unit, environment or unresolved interpreter placeholder is installed.
5. Supply separate protected fresh install receipts only after user approval;
   execute controller and all three `install_argv`. All services stay stopped.
   If any step fails, stop the sequence; retain evidence and use reviewed per-host
   rollback, not partial-cluster startup. Source installation is resumable only
   under the exact plan/preimages and fresh approval.
6. Run controller `migrate_argv` with distinct approval: verified DB recovery,
   rehearsal twice, then idle additive migration. No worker SQLite migration.
7. Use the exact start argv only for services originally active, agents pi01,
   pi02, pi03 then portal then dispatcher. Never enable new services, daemon-reload,
   reboot or reopen admission automatically. The dispatcher starts only with the
   reviewed queue/admission fence and controlled acceptance jobs; this tooling
   does not create a production maintenance provider. Test14.3 acceptance and
   service boot only within the separately approved plan/window.
8. On failed acceptance, keep admission closed, fence new attempts and prove
   descendants quiescent, stop affected services under approval, then invoke
   each `rollback_argv` with fresh rollback receipts. Reverse source installation
   order; preserve DB additions, snapshots, caches, objects, jobs and checkpoints.
   Restore originally active services only after legacy health checks and approval.

For example, each protected host kit's CLI is:

```text
<reviewed interpreter> -I -B run.py dry-run --plan host-plan.json --payload payload
<reviewed interpreter> -I -B run.py install --plan host-plan.json --payload payload --approval install-approval.json
<controller interpreter> -I -B run.py migrate --plan host-plan.json --approval migrate-approval.json
<reviewed interpreter> -I -B run.py rollback --plan host-plan.json --approval rollback-approval.json
```

Actual absolute executable/destination/receipt paths and sudo prefixes are in
OPERATIONS.json. These examples describe interfaces; none authorizes execution.

## Tests, source rollback and acceptance

Actual final command:

```bash
PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated \
  test_storage_config.py test_storage_selection.py test_deployment_review.py \
  test_nfs_client_setup.py test_delivery_plan.py test_state_backup.py test_readiness_probe.py \
  test_deployment_executor.py test_deployment_migrations.py test_deployment_toolkit.py
```

**139 PASS,0 failures/errors/skips,10 files**: prior105 plus new20+7+7. All
SQLite/job modifications use disposable actual legacy fixtures, including
successful install/retry/rollback/retry, interruption after a real additive table
commit, retained recovery hashes/modes/mtimes, partial snapshot refusal, locks,
full disk, changed backup/external edits, path/link attacks, stale/wrong approval,
resource/flag-off regressions, source/identity drift and portable launchers.

Initial failures are retained: fixture assertion omitted an existing job; a
recovery SHM mtime changed on readonly WAL open; later the strengthened symlink
guard changed the expected exception type. Corrected final runs pass. No
hardware performance, runtime dependency, real sudo/ownership installation,
cross-Pi job/DDP/recovery, NFS fault, service restart or boot test is claimed.

Development-only rollback patch: `reports/phase14-2-completion/tooling.patch`.
From the parent git root:

```bash
git apply --reverse --check distributed-dl-system/reports/phase14-2-completion/tooling.patch
```

Review exact applicability before omitting --check. Reverse application was tested
only in a NEW disposable copy; it restores the four prior docs and removes only
this continuation's new source/tests/docs there. Keep all reports and private
recovery data. Earlier14.1/14.2/14.3 uncommitted work remains preserved; no reset,
stash or commit. No live service rollback is required for this development turn.

**14.2 tooling/dry-run acceptance: PASS for the stated legacy-preserving delivery
profile. Hybrid-enabled deployment readiness: FAIL/BLOCKED; not claimed complete.
14.3: NOT RUN. STOP before any privileged copy, installation or restart.**
