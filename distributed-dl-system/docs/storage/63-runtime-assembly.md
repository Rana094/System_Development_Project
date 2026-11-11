# Phase 14.3 continuation — runtime assembly and two-worker compatibility

11 November 2026, Asia/Dhaka. **14.3 is NOT COMPLETE.** Scope is pi02 and pi03;
pi01 is deferred by the user's explicit instruction. This report supersedes only
the runtime-assembly gap described in [62](62-two-node-deployment-preflight.md).
It does not turn historical component tests into production deployment evidence.

## Implemented behavior and exact changes

- `hybrid_storage/runtime_bootstrap.py`: explicit, inactive-on-import service
  factories. `ControllerRuntime.start()` assembles the existing LOCAL registry,
  singleton authenticated inventory, coverage, measured-transfer observations,
  admin observations, staging client and committed-checkpoint collector/restorer/
  coordinator. It requires existing migrations, matching enabled operator worker
  enrollments and the previously initialized checkpoint-store identity. It does
  not create enrollments or initialize an empty replacement checkpoint store.
- `WorkerRuntime.start()` assembles the existing verified persistent cache,
  bounded fetcher, private workspaces, inventory reporter, transfer measurements,
  metadata-only NFS health and trusted checkpoint/staging routes. It requires
  protected LOCAL roots, exact worker identity, verified RO NFS mount identity
  and an explicit launch boundary before initialization. Partial setup poisons
  restart; the caller must exit rather than serve a partially configured app.
  Busy shutdown retains lifecycle ownership/watchdog/resources.
- `LaunchBoundary` is an interface requiring `preflight`, `isolate`, `terminate`
  and `quiescent`. **No production Linux implementation is supplied by this
  change.** The test fixture is deliberately unable to launch real training.
- `controller/dispatcher.py::main(storage_runtime=None)` now accepts a started
  coordinator, checks its configuration and exact database identity, and passes
  it into cancellation, start reconciliation, monitoring and dispatch. The
  original no-argument startup retains its config gate, call order and arguments.
  Explicit runtime shutdown closes owned inventory/transport state.
- `dispatcher_cycle(..., admission=False)` supports reconciliation without new
  dispatch; this is a building block, **not** a production admission/drain lock.
- `tests/test_runtime_bootstrap.py`: 28 disposable tests, including actual factory
  assembly, signed inventory exchange, real registry coverage, invalid-MAC
  invalidation, thread ownership, source/scope restrictions, partial-start cleanup,
  busy shutdown and actual dispatcher entry-point checks.
- This report, its evidence JSON and an append-only progress entry record the
  work. Existing uncommitted changes were retained. No commit/stash/reset occurred.

## API, DB, configuration and security

The only existing Python signature extended is the dispatcher's keyword-only
`storage_runtime` argument. No route, checkpoint format or schema was changed.
Factory startup does perform the existing explicit inventory session fencing and
bounded diagnostic snapshot writes in the supplied LOCAL state; it is not a
read-only operation. All tests use disposable databases and state.

`config/storage.json` remains **legacy / cache OFF / staging OFF / resource**.
Normal agent startup does not call `WorkerRuntime`. There is no new activation
CLI, credential file, live migration, export, mount, firewall, SSH or systemd
configuration. The 14.2 installer remains a legacy-preserving source installer,
not an enabled hybrid runtime profile.
Older review/source packages predate these intentional source changes; retain
them as historical evidence and prepare a fresh review before any installation.

The factories require literal RFC1918 NFS/worker endpoints; coordinator HTTP
requests are restricted to the selected endpoints validated by polling, and never
follow redirects, including signed POSTs. Controller sessions ignore environment
proxies. This rejects configured Tailscale/public
addresses; address validation alone does not attest a route. Actual selected
worker routes were separately checked below. SQLite connections, polling and
coordinator actions remain on their owning controller thread. Construction and
imports do not connect to a database or access NFS.

## Actual checks and test evidence

| Check | pi02 | pi03 |
|---|---|---|
| LAN SSH | 192.168.10.150, PASS | 192.168.10.101, PASS |
| Controller and peer routes | eth0, PASS | eth0, PASS |
| Expected input mount | RO NFS 4.1, PASS | RO NFS 4.1, PASS |
| Existing public 165-byte TEST SHA-256 | PASS | PASS |
| Write-open without writing any bytes | EROFS (30), PASS | EROFS (30), PASS |
| Basic isolated `/usr/bin/true` namespace check | PASS | PASS |
| Installed Bubblewrap / nftables | available / v1.0.6 | available / v1.0.6 |
| Existing CPU training environment pip check | exit 0 | exit 0 |
| CPU Torch / Gloo / tensor sum smoke check | 2.15.0+cpu / true / 3.0 | 2.15.0+cpu / true / 3.0 |
| New startup component tests, Python 3.11.16 | 28 PASS | 28 PASS |
| Original agent after validation | active, PID 946, IDLE | active, PID 902, IDLE |

The original agent PIDs match the pre-test snapshot. No service was restarted.
The CPU environment emits a missing-NumPy warning; NumPy workloads have not been
certified. The old service environment's unsupported NVIDIA dependency failure
documented in 62 has not been repaired. The temporary unit tests used that
existing service interpreter; they do not certify a clean production environment.

**Controller regressions:** 250 tests PASS in 13 isolated test files, exit 0,
306.1 seconds; no failures/errors/skips. The startup file in that run contained
21 tests. The expanded final startup suite subsequently passed 28 tests locally
and 28 on each Pi. The 27-case intermediate run is also retained. A final transport
review added an exact-endpoint/no-redirect regression case and reran the suite
locally and on both Pis. Do not add repeated executions as distinct test cases.

The final Pi source archive SHA-256 is
`fcc5e6172d83a2593cc88ef0df511a2ec13370e2978adf41edbe64e0ea889e64`.
223 source files (698095 compressed bytes) were transferred over the verified LAN
SSH endpoints to new private temporary roots only:

- pi02: `/tmp/dl-phase14-3-runtime-h6jg_46q`
- pi03: `/tmp/dl-phase14-3-runtime-qjoqhskb`

Earlier 27-case source roots `2akp8pbw` and `2pjy87i9` under the same prefix are
retained too. Final guarded-transport Pi receipts/bundle are under
`reports/phase14-3-runtime/transport-final/`.

Each archive was hashed, member paths/types checked, and unpacked without links
or replacement into its new directory. Tests used additional disposable LOCAL
fixtures; no installed source, live DB, student data or service config was changed.
The Pi directories and test-result receipts are retained. This is source
compatibility validation, **not live hybrid service deployment**.

Initial new-test runs exposed an SDK import-path ordering error and an `agent`
package-versus-entry-point import collision. Both were fixed before the final
passes; neither is presented as a pre-existing regression. Raw final logs, source
plan, host receipts and earlier capability checks are under
`reports/phase14-3-runtime/`; selected hashes are in the companion evidence JSON.

## Manual verification commands and expected results

From the application root, safe local checks:

```bash
PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_runtime_bootstrap.py

PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated \
  test_runtime_bootstrap.py test_storage_config.py test_cache_inventory.py \
  test_staging_dispatch.py test_staging_recovery.py test_ddp_staging.py \
  test_hybrid_checkpoint_recovery.py test_ddp_checkpoint_publication.py \
  test_scheduler_policy.py test_deployment_executor.py test_deployment_toolkit.py \
  test_deployment_scope.py test_delivery_plan.py

git diff --check
```

Expect 28 startup tests PASS; the expanded 13-file selection contains 257 cases.
The 257-case aggregate is the expected updated selection, **not a claim that all
257 ran together**: the recorded full run was 250 plus a separate final 28-case
startup run. Whitespace checking exits 0.

Read-only host checks, keeping the verified aliases/host keys but using LAN:

```bash
ssh -o HostName=192.168.10.150 pi02 'findmnt -rn -t nfs,nfs4 --mountpoint /srv/mlnfs/inputs'
ssh -o HostName=192.168.10.101 pi03 'findmnt -rn -t nfs,nfs4 --mountpoint /srv/mlnfs/inputs'
ssh -o HostName=192.168.10.150 pi02 'systemctl show dl-node-agent.service --property=ActiveState,MainPID,User'
ssh -o HostName=192.168.10.101 pi03 'systemctl show dl-node-agent.service --property=ActiveState,MainPID,User'
```

Expect the controller input source, RO NFS options and an active original agent.
PIDs are point-in-time observations, not an invariant after an operator restart.
The retained Pi `test-result.json` files include the exact test argv and raw output.
The local validation driver is a one-shot evidence run; its result filenames use
exclusive creation and should not be reused for another evidence run.

## Remaining engineering and manual administration

1. Implement and test a production `LaunchBoundary`, then a reviewed service
   entry point that actually installs `WorkerRuntime` before serving requests.
   The current agent and training share `pi`, whose sudo policy includes
   `NOPASSWD: ALL`; current units report `NoNewPrivileges=no`,
   `PrivateNetwork=no`, `ProtectSystem=no`, `MemoryMax=infinity`. Those facts do
   **not** satisfy the requested student-code trust boundary. A different training
   identity, private mounts, denied NFS endpoints, current-group DDP networking,
   bounded cgroups and independent whole-descendant termination are required.
   A successful `bwrap true` probe does not implement or verify these controls.
2. Add production intake/preparation wiring without weakening the existing portal
   authorization. `student_portal.create_app()` still uses
   `require_controller_config`, and ordinary preparation uses legacy worker
   environment/snapshot APIs. Provide explicit immutable binding and offline
   environment metadata preparation rather than invoking dependency builds in a
   privileged service or bypassing the held-job gate.
3. Review `job_artifacts.collect`: its existing SSH/rsync paths assume the legacy
   worker account/layout. Collection from new protected service roots and access
   to existing worker read routes need an enforced trusted-controller boundary.
   Do not relax directory permissions to make the old collector work.
4. Provision a clean offline service environment and protected roots/keys under
   reviewed service identities. The exported object publisher is UID/GID 125;
   current registrar verification requires matching service ownership. Keep the
   pilot DB LOCAL and separate from the original DB; pair snapshots with artifact
   manifests and committed checkpoints. Do not chown/adopt existing objects to
   make a pilot pass. Provide durable admission/drain and exact rollback before
   switching the old services.
5. Review synchronous inventory/staging/checkpoint work against lease deadlines
   with realistic transfer sizes before enabling production. Do not solve slow
   staging by accepting expired leases or weakening stale-attempt fencing.
6. The controller's `sudo -n /usr/bin/true` currently exits 1:
   `sudo: a password is required`. Privileged provisioning will need an exact
   prepared command executed in the operator's terminal. Passwords stay there.
   No broad passwordless-sudo change is requested. There is not yet a complete
   enabled hybrid install command to run; controller authentication is **not the
   only remaining blocker**.

The next work remains inside 14.3; do not move to 14.4 or claim that a source-only
legacy install completes hybrid delivery. Existing deployment authorization is
recorded; repeated generic approval requests would not supply the missing code.
Any new host privilege/network/service plan must be concrete and reviewed before
being applied, as required by the project instructions.

## Rollback, risks and acceptance

No production rollout, migration, service stop/start, firewall/export edit or
live fault injection began, so there is no production switch to reverse. Retain
the temporary validation roots; nothing in them was linked into a live service.
Source-only rollback is in `reports/phase14-3-runtime/continuation-final.patch`. From
the parent git root, first check exact applicability:

```bash
git apply --reverse --check distributed-dl-system/reports/phase14-3-runtime/continuation-final.patch
```

Only after that check, reverse that same scoped patch if desired. Its reversal
is rehearsed in a new disposable copy, preserving the previous uncommitted
delivery/progress changes. Keep reports and recovery copies; do not reset/clean
the repository or delete runtime data.

**PASS:** runtime assembly/legacy regressions, temporary Python compatibility on
both selected Pis, physical LAN routes and actual TEST hash/RO denial.
**NOT RUN:** live hybrid inventory, cold/warm training, cancellation, worker
failure/checkpoint recovery, two-rank DDP, boot and enabled-runtime rollback.
**FAIL/BLOCKED:** production student-code isolation and complete enabled startup/
preparation/collection/provisioning. **Overall 14.3 acceptance: NOT PASSED.**
Pi01 is deferred and is not the reason for this result. No new performance claim
is made. The central controller/NFS/SQLite host remains a single point of failure.
STOP before unsafe activation; this report is a partial 14.3 delivery.
