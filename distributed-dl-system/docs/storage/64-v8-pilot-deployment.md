# Phase 14.3 v8 pilot deployment

11 November 2026, Asia/Dhaka. Scope is the controller, pi02 and pi03 on the
private Ethernet LAN. The operator deferred pi01. The controller remains a
single point of failure. The pilot uses a separate controller SQLite database
on local disk and a CPU-only offline runtime.

**Phase 14.3 PASS for the approved two-worker scope.** v8 remains active on
the controller and both Pis. The original three-worker criterion remains
unverified because pi01 was deferred.

## Release and deployment

The reviewed release is `pilot-20261111-074d980811f0`. Controller plan SHA-256
is `8955e5b39b7327925da57413e77466ed5c8cd81d5e248f977f5875415d530352`.
The controller dry run reported `DRY_RUN_NO_CHANGES`, and the operator's terminal
preparation reported `PREPARED_SERVICES_UNCHANGED`. The subsequent controller
drain, both worker preparations, both worker activations and controller
activation exited successfully. The release-specific operation receipts are
under `reports/phase14-3-integration/`.

v7 activated but its first job failed to traverse a new `0700` CAS prefix under
the service's `UMask=0077`. v7 rolled back on all three hosts. The v8 fix in
`hybrid_storage/artifacts.py::_child_directory` sets and verifies mode `0755`
on service-owned CAS directories after opening them without following links.
Object files remain `0444`. The controller publisher retains write authority
and the clients use the same read-only export. The registry regression covers
the restricted umask and a previously created `0700` prefix.

The existing portal and worker APIs, checkpoint format and schema are reused.
The explicit pilot profile enables NFS, persistent cache, staging and locality.
The repository defaults still select legacy storage and resource scheduling.
Activation uses `/opt/dl-hybrid-pilot/<release>/app` and local pilot state under
`/var/lib/dl-hybrid-pilot/<release>/`. Legacy state and the controller's drain
snapshot are retained for rollback.

## Measured acceptance results

| Check | Result | Receipt in `reports/phase14-3-integration/` |
|---|---|---|
| Cold job and owner permissions | PASS. pi02 fetched 4,024 bytes, recorded zero cache-hit bytes, finished 12 steps, committed a checkpoint and returned results. Anonymous access, missing CSRF, non-admin storage access and cross-student job reads/actions were denied. | `acceptance-permissions-1794356546122732404.json` |
| Warm cache | PASS. Both workers finished in both paired cohorts. Every job hit 4,024 verified cache bytes and staged 0 bytes from NFS. The initial cohort was already warm. | `acceptance-cold-warm-1794364158351961970.json` |
| Live cancellation | PASS. A `RUNNING` test job reached `CANCELLED` and both workers returned to idle. | `acceptance-cancellation-1794364164106181053.json` |
| Single-worker recovery | PASS. pi03 was stopped after a committed step-25 checkpoint. pi02 resumed from step 25. Maximum prediction difference was 0.0 and both final MSE values were 0.5538421869277954. The pi03 agent started again. | `acceptance-single-recovery-1794364305624607037.json` |
| Two-rank DDP and full-group recovery | PASS. Both ranks resumed from step 11 after pi02 stopped, then completed 120 steps. Lease generations advanced from 6 to 7 on pi02 and from 8 to 9 on pi03. Both recovered ranks and the baseline produced identical model SHA-256 `06d049c54f732d4b6657a0a621d61bda714cb99e9030589e278f51b0f01ccff0`. | `acceptance-ddp-recovery-1794364414306781427.json` |
| Service startup, NFS and inventory | PASS. Both controller units and both worker units were active and enabled. Both workers were idle after recovery, and both controller routes used `eth0`. Both RO NFS 4.1 mounts returned the expected 165-byte TEST SHA-256 and denied write-open with `EROFS` (30), with zero bytes written. The controller recorded zero missing bytes for the warm jobs on both workers. | `service-verification-pilot-20261111-074d980811f0-1794364460439037668.json` |

The operator-seeded portal sessions use isolated test owners. Live password
login and a physical reboot are outside the evidence above. Both worker units
were started again during the recovery tests. The controller and portal units
started during activation. Worker mount ordering was checked under systemd. The scheduler observations
confirmed inventory coverage in this run. Resource ordering decided the two
warm placements because there was no resource tie.

## Commands and rollback

Run these commands from the repository root using the release-specific review
and the approved pilot maintenance window. A healthy worker should report
`active`, and the postflight should print `PASS` with a receipt path.

```bash
systemctl is-active dl-hybrid-controller.service dl-hybrid-portal.service
python3 -I -B reports/phase14-3-integration/verify-v8-services.py --require-idle \
  reports/phase14-3-integration/acceptance-cold-warm-1794364158351961970.json
```

The live harness is `reports/phase14-3-integration/live-acceptance.py`. Its cases
are `permissions`, `cold-warm`, `cancellation`, `single-recovery` and
`ddp-recovery`. Invoke each with the controller virtual environment's Python.
The last two cases stop a worker unit while it is running the named test job and
restart it in cleanup. Run live cases serially with the pilot reserved for tests.

For rollback, close pilot admissions and verify jobs have quiesced, then run
`/usr/bin/python3 -I -B reports/phase14-3-integration/live-orchestrator.py rollback`.
The expected receipt for each host is
`LEGACY_SERVICE_STATES_RESTORED_PILOT_DATA_RETAINED`. The action disables pilot
units, removes only the matching pilot worker firewall table and restores the
recorded legacy services. The original and pilot jobs, caches, checkpoints and
database snapshots are retained. Do not restore a snapshot over a running DB.

The relevant predeployment regression run covered 161 tests across artifact
storage, the registry, provisioning, portal, workspaces, cache, SQLite locking
and DDP dispatch. The DDP socket test needed a rerun outside the sandbox, where
all seven tests in its file passed. The live tests above are separate evidence.

The final local queue, storage registry and SQLite lock check passed 53 tests
across three files, with zero failures and zero skips.

```bash
PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated \
  test_queue_controls.py test_storage_registry.py test_sqlite_lock_safety.py
git diff --check
```
