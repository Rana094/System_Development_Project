# Phase14.3 — pi02/pi03 scope and actual deployment blockers

10 November2026, Asia/Dhaka. **14.3 NOT COMPLETE; no live deployment started.**
The user explicitly deferred pi01, which is at another site, and requested
pi02/pi03-only completion. That overrides the earlier all-three requirement for
this run. Keep private-only runtime data transfers. Do not interpret two-node
results as three-node acceptance or change an existing job's DDP world size.

The [14.2 tooling](61-installation-tooling.md) supplies a tested **legacy-preserving
source installer**, not a production hybrid bootstrap. Its scripts are implemented;
the remaining runtime integration below has not been implemented by these scripts.
Installing them and restarting legacy services cannot establish14.3's requested
hybrid cache/job/checkpoint/recovery behavior. Your deployment authorization is
recorded; missing implementation and dependency compatibility are the blockers.

## Implemented this continuation

`hybrid_storage/deployment_toolkit.py` now accepts explicit
`prepare --workers pi02 pi03`. It requests exactly controller/pi02/pi03 inventory,
builds only those host directories and operation lists, records pi01 deferred,
and defers only pi01-specific gateB3. Bootstrap, containment, dependency and other
gates remain. Default selection is still all three. Missing selected hosts,
extra inventories, unknown/duplicate workers and single-node scope are rejected.
Changing a completed review's scope requires a new output; retries preserve it.

`hybrid_storage/deployment_executor.py` binds receipts to exact selected workers.
Legacy `all_three_workers` receipts work only for three-node plans; two-node
plans require explicit `selected_workers`. Ambiguous/cross-scope approvals fail
before source writes. Existing fencing, quiescence, source/config/unit drift,
snapshot, lock and rollback checks remain. No active node config, storage flag,
auth key, DB schema or scheduler behavior changed. Pi01's running service and
existing jobs are not contacted or altered by this preflight.

Added `tests/test_deployment_scope.py` (6 tests). Appended progress and this report/
evidence. Prior changes, old reviewed packages and reports are retained; earlier
source packages are historical after this intentional source change.

## Fresh checks actually performed

| Check | pi02 | pi03 |
|---|---|---|
| SSH identity/source/unit inventory | PASS | PASS |
| Local `/health` | ONLINE, expected worker | ONLINE, expected worker |
| Local `/resources` | IDLE,1 available slot | IDLE,1 available slot |
| Controller NFS TCP2049 | reachable | reachable |
| Full-table input mount admission | PASS, expected RO input mount | PASS, expected RO input mount |
| Agent environment `pip check` | **FAIL exit1** | **FAIL exit1** |
| Basic unprivileged user/mount/PID/net namespace smoke check | exit0 | exit0 |

Both dependency failures report exactly:
`nvidia-cusparselt-cu13 0.8.1 is not supported on this platform`.
No package was removed, installed or repaired. The namespace smoke test runs only
a trusted `/usr/bin/true` in temporary namespaces; it proves capability, **not**
credential isolation, peer-only DDP networking, cgroup limits or descendant cleanup.

Controller SQLite was opened `mode=ro`, query_only with normal WAL handling and
closed explicitly; only aggregate counts were read. At that snapshot: active
jobs0, reservations0, unexpired nonrevoked leases0, nonterminal hybrid staging0.
This is not a durable admission fence or proof that no future job can start.
No job IDs, owners, input names, secrets or DB rows are published in this report.

No fresh privileged TEST hash/write-denial proof, runtime inventory sync, cold/warm
training, cancellation, worker fault/recovery, two-rank DDP, permission attack or
service boot acceptance ran. Pi01 was not probed. No sudo, remote file write,
package transfer, service stop/start, NFS/export/firewall change or live migration.
No rollout began, so no live rollout rollback was required.

## Engineering needed before a real hybrid rollout

1. **Wire the production runtime.** `controller/dispatcher.py::main` currently
   calls its four reconcile/dispatch functions without `storage_runtime`. Construct
   the reviewed dedicated LOCAL registry/checkpoint objects, authenticated control
   client, `CheckpointDispatch` and inventory heartbeat, and pass the runtime into
   reconciliation/dispatch. Review portal control routes and held NFS submissions
   too. `hybrid_storage/config.py::require_controller_config` still rejects cache/
   locality execution; replace that gate only alongside tested real integration.
2. **Provide worker isolation and lifecycle enforcement.** Normal `agent/agent.py`
   startup does not install `StagingService`, `InventoryReporter` or
   `CheckpointService`. `agent/staging.py::launch_inputs` requires real isolate,
   terminate and quiescent providers. Implement a reviewed launcher with separate
   training identity, private mounts, bounded cgroups, whole-descendant termination
   and network rules allowing only the current DDP group's required communication.
   Keep NFS, service keys, cache, SQLite and other jobs inaccessible. The existing
   root-readable NFS parent cannot be fixed by giving all training code access.
   Review checkpoint/auth hook registration and startup/restart ordering together.
3. **Prepare clean offline environments.** Use a separate candidate service env
   with pinned Flask/requests/psutil/waitress dependencies and a compatible CPU
   PyTorch training env on both Pis. Inventory reusable local artifacts first;
   use offline provisioning and verify complete dependency checks/imports before
   switching any unit. Do not delete CUDA-related packages from the live env just
   to make pip check green. Preserve the original env for rollback.
4. **Finish maintenance and recovery boundaries.** Provide an admission/drain
   mechanism that cannot race queued jobs, and reviewed protected credential,
   publisher/cache/workspace/checkpoint roots with per-host disk/resource budgets.
   Pair the controller DB recovery point with its manifests/checkpoints/objects
   and isolated restore proof before migrating/activating production.

The coding above is software engineering that can be developed/tested locally.
Root-owned service accounts/directories, launcher privileges, systemd/network
containment and trusted controller publication require a reviewed host provisioning
step. Where sudo needs an interactive password, the operator will need to execute
that exact prepared command locally; passwords stay in the terminal. There is no
single terminal command that substitutes for the missing startup/security code.

**Recommendation:** first run a dedicated two-node pilot using separate protected
state, LOCAL DB, service ports and disposable jobs while existing student services
continue in legacy mode. Test cold/warm staging, authenticated inventory, cancellation,
committed recovery and two-rank full-group recovery there; include an adversarial
check that training cannot read cache/keys/NFS or contact NFS over the network.
Then review the production switch/rollback. This recommendation is a proposed
engineering approach, not an implemented pilot or permission to alter ports/rules.
The controller/NFS/local DB remains a SPOF. Existing 100Mbps links constrain shared
I/O; use measured staging records before performance claims or hardware changes.

## Review, tests, manual verification and rollback

Current private two-node review:
`reports/phase14-3-two-node/final-review/`. Its exact snapshot/kit/host plan hashes,
raw read-only evidence and controller dry-run result are in the companion
`62-two-node-deployment-preflight-evidence.json`. The earlier `review/` is retained
as development evidence, not silently overwritten. New review verifies locally;
Pi payloads have not been staged and `apply_ready=false`, `hybrid_activation=false`.

Actual prepare/verify commands, from application root:

```bash
PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/hybrid_install.py prepare \
  --workers pi02 pi03 \
  --inventory '/home/rana/Projects/System Project/System_Development_Project/distributed-dl-system/reports/phase14-3-two-node/final-readonly/inventory.json' \
  --output '/home/rana/Projects/System Project/System_Development_Project/distributed-dl-system/reports/phase14-3-two-node/final-review'

PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/hybrid_install.py verify \
  --output '/home/rana/Projects/System Project/System_Development_Project/distributed-dl-system/reports/phase14-3-two-node/final-review'
```

Expected: verified kit, selected pi02/pi03, deferred pi01, executable installer
present, no hybrid activation or live readiness attestation. Preparation after
inventory expiry requires a new read-only inventory/review. Verification is safe
to repeat. This preflight performs no worker runtime data transfers; the
private-only requirement remains in effect.

Actual safe local regression command:

```bash
PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated \
  test_deployment_executor.py test_deployment_toolkit.py test_deployment_scope.py test_delivery_plan.py
```

**49 PASS (20+7+6+16),0 fail/error/skip,4 files.** Covers explicit pair/default-all-three
selection, stale/missing/extra hosts, unchanged review retries, cross-scope receipts,
real disposable install/rollback, fencing and source-package safety. Previous139
tests are a historical14.2 result; they are not all rerun or counted as live14.3
acceptance. Full real cluster acceptance remains NOT_RUN.

Development-only rollback: `reports/phase14-3-two-node/scope.patch`. From git root:

```bash
git apply --reverse --check distributed-dl-system/reports/phase14-3-two-node/scope.patch
```

After checking exact applicability, reverse only this continuation if desired.
Actual reverse application is tested only in a NEW disposable copy. It restores
the two prior deployment modules and progress file while preserving prior14.1/
14.2 work. Retain all reports and private recovery copies. No reset/stash/commit
or user-data cleanup. No runtime rollback needed for this preflight.

**Two-node scope/preflight tooling PASS. 14.3 hybrid deployment and acceptance
FAIL/BLOCKED by missing runtime integration and dependency compatibility.
Pi01 is deferred by user instruction, not a blocker for this two-node scope.
STOP before deploying an installer profile that cannot satisfy hybrid acceptance.**
