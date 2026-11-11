# Phase 14.3 — deployment preflight BLOCKED, not deployed

**14.3 is NOT complete.** The request to proceed was checked against the actual
14.2 package and current code. 14.2 produced an offline review packager, not an
apply-ready installer: `executor_implemented=false`, concrete runtime/unit/config
deltas null, no trusted production bootstrap. Approval alone cannot supply these
missing implementation pieces. No guards were bypassed and no live mutations
were attempted. This is a failed readiness gate before deployment, not a failed
post-deployment acceptance test.

## Actual fresh checks

Read-only SSH over existing aliases with strict host keys, no host-key updates,
local commands or connection-sharing files. Remote Python ran isolated/no-bytecode
stdlib metadata probes; it did not import training/project code, access NFS input
files or open SQLite. Three SSH calls completed with exit 0. Hostnames matched
pi01/pi02/pi03. Health reported ONLINE and resources IDLE at that instant; this is
not a drain/maintenance guarantee or authenticated cache-inventory acceptance.

| Host | Agent PID / state | Input NFS metadata | TCP 192.168.10.214:2049 | Queried hybrid modules in service root |
|---|---|---|---|---|
| pi01 | 1036 / active | rejected: exact input mount absent | timeout | staging/config/checkpoint service missing |
| pi02 | 946 / active | RO NFS4.1 mount 509 admitted | reachable | staging/config/checkpoint service missing |
| pi03 | 902 / active | RO NFS4.1 mount 508 admitted | reachable | staging/config/checkpoint service missing |

Full mount tables were checked using the existing `require_input_mount` contract.
Admission proves metadata only; it is not fresh byte access/hash, server export
policy, client write rejection, credentials or outage acceptance. No NFS file was
opened (including no access that could block on a hard mount). All three installed
`agent/agent.py` hashes remain
`4d510093da9302348feb132d4bb7ab2870fda0ed1d18692b296cab517fcc429a`.
Service PIDs match the historical 14.1 observations; no service was restarted.

Fresh local command outcomes:

| Command | Result |
|---|---|
| `tools/hybrid_deploy.py verify --output <review-v2>` | exit 0; exact package verified, **apply_ready=false**, executor absent |
| `tools/hybrid_deploy.py apply --output <review-v2>` | exit **3**, **REFUSED** before package read; no deployment actions exist |
| `python -B -m hybrid_storage.config` | exit 0; current **legacy/OFF/resource** |
| Same config CLI with `--config config/storage-examples/nfs-resource.json` | exit **2**, controller rejects cache/locality/bandwidth startup |
| Same CLI with `--runtime staging` and that example | exit 0; component settings valid, **automatic_startup=false** |

The refusal is an application guard, not an automatic approval-review rejection.
The five local checks behaved as expected; they do **not** make production ready.
The 88 passing tests from 14.2 remain historical evidence; they were not rerun or
represented as live 14.3 acceptance. No new runtime code required tests here.

## Why apply cannot proceed

Current `hybrid_storage/config.py::require_controller_config` still rejects new
runtime execution. `controller/dispatcher.py::main` invokes ordinary loops without
constructing a trusted storage runtime; `agent/agent.py` startup does not install
trusted staging/checkpoint/inventory/isolation providers. Merely copying modules
or toggling JSON flags will not integrate the system. The verified package's
`configuration.new_runtime_file`, host desired unit deltas and disk budgets remain
unresolved. The old legacy installer must not be used as an alternative.

Required prerequisite work before a new apply-ready review:

1. Implement/test controller and agent production bootstrap through the existing
   authenticated APIs, manifest/registry/staging/checkpoint/lease contracts, with
   enforceable OS and network isolation plus termination/quiescence providers.
   Preserve legacy defaults and in-flight jobs; do not just remove config guards.
2. Prepare concrete bootstrap/unit/config deltas and an actual idempotent executor
   with unique snapshots, journaled file operations and tested rollback. 14.2's
   plan/data-only restart argv is insufficient to execute deployment.
3. Resolve pi01's verified NFS route/export/client setup through a reviewed plan,
   retaining all-three validation. No guessed address, wildcard export or automatic
   reduction to two workers.
4. Verify pinned compatible offline dependencies, per-host aggregate disk budgets,
   protected credentials/service roots and consistent backups/restore points.
   Dependency failures, old environment versions and privileged backup unknowns
   from 14.1 have **not** been rechecked or repaired here. Review the unrelated
   legacy wildcard RW/root-unsquashed share without disturbing its users.
5. Review the resulting exact source/config/service/dependency changes and approved
   isolated maintenance/acceptance window before any live application. Current
   IDLE observations do not replace that plan. The central server remains a SPOF.

All required live 14.3 acceptance is **NOT RUN**: authorized TEST read/hash and
RO write denial, authenticated cache sync, cold/warm execution, cancellation,
worker-failure recovery, three-rank DDP/group recovery, student/admin permissions
and service boot. No fault was injected and no unrelated jobs were disrupted.

## Evidence, commands and changes

Detailed timestamped observations, source hashes, full command argv/exit outputs,
and probe source hashes:
[59-deployment-preflight-evidence.json](59-deployment-preflight-evidence.json).
Raw evidence/probe programs remain under `reports/phase14-3-preflight/`.
Exact current plan hash:
`382724cc15cce5bebd6e8390a82deb561160a3a28264816ff4d894fd770f0094`.
Manual read-only package check from the application root:

```bash
PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/hybrid_deploy.py verify \
  --output '/home/rana/Projects/System Project/System_Development_Project/distributed-dl-system/reports/phase14-2-delivery/review-v2'
.controller-venv/bin/python -B -m hybrid_storage.config
```

Expected: exact package verifies with readiness false; active config remains
legacy. Future host state can change; fresh checks never repair missing code.

Only this report, its evidence JSON and a progress-log append change first-party
files. API/DB/schema/config/security/dependencies: **unchanged**. No sudo, remote
writes, deployment, service restart, mount/export/firewall change, data move,
package transfer or live DB open. All preceding 14.1/14.2 modifications preserved;
404 original nonignored files snapshotted, 403 unchanged and one intended append.
No stash/reset/commit. No runtime rollback is required because nothing was applied.

Source-only rollback patch: `reports/phase14-3-preflight/14.3.patch`. From git root:

```bash
git apply --reverse --check distributed-dl-system/reports/phase14-3-preflight/14.3.patch
```

After reviewing exact applicability, omitting `--check` reverses only these
14.3 documentation edits. Actual reverse application was verified only in a
disposable copy; preceding 14.1/14.2 work and all runtime data/reports remain.
No rollback of the existing legacy services was attempted.

**Preflight: known blockers confirmed. Deployment readiness FAIL/BLOCKED.
14.3 deployment/acceptance NOT RUN; Phase14 incomplete. STOP before14.4.**
