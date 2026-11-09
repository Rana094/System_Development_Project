# Phase 11 closeout and live-validation gate

Phase 11.1–11.4 implementation and selected local acceptance are complete. This
closeout consolidates the evidence and refreshes live readiness in response to
the request to fully complete Phase 11. It adds documentation only, preserves all
earlier uncommitted changes, and does not claim production or physical acceptance.
See the dated results and exact probe source in
[46-phase11-closeout-evidence.json](46-phase11-closeout-evidence.json).

## Completed Phase 11 deliverables

| Subphase | Actual implementation / acceptance | Result |
|---|---|---|
| 11.1 | Real verified-copy payload/time observations; bounded rolling rate, variability, age and count; warm/failed/empty copies excluded | Local PASS; [42-transfer-observations.md](42-transfer-observations.md) |
| 11.2 | Authenticated optional inventory v2 summaries; opt-in missing-copy cost with measured rate/residual, age/count, capacity and concurrency uncertainty; original resource fallback | Local PASS; [43-bandwidth-policy.md](43-bandwidth-policy.md) |
| 11.3 | Explicit dry-run-first bounded LRU under existing quota/reserve; persistent pins/activity exclude deletion; crash-safe missing observations and nondeleting signed reconciliation | Local PASS; [44-cache-eviction.md](44-cache-eviction.md) |
| 11.4 | Combined changing-rate, stale/no-sample, concurrency, capacity, pin/crash/reconciliation, actual queue fairness and rollback checks | Local PASS; [45-efficiency-acceptance.md](45-efficiency-acceptance.md) |

Latest selected regression: **428 tests PASS across 20 files, zero failures,
errors or skips**, including ten new combined acceptance tests. This is the saved
11.4 run, not a newly repeated test run. The log hash was checked against its
recorded evidence; runtime/config bytes remain unchanged from that passing test
snapshot. There is no new source change requiring another broad test run.
The prior corrected test-fixture failures and historical loaded-run HTTP409
remain disclosed in the linked reports; this closeout does not erase them.

The acceptance criteria for Phase 11's algorithms and local fault checks are met:
prediction inputs/ranges/limitations are transparent; pinned and authoritative
data are protected under the documented root/isolation/cooperating-client
requirements; resource and locality remain independently usable. The active
installation still uses legacy/OFF/resource, as required until reviewed enablement.

## Fresh read-only readiness

Addresses came from `config/nodes.json`; no IP was invented. Bounded GET health/
resources and strict-existing-key, nonpersistent SSH ran against all three
configured identities. The remote program reads service properties, mount tables
and hashes known code files; it does not read credentials, launch jobs, touch NFS
input paths, activate automounts, create files or change services. A sandboxed
attempt could not open sockets (EPERM); the reviewed read-only retry with network
access succeeded for pi02/pi03 and timed out for pi01. That sandbox failure is not
evidence that the Pis were offline.

| Host / configured address | Fresh observation | Gate |
|---|---|---|
| pi01 / `100.101.90.46` | HTTP health/resources timeouts; SSH connection timeout | Actual identity, power/network state, LAN address, deployed code, storage and isolation remain unknown. Request sent for availability or verified updated address. |
| pi02 / `100.91.135.18` | Identity matches; ONLINE, IDLE, one slot available; active agent service at `/home/pi/distributed-dl-system`, User=pi, PrivateMounts=no | Known active service root lacks `agent/staging.py`, `agent/checkpoint_service.py`, `hybrid_storage/staging_protocol.py`, `worker_cache.py`, `transfer_metrics.py`, `bandwidth.py`, `cache_eviction.py`; deployed agent hash differs from checkout. |
| pi03 / `100.64.202.71` | Identity matches; ONLINE, IDLE, one slot available; same service-root/account/namespace properties | Same queried hybrid modules absent and agent hash differs. |

Both reachable hosts report the existing RW NFS4.2/AUTH_SYS share
`192.168.10.150:/srv/nfs/cifar10_data` at `/mnt/nfs_share`. The mount-table query
does not establish the proposed RO input mount's availability; no path access
was used to activate its automount. Existing mounts/data were retained.
The queried root is the active unit's WorkingDirectory; absence there does not
assert that no experimental files exist elsewhere. An IDLE report is an
observation, not a reservation or authorization to restart a service.

## Remaining work for physical and production acceptance

These are external deployment gates, not unimplemented Phase 11 algorithms. The
earlier two-Pi Phase 2 exception does not satisfy all-three-Pi acceptance. The
existing [Phase 9 plan](37-ddp-acceptance.md#5-reviewable-physical-test-prerequisites-and-safe-plan--not-applied)
still applies. Next work must proceed in this order:

1. Obtain pi01 availability/verified address and refresh identity, LAN, service,
   disk and NFS/isolation readiness on **pi01, pi02 and pi03**. No substitute IP,
   two-Pi acceptance or assumed power state.
2. Prepare and review an offline-capable exact source/hash deployment bundle for
   all three nodes and the controller. The current `tools/cluster_setup.py::bundle`
   omits `hybrid_storage`; it is not a hybrid deployment mechanism. Review protected
   local cache/workspace/checkpoint-spool roots, per-worker credentials and actual
   job OS/network containment plus termination/quiescence callbacks. A same-UID
   launcher or copying the four flags is insufficient. This closeout supplies no
   ready-to-apply bundle or isolation implementation and requests no deployment
   approval before those concrete artifacts exist.
3. Review restricted RO input topology with verified pi01 LAN address, exact
   server source, root_squash, mount identity/timeout admission and backups. Keep
   controller SQLite, jobs and checkpoints local; preserve existing mounts/data.
   Obtain explicit approval for exact deployment/service/system configuration
   actions after those artifacts are prepared, as the user's standing rules require.
4. In an agreed isolated window, with all three ready and test jobs positively
   identified, validate native two-/three-rank runs and full-group checkpoint
   recovery first. Then exercise Phase 11 on disposable authorized artifacts:
   cold/warm actual NFS copies, real per-worker summaries, rate age/reconnect,
   resource/locality/bandwidth logs, concurrent staging and policy rollback.
   Do not impose artificial link degradation on shared training traffic.
5. Preview idle-cache LRU with actual limits; review each issued plan before any
   explicit apply. Retain pinned/active/unknown/authoritative files. Use disposable
   process-crash/space-error fixtures instead of filling a production disk or
   power-cycling a worker. Record actual bytes/time and signed registry convergence.
6. Roll back reviewed test source/config/bootstrap only after test descendants are
   proven quiescent or fenced. Preserve DB/history/jobs/checkpoints and durable pins.
   No wildcard kill, recursive deletion, live NFS disruption or existing-job restart.

The controller's LOCAL SQLite and authoritative storage remain a single point of
failure. Successful local tests do not prove isolation, hardware durability,
shared-link prediction accuracy or live NFS/full-group recovery.

## Checks and rollback for this closeout

The evidence includes the exact GET/SSH program, actual replies, saved-test log
hash verification, runtime/config fingerprints and the unchanged active config.
Only these files change in this closeout:

* `docs/storage/46-phase11-closeout.md` — this consolidation and gated live plan.
* `docs/storage/46-phase11-closeout-evidence.json` — actual observations/checks.
* `docs/storage/implementation-progress.md` — appended closeout entry.

No new API, DB, schema, security, flag or runtime change; no test rerun, live
deployment, package installation, service restart, mount/export/firewall/systemd
change, job mutation, cache cleanup or Git commit. No Internet-dependent transfer.

From the application root, safe checks:

```bash
.controller-venv/bin/python -B -m hybrid_storage.config
git diff --check
# Optional: ten isolated local acceptance tests; no physical hosts contacted.
PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_storage_efficiency_acceptance.py
```

Expect legacy/OFF/resource, no whitespace errors, and (if rerun) ten PASS. The
optional test command is documented here, not claimed as newly executed.

Documentation-only scoped rollback, from the parent Git root, after preserving
any later edits:

```bash
git apply --reverse --check /tmp/hybrid-11-closeout/closeout.patch
# Apply only after reviewing the patch; earlier Phase 10/11 work is preserved.
git apply --reverse /tmp/hybrid-11-closeout/closeout.patch
```

Reverse check passed; nothing was reverted. Temporary transcripts/patches are
ephemeral; the repository evidence embeds the observations and exact probe source.

**Phase 11.1–11.4 implementation/local acceptance: COMPLETE, PASS. Physical
all-three-Pi and production acceptance: INCOMPLETE; pi01 readiness and reviewed
deployment/isolation prerequisites remain. STOP before any live mutation.**
