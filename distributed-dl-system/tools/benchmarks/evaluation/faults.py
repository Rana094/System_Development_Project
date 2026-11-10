"""13.3 allowlisted local failure campaign, with explicit physical-test deferrals."""
import csv
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

from . import runner

# These are reviewed repository tests, not student-supplied commands/test names.
CASES = {
    'agent_process_down': dict(worker='agent', injection='SIGKILL owned loopback health-only agent; restart new private agent'),
    'interrupted_staging': dict(worker='interrupt', injection='Copy helper reads a partial payload then injected ESTALE; real bounded retry'),
    'cache_corruption': dict(worker='corrupt', injection='Corrupt only a NEW unpinned local cache replica; verify/quarantine/refetch'),
    'worker_lease_outage': dict(test='test_staging_recovery.RecoveryTests.test_offline_worker_keeps_reservation_until_safe_expiry_and_cleans_on_return', injection='Modeled agent HTTP timeout and clock expiry; actual SQLite/fencing/cleanup'),
    'partial_checkpoint': dict(test='test_hybrid_checkpoint_faults.CheckpointFaultTests.test_interrupted_upload_and_real_connection_reopen_keep_only_previous_commit', injection='Injected checkpoint body interruption; actual publication and DB reopen'),
    'publisher_process_crash': dict(test='test_hybrid_checkpoint_faults.CheckpointFaultTests.test_sigkill_controller_publisher_process_after_rename_preserves_head_on_reopen', injection='SIGKILL owned publisher after rename; actual DB head/recovery checks'),
    'nfs_outage_model': dict(test='test_hybrid_checkpoint_faults.CheckpointFaultTests.test_nfs_outage_warm_recovery_uses_zero_missing_bytes_and_cold_replacement_fails_closed', injection='Injected ENETUNREACH at copy spawn; no actual NFS server stopped'),
    'ddp_rank_failure': dict(test='test_hybrid_checkpoint_ddp_resume.RealHybridDDPTests.test_sigkill_rank_then_committed_three_rank_resume_matches_baseline_with_stated_fp32_tolerance', injection='SIGKILL one owned CPU/Gloo rank; committed full3-rank resume vs real baseline'),
    'ddp_checkpoint_failure': dict(test='test_ddp_acceptance.NativeMatrixTests.test_three_rank_crash_during_checkpoint_ack_keeps_old_cut_then_recovers_entire_group', injection='SIGKILL owned rank during checkpoint acknowledgment; complete-cut/group recovery'),
    'stale_rank_fencing': dict(test='test_hybrid_checkpoint_faults.CheckpointFaultTests.test_ddp_stale_rank_checkpoint_result_rejected_for_entire_group', injection='Replay old fixture attempt checkpoint/result against actual fenced publication'),
}
DEFERRED = [
    dict(case='physical_network_disconnect', status='NOT_RUN', reason='Requires explicit disconnect approval, confirmed idle window, disposable job and independently tested return path.'),
    dict(case='physical_nfs_server_outage', status='NOT_RUN', reason='Shared controller/NFS is a single point of failure; no proven isolation from other students. Never stop the live server for this runner.'),
    dict(case='physical_pi_agent_or_rank_failure', status='NOT_RUN', reason='Pi training idle window remains unconfirmed; pi01 deferred. Prior staging/publication approval did not authorize live process kills.'),
]
METRICS = ('detection_ns', 'staging_ns', 'restart_ns', 'recovery_ns')
FIELDS = ('case', 'scope', 'status', 'exit_code', 'harness_elapsed_ns', *METRICS)


@dataclass(frozen=True)
class Settings:
    experiment_id: str
    cases: tuple[str, ...] = tuple(CASES)
    timeout_seconds: int = 120

    def validate(self):
        runner.Settings(self.experiment_id).validate()
        if not isinstance(self.cases, tuple) or not self.cases or any(type(c) is not str for c in self.cases) or len(set(self.cases)) != len(self.cases) or any(c not in CASES for c in self.cases):
            raise ValueError('Choose unique allowlisted local cases')
        if type(self.timeout_seconds) is not int or not 5 <= self.timeout_seconds <= 300:
            raise ValueError('Use a5..300 second per-case timeout')


def source_hashes():
    paths = set(runner.source_files())
    for directory in ('agent', 'controller', 'hybrid_storage', 'tests', 'training', 'tools/benchmarks/evaluation'):
        paths.update(str(p.relative_to(runner.ROOT)) for p in (runner.ROOT / directory).rglob('*.py') if '__pycache__' not in p.parts)
    paths.update(str(p.relative_to(runner.ROOT)) for p in (runner.ROOT / 'examples').rglob('*') if p.is_file() and p.suffix in ('.py', '.json', '.csv', '.txt'))
    paths.update(('tools/benchmarks/storage_fault_evaluation.py', 'config/storage.json', 'config/nodes.json', 'config/execution_leases.json'))
    # This file is static public timing policy, not per-attempt lease authority.
    # Refuse future extra fields rather than accidentally snapshot credentials.
    policy = json.loads(runner.read_source('config/execution_leases.json'))
    if set(policy) != {'enabled', 'duration_seconds', 'renew_every_seconds'}:
        raise ValueError('Lease timing policy has unreviewed fields')
    # No auth files, live DBs, jobs, cache, outputs, checkpoint or remote paths.
    return {name: hashlib.sha256(runner.read_source(name)).hexdigest() for name in sorted(paths)}


def plan(settings):
    settings.validate()
    return dict(schema_version=1, experiment_id=settings.experiment_id, scope='local_disposable_failure_rehearsal',
        status='PLAN_ONLY', cases={c: CASES[c] for c in settings.cases}, deferred=DEFERRED,
        timeout_seconds=settings.timeout_seconds, sources=source_hashes(), local_host=runner.local_observations(),
        units='integer monotonic nanoseconds and bytes; UTC timestamps use epoch nanoseconds',
        metric_definitions=dict(detection_ns='Injected failure to observed rejection/failed health probe; cache corruption uses explicit forced verification.',
            staging_ns='Successful production ensure() wall time; setup/cold fill excluded.',
            restart_ns='New private agent spawn to healthy response; not training restart.',
            recovery_ns='Not inferred from whole test duration; null unless separately measured.'),
        limitations=['No SSH, live service operation, interface unplug, mount/export mutation or NFS write.',
            'Kernel NFS mount facts and outer training launch/isolation remain mocked in component tests.',
            'Native DDP children are real local CPU/Gloo processes, not physical Pi ranks.',
            'Imported regression cases provide assertions/logs; their phase latencies are uninstrumented and stay null.',
            'Per-case elapsed time includes setup/assertions/cleanup and is not recovery latency.',
            'The allowlisted worker forces child Popen calls to inherit its owned case process group for timeout cleanup; no production process containment claim.'])


def child_environment():
    env = {k: os.environ[k] for k in ('PATH', 'HOME', 'LANG', 'LC_ALL', 'TZ', 'LD_LIBRARY_PATH') if k in os.environ}
    env.update(PYTHONDONTWRITEBYTECODE='1', OMP_NUM_THREADS='1', MKL_NUM_THREADS='1', CUDA_VISIBLE_DEVICES='', GLOO_SOCKET_IFNAME='lo')
    return env


def execute(command, folder, env, timeout):
    process = None
    with runner.new_file(folder / 'case.log') as log:
        try:
            process = subprocess.Popen(command, cwd=folder, env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            try:
                return process.wait(timeout=timeout), False
            except subprocess.TimeoutExpired:
                # Unlike runner.stop(), also signal this owned group if its leader
                # exits before the timeout while a child remains in that group.
                return None, True
        finally:
            if process is not None:
                import signal
                try: os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError: pass
                process.wait(timeout=10)


def run(settings, output_root):
    document = plan(settings)
    folder = runner.make_root(output_root, settings.experiment_id)
    document.update(status='RUNNING', started_at_ns=time.time_ns())
    runner.write_json(folder / 'experiment.json', document)
    records = []
    try:
        runner.snapshot(folder, document['sources'])
        with runner.new_file(folder / 'faults.jsonl') as raw, runner.new_file(folder / 'faults.csv') as flat:
            writer = csv.DictWriter(flat, fieldnames=FIELDS); writer.writeheader()
            for case in settings.cases:
                target = folder / case; target.mkdir(mode=0o700)
                # A writable, private execution copy per case keeps fixture import
                # side effects outside both live sources and the frozen snapshot.
                for name in document['sources']:
                    path = target / 'app' / name; path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
                    with runner.new_file(path) as stream: stream.buffer.write((folder / 'source' / name).read_bytes())
                command = [sys.executable, '-I', '-B', str(target / 'app/tools/benchmarks/evaluation/fault_worker.py'), case, str(target)]
                runner.write_json(target / 'invocation.json', dict(command=command, definition=CASES[case]))
                row = dict(case=case, scope=document['scope'], status='FAILED', exit_code=None, harness_elapsed_ns=None,
                    **{key: None for key in METRICS}, metric_scope='not_measured', details=None)
                started = time.monotonic_ns()
                try:
                    code, timed_out = execute(command, target, child_environment(), settings.timeout_seconds)
                    row.update(exit_code=code, status='TIMEOUT' if timed_out else 'FAILED')
                    receipt = target / 'measurements.json'
                    if code == 0 and not timed_out and receipt.is_file():
                        if receipt.stat().st_size > 1024 * 1024: raise ValueError('Oversized case receipt')
                        result = json.loads(receipt.read_text())
                        if result['case'] != case or result['status'] != 'PASS': raise ValueError('Wrong case receipt')
                        for key in METRICS:
                            value = result.get(key)
                            if value is not None and (type(value) is not int or not 0 <= value < 2**63): raise ValueError('Invalid phase duration')
                            row[key] = value
                        row.update(status='PASS', details=result.get('details'), metric_scope=result['metric_scope'])
                except (OSError, ValueError, KeyError) as error:
                    row['error'] = type(error).__name__
                finally:
                    row['harness_elapsed_ns'] = time.monotonic_ns() - started
                    runner.write_json(target / 'result.json', row)
                    raw.write(json.dumps(row, allow_nan=False) + '\n'); raw.flush(); os.fsync(raw.fileno())
                    writer.writerow({k: row[k] for k in FIELDS}); flat.flush(); os.fsync(flat.fileno()); records.append(row)
        status = 'LOCAL_PASS_LIVE_DEFERRED' if all(r['status'] == 'PASS' for r in records) else 'FAILED'
        summary = dict(status=status, passed=sum(r['status'] == 'PASS' for r in records), attempted=len(records),
            physical_tests_run=0, deferred=DEFERRED, scope=document['scope'], phase13_complete=False)
        runner.write_json(folder / 'summary.json', summary)
        return folder, summary
    finally:
        document.update(status='COMPLETE_LOCAL_RECORDS' if len(records) == len(settings.cases) else 'INTERRUPTED', finished_at_ns=time.time_ns(), recorded_cases=len(records))
        runner.write_json(folder / 'completion.json', document)
