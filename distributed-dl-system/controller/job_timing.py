"""Save controller wall times separately from trainer-reported monotonic durations."""
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import tempfile


def seconds(value):
    return round(value, 6) if type(value) in (int, float) and math.isfinite(value) and value >= 0 else None


def elapsed(start, end):
    return seconds(end-start) if start is not None and end is not None else None


def timestamp(value):
    return datetime.fromtimestamp(value, timezone.utc).isoformat() if value is not None else None


def build_report(job, attempts):
    """A failed attempt's finish time is detection time, not measured computation time."""
    started = [a['started_at'] for a in attempts if a.get('started_at') is not None]
    first = min(started) if started else None
    finished = job.get('finished_at')
    root = Path(job['storage_path'])/'results'
    spec = json.loads(job['job_config'])
    distributed = spec['execution']['mode'] == 'distributed'
    summaries = [(rank, root/f'rank-{rank}'/'summary.json') for rank in range(spec['execution']['workers'])] if distributed else [(0, root/'summary.json')]
    ranks = []
    for rank, path in summaries:
        loop = program = None
        if job['status'] == 'COMPLETED' and path.is_file() and not path.is_symlink() and path.stat().st_size <= 1024*1024:
            try:
                data = json.loads(path.read_text())
                timing = data.get('timing', {})
                loop = seconds(timing.get('training_loop_seconds', data.get('training_seconds')))
                program = seconds(timing.get('training_program_seconds'))
            except (ValueError, OSError, AttributeError, TypeError):
                pass  # Arbitrary student output need not implement timing instrumentation.
        ranks.append({'rank': rank, 'training_loop_seconds': loop, 'training_program_seconds': program})
    def maximum(key):
        values = [rank[key] for rank in ranks]
        return max(values) if values and all(value is not None for value in values) else None
    return {
        'schema_version': 1, 'job_id': job['job_id'], 'status': job['status'], 'units': 'seconds',
        'submitted_at_utc': timestamp(job.get('created_at')),
        'first_attempt_started_at_utc': timestamp(first), 'finished_at_utc': timestamp(finished),
        'preparation_and_initial_queue_seconds': elapsed(job.get('created_at'), first),
        'full_training_wall_seconds': elapsed(first, finished),
        'submission_to_completion_seconds': elapsed(job.get('created_at'), finished),
        'final_attempt_training_loop_seconds': maximum('training_loop_seconds'),
        'final_attempt_training_program_seconds': maximum('training_program_seconds'),
        'final_attempt_rank_timings': ranks,
        'attempts': [{'number': a['attempt_number'], 'node': a['node_id'], 'status': a['status'],
                      'started_at_utc': timestamp(a.get('started_at')), 'finished_at_utc': timestamp(a.get('finished_at')),
                      'observed_wall_seconds': elapsed(a.get('started_at'), a.get('finished_at'))} for a in attempts],
        'definitions': {
            'full_training_wall_seconds': 'First scheduled attempt start to final recorded finish, including startup, interruptions, retry waiting and recovery. DDP includes controller completion/collection overhead.',
            'submission_to_completion_seconds': 'Submission to final recorded finish, including environment preparation and initial queue waiting.',
            'final_attempt_training_loop_seconds': 'Trainer-reported loop duration for the successful final attempt only. Includes checkpointing, synchronization, logs and deliberate delays. DDP uses the slowest rank, not the sum.',
            'final_attempt_training_program_seconds': 'From before PyTorch import through setup, loop, final checkpoint, evaluation and model saving, before summary writing. Final attempt only; DDP uses the slowest rank.',
            'attempts': 'Controller-observed attempt intervals. Failed attempt finish can be failure-detection time; do not interpret these as compute time.',
            'missing_measurements': 'null means unavailable. Older jobs and uninstrumented user programs cannot provide previously unrecorded loop/program durations.',
            'clock_basis': 'Trainer durations use a monotonic clock; job intervals use persisted wall timestamps and require synchronized worker/controller clocks.'
        }
    }


def write_report(job_id):
    from job_store import get_job, get_attempts
    job = get_job(job_id)
    if not job or job.get('job_type') != 'custom' or not job.get('storage_path') or job['status'] not in ('COMPLETED', 'FAILED', 'CANCELLED'):
        return
    report = build_report(job, get_attempts(job_id))
    root = Path(job['storage_path'])/'results'
    root.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix='.timing-', dir=root.parent)
    try:
        with os.fdopen(fd, 'w') as output:
            json.dump(report, output, indent=2)
            output.write('\n')
            output.flush()
            os.fsync(output.fileno())
        os.replace(name, root/'system-timing.json')
    finally:
        Path(name).unlink(missing_ok=True)
    return report
