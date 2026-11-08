"""Read-only owner-scoped student projection; telemetry is never authority."""

import json

from hybrid_storage.staging_metrics import NAMES, validated

LABELS = {'STAGING': 'Preparing inputs', 'READY': 'Inputs ready; waiting for launch',
          'STARTED': 'Inputs ready', 'ABORTING': 'Stopping and releasing inputs',
          'TERMINAL': 'Input staging closed'}


def event_details(event):
    """Strict allowlist for service history in hybrid student responses."""
    if event.get('event_type') != 'INPUTS_STAGED':
        return {}
    try:
        data = json.loads(event.get('details_json', '{}'))
        metrics = validated(data.get('metrics'))
        if (metrics is None or type(data.get('attempt')) is not int or data['attempt'] < 1
                or type(data.get('rank')) is not int or not 0 <= data['rank'] <= 65535):
            return {}
        return dict(attempt=data['attempt'], rank=data['rank'], metrics=metrics)
    except (ValueError, TypeError, AttributeError):
        return {}


def read(db, job_id, owner):
    """Caller supplies a read-only snapshot. Owner checked again before side tables.

    Absence of the optional staging tables is normal for legacy installations.
    Do not migrate on a dashboard poll or treat cached inventory as verified input.
    """
    job = db.execute('''SELECT j.owner,j.job_type,COALESCE(b.backend,'legacy'),j.retry_count
        FROM jobs j LEFT JOIN job_storage_bindings b ON b.job_id=j.job_id WHERE j.job_id=?''', (job_id,)).fetchone()
    if job is None or tuple(job[:3]) != (owner, 'custom', 'nfs'):
        return None
    tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if not {'hybrid_staging_attempts', 'hybrid_staging_ranks'} <= tables:
        return None
    stage = db.execute('''SELECT stage_id,phase,world_size,updated_at_ns FROM hybrid_staging_attempts
        WHERE job_id=? AND owner=? AND retry_count=?''', (job_id, owner, job[3])).fetchone()
    if stage is None:
        return None
    stage_id, phase, total, updated = stage
    if phase not in LABELS:
        return None
    ranks = [dict(rank=row[0], ready=row[1] == 'READY', metrics=None)
             for row in db.execute('SELECT rank,phase FROM hybrid_staging_ranks WHERE stage_id=? ORDER BY rank', (stage_id,))]
    if 'recovery_events' in tables:
        # At most one event per first-READY transition. Bound historical reads.
        events = db.execute('''SELECT details_json FROM recovery_events
            WHERE job_id=? AND event_type='INPUTS_STAGED' ORDER BY id DESC LIMIT ?''', (job_id, total)).fetchall()
        by_rank = {rank['rank']:rank for rank in ranks}
        for row in events:
            event = dict(event_type='INPUTS_STAGED', details_json=row[0])
            details = event_details(event)
            try:
                same = json.loads(row[0]).get('stage_id') == stage_id
            except (ValueError, TypeError, AttributeError):
                same = False
            if same and details and details['attempt'] == job[3] + 1:
                rank = by_rank.get(details['rank'])
                if rank is not None and rank['ready'] and rank['metrics'] is None:
                    rank['metrics'] = details['metrics']
    measured = [rank['metrics'] for rank in ranks if rank['metrics'] is not None]
    # Totals cover acknowledged ranks only. Zero is never substituted for missing data.
    metrics = None if not measured else dict(schema_version=1, **{key:sum(item[key] for item in measured) for key in NAMES})
    return dict(schema_version=1, attempt=job[3]+1, phase=phase, label=LABELS[phase],
                ready_ranks=sum(rank['ready'] for rank in ranks), total_ranks=total,
                measured_ranks=len(measured), updated_at=updated/1e9, metrics=metrics, ranks=ranks)


def log_text(view):
    if view is None:
        return 'Input staging has not been recorded for this attempt.\n'
    lines = [f"Attempt {view['attempt']}: {view['label']}.",
             f"Verified workspaces acknowledged: {view['ready_ranks']}/{view['total_ranks']} ranks.",
             f"Measurements available: {view['measured_ranks']}/{view['total_ranks']} ranks."]
    for rank in view['ranks']:
        metrics = rank['metrics']
        if metrics is None:
            lines.append(f"Rank {rank['rank']}: measurements not recorded.")
        else:
            lines.append(f"Rank {rank['rank']}: cache reused {metrics['cache_hit_bytes']} bytes; "
                         f"staged {metrics['staged_bytes']} bytes; private copies {metrics['workspace_copy_bytes']} bytes; "
                         f"verification {metrics['verification_ns']/1e9:.3f} s; copy {metrics['copy_ns']/1e9:.3f} s.")
    lines.append('Measurements describe successful staging requests. Durations overlap; bytes are logical file sizes.')
    return '\n'.join(lines) + '\n'
