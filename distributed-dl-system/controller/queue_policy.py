"""Configurable student limits and persistent fair ordering of runnable queue entries."""
import json
from pathlib import Path

POLICY_FILE = Path(__file__).resolve().parents[1] / 'config/queue_policy.json'
DEFAULTS = {'max_active_per_owner':1,'max_pending_per_owner':20,'aging_seconds':300,'owners':{}}
ACTIVE_STATES = ('SCHEDULED','RUNNING','CANCELLING')
PENDING_STATES = ('RECEIVED','STAGED','READY','QUEUED','HELD')


def load_policy():
    """Reject malformed limits instead of silently disabling fairness on configuration errors."""
    value = json.loads(POLICY_FILE.read_text()) if POLICY_FILE.exists() else DEFAULTS.copy()
    if not isinstance(value,dict) or set(value) != set(DEFAULTS):
        raise ValueError('Queue policy must contain max_active_per_owner, max_pending_per_owner, aging_seconds and owners')
    for key in ('max_active_per_owner','max_pending_per_owner','aging_seconds'):
        if type(value[key]) is not int or value[key] < 1:
            raise ValueError('Queue policy ' + key + ' must be a positive integer')
    if not isinstance(value['owners'],dict):
        raise ValueError('Queue policy owners must be an object')
    from job_spec import validate_owner
    for owner, overrides in value['owners'].items():
        validate_owner(owner)
        if not isinstance(overrides,dict) or set(overrides) - {'max_active_per_owner','max_pending_per_owner'}:
            raise ValueError('Owner overrides may contain only active and pending limits')
        for key, limit in overrides.items():
            if type(limit) is not int or limit < 1:
                raise ValueError('Owner queue limits must be positive integers')
    return value


def limits(policy, owner):
    """Apply administrator-configured overrides to a student's default quotas."""
    return {**policy, **policy['owners'].get(owner,{})}


def identity(job):
    """Old bundled jobs share a compatibility lane and keep their existing concurrency."""
    return job['owner'] if job.get('job_type') == 'custom' else '__legacy__'


def fair_order(queued, active, service, policy, now):
    """Round-robin owners, preserve FIFO within a turn, and promote old waiting owners.

    Last-service timestamps survive controller restarts. Resource feasibility is
    checked by the dispatcher; an unplaceable job does not block other owners.
    """
    counts = {}
    for job in active:
        owner = identity(job)
        counts[owner] = counts.get(owner,0) + 1
    groups = {}
    for job in sorted(queued,key=lambda j:(j.get('queued_at') or j['created_at'],j['job_id'])):
        owner = identity(job)
        if job.get('job_type') == 'custom' and counts.get(owner,0) >= limits(policy,owner)['max_active_per_owner']:
            continue
        groups.setdefault(owner,[]).append(job)
    owners = sorted(groups,key=lambda owner:(
        0 if now - (groups[owner][0].get('queued_at') or groups[owner][0]['created_at']) >= policy['aging_seconds'] else 1,
        counts.get(owner,0), service.get(owner,0),
        groups[owner][0].get('queued_at') or groups[owner][0]['created_at'],owner))
    ordered = []
    while owners:
        for owner in owners:
            ordered.append(groups[owner].pop(0))
        owners = [owner for owner in owners if groups[owner]]
    return ordered
