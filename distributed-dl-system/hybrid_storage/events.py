"""Version-one, allowlisted operator JSON events; advisory, never execution grants.

No handlers, disk writes, SQLite, network, or logging configuration on import.
INFO must be enabled by a trusted bootstrap. Use a bounded/nonblocking handler.
"""
from collections import OrderedDict
from contextlib import contextmanager
from contextvars import ContextVar
import errno
import json
import logging
import re
import secrets
import time

LOGGER = logging.getLogger('distributed_ml.storage.events')
MAX_INTEGER = 2**63 - 1
MAX_RECORD_BYTES = 4096
_context = ContextVar('storage_event_context', default=None)
_COMMON = frozenset(('job_id', 'attempt_id', 'worker_id', 'rank', 'stage_id',
                     'manifest_version', 'manifest_schema_version', 'dataset_version'))
_FIELDS = {
    'storage.staging.started': frozenset(),
    'storage.staging.ready': frozenset(('cache_hit_bytes', 'transferred_bytes', 'verified_bytes',
                                      'staging_duration_ns', 'verification_duration_ns', 'copy_duration_ns')),
    'storage.staging.failed': frozenset(('staging_duration_ns', 'error_class', 'errno',
        'cache_hit_bytes', 'transferred_bytes', 'verified_bytes', 'verification_duration_ns', 'copy_duration_ns')),
    'storage.nfs.error': frozenset(('error_class', 'errno', 'copy_attempt')),
    'storage.nfs.retry': frozenset(('error_class', 'errno', 'copy_attempt')),
    'storage.scheduler.choice': frozenset(('requested_policy', 'scheduler_policy', 'reason', 'missing_bytes')),
    'storage.checkpoint.committed': frozenset(('checkpoint_id', 'verified_bytes', 'world_size')),
    'storage.recovery.started': frozenset(('checkpoint_id', 'world_size')),
    'storage.recovery.confirmed': frozenset(('checkpoint_id', 'world_size', 'recovery_latency_ns')),
}
_REASONS = frozenset(('resource_policy', 'coverage_unavailable', 'unsupported_rank_input_plan',
    'verified_missing_bytes', 'inventory_unavailable_or_stale', 'no_resource_tie',
    'no_eligible_workers', 'no_complete_group', 'job_context_mismatch', 'measured_copy_cost',
    'transfer_facts_unavailable', 'transfer_inventory_unavailable',
    'reported_disk_capacity_insufficient', 'transfer_samples_unavailable_or_stale'))
_ERRORS = frozenset(('timeout', 'os_error', 'mount_unavailable', 'integrity', 'capacity',
                     'denied_or_fenced', 'other'))


def enabled():
    try:return LOGGER.isEnabledFor(logging.INFO)
    except Exception:return False


def _identifier(value, pattern):
    return type(value) is str and re.fullmatch(pattern, value) is not None


def emit(event_type, **values):
    """Return an event ID or None on disabled/invalid/failed logging.

    Reject the ENTIRE record on unknown fields: no exception messages, filenames,
    owners, lease credentials, arbitrary tags, or student-controlled descriptions.
    IDs name observations, not commands; delivery is best-effort, not an outbox.
    """
    if not enabled():return None
    try:
        if event_type not in _FIELDS or set(values) - (_COMMON | _FIELDS[event_type]):return None
        if not _FIELDS[event_type] <= values.keys():return None
        if not {'job_id', 'attempt_id'} <= values.keys():return None
        job = values['job_id']
        if not _identifier(job, r'[A-Za-z0-9][A-Za-z0-9_.-]{0,127}'):return None
        if not _identifier(values['attempt_id'], re.escape(job) + r':[0-9]{1,30}'):return None
        for key, value in values.items():
            if key in ('job_id', 'attempt_id'):continue
            if value is None:continue  # Unknown/unavailable never means zero.
            if key == 'worker_id':
                if not _identifier(value, r'[A-Za-z0-9][A-Za-z0-9_.-]{0,63}'):return None
            elif key in ('stage_id', 'checkpoint_id'):
                if not _identifier(value, r'[0-9a-f]{32}'):return None
            elif key == 'manifest_version':
                if not _identifier(value, r'[0-9a-f]{64}'):return None
            elif key in ('scheduler_policy', 'requested_policy'):
                if type(value) is not str or value not in ('resource','locality','bandwidth'):return None
            elif key == 'reason':
                if type(value) is not str or value not in _REASONS:return None
            elif key == 'error_class':
                if type(value) is not str or value not in _ERRORS:return None
            elif type(value) is not int or not 0 <= value <= MAX_INTEGER:return None
            if key == 'rank' and value > 65535:return None
            if key == 'manifest_schema_version' and value != 1:return None
            if key == 'dataset_version' and value < 1:return None
            if key == 'world_size' and not 1 <= value <= 65536:return None
            if key == 'copy_attempt' and not 1 <= value <= 5:return None
            if key == 'errno' and value > 4095:return None
        now = time.time_ns()
        if type(now) is not int or not 0 <= now <= MAX_INTEGER:return None
        event_id = secrets.token_hex(16)
        document = dict(schema_version=1, event_type=event_type, event_id=event_id,
                        occurred_at_ns=now, **values)
        raw = json.dumps(document, sort_keys=True, separators=(',', ':'), allow_nan=False)
        if len(raw.encode('utf-8')) > MAX_RECORD_BYTES:return None
        LOGGER.info('%s', raw)
        return event_id
    except Exception:
        return None  # Telemetry can never veto a commit, reservation or lease gate.


def plan_context(plan, worker_id, stage_id):
    return dict(job_id=plan.identity.job_id, attempt_id=plan.identity.attempt_token,
        worker_id=worker_id, rank=plan.identity.rank, stage_id=stage_id,
        manifest_version=plan.input_manifest_sha256, manifest_schema_version=1,
        dataset_version=None if plan.dataset is None else plan.dataset.version)


@contextmanager
def scope(values):
    """Thread/task-local attribution only; explicitly enter inside worker staging."""
    token = _context.set(values)
    try:yield
    finally:_context.reset(token)


def error_fields(error):
    # Import only when observing an error; never serialize str(error) or filename.
    from .artifacts import ArtifactMismatch
    from .nfs_mount import MountNotReady
    from .worker_cache import CacheCorrupt
    number = getattr(error, 'errno', None)
    number = number if type(number) is int and 0 <= number <= 4095 else None
    kind = ('timeout' if isinstance(error, TimeoutError) else
            'mount_unavailable' if isinstance(error, MountNotReady) else
            'integrity' if isinstance(error, (ArtifactMismatch,CacheCorrupt)) else
            'capacity' if number in (errno.ENOSPC, errno.EDQUOT) else
            'denied_or_fenced' if isinstance(error, PermissionError) else
            'os_error' if isinstance(error, OSError) else 'other')
    return dict(error_class=kind, errno=number)


def copy_error(error, attempt, *, retry=False):
    values = _context.get()
    if values is not None and enabled():
        try:
            emit('storage.nfs.retry' if retry else 'storage.nfs.error',
                 **values, **error_fields(error), copy_attempt=attempt)
        except Exception:pass


class _StagingObservation:
    def __init__(self, values=None, started=None):
        self.values, self.started = values, started
        self.counters = None

    def ready(self, metrics, results):
        if self.values is None:return
        try:
            # Unique accepted objects, not repeated hash passes or wire traffic.
            emit('storage.staging.ready', **self.values,
                cache_hit_bytes=sum(r.size_bytes for r in results if r.cache_hit),
                transferred_bytes=sum(r.size_bytes for r in results if not r.cache_hit),
                verified_bytes=sum(r.size_bytes for r in results),
                staging_duration_ns=max(0,time.monotonic_ns()-self.started),
                verification_duration_ns=metrics['verification_ns'], copy_duration_ns=metrics['copy_ns'])
        except Exception:pass


@contextmanager
def observe_staging(plan, worker_id, stage_id):
    observation = _StagingObservation()
    if enabled():
        try:
            observation = _StagingObservation(plan_context(plan,worker_id,stage_id), time.monotonic_ns())
            emit('storage.staging.started', **observation.values)
        except Exception:observation = _StagingObservation()
    with scope(observation.values):
        try:yield observation
        except BaseException as error:
            if observation.values is not None:
                try:
                    counts = None if observation.counters is None else observation.counters.snapshot()
                    hits = None if counts is None else counts.cache_hit_bytes
                    transferred = None if counts is None else counts.staged_bytes
                    emit('storage.staging.failed', **observation.values, **error_fields(error),
                         staging_duration_ns=max(0,time.monotonic_ns()-observation.started),
                         cache_hit_bytes=hits, transferred_bytes=transferred,
                         verified_bytes=None if counts is None else hits+transferred,
                         verification_duration_ns=None if counts is None else counts.verification_ns,
                         copy_duration_ns=None if counts is None else counts.copy_ns)
                except Exception:pass
            raise


class RecoveryClock:
    """Bounded controller-instance monotonic spans; missing after restart => null.

    Measures reserved recovery dispatch -> verified SDK restore ACK, including
    input/checkpoint staging. It does NOT measure failure detection or downtime.
    """
    def __init__(self):self._starts = OrderedDict()

    def begin(self, job):
        if not enabled():return
        try:
            key = (job['job_id'], job['retry_count'])
            if key not in self._starts:self._starts[key] = time.monotonic_ns()
            while len(self._starts) > 128:self._starts.popitem(last=False)
        except Exception:pass

    def finish(self, job):
        try:
            start = self._starts.pop((job['job_id'],job['retry_count']), None)
            return None if start is None else max(0, time.monotonic_ns() - start)
        except Exception:return None
