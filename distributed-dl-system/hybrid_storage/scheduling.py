"""Trusted coordinator policy wiring; default resource calls remain unobserved.

No startup, DB migration, transport, reservation, or training authority. Logs
describe a selection proposal; the existing scheduler/lease gates still commit it.
"""
from collections import deque
from dataclasses import asdict, replace
import json
import logging

from .cache_coverage import CoverageAPI
from .config import require_staging_config, StorageConfigError
from .input_manifests import job_identity
from .inventory_protocol import worker_id
from .locality import JobLocality
from .registry import StorageRegistry
from .workspaces import WorkspacePlan
from . import events

LOGGER = logging.getLogger('distributed_ml.scheduler')
MAX_NODES = 256
MAX_RECORD_BYTES = 128 * 1024


def _log_choice(document):
    LOGGER.info('SCHEDULER_CHOICE %s', json.dumps(document, sort_keys=True, allow_nan=False))


class SchedulingPolicy:
    """Controller-thread owned. Explicit logger also enables resource tracing.

    Locality automatically retains bounded operator records and emits INFO logs.
    Resource without an explicit logger calls the original selector directly.
    Use a reviewed nonblocking logging handler in a production bootstrap.
    """
    def __init__(self, registry, *, settings, coverage=None, choice_logger=None, max_choices=128,
                 stage_parallelism=3):
        if not isinstance(registry, StorageRegistry):raise TypeError('Expected trusted StorageRegistry')
        self.settings = require_staging_config(settings)
        if coverage is not None and (not isinstance(coverage, CoverageAPI) or coverage.registry is not registry):
            raise ValueError('Coverage must belong to this exact controller registry')
        if choice_logger is not None and not callable(choice_logger):raise TypeError('Expected callable choice logger')
        if type(max_choices) is not int or not 1 <= max_choices <= 4096:raise ValueError('Invalid choice history bound')
        self.registry, self.coverage = registry, coverage
        self.choice_logger = choice_logger
        if type(stage_parallelism) is not int or not 1 <= stage_parallelism <= 32:
            raise ValueError('Invalid stage parallelism')
        self.stage_parallelism=stage_parallelism
        self._history = deque(maxlen=max_choices)
        self.recorded = 0; self.dropped = 0

    def set_policy(self, policy):
        """Next placement only; no rebind, cache/attempt cleanup or running-job mutation."""
        if type(policy) is not str or policy not in ('resource', 'locality', 'bandwidth'):
            raise StorageConfigError('Runtime scheduler policy must be resource|locality|bandwidth')
        self.settings = require_staging_config(replace(self.settings, scheduler_policy=policy))

    def history(self):
        return tuple(json.loads(raw) for raw in self._history)

    def select(self, job, plans, selector, *args, **kwargs):
        requested = self.settings.scheduler_policy  # One policy snapshot per placement.
        if requested == 'resource' and self.choice_logger is None:
            result = selector(*args, **kwargs)
            self._emit(job, plans, result, requested, 'resource', 'resource_policy', None)
            return result
        from scheduler_metrics import SchedulerMetrics, observe_scheduling
        job_identity(job['job_id'])
        preference = None
        reason = 'resource_policy' if requested == 'resource' else 'coverage_unavailable'
        if requested in ('locality','bandwidth') and self.coverage is not None:
            # Actual coordinator plans currently require full inputs at every rank.
            # Refuse invented shard locality if that planner contract ever changes.
            if (type(plans) is tuple and plans and all(isinstance(p, WorkspacePlan) for p in plans)
                    and all(p.identity.job_id == job['job_id'] and p.identity.rank == rank
                            and p.input_manifest_sha256 == plans[0].input_manifest_sha256
                            and p.source == plans[0].source and p.dataset == plans[0].dataset
                            for rank, p in enumerate(plans))):
                kind=JobLocality
                extra={}
                if requested=='bandwidth':
                    from .bandwidth import JobBandwidth
                    kind=JobBandwidth;extra['stage_parallelism']=self.stage_parallelism
                preference = kind(self.coverage, job['owner'], job['job_id'], plans[0].input_manifest_sha256,**extra)
            else:
                reason = 'unsupported_rank_input_plan'
        collector = SchedulerMetrics(max_decisions=1, trace_nodes=True)
        with observe_scheduling(collector):
            result = selector(*args, **kwargs, **({'locality': preference} if preference is not None else {}))
        # Selection failures retain their original exceptions, not a fabricated choice.
        self._record(job, plans, requested, reason, preference, collector.snapshot(), result)
        return result

    def _emit(self, job, plans, result, requested, policy, reason, missing):
        if not events.enabled():return
        try:
            selected = (() if result is None else (result['node']['id'],) if isinstance(result,dict)
                        else tuple(node['id'] for node in result))
            for rank, node in enumerate(selected[:MAX_NODES] or (None,)):
                plan = plans[rank] if rank < len(plans) and isinstance(plans[rank],WorkspacePlan) else None
                events.emit('storage.scheduler.choice', job_id=job['job_id'],
                    attempt_id=f"{job['job_id']}:{job['retry_count']}", worker_id=node,
                    rank=rank if node is not None else None,
                    manifest_version=None if plan is None else plan.input_manifest_sha256,
                    manifest_schema_version=None if plan is None else 1,
                    dataset_version=None if plan is None or plan.dataset is None else plan.dataset.version,
                    requested_policy=requested,scheduler_policy=policy,reason=reason,
                    missing_bytes=None if missing is None or node is None else missing[rank])
        except Exception:pass

    def _record(self, job, plans, requested, fallback_reason, preference, snapshot, result):
        try:
            trace = snapshot.decisions[-1] if snapshot.decisions else None
            locality = preference.snapshot() if preference is not None else None
            selected = (() if result is None else (result['node']['id'],) if isinstance(result,dict)
                        else tuple(node['id'] for node in result))
            for node in selected:worker_id(node)
            workers = [] if locality is None else [asdict(w) for w in locality.workers[:MAX_NODES]]
            for row in workers:worker_id(row['worker_id'])
            total = None if locality is None else locality.document()['total_missing_bytes']
            document = dict(schema_version=1, event='SCHEDULER_CHOICE', job_id=job['job_id'],
                stage='selection_proposal',outcome='selected' if selected else 'no_eligible_placement',
                requested_policy=requested, policy='resource' if locality is None else locality.policy,
                reason=fallback_reason if locality is None else locality.reason,
                selector=None if trace is None else trace.selector,
                resource_primary_order='cpu_ascending_then_ram_descending',
                locality_scope='resource_equivalent_only',
                selected_nodes=list(selected[:MAX_NODES]),
                selected_count=len(selected),
                total_missing_bytes=total,
                per_rank_missing_bytes=None if locality is None or locality.per_rank_missing_bytes is None
                                       else list(locality.per_rank_missing_bytes[:MAX_NODES]),
                workers=workers, trace_available=trace is not None,
                duration_ns=None if trace is None else trace.duration_ns,
                eligible_nodes=None if trace is None else trace.eligible_nodes,
                examined_nodes=None if trace is None else trace.examined_nodes,
                resource_queries=None if trace is None else trace.resource_queries,
                complete_groups=None if trace is None else trace.complete_groups,
                compatibility_groups=None if trace is None else trace.compatibility_groups,
                filtered_counts={} if trace is None else dict(trace.filtered),
                filtered_nodes=[] if trace is None else [dict(worker_id=w, reason=r) for w, r in trace.filtered_nodes],
                candidates=[] if trace is None else [dict(worker_id=w, cpu_usage=cpu, available_ram_gb=ram)
                                                      for w, cpu, ram in trace.candidates],
                details_truncated=(len(selected)>MAX_NODES or (locality is not None and len(locality.workers)>MAX_NODES)
                                   or (trace is not None and (trace.omitted_nodes>0 or trace.selected_count>MAX_NODES))))
            if requested=='bandwidth':
                document['bandwidth']=None if preference is None else preference.prediction
                if document['bandwidth'] is not None:
                    prediction=dict(document['bandwidth'])
                    rows=prediction['workers']
                    prediction['workers']=rows[:32]
                    document['details_truncated'] |= len(rows)>32
                    document['bandwidth']=prediction
            raw = json.dumps(document, sort_keys=True, allow_nan=False)
            if len(raw.encode()) > MAX_RECORD_BYTES:raise ValueError('Choice log bound exceeded')
            if len(self._history) == self._history.maxlen:self.dropped += 1
            self._history.append(raw); self.recorded += 1
            self._emit(job,plans,result,requested,document['policy'],document['reason'],
                       document['per_rank_missing_bytes'])
            try:(self.choice_logger if self.choice_logger is not None else _log_choice)(json.loads(raw))
            except Exception:pass  # Logging failures never prevent a valid reservation/start.
        except Exception:
            self.dropped += 1  # Diagnostics, never an execution authority.
