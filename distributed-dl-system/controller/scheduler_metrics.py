"""Opt-in, bounded operator diagnostics for completed resource selections.

No files, DB, HTTP, configuration loading, cache lookup or scheduling authority.
Outside observe_scheduling(), selectors do not read the measurement clock.
"""
from collections import deque
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import asdict, dataclass
import threading
import time
import re
import math

_ACTIVE = ContextVar('resource_scheduler_metrics',default=None)
FILTERS = ('not_allowed','reserved','excluded','unavailable','busy','resources',
           'unprepared','lease','compatibility')
MAX_SELECTED = 256  # Diagnostic truncation only; never a worker-count limit.


@dataclass(frozen=True)
class SelectionDecision:
    selector: str
    duration_ns: int
    examined_nodes: int
    resource_queries: int
    eligible_nodes: int
    compatibility_groups: int
    complete_groups: int
    filtered: tuple[tuple[str,int], ...]
    selected_nodes: tuple[str, ...]
    selected_count: int
    policy: str = 'resource'
    filtered_nodes: tuple[tuple[str, str], ...] = ()
    candidates: tuple[tuple[str, float, float], ...] = ()
    omitted_nodes: int = 0
    trace_nodes: bool = False

    def document(self):
        result=asdict(self)
        result.update(schema_version=1,filtered=dict(self.filtered),
                      selected_nodes=list(self.selected_nodes),
                      selected_nodes_truncated=self.selected_count>len(self.selected_nodes))
        if not self.trace_nodes:
            for key in ('filtered_nodes','candidates','omitted_nodes','trace_nodes'):result.pop(key)
        return result


@dataclass(frozen=True)
class SchedulerMetricsSnapshot:
    recorded: int
    dropped: int
    decisions: tuple[SelectionDecision, ...]

    def document(self):
        return dict(schema_version=1,recorded=self.recorded,dropped=self.dropped,
                    decisions=[decision.document() for decision in self.decisions])


class SchedulerMetrics:
    """Caller-owned diagnostics; oldest records are evicted at the fixed bound."""
    def __init__(self, *, max_decisions=128, trace_nodes=False):
        if type(max_decisions) is not int or not 1<=max_decisions<=4096:
            raise ValueError('Expected a metrics history bound from 1 to 4096')
        if type(trace_nodes) is not bool:raise ValueError('trace_nodes must be boolean')
        self.trace_nodes=trace_nodes
        self._decisions=deque(maxlen=max_decisions)
        self._recorded=0;self._dropped=0;self._lock=threading.Lock()

    def _record(self, decision):
        with self._lock:
            if len(self._decisions)==self._decisions.maxlen:self._dropped+=1
            self._decisions.append(decision);self._recorded+=1

    def snapshot(self):
        with self._lock:
            return SchedulerMetricsSnapshot(self._recorded,self._dropped,tuple(self._decisions))


@contextmanager
def observe_scheduling(metrics):
    """Scope to this execution context; None explicitly disables nested recording.

    Wrap the trusted controller's existing dispatch call, not a second dispatcher.
    There is no implicit inheritance into newly created threads or processes.
    """
    if metrics is not None and not isinstance(metrics,SchedulerMetrics):
        raise TypeError('Expected caller-owned SchedulerMetrics or None')
    token=_ACTIVE.set(metrics)
    try:yield metrics
    finally:_ACTIVE.reset(token)


class _Selection:
    def __init__(self, metrics, selector):
        self.metrics=metrics;self.selector=selector;self.started=time.perf_counter_ns()
        self.examined=0;self.queries=0;self.eligible=0
        self.groups=0;self.complete=0;self.filtered={key:0 for key in FILTERS}
        self.trace_nodes=metrics.trace_nodes
        self.filtered_nodes=[];self.candidates=[];self.omitted_nodes=0

    def _capture(self, worker):
        if not self.trace_nodes:return False
        if (len(self.filtered_nodes)+len(self.candidates)>=MAX_SELECTED or type(worker) is not str
                or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,62}',worker) or worker=='local'):
            self.omitted_nodes+=1
            return False
        return True

    def skip(self, reason, worker=None):
        self.filtered[reason]+=1
        if self._capture(worker):self.filtered_nodes.append((worker,reason))

    def candidate(self, worker, cpu, ram):
        if not self.trace_nodes:return
        if any(type(value) not in (int,float) or (type(value) is float and not math.isfinite(value))
               for value in (cpu,ram)):
            self.omitted_nodes+=1
        elif self._capture(worker):self.candidates.append((worker,cpu,ram))

    def finish(self, nodes, *, policy='resource'):
        # Metrics failures must not replace or prevent an otherwise valid choice.
        # No extension callback is invoked inside the selection/filter loop.
        try:
            selected=tuple(node['id'] for node in nodes[:MAX_SELECTED])
            if any(type(node) is not str or len(node)>256 for node in selected):return
            self.metrics._record(SelectionDecision(self.selector,
                max(0,time.perf_counter_ns()-self.started),self.examined,self.queries,self.eligible,
                self.groups,self.complete,tuple(self.filtered.items()),selected,len(nodes),policy,
                tuple(self.filtered_nodes),tuple(self.candidates),self.omitted_nodes,self.trace_nodes))
        except Exception:
            pass


def begin_selection(selector):
    metrics=_ACTIVE.get()
    if metrics is None:return None
    try:return _Selection(metrics,selector)
    except Exception:return None
