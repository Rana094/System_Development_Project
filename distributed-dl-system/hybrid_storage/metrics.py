"""Process-local advisory counters; no I/O, identifiers, authority or persistence.

Durations overlap: copy_ns includes nested readback verification, fsync and helper
waits. verification_ns measures parent/local hash passes, not child CPU or wire
traffic. Staged bytes count accepted logical inputs, not retransmitted NFS bytes.
"""

from contextlib import contextmanager
from dataclasses import asdict, dataclass
import threading
import time


@dataclass(frozen=True)
class MetricsSnapshot:
    cache_hit_bytes: int = 0
    staged_bytes: int = 0
    workspace_copy_bytes: int = 0
    verification_ns: int = 0
    copy_ns: int = 0
    copy_attempts: int = 0
    copy_failures: int = 0
    copy_timeouts: int = 0
    cache_corruptions: int = 0
    workspace_failures: int = 0

    def document(self):
        return dict(schema_version=1, **asdict(self))


class StorageMetrics:
    """Thread-safe cumulative counters, scoped to an explicit cache handle.

    Related fetch/workspace handles share cache.metrics. Fresh processes/handles
    start at zero unless callers explicitly share a collector in that process.
    Snapshots are immutable; there is no disk log or metrics endpoint. A future
    trusted telemetry owner can consume snapshots without involving job state.
    """

    def __init__(self):
        self._values = asdict(MetricsSnapshot())
        self._lock = threading.Lock()

    def add(self, **values):
        if any(name not in self._values or type(value) is not int or value < 0
               for name, value in values.items()):
            raise ValueError("Expected known metrics with nonnegative integer increments")
        with self._lock:
            for name, value in values.items():
                self._values[name] += value

    def snapshot(self) -> MetricsSnapshot:
        with self._lock:
            return MetricsSnapshot(**self._values)

    @contextmanager
    def span(self, name):
        if name not in ("verification_ns", "copy_ns"):
            raise ValueError("Expected a duration metric")
        started = time.perf_counter_ns()
        try:
            yield
        finally:
            self.add(**{name: max(0, time.perf_counter_ns() - started)})
