"""Optional worker-local observations of real, verified NFS cache copies.

No probes, persistence, transport, scheduler activation or execution authority.
Rates count application payload, including verification/fsync/helper overhead in
the denominator; they are not NIC bytes, cold-wire speed or training runtime.
"""

from collections import deque
from dataclasses import dataclass
import math
import statistics
import threading
import time

from .inventory_protocol import worker_id

MAX_INT = 2**63 - 1
MIB = 1024 * 1024


@dataclass(frozen=True)
class TransferSample:
    payload_bytes: int
    duration_ns: int
    observed_at: float
    monotonic_at: float
    transfer_ns: int | None = None

    @property
    def bytes_per_second(self):
        return self.payload_bytes * 1_000_000_000 / self.duration_ns


class TransferMetrics:
    """One configured worker, bounded count/age, median and median deviation.

    Construct explicitly in the trusted worker bootstrap and pass to CacheFetcher
    as transfer_metrics. Never derive worker identity from student input. Copies
    serialize through the existing cache-root flock; concurrent transfers on other
    roots/workers are UNKNOWN. Thread-safe snapshots; history resets on reboot.
    Wall time labels observations; monotonic time alone determines freshness.
    """

    def __init__(self, worker, *, max_samples=64, max_age_seconds=900,
                 clock=time.monotonic, wall_clock=time.time):
        self.worker = worker_id(worker)
        if type(max_samples) is not int or not 1 <= max_samples <= 4096:
            raise ValueError('max_samples must be between 1 and 4096')
        if (type(max_age_seconds) not in (int, float) or not math.isfinite(max_age_seconds)
                or not 0 < max_age_seconds <= 86400):
            raise ValueError('max_age_seconds must be finite and at most one day')
        if not callable(clock) or not callable(wall_clock):
            raise ValueError('Expected explicit clocks')
        self.max_age_seconds = max_age_seconds
        self._samples = deque(maxlen=max_samples)
        self._clock, self._wall = clock, wall_clock
        self._lock = threading.Lock()
        self._recorded = 0

    def record(self, payload_bytes, duration_ns, *, transfer_ns=None):
        """Record only a nonempty successful copy after parent verification.

        Zero-byte objects, failures, retries that fail, hits and workspace copies
        are not rate samples. Callers must not treat this advisory data as proof
        of authorized publication or stage readiness.
        """
        if (type(payload_bytes) is not int or not 1 <= payload_bytes <= MAX_INT
                or type(duration_ns) is not int or not 1 <= duration_ns <= MAX_INT):
            raise ValueError('Expected positive bounded bytes and nanoseconds')
        if transfer_ns is not None and (type(transfer_ns) is not int or not 0 < transfer_ns <= duration_ns):
            raise ValueError('Transfer interval must be within the measured copy attempt')
        with self._lock:
            now, wall = self._clock(), self._wall()
            if not math.isfinite(now) or not math.isfinite(wall):
                raise ValueError('Expected finite clock readings')
            self._samples.append(TransferSample(payload_bytes, duration_ns, wall, now, transfer_ns))
            self._recorded += 1

    def prediction_snapshot(self):
        """Decomposed copies only: no double counting of inclusive 11.1 rates.

        Residual setup/helper/parent-verification cost is observed per object,
        not proven constant or independent of size. Return size range for that
        model limitation; old inclusive-only samples never become calibration.
        """
        with self._lock:
            now = self._clock()
            if not math.isfinite(now):raise ValueError('Expected finite clock reading')
            samples = tuple(s for s in self._samples if s.transfer_ns is not None
                            and 0 <= now-s.monotonic_at <= self.max_age_seconds)
        rates = [s.payload_bytes*1e9/s.transfer_ns for s in samples]
        overhead = [(s.duration_ns-s.transfer_ns)/1e9 for s in samples]
        return dict(schema_version=1, sample_count=len(samples), max_age_seconds=self.max_age_seconds,
            oldest_age_seconds=max((now-s.monotonic_at for s in samples),default=None),
            median_bytes_per_second=statistics.median(rates) if rates else None,
            min_bytes_per_second=min(rates,default=None), max_bytes_per_second=max(rates,default=None),
            median_overhead_seconds=statistics.median(overhead) if overhead else None,
            min_overhead_seconds=min(overhead,default=None), max_overhead_seconds=max(overhead,default=None),
            min_payload_bytes=min((s.payload_bytes for s in samples),default=None),
            max_payload_bytes=max((s.payload_bytes for s in samples),default=None))

    def snapshot(self):
        with self._lock:
            now = self._clock()
            if not math.isfinite(now):
                raise ValueError('Expected finite clock reading')
            retained = tuple(self._samples)
            # Reject a backwards clock, too. A wall-clock correction has no effect.
            samples = tuple(s for s in retained if 0 <= now - s.monotonic_at <= self.max_age_seconds)
            recorded = self._recorded
        rates = [s.bytes_per_second for s in samples]
        median = statistics.median(rates) if rates else None
        return dict(
            schema_version=1, worker_id=self.worker,
            source='verified_nfs_to_local_copy',
            duration_scope='copy_attempt_through_parent_verification',
            payload_scope='application_bytes_not_wire_traffic',
            max_age_seconds=self.max_age_seconds,
            max_samples=self._samples.maxlen,
            sample_count=len(samples), retained_sample_count=len(retained),
            recorded_sample_count=recorded,
            status='recent' if samples else ('stale' if retained else 'no_samples'),
            newest_age_seconds=min((now-s.monotonic_at for s in samples), default=None),
            oldest_age_seconds=max((now-s.monotonic_at for s in samples), default=None),
            last_observed_at=retained[-1].observed_at if retained else None,
            last_observation_age_seconds=(now-retained[-1].monotonic_at) if retained else None,
            payload_bytes=sum(s.payload_bytes for s in samples),
            duration_ns=sum(s.duration_ns for s in samples),
            median_bytes_per_second=median,
            median_mib_per_second=median/MIB if median is not None else None,
            mad_bytes_per_second=(statistics.median(abs(r-median) for r in rates) if rates else None),
            min_bytes_per_second=min(rates, default=None),
            max_bytes_per_second=max(rates, default=None),
            per_cache_root_copy_limit=1, cluster_concurrent_transfers=None,
            fixed_overhead_seconds=None,
        )
