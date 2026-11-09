"""Advisory job-bound tie breaking, explicitly passed by trusted controller code.

No configuration activation, reservations, input access grant or network I/O.
Current trainers require the complete registered input set at every DDP rank.
"""
from dataclasses import asdict, dataclass

from .cache_coverage import CacheCoverage, CoverageAPI
from .dataset_manifests import principal
from .input_manifests import job_identity
from .inputs import object_key


@dataclass(frozen=True)
class WorkerLocality:
    worker_id: str
    inventory_eligible: bool
    inventory_status: str
    inventory_age_ns: int | None
    inventory_session_id: str | None
    inventory_sequence: int | None
    computed_at_ns: int
    required_bytes: int
    verified_cached_bytes: int | None
    missing_bytes: int | None


@dataclass(frozen=True)
class LocalityDecision:
    policy: str
    reason: str
    workers: tuple[WorkerLocality, ...]
    selected_nodes: tuple[str, ...]
    # One entry per actual selected rank. Same digest on two workers incurs
    # two physical stage copies; aliases within each rank are counted once.
    per_rank_missing_bytes: tuple[int, ...] | None

    def document(self):
        result = asdict(self)
        result['requested_policy'] = 'locality'
        result['total_missing_bytes'] = (None if self.per_rank_missing_bytes is None
                                         else sum(self.per_rank_missing_bytes))
        return result


class JobLocality:
    """One placement's trusted job/owner/binding and freshness explanation.

    Construct after existing dispatch authorization. Registry ACL checks run
    again in the read snapshot; failures fall back in placement, never authorize
    staging. Do not share this object or its SQLite connection across threads.
    """
    def __init__(self, coverage, actor, job_id, input_manifest_sha256):
        if not isinstance(coverage, CoverageAPI):
            raise TypeError('Expected authenticated controller CoverageAPI')
        principal(actor); job_identity(job_id); object_key(input_manifest_sha256)
        self.coverage, self.actor, self.job_id = coverage, actor, job_id
        self.input_manifest_sha256 = input_manifest_sha256
        self._decision = None

    def snapshot(self):
        return self._decision

    def reset(self):
        self._decision = None

    def _read(self, worker_ids, world_size):
        self._coverage_rows = ()  # Private exact rows for the bandwidth subclass.
        try:
            rows = self.coverage.for_job_workers(self.actor, worker_ids, self.job_id,
                                                 rank=0 if world_size > 1 else None)
            if len(rows) != len(worker_ids):
                raise ValueError('Incomplete coverage snapshot')
            observed = []
            for worker, row in zip(worker_ids, rows):
                if (not isinstance(row, CacheCoverage) or row.worker_id != worker
                        or row.target_kind != 'job' or row.target_id != self.job_id
                        or row.input_manifest_sha256 != self.input_manifest_sha256
                        or row.rank != (0 if world_size > 1 else None)
                        or row.world_size != (world_size if world_size > 1 else None)
                        or type(row.required_bytes) is not int or row.required_bytes < 0):
                    raise ValueError('Coverage differs from exact placement inputs')
                fresh = (row.inventory_eligible is True and row.inventory_status == 'current'
                         and type(row.inventory_age_ns) is int and row.inventory_age_ns >= 0
                         and type(row.inventory_sequence) is int and row.inventory_sequence >= 1
                         and type(row.inventory_session_id) is str
                         and type(row.missing_bytes) is int and type(row.verified_cached_bytes) is int
                         and 0 <= row.missing_bytes <= row.required_bytes
                         and row.verified_cached_bytes + row.missing_bytes == row.required_bytes)
                observed.append(WorkerLocality(worker, fresh, row.inventory_status, row.inventory_age_ns,
                    row.inventory_session_id, row.inventory_sequence, row.computed_at_ns,
                    row.required_bytes, row.verified_cached_bytes if fresh else None,
                    row.missing_bytes if fresh else None))
            if len({row.required_bytes for row in rows}) != 1:
                raise ValueError('Full-input ranks must have identical requirements')
            if getattr(self,'_retain_coverage_rows',False):self._coverage_rows = tuple(rows)
            return tuple(observed), ('verified_missing_bytes' if all(r.inventory_eligible for r in observed)
                                     else 'inventory_unavailable_or_stale')
        except Exception:
            # Advisory inventory/DB failures cannot disable an otherwise valid
            # resource choice. Existing auth/staging/lease gates still run.
            return (), 'coverage_unavailable'

    def _finish(self, nodes, observed, reason, *, used=False):
        by_worker = {row.worker_id: row for row in observed}
        known = bool(observed) and all(row.inventory_eligible for row in observed)
        missing = tuple(by_worker[node['id']].missing_bytes for node in nodes) if known else None
        self._decision = LocalityDecision('locality' if used else 'resource', reason, observed,
                                          tuple(node['id'] for node in nodes), missing)

    def single(self, candidates):
        """Candidates already sorted by the unchanged resource primary keys."""
        if not candidates:
            self._finish([], (), 'no_eligible_workers')
            return None
        observed, reason = self._read(tuple(c['node']['id'] for c in candidates), 1)
        selected = candidates[0]
        used = reason == 'verified_missing_bytes'
        if used:
            missing = {r.worker_id: r.missing_bytes for r in observed}
            equivalent = [c for c in candidates if (c['cpu'], c['ram']) == (selected['cpu'], selected['ram'])]
            selected = min(equivalent, key=lambda c: missing[c['node']['id']])
            if len(equivalent) == 1:
                used, reason = False, 'no_resource_tie'
        self._finish([selected['node']], observed, reason, used=used)
        return selected

    def group(self, job_id, groups, count, baseline):
        """Preserve baseline CPU/RAM profile, full buckets and exact world size.

        No combinatorial group enumeration: sort within existing compatibility
        buckets, then compare resource-equivalent complete groups only.
        """
        if not baseline:
            self._finish([], (), 'no_complete_group')
            return baseline
        if job_id != self.job_id:
            self._finish([n[3] for n in baseline], (), 'job_context_mismatch')
            return baseline
        complete = [nodes for nodes in groups.values() if len(nodes) >= count]
        workers = tuple(n[2] for nodes in complete for n in nodes)
        observed, reason = self._read(workers, count)
        chosen = baseline
        used = reason == 'verified_missing_bytes'
        if used:
            missing = {r.worker_id: r.missing_bytes for r in observed}
            profile = tuple(n[:2] for n in baseline)
            choices = [sorted(nodes, key=lambda n: (n[0], n[1], missing[n[2]], n[2]))[:count]
                       for nodes in complete]
            equivalent = [nodes for nodes in choices if tuple(n[:2] for n in nodes) == profile]
            chosen = min(equivalent, key=lambda nodes: sum(missing[n[2]] for n in nodes))
        self._finish([n[3] for n in chosen], observed, reason, used=used)
        return chosen
