"""Optional staging-copy cost, never training completion time or authority.

Authenticated inventory supplies the observations; unknown facts restore the
resource selector. Resource profiles, group compatibility and fairness survive.
"""
from copy import deepcopy
from dataclasses import dataclass
import math
import time

from .locality import JobLocality, LocalityDecision

FIELDS = frozenset(('schema_version', 'sample_count', 'max_age_seconds', 'oldest_age_seconds',
    'median_bytes_per_second', 'min_bytes_per_second', 'max_bytes_per_second',
    'median_overhead_seconds', 'min_overhead_seconds', 'max_overhead_seconds',
    'min_payload_bytes', 'max_payload_bytes', 'generated_at_ns'))


@dataclass(frozen=True)
class BandwidthDecision(LocalityDecision):
    def document(self):
        value=super().document();value['requested_policy']='bandwidth'
        return value


class TransferObservations:
    """Controller-local volatile store. Only CacheInventory's verified accept calls
    update it; no HTTP ingestion, DB migration, startup probe or persistence.
    Repeated challenge replies do not refresh age. Inventory still gates liveness.
    """
    def __init__(self, *, max_age_seconds=900, min_samples=3, clock=time.monotonic):
        if type(max_age_seconds) is not int or not 1 <= max_age_seconds <= 3600:
            raise ValueError('Invalid controller transfer TTL')
        if type(min_samples) is not int or not 3 <= min_samples <= 4096:
            raise ValueError('At least three measured copies are required')
        if not callable(clock):raise ValueError('Expected monotonic clock')
        self.max_age, self.min_samples, self.clock = max_age_seconds, min_samples, clock
        self._latest = {}

    def forget(self, worker=None):
        if worker is None:self._latest.clear()
        else:self._latest.pop(worker, None)

    def accept(self, worker, session, sequence, value, now_ns):
        """Private trusted ingestion seam; invalid/missing data clears old certainty."""
        self.forget(worker)
        if type(value) is not dict or set(value) != FIELDS:return
        if type(value['schema_version']) is not int or value['schema_version'] != 1:return
        if type(value['sample_count']) is not int or not self.min_samples <= value['sample_count'] <= 4096:return
        if type(value['generated_at_ns']) is not int or abs(now_ns-value['generated_at_ns']) > 30_000_000_000:return
        for name in FIELDS-{'schema_version','sample_count','generated_at_ns'}:
            number=value[name]
            if type(number) not in (int,float) or not math.isfinite(number) or not 0 <= number <= 1e30:return
        if not 0 < value['max_age_seconds'] <= 86400:return
        if not 1e-9 <= value['min_bytes_per_second'] <= value['median_bytes_per_second'] <= value['max_bytes_per_second']:return
        if not value['min_overhead_seconds'] <= value['median_overhead_seconds'] <= value['max_overhead_seconds']:return
        if any(type(value[k]) is not int for k in ('min_payload_bytes','max_payload_bytes')):return
        if not 0 < value['min_payload_bytes'] <= value['max_payload_bytes'] <= 2**63-1:return
        # Conservative bound for the existing challenge's permitted 30s clock
        # skew. Monotonic receiver age continues even without more heartbeats.
        age=value['oldest_age_seconds']+max(0,now_ns-value['generated_at_ns'])/1e9+30
        if age > min(self.max_age,value['max_age_seconds']):return
        self._latest[worker]=(session,sequence,deepcopy(value),self.clock(),age)

    def read(self, worker, session, sequence):
        row=self._latest.get(worker)
        if row is None or row[:2] != (session,sequence):return None
        elapsed=self.clock()-row[3]
        if not math.isfinite(elapsed) or elapsed < 0:return None
        value=deepcopy(row[2]);age=row[4]+elapsed
        if age > min(self.max_age,value['max_age_seconds']):return None
        value.update(age_upper_seconds=age, min_samples=self.min_samples,
                     clock_skew_bound_seconds=30)
        return value


class JobBandwidth(JobLocality):
    """Same resource-equivalent tie surface as JobLocality, with measured costs.

    Full-input ranks only. Group estimate uses bounded greedy scheduling waves
    (the current rank-order stage pool), not sum-as-parallel-time or independent
    unlimited NFS links. Contention multiplier is an explicit conservative model
    assumption, not measured aggregate server throughput.
    """
    def __init__(self, *args, stage_parallelism=3):
        super().__init__(*args)
        if type(stage_parallelism) is not int or not 1 <= stage_parallelism <= 32:
            raise ValueError('Invalid bounded stage parallelism')
        self.parallelism=stage_parallelism
        self._retain_coverage_rows=True
        self.prediction=None

    def _estimates(self, observed):
        estimates=[];failure=None
        if not observed or not all(r.inventory_eligible for r in observed):return {}, 'inventory_unavailable_or_stale'
        for row, exact in zip(observed,self._coverage_rows):
            facts=self.coverage.inventory.bandwidth_facts(row.worker_id,row.inventory_session_id,row.inventory_sequence)
            count=len(set(exact.missing_hashes))  # Unique, including empty objects.
            if row.missing_bytes and count == 0:raise ValueError('Missing object count unavailable')
            minimum_space=row.required_bytes+row.missing_bytes
            entry=dict(worker_id=row.worker_id,missing_bytes=row.missing_bytes,missing_objects=count,
                disk_available_bytes=None if facts is None else facts['disk_available_bytes'],minimum_space_bytes=minimum_space,
                capacity_status='disk_only_quota_and_workspace_limits_unverified',
                rate=None,seconds=None,lower_seconds=None,upper_seconds=None)
            estimates.append(entry)
            if facts is None:
                entry['capacity_status']='inventory_unknown';failure=failure or 'transfer_inventory_unavailable'
                continue
            if facts['disk_available_bytes'] < minimum_space:
                entry['capacity_status']='insufficient_reported_disk'
                failure=failure or 'reported_disk_capacity_insufficient';continue
            # A warm set may still need verified empty objects copied. Do not
            # fabricate their cost from absent throughput samples.
            if count == 0:
                entry.update(seconds=0.0,lower_seconds=0.0,upper_seconds=0.0)
            else:
                rate=facts['transfer'];entry['rate']=rate
                if rate is None:
                    failure=failure or 'transfer_samples_unavailable_or_stale';continue
                entry['average_missing_object_bytes']=row.missing_bytes/count
                entry['average_size_extrapolation'] = not rate['min_payload_bytes'] <= row.missing_bytes/count <= rate['max_payload_bytes']
                entry.update(seconds=row.missing_bytes/rate['median_bytes_per_second']+count*rate['median_overhead_seconds'],
                    lower_seconds=row.missing_bytes/rate['max_bytes_per_second']+count*rate['min_overhead_seconds'],
                    upper_seconds=row.missing_bytes/rate['min_bytes_per_second']+count*rate['max_overhead_seconds'])
        self._estimates_document=estimates
        if failure:return {},failure
        return {e['worker_id']:e for e in estimates}, 'measured_copy_cost'

    def _cost(self, ids, estimates):
        # Slowdown up to the active group size models a shared saturated server.
        # Use that conservative scalar consistently for ranking and logging.
        concurrency=min(len(ids),self.parallelism)
        def waves(field,multiplier):
            slots=[0.0]*concurrency
            for worker in ids:
                index=min(range(len(slots)),key=slots.__getitem__)
                slots[index]+=estimates[worker][field]*multiplier
            return max(slots)
        predicted=waves('seconds',concurrency)
        return dict(seconds=predicted,
                    lower_seconds=min(predicted,waves('lower_seconds',1)),
                    upper_seconds=max(predicted,waves('upper_seconds',concurrency)),
                    assumed_concurrent_transfers=concurrency)

    def _finish_bandwidth(self,nodes,observed,reason,estimates=None):
        used=reason=='measured_copy_cost'
        super()._finish(nodes,observed,reason,used=False)
        decision=self._decision
        self._decision=BandwidthDecision('bandwidth' if used else 'resource',decision.reason,
            decision.workers,decision.selected_nodes,decision.per_rank_missing_bytes)
        ids=[n['id'] for n in nodes]
        self.prediction=dict(scope='missing_input_copy_to_verified_cache_only',
            formula='missing_bytes / measured_transfer_Bps + missing_objects * measured_residual_seconds',
            overhead_assumption='per_object_residual_assumed_fixed; includes_size_dependent_parent_hash',
            uncertainty='observed_extrema_not_confidence_interval; size_extrapolation_and_shared_NFS_contention_unknown',
            excludes=['training','checkpoint_restore','workspace_copy','cache_hit_verification','queue_wait','failed_attempts'],
            workers=getattr(self,'_estimates_document',[]),
            selected_estimate=self._cost(ids,estimates) if used else None,
            contention_model='rank_order_greedy_waves; conservative_parallelism_multiplier; not_measured_server_capacity',
            cluster_active_transfers=None)

    def single(self,candidates):
        self._estimates_document=[]
        if not candidates:
            self._finish_bandwidth([],(),'no_eligible_workers');return None
        observed,reason=self._read(tuple(c['node']['id'] for c in candidates),1)
        chosen=candidates[0];estimates={}
        try:
            if reason=='verified_missing_bytes':estimates,reason=self._estimates(observed)
            if reason=='measured_copy_cost':
                tied=[c for c in candidates if (c['cpu'],c['ram'])==(chosen['cpu'],chosen['ram'])]
                chosen=min(tied,key=lambda c:estimates[c['node']['id']]['seconds'])
        except Exception:reason='transfer_facts_unavailable';estimates={}
        self._finish_bandwidth([chosen['node']],observed,reason,estimates)
        return chosen

    def group(self,job_id,groups,count,baseline):
        self._estimates_document=[]
        if not baseline:
            self._finish_bandwidth([],(),'no_complete_group');return baseline
        if job_id!=self.job_id:
            self._finish_bandwidth([n[3] for n in baseline],(),'job_context_mismatch');return baseline
        complete=[nodes for nodes in groups.values() if len(nodes)>=count]
        observed,reason=self._read(tuple(n[2] for nodes in complete for n in nodes),count)
        chosen=baseline;estimates={}
        try:
            if reason=='verified_missing_bytes':estimates,reason=self._estimates(observed)
            if reason=='measured_copy_cost':
                profile=tuple(n[:2] for n in baseline)
                choices=[sorted(nodes,key=lambda n:(n[0],n[1],estimates[n[2]]['seconds'],n[2]))[:count]
                         for nodes in complete]
                equivalent=[nodes for nodes in choices if tuple(n[:2] for n in nodes)==profile]
                chosen=min(equivalent,key=lambda nodes:self._cost([n[2] for n in nodes],estimates)['seconds'])
        except Exception:reason='transfer_facts_unavailable';estimates={};chosen=baseline
        self._finish_bandwidth([n[3] for n in chosen],observed,reason,estimates)
        return chosen
