"""10.3 deterministic placement acceptance; no Pi, listener, mount or trainer.

The real-byte test needs ~1.504 GB plus metadata in a disposable directory.
Prepared job manifests/resource telemetry are fixtures; object publication,
SHA verification, signed inventory, registry and selectors execute real code.
Queue tests mock inventory/launch and use real queue policy + SQLite transitions.
"""
from collections import Counter
from contextlib import redirect_stdout
from copy import deepcopy
from dataclasses import replace
import hashlib
import io
import itertools
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import test_scheduler_locality as support
from test_cache_registry_acceptance import RepeatedStream, MB, BOOT
import job_store as store
import dispatcher
import queue_policy
import scheduler
import distributed_dispatch as ddp
from hybrid_storage.cache_coverage import CoverageAPI
from hybrid_storage.cache_inventory import CacheInventory
from hybrid_storage.dataset_manifests import DatasetEntry
from hybrid_storage.inventory_reporter import InventoryReporter
from hybrid_storage.inputs import object_key
from hybrid_storage.locality import JobLocality
from hybrid_storage.worker_cache import WorkerCache
from hybrid_storage import inventory_protocol as protocol


class ExactMBTests(unittest.TestCase):
    def setUp(self):
        self.fixture = support.fixtures.CoverageTests(methodName='runTest')
        self.addCleanup(self.fixture.doCleanups)
        self.fixture.setUp()
        self.storage = self.fixture.storage
        self.registry = self.fixture.registry
        self.clock = self.fixture.fixture.clock

    def job(self, world, source, dataset):
        """Prepared metadata fixture pinned through the real binding validator.

        Do not copy gigabytes into fake upload workspaces; every referenced
        object is actually published/hashed, and bind_job verifies its bytes.
        This is deliberately not an upload/end-to-end execution test.
        """
        identity = f'exact-mb-{world}'
        raw = json.loads(self.storage.raw)
        spec = deepcopy(raw['execution_spec'])
        spec['execution'] = {'mode': 'single' if world == 1 else 'distributed', 'workers': world}
        raw.update(job_id=identity, execution_spec=spec)
        raw['data']['kind'] = 'directory'
        raw['data']['inventory'] = {'directories': [], 'files': [
            {'path': e.path, 'sha256': e.sha256, 'size': e.size_bytes} for e in dataset.entries]}
        body = json.dumps(raw, sort_keys=True).encode()
        directory = self.storage.base/'prepared-metadata'/identity
        directory.mkdir(parents=True)
        (directory/'manifest.json').write_bytes(body)
        store.add_job(identity, 1, job_type='custom', owner='alice', status='RECEIVED',
                      job_config=spec, storage_backend='nfs')
        with self.storage.connect() as db:
            db.execute("UPDATE jobs SET status='HELD',storage_path=?,storage_manifest_sha256=? WHERE job_id=?",
                       (str(directory), hashlib.sha256(body).hexdigest(), identity))
        binding = self.registry.bind_job('alice', identity, legacy_manifest_bytes=body,
            source_sha256=source.sha256, dataset_id=dataset.dataset_id, dataset_version=dataset.version)
        return store.get_job(identity), binding

    def test_exact_MB_single_DDP_freshness_capacity_and_determinism(self):
        needed = 1504*MB + 128*1024*1024
        if shutil.disk_usage(self.storage.base).free < needed:
            self.skipTest('Exact MB fixture needs 1.638 GB free; never scale to bytes or fake presence')
        artifacts = [self.registry.register_artifact('alice', RepeatedStream(size, byte), expected_size=size)
                     for size, byte in ((500*MB, b'A'), (2*MB, b'B'))]
        dataset = self.registry.create_dataset('alice', [
            DatasetEntry('alias-large.bin', 'file', artifacts[0].sha256, 500*MB),
            DatasetEntry('large.bin', 'file', artifacts[0].sha256, 500*MB),
            DatasetEntry('small.bin', 'file', artifacts[1].sha256, 2*MB)])
        source = self.storage.source(self.registry)
        jobs = {world: self.job(world, source, dataset) for world in (1, 2, 3)}
        self.fixture.owner.close()
        keys = [self.fixture.fixture.key, self.fixture.fixture.key2,
                protocol.Credential('pi03', 'c'*32, b'3'*32)]
        owner = CacheInventory(self.storage.connect, keys, configured_workers=[k.worker for k in keys],
                               state_root=self.storage.base, clock=self.clock)
        owner.start(); self.addCleanup(owner.close)
        api = CoverageAPI(self.registry, owner)
        reporters, caches = {}, {}
        for index, key in enumerate(keys):
            owner.enroll(key.worker)
            root = self.storage.base/(key.worker+'-placement-cache')
            root.mkdir(mode=0o700)
            cache = WorkerCache(root, input_mount=self.storage.base/'unused-nfs')
            cache.initialize(); caches[key.worker] = cache
            required = {f.sha256: (f.size_bytes, object_key(f.sha256)) for f in source.files}
            for artifact in artifacts[:2-index] if index < 2 else ():
                required[artifact.sha256] = (artifact.size_bytes, artifact.key)
            for digest, (size, key_path) in required.items():
                original = self.storage.objects/key_path
                target = root/'objects'/key_path
                target.parent.parent.mkdir(mode=0o700, exist_ok=True)
                target.parent.mkdir(mode=0o700, exist_ok=True)
                with original.open('rb') as src, target.open('xb') as dst:
                    shutil.copyfileobj(src, dst, length=1024*1024)
                    dst.flush(); os.fsync(dst.fileno())
                target.chmod(0o400)
                self.assertNotEqual(original.stat().st_ino, target.stat().st_ino)
                cache.verify(digest, size, force=True)
            reporter = InventoryReporter(cache, key, clock=self.clock, monotonic=self.clock.monotonic,
                                         full_interval_seconds=1)
            reporter.initialize(boot_id=BOOT); self.addCleanup(reporter.close)
            reporters[key.worker] = reporter

        def publish(worker):
            self.clock.advance(1)
            request, headers = owner.challenge(worker)
            body, mac = reporters[worker].reply(request, headers['X-DL-Inventory-MAC'], headers['X-DL-Worker-Key'])
            self.assertEqual(owner.accept(worker, request, body, mac), 'FULL')

        for key in keys: publish(key.worker)
        nodes = [{'id': key.worker, 'ip': f'192.0.2.{i+1}', 'port': 5000} for i, key in enumerate(keys)]
        resources = {n['id']: support.resource() for n in nodes}
        required_source = sum({f.sha256: f.size_bytes for f in source.files}.values())
        expected = [0, 2*MB, 502*MB]
        job, binding = jobs[1]
        rows = api.for_job_workers('alice', tuple(k.worker for k in keys), job['job_id'])
        self.assertEqual([r.missing_bytes for r in rows], expected)
        self.assertEqual([r.required_bytes for r in rows], [502*MB+required_source]*3)
        self.assertTrue(all(r.inventory_eligible for r in rows))
        self.assertEqual([r.verified_cached_bytes for r in rows],
                         [502*MB+required_source, 500*MB+required_source, required_source])
        self.assertEqual([r.required_objects for r in rows], [len({f.sha256 for f in source.files})+2]*3)

        def select(world=1, *, order=None, locality=True, reserved=()):
            job, binding = jobs[world]
            preference = JobLocality(api, 'alice', job['job_id'], binding.sha256) if locality else None
            ordered = order if order is not None else list(reversed(nodes))
            target = scheduler if world == 1 else ddp
            with patch.object(target, 'load_nodes', return_value=ordered), \
                    patch.object(target, 'get_resources', side_effect=lambda n: resources[n['id']]), \
                    redirect_stdout(io.StringIO()):
                if world == 1:
                    selected = scheduler.select_best_node(requirements=job_spec(job)['resources_per_worker'],
                        reserved_nodes=set(reserved), locality=preference)
                    result = [] if selected is None else [selected['node']['id']]
                else:
                    prepared = [{'node_id': n['id'], 'record': {'compatibility_key': 'same'}} for n in nodes]
                    with patch.object(ddp, 'get_job_environments', return_value=prepared), \
                            patch.object(ddp, 'get_reservations', return_value=[{'node_id': w} for w in reserved]), \
                            patch.object(ddp.ownership, 'supported', side_effect=lambda s: s.get('lease', True)):
                        selected = ddp.select_group(job, set(), require_ownership=True, locality=preference)
                    result = [n['id'] for n in selected or []]
            return result, preference.snapshot() if preference else None

        with self.subTest('all_inventory_permutations'):
            before = list(self.fixture.db.iterdump())
            for order in itertools.permutations(nodes):
                self.assertEqual(select(order=list(order))[0], ['pi01'])
                self.assertEqual(select(order=list(order), locality=False)[0], [order[0]['id']])
                for world, total in ((2, 2*MB), (3, 504*MB)):
                    selected, view = select(world, order=list(order))
                    self.assertEqual(selected, ['pi01', 'pi02', 'pi03'][:world])
                    self.assertEqual(view.document()['total_missing_bytes'], total)
            self.assertEqual(list(self.fixture.db.iterdump()), before)
        with self.subTest('hard_CPU_and_RAM_filters'):
            for component, field, value in [('cpu', 'available_cores', 0), ('memory', 'available_gb', 0)]:
                original = deepcopy(resources['pi01'])
                resources['pi01'][component][field] = value
                selected, view = select()
                self.assertEqual(selected, ['pi02'])
                self.assertNotIn('pi01', {r.worker_id for r in view.workers})
                resources['pi01'] = original
        with self.subTest('two_and_three_rank_staging_byte_sums'):
            selected, two = select(2)
            self.assertEqual(selected, ['pi01', 'pi02'])
            self.assertEqual(two.per_rank_missing_bytes, (0, 2*MB))
            self.assertEqual(two.document()['total_missing_bytes'], 2*MB)
            selected, three = select(3)
            self.assertEqual(selected, ['pi01', 'pi02', 'pi03'])
            self.assertEqual(three.per_rank_missing_bytes, tuple(expected))
            self.assertEqual(three.document()['total_missing_bytes'], 504*MB)
            self.assertEqual([r.required_bytes for r in three.workers], [502*MB+required_source]*3)
        with self.subTest('DDP_capacity_and_reservation_are_per_rank'):
            for component, field, value in [('cpu', 'available_cores', 0), ('memory', 'available_gb', 0)]:
                original = deepcopy(resources['pi01'])
                resources['pi01'][component][field] = value
                self.assertEqual(select(3)[0], [])
                result, view = select(2)
                self.assertEqual(result, ['pi02', 'pi03'])
                self.assertEqual(view.document()['total_missing_bytes'], 504*MB)
                resources['pi01'] = original
            self.assertEqual(select(3, reserved=['pi01'])[0], [])
            resources['pi01']['lease'] = False
            self.assertEqual(select(3)[0], [])
            del resources['pi01']['lease']
        with self.subTest('stale_inventory_is_unknown_and_resource_fallback'):
            self.clock.advance(16)
            result, view = select()
            self.assertEqual(result, ['pi03'])
            self.assertEqual(view.policy, 'resource')
            self.assertIsNone(view.per_rank_missing_bytes)
            self.assertTrue(all(not r.inventory_eligible and r.missing_bytes is None for r in view.workers))
            self.assertEqual(select(3)[0], ['pi01', 'pi02', 'pi03'])
            self.assertIsNone(select(3)[1].per_rank_missing_bytes)
        with self.subTest('missing_inventory_session_not_a_verified_cold_or_warm_cache'):
            for key in keys: publish(key.worker)
            owner.offline('pi01')
            result, view = select()
            self.assertEqual(result, ['pi03'])
            missing = next(r for r in view.workers if r.worker_id == 'pi01')
            self.assertFalse(missing.inventory_eligible)
            self.assertIsNone(missing.missing_bytes)
            self.assertIsNone(view.document()['total_missing_bytes'])
        with self.subTest('missing_verified_object_row_never_counts_a_hit'):
            publish('pi01')
            with self.fixture.db:
                self.fixture.db.execute('DELETE FROM cache_registry_entries WHERE worker_id=? AND sha256=?',
                                        ('pi01', artifacts[0].sha256))
            result, view = select()
            self.assertEqual(result, ['pi02'])
            self.assertEqual(next(r.missing_bytes for r in view.workers if r.worker_id == 'pi01'), 500*MB)
            publish('pi01')
            self.assertEqual(select()[0], ['pi01'])
        print('LOCALITY_EXACT_MB '+json.dumps({'MB': MB, 'required_input_bytes': 502*MB+required_source,
            'missing_bytes': dict(zip(('pi01','pi02','pi03'), expected)), 'two_rank_missing_bytes': 2*MB,
            'three_rank_missing_bytes': 504*MB, 'inventory_permutations': 6,
            'storage': 'real distinct local inodes; no sparse/hardlink/synthetic presence',
            'scope': 'fake prepared manifests/resources; signed inventory and registry selectors real'}))


def job_spec(job):
    return json.loads(job['job_config'])


class QueueAcceptanceTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='locality-queue-')
        self.addCleanup(self.temporary.cleanup)
        self.base = Path(self.temporary.name)
        self.now = 500
        self.policy = {**queue_policy.DEFAULTS, 'max_pending_per_owner': 20}
        for mocked in (patch.object(queue_policy, 'load_policy', return_value=self.policy),
                       patch.object(store, 'load_policy', return_value=self.policy),
                       patch.object(store.time, 'time', side_effect=lambda: self.now),
                       patch('job_timing.write_report')):
            mocked.start(); self.addCleanup(mocked.stop)
        self.nodes = [{'id': f'pi0{i}', 'ip': f'192.0.2.{i}', 'port': 5000} for i in (3, 2, 1)]

    def database(self, name):
        store.DB_FILE = str(self.base/(name+'.db'))
        store.initialize_database()

    def add(self, identity, owner, *, queued=1, workers=1, gpus=0):
        spec = {'execution': {'mode': 'single' if workers == 1 else 'distributed', 'workers': workers},
                'recovery': {'mode': 'restart'},
                'resources_per_worker': {'cpu_cores': 1, 'memory_gb': 1, 'gpus': gpus}}
        store.add_job(identity, 1, job_type='custom', owner=owner, status='RECEIVED', job_config=spec)
        with store.get_connection() as db:
            db.execute("UPDATE jobs SET status='QUEUED',created_at=?,queued_at=? WHERE job_id=?", (queued, queued, identity))
            for node in self.nodes:
                db.execute('INSERT INTO job_environments VALUES(?,?,?,?,?,?,?)',
                           (identity, node['id'], 'fixture', 'environment', '{"compatibility_key":"same"}', 1, 1))

    def run_pass(self, locality):
        started, attempted = [], []

        def dispatch(job):
            attempted.append(job['job_id'])
            spec = job_spec(job)
            preference = None
            if locality:
                api = Mock(spec=CoverageAPI)
                missing = 502*MB if job['owner'] == 'cold' else 0
                def batch(actor, workers, job_id, *, rank=None):
                    return tuple(replace(support.coverage(w, 0, rank=rank,
                        world=spec['execution']['workers'] if rank is not None else None),
                        target_id=job_id, required_bytes=502*MB, verified_cached_bytes=502*MB-missing,
                        missing_bytes=missing, locality_ratio=0.0 if missing else 1.0) for w in workers)
                api.for_job_workers.side_effect = batch
                preference = JobLocality(api, job['owner'], job['job_id'], support.SHA)
            if spec['execution']['workers'] > 1:
                workers = ddp.select_group(job, set(), require_ownership=True, locality=preference)
                if workers is None:return False
                context = {'world_size': len(workers), 'master_addr': workers[0]['ip'], 'master_port': 29611}
            else:
                selected = scheduler.select_best_node(requirements=spec['resources_per_worker'],
                    reserved_nodes={r['node_id'] for r in store.get_reservations()}, locality=preference)
                if selected is None:return False
                workers, context = [selected['node']], None
            store.mark_scheduled(job['job_id'], workers[0]['id'], workers[0]['ip'],
                                 spec['resources_per_worker'], workers if context else None, context)
            started.append(job['job_id'])
            return True

        with patch.object(dispatcher, 'dispatch_one_job', side_effect=dispatch), \
                patch.object(scheduler, 'load_nodes', return_value=self.nodes), \
                patch.object(scheduler, 'get_resources', return_value=support.resource()), \
                patch.object(ddp, 'load_nodes', return_value=self.nodes), \
                patch.object(ddp, 'get_resources', return_value=support.resource()), \
                patch.object(ddp.ownership, 'supported', return_value=True), redirect_stdout(io.StringIO()):
            dispatcher.dispatch_queued_jobs()
        reservations = store.get_reservations()
        self.assertEqual(len({r['node_id'] for r in reservations}), len(reservations))
        counts = Counter(j['owner'] for j in store.get_all_jobs() if j['status'] in queue_policy.ACTIVE_STATES)
        self.assertTrue(all(n <= self.policy['max_active_per_owner'] for n in counts.values()))
        return started, attempted

    def complete(self, identities):
        for identity in identities:
            store.mark_running(identity, self.now)
            self.assertTrue(store.mark_completed(identity, finished_at=self.now+1))

    def test_continuous_warm_arrivals_cannot_preempt_aged_cold_FIFO(self):
        traces = []
        original_db = store.DB_FILE
        self.addCleanup(setattr, store, 'DB_FILE', original_db)
        for policy in (False, True, True):
            self.now = 500
            self.database('queue-'+str(len(traces)))
            self.add('cold-1', 'cold', queued=1); self.add('cold-2', 'cold', queued=2)
            self.add('hot-1', 'hot', queued=400)
            trace, attempted_trace, cold_rounds = [], [], {}
            for tick in range(8):
                self.add(f'hot-new-{tick}', 'hot', queued=self.now)
                # Re-open the existing DB without resetting service turns/slots.
                store.initialize_database()
                started, attempted = self.run_pass(policy)
                trace.extend(started); attempted_trace.append(attempted)
                for identity in started:
                    if identity.startswith('cold'):cold_rounds[identity] = tick
                self.complete(started)
                self.now += 1
            self.assertEqual(cold_rounds, {'cold-1': 0, 'cold-2': 1})
            self.assertEqual(trace[0], 'cold-1')
            self.assertEqual([j for j in trace if j.startswith('cold')], ['cold-1', 'cold-2'])
            self.assertEqual(len(trace), 10)
            traces.append((trace, attempted_trace))
        self.assertEqual(traces[0], traces[1]); self.assertEqual(traces[1], traces[2])
        print('LOCALITY_QUEUE '+json.dumps({'arrival_rounds':8, 'cold_start_rounds':[0,1],
            'dispatches':len(traces[0][0]), 'resource_locality_repeated_traces_equal':True,
            'scope':'controlled queue simulation; inventory and launch mocked'}))

    def test_infeasible_cold_head_does_not_block_other_owners(self):
        original_db = store.DB_FILE; self.addCleanup(setattr, store, 'DB_FILE', original_db)
        for mode in (False, True):
            self.database('infeasible-'+str(mode))
            self.add('impossible', 'cold', gpus=1); self.add('feasible', 'hot', queued=400)
            started, attempted = self.run_pass(mode)
            self.assertEqual(started, ['feasible'])
            self.assertEqual(attempted, ['impossible', 'feasible'])
            self.assertEqual(store.get_job('impossible')['status'], 'QUEUED')
            self.assertEqual([r['job_id'] for r in store.get_reservations()], ['feasible'])

    def test_uncertain_and_cancelling_jobs_keep_quota_until_safe_release(self):
        original_db = store.DB_FILE; self.addCleanup(setattr, store, 'DB_FILE', original_db)
        self.database('quotas')
        self.add('cold-1', 'cold'); self.add('cold-2', 'cold', queued=2)
        self.assertEqual(self.run_pass(True)[0], ['cold-1'])
        store.initialize_database()
        self.add('warm-1', 'hot', queued=400)
        self.assertEqual(self.run_pass(True)[0], ['warm-1'])
        self.assertEqual(store.get_job('cold-1')['status'], 'SCHEDULED')
        store.control_job('cold-1', 'cold', 'cancel')
        self.assertEqual(self.run_pass(True)[0], [])
        self.assertEqual(len(store.get_reservations()), 2)
        with self.assertRaisesRegex(ValueError, 'quota'):
            store.mark_scheduled('cold-2', 'pi01', '192.0.2.1')
        store.finish_cancellation('cold-1')  # Simulated confirmed quiescence.
        self.assertEqual(self.run_pass(True)[0], ['cold-2'])

    def test_DDP_one_owner_slot_multiple_exclusive_workers_never_partial(self):
        original_db = store.DB_FILE; self.addCleanup(setattr, store, 'DB_FILE', original_db)
        self.database('gang')
        self.add('cold-group', 'cold', workers=2)
        self.add('cold-next', 'cold', queued=2)
        self.add('warm-group', 'hot', workers=2, queued=400)
        self.add('other-single', 'other', queued=401)
        started, attempted = self.run_pass(True)
        self.assertEqual(started, ['cold-group', 'other-single'])
        self.assertEqual(attempted, ['cold-group', 'warm-group', 'other-single'])
        self.assertEqual(Counter(r['job_id'] for r in store.get_reservations()),
                         {'cold-group': 2, 'other-single': 1})
        self.assertEqual(len(store.get_ranks('cold-group')), 2)
        self.assertEqual(store.get_ranks('warm-group'), [])
        self.assertEqual(store.get_job('warm-group')['status'], 'QUEUED')
        self.assertEqual(store.get_job('cold-next')['status'], 'QUEUED')
        self.assertNotIn('cold-next', [j['job_id'] for j in store.fair_queue()])


if __name__ == '__main__':
    unittest.main(verbosity=2)
