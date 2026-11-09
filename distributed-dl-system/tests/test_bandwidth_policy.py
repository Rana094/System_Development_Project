"""11.2 measured-copy arithmetic, authenticated inventory and policy; no live NFS/Pis."""
from contextlib import redirect_stdout
from copy import deepcopy
from dataclasses import replace
import io
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import Mock, patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
import test_scheduler_policy as support
import test_cache_inventory as auth_fixture
import test_transfer_metrics as copy_fixture
import scheduler
import distributed_dispatch as ddp
from hybrid_storage.cache_coverage import CoverageAPI
from hybrid_storage import cache_registry_schema
from hybrid_storage.config import load_config, require_staging_config
from hybrid_storage.staging_protocol import STAGE_PATH, START_PATH
from staging_dispatch import StagingDispatch
from hybrid_storage.bandwidth import JobBandwidth, TransferObservations
from hybrid_storage.cache_inventory import CacheInventory
from hybrid_storage.config import StorageConfig, require_controller_config, StorageConfigError
from hybrid_storage.inventory_reporter import InventoryReporter
from hybrid_storage import inventory_protocol as p
from hybrid_storage.scheduling import SchedulingPolicy
from hybrid_storage.transfer_metrics import TransferMetrics

BANDWIDTH=StorageConfig('nfs',True,True,'bandwidth')


def measured(rate=100,overhead=.2,*,clock=lambda:100):
    metrics=TransferMetrics('pi01',clock=clock,wall_clock=lambda:1000)
    for _ in range(3):
        transfer=int(1000/rate*1e9)
        metrics.record(1000,transfer+int(overhead*1e9),transfer_ns=transfer)
    doc=metrics.prediction_snapshot();doc['generated_at_ns']=1_000_000_000_000
    return doc


def usable(rate=100,overhead=.2):
    store=TransferObservations(clock=lambda:100)
    store.accept('pi01','sid',1,measured(rate,overhead),1_000_000_000_000)
    return store.read('pi01','sid',1)


class MeasurementTests(unittest.TestCase):
    def test_decomposition_avoids_double_counting_inclusive_rate(self):
        metrics=TransferMetrics('pi01')
        metrics.record(1000,12_000_000_000,transfer_ns=10_000_000_000)
        doc=metrics.prediction_snapshot()
        self.assertEqual(doc['median_bytes_per_second'],100)
        self.assertEqual(doc['median_overhead_seconds'],2)
        self.assertAlmostEqual(metrics.snapshot()['median_bytes_per_second'],1000/12)
        self.assertEqual(1000/doc['median_bytes_per_second']+doc['median_overhead_seconds'],12)
        for value in (True,0,-1,13_000_000_000):
            with self.subTest(value=value),self.assertRaises(ValueError):
                metrics.record(1000,12_000_000_000,transfer_ns=value)

    def test_inclusive_only_samples_do_not_become_predictive_calibration(self):
        metrics=TransferMetrics('pi01')
        for _ in range(3):metrics.record(1000,12_000_000_000)
        self.assertEqual(metrics.snapshot()['sample_count'],3)
        self.assertEqual(metrics.prediction_snapshot()['sample_count'],0)
        self.assertIsNone(metrics.prediction_snapshot()['median_overhead_seconds'])

    def test_store_bounds_sample_count_age_skew_and_monotonic_replay(self):
        now=[100.0];store=TransferObservations(clock=lambda:now[0])
        doc=measured()
        store.accept('pi01','sid',1,doc,1_000_000_000_000)
        self.assertEqual(store.read('pi01','sid',1)['age_upper_seconds'],30)
        self.assertIsNone(store.read('pi01','other',1))
        self.assertIsNone(store.read('pi01','sid',2))
        now[0]+=871
        self.assertIsNone(store.read('pi01','sid',1))
        for mutation in ({'sample_count':2},{'median_bytes_per_second':True},
                         {'oldest_age_seconds':901},{'generated_at_ns':1},
                         {'median_overhead_seconds':-1},{'extra':'secret'},
                         {'max_payload_bytes':0},{'min_bytes_per_second':float('nan')}):
            with self.subTest(mutation=mutation):
                value=dict(doc,**mutation)
                store.accept('pi01','sid',1,value,1_000_000_000_000)
                self.assertIsNone(store.read('pi01','sid',1))
        for kwargs in ({'min_samples':1},{'max_age_seconds':0},{'clock':None}):
            with self.subTest(kwargs=kwargs),self.assertRaises(ValueError):TransferObservations(**kwargs)

    def test_single_peak_does_not_override_median_overhead_or_rate(self):
        metrics=TransferMetrics('pi01')
        for rate,overhead in ((10,1),(10,1),(1000,0)):
            io_ns=int(1000/rate*1e9)
            metrics.record(1000,io_ns+int(overhead*1e9),transfer_ns=io_ns)
        doc=metrics.prediction_snapshot()
        self.assertEqual(doc['median_bytes_per_second'],10)
        self.assertEqual(doc['median_overhead_seconds'],1)
        self.assertEqual((doc['min_bytes_per_second'],doc['max_bytes_per_second']),(10,1000))


class CopyIntervalTests(unittest.TestCase):
    setUp=copy_fixture.TransferCopyTests.setUp
    fetcher_handle=copy_fixture.TransferCopyTests.fetcher_handle
    source_file=copy_fixture.TransferCopyTests.source_file
    fake_spawn=copy_fixture.TransferCopyTests.fake_spawn
    ensure=copy_fixture.TransferCopyTests.ensure
    fake_nfs=copy_fixture.TransferCopyTests.fake_nfs

    def test_real_helper_intervals_are_parent_bounded_and_feed_calibration(self):
        metrics=TransferMetrics('pi01')
        handle=self.fetcher_handle(transfer_metrics=metrics)
        files=[]
        from hybrid_storage.inputs import InputFile
        for i in range(3):
            data=self.data+str(i).encode();_,sha=self.source_file(data)
            files.append(InputFile(str(i),sha,len(data)))
        with self.fake_nfs(handle):self.ensure(files,handle=handle)
        doc=metrics.prediction_snapshot()
        self.assertEqual(doc['sample_count'],3)
        self.assertGreater(doc['median_bytes_per_second'],0)
        self.assertGreaterEqual(doc['min_overhead_seconds'],0)
        self.assertEqual(doc['min_payload_bytes'],len(self.data)+1)
        with self.fake_nfs(handle):self.ensure(files,handle=handle)
        self.assertEqual(metrics.prediction_snapshot()['sample_count'],3)


class BandwidthPolicyTests(unittest.TestCase):
    setUp=support.PolicyTests.setUp
    single=support.PolicyTests.single

    def configure(self,parallelism=3,logger=None):
        self.missing={'pi03':80,'pi02':50,'pi01':10}
        self.api.inventory=Mock(spec=CacheInventory)
        self.rates={'pi03':usable(10),'pi02':usable(100),'pi01':usable(1)}
        self.api.for_job_workers.side_effect=self.batch
        self.api.inventory.bandwidth_facts.side_effect=self.facts
        return SchedulingPolicy(self.registry,settings=BANDWIDTH,coverage=self.api,
                                stage_parallelism=parallelism,choice_logger=logger)

    def batch(self,actor,workers,job_id,*,rank=None):
        return tuple(replace(support.support.coverage(w,self.missing[w],rank=rank,
            world=self.world if rank is not None else None),target_id=job_id,
            missing_hashes=('d'*64,) if self.missing[w] else ()) for w in workers)

    def facts(self,worker,session,sequence):
        return dict(disk_available_bytes=1024,transfer=self.rates[worker])

    def test_opt_in_bandwidth_can_prefer_more_bytes_on_faster_measured_link(self):
        policy=self.configure()
        self.assertEqual(self.single(policy)['node']['id'],'pi02')
        record=policy.history()[-1]
        self.assertEqual((record['policy'],record['reason']),('bandwidth','measured_copy_cost'))
        prediction=record['bandwidth']; selected=prediction['selected_estimate']
        self.assertAlmostEqual(selected['seconds'],.7)
        entries={r['worker_id']:r for r in prediction['workers']}
        self.assertAlmostEqual(entries['pi01']['seconds'],10.2)
        self.assertEqual(entries['pi02']['rate']['sample_count'],3)
        self.assertEqual(entries['pi02']['rate']['age_upper_seconds'],30)
        self.assertTrue(entries['pi02']['average_size_extrapolation'])
        self.assertIsNone(prediction['cluster_active_transfers'])
        for word in ('missing_hashes','generated_source','password','storage_path'):
            self.assertNotIn(word,json.dumps(record))
        json.dumps(record,allow_nan=False)

    def test_resource_locality_and_rapid_rollback_use_original_behavior(self):
        policy=self.configure()
        self.assertEqual(self.single(policy)['node']['id'],'pi02')
        policy.set_policy('locality')
        self.api.inventory.bandwidth_facts.reset_mock()
        self.assertEqual(self.single(policy)['node']['id'],'pi01')
        self.api.inventory.bandwidth_facts.assert_not_called()
        policy.set_policy('resource');self.api.for_job_workers.reset_mock()
        self.assertEqual(self.single(policy)['node']['id'],'pi03')
        self.api.for_job_workers.assert_not_called()
        policy.set_policy('bandwidth')
        self.assertEqual(self.single(policy)['node']['id'],'pi02')

    def test_hard_resource_filters_reservations_and_primary_resource_order_win(self):
        policy=self.configure()
        self.resources['pi02']['cpu']['available_cores']=0
        self.assertEqual(self.single(policy,requirements={'cpu_cores':1})['node']['id'],'pi03')
        self.resources['pi02']=support.support.resource(cpu=1,ram=1)
        self.assertEqual(self.single(policy)['node']['id'],'pi02')
        self.resources['pi01']=support.support.resource(cpu=0,ram=1)
        self.assertEqual(self.single(policy)['node']['id'],'pi01')
        self.assertEqual(self.single(policy,reserved_nodes={'pi01'},excluded_nodes={'pi02'})['node']['id'],'pi03')

    def test_unknown_stale_disk_and_failing_inventory_restore_resource_baseline(self):
        for reason in ('missing','stale','disk','exception'):
            with self.subTest(reason=reason):
                policy=self.configure()
                if reason in ('missing','stale'):self.rates['pi01']=None
                elif reason=='disk':self.api.inventory.bandwidth_facts.side_effect=lambda *a:dict(disk_available_bytes=0,transfer=usable())
                else:self.api.inventory.bandwidth_facts.side_effect=OSError('unavailable')
                self.assertEqual(self.single(policy)['node']['id'],'pi03')
                doc=policy.history()[-1]
                self.assertEqual(doc['policy'],'resource')
                self.assertIsNone(doc['bandwidth']['selected_estimate'])

    def test_all_warm_zero_copy_cost_without_invented_rates_and_empty_objects_need_samples(self):
        policy=self.configure();self.missing={w:0 for w in self.missing};self.rates={w:None for w in self.rates}
        self.assertEqual(self.single(policy)['node']['id'],'pi03')
        self.assertEqual(policy.history()[-1]['bandwidth']['selected_estimate']['seconds'],0)
        previous=self.api.for_job_workers.side_effect
        self.api.for_job_workers.side_effect=lambda *a,**kw:tuple(replace(r,missing_hashes=('e'*64,)) for r in previous(*a,**kw))
        self.assertEqual(self.single(policy)['node']['id'],'pi03')
        self.assertEqual(policy.history()[-1]['policy'],'resource')

    def test_DDP_full_group_uses_bounded_waves_and_capacity_not_independent_links(self):
        for world in (2,3):
            for parallelism in (1,3):
                with self.subTest(world=world,parallelism=parallelism):
                    policy=self.configure(parallelism);self.world=world
                    job=dict(self.job,job_config=json.dumps(dict(execution=dict(workers=world),resources_per_worker=dict(cpu_cores=1))))
                    prepared=[dict(node_id=n['id'],record=dict(compatibility_key='same')) for n in self.nodes]
                    with patch.object(ddp,'load_nodes',return_value=self.nodes),patch.object(ddp,'get_resources',side_effect=lambda n:self.resources[n['id']]), \
                         patch.object(ddp,'get_job_environments',return_value=prepared),patch.object(ddp,'get_reservations',return_value=[]):
                        result=policy.select(job,support.plans(job,world),ddp.select_group,job,set())
                    self.assertEqual(len(result),world)
                    self.assertEqual(result[0]['id'],'pi02')
                    estimate=policy.history()[-1]['bandwidth']['selected_estimate']
                    self.assertEqual(estimate['assumed_concurrent_transfers'],min(world,parallelism))
                    if world==2:self.assertAlmostEqual(estimate['seconds'],8.9 if parallelism==1 else 16.4)
                    self.assertGreaterEqual(estimate['upper_seconds'],estimate['lower_seconds'])

    def test_no_coverage_unsupported_plan_and_logging_failure_are_safe(self):
        policy=self.configure(logger=lambda _:(_ for _ in ()).throw(OSError('sink')))
        self.assertEqual(self.single(policy)['node']['id'],'pi02')
        policy.coverage=None
        self.assertEqual(self.single(policy)['node']['id'],'pi03')
        self.assertIsNone(policy.history()[-1]['bandwidth'])
        with self.assertRaises(StorageConfigError):require_controller_config(BANDWIDTH)

    def test_component_sample_opt_in_and_ordinary_startup_still_blocked(self):
        self.assertEqual(require_staging_config(load_config(ROOT/'config/storage-examples/nfs-bandwidth.json')),BANDWIDTH)
        self.assertEqual(load_config(),StorageConfig())
        with self.assertRaises(StorageConfigError):require_controller_config(BANDWIDTH)
        policy=self.configure()
        with patch.object(scheduler,'load_nodes',return_value=self.nodes),patch.object(scheduler,'get_resources',return_value=support.support.resource()):
            result=policy.select(self.job,(),scheduler.select_best_node)
        self.assertEqual(result['node']['id'],'pi03')
        self.assertEqual(policy.history()[-1]['reason'],'unsupported_rank_input_plan')

    def test_large_bandwidth_explanations_remain_bounded_and_serializable(self):
        self.configure()
        self.nodes=[dict(id=f'worker-{i:03}') for i in range(64)]
        self.missing={n['id']:50 for n in self.nodes}
        self.resources={n['id']:support.support.resource() for n in self.nodes}
        self.rates={n['id']:usable(100) for n in self.nodes}
        policy=SchedulingPolicy(self.registry,settings=BANDWIDTH,coverage=self.api,max_choices=1)
        for _ in range(2):self.single(policy)
        doc=policy.history()[0]
        self.assertEqual(doc['policy'],'bandwidth')
        self.assertEqual(len(doc['bandwidth']['workers']),32)
        self.assertTrue(doc['details_truncated'])
        self.assertLess(len(json.dumps(doc).encode()),128*1024)
        self.assertEqual((policy.recorded,policy.dropped),(2,1))


class ConfiguredDispatchTests(unittest.TestCase):
    setUp=support.RuntimeTests.setUp
    dispatch=support.RuntimeTests.dispatch

    def configure(self,world):
        f=self.f
        if world>1:f.ddp_job(workers=world)
        cache_registry_schema.migrate(f.db)
        owner=CacheInventory(f.fixture.connect,list(f.client.credentials.values()),
            configured_workers=[n['id'] for n in f.nodes],state_root=f.fixture.base,
            transfer_observations=TransferObservations())
        owner.start();self.addCleanup(owner.close)
        api=CoverageAPI(f.registry,owner)
        f.runtime=StagingDispatch(f.registry,f.client,settings=BANDWIDTH,coverage=api,
                                 stage_parallelism=2,choice_logger=self.records.append)
        for index,service in enumerate([f.service,*getattr(f,'extra_services',[])]):
            key=service.credential;owner.enroll(key.worker)
            metrics=TransferMetrics(key.worker)
            # Synthetic calibration fixtures, not manufactured hardware measurements.
            for _ in range(3):metrics.record(1000,10_200_000_000//(index+1),transfer_ns=10_000_000_000//(index+1))
            service.fetcher.transfer_metrics=metrics
            reporter=InventoryReporter(service.fetcher.cache,key,transfer_metrics=metrics)
            reporter.initialize(boot_id='01234567-0123-4567-8901-012345678901');self.addCleanup(reporter.close)
            request,headers=owner.challenge(key.worker)
            body,mac=reporter.reply(request,headers['X-DL-Inventory-MAC'],headers['X-DL-Worker-Key'])
            self.assertEqual(owner.accept(key.worker,request,body,mac),'FULL')

    def test_single_actual_authorized_stage_and_start_and_resource_rollback(self):
        self.configure(1)
        self.assertTrue(self.dispatch())
        self.assertEqual(self.records[0]['policy'],'bandwidth')
        self.assertEqual([path for path,_ in self.f.calls],[STAGE_PATH,START_PATH])
        before=list(self.f.db.iterdump());self.f.runtime.set_scheduler_policy('resource')
        self.assertEqual(list(self.f.db.iterdump()),before)
        self.assertEqual(self.f.runtime.lifecycle.settings,support.RESOURCE)

    def test_three_rank_actual_dispatch_preserves_all_ready_barrier(self):
        self.configure(3)
        with patch.object(ddp,'get_resources',return_value=support.support.resource()),patch.object(ddp,'load_nodes',return_value=self.f.nodes):
            self.assertTrue(self.dispatch())
        self.assertEqual(self.records[0]['policy'],'bandwidth')
        self.assertEqual(self.records[0]['selected_nodes'],['pi03','pi02','pi01'])
        first=next(i for i,(path,_) in enumerate(self.f.calls) if path==START_PATH)
        self.assertEqual([path for path,_ in self.f.calls[:first]].count(STAGE_PATH),3)
        self.assertEqual(self.records[0]['bandwidth']['selected_estimate']['assumed_concurrent_transfers'],2)


class AuthenticatedRatesTests(unittest.TestCase):
    def setUp(self):
        self.f=auth_fixture.InventoryTests(methodName='runTest')
        self.addCleanup(self.f.doCleanups);self.f.setUp()
        self.f.owner.transfer_observations=TransferObservations(clock=self.f.clock.monotonic)
        self.metrics=TransferMetrics('pi01',clock=self.f.clock.monotonic,wall_clock=lambda:self.f.clock()/1e9)
        for _ in range(3):self.metrics.record(1000,12_000_000_000,transfer_ns=10_000_000_000)
        self.f.reporter.transfer_metrics=self.metrics

    def facts(self):
        context=self.f.owner._contexts['pi01']
        return self.f.owner.bandwidth_facts('pi01',context['session_id'],context['sequence'])

    def test_real_signed_agent_v2_inventory_carries_rates_disk_and_live_binding(self):
        self.assertTrue(self.f.poll())
        before=list(self.f.db.iterdump())
        facts=self.facts()
        self.assertEqual(list(self.f.db.iterdump()),before)
        self.assertEqual(facts['transfer']['median_bytes_per_second'],100)
        self.assertEqual(facts['transfer']['median_overhead_seconds'],2)
        self.assertGreater(facts['disk_available_bytes'],0)
        self.assertIsNone(self.f.owner.bandwidth_facts('pi01','wrong-session',1))
        self.f.clock.advance(16)
        self.assertIsNone(self.facts())

    def test_forgery_replay_and_duplicates_cannot_refresh_sample_age(self):
        request,body,signature=self.f.exchange()
        value=p.decode(body);self.assertEqual(value['version'],2)
        changed=deepcopy(value);changed['transfer']['median_bytes_per_second']=999
        with self.assertRaises(p.AuthenticationError):self.f.owner.accept('pi01',request,p.encode(changed),signature)
        self.f.owner.accept('pi01',request,body,signature)
        age=self.facts()['transfer']['age_upper_seconds']
        self.f.clock.advance(2)
        self.assertEqual(self.f.owner.accept('pi01',request,body,signature),'duplicate')
        self.assertAlmostEqual(self.facts()['transfer']['age_upper_seconds'],age+2)
        fresh,_,_=self.f.exchange()
        with self.assertRaises(p.AuthenticationError):self.f.owner.accept('pi01',fresh,body,signature)

    def test_v1_and_malformed_optional_metrics_keep_inventory_but_clear_prediction(self):
        self.assertTrue(self.f.poll());self.assertIsNotNone(self.facts()['transfer'])
        self.f.reporter.transfer_metrics=None
        self.assertTrue(self.f.poll());self.assertIsNone(self.facts()['transfer'])
        self.f.reporter.transfer_metrics=self.metrics
        request,body,_=self.f.exchange();value=p.decode(body)
        value['transfer']={'path':'secret'};raw=p.encode(value)
        self.f.owner.accept('pi01',request,raw,p.mac(self.f.key,'report',raw,request=request))
        self.assertTrue(self.f.owner.eligible('pi01'));self.assertIsNone(self.facts()['transfer'])

    def test_offline_reconnect_restart_and_old_generation_do_not_reuse_facts(self):
        self.assertTrue(self.f.poll());old=self.f.owner._contexts['pi01'].copy()
        self.f.owner.offline('pi01')
        self.assertIsNone(self.f.owner.bandwidth_facts('pi01',old['session_id'],old['sequence']))
        self.assertTrue(self.f.poll())
        self.assertIsNone(self.f.owner.bandwidth_facts('pi01',old['session_id'],old['sequence']))
        self.f.owner.close();self.f.owner.start()
        self.assertIsNone(self.f.owner.bandwidth_facts('pi01',old['session_id'],old['sequence']))

    def test_heartbeats_cannot_rejuvenate_old_worker_samples(self):
        self.assertTrue(self.f.poll())
        for _ in range(61):
            self.f.clock.advance(14)
            self.assertTrue(self.f.poll())
        self.assertIsNotNone(self.facts()['transfer'])
        for _ in range(4):
            self.f.clock.advance(14)
            self.assertTrue(self.f.poll())
        self.assertIsNone(self.facts()['transfer'])

    def test_identity_mismatch_rejected_and_reporter_failure_is_optional(self):
        with self.assertRaises(ValueError):InventoryReporter(self.f.cache,self.f.key,transfer_metrics=TransferMetrics('pi02'))
        with patch.object(self.metrics,'prediction_snapshot',side_effect=OSError('clock')):
            self.assertTrue(self.f.poll())
        self.assertIsNone(self.facts()['transfer'])


if __name__=='__main__':unittest.main(verbosity=2)
