"""10.4 configured policy, bounded explanations and synthetic workloads; no live cluster."""
from contextlib import redirect_stdout
from copy import deepcopy
from dataclasses import replace
import io
import json
import random
import shutil
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import Mock, patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
import test_scheduler_locality as support
import test_staging_dispatch as staging_fixture
import scheduler
import distributed_dispatch as ddp
import scheduler_metrics
import dispatcher
from hybrid_storage.config import StorageConfig, StorageConfigError, load_config, require_staging_config
from hybrid_storage.cache_coverage import CoverageAPI
from hybrid_storage.cache_inventory import CacheInventory
from hybrid_storage import cache_registry_schema
from hybrid_storage.input_manifests import SourceFile, SourceManifest
from hybrid_storage.inventory_reporter import InventoryReporter
from hybrid_storage.inputs import object_key
from hybrid_storage.registry import StorageRegistry
from hybrid_storage.scheduling import SchedulingPolicy
from hybrid_storage.staging_protocol import STAGE_PATH,START_PATH
from hybrid_storage.workspaces import AttemptIdentity, WorkspacePlan
from staging_dispatch import StagingDispatch


RESOURCE=StorageConfig('nfs',True,True,'resource')
LOCALITY=replace(RESOURCE,scheduler_policy='locality')


def plans(job, world=1):
    source=SourceManifest((SourceFile('requirements.txt','b'*64,20,False),
                           SourceFile('train.py','c'*64,80,True)),())
    return tuple(WorkspacePlan(AttemptIdentity(job['job_id'],job['job_id']+':0',rank),support.SHA,source)
                 for rank in range(world))


class PolicyTests(unittest.TestCase):
    def setUp(self):
        self.registry=Mock(spec=StorageRegistry)
        self.api=Mock(spec=CoverageAPI);self.api.registry=self.registry
        self.missing={'pi03':90,'pi02':20,'pi01':0}
        self.job={'job_id':'placement','owner':'alice','retry_count':0}
        self.nodes=[{'id':w} for w in self.missing]
        self.resources={w:support.resource() for w in self.missing}
        self.api.for_job_workers.side_effect=self.batch

    def batch(self,actor,workers,job_id,*,rank=None):
        return tuple(replace(support.coverage(w,self.missing[w],rank=rank,world=2 if rank is not None else None),
                             target_id=job_id) for w in workers)

    def policy(self,settings=LOCALITY,*,coverage=True,logger=None,bound=128):
        return SchedulingPolicy(self.registry,settings=settings,coverage=self.api if coverage else None,
                                choice_logger=logger,max_choices=bound)

    def single(self,policy,**kwargs):
        with patch.object(scheduler,'load_nodes',return_value=self.nodes), \
             patch.object(scheduler,'get_resources',side_effect=lambda n:self.resources[n['id']]), \
             redirect_stdout(io.StringIO()):
            return policy.select(self.job,plans(self.job),scheduler.select_best_node,**kwargs)

    def test_resource_default_calls_original_selector_without_metrics_or_inventory(self):
        policy=self.policy(RESOURCE)
        selector=Mock(return_value={'original':'identity'})
        with patch.object(scheduler_metrics.time,'perf_counter_ns') as clock:
            result=policy.select(self.job,(),selector,allowed_nodes=['pi03'])
        self.assertIs(result,selector.return_value)
        selector.assert_called_once_with(allowed_nodes=['pi03'])
        clock.assert_not_called();self.api.for_job_workers.assert_not_called()
        self.assertEqual(policy.history(),())

    def test_locality_flag_emits_filtered_nodes_missing_bytes_policy_and_reason(self):
        records=[];policy=self.policy(logger=records.append)
        self.nodes.extend({'id':w} for w in ('pi04','pi05','pi06','pi07','pi08','pi09'))
        self.resources.update({w:support.resource() for w in ('pi04','pi05','pi06','pi07','pi08','pi09')})
        self.resources['pi04']['cpu']['available_cores']=0
        self.resources['pi08']=None;self.resources['pi09']['node_state']='RUNNING'
        result=self.single(policy,requirements={'cpu_cores':1},reserved_nodes={'pi05'},
                           excluded_nodes={'pi07'},allowed_nodes={n['id'] for n in self.nodes}-{'pi06'})
        self.assertEqual(result['node']['id'],'pi01')
        record=records[0]
        self.assertEqual((record['requested_policy'],record['policy'],record['reason']),
                         ('locality','locality','verified_missing_bytes'))
        self.assertEqual({r['worker_id']:r['reason'] for r in record['filtered_nodes']},
                         {'pi04':'resources','pi05':'reserved','pi06':'not_allowed','pi07':'excluded',
                          'pi08':'unavailable','pi09':'busy'})
        self.assertEqual({r['worker_id']:r['missing_bytes'] for r in record['workers']},self.missing)
        self.assertEqual(record['total_missing_bytes'],0)
        self.assertTrue(record['trace_available']);self.assertFalse(record['details_truncated'])
        for forbidden in ('missing_hashes','credentials','source','dataset','storage_path','node_ip'):
            self.assertNotIn(forbidden,record)

    def test_resource_explicit_logger_explains_without_inventory_lookup(self):
        policy=self.policy(RESOURCE,logger=lambda _:None)
        self.assertEqual(self.single(policy)['node']['id'],'pi03')
        self.api.for_job_workers.assert_not_called()
        self.assertEqual(policy.history()[0]['reason'],'resource_policy')
        self.assertIsNone(policy.history()[0]['total_missing_bytes'])

    def test_unavailable_or_stale_inventory_reports_fallback_without_certainty(self):
        policy=self.policy(coverage=False)
        with patch('hybrid_storage.scheduling.LOGGER.info') as log:
            self.assertEqual(self.single(policy)['node']['id'],'pi03')
        log.assert_called_once()
        self.assertEqual(policy.history()[0]['reason'],'coverage_unavailable')
        self.assertIsNone(policy.history()[0]['total_missing_bytes'])
        policy=self.policy()
        self.api.for_job_workers.side_effect=lambda *a,**k:tuple(replace(r,inventory_eligible=False,
            inventory_status='untrusted_or_stale',inventory_age_ns=90_000_000_000) for r in self.batch(*a,**k))
        self.assertEqual(self.single(policy)['node']['id'],'pi03')
        self.assertEqual(policy.history()[0]['policy'],'resource')
        self.assertTrue(all(r['missing_bytes'] is None for r in policy.history()[0]['workers']))

    def test_immediate_resource_rollback_stops_queries_and_default_logging(self):
        policy=self.policy()
        self.assertEqual(self.single(policy)['node']['id'],'pi01')
        policy.set_policy('resource');self.api.for_job_workers.reset_mock()
        self.api.for_job_workers.side_effect=AssertionError('rollback must not query cache')
        self.assertEqual(self.single(policy)['node']['id'],'pi03')
        self.api.for_job_workers.assert_not_called();self.assertEqual(len(policy.history()),1)
        for bad in ('unknown',True):
            with self.assertRaises(StorageConfigError):policy.set_policy(bad)
        self.assertEqual(policy.settings,RESOURCE)

    def test_logger_failure_mutation_and_clock_failure_never_change_choice(self):
        def broken(document):document.clear();raise OSError('log sink unavailable')
        policy=self.policy(logger=broken)
        self.assertEqual(self.single(policy)['node']['id'],'pi01')
        self.assertEqual(policy.history()[0]['selected_nodes'],['pi01'])
        copy=policy.history()[0];copy.clear()
        self.assertEqual(policy.history()[0]['policy'],'locality')
        with patch.object(scheduler_metrics.time,'perf_counter_ns',side_effect=OSError('clock')):
            self.assertEqual(self.single(policy)['node']['id'],'pi01')
        self.assertFalse(policy.history()[-1]['trace_available'])
        self.assertEqual(policy.history()[-1]['selected_nodes'],['pi01'])

    def test_bounded_history_and_node_details(self):
        self.nodes=[{'id':f'worker-{i:03}'} for i in range(300)]
        self.resources={n['id']:support.resource() for n in self.nodes}
        self.missing={w:10 for w in self.resources}
        policy=self.policy(bound=2,logger=lambda _:None)
        for _ in range(3):self.single(policy)
        self.assertEqual((policy.recorded,policy.dropped,len(policy.history())),(3,1,2))
        record=policy.history()[-1]
        self.assertEqual(record['eligible_nodes'],300)
        self.assertEqual(len(record['candidates']),256)
        self.assertEqual(len(record['workers']),256)
        self.assertTrue(record['details_truncated'])

    def test_original_selection_exception_propagates_without_fake_success_log(self):
        policy=self.policy()
        with self.assertRaisesRegex(OSError,'original'):
            policy.select(self.job,plans(self.job),Mock(side_effect=OSError('original')))
        self.assertEqual(policy.history(),())

    def test_invalid_registry_settings_and_rank_plan_are_not_enabled(self):
        for settings in (StorageConfig(),):
            with self.assertRaises(StorageConfigError):self.policy(settings)
        wrong=Mock(spec=CoverageAPI);wrong.registry=Mock(spec=StorageRegistry)
        with self.assertRaises(ValueError):SchedulingPolicy(self.registry,settings=LOCALITY,coverage=wrong)
        policy=self.policy()
        with patch.object(scheduler,'load_nodes',return_value=self.nodes), \
             patch.object(scheduler,'get_resources',return_value=support.resource()),redirect_stdout(io.StringIO()):
            result=policy.select(self.job,(),scheduler.select_best_node)
        self.assertEqual(result['node']['id'],'pi03')
        self.api.for_job_workers.assert_not_called()
        self.assertEqual(policy.history()[0]['reason'],'unsupported_rank_input_plan')

    def test_DDP_config_uses_same_group_selector_and_logs_full_rank_sum(self):
        policy=self.policy(logger=lambda _:None)
        self.job['job_config']=json.dumps({'execution':{'workers':2},'resources_per_worker':{'cpu_cores':1}})
        prepared=[{'node_id':n['id'],'record':{'compatibility_key':'same'}} for n in self.nodes]
        with patch.object(ddp,'load_nodes',return_value=self.nodes), \
             patch.object(ddp,'get_resources',side_effect=lambda n:self.resources[n['id']]), \
             patch.object(ddp,'get_job_environments',return_value=prepared), \
             patch.object(ddp,'get_reservations',return_value=[]):
            result=policy.select(self.job,plans(self.job,2),ddp.select_group,self.job,set())
        self.assertEqual([n['id'] for n in result],['pi01','pi02'])
        record=policy.history()[0]
        self.assertEqual((record['selector'],record['complete_groups'],record['total_missing_bytes']),('ddp',1,20))
        self.assertEqual(record['per_rank_missing_bytes'],[0,20])

    def test_staging_CLI_samples_validate_without_startup_or_DB_imports(self):
        for sample,policy in [('nfs-resource.json','resource'),('nfs-locality.json','locality')]:
            path=ROOT/'config/storage-examples'/sample
            settings=load_config(path);self.assertEqual(require_staging_config(settings).scheduler_policy,policy)
            result=subprocess.run([sys.executable,'-B','-m','hybrid_storage.config','--runtime','staging','--config',str(path)],
                                   cwd=ROOT,text=True,capture_output=True,timeout=10)
            self.assertEqual(result.returncode,0,result.stderr)
            report=json.loads(result.stdout)
            self.assertFalse(report['automatic_startup']);self.assertEqual(report['runtime'],'trusted-staging-component')
            check=subprocess.run([sys.executable,'-B','-c',
                "import sys;from hybrid_storage.config import load_config,require_staging_config;"
                "require_staging_config(load_config(__import__('pathlib').Path(sys.argv[1])));"
                "assert 'sqlite3' not in sys.modules and 'job_store' not in sys.modules",str(path)],
                cwd=ROOT,text=True,capture_output=True,timeout=10)
            self.assertEqual(check.returncode,0,check.stderr)
        self.assertEqual(load_config(ROOT/'config/storage.json'),StorageConfig())

    def test_workload_counterexample_and_rollback_do_not_claim_global_optimum(self):
        self.nodes=[{'id':'pi01'},{'id':'pi02'}]
        self.resources={n['id']:support.resource() for n in self.nodes}
        def wave(policy):
            total=0;reserved=set();trace=[]
            for identity,misses in [('first',{'pi01':2,'pi02':0}),('second',{'pi01':100,'pi02':0})]:
                self.job['job_id']=identity;self.missing=misses
                result=self.single(policy,reserved_nodes=reserved)
                node=result['node']['id'];reserved.add(node);total+=misses[node];trace.append(identity)
            return total,trace
        resource=self.policy(RESOURCE);locality=self.policy()
        baseline,trace=wave(resource);cost,other=wave(locality)
        self.assertEqual((baseline,cost),(2,100));self.assertEqual(trace,other)
        locality.set_policy('resource')
        self.api.for_job_workers.reset_mock()
        self.assertEqual(wave(locality),(baseline,trace));self.api.for_job_workers.assert_not_called()
        print('POLICY_GREEDY_REGRESSION '+json.dumps({'modeled_bytes':True,'resource_bytes':baseline,
            'locality_bytes':cost,'after_resource_rollback_bytes':baseline,'start_order_unchanged':True}))

    def test_seeded_policy_matrix_preserves_resources_and_replays_deterministically(self):
        traces=[]
        for repeat in range(2):
            rng=random.Random(104)
            trace=[]
            for world in (1,2,3):
                for state in ('fresh','stale','absent','error'):
                    for layout in ('equal','cpu','ram','ineligible','reserved','compatibility'):
                        self.nodes=[{'id':w} for w in ('pi03','pi02','pi01')]
                        self.resources={n['id']:support.resource() for n in self.nodes}
                        self.missing={w:rng.randrange(101) for w in self.resources}
                        self.job={'job_id':'placement','owner':'alice','retry_count':0,
                            'job_config':json.dumps({'execution':{'workers':world},'resources_per_worker':{'cpu_cores':1}})}
                        if layout=='cpu':self.resources['pi03']['cpu']['usage_percent']=1
                        if layout=='ram':self.resources['pi02']['memory']['available_gb']=8
                        if layout=='ineligible':self.resources['pi01']['cpu']['available_cores']=0
                        reserved={'pi02'} if layout=='reserved' else set()
                        prepared=[{'node_id':n['id'],'record':{'compatibility_key':
                            'other' if layout=='compatibility' and n['id']=='pi03' else 'same'}} for n in self.nodes]
                        def batch(actor,workers,job_id,*,rank=None):
                            result=tuple(replace(support.coverage(w,self.missing[w]),target_id=job_id,
                                rank=rank,world_size=world if rank is not None else None) for w in workers)
                            return result if state=='fresh' else tuple(replace(r,inventory_eligible=False,
                                inventory_status='untrusted_or_stale',inventory_age_ns=90_000_000_000) for r in result)
                        self.api.for_job_workers.side_effect=OSError('model unavailable') if state=='error' else batch
                        resource=self.policy(RESOURCE);locality=self.policy(coverage=state!='absent',logger=lambda _:None)
                        def run(policy):
                            if world==1:
                                result=self.single(policy,requirements={'cpu_cores':1},reserved_nodes=reserved)
                                return [] if result is None else [result['node']['id']]
                            with patch.object(ddp,'load_nodes',return_value=self.nodes), \
                                 patch.object(ddp,'get_resources',side_effect=lambda n:self.resources[n['id']]), \
                                 patch.object(ddp,'get_job_environments',return_value=prepared), \
                                 patch.object(ddp,'get_reservations',return_value=[{'node_id':w} for w in reserved]):
                                result=policy.select(self.job,plans(self.job,world),ddp.select_group,self.job,set())
                            return [n['id'] for n in result or []]
                        baseline,selected=run(resource),run(locality)
                        self.assertEqual(len(selected),len(baseline))
                        def profile(ids):
                            return sorted((self.resources[w]['cpu']['usage_percent'],
                                           -self.resources[w]['memory']['available_gb']) for w in ids)
                        self.assertEqual(profile(selected),profile(baseline))
                        self.assertTrue(all(w not in reserved and self.resources[w]['cpu']['available_cores']>=1 for w in selected))
                        if state!='fresh':self.assertEqual(selected,baseline)
                        if state=='fresh' and selected:
                            self.assertLessEqual(sum(self.missing[w] for w in selected),sum(self.missing[w] for w in baseline))
                        locality.set_policy('resource')
                        self.assertEqual(run(locality),baseline)
                        trace.append((world,state,layout,baseline,selected))
            traces.append(trace)
        self.assertEqual(traces[0],traces[1])
        print('POLICY_MATRIX '+json.dumps({'seed':104,'scenarios':72,'repetitions':2,
            'policy_selections':432,'resource_profiles_preserved':True,'unknown_falls_back':True,
            'resource_rollback_exact':True,'replay_equal':True,'costs':'synthetic bytes; not throughput'}))


class RuntimeTests(unittest.TestCase):
    def setUp(self):
        self.f=staging_fixture.DispatchTests(methodName='runTest')
        self.addCleanup(self.f.doCleanups);self.f.setUp()
        self.records=[]

    def runtime(self,*,world=1,warm=False):
        if world>1:self.f.ddp_job(workers=world)
        cache_registry_schema.migrate(self.f.db)
        owner=CacheInventory(self.f.fixture.connect,list(self.f.client.credentials.values()),
                             configured_workers=[n['id'] for n in self.f.nodes],state_root=self.f.fixture.base)
        owner.start();self.addCleanup(owner.close)
        api=CoverageAPI(self.f.registry,owner)
        runtime=StagingDispatch(self.f.registry,self.f.client,settings=LOCALITY,coverage=api,choice_logger=self.records.append)
        self.f.runtime=runtime
        actual=runtime.plans(staging_fixture.fixtures.store.get_job(self.f.job_id))
        services=[self.f.service,*getattr(self.f,'extra_services',[])]
        for index,service in enumerate(services):
            key=service.credential;owner.enroll(key.worker)
            if warm and index==len(services)-1:
                for item in {f.sha256:f for f in actual[0].inputs()}.values():
                    target=service.fetcher.cache.root/'objects'/object_key(item.sha256)
                    target.parent.parent.mkdir(mode=0o700,exist_ok=True);target.parent.mkdir(mode=0o700,exist_ok=True)
                    with (self.f.fixture.objects/object_key(item.sha256)).open('rb') as src,target.open('xb') as dst:
                        shutil.copyfileobj(src,dst,length=1024*1024)
                    target.chmod(0o400);service.fetcher.cache.verify(item.sha256,item.size_bytes,force=True)
            reporter=InventoryReporter(service.fetcher.cache,key)
            reporter.initialize(boot_id='01234567-0123-4567-8901-012345678901');self.addCleanup(reporter.close)
            raw,headers=owner.challenge(key.worker)
            body,signature=reporter.reply(raw,headers['X-DL-Inventory-MAC'],headers['X-DL-Worker-Key'])
            self.assertEqual(owner.accept(key.worker,raw,body,signature),'FULL')
        return runtime,api

    def dispatch(self):
        with patch.object(dispatcher,'select_best_node',scheduler.select_best_node), \
             patch.object(scheduler,'get_resources',return_value=support.resource()):
            return self.f.dispatch()

    def test_configured_single_actual_authorized_dispatch_stages_then_starts(self):
        runtime,api=self.runtime()
        with patch.object(api,'for_job_workers',wraps=api.for_job_workers) as query:
            self.assertTrue(self.dispatch())
        query.assert_called_once()
        self.assertEqual(staging_fixture.fixtures.store.get_job(self.f.job_id)['status'],'RUNNING')
        self.assertEqual(self.records[0]['requested_policy'],'locality')
        self.assertEqual(self.records[0]['selected_nodes'],['pi01'])
        self.assertEqual(self.records[0]['workers'][0]['inventory_status'],'current')
        self.assertEqual([p for p,_ in self.f.calls],[STAGE_PATH,START_PATH])
        before=list(self.f.db.iterdump())
        runtime.set_scheduler_policy('resource')
        self.assertEqual(list(self.f.db.iterdump()),before)
        self.assertEqual(runtime.lifecycle.settings,RESOURCE)

    def test_configured_three_rank_actual_dispatch_prefers_warm_rank_and_preserves_barrier(self):
        runtime,_=self.runtime(world=3,warm=True)
        with patch.object(ddp,'get_resources',return_value=support.resource()), \
             patch.object(ddp,'load_nodes',return_value=self.f.nodes):self.assertTrue(self.dispatch())
        self.assertEqual(self.records[0]['selected_nodes'],['pi03','pi01','pi02'])
        self.assertEqual(self.records[0]['policy'],'locality')
        self.assertEqual(self.records[0]['per_rank_missing_bytes'][0],0)
        self.assertEqual(staging_fixture.fixtures.store.get_job(self.f.job_id)['status'],'RUNNING')
        self.assertEqual(len(staging_fixture.fixtures.store.get_reservations()),3)
        first_start=next(i for i,(p,_) in enumerate(self.f.calls) if p==START_PATH)
        self.assertEqual([p for p,_ in self.f.calls[:first_start]].count(STAGE_PATH),3)

    def test_no_inventory_dependency_and_resource_rollback_keep_staging_enabled(self):
        runtime=StagingDispatch(self.f.registry,self.f.client,settings=LOCALITY,choice_logger=self.records.append)
        self.f.runtime=runtime
        runtime.set_scheduler_policy('resource')
        self.assertTrue(self.dispatch())
        self.assertEqual((self.records[0]['requested_policy'],self.records[0]['policy']),('resource','resource'))
        self.assertEqual(self.records[0]['workers'],[])


class CheckpointRuntimeTests(unittest.TestCase):
    def test_checkpoint_coordinator_accepts_locality_then_restores_existing_resume_flow(self):
        import test_hybrid_checkpoint_recovery as checkpoint_fixture
        from checkpoint_recovery import CheckpointDispatch
        fixture=checkpoint_fixture.RecoveryTests(methodName='runTest')
        self.addCleanup(fixture.doCleanups);fixture.setUp()
        previous=fixture.runtime;records=[]
        runtime=CheckpointDispatch(fixture.registry,fixture.f.client,publisher=fixture.publisher,
            collector=previous.collector,restorer=previous.restorer,settings=LOCALITY,
            choice_logger=records.append)
        fixture.runtime=fixture.f.runtime=runtime
        with patch.object(dispatcher,'select_best_node',scheduler.select_best_node), \
             patch.object(scheduler,'get_resources',return_value=support.resource()),redirect_stdout(io.StringIO()):
            fixture.cold();head=fixture.save();fixture.fail()
            self.assertTrue(fixture.f.dispatch())
            self.assertEqual(fixture.job()['status'],'SCHEDULED')
            fixture.ack();self.assertEqual(runtime.reconcile(fixture.job()),'RUNNING')
        self.assertEqual(len(records),2)
        self.assertTrue(all(r['requested_policy']=='locality' and r['reason']=='coverage_unavailable' for r in records))
        self.assertEqual(fixture.publisher.head('alice',fixture.f.job_id),head)
        before=list(fixture.db.iterdump());runtime.set_scheduler_policy('resource')
        self.assertEqual(list(fixture.db.iterdump()),before)


if __name__=='__main__':unittest.main(verbosity=2)
