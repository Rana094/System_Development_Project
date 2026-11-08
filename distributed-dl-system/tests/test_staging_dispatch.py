"""7.2 real local HTTP clients/DB/cache/private copies; fake NFS facts and launch.

No network listener, Pi, service, mount, credential provisioning or live DB.
The isolation callback and Popen are mocks, NOT evidence of OS tenant isolation.
"""

from contextlib import ExitStack
import importlib.util
import json
from pathlib import Path
import shutil
import subprocess
import sys
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
import test_storage_registry as fixtures
import test_cache_fetch as copy_fixture
sys.path.insert(0,str(ROOT/"agent"))
import execution_leases as leases
import execution_lease as worker_lease
import dispatcher
from hybrid_storage import staging_schema,cleanup_schema
from hybrid_storage.config import StorageConfig
from hybrid_storage.inputs import object_key
from hybrid_storage.worker_cache import WorkerCache
from hybrid_storage.cache_fetch import CacheFetcher
from hybrid_storage.workspaces import WorkspaceManager
from hybrid_storage.inventory_protocol import Credential, encode, decode
from hybrid_storage.staging_protocol import (HEADER, STAGE_PATH, START_PATH, StagingClient,
    request_document, signature)
from staging_dispatch import StagingDispatch
from agent.staging import StagingService, install_staging_service


class DispatchTests(unittest.TestCase):
    def setUp(self):
        self.fixture=fixtures.RegistryTests(methodName='runTest')
        self.addCleanup(self.fixture.doCleanups);self.fixture.setUp()
        self.db=self.fixture.db;self.registry=self.fixture.registry()
        self.source=self.fixture.source(self.registry);self.data=self.fixture.dataset(self.registry)
        self.binding=self.fixture.bind(self.registry,self.source,self.data)
        self.job_id=self.fixture.job_id
        staging_schema.migrate(self.db);cleanup_schema.migrate(self.db)
        self.key=Credential('pi01','a'*32,b'k'*32)
        self.node=dict(id='pi01',ip='192.0.2.1',port=5000)
        self.client=StagingClient({'pi01':self.key},post=self.post)
        self.runtime=StagingDispatch(self.registry,self.client,settings=StorageConfig('nfs',True,True,'resource'))
        self.calls=[]
        self.nodes=[self.node]
        self.http_by_ip={}
        self.worker=self.fixture.base/'worker';self.worker.mkdir(mode=0o700)
        (self.worker/'agent').mkdir()
        (self.worker/'agent/custom_runner.py').write_text('# disposable launcher marker\n')
        self.cache_root=self.fixture.base/'cache';self.cache_root.mkdir(mode=0o700)
        self.workspace_root=self.fixture.base/'workspaces';self.workspace_root.mkdir(mode=0o700)
        self.cache=WorkerCache(self.cache_root,input_mount=self.fixture.objects)
        self.fetcher=CacheFetcher(self.cache,source=copy_fixture.SOURCE,quota_bytes=16*1024*1024,
                                  reserve_free_bytes=0,retry_delay=0,io_timeout=3)
        self.fetcher.initialize();self.addCleanup(self.fetcher.close)
        self.manager=WorkspaceManager(self.cache,self.workspace_root,max_input_bytes=16*1024*1024,reserve_free_bytes=0)
        self.manager.initialize()
        spec=importlib.util.spec_from_file_location('hybrid_test_worker',ROOT/'agent/agent.py')
        self.agent=importlib.util.module_from_spec(spec);spec.loader.exec_module(self.agent)
        self.agent.BASE_DIR=self.worker
        self.agent.LOG_DIR=self.worker/'logs';self.agent.LOG_DIR.mkdir()
        self.agent.node_id=lambda:'pi01'
        self.isolate=Mock(side_effect=lambda command,workspace,data:['/reviewed-test-isolator',*command])
        self.service=StagingService(self.fetcher,self.manager,self.key,worker_root=self.worker,
             ownership=worker_lease,cancelled=self.agent.attempt_cancelled,isolate=self.isolate,
             quiescent=lambda *_:False,terminate=Mock())
        install_staging_service(self.agent,self.service,watchdog=False)
        self.addCleanup(self.service.close)
        self.http=self.agent.app.test_client()
        self.process=Mock(pid=99999999);self.process.poll.return_value=None
        self.stack=ExitStack();self.addCleanup(self.stack.close)
        self.spawn=self.stack.enter_context(patch.object(self.fetcher,'_spawn',side_effect=self.fake_spawn))
        self.popen=self.stack.enter_context(patch.object(self.agent.subprocess,'Popen',return_value=self.process))
        self.addCleanup(lambda: [call.kwargs['stdout'].close() for call in self.popen.call_args_list])
        # Use real Popen for cache helper: patching the shared module above would
        # otherwise replace it too. Saved callable below predates the patch.
        self.stack.enter_context(patch.object(self.agent.threading,'Thread'))
        self.stack.enter_context(patch.object(dispatcher,'select_best_node',return_value={'node':self.node,'resources':{}}))
        self.stack.enter_context(patch.object(leases,'supported',return_value=True))
        self.stack.enter_context(patch.object(dispatcher,'get_excluded_nodes',return_value=set()))
        self.stack.enter_context(patch('scheduler.load_nodes',side_effect=lambda:self.nodes))
        self.prepare_environment()

    real_popen = staticmethod(subprocess.Popen)

    def fake_spawn(self, request, output, lock):
        return self.real_popen([sys.executable,'-I','-B','-c',copy_fixture.FAKE_HELPER,str(ROOT),json.dumps(request)],
                 stdin=subprocess.DEVNULL,stdout=output,stderr=subprocess.DEVNULL,pass_fds=(request['output_fd'],lock))

    def prepare_environment(self, *, enqueue=True):
        record=dict(job_id=self.job_id,owner='alice',manifest_sha256=fixtures.store.get_job(self.job_id)['storage_manifest_sha256'],
                    environment_key='b'*64,compatibility_key='c'*64,profile='isolated')
        directory=self.worker/'worker-storage/jobs'/self.job_id
        directory.mkdir(parents=True)
        (directory/'prepared.json').write_text(json.dumps(record))
        (directory/'request.json').write_bytes(encode(dict(manifest=json.loads(self.fixture.raw),
                    manifest_sha256=record['manifest_sha256'])))
        (directory/'checkpoints').mkdir()
        fixtures.store.record_environment(self.job_id,'pi01',record,storage_runtime=self.runtime)
        if enqueue:fixtures.store.enqueue_custom_job(self.job_id,['pi01'],storage_runtime=self.runtime)

    def post(self,url,*,data,headers,timeout,stream):
        import requests
        path='/' + url.split('/',3)[3]
        self.calls.append((path,decode(data)['body']))
        address=url.split('/')[2].split(':')[0]
        response=self.http_by_ip.get(address,self.http).post(path,data=data,headers=headers)
        result=requests.Response();result.status_code=response.status_code
        result._content=response.data;result._content_consumed=True;result.headers.update(response.headers)
        return result

    def ddp_job(self):
        document=json.loads((ROOT/'examples/job.single.json').read_text())
        document.update(project=str(self.fixture.project),dataset=str(self.fixture.data),
                        execution=dict(mode='distributed',workers=3))
        spec=fixtures.validate_spec(document,self.fixture.base)
        self.job_id='ddp-staged'
        fixtures.store.add_job(self.job_id,1,job_type='custom',owner='alice',status='RECEIVED',
                               job_config=spec,storage_backend='nfs')
        workspace=Path(fixtures.job_storage.stage_job(self.job_id)['storage_path'])
        self.fixture.raw=(workspace/'manifest.json').read_bytes()
        source=self.fixture.source(self.registry,workspace=workspace)
        data=self.fixture.dataset(self.registry,workspace=workspace)
        self.registry.bind_job('alice',self.job_id,legacy_manifest_bytes=self.fixture.raw,
            source_sha256=source.sha256,dataset_id=data.dataset_id,dataset_version=1)
        self.prepare_environment(enqueue=False)
        nodes=[self.node]
        self.extra_services=[]
        for number in (2,3):
            node=dict(id=f'pi{number:02}',ip=f'192.0.2.{number}',port=5000);nodes.append(node)
            key=Credential(node['id'],str(number)*32,bytes([number])*32)
            self.client.credentials[node['id']]=key
            base=self.fixture.base/f'worker{number}';base.mkdir(mode=0o700)
            (base/'agent').mkdir();(base/'agent/custom_runner.py').write_text('# disposable test\n')
            (base/'logs').mkdir()
            shutil.copytree(self.worker/'worker-storage/jobs'/self.job_id,base/'worker-storage/jobs'/self.job_id)
            for folder in ('cache','private'):(base/folder).mkdir(mode=0o700)
            cache=WorkerCache(base/'cache',input_mount=self.fixture.objects)
            fetch=CacheFetcher(cache,source=copy_fixture.SOURCE,quota_bytes=16*1024*1024,reserve_free_bytes=0,io_timeout=3)
            fetch.initialize();self.addCleanup(fetch.close)
            self.stack.enter_context(patch.object(fetch,'_spawn',side_effect=self.fake_spawn))
            manager=WorkspaceManager(cache,base/'private',max_input_bytes=16*1024*1024,reserve_free_bytes=0)
            manager.initialize()
            module_spec=importlib.util.spec_from_file_location(f'ddp_worker{number}',ROOT/'agent/agent.py')
            agent=importlib.util.module_from_spec(module_spec)
            with patch.object(subprocess,'run',return_value=Mock(returncode=1)):
                module_spec.loader.exec_module(agent)
            agent.BASE_DIR=base;agent.LOG_DIR=base/'logs';agent.node_id=lambda name=node['id']:name
            service=StagingService(fetch,manager,key,worker_root=base,ownership=worker_lease,
                                    cancelled=agent.attempt_cancelled,isolate=self.isolate,
                                    quiescent=lambda *_:False,terminate=Mock())
            install_staging_service(agent,service,watchdog=False)
            self.addCleanup(service.close)
            self.extra_services.append(service)
            self.http_by_ip[node['ip']]=agent.app.test_client()
            record=json.loads((base/'worker-storage/jobs'/self.job_id/'prepared.json').read_text())
            fixtures.store.record_environment(self.job_id,node['id'],record,storage_runtime=self.runtime)
        fixtures.store.enqueue_custom_job(self.job_id,[n['id'] for n in nodes],storage_runtime=self.runtime)
        self.nodes=nodes
        return nodes

    def dispatch(self):
        return dispatcher.dispatch_one_job(fixtures.store.get_job(self.job_id),storage_runtime=self.runtime)

    def reserve(self):
        job=fixtures.store.get_job(self.job_id)
        fixtures.store.mark_scheduled(self.job_id,'pi01',self.node['ip'],
            json.loads(job['job_config'])['resources_per_worker'],storage_runtime=self.runtime)
        job=fixtures.store.get_job(self.job_id)
        lease=leases.ensure(job,'pi01')
        return job,lease

    def test_cold_actual_dispatch_reserves_stages_verifies_and_launches_only_local_inputs(self):
        self.assertTrue(self.dispatch())
        self.assertEqual([path for path,_ in self.calls],[STAGE_PATH,START_PATH])
        self.assertEqual(fixtures.store.get_job(self.job_id)['status'],'RUNNING')
        self.assertEqual(len(fixtures.store.get_reservations()),1)
        self.assertEqual(fixtures.store.get_attempts(self.job_id)[0]['status'],'RUNNING')
        self.assertEqual(self.db.execute('SELECT phase FROM hybrid_staging_attempts').fetchone()[0],'STARTED')
        argv=self.popen.call_args.args[0]
        self.assertIn('--lease-path',argv);self.assertIn('/reviewed-test-isolator',argv)
        paths=json.loads(argv[argv.index('--staged-inputs-json')+1])
        self.assertTrue(Path(paths['source']).is_relative_to(self.workspace_root))
        self.assertTrue(Path(paths['data']).is_relative_to(self.workspace_root))
        self.assertEqual((Path(paths['data'])/'a/rows.csv').read_bytes(),b'x,y\n1,2\n')
        self.assertNotIn(str(self.fixture.objects),' '.join(argv))
        self.assertNotIn(str(self.cache_root),' '.join(argv))
        self.assertEqual(self.isolate.call_count,1)
        self.assertGreater(self.cache.metrics.snapshot().staged_bytes,0)

    def test_warm_second_authorized_job_stages_zero_nfs_bytes(self):
        self.assertTrue(self.dispatch());first=self.cache.metrics.snapshot()
        # The trainer is mocked. Exercise actual durable worker finish and
        # controller reconciliation; prove that no real descendants exist.
        _,lease,_=next(iter(self.service._ready.values()))
        self.process.poll.return_value=0
        self.service.quiescent=lambda *_:True  # No real trainer in this mock fixture.
        self.assertTrue(self.service.note_exit(lease,0));self.service.tick()
        with patch('job_artifacts.collect'):
            self.assertEqual(self.runtime.reconcile(fixtures.store.get_job(self.job_id)),'COMPLETED')
        self.agent.job_process=None;self.process.poll.return_value=None
        self.job_id,workspace,raw=self.fixture.job()
        source=self.fixture.source(self.registry,workspace=workspace)
        data=self.fixture.dataset(self.registry,workspace=workspace)
        self.registry.bind_job('alice',self.job_id,legacy_manifest_bytes=raw,source_sha256=source.sha256,
                              dataset_id=data.dataset_id,dataset_version=data.version)
        self.fixture.raw=raw;self.prepare_environment()
        with patch.object(self.fetcher,'_spawn',side_effect=AssertionError('warm job must not read NFS')):
            self.assertTrue(self.dispatch())
        second=self.cache.metrics.snapshot()
        self.assertEqual(first.staged_bytes,second.staged_bytes)
        self.assertGreater(second.cache_hit_bytes,first.cache_hit_bytes)

    def test_default_dispatch_remains_blocked_for_nfs_and_never_calls_worker(self):
        self.assertFalse(dispatcher.dispatch_one_job(fixtures.store.get_job(self.job_id)))
        self.assertEqual(self.calls,[]);self.assertEqual(fixtures.store.get_reservations(),[])

    def test_actual_fair_queue_loop_requires_optin_and_reuses_normal_quota_policy(self):
        self.assertEqual(fixtures.store.fair_queue(),[])
        queued=fixtures.store.fair_queue(storage_runtime=self.runtime)
        self.assertEqual([row['job_id'] for row in queued],[self.job_id])
        dispatcher.dispatch_queued_jobs(storage_runtime=self.runtime)
        self.assertEqual(fixtures.store.get_job(self.job_id)['status'],'RUNNING')
        self.assertEqual([path for path,_ in self.calls],[STAGE_PATH,START_PATH])
        self.assertEqual(fixtures.store.fair_queue(storage_runtime=self.runtime),[])

    def test_current_acl_revocation_prevents_reservation(self):
        self.job_id,workspace,raw=self.fixture.job()
        source=self.fixture.source(self.registry,workspace=workspace)
        data=self.fixture.dataset(self.registry,owner='bob',readers=['alice'],workspace=workspace)
        self.registry.bind_job('alice',self.job_id,legacy_manifest_bytes=raw,source_sha256=source.sha256,
                              dataset_id=data.dataset_id,dataset_version=1)
        self.fixture.raw=raw;self.prepare_environment()
        self.registry.set_readers('bob',data.dataset_id,[])
        with self.assertRaises(PermissionError):self.dispatch()
        self.assertEqual(self.calls,[]);self.assertEqual(fixtures.store.get_reservations(),[])

    def test_source_missing_cannot_produce_receipt_or_start(self):
        (self.fixture.objects/object_key(self.source.files[0].sha256)).unlink()
        with self.assertRaises(Exception):self.dispatch()
        self.assertEqual([path for path,_ in self.calls],[STAGE_PATH]);self.popen.assert_not_called()
        self.assertEqual(fixtures.store.get_job(self.job_id)['status'],'SCHEDULED')
        self.assertTrue(leases.get(fixtures.store.get_job(self.job_id),'pi01')['renewal_blocked'])
        self.assertEqual(len(fixtures.store.get_reservations()),1)

    def test_cancellation_between_ready_and_start_prevents_launch(self):
        job,lease=self.reserve();stage,replies=self.runtime.stage_group(job,[self.node],[lease])
        fixtures.store.control_job(self.job_id,'alice','cancel')
        with self.assertRaises(ValueError):self.runtime.start(job,self.node,{},stage,replies[0])
        self.popen.assert_not_called()

    def test_corrupt_private_copy_is_rejected_at_launch_even_with_matching_receipt(self):
        job,lease=self.reserve();stage,replies=self.runtime.stage_group(job,[self.node],[lease])
        plan,_,_=self.service._ready[stage.stage_id]
        path=self.workspace_root/'attempts'/plan.identity.key/'inputs/source/train.py'
        path.chmod(0o600);path.write_bytes(b'tampered');path.chmod(0o500)
        payload=dispatcher.build_start_payload(job);payload.update(attempt_token=f'{self.job_id}:0',lease=lease)
        with self.assertRaises(Exception):self.runtime.start(job,self.node,payload,stage,replies[0])
        self.popen.assert_not_called()

    def test_prepared_execution_drift_cannot_launch_with_same_manifest_label(self):
        path=self.worker/'worker-storage/jobs'/self.job_id/'request.json'
        descriptor=json.loads(path.read_text())
        descriptor['manifest']['execution_spec']['entrypoint']=['python','other.py']
        path.write_text(json.dumps(descriptor))
        with self.assertRaises(Exception):self.dispatch()
        self.popen.assert_not_called();self.isolate.assert_not_called()

    def test_bare_mark_running_cannot_use_even_a_ready_receipt(self):
        job,lease=self.reserve();self.runtime.stage_group(job,[self.node],[lease])
        with self.assertRaises(ValueError):fixtures.store.mark_running(self.job_id,storage_runtime=self.runtime)
        self.assertEqual(fixtures.store.get_job(self.job_id)['status'],'SCHEDULED')

    def test_wrong_key_worker_action_body_replay_and_unsigned_mutation_rejected(self):
        job,lease=self.reserve();plan=self.runtime.plans(job)[0]
        body=dict(stage_id='d'*32,plan=plan.document(),lease=lease)
        raw=encode(request_document(self.key,body));signed=signature(self.key,STAGE_PATH,raw)
        for path,content,mac in ((STAGE_PATH,raw,'0'*64),(START_PATH,raw,signed),
                                  (STAGE_PATH,raw+b' ',signed)):
            self.assertEqual(self.http.post(path,data=content,headers={HEADER:mac}).status_code,403)
        stale=decode(raw);stale['sent_at_ns']-=3_000_000_000;stale=encode(stale)
        self.assertEqual(self.http.post(STAGE_PATH,data=stale,headers={HEADER:signature(self.key,STAGE_PATH,stale)}).status_code,403)
        self.assertEqual(self.http.post('/jobs/start',json={'job_id':self.job_id}).status_code,403)
        self.assertEqual(self.http.post(f'/jobs/{self.job_id}/lease',json={'lease':lease}).status_code,403)
        self.assertEqual(self.http.post(f'/jobs/{self.job_id}/stop',json={}).status_code,403)
        self.popen.assert_not_called()

    def test_isolation_is_required_and_failure_never_uses_legacy_fallback(self):
        with self.assertRaises(ValueError):
            StagingService(self.fetcher,self.manager,self.key,worker_root=self.worker,ownership=worker_lease,
                           cancelled=lambda *_:False,isolate=None)
        self.isolate.side_effect=PermissionError('unavailable isolation')
        with self.assertRaises(Exception):self.dispatch()
        self.popen.assert_not_called()

    def test_late_bootstrap_failure_cannot_activate_unauthenticated_launch_hook(self):
        from types import SimpleNamespace
        from flask import Flask
        app=Flask('late-staging-bootstrap')
        app.add_url_rule('/probe','probe',lambda:'ready')
        self.assertEqual(app.test_client().get('/probe').status_code,200)
        module=SimpleNamespace(app=app,BASE_DIR=self.worker,node_id=lambda:'pi01',job_process=None)
        with self.assertRaises(AssertionError):install_staging_service(module,self.service)
        self.assertNotIn('hybrid_staging',app.extensions)

    def test_ready_reply_mac_cannot_be_replayed_for_a_new_request(self):
        job,lease=self.reserve();plan=self.runtime.plans(job)[0]
        original=self.client.post
        saved=[]
        def replay(*args,**kwargs):
            if saved:return saved[0]
            result=original(*args,**kwargs);saved.append(result);return result
        self.client.post=replay
        body=dict(stage_id='d'*32,plan=plan.document(),lease=lease)
        self.client.call(self.node,STAGE_PATH,body)
        with self.assertRaises(ValueError):self.client.call(self.node,STAGE_PATH,body)

    def test_three_rank_dispatch_stages_all_before_any_existing_launcher_starts(self):
        import distributed_dispatch as ddp
        nodes=self.ddp_job()
        with patch.object(ddp,'select_group',return_value=nodes) as selection:
            self.assertTrue(self.dispatch())
        self.assertTrue(selection.call_args.kwargs['require_ownership'])
        self.assertEqual([path for path,_ in self.calls],[STAGE_PATH]*3+[START_PATH]*3)
        self.assertEqual(len(fixtures.store.get_reservations()),3)
        self.assertEqual([row['status'] for row in fixtures.store.get_ranks(self.job_id,0)],['RUNNING']*3)
        self.assertEqual(self.popen.call_count,3)
        self.assertEqual([body['ddp']['rank'] for path,body in self.calls if path==START_PATH],[0,1,2])

    def test_failed_third_rank_keeps_entire_group_reserved_and_launches_none(self):
        import distributed_dispatch as ddp
        nodes=self.ddp_job()
        with patch.object(ddp,'select_group',return_value=nodes), \
             patch.object(self.extra_services[-1].fetcher,'ensure',side_effect=OSError('fixture NFS outage')):
            with self.assertRaises(Exception):self.dispatch()
        self.assertEqual([path for path,_ in self.calls],[STAGE_PATH]*3)
        self.assertEqual(len(fixtures.store.get_reservations()),3)
        self.assertEqual(fixtures.store.get_job(self.job_id)['status'],'SCHEDULED')
        self.popen.assert_not_called()

    def test_validly_signed_but_wrong_ready_receipt_never_opens_running_barrier(self):
        original=self.service.stage
        def wrong(body):
            result=original(body)
            result['receipt']['generation']+=1
            return result
        with patch.object(self.service,'stage',side_effect=wrong):
            with self.assertRaises(PermissionError):self.dispatch()
        self.popen.assert_not_called()
        self.assertEqual(self.db.execute('SELECT phase FROM hybrid_staging_attempts').fetchone()[0],'STAGING')

    def test_workspace_disk_admission_failure_has_no_direct_nfs_fallback(self):
        self.manager.max_input_bytes=0
        with self.assertRaises(Exception):self.dispatch()
        self.popen.assert_not_called()
        self.assertEqual([path for path,_ in self.calls],[STAGE_PATH])
        with self.cache._session() as (db,_,__):
            self.assertEqual(db.execute('SELECT COUNT(*) FROM cache_pins').fetchone()[0],0)

    def test_lost_start_reply_preserves_reservation_and_blocks_renewal(self):
        import requests
        original=self.client.post
        def lose(url,**kwargs):
            result=original(url,**kwargs)
            if url.endswith(START_PATH):raise requests.Timeout('lost after remote Popen')
            return result
        self.client.post=lose
        with self.assertRaises(requests.Timeout):self.dispatch()
        self.popen.assert_called_once()
        job=fixtures.store.get_job(self.job_id)
        self.assertEqual(job['status'],'SCHEDULED')
        self.assertEqual(len(fixtures.store.get_reservations()),1)
        self.assertTrue(leases.get(job,'pi01')['renewal_blocked'])

    def test_expired_worker_lease_stops_staging_before_any_copy(self):
        import time
        job,lease=self.reserve()
        lease.update(issued_at=time.time()-31,expires_at=time.time()-1)
        plan=self.runtime.plans(job)[0]
        with self.assertRaises(Exception):
            self.client.call(self.node,STAGE_PATH,dict(stage_id='d'*32,plan=plan.document(),lease=lease))
        self.spawn.assert_not_called();self.popen.assert_not_called()

    def test_no_eligible_resource_candidate_does_not_reserve_or_stage(self):
        with patch.object(dispatcher,'select_best_node',return_value=None):
            self.assertFalse(self.dispatch())
        self.assertEqual(self.calls,[]);self.assertEqual(fixtures.store.get_reservations(),[])

    def test_stale_queued_snapshot_cannot_dispatch_a_now_cancelled_job(self):
        stale=fixtures.store.get_job(self.job_id)
        fixtures.store.control_job(self.job_id,'alice','cancel')
        self.assertFalse(dispatcher.dispatch_one_job(stale,storage_runtime=self.runtime))
        self.assertEqual(self.calls,[]);self.assertEqual(fixtures.store.get_reservations(),[])

    def test_signed_path_traversal_plan_is_rejected_without_file_or_cache_access(self):
        job,lease=self.reserve();plan=self.runtime.plans(job)[0].document()
        plan['source']['files'][0]['path']='../outside.py'
        with self.assertRaises(Exception):
            self.client.call(self.node,STAGE_PATH,dict(stage_id='d'*32,plan=plan,lease=lease))
        self.spawn.assert_not_called();self.popen.assert_not_called()

    def test_same_ready_stage_retries_reverify_and_do_not_fetch_again(self):
        job,lease=self.reserve();stage,replies=self.runtime.stage_group(job,[self.node],[lease])
        before=self.cache.metrics.snapshot()
        with patch.object(self.fetcher,'_spawn',side_effect=AssertionError('ready retry is warm')):
            repeated_stage,repeated=self.runtime.stage_group(job,[self.node],[leases.ensure(job,'pi01')])
        self.assertEqual(stage,repeated_stage);self.assertEqual(replies[0]['receipt'],repeated[0]['receipt'])
        self.assertEqual(self.cache.metrics.snapshot().staged_bytes,before.staged_bytes)

    def test_staging_client_bounds_peer_response_before_decoding(self):
        import requests
        response=requests.Response();response.status_code=200
        response._content=b'x'*(1024*1024+1);response._content_consumed=True
        with patch.object(self.client,'post',return_value=response):
            with self.assertRaisesRegex(ValueError,'bound'):
                self.client.call(self.node,STAGE_PATH,{})


if __name__=='__main__':unittest.main(verbosity=2)
