"""Service assembly on disposable LOCAL state; isolation/mount facts are fixtures.

These tests do not establish a production sandbox or deploy to any Pi.
"""
import importlib.util
import io
from pathlib import Path
import sys
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'agent'))
import test_storage_registry as fixtures
from hybrid_storage import cache_registry_schema, staging_schema, cleanup_schema, checkpoint_schema, restore_schema
from hybrid_storage.config import StorageConfig
from hybrid_storage.inventory_protocol import Credential
from hybrid_storage.checkpoint_publication import LocalCheckpointPublisher
from hybrid_storage.dataset_manifests import DatasetEntry
from hybrid_storage.inputs import object_key
from hybrid_storage.runtime_bootstrap import ControllerRuntime, WorkerRuntime, WorkerPaths, LaunchBoundary, dispatcher_cycle



class FixtureBoundary(LaunchBoundary):
    def preflight(self): return True
    def isolate(self, *args): raise AssertionError('No real trainer is launched')
    def terminate(self, *args): raise AssertionError('No real trainer exists')
    def quiescent(self, *args): return True


class RuntimeTests(unittest.TestCase):
    def setUp(self):
        self.f = fixtures.RegistryTests(methodName='runTest')
        self.addCleanup(self.f.doCleanups)
        self.f.setUp()
        self.registry = self.f.registry()
        self.key = Credential('pi02', '2'*32, b'2'*32)
        self.settings = StorageConfig('nfs', True, True, 'locality')
        self.checkpoints = self.f.base/'committed-checkpoints'
        self.checkpoints.mkdir(mode=0o700)
        self.session = Mock()

    def provision(self):
        for module in (cache_registry_schema, staging_schema, cleanup_schema, checkpoint_schema, restore_schema):
            module.migrate(self.f.db)
        with self.f.db:
            self.f.db.execute('''INSERT INTO cache_registry_workers
                (worker_id,auth_principal,auth_state,enabled,created_at_ns,updated_at_ns)
                VALUES (?,?,'ENROLLED',1,1,1)''', (self.key.worker,self.key.principal))
        LocalCheckpointPublisher(self.registry,self.checkpoints).initialize()

    def runtime(self, **kw):
        options = dict(configured_workers=(worker for worker in ['pi02']), inventory_root=self.f.base,
                       checkpoint_root=self.checkpoints, settings=self.settings, session=self.session)
        options.update(kw)
        result = ControllerRuntime(self.registry, [self.key], **options)
        self.addCleanup(result.close)
        return result

    def test_controller_constructor_has_no_io_and_consumes_worker_generator_once(self):
        before = list(self.f.db.iterdump())
        with patch('sqlite3.connect',side_effect=AssertionError('Constructor I/O')):
            r = self.runtime()
        self.assertEqual(r.workers, ('pi02',))
        self.assertFalse(r._started)
        self.assertEqual(list(self.f.db.iterdump()),before)
        self.assertIs(self.session.trust_env,False)

    def test_duplicate_scope_and_credentials_rejected(self):
        with self.assertRaises(ValueError): self.runtime(configured_workers=['pi02','pi02'])
        with self.assertRaises(ValueError):
            ControllerRuntime(self.registry,[self.key,self.key],configured_workers=['pi02'],
                inventory_root=self.f.base,checkpoint_root=self.checkpoints,settings=self.settings)

    def test_flags_off_cannot_construct_hybrid_runtime(self):
        with self.assertRaises(ValueError): self.runtime(settings=StorageConfig())

    def test_missing_schema_does_not_migrate_or_enroll(self):
        before = list(self.f.db.iterdump())
        with self.assertRaises(ValueError): self.runtime().start()
        self.assertEqual(list(self.f.db.iterdump()),before)
        self.assertFalse((self.f.base/'cache-inventory.lock').exists())

    def test_requires_operator_enrollment_and_does_not_repair_it(self):
        self.provision()
        for sql in ("UPDATE cache_registry_workers SET enabled=0", "UPDATE cache_registry_workers SET enabled=1,auth_principal='wrong'"):
            with self.f.db: self.f.db.execute(sql)
            before = list(self.f.db.iterdump())
            with self.assertRaises(PermissionError): self.runtime().start()
            self.assertEqual(list(self.f.db.iterdump()),before)

    def test_initialized_store_identity_required_before_inventory_activation(self):
        self.provision()
        other = self.f.base/'wrong-checkpoint-root'
        other.mkdir(mode=0o700)
        with self.assertRaises((ValueError,OSError)): self.runtime(checkpoint_root=other).start()
        self.assertFalse((self.f.base/'cache-inventory.lock').exists())

    def test_assembly_reuses_real_inventory_coverage_checkpoint_and_staging_services(self):
        self.provision()
        r = self.runtime().start()
        self.assertIs(r.dispatch.registry,self.registry)
        self.assertIs(r.dispatch.scheduler.coverage,r.coverage)
        self.assertIs(r.coverage.inventory,r.inventory)
        self.assertIs(r.dispatch.publisher,r.publisher)
        self.assertEqual(r.client.credentials,r.collector.credentials)
        self.assertEqual(r.restorer.credentials,r.client.credentials)
        r.close()
        self.assertFalse(r._started)
        # Caller-supplied transport remains caller-owned.
        self.session.close.assert_not_called()

    def test_failed_assembly_releases_singleton_inventory_lock(self):
        self.provision()
        with patch('checkpoint_recovery.CheckpointDispatch',side_effect=ValueError('Injected assembly failure')):
            with self.assertRaises(ValueError): self.runtime().start()
        self.runtime().start().close()

    def test_poll_checks_all_endpoints_before_any_network_request(self):
        self.provision()
        r = self.runtime().start()
        r.heartbeat.poll = Mock()
        invalid = [[], [dict(id='pi03',ip='192.168.10.101',port=5001)],
                   [dict(id='pi02',ip='100.91.135.18',port=5001)],
                   [dict(id='pi02',ip='127.0.0.1',port=5001)],
                   [dict(id='pi02',ip='192.168.10.150',port=True)]]
        for nodes in invalid:
            with self.assertRaises(ValueError): r.poll(nodes)
        r.heartbeat.poll.assert_not_called()
        node = dict(id='pi02',ip='192.168.10.150',port=5001)
        r.poll([node])
        r.heartbeat.poll.assert_called_once_with(node)

    def test_runtime_is_bound_to_database_owner_thread(self):
        self.provision()
        r = self.runtime().start()
        failures=[]
        def foreign():
            for action in (r.start,lambda:r.poll([]),r.close):
                try: action()
                except ValueError: failures.append(True)
        t=threading.Thread(target=foreign);t.start();t.join(5)
        self.assertEqual(len(failures),3)
        self.assertTrue(r._started)

    def test_transport_rejects_unpolled_or_other_hosts_and_never_follows_redirects(self):
        self.provision()
        r=self.runtime().start()
        url='http://192.168.10.150:5001/jobs/stage'
        with self.assertRaises(PermissionError): r._post(url,data=b'private')
        r.heartbeat.poll=Mock()
        r.poll([dict(id='pi02',ip='192.168.10.150',port=5001)])
        for wrong in ('https://192.168.10.150:5001/jobs/stage',
                      'http://192.168.10.101:5001/jobs/stage',
                      'http://100.91.135.18:5001/jobs/stage',
                      'http://192.168.10.150:5000/jobs/stage',
                      'http://user@192.168.10.150:5001/jobs/stage',
                      url+'#fragment'):
            with self.assertRaises(PermissionError): r._post(wrong,data=b'private')
        self.session.post.assert_not_called()
        r._post(url,data=b'private',allow_redirects=True)
        self.session.post.assert_called_once_with(url,data=b'private',allow_redirects=False)
        r._get(url,stream=True)
        self.session.get.assert_called_once_with(url,stream=True,allow_redirects=False)
        r.close()
        with self.assertRaises(PermissionError): r._post(url,data=b'private')

    def module(self):
        calls=[]
        module=SimpleNamespace()
        for name in ('reconcile_cancellations','reconcile_scheduled_jobs','monitor_running_jobs','dispatch_queued_jobs'):
            setattr(module,name,lambda name=name,**kw:calls.append((name,kw)))
        return module,calls

    def test_off_cycle_preserves_exact_legacy_order_and_arguments(self):
        module,calls=self.module()
        dispatcher_cycle(module)
        self.assertEqual(calls,[(name,{}) for name in ('reconcile_cancellations','reconcile_scheduled_jobs','monitor_running_jobs','dispatch_queued_jobs')])

    def test_hybrid_cycle_polls_before_reconciliation_and_passes_same_runtime_everywhere(self):
        self.provision()
        r=self.runtime().start()
        module,calls=self.module()
        r.poll=lambda nodes:calls.append(('inventory',nodes))
        nodes=[dict(id='pi02',ip='192.168.10.150',port=5001)]
        with patch('scheduler.load_nodes',return_value=nodes): dispatcher_cycle(module,r)
        self.assertEqual(calls[0],('inventory',nodes))
        self.assertEqual([c[1] for c in calls[1:]],[dict(storage_runtime=r.dispatch)]*4)

    def test_admission_freeze_still_reconciles_cancel_and_running_work(self):
        module,calls=self.module()
        dispatcher_cycle(module,admission=False)
        self.assertEqual(len(calls),3)
        with self.assertRaises(TypeError): dispatcher_cycle(module,admission='false')

    def test_dispatcher_entrypoint_uses_started_runtime_and_closes_on_exit(self):
        import dispatcher
        self.provision()
        r=self.runtime().start()
        calls=[]
        r.poll=lambda nodes:calls.append(('inventory',nodes))
        with patch('hybrid_storage.config.load_config',return_value=self.settings), \
             patch.object(dispatcher,'DB_PATH',self.f.db_path), \
             patch('job_store.initialize_database'), patch.object(dispatcher,'init_recovery_table'), \
             patch.object(dispatcher.ownership,'start_renewals'), \
             patch('scheduler.load_nodes',return_value=[]), \
             patch.object(dispatcher,'reconcile_cancellations') as cancelled, \
             patch.object(dispatcher,'reconcile_scheduled_jobs') as scheduled, \
             patch.object(dispatcher,'monitor_running_jobs') as running, \
             patch.object(dispatcher,'dispatch_queued_jobs') as queued, \
             patch.object(dispatcher.time,'sleep',side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt): dispatcher.main(storage_runtime=r)
        self.assertFalse(r._started)
        for action in (cancelled,scheduled,running,queued):
            action.assert_called_once_with(storage_runtime=r.dispatch)
        self.assertEqual(calls,[('inventory',[])])

    def test_dispatcher_refuses_registry_for_another_database_before_launch(self):
        import dispatcher
        self.provision()
        r=self.runtime().start()
        with patch('hybrid_storage.config.load_config',return_value=self.settings), \
             patch.object(dispatcher,'DB_PATH',self.f.base/'wrong.db'), \
             patch.object(dispatcher,'dispatch_queued_jobs') as queued:
            with self.assertRaises(ValueError): dispatcher.main(storage_runtime=r)
        queued.assert_not_called()

    def test_default_dispatcher_entrypoint_keeps_legacy_call_arguments(self):
        import dispatcher
        with patch('hybrid_storage.config.load_config',return_value=StorageConfig()), \
             patch('job_store.initialize_database'), patch.object(dispatcher,'init_recovery_table'), \
             patch.object(dispatcher.ownership,'start_renewals'), \
             patch.object(dispatcher,'reconcile_cancellations') as cancelled, \
             patch.object(dispatcher,'reconcile_scheduled_jobs') as scheduled, \
             patch.object(dispatcher,'monitor_running_jobs') as running, \
             patch.object(dispatcher,'dispatch_queued_jobs') as queued, \
             patch.object(dispatcher.time,'sleep',side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt): dispatcher.main()
        for action in (cancelled,scheduled,running,queued): action.assert_called_once_with()

    def worker(self, boundary=None):
        paths={name:self.f.base/name for name in ('worker','cache','workspaces','checkpoint_spool','inputs')}
        for path in paths.values(): path.mkdir(mode=0o700)
        spec=importlib.util.spec_from_file_location('runtime_fixture_agent',ROOT/'agent/agent.py')
        module=importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        module.BASE_DIR=paths['worker'];module.node_id=lambda:'pi02'
        r=WorkerRuntime(module,WorkerPaths(**paths),self.key,boundary or FixtureBoundary(),
            source='192.168.10.214:/srv/ml-shared/objects',cache_bytes=1024**2,workspace_bytes=1024**2,
            reserve_bytes=0,watchdog=False)
        self.addCleanup(r.close)
        return r

    def test_overlapping_worker_roots_rejected(self):
        with self.assertRaises(ValueError):
            WorkerPaths(self.f.base,self.f.base/'cache',self.f.base/'work',self.f.base/'spool',self.f.base/'inputs')

    def test_missing_boundary_fails_before_cache_files_or_flask_hooks(self):
        boundary=FixtureBoundary();boundary.preflight=lambda:False
        r=self.worker(boundary)
        with self.assertRaises(PermissionError): r.start()
        self.assertEqual(list(r.paths.cache.iterdir()),[])
        self.assertEqual(r.module.app.extensions,{})

    def test_mount_failure_fails_before_cache_initialization(self):
        r=self.worker()
        with patch('hybrid_storage.nfs_mount.require_input_mount',side_effect=OSError('Unavailable NFS')):
            with self.assertRaises(OSError): r.start()
        self.assertEqual(list(r.paths.cache.iterdir()),[])

    def test_public_or_tailscale_nfs_source_rejected_at_construction(self):
        r=self.worker()
        with self.assertRaises(ValueError):
            WorkerRuntime(r.module,r.paths,self.key,FixtureBoundary(),
                source='100.73.90.91:/srv/ml-shared/objects',cache_bytes=1024,workspace_bytes=1024)

    def test_nonprivate_root_rejected_before_cache_initialization(self):
        r=self.worker()
        r.paths.cache.chmod(0o755)
        with self.assertRaises(PermissionError): r.start()
        self.assertEqual(list(r.paths.cache.iterdir()),[])

    def test_bootstrap_after_flask_has_served_or_with_live_process_is_rejected(self):
        r=self.worker()
        r.module.job_process=Mock()
        with self.assertRaises(ValueError): r.start()
        r.module.job_process=None
        r.module.app.test_client().get('/health')
        with self.assertRaises(ValueError): r.start()
        self.assertEqual(list(r.paths.cache.iterdir()),[])

    def test_worker_assembly_installs_real_authentication_and_closes_owned_resources(self):
        r=self.worker()
        with patch('hybrid_storage.nfs_mount.require_input_mount'):
            r.start()
        self.assertIs(r.module.app.extensions['hybrid_staging'],r.service)
        self.assertIs(r.module.app.extensions['checkpoint_publication'],r.checkpoints)
        self.assertIs(r.module.app.extensions['cache_inventory'],r.reporter)
        self.assertIs(r.fetcher.transfer_metrics,r.reporter.transfer_metrics)
        client=r.module.app.test_client()
        for path in ('/jobs/start','/jobs/stop','/jobs/renew'):
            self.assertEqual(client.post(path,json={}).status_code,403)
        self.assertTrue(r.close())
        self.assertIsNone(r.reporter._owner)

    def test_worker_shutdown_keeps_watchdog_and_owners_when_attempt_still_busy(self):
        r=self.worker()
        with patch('hybrid_storage.nfs_mount.require_input_mount'): r.start()
        with patch.object(r.service,'busy',return_value=True), patch.object(r.service,'close') as close:
            self.assertFalse(r.close())
            close.assert_not_called()
            self.assertIsNotNone(r.reporter._owner)
            self.assertTrue(r._started)

    def test_partial_worker_setup_cannot_restart_or_serve_as_legacy(self):
        r=self.worker()
        with patch('hybrid_storage.nfs_mount.require_input_mount'), patch('checkpoint_service.install_checkpoint_service',side_effect=ValueError('Injected setup failure')):
            with self.assertRaises(ValueError): r.start()
        self.assertTrue(r._failed)
        self.assertIsNone(r.reporter._owner)
        with self.assertRaises(ValueError): r.start()

    def test_failed_bootstrap_keeps_resources_when_staging_cannot_yet_close(self):
        r=self.worker()
        with patch('hybrid_storage.nfs_mount.require_input_mount'), \
             patch('checkpoint_service.install_checkpoint_service',side_effect=ValueError('Injected setup failure')), \
             patch('staging.StagingService.close',return_value=False):
            with self.assertRaises(RuntimeError): r.start()
        self.assertTrue(r._cleanup_pending)
        self.assertIsNotNone(r.reporter._owner)
        self.assertTrue(r.close())
        self.assertIsNone(r.reporter._owner)

    def test_factories_exchange_authenticated_inventory_and_compute_real_coverage(self):
        self.provision()
        worker=self.worker()
        with patch('hybrid_storage.nfs_mount.require_input_mount'): worker.start()
        data=b'local disposable verified cache object'
        artifact=self.registry.register_artifact('alice',io.BytesIO(data),expected_size=len(data))
        dataset=self.registry.create_dataset('alice',[DatasetEntry('a.bin','file',artifact.sha256,len(data))])
        target=worker.paths.cache/'objects'/object_key(artifact.sha256)
        target.parent.parent.mkdir(mode=0o700,exist_ok=True)
        target.parent.mkdir(mode=0o700,exist_ok=True)
        target.write_bytes(data);target.chmod(0o400)
        worker.cache.verify(artifact.sha256,len(data))
        controller=self.runtime().start()
        http=worker.module.app.test_client()
        def post(node,raw,headers):
            response=http.post('/resources/cache-inventory',data=raw,headers=headers)
            self.assertEqual(response.status_code,200)
            return response.data,response.headers['X-DL-Inventory-MAC']
        controller.heartbeat.transport=post
        node=dict(id='pi02',ip='192.168.10.150',port=5001)
        controller.poll([node])
        self.assertTrue(controller.inventory.eligible('pi02'))
        observed=controller.coverage.for_dataset('alice','pi02',dataset.dataset_id,dataset.version)
        self.assertEqual(observed.missing_bytes,0)
        self.assertEqual(observed.verified_cached_bytes,len(data))
        self.assertTrue(observed.inventory_eligible)
        # An invalid reply must invalidate previous certainty, not retain a warm node.
        controller.heartbeat.transport=lambda *args:(b'{}','0'*64)
        controller.poll([node])
        self.assertFalse(controller.inventory.eligible('pi02'))


if __name__=='__main__': unittest.main()
