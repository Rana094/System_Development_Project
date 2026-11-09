"""8.2 fake/Pi-less publisher and authenticated HTTP tests in disposable roots.

The production checkpoint-execution gate stays CLOSED. Fixtures explicitly build
valid RUNNING staging rows to exercise the publication boundary independently;
they are not a deployed/launchable checkpoint-mode hybrid job or a DDP trainer.
"""
from contextlib import closing
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import sqlite3
import sys
import time
import unittest
from unittest.mock import patch,Mock
import zipfile

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT));sys.path.insert(0,str(ROOT/'agent'))
import test_storage_registry as fixtures
import execution_leases as leases
import execution_lease as worker_lease
from hybrid_storage import checkpoint_schema as schema,staging_schema
from hybrid_storage import checkpoint_publication as publication
from hybrid_storage.config import StorageConfig
from hybrid_storage.staging_lifecycle import StagingLifecycle
from hybrid_storage.workspaces import WorkspacePlan,AttemptIdentity
from hybrid_storage.inventory_protocol import Credential
from hybrid_storage.checkpoint_transfer import CheckpointCollector,REQUEST,RECEIPT,MAC,header,signature,request_document
from checkpoint_protocol import group_identity
from checkpoint_service import CheckpointService,install_checkpoint_service


class PublicationTests(unittest.TestCase):
    def setUp(self):
        self.f=fixtures.RegistryTests(methodName='runTest')
        self.addCleanup(self.f.doCleanups);self.f.setUp()
        self.db=self.f.db;self.registry=self.f.registry()
        self.world=3 if 'ddp' in self._testMethodName else 1
        self.job_id='checkpoint-boundary'
        document=json.loads((ROOT/'examples/job.checkpoint.json').read_text())
        document.update(project=str(self.f.project),dataset=str(self.f.data),
                        execution=dict(mode='distributed' if self.world>1 else 'single',workers=self.world))
        spec=fixtures.validate_spec(document,self.f.base)
        fixtures.store.add_job(self.job_id,1,job_type='custom',owner='alice',status='RECEIVED',job_config=spec,storage_backend='nfs')
        workspace=Path(fixtures.job_storage.stage_job(self.job_id)['storage_path'])
        self.manifest=(workspace/'manifest.json').read_bytes()
        source=self.f.source(self.registry,workspace=workspace)
        data=self.f.dataset(self.registry,workspace=workspace,**(
            dict(owner='bob',readers=['alice']) if 'dataset_acl' in self._testMethodName else {}))
        self.data=data
        self.binding=self.registry.bind_job('alice',self.job_id,legacy_manifest_bytes=self.manifest,
            source_sha256=source.sha256,dataset_id=data.dataset_id,dataset_version=data.version)
        staging_schema.migrate(self.db);schema.migrate(self.db)
        self.context=dict(world_size=self.world,master_addr='192.0.2.1',master_port=29601)
        with self.db:
            self.db.execute("UPDATE jobs SET status='SCHEDULED',node_id='pi01',node_ip='192.0.2.1' WHERE job_id=?",(self.job_id,))
            self.db.execute("INSERT INTO job_attempts(job_id,attempt_number,node_id,status) VALUES(?,1,'pi01','SCHEDULED')",(self.job_id,))
            for rank in range(self.world):
                node=f'pi{rank+1:02}'
                self.db.execute('INSERT INTO resource_reservations VALUES(?,?,?,?)',(node,self.job_id,json.dumps(spec['resources_per_worker']),time.time()))
                record=dict(job_id=self.job_id,manifest_sha256=hashlib.sha256(self.manifest).hexdigest(),compatibility_key='compat')
                self.db.execute('INSERT INTO job_environments VALUES(?,?,?,?,?,?,?)',(self.job_id,node,'isolated','env',json.dumps(record),1,1))
                if self.world>1:
                    self.db.execute('INSERT INTO job_ranks VALUES(?,?,?,?,?,?,?,?,?)',(self.job_id,0,rank,node,f'192.0.2.{rank+1}',5000,'RUNNING',1,None))
            if self.world>1:self.db.execute('INSERT INTO distributed_attempts VALUES(?,?,?,?,?,?)',(self.job_id,0,'STARTING',json.dumps(self.context),None,1))
        self.job=fixtures.store.get_job(self.job_id)
        self.leases=[leases.ensure(self.job,f'pi{rank+1:02}') for rank in range(self.world)]
        plans=tuple(WorkspacePlan(AttemptIdentity(self.job_id,self.job_id+':0',rank),self.binding.sha256,source,data) for rank in range(self.world))
        life=StagingLifecycle(self.registry,settings=StorageConfig('nfs',True,True,'resource'))
        stage=life.begin('alice',self.job_id,plans)
        for rank,plan in enumerate(plans):life.record_ready('alice',stage,rank,plan_sha256=plan.sha256,receipt_sha256='a'*64)
        with self.db:
            self.db.execute("UPDATE jobs SET status='RUNNING' WHERE job_id=?",(self.job_id,))
            self.db.execute("UPDATE job_attempts SET status='RUNNING' WHERE job_id=?",(self.job_id,))
            self.db.execute("UPDATE hybrid_staging_attempts SET phase='STARTED',updated_at_ns=? WHERE stage_id=?",(time.time_ns(),stage.stage_id))
            if self.world>1:self.db.execute("UPDATE distributed_attempts SET phase='RUNNING' WHERE job_id=?",(self.job_id,))
        self.root=self.f.base/'private-checkpoints';self.root.mkdir(mode=0o700)
        self.publisher=publication.LocalCheckpointPublisher(self.registry,self.root,reserve_free_bytes=0)
        self.publisher.initialize()

    def archive(self,step=5,completed=False,**changes):
        meta=dict(format_version=2 if self.world>1 else 1,job_id=self.job_id,
            manifest_sha256=self.job['storage_manifest_sha256'],attempt_token=self.job_id+':0',
            compatibility_key='compat',global_step=step,completed=completed,created_at=1)
        states={f'rank-{rank}.pt':f'opaque-tensor-state-{rank}'.encode() for rank in range(self.world)}
        if self.world==1:
            states={'state.pt':b'opaque-tensor-state'}
            meta.update(state_sha256=hashlib.sha256(states['state.pt']).hexdigest(),lease_fence=worker_lease.fence(self.leases[0]))
        else:
            save_id=hashlib.sha256(f'{step}:{completed}'.encode()).hexdigest()[:32]
            hashes={str(rank):hashlib.sha256(states[f'rank-{rank}.pt']).hexdigest() for rank in range(self.world)}
            meta.update(kind='ddp',world_size=self.world,checkpoint_id=save_id,
                group_id=group_identity(self.job_id,self.job_id+':0',self.job['storage_manifest_sha256'],'compat',self.context),
                shared_state_sha256='c'*64,rank_state_sha256=hashes,
                rank_acknowledgments=[dict(rank=rank,checkpoint_id=save_id,state_sha256=hashes[str(rank)],accepted=True) for rank in range(self.world)],
                rank_fences={str(rank):worker_lease.fence(lease) for rank,lease in enumerate(self.leases)})
        meta.update(changes);stream=io.BytesIO()
        with zipfile.ZipFile(stream,'w',compression=zipfile.ZIP_STORED) as archive:
            archive.writestr('metadata.json',json.dumps(meta))
            for name,state in states.items():archive.writestr(name,state)
        return stream.getvalue()

    def publish(self,raw=None,identity='1'*32,head=None,**kwargs):
        raw=self.archive() if raw is None else raw
        return self.publisher.publish('alice',self.job_id,'pi01',io.BytesIO(raw),publication_id=identity,
            expected_head=head,expected_sha256=hashlib.sha256(raw).hexdigest(),expected_size=len(raw),**kwargs)

    def test_immutable_snapshot_atomic_head_and_existing_latest_untouched(self):
        latest=Path(self.job['storage_path'])/'checkpoints/latest.dlckpt';latest.write_bytes(b'preexisting legacy bytes')
        first=self.publish();before=(self.root/'commits'/(first.publication_id+'.dlckpt')).read_bytes()
        second=self.publish(self.archive(step=6),identity='2'*32,head=first.publication_id)
        self.assertEqual(self.publisher.head('alice',self.job_id),second)
        self.assertEqual((self.root/'commits'/(first.publication_id+'.dlckpt')).read_bytes(),before)
        self.assertEqual(latest.read_bytes(),b'preexisting legacy bytes')
        self.assertEqual(fixtures.store.get_job(self.job_id)['checkpoint_path'],str(self.root/'commits'/(second.publication_id+'.dlckpt')))
        self.assertEqual(len(list((self.root/'commits').iterdir())),2)
        with self.assertRaises(sqlite3.IntegrityError),self.db:
            self.db.execute("DELETE FROM hybrid_checkpoint_publications")
        with self.assertRaises(sqlite3.IntegrityError),self.db:
            self.db.execute("UPDATE hybrid_checkpoint_publications SET global_step=99 WHERE publication_id=?",(first.publication_id,))

    def test_idempotent_commit_ack_after_expiry_never_rewinds_newer_head(self):
        raw=self.archive();first=self.publish(raw)
        second=self.publish(self.archive(step=6),identity='2'*32,head=first.publication_id)
        with self.db:self.db.execute('UPDATE execution_leases SET expires_at=0,expires_clock=0')
        self.assertEqual(self.publish(raw),first)
        self.assertEqual(self.publisher.head('alice',self.job_id),second)
        with self.assertRaises(ValueError):self.publish(self.archive(step=99))

    def test_wrong_owner_stale_attempt_or_revoked_lease_cannot_publish(self):
        raw=self.archive()
        with self.assertRaises(PermissionError):
            self.publisher.publish('bob',self.job_id,'pi01',io.BytesIO(raw),publication_id='1'*32,
                expected_head=None,expected_sha256=hashlib.sha256(raw).hexdigest(),expected_size=len(raw))
        with self.assertRaises(ValueError):self.publish(self.archive(attempt_token=self.job_id+':9'))
        with self.db:self.db.execute("UPDATE execution_leases SET revoked=1")
        with self.assertRaises(ValueError):self.publish(identity='2'*32)
        self.assertIsNone(self.publisher.head('alice',self.job_id))

    def test_missing_reservation_cannot_publish(self):
        with self.db:self.db.execute('DELETE FROM resource_reservations WHERE job_id=?',(self.job_id,))
        with self.assertRaises(ValueError):self.publish()
        self.assertIsNone(self.publisher.head('alice',self.job_id))

    def test_dataset_acl_revoked_cannot_publish_registered_inputs(self):
        self.registry.set_readers('bob',self.data.dataset_id,[])
        with self.assertRaises(PermissionError):self.publish()
        self.assertIsNone(self.publisher.head('alice',self.job_id))

    def test_cancellation_or_expiry_during_stream_cannot_commit_or_release_existing_head(self):
        first=self.publish();raw=self.archive(step=6)
        owner=self
        class Cancelled(io.BytesIO):
            def read(stream,size):
                value=super().read(size)
                with owner.db:owner.db.execute("UPDATE jobs SET status='CANCELLING',cancel_requested_at=? WHERE job_id=?",(time.time(),owner.job_id))
                return value
        with self.assertRaises(ValueError):
            self.publisher.publish('alice',self.job_id,'pi01',Cancelled(raw),publication_id='2'*32,
                expected_head=first.publication_id,expected_sha256=hashlib.sha256(raw).hexdigest(),expected_size=len(raw))
        self.assertEqual(self.publisher.head('alice',self.job_id),first)

    def test_bad_sizes_hashes_names_and_metadata_never_commit(self):
        for number,changes in enumerate((dict(manifest_sha256='f'*64),dict(state_sha256='f'*64),dict(compatibility_key='wrong'))):
            with self.assertRaises(ValueError):self.publish(self.archive(**changes),identity=f'{number+1:032x}')
        raw=self.archive()
        for size in (len(raw)-1,len(raw)+1):
            with self.assertRaises(ValueError):
                self.publisher.publish('alice',self.job_id,'pi01',io.BytesIO(raw),publication_id=f'{size:032x}',
                    expected_head=None,expected_sha256=hashlib.sha256(raw).hexdigest(),expected_size=size)
        broken=io.BytesIO(raw)
        with zipfile.ZipFile(broken,'a') as archive:archive.writestr('../unsafe',b'bad')
        with self.assertRaises(ValueError):self.publish(broken.getvalue(),identity='e'*32)
        self.assertIsNone(self.publisher.head('alice',self.job_id))

    def test_ddp_complete_three_rank_bundle_and_all_current_fences_required(self):
        first=self.publish();self.assertEqual(first.kind,'ddp')
        for number,changes in enumerate((dict(rank_acknowledgments=[]),dict(group_id='f'*64),dict(rank_fences={}))):
            with self.assertRaises((ValueError,PermissionError)):self.publish(self.archive(step=6,**changes),identity=f'{number+2:032x}',head=first.publication_id)
        with self.db:self.db.execute("UPDATE execution_leases SET expires_at=0,expires_clock=0 WHERE node_id='pi03'")
        with self.assertRaises(ValueError):self.publish(self.archive(step=7),identity='f'*32,head=first.publication_id)
        self.assertEqual(self.publisher.head('alice',self.job_id),first)

    def test_regressing_progress_and_completion_keep_previous_committed_snapshot(self):
        first=self.publish(self.archive(completed=True))
        for number,raw in enumerate((self.archive(step=4,completed=True),self.archive(step=6))):
            with self.assertRaises(ValueError):self.publish(raw,identity=f'{number+2:032x}',head=first.publication_id)
        self.assertEqual(self.publisher.head('alice',self.job_id),first)
        self.assertEqual(len(list((self.root/'commits').iterdir())),3)  # Rejected immutable orphans retained.

    def test_file_before_db_crash_never_selects_or_promotes_orphan_on_restart(self):
        class Crash(BaseException):pass
        first=self.publish();real=publication._rename_noreplace
        def interrupted(*args):real(*args);raise Crash('crash after immutable rename')
        with patch.object(publication,'_rename_noreplace',side_effect=interrupted):
            with self.assertRaises(Crash):self.publish(self.archive(step=6),identity='2'*32,head=first.publication_id)
        with closing(self.f.connect()) as db:
            publisher=publication.LocalCheckpointPublisher(self.f.registry(db),self.root,reserve_free_bytes=0)
            publisher.initialize();self.assertEqual(publisher.head('alice',self.job_id),first)
            with self.assertRaises(PermissionError):publisher.committed('alice','2'*32)
        self.assertEqual(self.db.execute("SELECT phase FROM hybrid_checkpoint_publications WHERE publication_id=?",('2'*32,)).fetchone()[0],'PUBLISHING')

    def test_disk_error_and_noreplace_failure_do_not_advance_head(self):
        first=self.publish()
        for number,call in enumerate(('space','_rename_noreplace')):
            with patch.object(publication,call,side_effect=OSError('injected disk failure')):
                with self.assertRaises(OSError):self.publish(self.archive(step=6),identity=f'{number+2:032x}',head=first.publication_id)
        self.assertEqual(self.publisher.head('alice',self.job_id),first)

    def test_two_real_sqlite_writers_compare_and_swap_one_head_without_overwrite(self):
        from concurrent.futures import ThreadPoolExecutor
        import threading
        barrier=threading.Barrier(2)
        owner=self
        class Concurrent(io.BytesIO):
            waited=False
            def read(stream,size):
                if not stream.waited:
                    stream.waited=True;barrier.wait(timeout=10)
                return super().read(size)
        def publish(number):
            raw=owner.archive(step=number+5)
            with closing(owner.f.connect()) as db:
                registry=owner.f.registry(db)
                publisher=publication.LocalCheckpointPublisher(registry,owner.root,reserve_free_bytes=0)
                publisher.initialize()
                try:
                    return publisher.publish('alice',owner.job_id,'pi01',Concurrent(raw),publication_id=f'{number+1:032x}',
                        expected_head=None,expected_sha256=hashlib.sha256(raw).hexdigest(),expected_size=len(raw))
                except publication.base.RegistryConflict:return None
        with ThreadPoolExecutor(max_workers=2) as pool:outcomes=list(pool.map(publish,(0,1)))
        winners=[outcome for outcome in outcomes if outcome is not None]
        self.assertEqual(len(winners),1)
        self.assertEqual(self.publisher.head('alice',self.job_id),winners[0])
        self.assertEqual(dict(self.db.execute('SELECT phase,COUNT(*) FROM hybrid_checkpoint_publications GROUP BY phase')),
                         dict(COMMITTED=1,REJECTED=1))
        self.assertEqual(len(list((self.root/'commits').iterdir())),2)

    def test_bad_publication_key_or_declared_bound_refused_before_temp_creation(self):
        raw=self.archive()
        for identity,size,digest in (('../traversal',len(raw),'a'*64),('1'*32,True,'a'*64),
                                     ('1'*32,len(raw),'not-a-digest'),('1'*32,self.publisher.max_bytes+1,'a'*64)):
            with self.assertRaises(ValueError):
                self.publisher.publish('alice',self.job_id,'pi01',io.BytesIO(raw),publication_id=identity,
                    expected_head=None,expected_sha256=digest,expected_size=size)
        self.assertEqual(list((self.root/'pending').iterdir()),[])
        self.assertIsNone(self.publisher.head('alice',self.job_id))

    def test_expired_after_rename_before_commit_cannot_become_head(self):
        real=publication._rename_noreplace
        def expire(*args):
            real(*args)
            with self.db:self.db.execute('UPDATE execution_leases SET expires_at=0,expires_clock=0')
        with patch.object(publication,'_rename_noreplace',side_effect=expire):
            with self.assertRaises(ValueError):self.publish()
        self.assertIsNone(self.publisher.head('alice',self.job_id))
        self.assertEqual(self.db.execute('SELECT phase FROM hybrid_checkpoint_publications').fetchone()[0],'REJECTED')

    def test_private_local_store_no_symlinks_wrong_root_or_export_overlap(self):
        with self.assertRaises(ValueError):publication.LocalCheckpointPublisher(self.registry,self.f.objects/'private')
        alternate=self.f.base/'alternate';alternate.mkdir(mode=0o700)
        with self.assertRaises(ValueError):publication.LocalCheckpointPublisher(self.registry,alternate).initialize()
        link=self.f.base/'linked';link.symlink_to(self.root,target_is_directory=True)
        with self.assertRaises(OSError):publication.LocalCheckpointPublisher(self.registry,link).initialize()
        first=self.publish();target=self.root/'commits'/(first.publication_id+'.dlckpt')
        target.chmod(0o600);target.write_bytes(b'corrupt')
        with self.assertRaises(PermissionError):self.publisher.head('alice',self.job_id)
        target.chmod(0o400)
        with self.assertRaises(ValueError):self.publisher.head('alice',self.job_id)

    def test_migration_idempotent_preserves_existing_rows_versions_and_rejects_partial(self):
        before={name:[tuple(row) for row in self.db.execute('SELECT * FROM '+name)]
                for name in ('jobs','storage_job_inputs','hybrid_staging_attempts','execution_leases')}
        with self.db:self.db.execute('PRAGMA user_version=81')
        schema.migrate(self.db);self.assertEqual(schema.migration_plan(self.db)['action'],'unchanged')
        for name,rows in before.items():self.assertEqual([tuple(row) for row in self.db.execute('SELECT * FROM '+name)],rows)
        self.assertEqual(self.db.execute('PRAGMA user_version').fetchone()[0],81)
        with self.db:self.db.execute('CREATE TABLE hybrid_checkpoint_future (value TEXT)')
        with self.assertRaises(ValueError):schema.migrate(self.db)

    def worker(self):
        worker=self.f.base/'worker';worker.mkdir(mode=0o700)
        directory=worker/'worker-storage/jobs'/self.job_id;directory.mkdir(parents=True)
        (directory/'checkpoints').mkdir()
        record=dict(job_id=self.job_id,manifest_sha256=self.job['storage_manifest_sha256'],compatibility_key='compat')
        (directory/'prepared.json').write_text(json.dumps(record))
        (directory/'request.json').write_text(json.dumps(dict(manifest=json.loads(self.manifest))))
        (directory/'checkpoints/latest.dlckpt').write_bytes(self.archive())
        module_spec=importlib.util.spec_from_file_location('publication_test_worker',ROOT/'agent/agent.py')
        self.agent=importlib.util.module_from_spec(module_spec)
        with patch('subprocess.run',return_value=Mock(returncode=1)):module_spec.loader.exec_module(self.agent)
        self.agent.BASE_DIR=worker;self.agent.node_id=lambda:'pi01'
        spool=self.f.base/'worker-checkpoint-spool';spool.mkdir(mode=0o700)
        self.key=Credential('pi01','a'*32,b'k'*32)
        self.service=CheckpointService(self.key,worker_root=worker,spool_root=spool,reserve_free_bytes=0)
        install_checkpoint_service(self.agent,self.service)
        worker_lease.accept(worker,self.leases[0],self.job_id,self.job_id+':0','pi01')
        self.http=self.agent.app.test_client();self.node=dict(id='pi01',ip='192.0.2.1',port=5000)
        self.source=directory/'checkpoints/latest.dlckpt'

    def get(self,url,**kwargs):
        import requests
        response=self.http.get('/'+url.split('/',3)[3],headers=kwargs['headers'])
        result=requests.Response();result.status_code=response.status_code;result.headers.update(response.headers)
        result._content=response.data;result._content_consumed=True;response.close()
        return result

    def test_authenticated_existing_http_get_to_immutable_publication(self):
        self.worker();before=self.source.read_bytes();collector=CheckpointCollector(self.publisher,{'pi01':self.key},get=self.get)
        receipt=collector.sync('alice',self.job_id,self.node,self.leases[0],publication_id='1'*32,expected_head=None)
        self.assertEqual(receipt,self.publisher.head('alice',self.job_id))
        self.assertFalse(self.service.slot.locked());self.assertEqual(list(self.service.spool.iterdir()),[])
        self.assertEqual(self.source.read_bytes(),before)

    def test_ddp_authenticated_group_transfer_keeps_one_complete_archive(self):
        self.worker();before=self.source.read_bytes()
        collector=CheckpointCollector(self.publisher,{'pi01':self.key},get=self.get)
        receipt=collector.sync('alice',self.job_id,self.node,self.leases[0],publication_id='1'*32,expected_head=None)
        self.assertEqual(receipt.kind,'ddp');self.assertEqual(receipt.sha256,hashlib.sha256(before).hexdigest())
        self.assertEqual((self.root/'commits'/('1'*32+'.dlckpt')).read_bytes(),before)

    def test_unsigned_wrong_key_stale_and_restore_post_are_rejected(self):
        self.worker();path=f'/jobs/{self.job_id}/custom-checkpoint'
        self.assertEqual(self.http.get(path).status_code,403)
        self.assertEqual(self.http.post(path,data={}).status_code,403)
        doc=request_document(self.key,self.job_id,self.leases[0])
        for changes in (dict(sent_at_ns=1),dict(worker_id='pi02'),dict(generation=999)):
            bad=dict(doc,**changes)
            result=self.http.get(path,headers={REQUEST:header(bad),MAC:signature(self.key,path,bad)})
            self.assertEqual(result.status_code,403);self.assertNotIn(str(self.f.base),result.get_data(as_text=True))
        result=self.http.get(path,headers={REQUEST:header(doc),MAC:'0'*64});self.assertEqual(result.status_code,403)

    def test_response_replay_tamper_or_truncated_stream_cannot_publish(self):
        self.worker();saved=[]
        def replay(url,**kwargs):
            response=self.get(url,**kwargs)
            if not saved:saved.append((dict(response.headers),response.content))
            else:response.headers.update(saved[0][0]);response._content=saved[0][1]
            return response
        collector=CheckpointCollector(self.publisher,{'pi01':self.key},get=replay)
        first=collector.sync('alice',self.job_id,self.node,self.leases[0],publication_id='1'*32,expected_head=None)
        with self.assertRaises(ValueError):collector.sync('alice',self.job_id,self.node,self.leases[0],publication_id='2'*32,expected_head=first.publication_id)
        def truncate(url,**kwargs):
            response=self.get(url,**kwargs);response._content=response.content[:-8];return response
        collector.get=truncate
        with self.assertRaises(ValueError):collector.sync('alice',self.job_id,self.node,self.leases[0],publication_id='3'*32,expected_head=first.publication_id)
        def tamper(url,**kwargs):
            response=self.get(url,**kwargs);raw=response.content;response._content=raw[:-1]+bytes([raw[-1]^1]);return response
        collector.get=tamper
        with self.assertRaises(ValueError):collector.sync('alice',self.job_id,self.node,self.leases[0],publication_id='4'*32,expected_head=first.publication_id)
        self.assertEqual(self.publisher.head('alice',self.job_id),first)

    def test_transfer_connection_loss_retains_partial_without_accepting_it(self):
        import requests
        self.worker()
        def interrupted(url,**kwargs):
            response=self.get(url,**kwargs);raw=response.content
            def chunks(size):
                yield raw[:100]
                raise requests.ConnectionError('disconnected')
            response.iter_content=chunks
            return response
        collector=CheckpointCollector(self.publisher,{'pi01':self.key},get=interrupted)
        with self.assertRaises(requests.ConnectionError):collector.sync('alice',self.job_id,self.node,self.leases[0],publication_id='1'*32,expected_head=None)
        self.assertIsNone(self.publisher.head('alice',self.job_id))
        self.assertEqual(self.db.execute('SELECT phase FROM hybrid_checkpoint_publications').fetchone()[0],'REJECTED')
        self.assertTrue((self.root/'pending'/('1'*32)/'candidate').is_file())

    def test_empty_http_chunks_cannot_hide_trailing_bytes_or_spin_forever(self):
        from hybrid_storage.checkpoint_transfer import _ResponseStream
        stream=_ResponseStream(Mock(iter_content=lambda size:iter([b'',b'abc',b'',b'extra'])))
        self.assertEqual(stream.read(3),b'abc');self.assertEqual(stream.read(1),b'e')
        stream=_ResponseStream(Mock(iter_content=lambda size:iter([b'']*33)))
        with self.assertRaises(ValueError):stream.read(1)

    def test_explicit_migration_and_local_private_root_required(self):
        with closing(self.f.connect()) as db:
            db.execute('PRAGMA synchronous=NORMAL')
            with self.assertRaises(ValueError):schema.migrate(db)
        with patch('hybrid_storage.worker_cache.require_local_fd',side_effect=PermissionError('remote mount')):
            with self.assertRaises(PermissionError):self.publisher.initialize()
        self.root.chmod(0o755)
        with self.assertRaises(PermissionError):self.publisher.initialize()
        self.root.chmod(0o700)

    def test_bootstrap_cannot_activate_after_flask_has_served_a_request(self):
        self.worker();self.http.get('/resources')
        self.agent.app.extensions.pop('checkpoint_publication')
        with self.assertRaises(AssertionError):install_checkpoint_service(self.agent,self.service)
        self.assertNotIn('checkpoint_publication',self.agent.app.extensions)

    def test_worker_symlink_oversize_and_busy_spool_fail_closed(self):
        self.worker();path=f'/jobs/{self.job_id}/custom-checkpoint';doc=request_document(self.key,self.job_id,self.leases[0])
        headers={REQUEST:header(doc),MAC:signature(self.key,path,doc)}
        self.service.slot.acquire()
        try:self.assertEqual(self.http.get(path,headers=headers).status_code,403)
        finally:self.service.slot.release()
        self.service.max_bytes=1;self.assertEqual(self.http.get(path,headers=headers).status_code,403)
        self.service.max_bytes=1024**2
        self.source.unlink();self.source.symlink_to(self.f.base/'nonexistent')
        self.assertEqual(self.http.get(path,headers=headers).status_code,403)
        self.assertFalse(self.service.slot.locked())


if __name__=='__main__':unittest.main(verbosity=2)
