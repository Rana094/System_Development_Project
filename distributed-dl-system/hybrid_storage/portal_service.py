"""Per-operation LOCAL registry contexts for existing portal preparation/control.

No shared SQLite connection across Flask/background threads; no inventory owner
or scheduler daemon is created by the portal. Existing owner/CSRF gates remain.
"""
from contextlib import contextmanager
from pathlib import Path
import sqlite3
from urllib.parse import urlsplit

from .config import require_staging_config
from .runtime_bootstrap import _private_address
from .artifacts import ArtifactRegistrar
from .registry import StorageRegistry
from .checkpoint_publication import LocalCheckpointPublisher
from .checkpoint_transfer import CheckpointCollector
from .checkpoint_restore import CheckpointRestorer
from .staging_protocol import StagingClient,PREPARE_PATH


class PrivateTransport:
    def __init__(self,nodes,session=None):
        import requests
        self.session=session or requests.Session();self.session.trust_env=False
        self.endpoints=frozenset((str(_private_address(n['ip'])),n['port']) for n in nodes)
        if not self.endpoints or any(type(p) is not int or not 1<=p<=65535 for _,p in self.endpoints):raise ValueError('Private worker endpoints required')
    def request(self,method,url,**kwargs):
        p=urlsplit(url)
        if p.scheme!='http' or p.username is not None or p.fragment or (p.hostname,p.port) not in self.endpoints:
            raise PermissionError('Unreviewed worker endpoint')
        kwargs['allow_redirects']=False
        return getattr(self.session,method)(url,**kwargs)
    def get(self,url,**kwargs):return self.request('get',url,**kwargs)
    def post(self,url,**kwargs):return self.request('post',url,**kwargs)
    def close(self):self.session.close()


class HybridPortal:
    def __init__(self,*,db_path,objects,scratch,checkpoints,nodes,credentials,settings):
        self.settings=require_staging_config(settings)
        self.db_path,self.objects,self.scratch,self.checkpoints=map(Path,(db_path,objects,scratch,checkpoints))
        self.nodes=tuple(dict(n) for n in nodes);self.credentials=dict(credentials)
        if not self.nodes or {n['id'] for n in self.nodes}!=set(self.credentials):raise ValueError('Exact enrolled portal worker scope required')
        for n in self.nodes:_private_address(n['ip'])

    @contextmanager
    def context(self):
        from checkpoint_recovery import CheckpointDispatch
        db=sqlite3.connect(self.db_path.as_uri()+'?mode=rw',uri=True,timeout=10)
        transport=PrivateTransport(self.nodes)
        try:
            db.execute('PRAGMA foreign_keys=ON');db.execute('PRAGMA synchronous=FULL')
            registry=StorageRegistry(db,ArtifactRegistrar(self.objects,staging_root=self.scratch))
            for worker,key in self.credentials.items():
                row=db.execute('SELECT auth_principal,auth_state,enabled FROM cache_registry_workers WHERE worker_id=?',(worker,)).fetchone()
                if row is None or tuple(row)!=(key.principal,'ENROLLED',1):raise PermissionError('Portal worker enrollment differs')
            publisher=LocalCheckpointPublisher(registry,self.checkpoints)
            client=StagingClient(self.credentials,post=transport.post)
            dispatch=CheckpointDispatch(registry,client,settings=self.settings,publisher=publisher,
                collector=CheckpointCollector(publisher,self.credentials,get=transport.get),
                restorer=CheckpointRestorer(publisher,self.credentials,post=transport.post))
            yield dispatch
        finally:
            transport.close();db.close()

    def pending(self,db,job_id,profile):
        """Authorize only metadata preparation; never grants execution or unholds."""
        import storage_selection
        if profile not in ('pytorch-cpu','isolated'):raise ValueError('Use an approved offline environment profile')
        if storage_selection.binding(db,job_id)['backend']!='nfs':raise ValueError('Not a hybrid submission')
        row=db.execute('SELECT owner,status,queue_reason,cancel_requested_at FROM jobs WHERE job_id=?',(job_id,)).fetchone()
        if (not row or not row[0] or row[3] is not None or
            (row[1] not in ('STAGED','READY') and not (row[1]=='HELD' and row[2]==storage_selection.NFS_HELD_REASON))):
            raise PermissionError('Job is cancelled, held by its owner, or not ready for preparation')

    def prepare(self,job_id,profile):
        import job_store as store
        from job_storage import verify_workspace,storage_root
        from input_storage import register_legacy_inputs
        if Path(store.DB_FILE).resolve()!=self.db_path.resolve():raise PermissionError('Portal DB differs from trusted registry')
        with self.context() as runtime:
            self.pending(runtime.registry.db,job_id,profile)
            job=store.get_job(job_id)
            # Reuse the controller's managed snapshot verifier and registrar.
            register_legacy_inputs(runtime.registry,job['owner'],job_id,managed_root=storage_root(),verify_workspace=verify_workspace)
            manifest,fingerprint=verify_workspace(Path(job['storage_path']),job)
            request=dict(job_id=job_id,profile=profile,manifest=manifest,manifest_sha256=fingerprint)
            success=[]
            for node in self.nodes:
                current=store.get_job(job_id)
                if current['status']=='CANCELLED' or current['cancel_requested_at'] is not None:
                    raise PermissionError('Cancelled during metadata preparation')
                reply=runtime.client.call(node,PREPARE_PATH,request)
                if reply.get('prepared') is not True or type(reply.get('record')) is not dict:
                    raise PermissionError('Missing authenticated preparation receipt')
                store.record_environment(job_id,node['id'],reply['record'],storage_runtime=runtime)
                success.append(node['id'])
            store.enqueue_custom_job(job_id,set(success),storage_runtime=runtime)
            return success
