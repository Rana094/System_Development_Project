"""Explicit trusted snapshots/restores on the existing checkpoint route, OFF by default.

No unsigned restore upload bypass. Reviewed OS/network isolation must keep this service's
credential and private spool inaccessible to student code. No NFS RW access.
"""
import os
import tempfile
import threading

from hybrid_storage.backends import _directory,_root
from hybrid_storage.checkpoint_files import private_root,regular,snapshot_source,validate_fd
from hybrid_storage.checkpoint_transfer import REQUEST,RECEIPT,MAC,read_request,header,signature
from hybrid_storage.inventory_protocol import Credential


class _Snapshot:
    """WSGI file-wrapper close releases the single in-flight disk/I/O slot."""
    def __init__(self,stream,slot):self.stream,self.slot,self.closed=stream,slot,False
    def __getattr__(self,name):return getattr(self.stream,name)
    def close(self):
        if self.closed:return
        self.closed=True
        try:self.stream.close()
        finally:self.slot.release()


class CheckpointService:
    def __init__(self,credential,*,worker_root,spool_root,max_bytes=10*1024**3+16*1024**2,
                 reserve_free_bytes=256*1024**2):
        if not isinstance(credential,Credential):raise TypeError('Explicit worker credential required')
        self.credential,self.root,self.spool=credential,_root(worker_root),_root(spool_root)
        if self.spool.is_relative_to(self.root) or self.root.is_relative_to(self.spool):
            raise ValueError('Checkpoint spool must be outside trainer job directories')
        if (type(max_bytes) is not int or not 0<max_bytes<=2**63-1
                or type(reserve_free_bytes) is not int or not 0<=reserve_free_bytes<=2**63-1):
            raise ValueError('Bounded checkpoint spool admission required')
        self.max_bytes,self.reserve=max_bytes,reserve_free_bytes
        self.slot=threading.Lock()

    def validate_stager(self,service):
        if service.credential!=self.credential:raise PermissionError('Checkpoint credential differs from stager')
        for other in (service.fetcher.cache.root,service.fetcher.cache.input_mount,service.workspaces.root):
            if self.spool.is_relative_to(other) or other.is_relative_to(self.spool):
                raise PermissionError('Checkpoint spool overlaps cache/input/workspace root')

    def snapshot(self,module,document):
        if not self.slot.acquire(blocking=False):raise ValueError('Checkpoint snapshot slot busy')
        handed_off=False
        try:
            import json
            job_id=document['job_id'];token=document['attempt_token']
            def authority():
                module.ownership.assert_owned(self.root,job_id,token,document['generation'],document['lease_id'])
            authority()
            directory=self.root/'worker-storage/jobs'/job_id
            with _directory(directory) as job:
                with regular(job,'prepared.json') as stream:record=json.load(stream)
                with regular(job,'request.json') as stream:spec=json.load(stream)['manifest']['execution_spec']
                if record['job_id']!=job_id or spec['recovery']['mode']!='checkpoint':
                    raise PermissionError('Checkpoint needs an exact prepared checkpoint-mode job')
                kind='ddp' if spec['execution']['mode']=='distributed' else 'single'
                service=module.app.extensions.get('hybrid_staging')
                if service is not None:
                    self.validate_stager(service)
                    records=[r for r in service.journal.records() if r['lease']['job_id']==job_id
                             and r['lease']['attempt_token']==token]
                    if len(records)!=1 or records[0]['phase'] not in ('RUNNING','ABORTING','TERMINAL'):
                        raise PermissionError('No matching staged checkpoint producer')
                    if records[0]['outcome'] not in (None,'COMPLETED'):
                        raise PermissionError('Checkpoint producer failed/cancelled')
                    plan=records[0]['plan']
                    if plan['identity']['rank']!=0:raise PermissionError('Only rank zero publishes a group checkpoint')
                with _directory(directory/'checkpoints') as checkpoints, regular(checkpoints,'latest.dlckpt') as source:
                    with private_root(self.spool):
                        frozen=tempfile.TemporaryFile(mode='w+b',dir=self.spool)
                    try:
                        digest,size=snapshot_source(source,frozen,maximum=self.max_bytes,reserve=self.reserve,check=authority)
                        info=validate_fd(frozen.fileno(),job_id,record['manifest_sha256'],token,kind)
                        if info['compatibility_key']!=record['compatibility_key']:
                            raise ValueError('Checkpoint environment differs from prepared job')
                        if kind=='ddp' and info['world_size']!=spec['execution']['workers']:
                            raise ValueError('Checkpoint rank set differs from submission')
                        authority();frozen.seek(0)
                        handed_off=True
                        return _Snapshot(frozen,self.slot),dict(version=1,sha256=digest,size_bytes=size)
                    except BaseException:frozen.close();raise
        finally:
            if not handed_off:self.slot.release()


def install_checkpoint_service(module,service):
    """Register only at reviewed idle bootstrap; never launches training."""
    if (not isinstance(service,CheckpointService) or service.root!=module.BASE_DIR
            or service.credential.worker!=module.node_id() or module.job_process is not None
            or 'checkpoint_publication' in module.app.extensions):
        raise ValueError('Checkpoint service requires exact idle worker identity')
    with private_root(service.spool):pass
    existing=module.app.extensions.get('hybrid_staging')
    if existing is not None:service.validate_stager(existing)
    from flask import Response,request,send_file

    @module.app.before_request
    def checkpoint_download():
        if request.endpoint!='custom_checkpoint':return None
        if request.method=='POST':
            from hybrid_storage import checkpoint_restore as protocol
            from checkpoint_restore_service import install
            try:
                document=protocol.read_request(service.credential,request.path,request.headers)
                value=install(service,module,document,request.stream,request.content_length)
                response=Response('',status=200)
                response.headers[RECEIPT]=header(value)
                response.headers[MAC]=protocol.signature(service.credential,request.path,value,request=document)
                return response
            except (OSError,ValueError,KeyError,TypeError):
                return Response('Checkpoint restore rejected',status=403)
        if request.method!='GET':return Response('Checkpoint method unavailable',status=403)
        try:
            document=read_request(service.credential,request.path,request.headers)
            stream,receipt=service.snapshot(module,document)
        except (OSError,ValueError,KeyError,TypeError):
            return Response('Checkpoint publication rejected',status=403)
        try:
            response=send_file(stream,mimetype='application/octet-stream',conditional=False)
            response.headers[RECEIPT]=header(receipt)
            response.headers[MAC]=signature(service.credential,request.path,receipt,request=document)
            response.headers['Content-Length']=str(receipt['size_bytes'])
            response.call_on_close(stream.close)
            return response
        except BaseException:stream.close();raise

    # A failed late Flask hook registration never advertises an active service.
    hooks=module.app.before_request_funcs[None]
    hooks.remove(checkpoint_download);hooks.insert(0,checkpoint_download)
    module.app.extensions['checkpoint_publication']=service
