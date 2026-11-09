"""Private, attempt-bound restore materialization; no tensor deserialization here."""
import json
import os
import tempfile

from hybrid_storage.artifacts import _rename_noreplace,_verify
from hybrid_storage.backends import _directory
from hybrid_storage.checkpoint_files import private_root,regular,space,stream_copy,validate_fd
from hybrid_storage.checkpoint_restore import receipt
from hybrid_storage.input_manifests import encoded
from hybrid_storage.inputs import object_key
from hybrid_storage.inventory_protocol import fields,hex_id
from hybrid_storage.staging_protocol import parse_plan
from hybrid_storage.worker_cache import _child


def source_document(source):
    fields(source,'publication_id sha256 size_bytes binding_sha256 metadata')
    hex_id(source['publication_id']);object_key(source['sha256']);object_key(source['binding_sha256'])
    if type(source['size_bytes']) is not int or source['size_bytes']<=0 or type(source['metadata']) is not dict:
        raise ValueError('Exact bounded restore source required')
    return source


def context(service,module,document):
    stager=module.app.extensions.get('hybrid_staging')
    if stager is None:raise PermissionError('Restore requires authenticated staging')
    service.validate_stager(stager)
    record=stager._record(document['stage_id'])
    if record is None or record['phase']!='READY' or record['launched']:
        raise PermissionError('Restore requires unlaunched READY workspace')
    plan=parse_plan(record['plan']);lease=record['lease']
    if any(document[key]!=lease[key] for key in ('job_id','attempt_token','generation','lease_id')):
        raise PermissionError('Restore destination fence differs')
    source=source_document(document['source'])
    if source['binding_sha256']!=plan.input_manifest_sha256:
        raise PermissionError('Restore immutable input version differs')
    stager.authority(plan,lease,document['stage_id'])
    return stager,plan,lease


def install(service,module,document,incoming,length):
    stager,plan,lease=context(service,module,document)
    source=document['source'];size=source['size_bytes']
    if size>service.max_bytes or length!=size:raise ValueError('Restore exceeds declared bound')
    if not service.slot.acquire(blocking=False):raise ValueError('Checkpoint transfer slot busy')
    if not stager._lock.acquire(blocking=False):
        service.slot.release();raise ValueError('Materializer busy')
    try:
        check=lambda:stager.authority(plan,lease,document['stage_id'])
        # Protected record is outside the trainer workspace. The trainer receives
        # an independent read-only file, never a hardlink to service state.
        with private_root(service.spool) as root, _child(root,'restores',create=True) as restores:
            with _child(restores,document['stage_id'],create=True) as saved:
                try:
                    with regular(saved,'receipt.json',private=True) as stream:previous=json.load(stream)
                except FileNotFoundError:previous=None
                expected=receipt(document)
                if previous is not None and previous!=expected:raise PermissionError('Restore source already frozen')
                with tempfile.TemporaryFile(mode='w+b',dir=service.spool) as candidate:
                    space(candidate.fileno(),size*2,service.reserve)
                    stream_copy(incoming,candidate,size=size,sha256=source['sha256'],check=check)
                    directory=service.root/'worker-storage/jobs'/plan.identity.job_id
                    with _directory(directory) as job:
                        with regular(job,'prepared.json') as stream:prepared=json.load(stream)
                        with regular(job,'request.json') as stream:spec=json.load(stream)['manifest']['execution_spec']
                    kind='ddp' if spec['execution']['workers']>1 else 'single'
                    info=validate_fd(candidate.fileno(),plan.identity.job_id,prepared['manifest_sha256'],None,kind)
                    if (info!=source['metadata'] or spec['recovery']['mode']!='checkpoint'
                            or info['compatibility_key']!=prepared['compatibility_key']
                            or int(info['attempt_token'].rsplit(':',1)[1])>=int(plan.identity.attempt_token.rsplit(':',1)[1])
                            or (kind=='ddp' and info['world_size']!=spec['execution']['workers'])):
                        raise PermissionError('Restore format/environment/source attempt differs')
                    with stager.workspaces._locked(plan) as (_,__,attempts):
                        workspace=stager.workspaces._receipt(attempts,plan)
                        with _directory(workspace.checkpoints) as dest:
                            try:
                                with regular(dest,'resume.dlckpt') as stream:_verify(stream,source['sha256'],size)
                            except FileNotFoundError:
                                name='.'+os.urandom(16).hex()+'.part'
                                fd=os.open(name,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600,dir_fd=dest)
                                try:
                                    with os.fdopen(fd,'wb') as output:
                                        candidate.seek(0);stream_copy(candidate,output,size=size,sha256=source['sha256'],check=check)
                                        os.fchmod(output.fileno(),0o400);os.fsync(output.fileno())
                                    check();_rename_noreplace(dest,name,dest,'resume.dlckpt');os.fsync(dest)
                                finally:
                                    try:os.unlink(name,dir_fd=dest)
                                    except FileNotFoundError:pass
                    check()
                    if previous is None:
                        fd=os.open('receipt.json',os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o400,dir_fd=saved)
                        with os.fdopen(fd,'wb') as stream:stream.write(encoded(expected));stream.flush();os.fsync(stream.fileno())
                        os.fsync(saved)
        return expected
    finally:
        stager._lock.release();service.slot.release()


def launch_path(service,module,data):
    """Reverify protected receipt and independent local bytes before Popen."""
    stager=module.app.extensions['hybrid_staging']
    record=stager._record(data['staging_id']);plan=parse_plan(record['plan'])
    directory=service.root/'worker-storage/jobs'/plan.identity.job_id
    with _directory(directory) as job,regular(job,'request.json') as stream:
        mode=json.load(stream)['manifest']['execution_spec']['recovery']['mode']
    if mode!='checkpoint':return None
    if 'checkpoint_source' not in data:raise PermissionError('Explicit frozen source required')
    source=data['checkpoint_source']
    if source is None:
        if data.get('resume_checkpoint') is not None:raise PermissionError('Conflicting cold start')
        return None
    source_document(source)
    doc=dict(stage_id=data['staging_id'],job_id=plan.identity.job_id,attempt_token=plan.identity.attempt_token,
             generation=record['lease']['generation'],lease_id=record['lease']['lease_id'],source=source)
    with private_root(service.spool) as root,_child(root,'restores') as restores,_child(restores,data['staging_id']) as saved:
        with regular(saved,'receipt.json',private=True) as stream:value=json.load(stream)
    if value!=receipt(doc) or data.get('resume_checkpoint')!=source['metadata']:
        raise PermissionError('Start differs from trusted restore receipt')
    path=stager.workspaces.root/'attempts'/plan.identity.key/'checkpoints/resume.dlckpt'
    with _directory(path.parent) as parent,regular(parent,path.name) as stream:_verify(stream,source['sha256'],source['size_bytes'])
    stager.authority(plan,record['lease'],data['staging_id'])
    return path
