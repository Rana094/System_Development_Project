"""Authenticated raw checkpoint restore on the existing HTTP route, no NFS RW."""
import hashlib
import hmac
import re
import time

from .checkpoint_transfer import REQUEST,RECEIPT,MAC,header,read_header,path_for,request_document
from .inventory_protocol import AuthenticationError,fields,hex_id
from .input_manifests import encoded


def signature(key,path,document,*,request=None):
    context=encoded(dict(protocol='dl-checkpoint-restore-v1',method='POST',path=path,
                         worker=key.worker,key_id=key.key_id,direction='request' if request is None else 'reply'))
    return hmac.new(key.secret,context+b'\0'+hashlib.sha256(encoded(request) if request is not None else b'').digest()
                    +encoded(document),hashlib.sha256).hexdigest()


def authenticate(key,path,document,value,*,request=None):
    if type(value) is not str or not hmac.compare_digest(signature(key,path,document,request=request),value):
        raise AuthenticationError('Checkpoint restore authentication failed')


def read_request(key,path,headers):
    doc=read_header(headers.get(REQUEST));authenticate(key,path,doc,headers.get(MAC))
    fields(doc,'version worker_id job_id attempt_token generation lease_id nonce sent_at_ns stage_id source')
    from .checkpoint_files import validate_fence
    validate_fence(dict(node_id=doc['worker_id'],generation=doc['generation'],lease_id=doc['lease_id']))
    hex_id(doc['nonce']);hex_id(doc['stage_id'])
    if (type(doc['version']) is not int or doc['version']!=1 or doc['worker_id']!=key.worker
            or path!=path_for(doc['job_id']) or type(doc['sent_at_ns']) is not int
            or abs(time.time_ns()-doc['sent_at_ns'])>2_000_000_000
            or not re.fullmatch(re.escape(doc['job_id'])+r':[0-9]+',str(doc['attempt_token']))):
        raise AuthenticationError('Stale or misdirected restore request')
    return doc


def receipt(document):
    return {key:document[key] for key in ('job_id','attempt_token','generation','lease_id','stage_id','source')}


class CheckpointRestorer:
    def __init__(self,publisher,credentials,*,post,timeout=(2,10)):
        from .checkpoint_publication import LocalCheckpointPublisher
        if not isinstance(publisher,LocalCheckpointPublisher) or not callable(post):raise TypeError('Trusted publisher and HTTP client required')
        if not credentials or any(node!=key.worker for node,key in credentials.items()):raise ValueError('Exact per-worker credentials required')
        if len(timeout)!=2 or any(type(v) not in (int,float) or not 0<v<=60 for v in timeout):raise ValueError('Bounded timeouts required')
        self.publisher,self.credentials,self.post,self.timeout=publisher,dict(credentials),post,timeout

    def stage(self,actor,job_id,node,lease,stage_id,source):
        key=self.credentials[node['id']];path=path_for(job_id)
        document=dict(request_document(key,job_id,lease),stage_id=hex_id(stage_id),source=source)
        with self.publisher.open_committed(actor,source['publication_id']) as stream:
            response=self.post(f"http://{node['ip']}:{node['port']}{path}",data=stream,headers={
                REQUEST:header(document),MAC:signature(key,path,document),'Content-Type':'application/octet-stream',
                'Content-Length':str(source['size_bytes'])},timeout=self.timeout,stream=True,allow_redirects=False)
        try:
            response.raise_for_status()
            if response.status_code!=200:raise AuthenticationError('Restore redirect/partial response rejected')
            value=read_header(response.headers.get(RECEIPT))
            authenticate(key,path,value,response.headers.get(MAC),request=document)
            if value!=receipt(document):raise AuthenticationError('Restore acknowledgement differs from exact staged source')
            return hashlib.sha256(encoded(value)).hexdigest()
        finally:response.close()
