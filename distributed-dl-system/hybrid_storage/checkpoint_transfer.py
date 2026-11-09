"""Authenticated bounded checkpoint bytes over the EXISTING HTTP GET route.

Small signed request/receipt headers bind worker, job, attempt, generation and
complete archive SHA/size. No artifact download API, new transport, or secrets in
student input. Authenticity is not encryption; isolated trusted networks required.
"""
import base64
import hashlib
import hmac
import re
import secrets
import time

from .input_manifests import encoded,job_identity
from .inputs import object_key
from .inventory_protocol import Credential,AuthenticationError,fields,decode
from .checkpoint_files import validate_fence

REQUEST='X-DL-Checkpoint-Request'
RECEIPT='X-DL-Checkpoint-Receipt'
MAC='X-DL-Checkpoint-MAC'
MAX_HEADER=8192


def path_for(job_id):
    job_identity(job_id)
    return f'/jobs/{job_id}/custom-checkpoint'


def header(document):
    value=base64.b64encode(encoded(document)).decode('ascii')
    if len(value)>MAX_HEADER:raise AuthenticationError('Checkpoint metadata too large')
    return value


def read_header(value):
    if type(value) is not str or not 0<len(value)<=MAX_HEADER:raise AuthenticationError('Bounded checkpoint header required')
    try:return decode(base64.b64decode(value,validate=True))
    except (ValueError,TypeError) as error:raise AuthenticationError('Invalid checkpoint header') from error


def signature(key,path,document,*,request=None):
    if not isinstance(key,Credential) or not re.fullmatch(r'/jobs/[A-Za-z0-9._-]+/custom-checkpoint',path):
        raise AuthenticationError('Unsupported checkpoint request')
    context=encoded(dict(protocol='dl-checkpoint-get-v1',method='GET',path=path,worker=key.worker,
                         key_id=key.key_id,direction='request' if request is None else 'reply'))
    return hmac.new(key.secret,context+b'\0'+hashlib.sha256(encoded(request) if request is not None else b'').digest()
                    +encoded(document),hashlib.sha256).hexdigest()


def authenticate(key,path,document,value,*,request=None):
    if (type(value) is not str or not re.fullmatch('[0-9a-f]{64}',value)
            or not hmac.compare_digest(signature(key,path,document,request=request),value)):
        raise AuthenticationError('Checkpoint authentication failed')


def request_document(key,job_id,lease):
    validate_fence({name:lease[name] for name in ('node_id','generation','lease_id')})
    if lease['node_id']!=key.worker or lease['job_id']!=job_id:
        raise AuthenticationError('Wrong checkpoint worker/job')
    return dict(version=1,worker_id=key.worker,job_id=job_id,attempt_token=lease['attempt_token'],
                generation=lease['generation'],lease_id=lease['lease_id'],nonce=secrets.token_hex(16),sent_at_ns=time.time_ns())


def read_request(key,path,headers):
    document=read_header(headers.get(REQUEST));authenticate(key,path,document,headers.get(MAC))
    fields(document,'version worker_id job_id attempt_token generation lease_id nonce sent_at_ns')
    validate_fence(dict(node_id=document['worker_id'],generation=document['generation'],lease_id=document['lease_id']))
    if (type(document['version']) is not int or document['version']!=1 or document['worker_id']!=key.worker
            or path!=path_for(document['job_id']) or type(document['sent_at_ns']) is not int
            or abs(time.time_ns()-document['sent_at_ns'])>2_000_000_000
            or not re.fullmatch('[0-9a-f]{32}',str(document['nonce']))
            or not re.fullmatch(re.escape(document['job_id'])+r':[0-9]+',str(document['attempt_token']))):
        raise AuthenticationError('Stale or misdirected checkpoint request')
    return document


class _ResponseStream:
    def __init__(self,response):
        self.chunks=iter(response.iter_content(1024*1024));self.pending=b''

    def read(self,size):
        # Empty HTTP keep-alive chunks are not EOF. Bound empty iterations too;
        # a faulty peer must not hide trailing bytes or spin indefinitely.
        empty=0
        while not self.pending:
            try:self.pending=next(self.chunks)
            except StopIteration:return b''
            if type(self.pending) is not bytes:raise AuthenticationError('Expected binary checkpoint chunk')
            if not self.pending:
                empty+=1
                if empty>32:raise AuthenticationError('Excess empty checkpoint chunks')
        if len(self.pending)>1024*1024:raise AuthenticationError('Peer exceeded stream chunk bound')
        value,self.pending=self.pending[:size],self.pending[size:]
        return value


class CheckpointCollector:
    """Explicit controller adapter. No implicit credentials/HTTP retries/fallback."""
    def __init__(self,publisher,credentials,*,get,timeout=(2,10)):
        from .checkpoint_publication import LocalCheckpointPublisher
        if not isinstance(publisher,LocalCheckpointPublisher) or not credentials or not callable(get):
            raise TypeError('Explicit publisher, keys and existing HTTP client required')
        if any(key.worker!=worker for worker,key in credentials.items()):raise ValueError('Worker credentials differ')
        if len(timeout)!=2 or any(type(v) not in (int,float) or not 0<v<=60 for v in timeout):
            raise ValueError('Bounded connect/read timeouts required')
        self.publisher,self.credentials,self.get,self.timeout=publisher,dict(credentials),get,timeout

    def sync(self,actor,job_id,node,lease,*,publication_id,expected_head,skip_unchanged=False):
        key=self.credentials[node['id']];path=path_for(job_id);request=request_document(key,job_id,lease)
        response=self.get(f"http://{node['ip']}:{node['port']}{path}",
                          headers={REQUEST:header(request),MAC:signature(key,path,request)},
                          stream=True,timeout=self.timeout,allow_redirects=False)
        try:
            response.raise_for_status()
            if response.status_code!=200:raise AuthenticationError('Checkpoint redirect/partial response rejected')
            receipt=read_header(response.headers.get(RECEIPT))
            authenticate(key,path,receipt,response.headers.get(MAC),request=request)
            fields(receipt,'version sha256 size_bytes')
            object_key(receipt['sha256'])
            if type(receipt['version']) is not int or receipt['version']!=1:
                raise AuthenticationError('Unsupported checkpoint receipt')
            if skip_unchanged and expected_head is not None:
                previous=self.publisher.committed(actor,expected_head)
                if previous.sha256==receipt['sha256'] and previous.size_bytes==receipt['size_bytes']:
                    return previous
            return self.publisher.publish(actor,job_id,node['id'],_ResponseStream(response),
                publication_id=publication_id,expected_head=expected_head,
                expected_sha256=receipt['sha256'],expected_size=receipt['size_bytes'])
        finally:response.close()
