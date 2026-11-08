"""Authenticated staging metadata on the existing worker HTTP control channel.

Reuse operator-provisioned inventory credentials; domain separate every action
and bind replies to the complete request. This transports no artifact bytes.
"""

import hashlib
import hmac
import re
import secrets
import time

from .inventory_protocol import Credential, AuthenticationError, MAX_BYTES, decode, encode, fields, hex_id
from .input_manifests import encoded, parse_source
from .dataset_manifests import parse_manifest
from .workspaces import AttemptIdentity, WorkspacePlan

STAGE_PATH = '/jobs/stage'
START_PATH = '/jobs/start'
CONTROL_PATH = '/jobs/staging-control'
HEADER = 'X-DL-Staging-MAC'


def signature(key, path, raw, *, reply_to=None):
    if not isinstance(key, Credential) or path not in (STAGE_PATH, START_PATH, CONTROL_PATH):
        raise AuthenticationError('Unsupported authenticated control action')
    context = encoded(dict(protocol='dl-staging-v1', method='POST', path=path,
                           worker=key.worker, key_id=key.key_id,
                           direction='request' if reply_to is None else 'reply'))
    return hmac.new(key.secret, context + b'\0' + hashlib.sha256(reply_to or b'').digest() + raw,
                    hashlib.sha256).hexdigest()


def authenticate(key, path, raw, value, *, reply_to=None):
    if (type(raw) is not bytes or type(value) is not str or not re.fullmatch('[0-9a-f]{64}', value)
            or not hmac.compare_digest(signature(key, path, raw, reply_to=reply_to), value)):
        raise AuthenticationError('Invalid staging authentication')


def request_document(key, body):
    return dict(version=1, worker_id=key.worker, nonce=secrets.token_hex(16),
                sent_at_ns=time.time_ns(), body=body)


def read_request(key, path, raw, value):
    authenticate(key, path, raw, value)
    document = fields(decode(raw), 'version worker_id nonce sent_at_ns body')
    hex_id(document['nonce'])
    if (type(document['version']) is not int or document['version'] != 1 or document['worker_id'] != key.worker
            or type(document['sent_at_ns']) is not int
            or abs(time.time_ns() - document['sent_at_ns']) > 2_000_000_000
            or type(document['body']) is not dict):
        raise AuthenticationError('Stale or misdirected staging request')
    return document['body']


def parse_plan(document):
    fields(document, 'schema_version identity input_manifest_sha256 source dataset')
    if type(document['schema_version']) is not int or document['schema_version'] != 1:
        raise ValueError('Unsupported workspace plan version')
    fields(document['identity'], 'job_id attempt_token rank stage_revision')
    return WorkspacePlan(AttemptIdentity(**document['identity']), document['input_manifest_sha256'],
                         parse_source(encoded(document['source'])),
                         None if document['dataset'] is None else parse_manifest(encoded(document['dataset'])))


def receipt_document(stage_id, plan, lease):
    """Path-free identity of a fully verified private workspace, not launch authority."""
    return dict(stage_id=hex_id(stage_id), plan_sha256=plan.sha256,
                identity_key=plan.identity.key, lease_id=lease['lease_id'], generation=lease['generation'])


def receipt_hash(receipt):
    return hashlib.sha256(encode(receipt)).hexdigest()


class StagingClient:
    """No implicit key loading, retries, enrollment, artifact downloads or fallback."""
    def __init__(self, credentials, *, post, timeout=20):
        if not credentials or any(node != key.worker for node, key in credentials.items()):
            raise ValueError('Expected explicit per-worker credentials')
        if type(timeout) not in (int, float) or not 0 < timeout <= 60:
            raise ValueError('Bounded control timeout required')
        self.credentials, self.post, self.timeout = dict(credentials), post, timeout

    def call(self, node, path, body):
        key = self.credentials[node['id']]
        raw = encode(request_document(key, body))
        response = self.post(f"http://{node['ip']}:{node['port']}{path}", data=raw,
                             headers={HEADER: signature(key, path, raw), 'Content-Type':'application/json'},
                             timeout=self.timeout, stream=True)
        try:
            response.raise_for_status()
            chunks = []; size = 0
            for chunk in response.iter_content(64*1024):
                size += len(chunk)
                if size > MAX_BYTES:
                    raise AuthenticationError('Staging response exceeds metadata bound')
                chunks.append(chunk)
            reply = b''.join(chunks)
            authenticate(key, path, reply, response.headers.get(HEADER), reply_to=raw)
            return decode(reply)
        finally:
            response.close()
