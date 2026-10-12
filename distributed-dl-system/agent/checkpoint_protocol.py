"""Tensor-library-independent validation for the versioned checkpoint transport."""
import hashlib
import json
import math
from pathlib import Path
import zipfile

MAX_STATE_BYTES = 10 * 1024**3


def sha256(path):
    digest = hashlib.sha256()
    with open(path,'rb') as stream:
        for chunk in iter(lambda:stream.read(1024*1024),b''):
            digest.update(chunk)
    return digest.hexdigest()


def validate(path, job_id, manifest_sha256, expected_attempt=None):
    """Verify identity, structure and every state byte before publishing a replacement."""
    with zipfile.ZipFile(path) as archive:
        entries = archive.infolist()
        if len(entries)!=2 or {e.filename for e in entries}!={'metadata.json','state.pt'}:
            raise ValueError('Checkpoint must contain exactly metadata.json and state.pt')
        if any(e.compress_type!=zipfile.ZIP_STORED for e in entries):
            raise ValueError('Compressed checkpoint entries are not supported')
        if archive.getinfo('metadata.json').file_size>65536 or not 0<archive.getinfo('state.pt').file_size<=MAX_STATE_BYTES:
            raise ValueError('Checkpoint entry size is invalid')
        meta = json.loads(archive.read('metadata.json'))
        if meta.get('format_version')!=1 or meta.get('job_id')!=job_id or meta.get('manifest_sha256')!=manifest_sha256:
            raise ValueError('Checkpoint belongs to a different job or input snapshot')
        if type(meta.get('global_step')) is not int or meta['global_step']<0 or type(meta.get('completed')) is not bool:
            raise ValueError('Invalid checkpoint progress')
        if type(meta.get('created_at')) not in (int,float) or not math.isfinite(meta['created_at']) or meta['created_at']<=0:
            raise ValueError('Invalid checkpoint timestamp')
        if not isinstance(meta.get('compatibility_key'),str) or not meta['compatibility_key']:
            raise ValueError('Checkpoint environment identity is missing')
        token=meta.get('attempt_token')
        if not isinstance(token,str) or not token.startswith(job_id+':') or not token[len(job_id)+1:].isdigit():
            raise ValueError('Invalid checkpoint attempt identity')
        if expected_attempt is not None and token!=expected_attempt:
            raise ValueError('Checkpoint belongs to a different attempt')
        digest=hashlib.sha256()
        with archive.open('state.pt') as stream:
            for chunk in iter(lambda:stream.read(1024*1024),b''):
                digest.update(chunk)
        if digest.hexdigest()!=meta.get('state_sha256'):
            raise ValueError('Checkpoint state hash mismatch')
    return {**meta,'sha256':sha256(path),'size_bytes':Path(path).stat().st_size}
