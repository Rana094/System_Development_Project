"""Tensor-library-independent validation for the versioned checkpoint transport."""
import hashlib
import json
import math
import re
from pathlib import Path
import zipfile
from execution_lease import validate_fence

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
        if 'lease_fence' in meta:validate_fence(meta['lease_fence'])
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


def group_identity(job_id, attempt, manifest, compatibility, context):
    """Bind a checkpoint to the persisted rendezvous group, not just its size."""
    value = [job_id,attempt,manifest,compatibility,context['world_size'],
             context['master_addr'],context['master_port']]
    return hashlib.sha256(json.dumps(value,separators=(',',':')).encode()).hexdigest()


def validate_group(path, job_id, manifest_sha256, expected_attempt=None):
    """Accept only a complete, acknowledged archive containing every rank state."""
    with zipfile.ZipFile(path) as archive:
        entries = archive.infolist()
        if archive.getinfo('metadata.json').file_size > 65536:
            raise ValueError('Group checkpoint metadata is oversized')
        meta = json.loads(archive.read('metadata.json'))
        count = meta.get('world_size')
        if type(count) is not int or not 2 <= count <= 256:
            raise ValueError('Invalid group checkpoint worker count')
        names = {'metadata.json',*[f'rank-{rank}.pt' for rank in range(count)]}
        if len(entries) != len(names) or {entry.filename for entry in entries} != names:
            raise ValueError('Group checkpoint must contain every rank exactly once')
        if any(entry.compress_type != zipfile.ZIP_STORED for entry in entries):
            raise ValueError('Compressed group checkpoints are unsupported')
        sizes = [archive.getinfo(f'rank-{rank}.pt').file_size for rank in range(count)]
        if any(size <= 0 for size in sizes) or sum(sizes) > MAX_STATE_BYTES:
            raise ValueError('Group checkpoint state size is invalid')
        if (meta.get('format_version') != 2 or meta.get('kind') != 'ddp'
                or meta.get('job_id') != job_id or meta.get('manifest_sha256') != manifest_sha256):
            raise ValueError('Group checkpoint belongs to a different job or input snapshot')
        token = meta.get('attempt_token')
        if not isinstance(token,str) or not re.fullmatch(re.escape(job_id)+r':[0-9]+',token):
            raise ValueError('Invalid group attempt token')
        if expected_attempt is not None and token != expected_attempt:
            raise ValueError('Group checkpoint belongs to a different attempt')
        if (type(meta.get('global_step')) is not int or meta['global_step'] < 0
                or type(meta.get('completed')) is not bool):
            raise ValueError('Invalid group checkpoint progress')
        if (type(meta.get('created_at')) not in (int,float)
                or not math.isfinite(meta['created_at']) or meta['created_at'] <= 0):
            raise ValueError('Invalid group checkpoint timestamp')
        if not isinstance(meta.get('compatibility_key'),str) or not meta['compatibility_key']:
            raise ValueError('Missing group environment identity')
        for key,size in [('checkpoint_id',32),('group_id',64),('shared_state_sha256',64)]:
            if not re.fullmatch(r'[0-9a-f]{'+str(size)+'}',str(meta.get(key,''))):
                raise ValueError('Invalid group checkpoint '+key)
        if 'rank_fences' in meta:
            if not isinstance(meta['rank_fences'],dict) or set(meta['rank_fences'])!={str(r) for r in range(count)}:
                raise ValueError('Missing rank execution fences')
            for value in meta['rank_fences'].values():validate_fence(value)
            if len({v['node_id'] for v in meta['rank_fences'].values()})!=count:
                raise ValueError('Duplicate checkpoint worker ownership')
        hashes = meta.get('rank_state_sha256')
        if not isinstance(hashes,dict) or set(hashes) != {str(rank) for rank in range(count)}:
            raise ValueError('Missing rank state hashes')
        expected = []
        for rank in range(count):
            digest = hashlib.sha256()
            with archive.open(f'rank-{rank}.pt') as stream:
                for chunk in iter(lambda:stream.read(1024*1024),b''):
                    digest.update(chunk)
            if digest.hexdigest() != hashes[str(rank)]:
                raise ValueError(f'Rank {rank} state hash mismatch')
            expected.append({'rank':rank,'checkpoint_id':meta['checkpoint_id'],
                             'state_sha256':hashes[str(rank)],'accepted':True})
        acknowledgments = meta.get('rank_acknowledgments')
        if (not isinstance(acknowledgments,list) or acknowledgments != expected
                or any(type(ack.get('rank')) is not int or type(ack.get('accepted')) is not bool
                       for ack in acknowledgments)):
            raise ValueError('Group checkpoint is not acknowledged by every rank')
    return {**meta,'sha256':sha256(path),'size_bytes':Path(path).stat().st_size}
