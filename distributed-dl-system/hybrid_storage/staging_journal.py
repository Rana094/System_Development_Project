"""Protected local worker intents: persist before copy/launch; never adopt a path.

One service owns the journal lock for its lifetime. Malformed/unknown records
block bootstrap rather than resetting history. No training output is deleted.
"""
from contextlib import contextmanager
import fcntl
import json
import math
import os
import secrets
import threading

from .backends import _directory
from .worker_cache import _child, _private_file, _protected
from .input_manifests import encoded
from .inventory_protocol import hex_id
from .staging_protocol import parse_plan

PHASES = {'STAGING','READY','LAUNCHING','RUNNING','ABORTING','TERMINAL'}
FIELDS = {'version','stage_id','plan','plan_sha256','lease','boot_id','phase','outcome','launched','pins_released'}
MAX_BYTES = 4*1024*1024+16384


class StagingJournal:
    def __init__(self, root):
        self.root = root
        self._lock = None
        self._mutex = threading.RLock()

    def initialize(self):
        if self._lock is not None:raise ValueError('Journal already owned')
        with _directory(self.root) as root:
            _protected(root,directory=True)
            with _child(root,'lifecycle',create=True) as state:
                fd = _private_file(state,'service.lock',create=True)
                try:fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
                except BaseException:
                    os.close(fd);raise
                self._lock = fd

    @contextmanager
    def session(self):
        if self._lock is None:raise ValueError('Journal not owned')
        with self._mutex, _directory(self.root) as root:
            _protected(root,directory=True)
            with _child(root,'lifecycle') as state:
                info=os.stat('service.lock',dir_fd=state,follow_symlinks=False);held=os.fstat(self._lock)
                if (info.st_dev,info.st_ino)!=(held.st_dev,held.st_ino):
                    raise ValueError('Journal lock identity changed')
                yield state

    @staticmethod
    def validate(record):
        if type(record) is not dict or set(record)!=FIELDS or type(record['version']) is not int or record['version']!=1:
            raise ValueError('Invalid lifecycle journal')
        hex_id(record['stage_id']);plan=parse_plan(record['plan'])
        if (plan.sha256!=record['plan_sha256'] or record['phase'] not in PHASES
                or type(record['launched']) is not bool or type(record['pins_released']) is not bool
                or (record['pins_released'] and record['phase']!='TERMINAL')):
            raise ValueError('Conflicting lifecycle journal')
        from .workspaces import OUTCOMES
        if record['outcome'] is not None and record['outcome'] not in OUTCOMES:raise ValueError('Unknown outcome')
        if record['phase'] in ('ABORTING','TERMINAL') and record['outcome'] is None:raise ValueError('Missing terminal outcome')
        if record['phase'] not in ('ABORTING','TERMINAL') and record['outcome'] is not None:raise ValueError('Unexpected outcome')
        if (record['phase'] in ('LAUNCHING','RUNNING') and not record['launched']) or (record['phase'] in ('STAGING','READY') and record['launched']):
            raise ValueError('Conflicting launch intent')
        lease=record['lease']
        if type(lease) is not dict or set(lease)!=set('node_id generation lease_id job_id attempt_token issued_at expires_at'.split()):
            raise ValueError('Invalid journal lease')
        hex_id(lease['lease_id'])
        if (type(lease['generation']) is not int or lease['generation']<1
                or any(type(lease[k]) not in (int,float) or not math.isfinite(lease[k]) for k in ('issued_at','expires_at'))
                or not 0<lease['expires_at']-lease['issued_at']<=60):raise ValueError('Invalid journal lease bounds')
        if (lease['job_id']!=plan.identity.job_id or lease['attempt_token']!=plan.identity.attempt_token
                or type(record['boot_id']) is not str):raise ValueError('Conflicting lifecycle identity')
        return plan

    def read(self, state, stage_id):
        hex_id(stage_id)
        try:fd=_private_file(state,stage_id+'.json',create=False)
        except FileNotFoundError:return None
        with os.fdopen(fd,'rb') as stream:raw=stream.read(MAX_BYTES+1)
        if len(raw)>MAX_BYTES:raise ValueError('Oversized journal')
        record=json.loads(raw)
        self.validate(record)
        if encoded(record)!=raw or record['stage_id']!=stage_id:raise ValueError('Noncanonical journal')
        return record

    def write(self, state, record):
        self.validate(record)
        name='.intent-'+secrets.token_hex(16)+'.tmp'
        fd=os.open(name,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600,dir_fd=state)
        try:
            with os.fdopen(fd,'wb') as stream:
                stream.write(encoded(record));stream.flush();os.fsync(stream.fileno())
            os.replace(name,record['stage_id']+'.json',src_dir_fd=state,dst_dir_fd=state)
            os.fsync(state)
        finally:
            try:os.unlink(name,dir_fd=state)
            except FileNotFoundError:pass

    def records(self):
        with self.session() as state:
            result=[]
            for name in os.listdir(state):
                if name=='service.lock' or name.startswith('.intent-'):continue
                if not name.endswith('.json'):raise ValueError('Unknown journal entry')
                result.append(self.read(state,name[:-5]))
            return result

    def close(self):
        with self._mutex:
            if self._lock is not None:os.close(self._lock);self._lock=None
