"""Optional, bounded LOCAL diagnostic snapshot from accepted signed inventory.

Not an event log, scheduler authority or new transport. No I/O on import/creation.
Only the singleton inventory owner writes; portal threads only read atomically.
"""
import json
import os
from pathlib import Path
import secrets
import stat
import time

from .backends import _directory
from .worker_cache import _protected
from .inventory_protocol import worker_id, hex_id, integer
from .nfs_mount import require_input_mount, validate_source

FILENAME = 'cache-admin-observations.json'
MAX_BYTES = 256 * 1024
MAX_WORKERS = 256


def private_json(directory, name, *, maximum=MAX_BYTES):
    """Read-only, nofollow, bounded regular private LOCAL file; never create."""
    with _directory(Path(directory)) as parent:
        _protected(parent,directory=True)
        fd=os.open(name,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK,dir_fd=parent)
        try:
            info=_protected(fd,directory=False)
            if stat.S_IMODE(info.st_mode)!=0o600 or info.st_size>maximum:raise ValueError('Invalid private document')
            raw=os.read(fd,maximum+1)
            if len(raw)>maximum:raise ValueError('Private document exceeds bound')
            return json.loads(raw)
        finally:os.close(fd)


def health(value, now_ns):
    if (type(value) is not dict or set(value)!={'schema_version','status','observed_at_ns'}
            or type(value['schema_version']) is not int or value['schema_version']!=1
            or value['status'] not in ('verified_readonly','unavailable')
            or type(value['observed_at_ns']) is not int or value['observed_at_ns']<1
            or abs(now_ns-value['observed_at_ns'])>30*10**9):return None
    return dict(value)


class NFSHealthProbe:
    """Kernel mount identity/options only: no NFS file access or liveness claim."""
    def __init__(self,source,*,target,clock=time.time_ns):
        self.source=validate_source(source);self.target=str(target);self.clock=clock
        if not Path(target).is_absolute() or '..' in Path(target).parts or Path(target)==Path('/'):
            raise ValueError('Expected trusted absolute input mountpoint')

    def __call__(self):
        try:require_input_mount(self.source,target=self.target);status='verified_readonly'
        except (OSError,ValueError):status='unavailable'
        return dict(schema_version=1,status=status,observed_at_ns=self.clock())


class AdminObservations:
    def __init__(self,root):
        self.root=Path(root)
        if not self.root.is_absolute():raise ValueError('Expected absolute LOCAL inventory state root')
        self._rows={}

    def reset(self):
        self._rows.clear();self._write()

    def accept(self,worker,session,sequence,received_ns,value):
        worker_id(worker);hex_id(session);integer(sequence,1);integer(received_ns,1)
        self._rows.pop(worker,None)
        self._rows[worker]=dict(session_id=session,sequence=sequence,received_at_ns=received_ns,
                               nfs=health(value,received_ns))
        while len(self._rows)>MAX_WORKERS:self._rows.pop(next(iter(self._rows)))
        self._write()

    def _write(self):
        raw=(json.dumps(dict(schema_version=1,workers=self._rows),sort_keys=True,allow_nan=False)+'\n').encode()
        if len(raw)>MAX_BYTES:raise ValueError('Observation bound exceeded')
        with _directory(self.root) as parent:
            _protected(parent,directory=True)
            name='.admin-observations-'+secrets.token_hex(16)+'.tmp'
            fd=os.open(name,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600,dir_fd=parent)
            try:
                with os.fdopen(fd,'wb') as stream:
                    _protected(stream.fileno(),directory=False)
                    stream.write(raw);stream.flush();os.fsync(stream.fileno())
                os.replace(name,FILENAME,src_dir_fd=parent,dst_dir_fd=parent);os.fsync(parent)
            finally:
                try:os.unlink(name,dir_fd=parent)
                except FileNotFoundError:pass


def read(root):
    try:
        value=private_json(root,FILENAME)
        if (type(value) is not dict or set(value)!={'schema_version','workers'}
                or type(value['schema_version']) is not int or value['schema_version']!=1
                or type(value['workers']) is not dict or len(value['workers'])>MAX_WORKERS):return {}
        return value['workers']
    except (OSError,ValueError,TypeError):return {}
