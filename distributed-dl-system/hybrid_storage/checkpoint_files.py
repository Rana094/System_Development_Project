"""Protected local checkpoint files. Never write to the exported artifact CAS."""
from contextlib import contextmanager
import errno
import hashlib
import os
from pathlib import Path
import stat
import sys

from .artifacts import _identity
from .backends import _directory
from .worker_cache import _protected, CHUNK_BYTES

# Reuse the existing non-package SDK imports, as controller/custom_checkpoints
# does. Do not import agent.py (its Flask runtime creates local directories).
_sdk=str(Path(__file__).resolve().parents[1]/'agent')
if _sdk not in sys.path:sys.path.append(_sdk)
from execution_lease import validate_fence


@contextmanager
def private_root(path):
    with _directory(path) as fd:
        _protected(fd,directory=True)
        yield fd


@contextmanager
def regular(parent,name,*,private=False):
    fd=os.open(name,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK,dir_fd=parent)
    try:
        value=os.fstat(fd)
        if not stat.S_ISREG(value.st_mode) or value.st_nlink!=1:
            raise PermissionError('Checkpoint must be a single-link regular file')
        if private:_protected(fd,directory=False)
        with os.fdopen(fd,'rb') as stream:
            fd=None
            yield stream
    finally:
        if fd is not None:os.close(fd)


def stream_copy(source,destination,*,size,sha256=None,check=lambda:None):
    """Consume at most declared bytes plus one, with no unbounded read allocation."""
    count=0;digest=hashlib.sha256()
    while True:
        check()
        chunk=source.read(min(CHUNK_BYTES,size-count+1))
        if type(chunk) is not bytes:raise ValueError('Expected binary checkpoint stream')
        if not chunk:break
        count+=len(chunk)
        if count>size:raise ValueError('Checkpoint exceeds declared size')
        destination.write(chunk);digest.update(chunk)
    if count!=size or (sha256 is not None and digest.hexdigest()!=sha256):
        raise ValueError('Checkpoint size/hash mismatch')
    destination.flush();os.fsync(destination.fileno())
    return digest.hexdigest()


def space(fd,size,reserve):
    free=os.fstatvfs(fd)
    if free.f_bavail*free.f_frsize<size+reserve:
        raise OSError(errno.ENOSPC,'Checkpoint free-space admission failed')


def snapshot_source(source,destination,*,maximum,reserve,check=lambda:None):
    before=os.fstat(source.fileno())
    if not 0<before.st_size<=maximum:raise ValueError('Checkpoint source size exceeds bound')
    space(destination.fileno(),before.st_size,reserve)
    digest=stream_copy(source,destination,size=before.st_size,check=check)
    if _identity(before)!=_identity(os.fstat(source.fileno())):
        raise ValueError('Checkpoint candidate changed during snapshot')
    return digest,before.st_size


def validate_fd(fd,job_id,manifest,token,kind):
    # A controlled /proc descriptor alias keeps validation anchored to our file,
    # even if an untrusted candidate path changes. No student-supplied symlink.
    from checkpoint_protocol import validate,validate_group
    return (validate_group if kind=='ddp' else validate)(Path(f'/proc/self/fd/{fd}'),job_id,manifest,token)
