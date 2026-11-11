"""Read-only protected NFS readiness checks. No install/config/service actions.

The TEST write-denial probe opens an EXISTING immutable public TEST object with
O_WRONLY, without CREATE/TRUNCATE/APPEND, writes zero bytes and closes immediately
if unexpectedly permitted. Only EROFS proves the expected read-only rejection.
Do not use this probe for outage/fault injection or untrusted path selection.
"""
import argparse
import errno
import hashlib
import json
import os
from pathlib import Path
import socket
import stat
import sys
import time

from .nfs_mount import require_input_mount, validate_source

TEST_SHA='0cb28871cf66d68026f61a434086adfca958eb18c1353bd76fde4151a687e897'
TEST_KEY='sha256/0c/'+TEST_SHA
TEST_BYTES=165


def _open(path, flags):
    """Anchor all parent components without following symlinks."""
    path=Path(path)
    if not path.is_absolute() or '..' in path.parts:raise ValueError('Expected absolute controlled path')
    parent=os.open('/',os.O_RDONLY|os.O_DIRECTORY)
    try:
        for part in path.parts[1:-1]:
            child=os.open(part,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=parent)
            os.close(parent);parent=child
        return os.open(path.name,flags|os.O_NOFOLLOW|os.O_NONBLOCK,dir_fd=parent)
    finally:os.close(parent)


def _text(path,limit=1024*1024):
    fd=_open(path,os.O_RDONLY)
    with os.fdopen(fd,'rb') as stream:
        info=os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode):raise ValueError('Expected regular metadata file')
        data=stream.read(limit+1)
        if len(data)>limit:raise ValueError('Metadata file exceeds bound')
        return data.decode()


def _metadata(path):
    fd=_open(path,os.O_RDONLY|os.O_DIRECTORY)
    try:
        info=os.fstat(fd);names=sorted(os.listdir(fd))
        if len(names)>128:raise ValueError('Backup journal entry count exceeds probe bound')
        return dict(uid=info.st_uid,gid=info.st_gid,mode=oct(stat.S_IMODE(info.st_mode)),
                    entry_names=names,contents_read=False,restore_verified=False)
    finally:os.close(fd)


def _attempt(operation):
    try:return dict(ok=True,value=operation())
    except (OSError,ValueError) as e:return dict(ok=False,error=type(e).__name__,errno=getattr(e,'errno',None),reason=str(e))


def _test_object(path, *, test_write):
    fd=_open(path,os.O_RDONLY)
    with os.fdopen(fd,'rb') as stream:
        info=os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_size!=TEST_BYTES:raise ValueError('Unexpected TEST object type/size')
        data=stream.read(TEST_BYTES+1)
    if len(data)!=TEST_BYTES or hashlib.sha256(data).hexdigest()!=TEST_SHA:raise ValueError('TEST hash mismatch')
    result=dict(sha256=TEST_SHA,bytes=TEST_BYTES,hash_verified=True,bytes_written=0,
                write_open_attempted=test_write,read_only_write_denial_verified=False)
    if test_write:
        try:
            fd=_open(path,os.O_WRONLY)
        except OSError as e:
            result.update(write_open_errno=e.errno,read_only_write_denial_verified=e.errno==errno.EROFS)
        else:
            os.close(fd);result.update(write_open_errno=None,unexpected_write_open=True)
    return result


def probe(role, *, source=None, privileged=False, verify_test=False):
    if role not in ('controller','worker'):raise ValueError('Invalid role')
    if privileged and os.geteuid()!=0:raise PermissionError('Explicit privileged checks require approved root execution')
    if verify_test and not privileged:raise PermissionError('TEST read/write-open check requires explicit privileged mode')
    result=dict(schema_version=1,at_utc_ns=time.time_ns(),role=role,hostname=socket.gethostname(),
                euid=os.geteuid(),privileged=privileged,read_only_probe=True,
                writes_or_config_changes=False,live_restore=False)
    if role=='controller':
        files=['/etc/exports','/etc/exports.d/distributed-ml.exports']
        if privileged:files+=['/var/lib/nfs/etab','/proc/fs/nfsd/versions']
        result['metadata_files']={p:_attempt(lambda p=p:_text(p)) for p in files}
        if privileged:result['setup_backup_journal']=_attempt(lambda:_metadata('/var/backups/distributed-ml-nfs'))
        if verify_test:result['public_test_object']=_attempt(lambda:_test_object('/srv/ml-shared/objects/'+TEST_KEY,test_write=False))
    else:
        source=validate_source(source)
        # /proc/self is a kernel symlink; keep no-follow traversal by naming
        # only this process's numeric PID instead of weakening _open globally.
        mountinfo=_text('/proc/'+str(os.getpid())+'/mountinfo');result['source']=source
        result['mount_admission']=_attempt(lambda:require_input_mount(source,mountinfo))
        if privileged:result['setup_backup_journal']=_attempt(lambda:_metadata('/var/backups/distributed-ml-nfs-client'))
        if verify_test:
            if not result['mount_admission']['ok']:
                result['public_test_object']=dict(ok=False,reason='No matching RO NFS input mount; no ordinary-directory fallback')
            else:result['public_test_object']=_attempt(lambda:_test_object('/srv/mlnfs/inputs/'+TEST_KEY,test_write=True))
    return result


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--role',required=True,choices=('controller','worker'))
    parser.add_argument('--source',help='Reviewed literal IPv4 NFS input source, worker only')
    parser.add_argument('--privileged',action='store_true',help='Only after operator approval; requires uid0')
    parser.add_argument('--verify-test-input',action='store_true',help='Read existing public 165-byte TEST object; worker also tests write-open denial without writing')
    args=parser.parse_args(argv)
    print(json.dumps(probe(args.role,source=args.source,privileged=args.privileged,verify_test=args.verify_test_input),indent=2))


if __name__=='__main__':main()
