"""Explicit LOCAL SQLite backup/restore verification, with no live restore API.

Only creates a NEW private directory; source is opened mode=ro and never migrated.
This is a database recovery point, not a coordinated artifact/checkpoint backup,
not off-host redundancy and not deployment/maintenance approval.
"""
import argparse
from contextlib import closing
import errno
import hashlib
import json
import os
from pathlib import Path
import stat
import struct
import time
from urllib.parse import quote

from .backends import _directory, _root
from .mount_identity import require_local_fd


def _uri(path, mode):
    return 'file:'+quote(str(path),safe='/')+'?mode='+mode


def _connect(path, mode):
    import sqlite3
    db=sqlite3.connect(_uri(path,mode),uri=True,timeout=1)
    db.execute('PRAGMA trusted_schema=OFF')
    db.execute('PRAGMA busy_timeout=1000')
    if mode=='ro':db.execute('PRAGMA query_only=ON')
    return db


def _check_deadline(deadline):
    if time.monotonic()>deadline:raise TimeoutError('Backup verification deadline exceeded')


def _fingerprint(db, deadline):
    digest=hashlib.sha256();counts={}
    schema=list(db.execute("SELECT type,name,tbl_name,sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY type,name"))
    digest.update(json.dumps(schema,separators=(',',':')).encode())
    for kind,name,table,sql in schema:
        if kind!='table':continue
        if sql.upper().startswith('CREATE VIRTUAL TABLE'):
            raise ValueError('Virtual-table backup verification requires explicit review')
        identifier='"'+name.replace('"','""')+'"';count=0
        for row in db.execute('SELECT * FROM '+identifier):
            _check_deadline(deadline);digest.update(b'ROW')
            for value in row:
                if value is None:tag,data=b'n',b''
                elif isinstance(value,int):tag,data=b'i',str(value).encode()
                elif isinstance(value,float):tag,data=b'f',struct.pack('>d',value)
                elif isinstance(value,str):tag,data=b's',value.encode()
                elif isinstance(value,bytes):tag,data=b'b',value
                else:raise ValueError('Unsupported SQLite value')
                digest.update(tag);digest.update(len(data).to_bytes(8,'big'));digest.update(data)
            count+=1
        counts[name]=count
    return dict(logical_sha256=digest.hexdigest(),table_counts=counts,
                user_version=db.execute('PRAGMA user_version').fetchone()[0])


def _new_file(parent,name):
    fd=os.open(name,os.O_RDWR|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600,dir_fd=parent)
    try:require_local_fd(fd);os.fsync(fd)
    finally:os.close(fd)


def _attest_source(path):
    with _directory(path.parent) as parent:
        fd=os.open(path.name,os.O_PATH|os.O_NOFOLLOW|os.O_CLOEXEC,dir_fd=parent)
    try:
        info=os.fstat(fd)
        if stat.S_ISLNK(info.st_mode):raise OSError(errno.ELOOP,'Symlink source DB refused',path)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink!=1:raise ValueError('Expected single-link source DB')
        require_local_fd(fd)
        return info.st_dev,info.st_ino
    finally:os.close(fd)


def _validate(db):
    if db.execute('PRAGMA quick_check').fetchall()!=[('ok',)]:raise ValueError('SQLite quick_check failed')
    if db.execute('PRAGMA foreign_key_check').fetchone() is not None:raise ValueError('SQLite foreign_key_check failed')


def backup(source, output, *, max_bytes=512*1024**2, reserve_bytes=256*1024**2, timeout_seconds=120):
    """Bounded online backup + separate restored copy; never replace any path.

    Readers may coexist with the live writer. Source WAL is read through SQLite;
    immutable=1 is deliberately forbidden. Failure retains only NEW incomplete
    output, without a completion manifest. Never retry over it.
    """
    source=_root(Path(source));output=_root(Path(output))
    if source.is_relative_to(output) or output.is_relative_to(source):raise ValueError('Source/output overlap')
    if type(max_bytes) is not int or max_bytes<=0 or type(reserve_bytes) is not int or reserve_bytes<0:
        raise ValueError('Expected positive byte limit and nonnegative free reserve')
    if type(timeout_seconds) not in (int,float) or not 0<timeout_seconds<=3600:raise ValueError('Invalid deadline')
    identity=_attest_source(source);deadline=time.monotonic()+timeout_seconds
    with _directory(output.parent) as parent:
        require_local_fd(parent);usage=os.fstatvfs(parent)
        free=usage.f_bavail*usage.f_frsize
        with closing(_connect(source,'ro')) as src:
            estimate=src.execute('PRAGMA page_count').fetchone()[0]*src.execute('PRAGMA page_size').fetchone()[0]
            if estimate>max_bytes:raise OSError('Source DB exceeds backup byte limit')
            if free<2*estimate+reserve_bytes:raise OSError('Insufficient free space for backup plus restored copy and reserve')
        os.mkdir(output.name,mode=0o700,dir_fd=parent);os.fsync(parent)
    with _directory(output) as target:
        if stat.S_IMODE(os.fstat(target).st_mode)!=0o700:raise PermissionError('Private output mode required')
        _new_file(target,'snapshot.sqlite');_new_file(target,'restored.sqlite')
    def progress(status,remaining,total):
        _check_deadline(deadline)
        if total*page_size>max_bytes:raise OSError('Growing source DB exceeds backup byte limit')
    # sqlite Connection context managers do not close; explicitly close every handle.
    with closing(_connect(source,'ro')) as src, \
            closing(_connect(output/'snapshot.sqlite','rw')) as snapshot:
        page_size=src.execute('PRAGMA page_size').fetchone()[0]
        src.backup(snapshot,pages=128,progress=progress,sleep=0.01)
        if _attest_source(source)!=identity:raise ValueError('Live DB path changed during backup')
    with closing(_connect(output/'snapshot.sqlite','ro')) as snapshot, \
            closing(_connect(output/'restored.sqlite','rw')) as restored:
        snapshot.backup(restored,pages=128,progress=progress,sleep=0.01)
        for db in (snapshot,restored):
            db.set_progress_handler(lambda:int(time.monotonic()>deadline),1000)
            _validate(db)
        original=_fingerprint(snapshot,deadline);copy=_fingerprint(restored,deadline)
        if original!=copy:raise ValueError('Restored DB differs from recovery point')
    records={}
    with _directory(output) as target:
        for name in ('snapshot.sqlite','restored.sqlite'):
            fd=os.open(name,os.O_RDONLY|os.O_NOFOLLOW,dir_fd=target)
            with os.fdopen(fd,'rb') as stream:
                os.fsync(stream.fileno());info=os.fstat(stream.fileno())
                if info.st_size>max_bytes:raise OSError('Recovery copy exceeds byte limit')
                records[name]=dict(bytes=info.st_size,sha256=hashlib.file_digest(stream,'sha256').hexdigest())
        document=dict(schema_version=1,scope='DATABASE_ONLY_LOCAL_RECOVERY_POINT',source=str(source),
            source_open_mode='ro',source_identity=list(identity),verified_at_ns=time.time_ns(),
            restored_copy_verified=True,live_restore=False,coordinated_filesystem_backup=False,
            off_host_redundancy=False,files=records,**original)
        raw=(json.dumps(document,indent=2,sort_keys=True)+'\n').encode()
        fd=os.open('backup.json',os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600,dir_fd=target)
        with os.fdopen(fd,'wb') as stream:stream.write(raw);stream.flush();os.fsync(stream.fileno())
        os.fsync(target)
    return document


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source',required=True,type=Path)
    parser.add_argument('--output',required=True,type=Path)
    args=parser.parse_args(argv)
    result=backup(args.source,args.output)
    # No row data, principals or stored credentials are printed.
    print(json.dumps(dict(scope=result['scope'],restored_copy_verified=True,
                         coordinated_filesystem_backup=False,output=str(args.output)),indent=2))


if __name__=='__main__':main()
