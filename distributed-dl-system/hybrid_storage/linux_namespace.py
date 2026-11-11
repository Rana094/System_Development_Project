"""Trusted mount setup, then irreversible host UID drop before Bubblewrap.

Inherited O_PATH descriptors are bound to aliases in a private mount namespace.
The aliases never appear on the host. This avoids granting the trainer traversal
of private service/cache/credential parents, or mapping host root into its userns.
"""
import ctypes
import json
import os
from pathlib import Path
import stat
import subprocess


def command(argv,sources,*,mount):
    """Convert only inherited mount operands to private accessible aliases."""
    if not isinstance(argv,list) or not argv or argv[0]!='/usr/bin/bwrap' or any(type(x) is not str for x in argv):
        raise ValueError('Trusted Bubblewrap argv required')
    if not isinstance(sources,list) or len(sources)>32 or any(type(x) is not int or not 3<=x<65536 for x in sources) or len(set(sources))!=len(sources):
        raise ValueError('Bounded exact source descriptors required')
    result=[];seen=set();index=0
    while index<len(argv):
        value=argv[index]
        if value=='--':
            result.extend(argv[index:]);break
        if value in ('--ro-bind-fd','--bind-fd'):
            fd=int(argv[index+1]);destination=argv[index+2]
            if fd not in sources or fd in seen or not Path(destination).is_absolute():raise ValueError('Uncontrolled mount descriptor')
            seen.add(fd);alias=Path('/tmp')/('dl-binding-'+str(len(seen)))
            mount(fd,alias)
            result.extend(['--ro-bind' if value=='--ro-bind-fd' else '--bind',str(alias),destination]);index+=3
        else:result.append(value);index+=1
    if seen!=set(sources) or '--' not in result:raise ValueError('Missing mount/command boundary')
    return result


def main(fd):
    if os.geteuid()!=0:raise PermissionError('Only trusted guardian may set up mount aliases')
    info=os.fstat(fd)
    if not stat.S_ISREG(info.st_mode) or info.st_uid!=0 or info.st_size>1024**2:raise PermissionError('Invalid guardian manifest descriptor')
    raw=os.read(fd,1024**2+1);os.close(fd);value=json.loads(raw)
    if set(value)!={'argv','sources','uid','gid'} or any(type(value[n]) is not int or not 100<=value[n]<60000 for n in ('uid','gid')):
        raise ValueError('Invalid namespace manifest')
    libc=ctypes.CDLL(None,use_errno=True)
    libc.mount.argtypes=[ctypes.c_char_p,ctypes.c_char_p,ctypes.c_char_p,ctypes.c_ulong,ctypes.c_void_p]
    libc.mount.restype=ctypes.c_int
    if libc.prctl(39,0,0,0,0)!=1:raise PermissionError('Independent unit must enforce NoNewPrivileges')
    if libc.unshare(0x00020000)!=0:raise OSError(ctypes.get_errno(),'Private mount namespace unavailable')
    # An inherited descriptor retains its old namespace's vfsmount. Reopen the
    # same protected inode through the freshly cloned namespace BEFORE masking
    # /tmp; binding the old namespace descriptor would fail with EINVAL.
    for source in value['sources']:
        original=os.fstat(source);name=os.readlink('/proc/self/fd/'+str(source))
        replacement=os.open(name,os.O_PATH|os.O_NOFOLLOW|os.O_CLOEXEC)
        try:
            current=os.fstat(replacement)
            if (original.st_dev,original.st_ino,original.st_mode)!=(current.st_dev,current.st_ino,current.st_mode):
                raise PermissionError('Source identity changed at namespace boundary')
            os.dup2(replacement,source,inheritable=False)
        finally:os.close(replacement)
    def run(argv,**kw):subprocess.run(argv,check=True,capture_output=True,timeout=10,**kw)
    run(['/usr/bin/mount','--make-rprivate','/'])
    run(['/usr/bin/mount','-t','tmpfs','-o','size=1048576,nr_inodes=128,mode=0755,nosuid,nodev','dl-private-bindings','/tmp'])
    os.umask(0o022)
    def mount(source,alias):
        os.fstat(source);alias.mkdir(mode=0o755)
        # libmount canonicalizes proc-fd paths back to host names, which may be
        # hidden by the private /tmp mount. Use the kernel's descriptor-following
        # bind operation directly, before dropping host root privileges.
        if libc.mount(os.fsencode('/proc/self/fd/'+str(source)),os.fsencode(alias),None,4096,None)!=0:
            raise OSError(ctypes.get_errno(),'Private descriptor bind failed for fd '+str(source)+' ('+os.readlink('/proc/self/fd/'+str(source))+')')
    argv=command(value['argv'],value['sources'],mount=mount)
    for source in value['sources']:os.close(source)
    os.setgroups([]);os.setgid(value['gid']);os.setuid(value['uid'])
    # All root mount/source descriptors are closed before untrusted execution.
    # Bubblewrap maps this already non-root host identity, then drops namespace
    # capabilities and prevents further user namespaces.
    os.execv('/usr/bin/bwrap',argv)
