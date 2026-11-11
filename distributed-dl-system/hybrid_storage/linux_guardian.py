"""Independent root lease guardian. The child receives only curated FD mounts.

Invoked by a watchdog-enabled transient systemd service, never by student argv.
The training child has a dedicated non-root host UID, no capabilities and no
further user namespaces. All network policy changes are scoped to the owned set.
"""
import fcntl
import hashlib
import json
import os
from pathlib import Path
import selectors
import signal
import socket
import stat
import subprocess
import sys
import time

from .linux_boundary import LinuxConfig, LinuxBoundary, protected_json, atomic_json, run
from .input_manifests import encoded


def notify(value):
    address=os.environ.get('NOTIFY_SOCKET')
    if not address:raise PermissionError('Independent systemd watchdog required')
    if address.startswith('@'):address='\0'+address[1:]
    with socket.socket(socket.AF_UNIX,socket.SOCK_DGRAM) as sock:
        sock.connect(address);sock.sendall(value.encode())


class Guardian:
    def __init__(self,path):
        if os.geteuid()!=0:raise PermissionError('Guardian requires trusted service privileges')
        self.path=Path(path);self.record=protected_json(path)
        self.config=LinuxConfig(**self.record['config'])
        from .workspaces import AttemptIdentity
        self.identity=AttemptIdentity(**self.record['identity'])
        if self.path!=self.config.state_root/(self.identity.key+'.json'):
            raise PermissionError('Uncontrolled launch record')
        self.boundary=LinuxBoundary(self.config)
        self.boundary._record(self.identity,self.record)
        self.root=self.config.state_root/self.identity.key
        self.projection=self.root/'root'
        self.worker=self.config.worker_root
        self.stop=False;self.fds=[];self.last_notify=0

    def authority(self,*,attached=True):
        from execution_lease import read,alive,fence,lease_path,authority_path,process_key
        lease=self.record['lease']
        value=read(lease_path(self.worker,lease));owner=read(authority_path(self.worker))
        if (self.stop or not alive(value) or not alive(owner)
                or fence(value['lease'])!=fence(lease) or fence(owner['lease'])!=fence(lease)
                or value['lease']['job_id']!=self.identity.job_id
                or value['lease']['attempt_token']!=self.identity.attempt_token
                or owner['lease']['job_id']!=self.identity.job_id
                or owner['lease']['attempt_token']!=self.identity.attempt_token):
            raise PermissionError('Training lease revoked, expired or superseded')
        if attached and (not value.get('started') or type(value.get('guardian_pid')) is not int
                         or value['guardian_pid']<2 or not value.get('guardian_key')
                         or process_key(value['guardian_pid'])!=value['guardian_key']):
            raise PermissionError('Outer launch guardian is not alive')
        now=time.monotonic()
        if now-self.last_notify>=0.2:
            notify('WATCHDOG=1');self.last_notify=now
        return value,owner

    def target(self,path):
        path=Path(path)
        if not path.is_absolute() or '..' in path.parts:raise PermissionError('Noncanonical projection path')
        result=self.projection/path.relative_to('/')
        result.parent.mkdir(parents=True,exist_ok=True,mode=0o755)
        return result

    def project_json(self,path,value):
        target=self.target(path)
        raw=encoded(value)
        # This directory is inaccessible on the host; the child sees its RO bind.
        temporary=target.with_name('.'+target.name+'.next')
        with temporary.open('wb') as f:f.write(raw);f.flush();os.fsync(f.fileno())
        temporary.chmod(0o444);os.replace(temporary,target)

    def copy_file(self,source,target,remaining,*,executable=False):
        self.authority()
        fd=os.open(source,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
        with os.fdopen(fd,'rb') as incoming:
            before=os.fstat(incoming.fileno())
            from .mount_identity import require_local_fd
            require_local_fd(incoming.fileno())
            if not stat.S_ISREG(before.st_mode) or before.st_nlink!=1 or before.st_size>remaining:
                raise PermissionError('Input projection must be bounded regular LOCAL data')
            target.parent.mkdir(parents=True,exist_ok=True,mode=0o755)
            total=0
            with target.open('xb') as out:
                while True:
                    self.authority()
                    chunk=incoming.read(min(1024**2,before.st_size-total+1))
                    if not chunk:break
                    total+=len(chunk)
                    if total>before.st_size:raise ValueError('Projection input changed')
                    out.write(chunk)
                out.flush();os.fsync(out.fileno())
            after=os.fstat(incoming.fileno())
            if (before.st_ino,before.st_size,before.st_mtime_ns,before.st_ctime_ns)!=(after.st_ino,after.st_size,after.st_mtime_ns,after.st_ctime_ns) or total!=before.st_size:
                raise ValueError('Projection input changed')
            target.chmod(0o555 if executable else 0o444)
            return total

    def copy_inputs(self):
        source=Path(self.record['workspace']);used=0
        free=os.statvfs(self.root)
        if free.f_bavail*free.f_frsize<self.config.max_input_bytes+self.config.reserve_bytes:
            raise OSError('Insufficient LOCAL space for isolated input projection')
        def tree(path):
            nonlocal used
            for entry in sorted(os.scandir(path),key=lambda e:e.name):
                item=Path(entry.path);info=entry.stat(follow_symlinks=False)
                if stat.S_ISDIR(info.st_mode):
                    self.target(item).mkdir(mode=0o755,exist_ok=True);tree(item)
                elif stat.S_ISREG(info.st_mode):
                    used+=self.copy_file(item,self.target(item),self.config.max_input_bytes-used,executable=bool(info.st_mode&0o111))
                else:raise PermissionError('Unsafe input projection entry')
        tree(source/'inputs')
        resume=source/'checkpoints/resume.dlckpt'
        if resume.exists():self.copy_file(resume,self.target(resume),self.config.max_checkpoint_bytes)

    def mount_output(self,path,size):
        if path.is_symlink() or not path.is_dir() or any(path.iterdir()):
            raise PermissionError('New empty controlled output directory required')
        # An ordinary mountpoint must never be mistaken for a previous tmpfs.
        from .nfs_mount import parse_mountinfo
        if any(m['target']==str(path) for m in parse_mountinfo(Path('/proc/self/mountinfo').read_text())):
            raise PermissionError('Output path is already a mount; never cover existing data')
        self.authority()
        run(['/usr/bin/mount','-t','tmpfs','-o',f'size={size},nr_inodes=4096,mode=0700,uid={self.config.trainer_uid},gid={self.config.trainer_gid},nosuid,nodev,noexec',
             'dl-hybrid-'+self.identity.key,str(path)])

    def set_peers(self,values):
        if not set(values)<=set(self.config.peers):raise PermissionError('Unknown DDP peers')
        script=f'flush set inet {self.config.nft_table} active_peers\n'
        if values:script+=f'add element inet {self.config.nft_table} active_peers {{ '+', '.join(values)+' }\n'
        run(['/usr/sbin/nft','-f','-'],input=script)

    def bindings(self):
        """Descriptors bypass private host parents, never grant host root identity."""
        c=self.config
        argv=['/usr/bin/bwrap','--unshare-user','--unshare-pid','--unshare-ipc','--unshare-uts',
              '--unshare-cgroup','--disable-userns','--cap-drop','ALL','--die-with-parent','--new-session']
        def bind(path,dest,readonly=True):
            fd=os.open(path,os.O_PATH|os.O_CLOEXEC)
            self.fds.append(fd);argv.extend(['--ro-bind-fd' if readonly else '--bind-fd',str(fd),str(dest)])
        bind(self.projection,'/')
        for path in ['/usr','/lib','/lib64']:
            if Path(path).exists():bind(path,path)
        bind(c.code_root,c.code_root)
        env=Path(self.record['environment']);bind(env,env)
        capsule=protected_json(c.worker_root/'worker-storage/capsule.json')
        from .backends import _root
        site=_root(Path(capsule['site_root']))
        self.target(site).mkdir(parents=True,exist_ok=True,mode=0o755)
        bind(site,site)
        job=c.worker_root/'worker-storage/jobs'/self.identity.job_id
        for name in ('results','checkpoints'):bind(job/name,job/name,False)
        argv.extend(['--proc','/proc','--dev','/dev','--size',str(64*1024**2),'--tmpfs','/tmp',
            '--clearenv','--setenv','PATH','/usr/bin:/bin','--setenv','HOME','/tmp',
            '--setenv','PYTHONDONTWRITEBYTECODE','1','--setenv','DL_EXECUTION_LEASE_PATH',
            str(c.worker_root/'worker-storage/leases'/(self.record['lease']['lease_id']+'.json'))])
        value,_=self.authority()
        argv.extend(['--setenv','DL_GUARDIAN_PID',str(value['guardian_pid']),
                     '--chdir',str(c.worker_root),'--',*self.record['command']])
        return argv

    def execute(self):
        c=self.config;self.boundary.preflight()
        lock=os.open(c.state_root/'execute.lock',os.O_RDWR|os.O_CREAT|os.O_NOFOLLOW,0o600)
        process=None;owned=False
        try:
            fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
            owned=True
            self.authority(attached=False)
            notify('READY=1\nWATCHDOG=1')
            # attach() follows Popen in the existing agent; wait only for that race.
            deadline=time.monotonic()+1
            while True:
                try:self.authority();break
                except PermissionError:
                    if time.monotonic()>=deadline:raise
                    time.sleep(0.02)
            self.root.mkdir(mode=0o700)
            # The service's 0077 umask protects host state. Curated directories
            # must be traversable by the dropped trainer UID inside its RO root;
            # the enclosing host attempt directory remains 0700 and inaccessible.
            os.umask(0o022)
            from execution_lease import process_key
            self.record.update(guardian_pid=os.getpid(),guardian_key=process_key(os.getpid()))
            atomic_json(self.path,self.record)
            self.projection.mkdir(mode=0o755)
            (self.projection/'bin').symlink_to('usr/bin')
            (self.projection/'sbin').symlink_to('usr/sbin')
            job=c.worker_root/'worker-storage/jobs'/self.identity.job_id
            for name in ('request.json','prepared.json'):
                self.project_json(job/name,protected_json(job/name))
            for name in ('usr','lib','lib64','proc','dev','tmp',str(c.code_root).lstrip('/'),str(Path(self.record['environment'])).lstrip('/')):
                (self.projection/name).mkdir(parents=True,exist_ok=True,mode=0o755)
            self.copy_inputs()
            mounted=[]
            for name,limit in [('results',c.max_output_bytes),('checkpoints',c.max_checkpoint_bytes)]:
                self.target(job/name).mkdir(mode=0o755,exist_ok=True)
                self.mount_output(job/name,limit);mounted.append(str(job/name))
                self.record['mounted']=mounted;atomic_json(self.path,self.record)
            self.set_peers(self.record['peers'])
            self.record['phase']='RUNNING';atomic_json(self.path,self.record)
            from execution_lease import lease_path,authority_path
            value,owner=self.authority()
            self.project_json(lease_path(c.worker_root,self.record['lease']),value)
            self.project_json(authority_path(c.worker_root),owner)
            argv=self.bindings()
            manifest=os.memfd_create('dl-namespace-manifest',os.MFD_CLOEXEC)
            os.fchmod(manifest,0o600)
            os.write(manifest,encoded(dict(argv=argv,sources=self.fds,uid=c.trainer_uid,gid=c.trainer_gid)));os.lseek(manifest,0,os.SEEK_SET)
            process=subprocess.Popen(['/usr/bin/python3','-I','-B',str(c.code_root/'tools/hybrid_namespace.py'),str(manifest)],
                                     pass_fds=(*self.fds,manifest),
                                     stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
            os.close(manifest)
            for fd in self.fds:os.close(fd)
            self.fds=[]
            total=0
            with selectors.DefaultSelector() as selector:
                selector.register(process.stdout,selectors.EVENT_READ)
                while process.poll() is None or selector.get_map():
                    value,owner=self.authority()
                    self.project_json(lease_path(c.worker_root,self.record['lease']),value)
                    self.project_json(authority_path(c.worker_root),owner)
                    for key,_ in selector.select(timeout=0.2):
                        chunk=os.read(key.fileobj.fileno(),64*1024)
                        if not chunk:selector.unregister(key.fileobj);continue
                        accepted=chunk[:max(0,c.max_log_bytes-total)]
                        if accepted:sys.stdout.buffer.write(accepted);sys.stdout.buffer.flush()
                        total+=len(accepted)
            return process.wait(timeout=1)
        finally:
            # Network admission disappears even when setup or training fails.
            try:
                if owned:self.set_peers([])
            finally:
                if process is not None and process.poll() is None:
                    process.kill()
                    try:process.wait(timeout=1)
                    except subprocess.TimeoutExpired:pass
                for fd in self.fds:os.close(fd)
                if self.root.exists():
                    self.record['phase']='FINISHED';atomic_json(self.path,self.record)
                os.close(lock)
            # KillMode=control-group closes every surviving namespace/descendant
            # after this root main process exits; quiescent() verifies the cgroup.


def main(path):
    guardian=Guardian(path)
    def stopped(*_):guardian.stop=True
    signal.signal(signal.SIGTERM,stopped);signal.signal(signal.SIGINT,stopped)
    return guardian.execute()
