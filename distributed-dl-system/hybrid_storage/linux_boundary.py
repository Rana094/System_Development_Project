"""Explicit Linux execution boundary; provisioning is a separate reviewed action.

Root services retain staging authority. Each trainer runs as a dedicated host UID
inside Bubblewrap, under a systemd cgroup with an independent lease guardian.
No fallback to host execution, automatic package installation or firewall setup.
"""
from dataclasses import asdict, dataclass
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import pwd
import re
import stat
import subprocess

from .runtime_bootstrap import LaunchBoundary, _private_address
from .backends import _directory, _root
from .worker_cache import _protected
from .input_manifests import encoded
from .workspaces import AttemptIdentity, WorkspaceReceipt


def protected_json(path, *, maximum=1024*1024):
    path = Path(path)
    with _directory(path.parent) as parent:
        _protected(parent, directory=True)
        fd = os.open(path.name, os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK, dir_fd=parent)
        try:
            info = _protected(fd, directory=False)
            if not stat.S_ISREG(info.st_mode) or info.st_size > maximum:
                raise ValueError('Invalid protected control file')
            raw = os.read(fd, maximum+1)
            if len(raw)>maximum: raise ValueError('Control file too large')
            from .input_manifests import decoded
            return decoded(raw)
        finally: os.close(fd)


def atomic_json(path, value, *, new=False):
    """Only root-private control records, never arbitrary student filenames."""
    import secrets
    path = Path(path)
    with _directory(path.parent) as parent:
        _protected(parent, directory=True)
        name = '.control-'+secrets.token_hex(16)
        fd = os.open(name,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600,dir_fd=parent)
        try:
            with os.fdopen(fd,'wb') as f:
                f.write(encoded(value));f.flush();os.fsync(f.fileno())
            if new:
                from .artifacts import _rename_noreplace
                _rename_noreplace(parent,name,parent,path.name)
            else: os.replace(name,path.name,src_dir_fd=parent,dst_dir_fd=parent)
            os.fsync(parent)
        finally:
            try: os.unlink(name,dir_fd=parent)
            except FileNotFoundError: pass


@dataclass(frozen=True)
class LinuxConfig:
    worker_root: Path
    code_root: Path
    state_root: Path
    trainer_uid: int
    trainer_gid: int
    peers: tuple[str,...]
    controller_ip: str
    policy_sha256: str
    nft_table: str = 'dl_hybrid_pilot'
    agent_unit: str = 'dl-hybrid-node.service'
    service_port: int = 5001
    max_input_bytes: int = 1024**3
    max_output_bytes: int = 64*1024**2
    max_checkpoint_bytes: int = 256*1024**2
    max_log_bytes: int = 64*1024**2
    reserve_bytes: int = 512*1024**2
    max_retained_attempts: int = 2

    def __post_init__(self):
        for key in ('worker_root','code_root','state_root'):
            object.__setattr__(self,key,_root(Path(getattr(self,key))))
        if self.state_root.is_relative_to(self.worker_root) or self.worker_root.is_relative_to(self.state_root):
            raise ValueError('Boundary state must be separate from worker state')
        if any(type(n) is not int or not 100<=n<60000 for n in (self.trainer_uid,self.trainer_gid)):
            raise ValueError('Dedicated non-root training identity required')
        peers=tuple(str(_private_address(ip)) for ip in self.peers)
        if not 1<=len(peers)<=256 or len(set(peers))!=len(peers):raise ValueError('Explicit unique LAN peers required')
        object.__setattr__(self,'peers',peers)
        _private_address(self.controller_ip)
        if self.controller_ip in peers:raise ValueError('Controller cannot be a training peer')
        if not re.fullmatch('[a-z][a-z0-9_]{0,31}',self.nft_table):raise ValueError('Invalid owned nft table')
        if not re.fullmatch('dl-hybrid-[a-z0-9-]+[.]service',self.agent_unit):raise ValueError('Invalid owned agent unit')
        if type(self.service_port) is not int or not 1024<=self.service_port<20000:raise ValueError('Control port must be outside DDP range')
        if not re.fullmatch('[0-9a-f]{64}',self.policy_sha256):raise ValueError('Reviewed policy receipt required')
        for key in ('max_input_bytes','max_output_bytes','max_checkpoint_bytes','max_log_bytes','reserve_bytes'):
            if type(getattr(self,key)) is not int or not 0<getattr(self,key)<=8*1024**3:raise ValueError('Invalid bounded disk budget')
        if type(self.max_retained_attempts) is not int or not 1<=self.max_retained_attempts<=16:raise ValueError('Invalid retained-attempt bound')

    def document(self):
        return {k:str(v) if isinstance(v,Path) else v for k,v in asdict(self).items()}


def nft_policy(config):
    """Fresh, dedicated table only; never flush or rewrite unrelated rules."""
    return f'''table inet {config.nft_table} {{
  set active_peers {{ type ipv4_addr; }}
  chain training_egress {{
    type filter hook output priority -150; policy accept;
    meta skuid {config.trainer_uid} ip daddr @active_peers tcp dport 20000-65535 accept
    meta skuid {config.trainer_uid} drop
  }}
  chain control_ingress {{
    type filter hook input priority -150; policy accept;
    ip saddr {config.controller_ip} tcp dport {config.service_port} accept
    tcp dport {config.service_port} drop
  }}
}}
'''


def policy_digest(document,config):
    """Ignore kernel handles and the separately validated per-attempt set only."""
    clean=[];seen=False
    for entry in document.get('nftables',[]):
        if set(entry)=={'metainfo'}:continue
        if len(entry)!=1:raise ValueError('Invalid nft receipt')
        kind,value=next(iter(entry.items()))
        if kind not in ('table','set','chain','rule') or not isinstance(value,dict):raise ValueError('Unexpected nft object')
        value=dict(value);value.pop('handle',None)
        if value.get('family')!='inet' or value.get('table',value.get('name'))!=config.nft_table:
            raise ValueError('Policy receipt escaped its owned table')
        if kind=='set':
            if value.get('name')!='active_peers' or value.get('type')!='ipv4_addr':raise ValueError('Unexpected nft set')
            elements=value.pop('elem',[])
            if any(type(ip) is not str or ip not in config.peers for ip in elements):raise ValueError('Unauthorized DDP peer')
            seen=True
        clean.append({kind:value})
    if not seen:raise ValueError('Missing restricted peer set')
    return hashlib.sha256(encoded(clean)).hexdigest()


def run(argv, **kwargs):
    return subprocess.run(argv,check=True,capture_output=True,text=True,timeout=10,**kwargs)


def unit_name(identity):
    if not isinstance(identity,AttemptIdentity):raise TypeError('Exact attempt identity required')
    return 'dl-hybrid-job-'+identity.key+'.service'


class LinuxBoundary(LaunchBoundary):
    def __init__(self,config,*,command=run):
        if not isinstance(config,LinuxConfig):raise TypeError('Operator Linux configuration required')
        self.config,self.command=config,command

    def preflight(self):
        if os.geteuid()!=0:raise PermissionError('Trusted boundary requires root service; training never runs as root')
        c=self.config
        for root in (c.worker_root,c.state_root):
            with _directory(root) as fd:_protected(fd,directory=True)
        for path in (c.code_root/'tools/hybrid_guardian.py',c.code_root/'tools/hybrid_namespace.py',Path('/usr/bin/bwrap'),Path('/usr/bin/systemd-run'),Path('/usr/sbin/nft')):
            info=path.stat()
            if info.st_uid!=0 or info.st_mode&0o022 or not stat.S_ISREG(info.st_mode):raise PermissionError('Trusted launcher code/tool ownership invalid')
        account=pwd.getpwuid(c.trainer_uid)
        if (not account.pw_name.startswith('dl-train-') or account.pw_gid!=c.trainer_gid
                or account.pw_shell not in ('/usr/sbin/nologin','/sbin/nologin','/bin/false')
                or set(os.getgrouplist(account.pw_name,c.trainer_gid))!={c.trainer_gid}):
            raise PermissionError('Dedicated training account must have no administrative/supplementary groups')
        if not Path('/sys/fs/cgroup/cgroup.controllers').is_file():raise PermissionError('Unified cgroup v2 required')
        low,high=map(int,Path('/proc/sys/net/ipv4/ip_local_port_range').read_text().split())
        if not 20000<=low<high<=65535:raise PermissionError('DDP ephemeral range differs from reviewed firewall range')
        value=json.loads(self.command(['/usr/sbin/nft','-j','list','table','inet',c.nft_table]).stdout)
        if policy_digest(value,c)!=c.policy_sha256:raise PermissionError('Enforced firewall differs from reviewed receipt')
        return True

    def _path(self,identity):return self.config.state_root/(identity.key+'.json')

    def _record(self,identity,record=None):
        held=protected_json(self._path(identity))
        if held['identity']!=identity.document() or held['unit']!=unit_name(identity):raise PermissionError('Boundary identity differs')
        if record is not None:
            from execution_lease import fence
            if fence(held['lease'])!=fence(record['lease']):raise PermissionError('Boundary fence differs')
        return held

    def isolate(self,command,workspace,data):
        c=self.config
        if not isinstance(workspace,WorkspaceReceipt) or type(command) is not list or any(type(x) is not str for x in command):
            raise ValueError('Verified workspace and argv required')
        # Identity comes from the already authenticated start, never an arbitrary path.
        identity=AttemptIdentity(data['job_id'],data['attempt_token'],(data.get('ddp') or {}).get('rank',0))
        if identity.key!=workspace.identity_key:raise PermissionError('Workspace attempt differs')
        expected=c.code_root/'agent/custom_runner.py'
        if len(command)<2 or Path(command[1])!=expected or Path(command[0]).resolve()!=Path('/usr/bin/python3').resolve():
            raise PermissionError('Only the trusted system-Python custom launcher is supported')
        # The service venv is deliberately absent from the trainer namespace.
        # custom_runner uses stdlib + approved job env; select the same verified
        # system interpreter explicitly rather than exposing service dependencies.
        command=['/usr/bin/python3',*command[1:]]
        ddp=data.get('ddp')
        if ddp is not None and (ddp['master_addr'] not in c.peers or not 20000<=ddp['master_port']<=49999
                               or ddp['world_size']!=len(c.peers)):
            raise PermissionError('DDP differs from reviewed physical peer group')
        allocation=data['resources']
        if allocation['gpus']!=0:raise ValueError('Reviewed pilot is CPU-only')
        directory=c.worker_root/'worker-storage/jobs'/identity.job_id
        request=protected_json(directory/'request.json');prepared=protected_json(directory/'prepared.json')
        env=c.worker_root/'worker-storage/environments'/prepared['environment_key']
        if prepared['python_path']!=str(env/'bin/python') or request['job_id']!=identity.job_id:
            raise PermissionError('Uncontrolled prepared environment')
        self.preflight()
        active=c.state_root/'active.json'
        if active.exists():
            previous=protected_json(active);old=AttemptIdentity(**previous['identity'])
            if old!=identity and not self.quiescent(old,previous):raise PermissionError('Previous cgroup is not quiescent')
        record=dict(version=1,identity=identity.document(),unit=unit_name(identity),lease=data['lease'],
            config=c.document(),command=command,workspace=str(workspace.root),environment=str(env),
            resources=allocation,peers=list(c.peers) if ddp else [],phase='PREPARED',boot_id=Path('/proc/sys/kernel/random/boot_id').read_text().strip())
        self._record_budget()
        atomic_json(self._path(identity),record,new=True)
        atomic_json(active,record)
        return ['/usr/bin/systemd-run','--quiet','--wait','--pipe','--service-type=notify',
            '--unit='+unit_name(identity),
            '--property=BindsTo='+c.agent_unit,'--property=After='+c.agent_unit,
            '--property=WatchdogSec=3s','--property=WatchdogSignal=SIGKILL','--property=NotifyAccess=main',
            '--property=TimeoutStartSec=120s','--property=TimeoutStopSec=1s','--property=KillMode=control-group',
            '--property=NoNewPrivileges=yes','--property=UMask=0077','--property=TasksMax=256',
            '--property=MemoryMax='+str(int(allocation['memory_gb']*1024**3)),
            '--property=CPUQuota='+str(allocation['cpu_cores']*100)+'%',
            # Guardian output mounts must be visible to the trusted agent for
            # checkpoint sync/collection. Do not create a service mount namespace
            # here: the unprivileged child has its own curated bwrap namespace,
            # private /dev, no /sys, no capabilities and no cgroup filesystem.
            '/usr/bin/python3','-I','-B',str(c.code_root/'tools/hybrid_guardian.py'),str(self._path(identity))]

    def _record_budget(self):
        records=[]
        for path in self.config.state_root.glob('*.json'):
            if re.fullmatch('[0-9a-f]{64}[.]json',path.name):
                held=protected_json(path)
                identity=AttemptIdentity(**held['identity'])
                if held.get('phase')!='ARCHIVED' and self.quiescent(identity,held):
                    self.archive(identity,held)
                    held=protected_json(path)
                if held.get('phase')!='ARCHIVED':records.append(held)
        if len(records)>=self.config.max_retained_attempts:raise OSError('Uncollected private attempts exceed retention budget')

    def quiescent(self,identity,record):
        try:
            held=self._record(identity,record)
            if held['boot_id']!=Path('/proc/sys/kernel/random/boot_id').read_text().strip():return True
            status=self.command(['/usr/bin/systemctl','show',held['unit'],'--property=LoadState,ActiveState,ControlGroup']).stdout
            values=dict(line.split('=',1) for line in status.splitlines())
            if values['LoadState']=='not-found':
                if held.get('phase') in ('FINISHED','ARCHIVED'):
                    from execution_lease import process_key
                    if held.get('guardian_pid') and process_key(held['guardian_pid'])==held.get('guardian_key'):return False
                    group=Path('/sys/fs/cgroup/system.slice')/held['unit']
                    if group.exists() and any(p.read_text().strip() for p in group.rglob('cgroup.procs')):return False
                    return True
                from execution_lease import alive,read,lease_path
                try:return not alive(read(lease_path(self.config.worker_root,held['lease'])))
                except FileNotFoundError:return True
            if values['ActiveState'] not in ('inactive','failed'):return False
            group=values.get('ControlGroup','')
            if group:
                if '..' in Path(group).parts or not group.startswith('/system.slice/'+held['unit']):return False
                base=Path('/sys/fs/cgroup')/group.lstrip('/')
                for child in [base,*base.rglob('*')]:
                    procs=child/'cgroup.procs'
                    if procs.is_file() and procs.read_text().strip():return False
            return True
        except (OSError,ValueError,KeyError,subprocess.SubprocessError):return False

    def terminate(self,identity,record):
        held=self._record(identity,record)
        if self.quiescent(identity,record):return True
        self.command(['/usr/bin/systemctl','stop',held['unit']])
        return self.quiescent(identity,record)

    def archive(self,identity,record):
        """Preserve candidates before unmounting only our quiescent bounded tmpfs.

        Archives remain private LOCAL forensic data. They are never extracted or
        served to training, and authoritative checkpoints/inputs are untouched.
        """
        import tarfile
        from .nfs_mount import parse_mountinfo
        held=self._record(identity,record)
        if held.get('phase')=='ARCHIVED':return True
        if not self.quiescent(identity,held):raise PermissionError('Cannot archive a live or uncertain cgroup')
        mounts=parse_mountinfo(Path('/proc/self/mountinfo').read_text())
        job=self.config.worker_root/'worker-storage/jobs'/identity.job_id
        owned=[];logical=0
        for name,limit in [('results',self.config.max_output_bytes),('checkpoints',self.config.max_checkpoint_bytes)]:
            path=job/name;matches=[m for m in mounts if m['target']==str(path)]
            if not matches:continue
            if len(matches)!=1 or matches[0]['fstype']!='tmpfs' or matches[0]['source']!='dl-hybrid-'+identity.key:
                raise PermissionError('Will not unmount an unrelated filesystem')
            count=0;size=0
            for current,dirs,files in os.walk(path,followlinks=False):
                count+=len(dirs)+len(files)
                if count>4096:raise PermissionError('Forensic member bound exceeded')
                for item in files:
                    info=(Path(current)/item).lstat()
                    if stat.S_ISREG(info.st_mode):size+=info.st_size
                if size>limit:raise PermissionError('Forensic logical byte budget exceeded')
            logical+=size;owned.append((name,path))
        root=self.config.state_root/identity.key;root.mkdir(mode=0o700,exist_ok=True)
        with _directory(root) as fd:_protected(fd,directory=True)
        free=os.statvfs(root)
        if free.f_bavail*free.f_frsize<logical+self.config.reserve_bytes:raise OSError('Preserving old candidates requires more LOCAL free space')
        archive=root/'forensic.tar'
        if owned and not archive.exists():
            temporary=root/'forensic.part'
            with temporary.open('xb') as out:
                with tarfile.open(fileobj=out,mode='w',dereference=False) as tar:
                    for name,path in owned:tar.add(path,arcname=name,recursive=True)
                out.flush();os.fsync(out.fileno())
            os.replace(temporary,archive)
        if archive.exists():
            digest=hashlib.sha256()
            with archive.open('rb') as stream:
                for chunk in iter(lambda:stream.read(1024**2),b''):digest.update(chunk)
            held['archive_sha256']=digest.hexdigest()
        for _,path in owned:self.command(['/usr/bin/umount','--',str(path)])
        held['phase']='ARCHIVED';atomic_json(self._path(identity),held)
        return True
