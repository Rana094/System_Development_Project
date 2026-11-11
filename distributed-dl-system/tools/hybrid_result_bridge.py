"""Fixed sudoers target: root reads operator config, then drops to SSH operator.

Only the reviewed two-worker pilot root is readable. Never accept a path, command
or SSH option from the caller. Network channel is existing SSH/rsync with host-key
checking. stdout is a bounded LOCAL archive for the controller service.
"""
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tarfile
import tempfile
sys.dont_write_bytecode=True
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))

def main():
    from hybrid_storage.linux_boundary import protected_json
    if os.geteuid()!=0 or len(sys.argv)!=4:raise PermissionError('Fixed privileged bridge invocation required')
    worker,job,lease=sys.argv[1:]
    if worker not in ('pi02','pi03') or not re.fullmatch('[A-Za-z0-9_-]{1,128}',job) or not re.fullmatch('[a-f0-9]{32}',lease):raise ValueError('Invalid result identity')
    cfg=protected_json(Path('/etc/dl-hybrid-pilot/result-bridge.json'))
    if cfg['code_root']!=str(Path(__file__).resolve().parents[1]):raise PermissionError('Bridge source differs')
    node=cfg['nodes'][worker]
    from hybrid_storage.runtime_bootstrap import _private_address
    _private_address(node['ip'])
    root=Path(node['worker_root'])
    if not root.is_absolute() or not str(root).startswith('/var/lib/dl-hybrid-pilot/'):raise PermissionError('Uncontrolled remote root')
    os.setgroups([]);os.setgid(cfg['operator_gid']);os.setuid(cfg['operator_uid']);os.umask(0o077)
    os.environ.clear();os.environ.update(PATH='/usr/bin:/bin',HOME=cfg['operator_home'],LANG='C.UTF-8')
    with tempfile.TemporaryDirectory(prefix='dl-hybrid-results-') as tmp:
        tmp=Path(tmp)
        shell=f"ssh -o BatchMode=yes -o ConnectTimeout=5 -o StrictHostKeyChecking=yes -o HostName={node['ip']}"
        remote=str(root/'worker-storage')
        sources=[f'{worker}:{remote}/jobs/{job}/results',f'{worker}:{remote}/leases/{lease}.json',f'{worker}:{root}/logs/{job}.log']
        # Bound remote local read with root rsync. Sender never executes user code.
        subprocess.run(['rsync','-rt','--no-links','--no-specials','--no-devices','--protect-args','--max-size=64m',
            '--rsync-path=sudo -n /usr/bin/rsync','-e',shell,*sources,str(tmp)+'/'],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=110)
        files=[];total=0
        for name in ('results',lease+'.json',job+'.log'):
            source=tmp/name
            items=sorted(source.rglob('*')) if source.is_dir() else [source]
            for path in items:
                info=path.lstat()
                import stat
                if stat.S_ISDIR(info.st_mode):continue
                if not stat.S_ISREG(info.st_mode) or info.st_nlink!=1:raise ValueError('Unsafe private result')
                relative=('results/'+path.relative_to(source).as_posix() if name=='results' else 'lease.json' if name.endswith('.json') else 'training.log')
                files.append((relative,path));total+=info.st_size
                if len(files)>8192 or total>128*1024**2:raise ValueError('Private result limit exceeded')
        with tarfile.open(fileobj=sys.stdout.buffer,mode='w|') as archive:
            for name,path in files:archive.add(path,arcname=name,recursive=False)

if __name__=='__main__':main()
