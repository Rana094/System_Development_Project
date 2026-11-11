"""Create a private offline two-worker deployment review. No host mutations."""
from pathlib import Path
import argparse
import hashlib
import json
import os
import secrets
import shutil
import sys
from datetime import datetime,timezone
sys.dont_write_bytecode=True
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from hybrid_storage.pilot_provision import encoded,digest,write,verify_payload,service_unit
from hybrid_storage.linux_boundary import LinuxConfig,nft_policy

def prepare(output,inventory):
    output=output.resolve()
    if output.exists():raise ValueError('Use a new private review directory; existing reviews are immutable')
    output.mkdir(mode=0o700)
    release='pilot-'+datetime.now(timezone.utc).strftime('%Y%m%d')+'-'+secrets.token_hex(6)
    code=Path('/opt/dl-hybrid-pilot')/release/'app';base=Path('/var/lib/dl-hybrid-pilot')/release
    source=[]
    for name in ('agent','controller','hybrid_storage','training','examples','config','tools','tests'):
        for path in (ROOT/name).rglob('*'):
            if (path.is_file() and not path.is_symlink() and '__pycache__' not in path.parts
                    and not any(part.startswith('.') for part in path.relative_to(ROOT).parts)
                    and not path.name.endswith(('.db','.lock','.pyc','.sqlite')) and '.db-' not in path.name):source.append(path)
    source.append(ROOT/'README.md')
    catalog={p.relative_to(ROOT).as_posix():digest(p.read_bytes()) for p in sorted(source)}
    nodes=[dict(id=n,ip=ip,port=5001,worker_root=str(base/'worker')) for n,ip in [('pi02','192.168.10.150'),('pi03','192.168.10.101')]]
    keys={n['id']:dict(worker_id=n['id'],key_id=secrets.token_hex(16),secret_hex=secrets.token_hex(32)) for n in nodes}
    settings=dict(storage_backend='nfs',worker_cache_enabled=True,storage_aware_staging=True,scheduler_policy='locality')
    requirements=sorted({hashlib.sha256(p.read_bytes()).hexdigest() for p in (ROOT/'examples').glob('*/requirements.txt')}|{hashlib.sha256(b'').hexdigest()})
    plans={}
    for host in ('controller','pi02','pi03'):
        role='controller' if host=='controller' else 'worker';inv=inventory[host]
        dest=output/host;dest.mkdir(mode=0o700);payload=dest/'payload';payload.mkdir(mode=0o700)
        for path in source:
            target=payload/path.relative_to(ROOT);target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(path.read_bytes())
        bind='192.168.10.214' if role=='controller' else next(n['ip'] for n in nodes if n['id']==host)
        plan=dict(schema_version=1,host=host,hostname=inv['hostname'],role=role,release=release,system_python=inv['python'],
            selected_workers=['pi02','pi03'],deferred_workers=['pi01'],code_root=str(code),state_base=str(base),bind_ip=bind,
            files=catalog,service_site=inv['service_site'],package_sources=inv['package_sources'],
            documents={},directories=[],nodes=nodes,legacy_root=str(ROOT) if role=='controller' else '/home/pi/distributed-dl-system')
        common=dict(schema_version=1,role=role,state_root=str(base/('controller' if role=='controller' else 'worker')),code_root=str(code),
            bind_ip=bind,port=8081 if role=='controller' else 5001,settings=settings,nodes=nodes if role=='controller' else [n for n in nodes if n['id']==host],
            credentials={},paths={},boundary=None,capsule=None)
        if role=='worker':
            plan.update(training_site=inv['training_site'],numpy_names=inv['numpy_names'],requirements_sha256=requirements)
            plan['directories']=['worker','worker/config','worker/worker-storage','cache','workspaces','spool','boundary','control','control/credentials']
            keypath=base/'control/credentials'/f'{host}.json';common['credentials']={host:str(keypath)}
            common['paths']=dict(worker_id=host,cache=str(base/'cache'),workspaces=str(base/'workspaces'),spool=str(base/'spool'),
                inputs='/srv/mlnfs/inputs',source='192.168.10.214:/srv/ml-shared/objects',cache_bytes=2*1024**3)
            common['boundary']=LinuxConfig(base/'worker',code,base/'boundary',599,599,tuple(n['ip'] for n in nodes),'192.168.10.214','0'*64).document()
            common['capsule']=str(base/'worker/worker-storage/capsule.json')
            plan['documents']={'control/config-worker.json':common,'control/credentials/'+host+'.json':keys[host],
                'worker/config/execution_leases.json':json.loads((ROOT/'config/execution_leases.json').read_bytes())}
            write(dest/'policy.nft',nft_policy(LinuxConfig(**common['boundary'])).encode())
            write(dest/'unit-preview.service',service_unit(plan,'worker',code.parent/'service/bin/python',base/'control/config-worker.json'),0o600)
        else:
            plan.update(operator_name='rana',operator_uid=os.getuid(),operator_gid=os.getgid(),operator_home=str(Path.home()))
            plan['directories']=['controller','controller/portal','controller/jobs','controller/checkpoints','controller/scratch','controller/credentials']
            common['credentials']={worker:str(base/'controller/credentials'/f'{worker}.json') for worker in keys}
            common['paths']=dict(jobs=str(base/'controller/jobs'),objects='/srv/ml-shared/objects',scratch=str(base/'controller/scratch'),
                checkpoints=str(base/'controller/checkpoints'),inventory=str(base/'controller'),nodes=str(base/'controller/nodes.json'),portal=str(base/'controller/portal'))
            plan['documents']={f'controller/credentials/{w}.json':k for w,k in keys.items()}
            plan['documents'].update({'controller/config-controller.json':common,'controller/config-portal.json':{**common,'role':'portal'},'controller/nodes.json':dict(nodes=nodes)})
            auth=ROOT/'storage/portal'
            plan['portal_files']={n:digest((auth/n).read_bytes()) for n in ('users.json','admins.json','session.key') if (auth/n).is_file()}
            if not {'users.json','session.key'}<=set(plan['portal_files']):raise ValueError('Existing portal authentication metadata missing')
            for r in ('controller','portal'):write(dest/('unit-'+r+'-preview.service'),service_unit(plan,r,code.parent/'service/bin/python',base/'controller'/('config-'+r+'.json')),0o600)
        verify_payload(plan,payload);write(dest/'plan.json',plan)
        plans[host]=dict(plan_sha256=digest(encoded(plan)),path=str(dest/'plan.json'))
    review=dict(schema_version=1,release=release,selected_workers=['pi02','pi03'],deferred_workers=['pi01'],
        plans=plans,source_files=len(catalog),source_catalog_sha256=digest(encoded(catalog)),
        policy='private LAN only; RO existing NFS; CPU-only; isolated UID599; locality opt-in; original DB unchanged')
    write(output/'REVIEW.json',review)
    return review

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',required=True,type=Path);parser.add_argument('--inventories',required=True,type=Path)
    args=parser.parse_args();print(json.dumps(prepare(args.output,json.loads(args.inventories.read_bytes())),indent=2))
