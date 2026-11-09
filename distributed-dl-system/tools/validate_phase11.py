"""Plan, or explicitly run approved two-Pi Phase 11 public-fixture acceptance.

Default prints a plan. --run uploads source into NEW private /var/tmp trees,
starts the existing RO input readiness unit, and runs only the trusted probe.
No production agent replacement, active config change or untrusted trainer.
"""
import argparse
import base64
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import secrets
import selectors
import shlex
import sqlite3
import subprocess
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))

SSH=['ssh','-T','-o','BatchMode=yes','-o','ConnectTimeout=5','-o','StrictHostKeyChecking=yes',
     '-o','UpdateHostKeys=no','-o','ControlMaster=no','-o','ControlPath=none','-o','PermitLocalCommand=no']
UPLOAD=r'''import hashlib,io,json,os,sys,tarfile
from pathlib import Path,PurePosixPath
meta=json.loads(sys.stdin.buffer.readline(1024*1024))
root=Path(meta['root'])
assert root.parent==Path('/var/tmp') and root.name.startswith('dl-phase11-') and len(root.name)<100
raw=sys.stdin.buffer.read(128*1024*1024+1)
assert len(raw)<=128*1024*1024 and hashlib.sha256(raw).hexdigest()==meta['archive_sha256']
with tarfile.open(fileobj=io.BytesIO(raw)) as archive:
 members=archive.getmembers();assert len(members)<=2048
 assert len({m.name for m in members})==len(members) and {m.name for m in members}==set(meta['files'])
 root.mkdir(mode=0o700);source=root/'source';source.mkdir(mode=0o700)
 for member in members:
  p=PurePosixPath(member.name)
  assert not p.is_absolute() and '..' not in p.parts and member.isfile() and 0<=member.size<=8*1024*1024
  data=archive.extractfile(member).read(8*1024*1024+1)
  assert dict(bytes=len(data),sha256=hashlib.sha256(data).hexdigest())==meta['files'][member.name]
  target=source/member.name;target.parent.mkdir(parents=True,mode=0o700,exist_ok=True)
  with target.open('xb') as stream:stream.write(data)
print(json.dumps({'root':str(root),'members':len(members),'archive_sha256':meta['archive_sha256']}))
'''


class Session:
    def __init__(self,node,root,credential,manifest,source,out):
        program="import sys;sys.path.insert(0,"+repr(root+'/source')+");from hybrid_storage.phase11_probe import main;main()"
        self.log=(out/(node+'-probe.stderr.log')).open('w')
        self.process=subprocess.Popen([*SSH,node,shlex.join(['sudo','-n','/usr/bin/python3','-B','-c',program])],
            stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=self.log,text=True,bufsize=1)
        self.send(dict(state=root+'/state',worker=node,mount='/srv/mlnfs/inputs',source=source,
                       manifest=manifest,key_id=credential.key_id,secret_hex=credential.secret.hex()))

    def send(self,value):
        self.process.stdin.write(json.dumps(value)+'\n');self.process.stdin.flush()

    def read(self,timeout=120):
        with selectors.DefaultSelector() as selector:
            selector.register(self.process.stdout,selectors.EVENT_READ)
            if not selector.select(timeout):raise TimeoutError('Owned probe response deadline')
        raw=self.process.stdout.readline(1024*1024+1)
        if not raw:raise RuntimeError('Owned probe stopped; inspect private stderr log')
        if len(raw)>1024*1024:raise ValueError('Probe response exceeded bound')
        return json.loads(raw)

    def call(self,value):
        self.send(value);return self.read()

    def close(self):
        if self.process.poll() is None:
            try:self.send(dict(action='close'));self.process.stdin.close();self.process.wait(timeout=10)
            except (OSError,subprocess.TimeoutExpired):
                if self.process.poll() is None:self.process.terminate()
                self.process.wait(timeout=10)
        self.log.close()


def run(nodes,publication,output):
    from hybrid_storage import registry_schema,cache_registry_schema
    from hybrid_storage.artifacts import ArtifactRegistrar
    from hybrid_storage.registry import StorageRegistry
    from hybrid_storage.cache_inventory import CacheInventory
    from hybrid_storage.cache_coverage import CoverageAPI
    from hybrid_storage.dataset_manifests import DatasetEntry
    from hybrid_storage.inventory_protocol import Credential
    from hybrid_storage.bandwidth import TransferObservations
    from hybrid_storage.phase11_probe import fixture_files
    from hybrid_storage.deployment_review import build
    from hybrid_storage.nfs_mount import validate_source
    from hybrid_storage.inputs import object_key

    output=Path(output)
    if not output.is_absolute():raise ValueError('New absolute local output required')
    document=json.loads(Path(publication).read_text())
    source=validate_source(document['source'])
    manifest={'files':[{k:row[k] for k in ('path','sha256','size_bytes')} for row in document['files']]}
    files=fixture_files(manifest)
    output.mkdir(mode=0o700)
    bundle=build(nodes,output/'source-review')
    archive=(output/'source-review/worker-source.tar.gz').read_bytes()
    run_id='dl-phase11-'+secrets.token_hex(12);remote='/var/tmp/'+run_id
    results={'selected_workers':nodes,'pi01':'deferred','remote_root':remote,'source':source,
             'archive':bundle['archives']['worker'],'nodes':{},'production_agent_replacement':False,
             'active_storage_config_changed':False,'existing_ro_mount_activation_requested':True}
    (output/'plan.json').write_text(json.dumps(results,indent=2)+'\n')

    def prepare(node):
        before=subprocess.run([*SSH,node,'systemctl show dl-node-agent.service --no-pager -p MainPID -p ActiveState'],
                              capture_output=True,text=True,timeout=10)
        if before.returncode:raise RuntimeError('Agent preflight failed')
        meta=dict(root=remote,archive_sha256=hashlib.sha256(archive).hexdigest(),files=bundle['files']['worker'])
        p=subprocess.run([*SSH,node,shlex.join(['sudo','-n','/usr/bin/python3','-B','-c',UPLOAD])],
            input=json.dumps(meta).encode()+b'\n'+archive,capture_output=True,timeout=30)
        if p.returncode:raise RuntimeError('Source-only private upload failed: '+p.stderr.decode()[:2048])
        ready=subprocess.run([*SSH,node,'sudo -n systemctl start ml-nfs-ready.service'],
                              capture_output=True,text=True,timeout=25)
        if ready.returncode:raise RuntimeError('Existing RO NFS admission failed: '+ready.stderr[:1024])
        return dict(agent_before=before.stdout,upload=json.loads(p.stdout),nfs_ready_returncode=ready.returncode)

    with ThreadPoolExecutor(max_workers=len(nodes)) as pool:
        prepared=list(pool.map(prepare,nodes))
    for node,details in zip(nodes,prepared):results['nodes'][node]=details

    # Disposable LOCAL controller registry; no job_store import or live DB.
    central=output/'controller-fixture';central.mkdir(mode=0o700)
    database=central/'registry.db'
    def connect():
        db=sqlite3.connect(database,timeout=10);db.execute('PRAGMA foreign_keys=ON');db.execute('PRAGMA synchronous=FULL');return db
    db=connect();db.execute('CREATE TABLE jobs(job_id TEXT PRIMARY KEY)');db.commit()
    registry_schema.migrate(db);cache_registry_schema.migrate(db)
    for name in ('objects','scratch'):(central/name).mkdir(mode=0o700)
    registry=StorageRegistry(db,ArtifactRegistrar(central/'objects',staging_root=central/'scratch'))
    for f in files:
        path=Path('/srv/ml-shared/objects')/object_key(f.sha256)
        with path.open('rb') as stream:registry.register_artifact('phase11',stream,expected_size=f.size_bytes)
    dataset=registry.create_dataset('phase11',[DatasetEntry(f.path,'file',f.sha256,f.size_bytes) for f in files])
    credentials={node:Credential(node,secrets.token_hex(16),secrets.token_bytes(32)) for node in nodes}
    owner=CacheInventory(connect,list(credentials.values()),configured_workers=nodes,state_root=central,
                         transfer_observations=TransferObservations())
    owner.start();api=CoverageAPI(registry,owner);sessions={}
    def poll(node):
        raw,headers=owner.challenge(node)
        reply=sessions[node].call(dict(action='inventory',request=base64.b64encode(raw).decode(),
                                      mac=headers['X-DL-Inventory-MAC'],key_id=headers['X-DL-Worker-Key']))
        body=base64.b64decode(reply['body']);kind=owner.accept(node,raw,body,reply['mac'])
        return kind
    def coverage(node):
        return api.for_dataset('phase11',node,dataset.dataset_id,dataset.version).document()
    try:
        for node in nodes:
            owner.enroll(node);sessions[node]=Session(node,remote,credentials[node],manifest,source,output)
        for node in nodes:
            ready=sessions[node].read();assert ready['ready']['isolation']['tested']
            results['nodes'][node]['probe']=ready['ready']
        for node in nodes:poll(node)
        for node in nodes:
            row=coverage(node);assert row['missing_bytes']==0 and row['inventory_eligible']
            results['nodes'][node]['initial_coverage']=row
            context=owner._contexts[node]
            facts=owner.bandwidth_facts(node,context['session_id'],context['sequence'])
            assert facts['transfer']['sample_count']>=3
            results['nodes'][node]['authenticated_bandwidth_facts']=facts
        for node in nodes:
            results['nodes'][node]['eviction_crash']=sessions[node].call(dict(action='evict-crash'))
            poll(node);row=coverage(node);assert row['missing_bytes']>0
            results['nodes'][node]['after_eviction_coverage']=row
            restaged=sessions[node].call(dict(action='restage'));assert restaged['staged_bytes']==row['missing_bytes']
            results['nodes'][node]['restage']=restaged;poll(node);assert coverage(node)['missing_bytes']==0
            owner.offline(node);assert not coverage(node)['inventory_eligible']
            poll(node);assert coverage(node)['missing_bytes']==0
            results['nodes'][node]['offline_reconnect_verified']=True
        for node in nodes:
            after=subprocess.run([*SSH,node,'systemctl show dl-node-agent.service --no-pager -p MainPID -p ActiveState'],
                                 capture_output=True,text=True,timeout=10)
            assert after.returncode==0 and after.stdout==results['nodes'][node]['agent_before']
            results['nodes'][node]['agent_after']=after.stdout
        results['status']='PASS'
    finally:
        for session in sessions.values():session.close()
        owner.close();db.close()
        (output/'results.json').write_text(json.dumps(results,indent=2,sort_keys=True)+'\n')
    print(json.dumps({'status':results['status'],'nodes':nodes,'evidence':str(output/'results.json')}))


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--nodes',nargs='+',choices=('pi02','pi03'),required=True)
    parser.add_argument('--publication',type=Path)
    parser.add_argument('--output',type=Path)
    parser.add_argument('--run',action='store_true')
    args=parser.parse_args()
    if len(args.nodes)!=len(set(args.nodes)):parser.error('Duplicate workers')
    if not args.run:
        print(json.dumps(dict(mode='plan_only',nodes=args.nodes,pi01='deferred',
            writes='new local and remote private fixture trees only',
            activation='existing RO NFS readiness unit only',production_agent_replacement=False)))
    else:
        if args.publication is None or args.output is None:parser.error('--run requires publication and new output')
        run(args.nodes,args.publication,args.output)


if __name__=='__main__':main()
