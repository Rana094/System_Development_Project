"""Reproducible local training smoke runs; no scheduler/agent/DB or remote actions."""
import csv
from dataclasses import dataclass
from datetime import datetime,timezone
import hashlib
import importlib.metadata
import json
import math
import os
from pathlib import Path
import platform
import re
import signal
import stat
import subprocess
import sys
import time
import uuid

from .datasets import KINDS,generate,digest

ROOT=Path(__file__).resolve().parents[3]
sys.path.insert(0,str(ROOT))
from hybrid_storage.backends import _directory
from hybrid_storage.worker_cache import _protected
from hybrid_storage.mount_identity import require_local_fd
from hybrid_storage.nfs_mount import parse_mountinfo

PROJECTS={'regression':'checkpoint_project','cnn':'classification_project','ddp':'ddp_project'}
SDK=('dl_checkpoint.py','checkpoint_protocol.py','execution_lease.py')
CSV_FIELDS=('schema_version','experiment_id','trial_id','host','workload','workload_sha256','input_binding_sha256','dataset_id',
 'dataset_version','dataset_sha256','dataset_bytes','dataset_rows','world_size','status','exit_code',
 'started_at_ns','finished_at_ns','elapsed_ns','training_loop_seconds','scheduler_mode','cache_state','scope')


def canonical(value):return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
def value_hash(value):return hashlib.sha256(canonical(value)).hexdigest()


def write_json(path,value):
    with new_file(path) as stream:json.dump(value,stream,indent=2,allow_nan=False);stream.write('\n');stream.flush();os.fsync(stream.fileno())


def new_file(path):
    fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
    return os.fdopen(fd,'w')


@dataclass(frozen=True)
class Settings:
    experiment_id:str
    rows:tuple=(64,1024)
    workloads:tuple=KINDS
    world_size:int=3
    steps:int=2
    epochs:int=1
    seed:int=123
    timeout_seconds:int=120
    max_dataset_bytes:int=64*1024*1024

    def validate(self):
        if not isinstance(self.experiment_id,str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,79}',self.experiment_id):raise ValueError('Invalid experiment ID')
        if not self.rows or len(self.rows)>8 or len(set(self.rows))!=len(self.rows) or any(type(n) is not int or not 1<=n<=1000000 for n in self.rows):raise ValueError('Use 1..8 unique positive dataset row counts, each <=1000000')
        if not self.workloads or len(set(self.workloads))!=len(self.workloads) or any(n not in KINDS for n in self.workloads):raise ValueError('Invalid workload selection')
        for value,low,high in ((self.world_size,2,3),(self.steps,1,10000),(self.epochs,1,1000),(self.seed,0,2**32-1),(self.timeout_seconds,1,3600),(self.max_dataset_bytes,1,1024**3)):
            if type(value) is not int or not low<=value<=high:raise ValueError('Invalid experiment bounds')


def source_files():
    result=[f'examples/{project}/{name}' for project in PROJECTS.values() for name in ('train.py','requirements.txt')]
    return [*result,*(f'agent/{name}' for name in SDK),'tools/benchmarks/storage_evaluation.py',
            *(f'tools/benchmarks/evaluation/{name}' for name in ('__init__.py','datasets.py','runner.py','rank_launcher.py'))]


def source_hashes():
    result={}
    for name in source_files():
        result[name]=hashlib.sha256(read_source(name)).hexdigest()
    return result


def read_source(name):
    path=ROOT/name
    with _directory(path.parent) as parent:
        fd=os.open(path.name,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK,dir_fd=parent)
        try:
            info=os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_size>1024*1024:raise ValueError('Unsafe or oversized evaluation source')
            with os.fdopen(fd,'rb',closefd=False) as stream:data=stream.read(1024*1024+1)
            if len(data)>1024*1024:raise ValueError('Source exceeds snapshot bound')
            return data
        finally:os.close(fd)


def local_observations():
    def version(name):
        try:return importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:return None
    try:
        mounts=parse_mountinfo(Path('/proc/self/mountinfo').read_text())
        nfs=[m for m in mounts if m['fstype'] in ('nfs','nfs4')];mount_status='observed'
    except (OSError,ValueError):nfs=None;mount_status='unavailable'
    try:memory=os.sysconf('SC_AVPHYS_PAGES')*os.sysconf('SC_PAGE_SIZE')
    except (ValueError,OSError):memory=None
    try:cpus=len(os.sched_getaffinity(0))
    except (AttributeError,OSError):cpus=os.cpu_count()
    return dict(hostname=platform.node(),platform=platform.platform(),machine=platform.machine(),
        python=platform.python_version(),python_executable=sys.executable,torch=version('torch'),numpy=version('numpy'),
        cpu_affinity_count=cpus,available_physical_bytes=memory,
        nfs_mounts=nfs,nfs_mount_observation=mount_status,nfs_io_probed=False,
        page_cache_state='uncontrolled; synthetic files generated and hashed before training; no cache dropping')


def plan(settings):
    settings.validate()
    config=json.loads((ROOT/'config/storage.json').read_text());nodes=json.loads((ROOT/'config/nodes.json').read_text())['nodes']
    def git(args):
        try:return subprocess.check_output(['git',*args],cwd=ROOT,text=True,stderr=subprocess.DEVNULL).strip()
        except (OSError,subprocess.CalledProcessError):return None
    return dict(schema_version=1,experiment_id=settings.experiment_id,scope='controller_local_smoke',
        requested=dict(rows=list(settings.rows),workloads=list(settings.workloads),world_size=settings.world_size,
            steps=settings.steps,epochs=settings.epochs,seed=settings.seed,timeout_seconds=settings.timeout_seconds,
            max_dataset_bytes=settings.max_dataset_bytes),
        scheduler_mode='not_exercised',configured_storage=config,
        application_cache=dict(state='not_used',persistent_reuse=False),
        workers=[dict(worker_id=n['id'],configured_host=n['ip'],pi_host_observed=None,eligibility=None,
            reason='not_probed; local runner never contacts workers') for n in nodes],
        local_host=local_observations(),sources=source_hashes(),git=dict(head=git(['rev-parse','HEAD']),status=git(['status','--porcelain'])),
        measurements_missing=['time_to_first_step','nfs_wire_bytes','cache_hit_ratio','staging_time',
            'checkpoint_save_restore_time','recovery_overhead','server_bandwidth','pi_eligibility','scheduler_choice'],
        comparison_config=None,notes=['Not configurations A/B/C/D/E; no performance comparison or physical Pi result.',
            'No distributed scheduler admission, authenticated agent, production lease or sandbox in local smoke.',
            'Existing regression/CNN SDK archives are local output only, not controller COMMITTED checkpoints.'])


def make_root(output_root,experiment_id):
    path=Path(output_root)
    if not path.is_absolute() or path==Path('/') or '..' in path.parts:raise ValueError('Expected canonical absolute report root')
    # No-follow creation of missing ancestors, without chmod of existing dirs.
    current=Path(path.anchor)
    for part in path.parts[1:]:
        with _directory(current) as parent:
            require_local_fd(parent)
            try:os.mkdir(part,0o700,dir_fd=parent)
            except FileExistsError:pass
        current=current/part
    with _directory(path) as parent:
        os.mkdir(experiment_id,0o700,dir_fd=parent)  # Never resume/overwrite an ID.
    result=path/experiment_id
    with _directory(result) as descriptor:_protected(descriptor,directory=True)
    return result


def snapshot(folder,hashes):
    for name,expected in hashes.items():
        target=folder/'source'/name;target.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
        data=read_source(name)
        with new_file(target) as output:
            output.buffer.write(data)
            output.flush();os.fsync(output.fileno())
        if digest(target)!=expected:raise ValueError('Source changed during snapshot')
        target.chmod(0o444)


def trainer_arguments(workload,settings):
    if workload=='ddp':return ['--epochs',str(settings.epochs),'--batch-delay','0','--log-every','1','--global-batch-size','96']
    return ([ '--task','cnn'] if workload=='cnn' else [])+['--steps',str(settings.steps),'--batch-delay','0']


def child_environment(folder,trial,binding):
    # A local benchmark cannot inherit a production lease/resume/controller key.
    allowed=('PATH','HOME','LANG','LC_ALL','TZ','TMPDIR','LD_LIBRARY_PATH')
    env={key:os.environ[key] for key in allowed if key in os.environ}
    env.update(PYTHONDONTWRITEBYTECODE='1',PYTHONHASHSEED='123',OMP_NUM_THREADS='1',MKL_NUM_THREADS='1',
        CUDA_VISIBLE_DEVICES='',GLOO_SOCKET_IFNAME='lo',EVAL_TRIAL_ROOT=str(trial),EVAL_SDK_ROOT=str(folder/'source/agent'),
        DL_JOB_ID=trial.name,DL_ATTEMPT_TOKEN=trial.name+':0',DL_MANIFEST_SHA256=binding,
        DL_COMPATIBILITY_KEY='local-evaluation-only',DL_CHECKPOINT_INTERVAL_SECONDS='3600',DL_RECOVERY_MODE='restart')
    return env


def stop(process):
    if process.poll() is None:
        os.killpg(process.pid,signal.SIGTERM)
        try:process.wait(timeout=5)
        except subprocess.TimeoutExpired:os.killpg(process.pid,signal.SIGKILL);process.wait(timeout=5)


def execute(command,trial,env,timeout):
    process=None
    with new_file(trial/'launcher.log') as log:
        try:
            process=subprocess.Popen(command,cwd=trial,env=env,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
            try:return process.wait(timeout=timeout),'EXITED'
            except subprocess.TimeoutExpired:return None,'TIMEOUT'
        finally:
            if process is not None:stop(process)


def collect(trial,world):
    records=[]
    for rank in range(world):
        base=trial/'ranks'/f'rank-{rank}';record=dict(rank=rank,runtime=None,summary=None)
        for key,name in (('runtime','runtime.json'),('summary','output/summary.json')):
            path=base/name
            if path.is_file():
                if path.stat().st_size>32*1024*1024:raise ValueError('Oversized rank result')
                value=json.loads(path.read_text(),parse_constant=lambda value:(_ for _ in ()).throw(ValueError('Nonfinite result')))
                if type(value) is not dict:raise ValueError('Invalid rank result')
                record[key]=value
        records.append(record)
    return records


def successful(records,world):
    if len(records)!=world:return False
    for row in records:
        runtime,summary=row['runtime'],row['summary']
        if not runtime or runtime.get('status')!='COMPLETED' or not summary or type(summary.get('training_steps')) is not int or summary['training_steps']<1:return False
        if world>1 and (summary.get('rank')!=row['rank'] or summary.get('world_size')!=world or summary.get('parameters_synchronized') is not True):return False
    hashes=[row['summary'].get('final_model_sha256') for row in records]
    return world==1 or all(type(h) is str and re.fullmatch('[0-9a-f]{64}',h) for h in hashes) and len(set(hashes))==1


def run(settings,output_root):
    document=plan(settings);folder=make_root(output_root,settings.experiment_id)
    document.update(started_at_ns=time.time_ns(),status='RUNNING',output_root=str(folder))
    write_json(folder/'experiment.json',document)
    results=[]
    try:
        snapshot(folder,document['sources']);datasets=folder/'datasets';datasets.mkdir(mode=0o700)
        with new_file(folder/'trials.jsonl') as raw,new_file(folder/'trials.csv') as flat:
            writer=csv.DictWriter(flat,fieldnames=CSV_FIELDS);writer.writeheader()
            for workload in settings.workloads:
                for rows in settings.rows:
                    dataset=generate(datasets,workload,rows,settings.seed,max_bytes=settings.max_dataset_bytes)
                    write_json(datasets/(dataset['path']+'.json'),dataset)
                    index=len(results)+1;trial=folder/f'trial-{index:04}';trial.mkdir(mode=0o700)
                    world=settings.world_size if workload=='ddp' else 1
                    project=PROJECTS[workload];argv=trainer_arguments(workload,settings)
                    identity=dict(workload=workload,argv=argv,world_size=world,threads_per_rank=1,
                        sources={k:v for k,v in document['sources'].items() if k.startswith('agent/') or k.startswith(f'examples/{project}/') or k.startswith('tools/')})
                    workload_hash=value_hash(identity);binding=value_hash(dict(workload_sha256=workload_hash,dataset=dataset))
                    command=[sys.executable]
                    if world>1:command+=['-m','torch.distributed.run','--rdzv-backend=c10d','--rdzv-endpoint=127.0.0.1:0',
                        '--nnodes=1',f'--nproc-per-node={world}','--max-restarts=0','--monitor-interval=0.1']
                    command+=[str(folder/'source/tools/benchmarks/evaluation/rank_launcher.py'),
                              str(folder/f'source/examples/{project}/train.py'),*argv]
                    row=dict(schema_version=1,experiment_id=settings.experiment_id,trial_id=trial.name,
                        host=document['local_host']['hostname'],workload=workload,
                        workload_sha256=workload_hash,input_binding_sha256=binding,dataset_id=dataset['dataset_id'],
                        dataset_version=dataset['version'],dataset_sha256=dataset['sha256'],dataset_bytes=dataset['size_bytes'],
                        dataset_rows=rows,world_size=world,status='FAILED',exit_code=None,started_at_ns=time.time_ns(),
                        finished_at_ns=None,elapsed_ns=None,training_loop_seconds=None,scheduler_mode='not_exercised',
                        cache_state='not_used',scope='controller_local_smoke')
                    write_json(trial/'invocation.json',dict(row,command=command,workload_identity=identity))
                    env=child_environment(folder,trial,binding);env['DL_DATASET_PATH']=str(datasets/dataset['path'])
                    started=time.monotonic_ns();record=dict(row,command=command,ranks=[],error=None)
                    try:
                        if digest(datasets/dataset['path'])!=dataset['sha256']:raise ValueError('Dataset changed before training')
                        code,state=execute(command,trial,env,settings.timeout_seconds)
                        record.update(exit_code=code,status='TIMEOUT' if state=='TIMEOUT' else 'FAILED')
                        record['ranks']=collect(trial,world)
                        if code==0 and successful(record['ranks'],world):record['status']='COMPLETED'
                        if digest(datasets/dataset['path'])!=dataset['sha256']:
                            record['status']='FAILED';raise ValueError('Dataset changed during training')
                        times=[r['summary'].get('timing',{}).get('training_loop_seconds') for r in record['ranks'] if r['summary']]
                        if len(times)==world and all(type(t) in (int,float) and math.isfinite(t) and t>=0 for t in times):record['training_loop_seconds']=max(times)
                    except (OSError,ValueError) as error:record['error']=type(error).__name__
                    finally:
                        record.update(finished_at_ns=time.time_ns(),elapsed_ns=time.monotonic_ns()-started)
                        write_json(trial/'result.json',record);raw.write(json.dumps(record,allow_nan=False)+'\n')
                        writer.writerow({key:record[key] for key in CSV_FIELDS});raw.flush();flat.flush();os.fsync(raw.fileno());os.fsync(flat.fileno())
                        results.append(record)
                    # Each run remains auditable even when a trainer fails.
        document['status']='COMPLETED' if all(r['status']=='COMPLETED' for r in results) else 'FAILED'
    except BaseException as error:
        document['status']='INTERRUPTED' if isinstance(error,KeyboardInterrupt) else 'FAILED'
        document['error']=type(error).__name__
        raise
    finally:
        document.update(finished_at_ns=time.time_ns(),trial_count=len(results),completed_trials=sum(r['status']=='COMPLETED' for r in results))
        write_json(folder/'completion.json',document)
    return folder,document


def new_id():return 'eval-'+datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-'+uuid.uuid4().hex[:8]
