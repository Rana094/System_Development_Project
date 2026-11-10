"""Controlled A/B/C/D component trials. Rehearsals never count as NFS evidence."""
from dataclasses import dataclass,replace
import csv
import hashlib
import json
import os
from pathlib import Path
import random
import statistics
import sys
import time

from . import runner
from .datasets import generate,digest
from .matrix_context import Context
from hybrid_storage.inputs import NFSObjectStorageBackend,LocalObjectStorageBackend,object_key
from hybrid_storage.nfs_mount import require_input_mount,validate_source

CONFIGS={
 'A':dict(input_mode='direct',persistent_reuse=False,scheduler_policy='resource'),
 'B':dict(input_mode='stage',persistent_reuse=False,scheduler_policy='resource'),
 'C':dict(input_mode='stage',persistent_reuse=True,scheduler_policy='resource'),
 'D':dict(input_mode='stage',persistent_reuse=True,scheduler_policy='locality')}
FIELDS=('schema_version','experiment_id','trial_id','block','order','config','repeat_phase','application_cache_state',
 'scope','workload','rows','world_size','workload_sha256','dataset_id','dataset_version','dataset_sha256','dataset_bytes','status',
 'cache_hit_bytes','transferred_bytes','selection_ns','staging_ns','process_ns','total_ns','exit_code')


@dataclass(frozen=True)
class MatrixSettings:
    base:runner.Settings
    repeats:int=3
    order_seed:int=1729
    source_mode:str='nfs'
    input_root:Path|None=None
    nfs_source:str|None=None
    trusted_direct_inputs:bool=False

    def validate(self):
        self.base.validate()
        if type(self.repeats) is not int or not 2<=self.repeats<=30:raise ValueError('Use2..30 independent repeat blocks')
        if type(self.order_seed) is not int or not 0<=self.order_seed<2**32:raise ValueError('Invalid randomization seed')
        if self.source_mode not in ('nfs','local-rehearsal'):raise ValueError('Expected nfs or local-rehearsal')
        if self.source_mode=='nfs':
            validate_source(self.nfs_source)
            if self.input_root is None or not self.input_root.is_absolute() or '..' in self.input_root.parts:raise ValueError('Explicit canonical NFS input root required')
        elif self.input_root is not None or self.nfs_source is not None:raise ValueError('Rehearsal cannot accept an external source path')
        if len(self.base.workloads)*len(self.base.rows)*self.repeats*8>1000:raise ValueError('Trial count exceeds1000')


def schedule(settings):
    settings.validate();rng=random.Random(settings.order_seed);result=[]
    # Blocks are randomized, but cold -> warm is a necessary paired dependency.
    cases=[(block,workload,rows) for block in range(1,settings.repeats+1) for workload in settings.base.workloads for rows in settings.base.rows]
    rng.shuffle(cases)
    for block,workload,rows in cases:
        configs=list(CONFIGS);rng.shuffle(configs)
        for config in configs:
            for phase in ('first','repeat'):
                result.append(dict(block=block,workload=workload,rows=rows,config=config,repeat_phase=phase,order=len(result)+1))
    return result


def hashes():
    paths=set(runner.source_files())|{f'tools/benchmarks/evaluation/{n}.py' for n in ('matrix','matrix_context')}
    paths|={str(p.relative_to(runner.ROOT)) for p in (runner.ROOT/'hybrid_storage').glob('*.py')}
    paths|={'controller/scheduler.py','controller/scheduler_metrics.py'}
    return {p:hashlib.sha256(runner.read_source(p)).hexdigest() for p in sorted(paths)}


def plan(settings):
    entries=schedule(settings);base=runner.plan(settings.base)
    mount=None;readiness='not_checked_local_rehearsal'
    if settings.source_mode=='nfs':
        try:mount=require_input_mount(settings.nfs_source,target=str(settings.input_root));readiness='verified_readonly_identity_io_untested'
        except (ValueError,OSError):readiness='not_ready'
    base.update(schema_version=2,scope='local_component_rehearsal' if settings.source_mode=='local-rehearsal' else 'single_host_nfs_component_comparison',
        comparison_config='A/B/C/D',configs=CONFIGS,trial_order=entries,repeats=settings.repeats,order_seed=settings.order_seed,
        application_cache=dict(A='not_applicable',B='fresh_each_trial',C='empty_then_persistent_repeat',D='empty_then_persistent_repeat'),
        notes=['Explicit component matrix; production storage flags are only observed, never modified.',
               'Only fixed known trusted repository trainers; no direct NFS access for submitted student code.'],
        measurements_missing=['time_to_first_step','nfs_wire_bytes','checkpoint_save_restore_time','recovery_overhead',
                              'server_bandwidth','physical_worker_eligibility','live_controller_placement'],
        sources=hashes(),nfs_input=dict(root=None if settings.input_root is None else str(settings.input_root),source=settings.nfs_source,
            mount=mount,readiness=readiness),scheduler_mode='production_selector_components_on_fixed_equal_logical_slots',
        limitations=['Logical slots share one measured host; no physical Pi or live controller scheduling result.',
            'A runs only hashed trusted repository trainer inputs; no general direct-NFS student mode.',
            'Kernel/NFS page caches uncontrolled; each cell preverified before measurement, no cache dropping.',
            'First/repeat ordering dependent; randomized configuration pairs and blocks, not impossible independent warm-before-cold.',
            'No A/B/C/D speedup inference from local-rehearsal; optional bandwidth E not implemented.'])
    return base


def distribution(samples,seed):
    if any(type(n) is not int or not 0<=n<2**63 for n in samples):raise ValueError('Expected measured bounded integer nanoseconds')
    if not samples:return dict(n=0,samples=[],mean=None,median=None,stdev=None,minimum=None,maximum=None,mean_bootstrap95=None)
    result=dict(n=len(samples),samples=samples,mean=statistics.mean(samples),median=statistics.median(samples),
        stdev=statistics.stdev(samples) if len(samples)>1 else None,minimum=min(samples),maximum=max(samples),mean_bootstrap95=None)
    if len(samples)>=3:
        rng=random.Random(seed);means=sorted(statistics.mean(rng.choices(samples,k=len(samples))) for _ in range(1000))
        result['mean_bootstrap95']=[means[24],means[974]]
    return result


def summarize(records):
    groups={}
    for row in records:
        key=(row['workload'],row['rows'],row['world_size'],row['config'],row['repeat_phase'])
        groups.setdefault(key,[]).append(row)
    result=[]
    for key,rows in sorted(groups.items()):
        for field in ('scope','workload_sha256','dataset_sha256','dataset_version','dataset_id'):
            if len({r.get(field) for r in rows})!=1:raise ValueError('Mixed comparison cohort: '+field)
        good=[r for r in rows if r['status']=='COMPLETED']
        seed=int(hashlib.sha256(runner.canonical(key)).hexdigest()[:8],16)
        result.append(dict(workload=key[0],rows=key[1],world_size=key[2],config=key[3],repeat_phase=key[4],
            attempted=len(rows),completed=len(good),failed=len(rows)-len(good),all_trials_completed=len(good)==len(rows),
            total_ns=distribution([r['total_ns'] for r in good],seed),staging_ns=distribution([r['staging_ns'] for r in good],seed),
            bootstrap_scope='descriptive within-run complete samples; not cluster/generalization confidence; null for n<3'))
    return result


def train(folder,trial,settings,paths,binding,world):
    mapping=trial/'rank-inputs.json';runner.write_json(mapping,paths)
    command=[sys.executable]
    if world>1:command+=['-m','torch.distributed.run','--rdzv-backend=c10d','--rdzv-endpoint=127.0.0.1:0',
        '--nnodes=1',f'--nproc-per-node={world}','--max-restarts=0','--monitor-interval=0.1']
    command+=[str(folder/'source/tools/benchmarks/evaluation/rank_launcher.py'),paths[0]['script'],
              *runner.trainer_arguments(settings.base.workloads[0],settings.base)]
    env=runner.child_environment(folder,trial,binding);env['EVAL_RANK_INPUTS']=str(mapping)
    code,state=runner.execute(command,trial,env,settings.base.timeout_seconds)
    ranks=runner.collect(trial,world)
    return dict(command=command,exit_code=code,status='COMPLETED' if code==0 and runner.successful(ranks,world) else 'TIMEOUT' if state=='TIMEOUT' else 'FAILED',ranks=ranks,
                trainer_quiescent=code==0 and runner.successful(ranks,world))


def run(settings,output_root):
    document=plan(settings)
    if settings.source_mode=='nfs':
        if not settings.trusted_direct_inputs:raise PermissionError('NFS baseline A requires explicit trusted-direct-inputs acknowledgment')
        require_input_mount(settings.nfs_source,target=str(settings.input_root))
    folder=runner.make_root(output_root,settings.base.experiment_id)
    document.update(status='RUNNING',started_at_ns=time.time_ns());runner.write_json(folder/'experiment.json',document)
    contexts={};records=[];pool_count=0
    try:
        runner.snapshot(folder,document['sources']);datasets=folder/'datasets';datasets.mkdir(mode=0o700)
        recipes={}
        for workload in settings.base.workloads:
            for rows in settings.base.rows:
                recipes[workload,rows]=generate(datasets,workload,rows,settings.base.seed,max_bytes=settings.base.max_dataset_bytes)
                runner.write_json(datasets/f'{workload}-{rows}.json',recipes[workload,rows])
        with runner.new_file(folder/'trials.csv') as flat,runner.new_file(folder/'trials.jsonl') as raw:
            writer=csv.DictWriter(flat,fieldnames=FIELDS);writer.writeheader()
            for entry in document['trial_order']:
                config=entry['config'];policy=CONFIGS[config];workload=entry['workload'];world=settings.base.world_size if workload=='ddp' else 1
                recipe=recipes[workload,entry['rows']];trial=folder/f"trial-{entry['order']:04}";trial.mkdir(mode=0o700)
                # Fresh pools retain cold cache dirs instead of deleting anything.
                key=(entry['block'],workload,entry['rows'],config,None if policy['persistent_reuse'] else entry['repeat_phase'])
                context=contexts.get(key)
                if context is None:
                    context=Context(folder/f'pool-{pool_count:04}',settings.input_root,workers=max(world,3),
                        rehearsal=settings.source_mode=='local-rehearsal',nfs_source=settings.nfs_source,library_root=folder/'source')
                    pool_count+=1
                    contexts[key]=context
                    context.register(folder/f'source/examples/{runner.PROJECTS[workload]}',datasets/recipe['path'],world)
                backend=(LocalObjectStorageBackend(context.root/'objects') if settings.source_mode=='local-rehearsal'
                         else NFSObjectStorageBackend(settings.input_root,source=settings.nfs_source))
                # Same immutable authorized objects for all policies. Integrity
                # checks warm kernel pages equally; excluded from timed trial.
                for item in context.template.inputs():
                    with backend.open_artifact(object_key(item.sha256)) as stream:
                        h=hashlib.sha256();size=0
                        for block in iter(lambda:stream.read(1024*1024),b''):h.update(block);size+=len(block)
                        if h.hexdigest()!=item.sha256 or size!=item.size_bytes:raise ValueError('Input differs from exact trial manifest')
                workload_hash=runner.value_hash(dict(workload=workload,world_size=world,args=runner.trainer_arguments(workload,settings.base),sources=document['sources']))
                row=dict(schema_version=2,experiment_id=settings.base.experiment_id,trial_id=trial.name,**entry,
                    application_cache_state=('cold' if entry['repeat_phase']=='first' else 'warm_repeat') if config in ('C','D') else 'not_applicable' if config=='A' else 'fresh_no_reuse',
                    scope=document['scope'],world_size=world,workload_sha256=workload_hash,
                    dataset_id=recipe['dataset_id'],dataset_version=recipe['version'],dataset_sha256=recipe['sha256'],dataset_bytes=recipe['size_bytes'],status='FAILED',
                    cache_hit_bytes=None,transferred_bytes=None,selection_ns=None,staging_ns=0,process_ns=None,total_ns=None,exit_code=None,
                    kernel_cache='uncontrolled_preverified',choice=None,ranks=[],error=None,trainer_quiescent=False)
                runner.write_json(trial/'invocation.json',dict(row,config_definition=policy,input_binding_sha256=context.binding.sha256))
                context.refresh();observations=context.observe(world)
                plans=[];started=time.monotonic_ns()
                try:
                    begin=time.monotonic_ns();selected,choice=context.choose(policy['scheduler_policy'],world,refresh=False,observations=observations)
                    row.update(choice=choice,selection_ns=time.monotonic_ns()-begin)
                    if config=='A':
                        script=next(f for f in context.tree.files if f.path=='train.py')
                        data=next(e for e in context.data.entries if e.type=='file')
                        paths=[dict(script=str(backend.resolve_path(object_key(script.sha256))),dataset=str(backend.resolve_path(object_key(data.sha256)))) for _ in selected]
                    else:
                        begin=time.monotonic_ns();paths,plans,counts=context.stage(selected,entry['order'])
                        row.update(counts,staging_ns=time.monotonic_ns()-begin)
                    begin=time.monotonic_ns()
                    outcome=train(folder,trial,replace(settings,base=replace(settings.base,workloads=(workload,))),paths,context.binding.sha256,world)
                    row.update(outcome,process_ns=time.monotonic_ns()-begin)
                    if row['status']=='COMPLETED' and config in ('C','D') and entry['repeat_phase']=='repeat' and row['transferred_bytes']!=0:
                        row['status']='FAILED';row['error']='WarmReuseInvariant'
                except (ValueError,OSError) as error:row['error']=type(error).__name__
                finally:
                    # execute() has reaped this owned trainer. Successful stages
                    # release pins through the production terminal-path code.
                    for manager,workspace in plans:manager.finish(workspace,'COMPLETED' if row['status']=='COMPLETED' else 'FAILED',quiescent=lambda _:row['trainer_quiescent'])
                    row['total_ns']=time.monotonic_ns()-started
                    runner.write_json(trial/'result.json',row);raw.write(json.dumps(row,allow_nan=False)+'\n')
                    writer.writerow({k:row[k] for k in FIELDS});raw.flush();flat.flush()
                    os.fsync(raw.fileno());os.fsync(flat.fileno());records.append(row)
                if not policy['persistent_reuse'] or entry['repeat_phase']=='repeat':
                    context.close();contexts.pop(key)
        document['status']='COMPLETED' if all(r['status']=='COMPLETED' for r in records) else 'FAILED'
        runner.write_json(folder/'summary.json',dict(schema_version=1,scope=document['scope'],groups=summarize(records),
            raw_units='nanoseconds and bytes',kernel_cache='uncontrolled_preverified',
            inference='No speedup/cluster inference from local rehearsal; confidence intervals descriptive only.'))
    except BaseException as error:
        document['status']='FAILED';document['error']=type(error).__name__;raise
    finally:
        for context in contexts.values():context.close()
        document.update(finished_at_ns=time.time_ns(),trial_count=len(records),completed_trials=sum(r['status']=='COMPLETED' for r in records))
        runner.write_json(folder/'completion.json',document)
    return folder,document


def prepare_inputs(settings,output_root):
    """Public synthetic/trusted-source CAS bundle only; never publish to live NFS."""
    from hybrid_storage.artifacts import ArtifactRegistrar
    document=plan(settings);folder=runner.make_root(output_root,settings.base.experiment_id)
    for name in ('objects','scratch','datasets'):(folder/name).mkdir(mode=0o700)
    publisher=ArtifactRegistrar(folder/'objects',staging_root=folder/'scratch');artifacts={};recipes=[]
    runner.snapshot(folder,document['sources'])
    for workload in settings.base.workloads:
        for rows in settings.base.rows:
            recipe=generate(folder/'datasets',workload,rows,settings.base.seed,max_bytes=settings.base.max_dataset_bytes);recipes.append(recipe)
            paths=[folder/'datasets'/recipe['path'],*(folder/f'source/examples/{runner.PROJECTS[workload]}'/name for name in ('train.py','requirements.txt'))]
            for path in paths:
                with path.open('rb') as stream:artifact=publisher.register(stream,expected_size=path.stat().st_size)
                artifacts[artifact.sha256]=dict(sha256=artifact.sha256,bytes=artifact.size_bytes,object_key=artifact.key)
    document.update(status='PREPARED_NOT_PUBLISHED',trial_count=0,completed_trials=0,artifacts=list(artifacts.values()),datasets=recipes)
    runner.write_json(folder/'input-bundle.json',document)
    return folder,document
