"""Measure individual and two-Pi DDP timing for an example or reference workload."""
import argparse
from datetime import datetime
import fcntl
import json
import hashlib
import math
from pathlib import Path
import statistics
import subprocess
import sys
import tempfile
import time
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'controller'))
from job_store import get_job, get_jobs_by_status, get_ranks, get_attempts, get_reservations, control_job
from scheduler import load_nodes, get_resources


def friend_project(directory):
    """Keep the reference bytes unchanged; adapt launch/output around its main()."""
    source = Path('/home/rana/System/System/distML/pretrained_ddp/benchmark_node.py')
    directory.mkdir()
    (directory/'reference.py').write_bytes(source.read_bytes())
    (directory/'requirements.txt').write_text('torch\ntorchvision\n')
    (directory/'train.py').write_text('''"""Launch the unchanged friend benchmark and retain its own timing marker."""
import argparse, hashlib, json, math, os, runpy, sys, time
from pathlib import Path
import torch
import torchvision.models as models

def digest(model):
    result=hashlib.sha256()
    for name,tensor in model.named_parameters():
        result.update(name.encode())
        result.update(bytes(tensor.detach().cpu().contiguous().view(torch.uint8).flatten().tolist()))
    return result.hexdigest()

class Capture:
    def __init__(self,stream): self.stream=stream;self.parts=[]
    def write(self,value): self.parts.append(value);self.stream.write(value);self.stream.flush();return len(value)
    def flush(self): self.stream.flush()

parser=argparse.ArgumentParser()
parser.add_argument('--epochs',type=int,default=1)
parser.add_argument('--samples',type=int,default=1000)
args=parser.parse_args()
rank=int(os.environ.get('RANK','0'));world=int(os.environ.get('WORLD_SIZE','1'))
torch.manual_seed(123)
created=[];initial=[]
original_model=models.resnet18;original_optimizer=torch.optim.Adam
def build(*a,**kw):
    model=original_model(*a,**kw);created.append(model);return model
def optimizer(*a,**kw):
    initial.append(digest(created[0]));return original_optimizer(*a,**kw)
models.resnet18=build;torch.optim.Adam=optimizer
reference=Path(__file__).with_name('reference.py')
sys.argv=[str(reference),'--epochs',str(args.epochs),'--samples',str(args.samples)]
if world>1: sys.argv.append('--distributed')
capture=Capture(sys.stdout);previous=sys.stdout;start=time.perf_counter()
try:
    sys.stdout=capture
    runpy.run_path(str(reference),run_name='__main__')
finally:
    sys.stdout=previous;models.resnet18=original_model;torch.optim.Adam=original_optimizer
elapsed=time.perf_counter()-start
markers=[line for line in ''.join(capture.parts).splitlines() if line.startswith('BENCHMARK_RESULT|')]
training=float(markers[-1].split('|')[-1]) if markers else None
output=Path(os.environ['DL_OUTPUT_DIR']);model=created[0]
if rank==0: torch.save({'model':model.state_dict()},output/'model.pt')
(output/'summary.json').write_text(json.dumps({
    'rank':rank,'world_size':world,'epochs':args.epochs,
    'training_steps':math.ceil(math.ceil(args.samples/world)/32)*args.epochs,
    'dataset_samples':args.samples,'samples_processed_per_rank':math.ceil(args.samples/world)*args.epochs,
    'global_batch_size':32*world,'local_batch_size':32,
    'training_seconds':training,'reference_wrapper_seconds':elapsed,
    'initial_model_sha256':initial[0],'final_model_sha256':digest(model),
    'torch_version':str(torch.__version__),
},indent=2))
''')
    return {'source':str(source),'sha256':hashlib.sha256(source.read_bytes()).hexdigest(),
            'reference_settings':'ResNet-18 frozen backbone; CIFAR-10; batch 32 per worker; 2 DataLoader workers; original timer; seed 123 supplied by adapter.'}


def command(arguments):
    result = subprocess.run([sys.executable,*arguments],cwd=ROOT,
                            stdout=subprocess.PIPE,text=True)
    if result.returncode:
        raise RuntimeError('Command failed: '+result.stdout)
    return json.loads(result.stdout)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--nodes',nargs=2,default=['pi02','pi03'])
    parser.add_argument('--epochs',type=int)
    parser.add_argument('--repeats',type=int,default=3)
    parser.add_argument('--workload',choices=['mlp','friend-resnet18'],default='mlp')
    parser.add_argument('--samples',type=int,default=1000)
    parser.add_argument('--cpu-cores',type=int)
    args = parser.parse_args()
    if args.epochs is None:
        args.epochs = 1 if args.workload=='friend-resnet18' else 1000
    if args.epochs < 1 or args.repeats < 1 or len(set(args.nodes)) != 2:
        parser.error('Use positive epochs/repeats and two different workers')
    if args.workload=='friend-resnet18' and (args.samples < 2 or args.samples > 50000 or args.samples % 2):
        parser.error('Use an even CIFAR-10 sample count between 2 and 50000')
    cores = args.cpu_cores if args.cpu_cores is not None else (4 if args.workload=='friend-resnet18' else 1)
    if cores < 1:
        parser.error('CPU cores must be positive')
    known = {node['id']:node for node in load_nodes()}
    if any(node not in known for node in args.nodes):
        parser.error('Use configured worker IDs')
    lock = open(ROOT/'controller/.dispatcher.lock','a')
    try:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    except BlockingIOError:
        parser.error('Stop the running dispatcher before this isolated benchmark')
    if any(get_jobs_by_status(s) for s in ('QUEUED','SCHEDULED','RUNNING','CANCELLING')):
        parser.error('Finish existing active jobs before benchmarking')
    snapshots = {node:get_resources(known[node]) for node in args.nodes}
    if any(not snapshot or snapshot['node_state'] != 'IDLE' for snapshot in snapshots.values()):
        parser.error('Both benchmark workers must be reachable and idle')
    stamp = datetime.now(ZoneInfo('Asia/Dhaka')).strftime('%Y%m%d-%H%M%S')
    output = ROOT/'storage/benchmarks'/stamp
    output.mkdir(parents=True)
    print('Results directory:',output,flush=True)
    reference = friend_project(output/'project') if args.workload=='friend-resnet18' else None
    project = output/'project' if reference else ROOT/'examples/ddp_project'
    owner = 'training-benchmark'
    rows = []
    owned = []
    import dispatcher
    dispatcher.init_recovery_table()
    try:
        # Snapshot every trial before measurement. Environment installation and
        # input staging are excluded; the actual scheduler executes each trial.
        plan = []
        with tempfile.TemporaryDirectory(prefix='training-benchmark-') as temporary:
            for trial in range(1,args.repeats+1):
                modes = ['individual','ddp'] if trial % 2 else ['ddp','individual']
                for mode in modes:
                    workers = 1 if mode == 'individual' else 2
                    spec = {'schema_version':1,'name':f'{mode}-timing-trial-{trial}',
                            'project':str(project),
                            'entrypoint':(['python','train.py','--epochs',str(args.epochs),'--samples',str(args.samples)]
                                         if reference else ['python','train.py','--epochs',str(args.epochs),
                                          '--batch-delay','0','--global-batch-size','64','--log-every','100']),
                            'dataset':None if reference else str(ROOT/'examples/binary-classification.csv'),
                            'execution':{'mode':'single' if workers==1 else 'distributed','workers':workers},
                            'resources_per_worker':{'cpu_cores':cores,'memory_gb':2 if reference else 1,'gpus':0},
                            'environment':{'requirements':'requirements.txt'},
                            'recovery':{'mode':'restart','max_retries':0}}
                    path = Path(temporary)/f'{mode}-{trial}.json'
                    path.write_text(json.dumps(spec))
                    job_id = command(['controller/submit_job.py','--spec',str(path),'--owner',owner])['job_id']
                    owned.append(job_id)
                    nodes = args.nodes[:workers]
                    print(f'Preparing {mode} trial {trial}: {job_id}',flush=True)
                    command(['controller/prepare_job.py','--job-id',job_id,'--nodes',*nodes,
                             '--profile','pytorch-cpu','--summary'])
                    if get_job(job_id)['status'] != 'READY':
                        raise RuntimeError('Environment preparation did not produce a READY job')
                    plan.append((trial,mode,job_id))

        with (output/'dispatcher.log').open('w') as log:
            # Keep production controller logs as evidence without mixing them
            # with the compact timing table printed for the operator.
            from contextlib import redirect_stdout
            for trial,mode,job_id in plan:
                start = time.perf_counter()
                command(['controller/queue_job.py','--job-id',job_id])
                deadline = time.monotonic()+1200
                while time.monotonic() < deadline:
                    with redirect_stdout(log):
                        dispatcher.reconcile_cancellations()
                        dispatcher.reconcile_scheduled_jobs()
                        dispatcher.monitor_running_jobs()
                        dispatcher.dispatch_queued_jobs()
                    job = get_job(job_id)
                    if job['status'] == 'COMPLETED':
                        total = time.perf_counter()-start
                        break
                    if job['status'] in ('FAILED','CANCELLED'):
                        raise RuntimeError(f"{job_id}: {job['last_error']}")
                    time.sleep(dispatcher.DISPATCH_INTERVAL)
                else:
                    raise RuntimeError('Training benchmark timed out')
                workspace = Path(job['storage_path'])
                ranks = get_ranks(job_id,0)
                paths = ([workspace/'results'/f"rank-{rank['rank']}"/'summary.json' for rank in ranks]
                         if ranks else [workspace/'results/summary.json'])
                summaries = [json.loads(path.read_text()) for path in paths]
                workers = 2 if mode=='ddp' else 1
                samples = args.samples if reference else 256
                batch = 32*workers if reference else 64
                expected_steps = args.epochs*math.ceil(math.ceil(samples/workers)/(32 if reference else 64//workers))
                if (job['retry_count'] or any(s['training_steps'] != expected_steps or s['dataset_samples'] != samples
                    or s['global_batch_size'] != batch or not s.get('parameters_synchronized',True)
                    or s['initial_model_sha256'] == s['final_model_sha256'] for s in summaries)
                    or sum(s['samples_processed_per_rank'] for s in summaries) != args.epochs*samples
                    or len({s['final_model_sha256'] for s in summaries}) != 1):
                    raise RuntimeError('Trial did not complete the expected synchronized training work')
                manifest = json.loads((workspace/'manifest.json').read_text())
                row = {'trial':trial,'mode':mode,'job_id':job_id,
                       'workers':[rank['node_id'] for rank in ranks] if ranks else [job['node_id']],
                       'training_seconds':max(s['training_seconds'] for s in summaries if s['training_seconds'] is not None),
                       'total_scheduler_seconds':total,'optimizer_updates':expected_steps,
                       'samples_processed':args.epochs*samples,'global_batch_size':batch,
                       'training_accuracy':summaries[0].get('training_accuracy'),
                       'final_loss':summaries[0].get('final_loss'),
                       'initial_model_sha256':summaries[0]['initial_model_sha256'],
                       'source_version':manifest['source']['version'],
                       'dataset_version':manifest['data']['version'],
                       'attempts':get_attempts(job_id)}
                rows.append(row)
                (output/'trials.json').write_text(json.dumps(rows,indent=2)+'\n')
                print(f"{mode} trial {trial}: training={row['training_seconds']:.3f}s; total={total:.3f}s",flush=True)

        if len({row['source_version'] for row in rows}) != 1 or len({row['dataset_version'] for row in rows}) != 1:
            raise RuntimeError('Trials used different source code or datasets')
        if len({row['initial_model_sha256'] for row in rows}) != 1:
            raise RuntimeError('Trials started from different model parameters')
        medians = {mode:{field:statistics.median(row[field] for row in rows if row['mode']==mode)
                         for field in ('training_seconds','total_scheduler_seconds')}
                   for mode in ('individual','ddp')}
        comparisons = {}
        for field in ('training_seconds','total_scheduler_seconds'):
            single,parallel = medians['individual'][field],medians['ddp'][field]
            comparisons[field] = {'speedup':single/parallel,
                                  'ddp_time_change_percent':100*(parallel-single)/single,
                                  'ddp_faster':parallel < single}
        report = {'created_at':datetime.now(ZoneInfo('Asia/Dhaka')).isoformat(),
                  'epochs':args.epochs,'repeats':args.repeats,'workload':args.workload,
                  'reference':reference,'global_batch_size':'32 single / 64 DDP' if reference else 64,
                  'artificial_delay_seconds':0,'per_worker_cpu_cores':cores,
                  'initial_worker_resources':snapshots,'medians':medians,'comparisons':comparisons,
                  'trials':rows,
                  'timing_scope':{'training':('Original friend BENCHMARK_RESULT timer rounded to 0.01 seconds, including training and final DDP barrier; excludes initialization and model loading.'
                                             if reference else 'Timed optimization loop, epoch logging, DDP synchronization and end barrier; excludes initialization/evaluation/artifact saving.'),
                                  'total':'Enqueue through controller COMPLETED, including launch, environment verification, process-group startup, two-second controller polling and artifact collection; excludes submission and preparation.'}}
        (output/'results.json').write_text(json.dumps(report,indent=2)+'\n')
        configuration = (f"Unchanged friend ResNet-18 benchmark, {args.samples} CIFAR-10 images, {args.epochs} epochs, "
                         'batch size 32 per worker (global 32 single / 64 DDP), no artificial delays. '
                         if reference else f'Same source code, 256 training examples, {args.epochs} epochs, global batch 64, '
                         f'{args.epochs*4} optimizer updates and no artificial delays. ')
        lines = ['# Individual and DDP training time comparison','',
                 configuration+
                 f'{args.repeats} alternating trials per mode; median times reported.','',
                 f"Individual: {args.nodes[0]}, {cores} CPU cores. DDP: {', '.join(args.nodes)}, {cores} CPU cores per Pi.",'',
                 '| Measurement | Individual | Two-Pi DDP | Speedup | DDP time change |',
                 '|---|---:|---:|---:|---:|']
        for field,label in [('training_seconds','Training loop'),('total_scheduler_seconds','Total through scheduler')]:
            single,parallel = medians['individual'][field],medians['ddp'][field]
            comparison = comparisons[field]
            lines.append(f"| {label} | {single:.3f} s | {parallel:.3f} s | {comparison['speedup']:.3f}× | {comparison['ddp_time_change_percent']:+.1f}% |")
            direction = 'less' if comparison['ddp_faster'] else 'more'
            print(f"{label}: DDP took {abs(comparison['ddp_time_change_percent']):.1f}% {direction} time; speedup={comparison['speedup']:.3f}x",flush=True)
        lines += ['', 'Speedup = individual time / DDP time. A value below 1 means DDP is slower.',
                  'Time change = (DDP time − individual time) / individual time × 100; positive means slower.', '',
                  report['timing_scope']['training'],report['timing_scope']['total'],'',
                  'Speedup depends on workload, communication, batch settings and hardware. '
                  'These results describe this CPU workload and these Pis, not all deep learning workloads. '
                  'Floating-point reduction order may produce different final weights between modes.','',
                  ('Per-trial times, job IDs and input hashes are in results.json. The original benchmark does not report final accuracy.'
                   if reference else 'Per-trial times, losses, training accuracy, job IDs and input hashes are in results.json.')]
        if reference:
            lines += ['',f"Original source: {reference['source']}",f"Verified source SHA-256: {reference['sha256']}",
                      'The original file is unchanged. The adapter supplies launch arguments and captures timing/model parameters after execution.',
                      'The original benchmark uses 32 updates on one worker and 16 per worker in two-worker DDP for 1000 images/one epoch. '
                      'This reproduces its batch convention rather than the earlier fixed-global-batch MLP experiment.']
        (output/'comparison.md').write_text('\n'.join(lines)+'\n')
        (output/'trials.json').unlink(missing_ok=True)
        print('Saved:',output/'comparison.md',flush=True)
    finally:
        for job_id in owned:
            if get_job(job_id)['status'] not in ('COMPLETED','FAILED','CANCELLED'):
                control_job(job_id,owner,'cancel')
        deadline = time.monotonic()+30
        while any(get_job(job_id)['status']=='CANCELLING' for job_id in owned) and time.monotonic()<deadline:
            dispatcher.reconcile_cancellations()
            time.sleep(1)


if __name__ == '__main__':
    main()
