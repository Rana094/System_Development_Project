"""Plan by default; --run executes only local existing regression/CNN/DDP trainers."""
import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0,str(Path(__file__).resolve().parent))
from evaluation.runner import Settings,new_id,plan,run,ROOT


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run',action='store_true')
    parser.add_argument('--experiment-id',default=None)
    parser.add_argument('--output-root',type=Path,default=ROOT/'reports')
    parser.add_argument('--rows',nargs='+',type=int,default=[64,1024])
    parser.add_argument('--workloads',nargs='+',choices=['regression','cnn','ddp'],default=['regression','cnn','ddp'])
    parser.add_argument('--world-size',type=int,choices=[2,3],default=3)
    parser.add_argument('--steps',type=int,default=2);parser.add_argument('--epochs',type=int,default=1)
    parser.add_argument('--seed',type=int,default=123);parser.add_argument('--timeout-seconds',type=int,default=120)
    parser.add_argument('--max-dataset-bytes',type=int,default=64*1024*1024)
    parser.add_argument('--matrix',action='store_true',help='Controlled A/B/C/D component comparison; plans by default')
    parser.add_argument('--repeats',type=int,default=3)
    parser.add_argument('--order-seed',type=int,default=1729)
    parser.add_argument('--source-mode',choices=['nfs','local-rehearsal'],default='nfs')
    parser.add_argument('--input-root',type=Path)
    parser.add_argument('--nfs-source')
    parser.add_argument('--trusted-direct-inputs',action='store_true')
    parser.add_argument('--prepare-inputs',action='store_true',help='Prepare a new local public-input bundle, never publish or train')
    args=parser.parse_args()
    if args.prepare_inputs and (not args.matrix or args.run):parser.error('--prepare-inputs requires --matrix and excludes --run')
    if not args.matrix and (args.input_root is not None or args.nfs_source is not None or args.trusted_direct_inputs
                           or args.source_mode!='nfs' or args.repeats!=3 or args.order_seed!=1729):
        parser.error('Matrix options require --matrix')
    settings=Settings(args.experiment_id or new_id(),tuple(args.rows),tuple(args.workloads),args.world_size,
        args.steps,args.epochs,args.seed,args.timeout_seconds,args.max_dataset_bytes)
    try:
        if args.matrix:
            from evaluation.matrix import MatrixSettings,plan as matrix_plan,run as matrix_run,prepare_inputs
            matrix=MatrixSettings(settings,args.repeats,args.order_seed,args.source_mode,args.input_root,args.nfs_source,args.trusted_direct_inputs)
            if args.prepare_inputs:
                folder,result=prepare_inputs(matrix,args.output_root.absolute())
                print(json.dumps(dict(output=str(folder),status=result['status'],artifact_count=len(result['artifacts'])),indent=2));return 0
            if not args.run:print(json.dumps(matrix_plan(matrix),indent=2,allow_nan=False));return 0
            folder,result=matrix_run(matrix,args.output_root.absolute())
            print(json.dumps(dict(experiment_id=settings.experiment_id,output=str(folder),status=result['status'],
                trial_count=result['trial_count'],completed_trials=result['completed_trials'],scope=result['scope']),indent=2))
            return int(result['status']!='COMPLETED')
        if not args.run:print(json.dumps(plan(settings),indent=2,allow_nan=False));return 0
        folder,result=run(settings,args.output_root.absolute())
        print(json.dumps(dict(experiment_id=settings.experiment_id,output=str(folder),status=result['status'],
            trial_count=result['trial_count'],completed_trials=result['completed_trials']),indent=2))
        return int(result['status']!='COMPLETED')
    except (ValueError,OSError) as error:parser.exit(2,f'{type(error).__name__}: {error}\n')


if __name__=='__main__':raise SystemExit(main())
