"""Local evaluation wrapper around an unchanged repository trainer, not an agent."""
from contextlib import redirect_stdout,redirect_stderr
import json
import os
import platform
from pathlib import Path
import runpy
import sys
import time
import traceback


def main():
    rank=int(os.environ.get('RANK','0'));world=int(os.environ.get('WORLD_SIZE','1'))
    folder=Path(os.environ['EVAL_TRIAL_ROOT'])/'ranks'/f'rank-{rank}'
    folder.mkdir(parents=True,mode=0o700,exist_ok=False)
    for name in ('output','checkpoints'):(folder/name).mkdir(mode=0o700)
    os.environ['DL_OUTPUT_DIR']=str(folder/'output')
    os.environ['DL_CHECKPOINT_DIR']=str(folder/'checkpoints')
    os.environ['DL_RESUME_STATUS_PATH']=str(folder/'resume-status.json')
    os.environ['DL_LAUNCHER_PID']=str(os.getpid())
    sys.path.insert(0,os.environ['EVAL_SDK_ROOT'])
    script=Path(sys.argv[1])
    if os.environ.get('EVAL_RANK_INPUTS'):
        paths=json.loads(Path(os.environ['EVAL_RANK_INPUTS']).read_text())
        if type(paths) is not list or len(paths)!=world or set(paths[rank])!={'script','dataset'}:
            raise ValueError('Invalid exact rank input map')
        script=Path(paths[rank]['script']);os.environ['DL_DATASET_PATH']=paths[rank]['dataset']
    sys.argv=[str(script),*sys.argv[2:]]
    record=dict(rank=rank,world_size=world,hostname=platform.node(),pid=os.getpid(),
                master_addr=os.environ.get('MASTER_ADDR'),master_port=os.environ.get('MASTER_PORT'),
                started_at_ns=time.time_ns(),status='FAILED')
    started=time.monotonic_ns()
    try:
        with (folder/'stdout.log').open('x') as stdout,(folder/'stderr.log').open('x') as stderr,redirect_stdout(stdout),redirect_stderr(stderr):
            try:runpy.run_path(str(script),run_name='__main__')
            except BaseException:traceback.print_exc();raise
        record['status']='COMPLETED'
    finally:
        record.update(finished_at_ns=time.time_ns(),wrapper_duration_ns=time.monotonic_ns()-started,
                      torch_version=str(sys.modules['torch'].__version__) if 'torch' in sys.modules else None,
                      numpy_version=str(sys.modules['numpy'].__version__) if 'numpy' in sys.modules else None)
        with (folder/'runtime.json').open('x') as stream:json.dump(record,stream,indent=2,allow_nan=False)


if __name__=='__main__':main()
