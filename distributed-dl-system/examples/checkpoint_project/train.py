"""Submitted MLP training with stochastic batches, dropout and cooperative recovery."""
import argparse
import csv
import json
import os
from pathlib import Path
import random
import time

import torch
from dl_checkpoint import CheckpointManager


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--steps',type=int,default=6000)
    parser.add_argument('--batch-delay',type=float,default=0.003)
    args=parser.parse_args()
    if args.steps<1 or args.batch_delay<0: parser.error('Invalid training bounds')
    torch.set_num_threads(int(os.environ.get('OMP_NUM_THREADS','1')))
    torch.manual_seed(123);random.seed(123)
    try:
        import numpy as np
        np.random.seed(123)
    except ImportError:
        pass
    with open(os.environ['DL_DATASET_PATH'],newline='') as stream:
        rows=list(csv.DictReader(stream))
    x=torch.tensor([[float(row['x'])] for row in rows])
    y=torch.tensor([[float(row['y'])] for row in rows])
    model=torch.nn.Sequential(torch.nn.Linear(1,16),torch.nn.ReLU(),torch.nn.Dropout(0.2),
                             torch.nn.Linear(16,16),torch.nn.ReLU(),torch.nn.Linear(16,1))
    optimizer=torch.optim.Adam(model.parameters(),lr=0.003)
    scheduler=torch.optim.lr_scheduler.StepLR(optimizer,step_size=1000,gamma=0.8)
    checkpoint=CheckpointManager()
    restored=checkpoint.load(model,optimizer,scheduler=scheduler)
    next_step=restored['progress']['next_step'] if restored else 0
    print(f'TRAINING_START next_step={next_step} model/Adam/scheduler/RNG restored={bool(restored)}',flush=True)
    for step in range(next_step,args.steps):
        # Random sampling and dropout make missing RNG restoration observable.
        indices=torch.randint(0,len(x),(16,))
        optimizer.zero_grad()
        loss=torch.nn.functional.mse_loss(model(x[indices]),y[indices])
        if not torch.isfinite(loss): raise ValueError('Nonfinite training loss')
        loss.backward();optimizer.step();scheduler.step()
        saved=checkpoint.save(model,optimizer,scheduler=scheduler,global_step=step+1,
                              progress={'next_step':step+1})
        if saved or step%1000==0:
            print(f'step={step+1} loss={loss.item():.6f} checkpoint_saved={saved}',flush=True)
        if args.batch_delay: time.sleep(args.batch_delay)
    # Completion is explicit: the final state must reach the controller before
    # the scheduler marks a checkpoint-enabled submission COMPLETED.
    checkpoint.save(model,optimizer,scheduler=scheduler,global_step=args.steps,
                    progress={'next_step':args.steps},completed=True,force=True)
    output=Path(os.environ['DL_OUTPUT_DIR'])
    torch.save({'model':model.state_dict(),'optimizer':optimizer.state_dict(),
                'scheduler':scheduler.state_dict(),'steps':args.steps},output/'model.pt')
    (output/'summary.json').write_text(json.dumps({'job_id':os.environ['DL_JOB_ID'],
        'training_steps':args.steps,'resumed_from':next_step,'torch_version':str(torch.__version__)}))
    print(f'TRAINING_COMPLETE steps={args.steps} resumed_from={next_step}',flush=True)


if __name__=='__main__':
    main()
