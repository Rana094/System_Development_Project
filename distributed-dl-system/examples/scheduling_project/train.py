"""Real CPU MLP training for observing concurrent scheduling on small Pi workers."""
import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import time

import torch


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--seconds',type=float,default=30)
    args = parser.parse_args()
    if not 1 <= args.seconds <= 300:
        parser.error('--seconds must be between 1 and 300')
    torch.set_num_threads(int(os.environ.get('OMP_NUM_THREADS','1')))
    torch.manual_seed(123)
    with open(os.environ['DL_DATASET_PATH'],newline='') as stream:
        rows = list(csv.DictReader(stream))
    inputs = torch.tensor([[float(row['x'])] for row in rows])
    targets = torch.tensor([[float(row['y'])] for row in rows])
    # Two hidden layers and Adam perform actual forward/backward parameter updates.
    model = torch.nn.Sequential(torch.nn.Linear(1,16),torch.nn.ReLU(),
                                torch.nn.Linear(16,16),torch.nn.ReLU(),torch.nn.Linear(16,1))
    initial = [p.detach().clone() for p in model.parameters()]
    optimizer = torch.optim.Adam(model.parameters(),lr=0.003)
    started_at = time.time()
    started = time.monotonic()
    steps = 0
    while steps < 10 or time.monotonic() - started < args.seconds:
        optimizer.zero_grad()
        loss = torch.nn.functional.mse_loss(model(inputs),targets)
        if not torch.isfinite(loss):
            raise ValueError('Training loss became nonfinite')
        loss.backward()
        optimizer.step()
        steps += 1
        if steps == 1 or steps % 5000 == 0:
            print(f'step={steps} loss={loss.item():.6f}',flush=True)
    changed = any(not torch.equal(before,after) for before,after in zip(initial,model.parameters()))
    if not changed:
        raise ValueError('No model parameters changed')
    output = Path(os.environ['DL_OUTPUT_DIR'])
    output.mkdir(parents=True,exist_ok=True)
    artifact = output / 'model.pt'
    torch.save({'model_state_dict':model.state_dict(),'optimizer_state_dict':optimizer.state_dict(),
                'steps':steps},artifact)
    summary = {'job_id':os.environ['DL_JOB_ID'],'training_steps':steps,
               'parameters_changed':changed,'elapsed_seconds':time.monotonic()-started,
               'started_at':started_at,'finished_at':time.time(),
               'final_loss':loss.item(),'torch_version':torch.__version__,
               'model_sha256':hashlib.sha256(artifact.read_bytes()).hexdigest()}
    (output / 'summary.json').write_text(json.dumps(summary,indent=2) + '\n')
    print(json.dumps(summary),flush=True)


if __name__ == '__main__':
    main()
