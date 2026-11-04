"""Train a binary MLP or a small image CNN with recoverable optimizer updates."""
import argparse
import csv
import json
import os
from pathlib import Path
import random
import time

# Include PyTorch import/setup in program time; keep timing outside checkpoint state.
PROGRAM_STARTED = time.perf_counter()

import torch
from dl_checkpoint import CheckpointManager


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--task', choices=['binary', 'cnn'], required=True)
    parser.add_argument('--steps', type=int, default=800)
    parser.add_argument('--batch-delay', type=float, default=0.08)
    args = parser.parse_args()
    if args.steps < 1 or args.batch_delay < 0:
        parser.error('Steps must be positive and delay must be nonnegative')
    torch.set_num_threads(int(os.environ.get('OMP_NUM_THREADS', '1')))
    torch.manual_seed(123)
    random.seed(123)

    # The scheduler supplies a snapshot of the submitted dataset to each worker.
    with open(os.environ['DL_DATASET_PATH'], newline='') as stream:
        rows = list(csv.DictReader(stream))
    labels = torch.tensor([int(row['label']) for row in rows], dtype=torch.long)
    if args.task == 'binary':
        inputs = torch.tensor([[float(row['x1']), float(row['x2'])] for row in rows])
        model = torch.nn.Sequential(
            torch.nn.Linear(2, 32), torch.nn.ReLU(), torch.nn.Dropout(0.2),
            torch.nn.Linear(32, 16), torch.nn.ReLU(), torch.nn.Linear(16, 2),
        )
    else:
        inputs = torch.tensor([[float(row[f'pixel{i}']) for i in range(64)]
                               for row in rows]).reshape(-1, 1, 8, 8)
        model = torch.nn.Sequential(
            torch.nn.Conv2d(1, 8, 3, padding=1), torch.nn.ReLU(),
            torch.nn.Conv2d(8, 16, 3, padding=1), torch.nn.ReLU(),
            torch.nn.Flatten(), torch.nn.Dropout(0.2), torch.nn.Linear(16 * 8 * 8, 3),
        )
    optimizer = torch.optim.Adam(model.parameters(), lr=0.003)
    scheduler = torch.optim.lr_scheduler.StepLR(optimizer, step_size=200, gamma=0.8)
    checkpoints = CheckpointManager()
    restored = checkpoints.load(model, optimizer, scheduler=scheduler)
    next_step = restored['progress']['next_step'] if restored else 0
    print(f'TRAINING_START task={args.task} next_step={next_step} '
          f'model/Adam/scheduler/RNG restored={bool(restored)}', flush=True)

    loop_started = time.perf_counter()
    for step in range(next_step, args.steps):
        # Random batches and dropout require restoring RNG state for exact recovery.
        indices = torch.randint(0, len(inputs), (32,))
        optimizer.zero_grad()
        loss = torch.nn.functional.cross_entropy(model(inputs[indices]), labels[indices])
        if not torch.isfinite(loss):
            raise ValueError('Nonfinite training loss')
        loss.backward()
        optimizer.step()
        scheduler.step()
        saved = checkpoints.save(model, optimizer, scheduler=scheduler,
                                 global_step=step + 1, progress={'next_step': step + 1})
        if saved:
            print(f'step={step + 1} loss={loss.item():.6f} checkpoint_saved=True', flush=True)
        if args.batch_delay:
            time.sleep(args.batch_delay)

    loop_seconds = time.perf_counter() - loop_started
    # Publish the final checkpoint before the controller accepts job completion.
    checkpoints.save(model, optimizer, scheduler=scheduler, global_step=args.steps,
                     progress={'next_step': args.steps}, completed=True, force=True)
    model.eval()
    with torch.no_grad():
        logits = model(inputs)
        final_loss = torch.nn.functional.cross_entropy(logits, labels).item()
        predictions = logits.argmax(dim=1)
        training_accuracy = (predictions == labels).float().mean().item()
    output = Path(os.environ['DL_OUTPUT_DIR'])
    torch.save({'model': model.state_dict(), 'optimizer': optimizer.state_dict(),
                'scheduler': scheduler.state_dict(), 'steps': args.steps}, output / 'model.pt')
    (output / 'summary.json').write_text(json.dumps({
        'job_id': os.environ['DL_JOB_ID'], 'task': args.task, 'training_steps': args.steps,
        'resumed_from': next_step, 'final_loss': final_loss,
        'training_accuracy': training_accuracy, 'predictions': predictions.tolist(),
        'torch_version': str(torch.__version__),
        'timing': {'scope': 'final_attempt', 'training_loop_seconds': loop_seconds,
                   'training_program_seconds': time.perf_counter() - PROGRAM_STARTED,
                   'loop_includes': 'batches, checkpoint saves, logging and configured batch delays',
                   'program_ends': 'after evaluation and model saving, before summary writing'},
    }, indent=2))
    print(f'TRAINING_COMPLETE steps={args.steps} resumed_from={next_step}', flush=True)


if __name__ == '__main__':
    main()
