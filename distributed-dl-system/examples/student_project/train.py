"""Example student-owned script; submission does not require a controller allowlist.

The execution layer supplies rank settings for distributed mode in later steps.
Step-one submission only validates and records this project.
"""

import argparse
import csv
import os
from pathlib import Path

import torch
import torch.distributed as dist
from torch.nn.parallel import DistributedDataParallel


def main():
    """Use a single process or a DDP group according to executor-provided settings."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--epochs", type=int, default=5)
    args = parser.parse_args()
    # The execution layer will supply a staged dataset path in a later step.
    default_dataset = Path(__file__).resolve().parents[1] / "dataset.csv"
    path = Path(os.environ.get("DL_DATASET_PATH", str(default_dataset)))
    with path.open(newline="") as stream:
        rows = list(csv.DictReader(stream))
    x = torch.tensor([[float(row["x"])] for row in rows])
    y = torch.tensor([[float(row["y"])] for row in rows])
    model = torch.nn.Linear(1, 1)
    world_size = int(os.environ.get("WORLD_SIZE", "1"))
    if world_size > 1:
        rank = int(os.environ["RANK"])
        dist.init_process_group("gloo", rank=rank, world_size=world_size)
        # Equal shards keep this four-row example's collective steps aligned.
        if world_size > len(rows) or len(rows) % world_size:
            raise ValueError("Example dataset requires equal, nonempty rank shards")
        x, y = x[rank::world_size], y[rank::world_size]
        model = DistributedDataParallel(model)
    optimizer = torch.optim.SGD(model.parameters(), lr=0.01)
    for epoch in range(args.epochs):
        optimizer.zero_grad()
        loss = torch.nn.functional.mse_loss(model(x), y)
        loss.backward()
        optimizer.step()
        print(f"Epoch {epoch + 1}: loss={loss.item():.6f}", flush=True)
    if world_size > 1:
        dist.destroy_process_group()


if __name__ == "__main__":
    main()
