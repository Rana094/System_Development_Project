"""Evaluate the prototype MLP on training data and independent synthetic examples.

The held-out examples use the same fixed synthetic teacher as pytorch_job.py,
but a separate random seed. These are synthetic-task metrics, not real-world
benchmark accuracy.
"""

import argparse
import json
from pathlib import Path

import torch
import torch.nn.functional as F
from pytorch_job import SimpleMLP, create_dataset


def held_out_dataset():
    """Generate unseen examples with the training task's fixed labeling rule."""
    # Recreate only the teacher used after the 6000 training feature vectors.
    teacher_rng = torch.Generator().manual_seed(42)
    torch.randn(6000, 20, generator=teacher_rng)
    teacher = torch.randn(20, 3, generator=teacher_rng)
    test_rng = torch.Generator().manual_seed(2026)
    x = torch.randn(2000, 20, generator=test_rng)
    logits = x @ teacher + 0.2 * torch.randn(2000, 3, generator=test_rng)
    return x, logits.argmax(dim=1)


@torch.no_grad()
def evaluate(model, x, y):
    """Measure classification loss and accuracy without updating any parameters."""
    model.eval()
    output = model(x)
    return {
        "samples": len(y),
        "cross_entropy": F.cross_entropy(output, y).item(),
        "accuracy_percent": (output.argmax(1) == y).float().mean().item() * 100,
    }


def main():
    """Compare the saved model with its initial state on training and held-out data."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("checkpoint", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    torch.set_num_threads(1)
    checkpoint = torch.load(args.checkpoint, map_location="cpu", weights_only=True)
    train = create_dataset()
    initial = SimpleMLP()  # Dataset generation leaves the same initialization seed.
    trained = SimpleMLP()
    trained.load_state_dict(checkpoint["model_state_dict"])
    test_x, test_y = held_out_dataset()
    parameter_delta = (
        sum(
            (a - b).square().sum().item()
            for a, b in zip(initial.parameters(), trained.parameters())
        )
        ** 0.5
    )
    result = {
        "job_id": checkpoint["job_id"],
        "model": "20-64-32-3 ReLU MLP",
        "dataset": "deterministic synthetic three-class classification",
        "parameter_count": sum(p.numel() for p in trained.parameters()),
        "global_step": checkpoint["global_step"],
        "completed": checkpoint["completed"],
        "initial_train": evaluate(initial, *train.tensors),
        "final_train": evaluate(trained, *train.tensors),
        "initial_held_out": evaluate(initial, test_x, test_y),
        "final_held_out": evaluate(trained, test_x, test_y),
        "parameter_change_l2": parameter_delta,
    }
    assert parameter_delta > 0
    assert (
        result["final_train"]["cross_entropy"]
        < result["initial_train"]["cross_entropy"]
    )
    assert (
        result["final_held_out"]["cross_entropy"]
        < result["initial_held_out"]["cross_entropy"]
    )
    output = json.dumps(result, indent=2)
    print(output)
    if args.output:
        args.output.write_text(output + "\n")


if __name__ == "__main__":
    main()
