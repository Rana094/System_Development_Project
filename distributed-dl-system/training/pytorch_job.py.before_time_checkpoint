import argparse
import os
import socket
import time

import torch
import torch.nn as nn
from torch.utils.data import DataLoader, TensorDataset


# ---------------------------------------------------------
# Simple neural network
# ---------------------------------------------------------
class SimpleMLP(nn.Module):
    def __init__(self):
        super().__init__()

        self.network = nn.Sequential(
            nn.Linear(20, 64),
            nn.ReLU(),

            nn.Linear(64, 32),
            nn.ReLU(),

            nn.Linear(32, 3)
        )

    def forward(self, x):
        return self.network(x)


# ---------------------------------------------------------
# Create a deterministic synthetic classification dataset
# ---------------------------------------------------------
def create_dataset(num_samples=6000):
    # Same dataset every time the job restarts.
    torch.manual_seed(42)

    num_features = 20
    num_classes = 3

    x = torch.randn(num_samples, num_features)

    true_weights = torch.randn(num_features, num_classes)

    logits = x @ true_weights

    # Add a little noise.
    logits += 0.2 * torch.randn_like(logits)

    y = torch.argmax(logits, dim=1)

    return TensorDataset(x, y)


# ---------------------------------------------------------
# Atomic checkpoint save
# ---------------------------------------------------------
def save_checkpoint(
    checkpoint_path,
    job_id,
    epoch,
    model,
    optimizer,
    loss,
    completed=False
):
    checkpoint = {
        "job_id": job_id,
        "epoch": epoch,
        "model_state_dict": model.state_dict(),
        "optimizer_state_dict": optimizer.state_dict(),
        "loss": loss,
        "completed": completed,
        "hostname": socket.gethostname(),
        "timestamp": time.time(),
    }

    checkpoint_dir = os.path.dirname(checkpoint_path)

    if checkpoint_dir:
        os.makedirs(checkpoint_dir, exist_ok=True)

    temporary_path = checkpoint_path + ".tmp"

    torch.save(checkpoint, temporary_path)

    # Atomic replacement.
    os.replace(temporary_path, checkpoint_path)

    print(
        f"[CHECKPOINT] "
        f"job={job_id} "
        f"epoch={epoch} "
        f"path={checkpoint_path}"
    )


# ---------------------------------------------------------
# Load checkpoint
# ---------------------------------------------------------
def load_checkpoint(
    checkpoint_path,
    job_id,
    model,
    optimizer
):
    if not os.path.exists(checkpoint_path):
        print("[START] No checkpoint found.")
        print("[START] Starting training from epoch 1.")

        return 1, False

    print(f"[CHECKPOINT_FOUND] {checkpoint_path}")

    checkpoint = torch.load(
        checkpoint_path,
        map_location="cpu",
        weights_only=False
    )

    checkpoint_job_id = checkpoint.get("job_id")

    if checkpoint_job_id != job_id:
        raise RuntimeError(
            f"Checkpoint belongs to {checkpoint_job_id}, "
            f"but current job is {job_id}"
        )

    model.load_state_dict(
        checkpoint["model_state_dict"]
    )

    optimizer.load_state_dict(
        checkpoint["optimizer_state_dict"]
    )

    last_epoch = checkpoint["epoch"]

    completed = checkpoint.get(
        "completed",
        False
    )

    print(
        f"[RESUMED] "
        f"job={job_id} "
        f"checkpoint_epoch={last_epoch} "
        f"hostname={checkpoint.get('hostname')}"
    )

    return last_epoch + 1, completed


# ---------------------------------------------------------
# Main training function
# ---------------------------------------------------------
def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--job-id",
        required=True
    )

    parser.add_argument(
        "--epochs",
        type=int,
        default=20
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=128
    )

    parser.add_argument(
        "--learning-rate",
        type=float,
        default=0.001
    )

    parser.add_argument(
        "--checkpoint",
        required=True
    )

    parser.add_argument(
        "--checkpoint-interval",
        type=int,
        default=1,
        help="Save checkpoint every N epochs"
    )

    parser.add_argument(
        "--batch-delay",
        type=float,
        default=0.0,
        help="Testing only: sleep after each batch"
    )

    args = parser.parse_args()

    print("=" * 60)
    print("PYTORCH TRAINING JOB")
    print("=" * 60)

    print(f"Job ID:      {args.job_id}")
    print(f"Hostname:    {socket.gethostname()}")
    print(f"Epochs:      {args.epochs}")
    print(f"Batch size:  {args.batch_size}")
    print(f"Checkpoint:  {args.checkpoint}")
    print("=" * 60)

    # -----------------------------------------------------
    # Dataset
    # -----------------------------------------------------
    dataset = create_dataset()

    dataloader = DataLoader(
        dataset,
        batch_size=args.batch_size,
        shuffle=True
    )

    # -----------------------------------------------------
    # Model
    # -----------------------------------------------------
    model = SimpleMLP()

    # -----------------------------------------------------
    # Loss function
    # -----------------------------------------------------
    criterion = nn.CrossEntropyLoss()

    # -----------------------------------------------------
    # Optimizer
    # -----------------------------------------------------
    optimizer = torch.optim.Adam(
        model.parameters(),
        lr=args.learning_rate
    )

    # -----------------------------------------------------
    # Try to restore checkpoint
    # -----------------------------------------------------
    start_epoch, already_completed = load_checkpoint(
        args.checkpoint,
        args.job_id,
        model,
        optimizer
    )

    if already_completed:
        print(
            f"[COMPLETED] Job {args.job_id} "
            f"was already completed."
        )

        return

    # -----------------------------------------------------
    # Training loop
    # -----------------------------------------------------
    last_loss = 0.0

    for epoch in range(
        start_epoch,
        args.epochs + 1
    ):
        model.train()

        running_loss = 0.0
        correct = 0
        total = 0

        epoch_start = time.time()

        for batch_x, batch_y in dataloader:
            # Clear previous gradients.
            optimizer.zero_grad()

            # Forward pass.
            outputs = model(batch_x)

            # Calculate loss.
            loss = criterion(
                outputs,
                batch_y
            )

            # Backpropagation.
            loss.backward()

            # Update model parameters.
            optimizer.step()

            running_loss += loss.item()

            _, predicted = torch.max(
                outputs,
                1
            )

            total += batch_y.size(0)

            correct += (
                predicted == batch_y
            ).sum().item()

            # Optional delay only for failure testing.
            if args.batch_delay > 0:
                time.sleep(args.batch_delay)

        average_loss = (
            running_loss /
            len(dataloader)
        )

        accuracy = (
            100.0 * correct / total
        )

        epoch_time = (
            time.time() -
            epoch_start
        )

        last_loss = average_loss

        print(
            f"[EPOCH] "
            f"epoch={epoch}/{args.epochs} "
            f"loss={average_loss:.4f} "
            f"accuracy={accuracy:.2f}% "
            f"time={epoch_time:.2f}s"
        )

        # ---------------------------------------------
        # Periodic checkpoint
        # ---------------------------------------------
        if (
            epoch %
            args.checkpoint_interval
            == 0
        ):
            save_checkpoint(
                args.checkpoint,
                args.job_id,
                epoch,
                model,
                optimizer,
                average_loss,
                completed=False
            )

    # -----------------------------------------------------
    # Final completed checkpoint
    # -----------------------------------------------------
    save_checkpoint(
        args.checkpoint,
        args.job_id,
        args.epochs,
        model,
        optimizer,
        last_loss,
        completed=True
    )

    print(
        f"[COMPLETED] "
        f"job={args.job_id}"
    )


if __name__ == "__main__":
    main()
