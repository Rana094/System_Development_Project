import argparse
import os
import socket
import time

import torch
import torch.nn as nn
from torch.utils.data import DataLoader, TensorDataset


# =========================================================
# Model
# =========================================================

class SimpleMLP(nn.Module):
    def __init__(self):
        super().__init__()

        self.network = nn.Sequential(
            nn.Linear(20, 64),
            nn.ReLU(),

            nn.Linear(64, 32),
            nn.ReLU(),

            nn.Linear(32, 3),
        )

    def forward(self, x):
        return self.network(x)


# =========================================================
# Dataset
# =========================================================

def create_dataset(
    num_samples=6000
):
    # Fixed dataset for reproducible experiments.
    torch.manual_seed(42)

    num_features = 20
    num_classes = 3

    x = torch.randn(
        num_samples,
        num_features
    )

    true_weights = torch.randn(
        num_features,
        num_classes
    )

    logits = x @ true_weights

    logits += (
        0.2
        * torch.randn_like(logits)
    )

    y = torch.argmax(
        logits,
        dim=1
    )

    return TensorDataset(
        x,
        y
    )


def create_dataloader(
    dataset,
    batch_size,
    epoch,
):
    """
    Create deterministic shuffled batch order for each epoch.

    This is important for mid-epoch recovery.

    When a job resumes:
      - same epoch
      - same random seed
      - same batch order
      - already completed batches are skipped
    """

    generator = torch.Generator()

    generator.manual_seed(
        1000 + epoch
    )

    return DataLoader(
        dataset,
        batch_size=batch_size,
        shuffle=True,
        generator=generator,
    )


# =========================================================
# Checkpoint
# =========================================================

def save_checkpoint(
    checkpoint_path,
    job_id,
    epoch,
    next_batch_index,
    global_step,
    model,
    optimizer,
    loss,
    completed=False,
    save_reason="periodic",
):
    checkpoint = {
        "checkpoint_version": 2,

        "job_id":
            job_id,

        "epoch":
            epoch,

        "next_batch_index":
            next_batch_index,

        "global_step":
            global_step,

        "model_state_dict":
            model.state_dict(),

        "optimizer_state_dict":
            optimizer.state_dict(),

        "loss":
            loss,

        "completed":
            completed,

        "save_reason":
            save_reason,

        "hostname":
            socket.gethostname(),

        "timestamp":
            time.time(),
    }

    checkpoint_dir = os.path.dirname(
        checkpoint_path
    )

    if checkpoint_dir:
        os.makedirs(
            checkpoint_dir,
            exist_ok=True
        )

    temporary_path = (
        checkpoint_path + ".tmp"
    )

    save_start = time.perf_counter()

    torch.save(
        checkpoint,
        temporary_path
    )

    os.replace(
        temporary_path,
        checkpoint_path
    )

    save_duration = (
        time.perf_counter()
        - save_start
    )

    print(
        f"[CHECKPOINT] "
        f"job={job_id} "
        f"epoch={epoch} "
        f"next_batch={next_batch_index} "
        f"step={global_step} "
        f"reason={save_reason} "
        f"save_time={save_duration:.6f}s "
        f"path={checkpoint_path}",
        flush=True,
    )

    return save_duration


def load_checkpoint(
    checkpoint_path,
    job_id,
    model,
    optimizer,
):
    if not os.path.exists(
        checkpoint_path
    ):
        print(
            "[START] No checkpoint found.",
            flush=True,
        )

        print(
            "[START] Starting training "
            "from epoch 1.",
            flush=True,
        )

        return 1, 0, 0, False

    print(
        f"[CHECKPOINT_FOUND] "
        f"{checkpoint_path}",
        flush=True,
    )

    checkpoint = torch.load(
        checkpoint_path,
        map_location="cpu",
        weights_only=False,
    )

    checkpoint_job_id = (
        checkpoint.get("job_id")
    )

    if checkpoint_job_id != job_id:
        raise RuntimeError(
            f"Checkpoint belongs to "
            f"{checkpoint_job_id}, "
            f"but current job is "
            f"{job_id}"
        )

    model.load_state_dict(
        checkpoint[
            "model_state_dict"
        ]
    )

    optimizer.load_state_dict(
        checkpoint[
            "optimizer_state_dict"
        ]
    )

    completed = checkpoint.get(
        "completed",
        False
    )

    if completed:
        return (
            checkpoint["epoch"],
            checkpoint.get(
                "next_batch_index",
                0
            ),
            checkpoint.get(
                "global_step",
                0
            ),
            True,
        )

    # -----------------------------------------------------
    # Version 2 checkpoint:
    # supports resume inside an epoch.
    # -----------------------------------------------------

    if (
        checkpoint.get(
            "checkpoint_version"
        ) == 2
    ):
        start_epoch = (
            checkpoint["epoch"]
        )

        start_batch = (
            checkpoint.get(
                "next_batch_index",
                0
            )
        )

        global_step = (
            checkpoint.get(
                "global_step",
                0
            )
        )

    else:
        # Backward compatibility with the old
        # epoch-only checkpoint format.
        start_epoch = (
            checkpoint["epoch"] + 1
        )

        start_batch = 0

        global_step = 0

    print(
        f"[RESUMED] "
        f"job={job_id} "
        f"checkpoint_epoch="
        f"{checkpoint['epoch']} "
        f"next_batch="
        f"{start_batch} "
        f"global_step="
        f"{global_step} "
        f"hostname="
        f"{checkpoint.get('hostname')}",
        flush=True,
    )

    return (
        start_epoch,
        start_batch,
        global_step,
        False,
    )


# =========================================================
# Main
# =========================================================

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--job-id",
        required=True,
    )

    parser.add_argument(
        "--epochs",
        type=int,
        default=20,
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=128,
    )

    parser.add_argument(
        "--learning-rate",
        type=float,
        default=0.001,
    )

    parser.add_argument(
        "--checkpoint",
        required=True,
    )

    # -----------------------------------------------------
    # Old mode:
    # checkpoint every N completed epochs.
    # -----------------------------------------------------

    parser.add_argument(
        "--checkpoint-interval",
        type=int,
        default=1,
        help=(
            "Checkpoint every N epochs "
            "when time-based mode is disabled"
        ),
    )

    # -----------------------------------------------------
    # New mode:
    # checkpoint every N seconds.
    #
    # 0 means disabled.
    # -----------------------------------------------------

    parser.add_argument(
        "--checkpoint-every-seconds",
        type=float,
        default=0.0,
        help=(
            "Checkpoint every N seconds. "
            "0 disables time-based mode."
        ),
    )

    parser.add_argument(
        "--batch-delay",
        type=float,
        default=0.0,
        help=(
            "Testing only: sleep after "
            "each batch"
        ),
    )

    args = parser.parse_args()

    if args.epochs < 1:
        raise ValueError(
            "epochs must be >= 1"
        )

    if args.batch_size < 1:
        raise ValueError(
            "batch-size must be >= 1"
        )

    if args.checkpoint_interval < 1:
        raise ValueError(
            "checkpoint-interval "
            "must be >= 1"
        )

    if args.checkpoint_every_seconds < 0:
        raise ValueError(
            "checkpoint-every-seconds "
            "must be >= 0"
        )

    time_based_mode = (
        args.checkpoint_every_seconds
        > 0
    )

    print("=" * 70)
    print("PYTORCH TRAINING JOB")
    print("=" * 70)

    print(
        f"Job ID:             "
        f"{args.job_id}"
    )

    print(
        f"Hostname:           "
        f"{socket.gethostname()}"
    )

    print(
        f"Epochs:             "
        f"{args.epochs}"
    )

    print(
        f"Batch size:         "
        f"{args.batch_size}"
    )

    print(
        f"Learning rate:      "
        f"{args.learning_rate}"
    )

    print(
        f"Checkpoint:         "
        f"{args.checkpoint}"
    )

    if time_based_mode:
        print(
            "Checkpoint mode:    "
            "TIME"
        )

        print(
            "Checkpoint every:   "
            f"{args.checkpoint_every_seconds}"
            " seconds"
        )

    else:
        print(
            "Checkpoint mode:    "
            "EPOCH"
        )

        print(
            "Checkpoint every:   "
            f"{args.checkpoint_interval}"
            " epoch(s)"
        )

    print("=" * 70)

    dataset = create_dataset()

    model = SimpleMLP()

    criterion = (
        nn.CrossEntropyLoss()
    )

    optimizer = torch.optim.Adam(
        model.parameters(),
        lr=args.learning_rate,
    )

    (
        start_epoch,
        resume_batch_index,
        global_step,
        already_completed,
    ) = load_checkpoint(
        args.checkpoint,
        args.job_id,
        model,
        optimizer,
    )

    if already_completed:
        print(
            f"[COMPLETED] "
            f"Job {args.job_id} "
            "was already completed.",
            flush=True,
        )

        return

    last_loss = 0.0

    # When time-based mode starts/restarts,
    # measure from now.
    last_checkpoint_time = (
        time.monotonic()
    )

    for epoch in range(
        start_epoch,
        args.epochs + 1
    ):
        model.train()

        dataloader = (
            create_dataloader(
                dataset,
                args.batch_size,
                epoch,
            )
        )

        running_loss = 0.0
        correct = 0
        total = 0

        processed_batches = 0

        epoch_start = (
            time.time()
        )

        for (
            batch_index,
            (
                batch_x,
                batch_y
            ),
        ) in enumerate(
            dataloader
        ):
            # ---------------------------------------------
            # Recovery:
            # skip batches already completed before
            # the checkpoint was made.
            # ---------------------------------------------

            if (
                epoch == start_epoch
                and batch_index
                < resume_batch_index
            ):
                continue

            optimizer.zero_grad()

            outputs = model(
                batch_x
            )

            loss = criterion(
                outputs,
                batch_y
            )

            loss.backward()

            optimizer.step()

            global_step += 1
            processed_batches += 1

            running_loss += (
                loss.item()
            )

            _, predicted = (
                torch.max(
                    outputs,
                    1
                )
            )

            total += (
                batch_y.size(0)
            )

            correct += (
                predicted
                == batch_y
            ).sum().item()

            last_loss = (
                loss.item()
            )

            if args.batch_delay > 0:
                time.sleep(
                    args.batch_delay
                )

            # ---------------------------------------------
            # TIME-BASED CHECKPOINT
            # ---------------------------------------------

            if time_based_mode:

                elapsed = (
                    time.monotonic()
                    - last_checkpoint_time
                )

                if (
                    elapsed
                    >= args.checkpoint_every_seconds
                ):
                    save_checkpoint(
                        checkpoint_path=
                            args.checkpoint,

                        job_id=
                            args.job_id,

                        epoch=
                            epoch,

                        next_batch_index=
                            batch_index + 1,

                        global_step=
                            global_step,

                        model=
                            model,

                        optimizer=
                            optimizer,

                        loss=
                            last_loss,

                        completed=
                            False,

                        save_reason=
                            "time",
                    )

                    last_checkpoint_time = (
                        time.monotonic()
                    )

        # After the first resumed epoch,
        # subsequent epochs start normally.
        resume_batch_index = 0

        if processed_batches > 0:

            average_loss = (
                running_loss
                / processed_batches
            )

            accuracy = (
                100.0
                * correct
                / total
            )

        else:
            average_loss = (
                last_loss
            )

            accuracy = 0.0

        epoch_time = (
            time.time()
            - epoch_start
        )

        last_loss = (
            average_loss
        )

        print(
            f"[EPOCH] "
            f"epoch={epoch}/"
            f"{args.epochs} "
            f"loss="
            f"{average_loss:.4f} "
            f"accuracy="
            f"{accuracy:.2f}% "
            f"time="
            f"{epoch_time:.2f}s "
            f"global_step="
            f"{global_step}",
            flush=True,
        )

        # ---------------------------------------------
        # EPOCH-BASED MODE
        # ---------------------------------------------

        if (
            not time_based_mode
            and epoch
            % args.checkpoint_interval
            == 0
        ):
            save_checkpoint(
                checkpoint_path=
                    args.checkpoint,

                job_id=
                    args.job_id,

                epoch=
                    epoch,

                # Epoch completed.
                # Resume from next epoch.
                next_batch_index=
                    len(dataloader),

                global_step=
                    global_step,

                model=
                    model,

                optimizer=
                    optimizer,

                loss=
                    average_loss,

                completed=
                    False,

                save_reason=
                    "epoch",
            )

            last_checkpoint_time = (
                time.monotonic()
            )

    # =====================================================
    # Final checkpoint
    # =====================================================

    save_checkpoint(
        checkpoint_path=
            args.checkpoint,

        job_id=
            args.job_id,

        epoch=
            args.epochs,

        next_batch_index=
            0,

        global_step=
            global_step,

        model=
            model,

        optimizer=
            optimizer,

        loss=
            last_loss,

        completed=
            True,

        save_reason=
            "final",
    )

    print(
        f"[COMPLETED] "
        f"job={args.job_id}",
        flush=True,
    )


if __name__ == "__main__":
    main()