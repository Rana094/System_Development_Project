"""Validate command-line training options and atomically publish complete jobs to the queue."""

import argparse
import json
import sqlite3
import time
import uuid
from pathlib import Path

from job_store import add_job

# Paths
CONTROLLER_DIR = Path(__file__).resolve().parent
DB_PATH = CONTROLLER_DIR / "jobs.db"


# Job ID
def create_job_id():
    """Generate a readable submission timestamp with a short collision-resistant suffix."""
    return "job-" + time.strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:4]


# Store extended job definition
def update_job_definition(job_id, job_type, job_config):
    """Update legacy job metadata; new submissions use atomic add_job instead."""
    connection = sqlite3.connect(DB_PATH, timeout=5)

    try:
        cursor = connection.execute(
            """
            UPDATE jobs
            SET
                job_type = ?,
                job_config = ?,
                updated_at = ?
            WHERE job_id = ?
            """,
            (
                job_type,
                json.dumps(job_config),
                time.time(),
                job_id,
            ),
        )

        if cursor.rowcount != 1:
            raise RuntimeError(f"Could not update job definition " f"for {job_id}")

        connection.commit()

    finally:
        connection.close()


# Dummy job
def create_dummy_job(duration, max_retries):
    """Queue the lightweight compatibility workload with its checkpoint configuration."""
    job_id = create_job_id()

    job_config = {
        "duration": duration,
        "checkpoint_interval": 5,
    }

    add_job(job_id, duration, max_retries, job_type="dummy", job_config=job_config)

    print()
    print("=" * 60)
    print("JOB SUBMITTED")
    print("=" * 60)

    print("Job ID:", job_id)

    print("Type:", "dummy")

    print("Duration:", duration, "seconds")

    print("Max retries:", max_retries)

    print("Status:", "QUEUED")

    print("=" * 60)

    return job_id


# PyTorch job
def create_pytorch_job(
    epochs,
    batch_size,
    learning_rate,
    checkpoint_interval,
    checkpoint_every_seconds,
    batch_delay,
    max_retries,
):
    """Queue a complete PyTorch definition while retaining the legacy duration column."""
    job_id = create_job_id()
    # Compatibility with the existing jobs table.
    #
    # The old job_store expects duration.
    # PyTorch jobs do not use duration for training.
    compatibility_duration = 1
    # Store all PyTorch-specific parameters inside JSON.
    job_config = {
        "epochs": epochs,
        "batch_size": batch_size,
        "learning_rate": learning_rate,
        "checkpoint_interval": checkpoint_interval,
        "checkpoint_every_seconds": checkpoint_every_seconds,
        "batch_delay": batch_delay,
    }

    add_job(
        job_id,
        compatibility_duration,
        max_retries,
        job_type="pytorch",
        job_config=job_config,
    )
    # Display submitted job
    print()
    print("=" * 60)
    print("PYTORCH JOB SUBMITTED")
    print("=" * 60)

    print("Job ID:", job_id)

    print("Type:", "pytorch")

    print("Epochs:", epochs)

    print("Batch size:", batch_size)

    print("Learning rate:", learning_rate)
    # Time-based mode has priority when > 0.
    if checkpoint_every_seconds > 0:

        print("Checkpoint mode:", "TIME")

        print("Checkpoint every:", checkpoint_every_seconds, "second(s)")

        print(
            "Epoch checkpoint interval:",
            checkpoint_interval,
            "(fallback / compatibility)",
        )

    else:

        print("Checkpoint mode:", "EPOCH")

        print("Checkpoint every:", checkpoint_interval, "epoch(s)")

    print("Batch delay:", batch_delay, "seconds")

    print("Max retries:", max_retries)

    print("Status:", "QUEUED")

    print("=" * 60)

    return job_id


# Validation
def validate_args(args, parser):
    # Common
    """Reject invalid training settings before publishing a job that workers cannot run."""
    if not 0 <= args.max_retries <= 10:
        parser.error("--max-retries must be " "between 0 and 10")
    # Dummy
    if args.job_type == "dummy":

        if not 1 <= args.duration <= 300:
            parser.error("duration must be between " "1 and 300 seconds")

        return
    # PyTorch
    if not 1 <= args.epochs <= 100000:
        parser.error("--epochs must be between " "1 and 100000")

    if not 1 <= args.batch_size <= 65536:
        parser.error("--batch-size must be between " "1 and 65536")

    if not 0 < args.learning_rate <= 10:
        parser.error("--learning-rate must be " "greater than 0 and at most 10")

    if not (1 <= args.checkpoint_interval <= args.epochs):
        parser.error(
            "--checkpoint-interval must be " "between 1 and the number of epochs"
        )

    if args.checkpoint_every_seconds < 0:
        parser.error("--checkpoint-every-seconds " "must be >= 0")

    if not 0 <= args.batch_delay <= 60:
        parser.error("--batch-delay must be between " "0 and 60 seconds")


# Main
def main():
    """Parse command-line options and enqueue the requested workload."""
    parser = argparse.ArgumentParser(
        description=(
            "Submit dummy or PyTorch jobs " "to the distributed DL controller."
        )
    )
    # Dummy job options
    parser.add_argument(
        "duration",
        type=int,
        nargs="?",
        default=30,
        help=("Dummy-job duration in seconds. " "Ignored for PyTorch jobs."),
    )
    # Common options
    parser.add_argument(
        "--type",
        dest="job_type",
        choices=[
            "dummy",
            "pytorch",
        ],
        default="dummy",
        help=("Job type " "(default: dummy)"),
    )

    parser.add_argument(
        "--max-retries",
        type=int,
        default=2,
        help=("Maximum automatic retries " "after failure."),
    )
    # PyTorch options
    parser.add_argument(
        "--epochs",
        type=int,
        default=30,
        help=("Number of PyTorch training epochs."),
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=128,
        help=("PyTorch training batch size."),
    )

    parser.add_argument(
        "--learning-rate",
        type=float,
        default=0.001,
        help=("Optimizer learning rate."),
    )
    # Epoch-based checkpoint mode
    parser.add_argument(
        "--checkpoint-interval",
        type=int,
        default=1,
        help=(
            "Checkpoint every N epochs " "when time-based checkpointing " "is disabled."
        ),
    )
    # Time-based checkpoint mode
    #
    # 0 = disabled.
    #
    # When > 0, pytorch_job.py uses this mode instead of
    # normal epoch-based checkpointing.
    parser.add_argument(
        "--checkpoint-every-seconds",
        type=float,
        default=0.0,
        help=(
            "Checkpoint every N seconds. "
            "Use 0 to disable time-based "
            "checkpointing."
        ),
    )

    parser.add_argument(
        "--batch-delay",
        type=float,
        default=0.05,
        help=(
            "Testing delay after each " "PyTorch batch. " "Use 0 for normal training."
        ),
    )
    # Parse + validate
    args = parser.parse_args()

    validate_args(args, parser)
    # Dummy
    if args.job_type == "dummy":

        create_dummy_job(
            duration=args.duration,
            max_retries=args.max_retries,
        )

        return
    # PyTorch
    create_pytorch_job(
        epochs=args.epochs,
        batch_size=args.batch_size,
        learning_rate=args.learning_rate,
        checkpoint_interval=args.checkpoint_interval,
        checkpoint_every_seconds=args.checkpoint_every_seconds,
        batch_delay=args.batch_delay,
        max_retries=args.max_retries,
    )


if __name__ == "__main__":
    main()
