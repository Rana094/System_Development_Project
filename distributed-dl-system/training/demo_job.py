import argparse
import json
import os
import socket
import time


def save_checkpoint(
    checkpoint_path,
    job_id,
    progress,
    duration,
    completed=False
):
    data = {
        "job_id": job_id,
        "progress": progress,
        "duration": duration,
        "completed": completed,
        "hostname": socket.gethostname(),
        "updated_at": time.time()
    }

    # Atomic checkpoint write
    temp_path = checkpoint_path + ".tmp"

    with open(temp_path, "w") as file:
        json.dump(
            data,
            file,
            indent=2
        )

    os.replace(
        temp_path,
        checkpoint_path
    )


def load_checkpoint(
    checkpoint_path
):
    if not os.path.exists(
        checkpoint_path
    ):
        return 0

    try:
        with open(
            checkpoint_path,
            "r"
        ) as file:
            data = json.load(file)

        return int(
            data.get(
                "progress",
                0
            )
        )

    except Exception:
        return 0


def main():

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--job-id",
        required=True
    )

    parser.add_argument(
        "--duration",
        type=int,
        required=True
    )

    parser.add_argument(
        "--checkpoint",
        required=True
    )

    parser.add_argument(
        "--checkpoint-interval",
        type=int,
        default=5
    )

    args = parser.parse_args()


    job_id = args.job_id
    duration = args.duration
    checkpoint_path = (
        args.checkpoint
    )

    os.makedirs(
        os.path.dirname(
            checkpoint_path
        ),
        exist_ok=True
    )


    start_progress = (
        load_checkpoint(
            checkpoint_path
        )
    )


    print(
        f"Job {job_id} started "
        f"on {socket.gethostname()}",
        flush=True
    )

    print(
        f"Target duration: "
        f"{duration}",
        flush=True
    )

    print(
        f"Resuming from progress: "
        f"{start_progress}",
        flush=True
    )


    for progress in range(
        start_progress + 1,
        duration + 1
    ):

        # Small CPU workload
        total = 0

        for i in range(500000):
            total += i * i


        print(
            f"{socket.gethostname()} "
            f"progress: "
            f"{progress}/{duration}",
            flush=True
        )


        # Checkpoint periodically
        if (
            progress
            % args.checkpoint_interval
            == 0
        ):
            save_checkpoint(
                checkpoint_path,
                job_id,
                progress,
                duration
            )

            print(
                f"Checkpoint saved "
                f"at progress "
                f"{progress}",
                flush=True
            )


        time.sleep(1)


    # Final checkpoint
    save_checkpoint(
        checkpoint_path,
        job_id,
        duration,
        duration,
        completed=True
    )


    print(
        "Job completed successfully",
        flush=True
    )


if __name__ == "__main__":
    main()