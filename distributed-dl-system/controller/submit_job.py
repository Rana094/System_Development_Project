import argparse
import time
import uuid

from job_store import add_job


def create_job(
    duration,
    max_retries
):
    job_id = (
        "job-"
        + time.strftime(
            "%Y%m%d-%H%M%S"
        )
        + "-"
        + uuid.uuid4().hex[:4]
    )

    add_job(
        job_id,
        duration,
        max_retries
    )

    print()
    print("=" * 60)
    print("JOB SUBMITTED")
    print("=" * 60)

    print(
        "Job ID:",
        job_id
    )

    print(
        "Duration:",
        duration,
        "seconds"
    )

    print(
        "Max retries:",
        max_retries
    )

    print(
        "Status:",
        "QUEUED"
    )

    print("=" * 60)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "duration",
        type=int,
        nargs="?",
        default=30
    )

    parser.add_argument(
        "--max-retries",
        type=int,
        default=2
    )

    args = parser.parse_args()

    if not 1 <= args.duration <= 300:
        print(
            "Duration must be 1-300."
        )
        return

    if not 0 <= args.max_retries <= 10:
        print(
            "max-retries must be 0-10."
        )
        return

    create_job(
        args.duration,
        args.max_retries
    )


if __name__ == "__main__":
    main()