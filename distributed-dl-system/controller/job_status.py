import sys
from datetime import datetime

from job_store import (
    get_job,
    get_all_jobs,
    get_attempts
)


def format_time(value):

    if value is None:
        return "-"

    return datetime.fromtimestamp(
        value
    ).strftime(
        "%H:%M:%S"
    )


def show_all():

    jobs = get_all_jobs()

    if not jobs:

        print(
            "No jobs found."
        )

        return


    print()

    print(
        f"{'JOB ID':<32}"
        f"{'STATUS':<12}"
        f"{'NODE':<8}"
        f"{'DURATION':<10}"
        f"{'CREATED':<10}"
        f"{'STARTED':<10}"
        f"{'FINISHED':<10}"
        f"{'RETRY':<8}"
    )

    print("-" * 92)


    for job in jobs:

        print(
            f"{job['job_id']:<32}"
            f"{job['status']:<12}"
            f"{(job['node_id'] or '-'):<8}"
            f"{job['duration']:<10}"
            f"{format_time(job['created_at']):<10}"
            f"{format_time(job['started_at']):<10}"
            f"{format_time(job['finished_at']):<10}"
            f"{job['retry_count']:<8}"
        )


def show_one(job_id):

    job = get_job(
        job_id
    )

    if job is None:

        print(
            "Job not found."
        )

        return


    print()

    print(
        "Job ID:",
        job["job_id"]
    )

    print(
        "Status:",
        job["status"]
    )

    print(
        "Node:",
        job["node_id"]
    )

    print(
        "Node IP:",
        job["node_ip"]
    )

    print(
        "Duration:",
        job["duration"]
    )

    print(
        "Created:",
        format_time(
            job["created_at"]
        )
    )

    print(
        "Started:",
        format_time(
            job["started_at"]
        )
    )

    print(
        "Finished:",
        format_time(
            job["finished_at"]
        )
    )

    print(
        "Return code:",
        job["return_code"]
    )

    print(
        "Error:",
        job["error"]
    )

    attempts = get_attempts(
        job_id
    )

    print()
    print("Attempts:")

    for attempt in attempts:

        print(
            f"  #{attempt['attempt_number']} "
            f"node={attempt['node_id']} "
            f"status={attempt['status']} "
            f"error={attempt['error']}"
        )


def main():

    if len(sys.argv) == 1:

        show_all()

    else:

        show_one(
            sys.argv[1]
        )


if __name__ == "__main__":

    main()
