"""Display persistent job state and the history of worker attempts."""

import sys
import json
from datetime import datetime

from job_store import get_job, get_all_jobs, get_attempts, get_job_environments


def format_time(value):
    """Render an optional stored Unix timestamp for the terminal display."""
    if value is None:
        return "-"

    return datetime.fromtimestamp(value).strftime("%H:%M:%S")


def show_all():
    """Print a compact queue and running-job summary."""
    jobs = get_all_jobs()

    if not jobs:

        print("No jobs found.")

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
    """Print one job and all its attempts to explain migration and retries."""
    job = get_job(job_id)

    if job is None:

        print("Job not found.")

        return

    print()

    print("Job ID:", job["job_id"])

    print("Status:", job["status"])

    if job.get("job_type") == "custom":
        print("Owner:", job["owner"])
        print("Submission:")
        print(json.dumps(json.loads(job["job_config"]), indent=2))
        print(
            "Execution: waiting for input staging"
            if job["status"] == "RECEIVED"
            else (
                "Execution: READY; use controller/queue_job.py --job-id to enqueue"
                if job["status"] == "READY"
                else ("Execution: waiting for environment preparation" if job["status"] == "STAGED" else "Execution: " + job["status"])
            )
        )
        if job.get("storage_path"):
            print("Storage:", job["storage_path"])
            print("Manifest SHA-256:", job["storage_manifest_sha256"])
        for prepared in get_job_environments(job_id):
            record = prepared["record"]
            print(
                "Prepared worker:",
                prepared["node_id"],
                "profile=",
                record["profile"],
                "python=",
                record["runtime"]["python"],
                "architecture=",
                record["runtime"]["machine"],
            )
            print("Environment interpreter:", record["python_path"])

    if job.get('checkpoint_path'):
        print('Central checkpoint:', job['checkpoint_path'])
        print('Checkpoint size:', job['checkpoint_size_bytes'], 'bytes')
    print("Queue reason:", job.get("queue_reason"))
    print("Node:", job["node_id"])

    print("Node IP:", job["node_ip"])

    print("Duration:", job["duration"])

    print("Created:", format_time(job["created_at"]))

    print("Started:", format_time(job["started_at"]))

    print("Finished:", format_time(job["finished_at"]))

    print("Return code:", job["return_code"])

    print("Error:", job["error"])

    attempts = get_attempts(job_id)

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
    """Choose the complete job list or one detailed job from command-line arguments."""
    if len(sys.argv) == 1:

        show_all()

    else:

        show_one(sys.argv[1])


if __name__ == "__main__":

    main()
