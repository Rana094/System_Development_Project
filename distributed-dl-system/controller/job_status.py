"""Display persistent job state and the history of worker attempts."""

import sys
import json
from datetime import datetime

from job_store import get_job, get_all_jobs, get_attempts, get_job_environments, get_ranks


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
        if job.get('job_type') == 'custom' and job.get('storage_path'):
            if json.loads(job['job_config'])['execution']['mode']=='distributed':
                from group_checkpoints import metadata as checkpoint_metadata
            else:
                from custom_checkpoints import metadata as checkpoint_metadata
            try:
                checkpoint = checkpoint_metadata(job)
            except (OSError, ValueError) as error:
                print('Checkpoint validation:', error)
            else:
                if checkpoint:
                    print('Checkpoint step:', checkpoint['global_step'])
                    print('Checkpoint complete:', checkpoint['completed'])
                    print('Checkpoint SHA-256:', checkpoint['sha256'])
                    if checkpoint.get('kind')=='ddp':
                        print('Checkpoint type: consistent DDP group')
                        print('Checkpoint ID:',checkpoint['checkpoint_id'])
                        print('Rank acknowledgments:',len(checkpoint['rank_acknowledgments']), '/', checkpoint['world_size'])
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
    if job.get('job_type')=='custom' and json.loads(job['job_config'])['execution']['mode']=='distributed':
        from recovery_metrics import get_events
        for event in get_events(job_id):
            if event['event_type']=='DDP_RESUMED':
                details=json.loads(event['details_json'])
                print(f"DDP recovery: attempt={details['retry_count']+1} restored_step={details['global_step']} "
                      f"ranks={len(details['acknowledged_ranks'])}/{details['world_size']} "
                      f"source_attempt={details['attempt_token']}")
    from job_store import get_connection
    connection=get_connection()
    try:
        leases=connection.execute('SELECT * FROM execution_leases WHERE job_id=? ORDER BY retry_count,node_id',(job_id,)).fetchall()
        if leases:
            print('Execution ownership:')
            for lease in leases:
                print(f"  attempt={lease['retry_count']+1} node={lease['node_id']} generation={lease['generation']} "
                      f"renewal_blocked={bool(lease['renewal_blocked'])} fenced={bool(lease['revoked'])} "
                      f"expires={format_time(lease['expires_at'])}")
    finally:connection.close()
    ranks = get_ranks(job_id)
    if ranks:
        print('DDP ranks:')
        for rank in ranks:
            print(f"  attempt={rank['retry_count'] + 1} rank={rank['rank']} "
                  f"node={rank['node_id']} status={rank['status']} error={rank['error']}")


def main():
    """Choose the complete job list or one detailed job from command-line arguments."""
    if len(sys.argv) == 1:

        show_all()

    else:

        show_one(sys.argv[1])


if __name__ == "__main__":

    main()
