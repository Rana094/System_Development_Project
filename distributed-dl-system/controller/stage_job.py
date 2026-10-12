"""Stage received submissions or audit a committed input snapshot after a restart."""

import argparse
import json
from pathlib import Path

from job_storage import StorageError, receipt, stage_job, verify_workspace
from job_store import get_job, get_jobs_by_status


def main():
    """Operate on one job or retry all RECEIVED jobs without starting training."""
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--job-id",
        help="Stage one RECEIVED submission, or verify one STAGED submission.",
    )
    group.add_argument(
        "--pending", action="store_true", help="Stage all RECEIVED student submissions."
    )
    group.add_argument(
        "--verify",
        metavar="JOB_ID",
        help="Verify metadata and every source/dataset file hash.",
    )
    args = parser.parse_args()
    if args.verify:
        job = get_job(args.verify)
        if (
            job is None
            or job["status"] not in ("STAGED", "READY")
            or not job["storage_path"]
        ):
            parser.error("Verification requires a STAGED student submission")
        try:
            workspace = Path(job["storage_path"])
            manifest, _ = verify_workspace(workspace, job)
            print(
                json.dumps(
                    {"verified": True, **receipt(manifest, workspace, job["status"])},
                    indent=2,
                )
            )
        except (OSError, ValueError, KeyError, RuntimeError) as error:
            parser.error(str(error))
        return
    jobs = (
        [
            job["job_id"]
            for job in get_jobs_by_status("RECEIVED")
            if job["job_type"] == "custom"
        ]
        if args.pending
        else [args.job_id]
    )
    results = []
    for job_id in jobs:
        try:
            results.append(stage_job(job_id))
        except StorageError as error:
            results.append({"job_id": job_id, "error": str(error)})
    print(json.dumps(results, indent=2))
    if any("error" in result for result in results):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
