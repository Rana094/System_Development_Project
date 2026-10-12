"""Queue a prepared single-worker submission for resource-aware dispatch."""
import argparse
import json
from pathlib import Path

from job_storage import verify_workspace
from job_store import enqueue_custom_job, get_job
from scheduler import load_nodes


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--job-id', required=True)
    args = parser.parse_args()
    try:
        job = get_job(args.job_id)
        if not job or not job.get('storage_path'):
            raise ValueError('Unknown staged job')
        verify_workspace(Path(job['storage_path']), job)
        enqueue_custom_job(args.job_id, {node['id'] for node in load_nodes()})
    except ValueError as error:
        parser.exit(1, str(error) + '\n')
    print(json.dumps({'job_id':args.job_id,'status':'QUEUED'}))


if __name__ == '__main__':
    main()
