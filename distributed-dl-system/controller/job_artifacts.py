"""Collect completed custom-job outputs into their controller-owned workspace."""
import os
from pathlib import Path
import subprocess

import requests
from scheduler import load_nodes


def collect(job):
    node = next(node for node in load_nodes() if node['id'] == job['node_id'])
    workspace = Path(job['storage_path'])
    # Copy regular files and directories only. Student-created symlinks and special
    # files must not cause collection to read outside the worker's results folder.
    remote = node.get('worker_root','distributed-dl-system') + '/worker-storage/jobs/' + job['job_id'] + '/results/'
    subprocess.run(['rsync','-rt','--no-links','--no-specials','--no-devices','--protect-args',
                    '-e','ssh -o BatchMode=yes -o ConnectTimeout=5',
                    node['id'] + ':' + remote,str(workspace / 'results') + '/'],
                   check=True,capture_output=True,text=True,timeout=120)
    path = workspace / 'logs' / ('attempt-' + str(job['retry_count']) + '.log')
    temporary = path.with_suffix('.tmp')
    try:
        with requests.get(f"http://{node['ip']}:{node['port']}/jobs/{job['job_id']}/log",
                          stream=True,timeout=10) as response:
            response.raise_for_status()
            with temporary.open('wb') as output:
                for chunk in response.iter_content(1024 * 1024):
                    output.write(chunk)
                output.flush()
                os.fsync(output.fileno())
        os.replace(temporary,path)
    finally:
        temporary.unlink(missing_ok=True)
