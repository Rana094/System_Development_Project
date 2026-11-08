"""Stage worker outputs and publish only while their execution still owns the job."""
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import requests
from scheduler import load_nodes
import execution_leases as ownership


def collect(job,rank=None):
    node=next(n for n in load_nodes() if n['id']==job['node_id'])
    workspace=Path(job['storage_path'])
    destination=workspace/'results' if rank is None else workspace/'results'/f'rank-{rank}'
    row=ownership.current(job,node['id'])
    completed=True
    if row:
        observed=requests.get(f"http://{node['ip']}:{node['port']}/jobs/{job['job_id']}",timeout=3)
        observed.raise_for_status();worker=observed.json()['job']
        if not ownership.matches(job,node['id'],worker):raise ValueError('FENCED: stale result producer')
        completed=worker['status']=='COMPLETED'
    suffix='' if rank is None else f'-rank-{rank}'
    log_path=workspace/'logs'/('attempt-'+str(job['retry_count'])+suffix+'.log')
    with tempfile.TemporaryDirectory(prefix='.artifact-stage-',dir=workspace) as temporary:
        stage=Path(temporary);results=stage/'results';results.mkdir()
        if completed:
            remote=node.get('worker_root','distributed-dl-system')+'/worker-storage/jobs/'+job['job_id']+'/results/'
            subprocess.run(['rsync','-rt','--no-links','--no-specials','--no-devices','--protect-args',
                            '-e','ssh -o BatchMode=yes -o ConnectTimeout=5',node['id']+':'+remote,str(results)+'/'],
                           check=True,capture_output=True,text=True,timeout=120)
        with requests.get(f"http://{node['ip']}:{node['port']}/jobs/{job['job_id']}/log",
                          params=ownership.params(job,node['id']),stream=True,timeout=10) as response:
            response.raise_for_status();ownership.response(job,node['id'],response.headers)
            with (stage/'training.log').open('wb') as output:
                for chunk in response.iter_content(1024*1024):output.write(chunk)
                output.flush();os.fsync(output.fileno())
        # This second check fences transfers that began before a newer attempt.
        ownership.current(job,node['id'])
        with ownership.publication(job,node['id']):
            if completed:
                destination.parent.mkdir(parents=True,exist_ok=True)
                previous=stage/'previous-results'
                if destination.exists():os.replace(destination,previous)
                try:os.replace(results,destination)
                except Exception:
                    if previous.exists():os.replace(previous,destination)
                    raise
            os.replace(stage/'training.log',log_path)
