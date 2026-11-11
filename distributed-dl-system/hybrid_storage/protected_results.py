"""Collect fenced private outputs over existing SSH/rsync, never an input API.

A fixed root-owned bridge drops to the existing SSH operator before rsync. Its
bounded stdout archive is LOCAL IPC; no training, DB or key path is exported.
"""
import os
from pathlib import Path,PurePosixPath
import re
import subprocess
import tarfile
import tempfile

MAX_BYTES=128*1024**2
MAX_MEMBERS=8192


def unpack(stream,destination):
    """Exclusive bounded regular files only, no tar extraction semantics."""
    names=set();total=0
    with tarfile.open(fileobj=stream,mode='r|') as archive:
        for member in archive:
            p=PurePosixPath(member.name)
            if (len(names)>=MAX_MEMBERS or member.name in names or p.is_absolute() or '..' in p.parts
                    or str(p)!=member.name or not p.parts or p.parts[0] not in ('results','training.log','lease.json')
                    or not member.isfile() or member.size<0):raise ValueError('Unsafe result archive')
            if p.parts[0]!='results' and len(p.parts)!=1:raise ValueError('Unexpected control member')
            names.add(member.name);total+=member.size
            if total>MAX_BYTES:raise ValueError('Result byte bound exceeded')
            target=destination/str(p);target.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
            with archive.extractfile(member) as incoming,target.open('xb') as output:
                remaining=member.size
                while remaining:
                    chunk=incoming.read(min(1024**2,remaining))
                    if not chunk:raise ValueError('Truncated result')
                    output.write(chunk);remaining-=len(chunk)
                output.flush();os.fsync(output.fileno())
    if not {'training.log','lease.json'}<=names:raise ValueError('Missing result lease/log')


class ProtectedCollector:
    def __init__(self,bridge):self.bridge=Path(bridge)
    def __call__(self,job,rank=None):
        import execution_leases as ownership
        import json
        node=job['node_id'];jobid=job['job_id']
        if node not in ('pi02','pi03') or not re.fullmatch('[A-Za-z0-9_-]{1,128}',jobid):raise ValueError('Unsupported result identity')
        expected=ownership.current(job,node)
        workspace=Path(job['storage_path'])
        with tempfile.TemporaryDirectory(prefix='.protected-result-',dir=workspace) as tmp:
            stage=Path(tmp);raw=stage/'transfer.tar'
            # A file + timeout avoids unbounded pipe/EOF waits on a compromised peer.
            with raw.open('xb') as output:
                process=subprocess.Popen(['/usr/bin/sudo','-n','/usr/bin/python3','-I','-B',str(self.bridge),node,jobid,expected['lease_id']],
                    stdout=output,stderr=subprocess.PIPE)
                import time
                deadline=time.monotonic()+120
                try:
                    while process.poll() is None:
                        if time.monotonic()>deadline or raw.stat().st_size>MAX_BYTES+8*1024**2:raise ValueError('Result transfer exceeded bounds')
                        time.sleep(.05)
                    if process.returncode!=0:raise ValueError('Protected result bridge failed')
                finally:
                    if process.poll() is None:process.kill()
                    process.communicate(timeout=5)
            with raw.open('rb') as stream:unpack(stream,stage)
            record=json.loads((stage/'lease.json').read_bytes())
            observed=record['lease']
            for key in ('job_id','attempt_token','generation','lease_id','node_id'):
                if observed.get(key)!=expected[key]:raise PermissionError('Stale private result producer')
            ownership.current(job,node)
            results=stage/'results';results.mkdir(exist_ok=True)
            destination=workspace/'results' if rank is None else workspace/'results'/f'rank-{rank}'
            log=workspace/'logs'/('attempt-'+str(job['retry_count'])+('' if rank is None else f'-rank-{rank}')+'.log')
            with ownership.publication(job,node):
                destination.parent.mkdir(parents=True,exist_ok=True)
                previous=stage/'previous-results'
                if destination.exists():os.replace(destination,previous)
                try:os.replace(results,destination)
                except Exception:
                    if previous.exists():os.replace(previous,destination)
                    raise
                log.parent.mkdir(exist_ok=True);os.replace(stage/'training.log',log)
