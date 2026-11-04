"""Fail-stop supervisor independent of Flask and the student's checkpoint calls."""
import argparse
import os
from pathlib import Path
import signal
import subprocess
import time
from execution_lease import read,alive,STOP_GRACE,clock


def clean_descendants():
    """Do not leave background children behind when the primary program exits."""
    group=os.getpgrp();deadline=clock()+STOP_GRACE
    while True:
        remaining=[]
        for path in Path('/proc').iterdir():
            if not path.name.isdigit() or int(path.name)==os.getpid():continue
            try:
                pid=int(path.name)
                if os.getpgid(pid)!=group:continue
                state=(path/'stat').read_text().rsplit(')',1)[1].split()[0]
                if state in ('Z','X'):continue
                os.kill(pid,signal.SIGKILL);remaining.append(pid)
            except (ProcessLookupError,FileNotFoundError):pass
        if not remaining:return
        if clock()>=deadline:raise RuntimeError('Training descendants did not terminate within the stop bound')
        time.sleep(0.05)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--lease-path',required=True)
    parser.add_argument('command',nargs=argparse.REMAINDER)
    args=parser.parse_args();command=args.command
    if command and command[0]=='--':command=command[1:]
    environment=dict(os.environ,DL_GUARDIAN_PID=str(os.getpid()),
                     DL_EXECUTION_LEASE_PATH=args.lease_path)
    process=None
    try:
        # The guardian and every submitted descendant share the agent-created
        # process group. Expiry kills that whole group, including the guardian.
        if not alive(read(args.lease_path)):raise ValueError('Lease expired before launch')
        process=subprocess.Popen(command,env=environment)
        while process.poll() is None:
            try:
                if not alive(read(args.lease_path)):raise ValueError('Lease expired')
            except (OSError,ValueError,KeyError) as error:
                print('EXECUTION_LEASE_EXPIRED: '+str(error),flush=True)
                os.killpg(os.getpgrp(),signal.SIGKILL)
            time.sleep(0.2)
        clean_descendants()
        return process.returncode
    finally:
        if process and process.poll() is None:process.kill();process.wait()


if __name__=='__main__':raise SystemExit(main())
