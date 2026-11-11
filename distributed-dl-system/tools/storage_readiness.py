"""Read-only NFS probe or explicit new LOCAL SQLite recovery-point verification."""
import argparse
from pathlib import Path
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))


def worker_program():
    """Produce reviewed stdin program; no remote file installation is needed."""
    from hybrid_storage.deployment_review import _read
    code='import sys, types\np=types.ModuleType("hybrid_storage");p.__path__=[];sys.modules["hybrid_storage"]=p\n'
    for module in ('nfs_mount','readiness_probe'):
        raw=_read(ROOT,'hybrid_storage/'+module+'.py').decode()
        name='hybrid_storage.'+module
        code+='m=types.ModuleType('+repr(name)+');m.__package__="hybrid_storage";sys.modules['+repr(name)+']=m\n'
        code+='exec(compile('+repr(raw)+','+repr(name)+',"exec"),m.__dict__)\n'
    return code+'m.main()\n'


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    sub=parser.add_subparsers(dest='command',required=True)
    sub.add_parser('print-worker-program',help='Print portable stdlib-only read-only probe; execute only reviewed commands')
    probe=sub.add_parser('probe');probe.add_argument('args',nargs=argparse.REMAINDER)
    backup=sub.add_parser('backup-db');backup.add_argument('--source',type=Path,required=True);backup.add_argument('--output',type=Path,required=True)
    args=parser.parse_args(argv)
    if args.command=='print-worker-program':print(worker_program(),end='')
    elif args.command=='probe':
        from hybrid_storage.readiness_probe import main as run
        run(args.args[1:] if args.args[:1]==['--'] else args.args)
    else:
        from hybrid_storage.state_backup import main as run
        run(['--source',str(args.source),'--output',str(args.output)])


if __name__=='__main__':main()
