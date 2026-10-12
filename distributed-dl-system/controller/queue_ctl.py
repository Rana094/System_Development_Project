"""Inspect student queues and persist owner-scoped hold, release and cancellation requests."""
import argparse
import json

from job_spec import validate_owner
from job_store import control_job, fair_queue, get_all_jobs
from queue_policy import load_policy, limits


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action',choices=['list','hold','release','cancel','policy'])
    parser.add_argument('--owner')
    parser.add_argument('--job-id')
    parser.add_argument('--history',action='store_true')
    args = parser.parse_args()
    try:
        if args.owner:
            validate_owner(args.owner)
        if args.action == 'policy':
            policy = load_policy()
            print(json.dumps(limits(policy,args.owner) if args.owner else policy,indent=2))
        elif args.action == 'list':
            # Refresh wait explanations without reserving or dispatching anything.
            fair_queue()
            jobs = [j for j in get_all_jobs() if j['job_type']=='custom'
                    and (not args.owner or j['owner']==args.owner)
                    and (args.history or j['status'] not in ('COMPLETED','FAILED','CANCELLED'))]
            print(json.dumps([{'job_id':j['job_id'],'owner':j['owner'],'status':j['status'],
                'node':j['node_id'],'queued_at':j['queued_at'],'reason':j['queue_reason'],
                'last_error':j['last_error']} for j in jobs],indent=2))
        else:
            if not args.owner or not args.job_id:
                raise ValueError('hold, release and cancel require --owner and --job-id')
            state = control_job(args.job_id,args.owner,args.action)
            print(json.dumps({'job_id':args.job_id,'owner':args.owner,'status':state}))
    except ValueError as error:
        parser.exit(1,str(error)+'\n')


if __name__ == '__main__':
    main()
