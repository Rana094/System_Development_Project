"""Compare final states and metrics from the supplied regression and classification examples."""
import argparse
import json
import io
import zipfile

from compare_regression_jobs import equal, load_result


def compare_ddp(baseline_id,recovered_id):
    """Compare full rank snapshots after equivalent input/model submissions."""
    from job_store import get_job,get_job_environments
    from group_checkpoints import metadata,path_for
    import torch
    jobs=[get_job(job_id) for job_id in (baseline_id,recovered_id)]
    specs=[json.loads(job['job_config']) for job in jobs]
    if not any(spec['execution']['mode']=='distributed' for spec in specs):
        return {}
    if (any(spec['execution']['mode']!='distributed' or spec['recovery']['mode']!='checkpoint' for spec in specs)
            or specs[0]['execution']!=specs[1]['execution'] or specs[0]['entrypoint']!=specs[1]['entrypoint']):
        raise ValueError('Compare matching checkpoint-enabled DDP submissions')
    records=[get_job_environments(job['job_id'])[0]['record'] for job in jobs]
    if any(records[0][key]!=records[1][key] for key in ('source_version','dataset_version','compatibility_key')):
        raise ValueError('DDP submissions have different source, dataset or environments')
    states=[]
    for job in jobs:
        info=metadata(job)
        if not info or not info['completed']:
            raise ValueError('Both DDP jobs need a validated final checkpoint')
        with zipfile.ZipFile(path_for(job)) as archive:
            states.append([torch.load(io.BytesIO(archive.read(f'rank-{rank}.pt')),
                                      map_location='cpu',weights_only=True) for rank in range(info['world_size'])])
    return {'exact_rank_checkpoint_states_match':equal(states[0],states[1]),
            'compared_ranks':len(states[0])}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('baseline_job_id')
    parser.add_argument('recovered_job_id')
    args = parser.parse_args()
    try:
        baseline_state, baseline = load_result(args.baseline_job_id)
        recovered_state, recovered = load_result(args.recovered_job_id)
        if baseline.get('task', 'regression') != recovered.get('task', 'regression'):
            raise ValueError('Compare two runs of the same training task')
        metrics = [key for key in ('final_mse', 'final_loss', 'training_accuracy', 'predictions')
                   if key in baseline]
        if not metrics:
            raise ValueError('No supported result metrics found')
        ddp = compare_ddp(args.baseline_job_id,args.recovered_job_id)
        states_match = equal(baseline_state, recovered_state)
        metrics_match = all(baseline[key] == recovered.get(key) for key in metrics)
    except (OSError, ValueError) as error:
        parser.exit(1, str(error) + '\n')
    passed = states_match and metrics_match and ddp.get('exact_rank_checkpoint_states_match',True)
    print(json.dumps({
        'baseline_job_id': args.baseline_job_id, 'recovered_job_id': args.recovered_job_id,
        'task': baseline.get('task', 'regression'),
        'baseline_resumed_from': baseline['resumed_from'],
        'recovered_resumed_from': recovered['resumed_from'],
        'exact_model_optimizer_scheduler_match': states_match,
        'same_metrics_and_predictions': metrics_match,
        **ddp,
        'pass': passed,
    }, indent=2))
    if not passed:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
