"""Optional Matplotlib/Agg artifacts; never synthesize observations for gaps."""
import importlib.util
import os
import tempfile


def require_matplotlib():
    if importlib.util.find_spec('matplotlib') is None:
        raise ImportError('Charts need the optional controller reporting environment; no automatic package installation')


def render(target, data, rows, groups):
    require_matplotlib()
    previous = os.environ.get('MPLCONFIGDIR')
    with tempfile.TemporaryDirectory(prefix='storage-report-plot-cache-') as cache:
        os.environ['MPLCONFIGDIR'] = cache
        try:
            import matplotlib
            matplotlib.use('Agg')
            from matplotlib import pyplot as plt
            import numpy as np
            plotted = []; figures = []
            def save(fig, name):
                for suffix in ('png', 'svg'): fig.savefig(target / (name + '.' + suffix), dpi=160, bbox_inches='tight')
                plt.close(fig); figures.append(name)
            def point(row, metric, value, figure):
                plotted.append(dict(figure=figure, experiment_id=row['experiment_id'], record_id=row['record_id'], status=row['status'], metric=metric, value=value))

            # Keep each matrix experiment separate, including world size/dataset.
            for d in data:
                identity = d['metadata']['experiment_id']; selected = [r for r in rows if r['experiment_id'] == identity]
                if d['kind'] == 'training' and d['metadata']['schema_version'] == 2:
                    cohorts = sorted({(r['workload'], r['rows'], r['world_size'], r['dataset_sha256'], r['workload_sha256']) for r in selected})
                    if len(cohorts) > 24: continue  # Table/raw records still retained; don't create unreadable figures.
                    for suffix, metrics, labels, factors in [
                        ('timing', ('completion_seconds', 'staging_seconds'), ('Harness completion (s)', 'Application staging (s)'), (1, 1)),
                        ('cache', ('copied_payload_bytes', 'cache_hit_ratio'), ('Copied payload (KiB)', 'Byte cache-hit ratio (%)'), (1 / 1024, 100))]:
                        fig, axes = plt.subplots(len(cohorts), 2, figsize=(11, max(3, len(cohorts) * 2.55)), squeeze=False)
                        name = identity + '-' + suffix
                        for i, cohort in enumerate(cohorts):
                            group = [r for r in selected if (r['workload'], r['rows'], r['world_size'], r['dataset_sha256'], r['workload_sha256']) == cohort and r['status'] == 'COMPLETED']
                            for j, metric in enumerate(metrics):
                                ax = axes[i, j]
                                for x, (config, phase) in enumerate((c, p) for c in 'ABCD' for p in ('first', 'repeat')):
                                    values = [r for r in group if r['config'] == config and r['repeat_phase'] == phase and r['metric'][metric] is not None]
                                    color = '#24628e' if phase == 'first' else '#bc5a18'
                                    observed = [r['metric'][metric] * factors[j] for r in values]
                                    if observed:
                                        jitter = np.linspace(-0.07, 0.07, len(observed)) if len(observed) > 1 else [0]
                                        ax.scatter([x + offset for offset in jitter], observed, color=color, s=18, zorder=3)
                                        ax.scatter([x], [sum(observed) / len(observed)], color=color, marker='_', s=150, zorder=4)
                                        for r in values: point(r, metric, r['metric'][metric], name)
                                    else:
                                        ax.text(x, 0.03, 'N/A', transform=ax.get_xaxis_transform(), ha='center', fontsize=7)
                                ax.set_xticks(range(8), [f'{c}\n{p}' for c in 'ABCD' for p in ('first', 'repeat')], fontsize=7)
                                ax.set_ylabel(labels[j]); ax.set_title(f'{cohort[0]} • {cohort[1]} rows • {cohort[2]} rank(s)', fontsize=9)
                                maximum = max((r['metric'][metric] * factors[j] for r in group if r['metric'][metric] is not None), default=0)
                                ax.set_ylim(bottom=0, top=maximum * 1.15 if maximum else 1)
                                ax.grid(axis='y', alpha=0.2); ax.set_axisbelow(True)
                        scope = d['metadata']['scope']
                        fig.suptitle(f'{identity}\n{scope} • raw dots + mean ticks • uncontrolled/preverified kernel pages', fontsize=10)
                        fig.text(0.5, 0.005, 'A direct inputs; B fresh staging; C persistent/resource; D persistent/locality. Payload is not NFS wire bytes. No speedup inference.', ha='center', fontsize=7)
                        fig.tight_layout(rect=(0, 0.03, 1, 0.93)); save(fig, name)
                if d['kind'] == 'fault':
                    observed = [r for r in selected if r['status'] == 'PASS' and any(r['metric'][key] is not None for key in ('detection_seconds', 'staging_seconds', 'agent_restart_seconds'))]
                    if observed:
                        fig, axes = plt.subplots(1, 3, figsize=(12, max(3, len(observed) * 0.55)))
                        name = identity + '-fault-phases'
                        for ax, metric, label in zip(axes, ('detection_seconds', 'staging_seconds', 'agent_restart_seconds'), ('Detection (ms)', 'Staging/repair (ms)', 'Private agent restart (ms)')):
                            for i, row in enumerate(observed):
                                value = row['metric'][metric]
                                if value is not None: ax.scatter([value * 1000], [i], color='#24628e'); point(row, metric, value, name)
                            ax.set_yticks(range(len(observed)), [r['record_id'] for r in observed], fontsize=8)
                            ax.set_xlabel(label); ax.set_xlim(left=0); ax.grid(axis='x', alpha=0.2)
                        fig.suptitle(f'{identity}\nLocal disposable probes • one observation each • phase boundaries differ/overlap', fontsize=10)
                        fig.text(0.5, 0.01, 'Blank = unmeasured/not applicable. End-to-end training recovery is unmeasured. No physical Pi/NFS failure result.', ha='center', fontsize=8)
                        fig.tight_layout(rect=(0, 0.04, 1, 0.9)); save(fig, name)

            fig, ax = plt.subplots(figsize=(11, max(3, len(data) * 0.65)))
            for i, d in enumerate(data):
                selected = [r for r in rows if r['experiment_id'] == d['metadata']['experiment_id']]
                good = sum(r['status'] in ('PASS', 'COMPLETED') for r in selected); bad = len(selected) - good
                ax.barh(i, good, color='#24628e', label='Successful' if i == 0 else None)
                ax.barh(i, bad, left=good, color='#b4453c', label='Failed / timeout' if i == 0 else None)
                ax.text(len(selected) + 0.4, i, f'{good}/{len(selected)}', va='center', fontsize=8)
            ax.set_yticks(range(len(data)), [d['metadata']['experiment_id'] for d in data], fontsize=8)
            ax.set_xlabel('Recorded trials / cases (different cohorts are not pooled)'); ax.legend(loc='lower right')
            ax.set_title('Collected outcomes • local cohorts only • retained development failures included', fontsize=10)
            ax.set_xlim(0, max(len(d['records']) for d in data) * 1.15); fig.tight_layout(); save(fig, 'collected-outcomes')
            from . import runner
            runner.write_json(target / 'chart-data.json', dict(schema_version=1, figures=figures, points=plotted,
                outcome_counts=[dict(experiment_id=d['metadata']['experiment_id'], successful=sum(r['status'] in ('PASS', 'COMPLETED') for r in rows if r['experiment_id'] == d['metadata']['experiment_id']), attempted=len(d['records'])) for d in data],
                plotting_versions=dict(matplotlib=matplotlib.__version__, numpy=np.__version__),
                interpretation='Only recorded non-null successful observations. No zero imputation; means are descriptive; failed denominators retained.'))
        finally:
            if previous is None: os.environ.pop('MPLCONFIGDIR', None)
            else: os.environ['MPLCONFIGDIR'] = previous
