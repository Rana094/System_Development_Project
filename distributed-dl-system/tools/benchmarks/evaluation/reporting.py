"""Offline measured metrics, cohort-safe summaries and optional static charts."""
import csv
import hashlib
import json
from pathlib import Path
import statistics
import time

from . import report_records as records, runner

UNITS = dict(completion_seconds='seconds', training_seconds='seconds', staging_seconds='seconds',
    cache_hit_bytes='bytes', copied_payload_bytes='bytes', cache_hit_ratio='ratio', time_to_first_step_seconds='seconds',
    nfs_wire_bytes='bytes', checkpoint_save_seconds='seconds', checkpoint_restore_seconds='seconds', recovery_seconds='seconds',
    server_bandwidth_bytes_per_second='bytes/second', detection_seconds='seconds', agent_restart_seconds='seconds', harness_seconds='seconds')
MISSING = dict(time_to_first_step_seconds='No timestamped first training-step event.', nfs_wire_bytes='No NFS client/server wire counters; copied payload is a different quantity.',
    checkpoint_save_seconds='No isolated checkpoint save timer.', checkpoint_restore_seconds='No isolated checkpoint restore timer.',
    recovery_seconds='No instrumented end-to-end recovered-training boundary.', server_bandwidth_bytes_per_second='No sampled server interface counters.')


def collect(inputs):
    if not inputs or len(inputs) > 32: raise ValueError('Use1..32 explicit input experiments')
    paths = [Path(p) for p in inputs]
    for i, path in enumerate(paths):
        if any(path == p or path.is_relative_to(p) or p.is_relative_to(path) for p in paths[:i]): raise ValueError('Duplicate/overlapping input trees')
    data = [records.load(p) for p in paths]
    if len({d['metadata']['experiment_id'] for d in data}) != len(data): raise ValueError('Duplicate experiment identity')
    return data


def normalize(experiment, row):
    meta = experiment['metadata']; fault = experiment['kind'] == 'fault'
    metric = {key: None for key in UNITS}
    origin = {key: MISSING.get(key, 'Not recorded/applicable for this input kind.') for key in UNITS}
    def put(key, value, description):
        metric[key] = records.number(value); origin[key] = description
    def seconds(key, field, description):
        if row.get(field) is not None: put(key, records.number(row[field], integer=True) / 1e9, description)
    if fault:
        for key, field in [('detection_seconds', 'detection_ns'), ('staging_seconds', 'staging_ns'), ('agent_restart_seconds', 'restart_ns'), ('recovery_seconds', 'recovery_ns'), ('harness_seconds', 'harness_elapsed_ns')]:
            seconds(key, field, row.get('metric_scope', 'Recorded fault phase.'))
    else:
        seconds('completion_seconds', 'total_ns' if row['schema_version'] == 2 else 'elapsed_ns', 'Measured harness wall time; includes launch/collection; not training-only.')
        seconds('staging_seconds', 'staging_ns', 'Measured staging wall time; A deliberately has no staging operation.')
        ranks = row.get('ranks', [])
        times = [rank.get('summary', {}).get('timing', {}).get('training_loop_seconds') for rank in ranks if rank.get('summary')]
        if len(times) == row['world_size'] and all(value is not None for value in times):
            put('training_seconds', max(records.number(value) for value in times), 'Maximum recorded per-rank training-loop seconds; includes checkpoint work inside that loop.')
        if row.get('training_loop_seconds') is not None:
            value = records.number(row['training_loop_seconds'])
            if metric['training_seconds'] is not None and value != metric['training_seconds']: raise ValueError('Top-level/rank training timing mismatch')
        for key, field in [('cache_hit_bytes', 'cache_hit_bytes'), ('copied_payload_bytes', 'transferred_bytes')]:
            if row.get(field) is not None: put(key, row[field], 'Accepted application payload bytes across staged workers; never NFS wire traffic.')
        hit, copied = metric['cache_hit_bytes'], metric['copied_payload_bytes']
        if hit is not None and copied is not None and hit + copied > 0:
            put('cache_hit_ratio', hit / (hit + copied), 'Derived byte hit ratio=hit/(hit+accepted copied bytes); includes source/requirements/data per worker.')
    workload = row.get('workload'); config = row.get('config')
    return dict(experiment_id=meta['experiment_id'], record_id=row.get('case', row.get('trial_id')), kind=experiment['kind'], scope=meta['scope'],
        status=row['status'], workload=workload, rows=row.get('rows', row.get('dataset_rows')), world_size=row.get('world_size'),
        config=config, repeat_phase=row.get('repeat_phase'), application_cache_state=row.get('application_cache_state', row.get('cache_state')),
        dataset_id=row.get('dataset_id'), dataset_version=row.get('dataset_version'), dataset_sha256=row.get('dataset_sha256'), workload_sha256=row.get('workload_sha256'),
        source_set_sha256=runner.value_hash(meta['sources']), host=meta['local_host'].get('hostname'),
        metric=metric, metric_origin=origin)


def summary(rows):
    groups = {}
    for row in rows:
        # Never pool separate experiments, sizes/worlds, immutable inputs, scopes,
        # workloads, source versions, policies or paired cache phases.
        keys = ('experiment_id', 'kind', 'scope', 'workload', 'rows', 'world_size', 'config', 'repeat_phase',
                'dataset_id', 'dataset_version', 'dataset_sha256', 'workload_sha256', 'source_set_sha256', 'host')
        if row['kind'] == 'fault': keys += ('record_id',)
        identity = {k: row[k] for k in keys}; key = runner.value_hash(identity)
        groups.setdefault(key, (identity, []))[1].append(row)
    result = []
    for key, (identity, values) in sorted(groups.items()):
        good = [r for r in values if r['status'] in ('PASS', 'COMPLETED')]
        metrics = {}
        for name in UNITS:
            samples = [r['metric'][name] for r in good if r['metric'][name] is not None]
            metrics[name] = dict(unit=UNITS[name], observed=len(samples), missing_successful=len(good) - len(samples), samples=samples,
                mean=statistics.mean(samples) if samples else None, median=statistics.median(samples) if samples else None,
                minimum=min(samples) if samples else None, maximum=max(samples) if samples else None,
                stdev=statistics.stdev(samples) if len(samples) > 1 else None)
        result.append(dict(group_id=key, identity=identity, attempted=len(values), successful=len(good), failed=len(values) - len(good), metrics=metrics))
    return result


def markdown(data, rows, groups):
    lines = ['# Collected storage evaluation', '',
        'Offline report from measured records. Local rehearsals and modeled failures are not physical Pi/NFS results.', '',
        'No speedup, physical reliability, server throughput or total-completion prediction is inferred. Kernel/NFS pages were uncontrolled/preverified in the application-cache matrix.', '',
        '## Cohorts', '', '| Experiment | Scope | Successful / attempted | Failed / timeout |', '|---|---|---:|---:|']
    for d in data:
        selected = [r for r in rows if r['experiment_id'] == d['metadata']['experiment_id']]
        good = sum(r['status'] in ('PASS', 'COMPLETED') for r in selected)
        lines.append(f"| {d['metadata']['experiment_id']} | {d['metadata']['scope']} | {good}/{len(selected)} | {len(selected)-good} |")
    lines += ['', '## Measurement coverage', '', '| Metric | Unit | Observed records / all records | Missing reason / interpretation |', '|---|---|---:|---|']
    for key, unit in UNITS.items():
        count = sum(r['metric'][key] is not None for r in rows)
        description = MISSING.get(key, 'See per-record metric_origin; different scopes/kinds stay separate.')
        lines.append(f'| {key} | {unit} | {count}/{len(rows)} | {description} |')
    lines += ['', '## Retained disk observation', '', '| Experiment | Logical unique bytes | Allocated unique bytes | Retained model/checkpoint bytes |', '|---|---:|---:|---:|']
    for d in data:
        disk = d['disk']; lines.append(f"| {d['metadata']['experiment_id']} | {disk['logical_unique_file_bytes']} | {disk['allocated_unique_file_bytes']} | {disk['retained_checkpoint_model_bytes']} |")
    lines += ['', 'Disk values are a current file-metadata observation of each retained experiment tree, not historical peak cache/storage or authoritative/server use. Hardlinked inodes count once; symlinks/special files are skipped and counted. Live DBs are never opened.', '',
        '## Interpreting the output', '',
        '- metrics.json/csv retain per-record values and missing values. coverage.csv explains every field; blank is unknown, never zero.',
        '- summary.json includes raw successful samples, n, mean, median, min/max and sample standard deviation; failures remain in the denominator. Null stdev for n<2. No confidence/speedup inference from these small dependent trials.',
        '- Training time is maximum per-rank loop time, not process duration or sum across ranks. It can include checkpoint work.',
        '- Accepted copied payload and byte cache-hit ratio are application counters, not NFS traffic or Linux page-cache hit rate. Config A has unknown byte counters, not zero reads.',
        '- Fault detection/staging/agent-restart boundaries differ and may overlap. Whole-case duration is not recovery latency.',
        '- Historical frozen source hashes are verified; no source is imported/executed. Each selected cohort retains its original version, host and scope.',
        '- Physical A/B/C/D comparisons and live faults are pending the previously required idle window/approval/isolation. pi01 remains deferred. Central controller/NFS remains a single point of failure.',
        '- Source input catalogs and snapshots in provenance.json make every plotted observation traceable. Charts show only non-null measured/derived values and label their local scope.', '']
    return '\n'.join(lines)


def write(data, output_root, report_id, *, charts=False):
    runner.Settings(report_id).validate()
    if type(charts) is not bool: raise ValueError('Expected explicit chart flag')
    target = Path(output_root) / report_id
    if any(target.is_relative_to(Path(d['path'])) or Path(d['path']).is_relative_to(target) for d in data): raise ValueError('Output overlaps an input experiment')
    plotter = None
    if charts:
        # Resolve dependency before any report directory is created. No automatic
        # install, network, worker code or production training-env modification.
        from . import report_charts as plotter
        plotter.require_matplotlib()
    rows = [normalize(d, r) for d in data for r in d['records']]; groups = summary(rows)
    target = runner.make_root(output_root, report_id)
    runner.write_json(target / 'metrics.json', dict(schema_version=1, records=rows))
    runner.write_json(target / 'summary.json', dict(schema_version=1, groups=groups))
    provenance = dict(schema_version=1, report_id=report_id, generated_at_utc_ns=time.time_ns(), inputs=[dict(path=d['path'], metadata=d['metadata'],
        completion=d['completion'], verified_files=d['provenance'], disk_observation=d['disk']) for d in data],
        scope='offline postprocessing only; zero new training, faults or physical measurements',
        reporting_sources={name: hashlib.sha256(runner.read_source(name)).hexdigest() for name in
            ('tools/benchmarks/storage_evaluation_report.py', 'tools/benchmarks/evaluation/report_records.py', 'tools/benchmarks/evaluation/reporting.py', 'tools/benchmarks/evaluation/report_charts.py', 'tools/benchmarks/evaluation/reporting-requirements.txt')})
    runner.write_json(target / 'provenance.json', provenance)
    with runner.new_file(target / 'metrics.csv') as stream:
        fields = ['experiment_id', 'record_id', 'kind', 'scope', 'status', 'workload', 'rows', 'world_size', 'config', 'repeat_phase', *UNITS]
        writer = csv.DictWriter(stream, fieldnames=fields); writer.writeheader()
        for row in rows: writer.writerow({**{k: row[k] for k in fields if k not in UNITS}, **row['metric']})
    with runner.new_file(target / 'coverage.csv') as stream:
        writer = csv.DictWriter(stream, fieldnames=['experiment_id', 'record_id', 'metric', 'unit', 'value', 'origin']); writer.writeheader()
        for row in rows:
            for key, unit in UNITS.items(): writer.writerow(dict(experiment_id=row['experiment_id'], record_id=row['record_id'], metric=key, unit=unit, value=row['metric'][key], origin=row['metric_origin'][key]))
    with runner.new_file(target / 'report.md') as stream: stream.write(markdown(data, rows, groups))
    if plotter is not None: plotter.render(target, data, rows, groups)
    files = {p.name: dict(bytes=p.stat().st_size, sha256=hashlib.sha256(p.read_bytes()).hexdigest()) for p in target.iterdir() if p.is_file()}
    runner.write_json(target / 'files.json', dict(schema_version=1, files=files))
    return target
