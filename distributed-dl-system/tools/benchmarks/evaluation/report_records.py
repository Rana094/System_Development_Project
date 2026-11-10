"""Bounded offline readers. No DB/torch deserialization or source execution."""
import csv
import hashlib
import io
import json
import math
import os
from pathlib import Path
import re
import stat
import time

from . import runner
from hybrid_storage.backends import _directory, _parts
from hybrid_storage.mount_identity import require_local_fd

LIMIT = 32 * 1024 * 1024
SCOPES = {'controller_local_smoke', 'local_component_rehearsal', 'single_host_nfs_component_comparison', 'local_disposable_failure_rehearsal'}


def strict_json(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result: raise ValueError('Duplicate JSON key')
            result[key] = value
        return result
    def finite_float(value):
        result = float(value)
        if not math.isfinite(result): raise ValueError('Nonfinite JSON number')
        return result
    try:
        return json.loads(raw, object_pairs_hook=pairs, parse_float=finite_float, parse_constant=lambda _: (_ for _ in ()).throw(ValueError('Nonfinite JSON')))
    except (RecursionError, UnicodeError) as error:
        raise ValueError('Invalid bounded JSON') from error


def read(path, catalog, base, *, limit=LIMIT):
    _parts(str(path.relative_to(base)))
    with _directory(path.parent) as parent:
        require_local_fd(parent)
        fd = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
        try:
            before = os.fstat(fd)
            if not stat.S_ISREG(before.st_mode) or before.st_size > limit: raise ValueError('Unsafe or oversized input file')
            with os.fdopen(fd, 'rb', closefd=False) as stream: raw = stream.read(limit + 1)
            after = os.fstat(fd)
            if len(raw) > limit or (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns):
                raise ValueError('Input changed while reading')
        finally: os.close(fd)
    catalog[str(path.relative_to(base))] = dict(bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest())
    return raw


def number(value, *, integer=False):
    if value is None: return None
    if type(value) not in ((int,) if integer else (int, float)) or not 0 <= value < 2**63 or not math.isfinite(value):
        raise ValueError('Invalid measured numeric value')
    return value


def disk_snapshot(folder):
    """Current retained files only. No links followed, deleted data inferred or DB opened."""
    seen = set(); logical = allocated = entries = checkpoints = skipped = 0
    started = time.time_ns()
    for directory, dirs, files in os.walk(folder, followlinks=False):
        kept = []
        for name in dirs:
            if (Path(directory) / name).is_symlink(): skipped += 1
            else: kept.append(name)
        dirs[:] = kept
        for name in files:
            path = Path(directory) / name
            info = path.lstat()
            if not stat.S_ISREG(info.st_mode): skipped += 1; continue
            entries += info.st_size
            identity = (info.st_dev, info.st_ino)
            if identity in seen: continue
            seen.add(identity); logical += info.st_size; allocated += info.st_blocks * 512
            if path.suffix in ('.pt', '.dlckpt'): checkpoints += info.st_size
    return dict(observed_at_utc_ns=started, logical_unique_file_bytes=logical, path_entry_bytes=entries,
        allocated_unique_file_bytes=allocated, retained_checkpoint_model_bytes=checkpoints,
        unique_regular_files=len(seen), skipped_links_or_special=skipped,
        scope='current retained report tree; not peak cache/disk use or authoritative/server storage; not an atomic filesystem snapshot')


def load(folder):
    folder = Path(folder)
    if not folder.is_absolute() or folder == Path('/') or '..' in folder.parts: raise ValueError('Canonical local report directory required')
    with _directory(folder) as fd: require_local_fd(fd)
    catalog = {}
    def document(name):
        result = strict_json(read(folder / name, catalog, folder))
        if type(result) is not dict: raise ValueError('Expected JSON object')
        return result
    metadata = document('experiment.json'); completion = document('completion.json')
    identity = metadata.get('experiment_id')
    if type(identity) is not str or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,79}', identity): raise ValueError('Invalid experiment identity')
    scope = metadata.get('scope')
    if scope not in SCOPES or completion.get('scope') != scope or completion.get('experiment_id') != identity: raise ValueError('Mixed metadata identity/scope')
    schema = metadata.get('schema_version')
    if type(schema) is not int or schema not in (1, 2): raise ValueError('Unsupported schema')
    kind = 'fault' if scope == 'local_disposable_failure_rehearsal' else 'training'
    if (kind == 'fault' and schema != 1) or (scope == 'local_component_rehearsal' and schema != 2): raise ValueError('Scope/schema mismatch')
    stem = 'faults' if kind == 'fault' else 'trials'
    raw = read(folder / (stem + '.jsonl'), catalog, folder)
    lines = raw.splitlines()
    if not lines or len(lines) > 10000 or any(len(line) > 4 * 1024 * 1024 for line in lines): raise ValueError('Invalid record count/size')
    records = [strict_json(line) for line in lines]
    table = csv.DictReader(io.StringIO(read(folder / (stem + '.csv'), catalog, folder).decode('utf-8')))
    if not table.fieldnames or len(table.fieldnames) != len(set(table.fieldnames)): raise ValueError('Invalid CSV header')
    flat = list(table)
    if len(flat) != len(records): raise ValueError('CSV/JSON count mismatch')
    seen = set()
    for row, csv_row in zip(records, flat):
        if type(row) is not dict or row.get('scope') != scope: raise ValueError('Mixed record scope')
        name = row.get('case' if kind == 'fault' else 'trial_id')
        pattern = r'[a-z][a-z0-9_]{0,63}' if kind == 'fault' else r'trial-[0-9]{4,6}'
        if type(name) is not str or not re.fullmatch(pattern, name) or name in seen: raise ValueError('Invalid/duplicate record ID')
        seen.add(name)
        if kind == 'training' and row.get('experiment_id') != identity: raise ValueError('Mixed trial experiment')
        result = document(name + '/result.json')
        if result != row: raise ValueError('Per-case/JSONL mismatch')
        for key, value in csv_row.items():
            if key not in row or value != ('' if row[key] is None else str(row[key])): raise ValueError('CSV/JSON value mismatch')
        if row.get('status') not in (('PASS', 'FAILED', 'TIMEOUT') if kind == 'fault' else ('COMPLETED', 'FAILED', 'TIMEOUT')): raise ValueError('Unknown status')
        code = row.get('exit_code')
        if code is not None and type(code) is not int: raise ValueError('Invalid exit code')
        if row['status'] in ('COMPLETED', 'PASS') and code != 0: raise ValueError('Successful record without zero exit')
        for key, value in row.items():
            if key.endswith(('_ns', '_bytes')): number(value, integer=True)
        if kind == 'training':
            if type(row.get('schema_version')) is not int or row['schema_version'] != schema: raise ValueError('Mixed record schema')
            if row.get('workload') not in ('regression', 'cnn', 'ddp'): raise ValueError('Unknown workload')
            if type(row.get('world_size')) is not int or not 1 <= row['world_size'] <= 3: raise ValueError('Unsupported rank count')
            for key in ('dataset_version', 'rows' if schema == 2 else 'dataset_rows'):
                if type(row.get(key)) is not int or not 1 <= row[key] <= 1000000: raise ValueError('Invalid dataset version/size')
            for key in ('dataset_sha256', 'workload_sha256'):
                if type(row.get(key)) is not str or not re.fullmatch('[0-9a-f]{64}', row[key]): raise ValueError('Invalid immutable identity')
            if schema == 2 and (row.get('config') not in ('A', 'B', 'C', 'D') or row.get('repeat_phase') not in ('first', 'repeat')): raise ValueError('Invalid matrix cell')
            ranks = row.get('ranks')
            if type(ranks) is not list or len(ranks) > row['world_size']: raise ValueError('Invalid rank evidence')
            rank_ids = set()
            for rank in ranks:
                number_id = rank.get('rank')
                if type(number_id) is not int or not 0 <= number_id < row['world_size'] or number_id in rank_ids: raise ValueError('Invalid/duplicate rank ID')
                rank_ids.add(number_id)
                for key, leaf in [('runtime', 'runtime.json'), ('summary', 'output/summary.json')]:
                    if rank.get(key) is not None and document(f'{name}/ranks/rank-{number_id}/{leaf}') != rank[key]: raise ValueError('Rank evidence mismatch')
            if row['status'] == 'COMPLETED' and not runner.successful(ranks, row['world_size']): raise ValueError('Incomplete successful rank evidence')
    sources = metadata.get('sources')
    if type(sources) is not dict or not 1 <= len(sources) <= 2048: raise ValueError('Missing bounded source identity')
    for name, digest in sources.items():
        _parts(name)
        if type(digest) is not str or not re.fullmatch('[0-9a-f]{64}', digest): raise ValueError('Invalid source hash')
        source = read(folder / 'source' / name, catalog, folder, limit=1024 * 1024)
        if hashlib.sha256(source).hexdigest() != digest: raise ValueError('Frozen source hash mismatch')
    return dict(path=str(folder), metadata=metadata, completion=completion, records=records, kind=kind,
        provenance=catalog, disk=disk_snapshot(folder))
