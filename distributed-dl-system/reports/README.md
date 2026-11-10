# Evaluation artifacts

The Phase13 runner writes a new private experiment directory here only with an
explicit `--run`. Reusing an experiment ID fails instead of overwriting data.
Generated subdirectories are ignored by Git and retained locally; they include
raw CSV/JSONL, metadata, source snapshots, synthetic datasets, logs, model outputs
and local SDK checkpoints. No cleanup command deletes older runs.

Follow [the methodology and exact commands](../docs/storage/13-experiments.md).
Phase13.1 records local CPU smoke execution only. It does not compare NFS/cache
policies, exercise the live scheduler or establish physical Pi performance.
