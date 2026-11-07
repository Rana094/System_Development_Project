# Built-in and reference workloads

Most student jobs run uploaded code or a trainer from [examples/](../examples/README.md).
This folder contains the controller-supported built-in workloads and fixed
reference snapshots used for system demonstrations and comparisons.

| File | Role | Needed for ordinary uploaded jobs? |
|---|---|---|
| [demo_job.py](demo_job.py) | Progress-only dummy job with legacy JSON checkpoints | No; scheduling smoke workload |
| [pytorch_job.py](pytorch_job.py) | Deterministic CPU MLP with model/Adam checkpoints and mid-epoch resume | No; built-in PyTorch recovery demonstrations |
| [evaluate_checkpoint.py](evaluate_checkpoint.py) | Evaluate the built-in MLP checkpoint on synthetic data | No; analysis utility |
| [benchmark_job.py](benchmark_job.py) | Adapter that launches fixed reference scripts and captures results | No; benchmark jobs |
| [reference/](reference/README.md) | Original reference benchmark, full-training and inference scripts | No; comparison workflows |

The built-in job types remain supported by the worker agent and are exercised by
the regression tests. They are optional workflows, not abandoned code.

## Built-in PyTorch demonstration

From the repository root, with a dispatcher and workers running:

```bash
.controller-venv/bin/python controller/submit_job.py --type pytorch --epochs 60 --checkpoint-every-seconds 5 --batch-delay 0.05
.controller-venv/bin/python controller/job_status.py
```

For automated recovery evidence use `tools/demos/reboot_recovery.py`;
see [the tools guide](../tools/README.md). The script intentionally reboots a worker.

## Reference snapshots

| Files | Purpose |
|---|---|
| `reference/equation.py` | Linear-model training and rank parameter export |
| `reference/logistic.py`, `reference/numpy_mlp.py` | Synthetic benchmark timing |
| `reference/resnet18.py`, `reference/tiny_cnn.py` | CIFAR-10 benchmark timing |
| `reference/logistic_train.py`, `reference/numpy_mlp_train.py` | Full synthetic training and parameter export |
| `reference/resnet18_train.py`, `reference/tiny_cnn_train.py` | Full image training and weight export |
| `reference/resnet18_inference.py`, `reference/tiny_cnn_inference.py` | Evaluate exported image weights |
| `reference/provenance.json` | Original source paths and SHA-256 hashes |

Keep reference Python files byte-for-byte unchanged so their provenance hashes
remain valid. Read the [reference explanations](reference/README.md) and
[benchmark commands](../docs/benchmarks.md) before running them.
