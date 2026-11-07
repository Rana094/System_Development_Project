# Training examples

Each `job.*.json` manifest points to a project directory and a sample dataset in
this folder. Paths are relative to the manifest. Keep the project and data files
together when copying an example.

## Choose a manifest

| Manifest | Training project | Recovery | Use |
|---|---|---|---|
| [job.single.json](job.single.json) | `student_project/` | Restart | Minimal submission and environment preparation |
| [job.scheduling.json](job.scheduling.json) | `scheduling_project/` | Restart | Concurrent jobs and queue demonstrations |
| [job.checkpoint.json](job.checkpoint.json) | `checkpoint_project/` | Checkpoint | Longer regression workload for recovery demonstrations |
| [job.integration.json](job.integration.json) | `checkpoint_project/` | Checkpoint | Shorter regression integration workflow; portal regression example |
| [job.binary.json](job.binary.json) | `classification_project/` | Checkpoint | Binary MLP classification |
| [job.cnn.json](job.cnn.json) | `classification_project/` | Checkpoint | Small image CNN classification |
| [job.distributed.json](job.distributed.json) | `ddp_project/` | Restart | Two-worker CPU DDP |
| [job.distributed-checkpoint.json](job.distributed-checkpoint.json) | `ddp_project/` | Checkpoint | Two-worker DDP with coordinated save and resume |

The two regression checkpoint manifests intentionally share a trainer but use
different durations. The two classification manifests intentionally share a
trainer selected with `--task`. These are useful variations, not duplicate source.

## Project files

Every project contains `train.py` and `requirements.txt`. The training script is
student-owned code; the requirements file declares that project's Python packages.

| Project | What to learn from its trainer |
|---|---|
| [student_project/](student_project/train.py) | Minimal CSV regression entrypoint |
| [scheduling_project/](scheduling_project/train.py) | Time-bounded MLP and collected model artifacts |
| [checkpoint_project/](checkpoint_project/train.py) | Checkpoint SDK, stochastic batches, dropout and exact resume |
| [classification_project/](classification_project/train.py) | Shared checkpoint support for binary and CNN tasks |
| [ddp_project/](ddp_project/train.py) | Sharded data, gradient synchronization and coordinated checkpoints |

## Sample data

| File | Used by |
|---|---|
| [dataset.csv](dataset.csv) | Minimal, scheduling and checkpoint regression examples |
| [binary-classification.csv](binary-classification.csv) | Binary classification and both DDP manifests |
| [image-classification.csv](image-classification.csv) | CNN classification |

These are small synthetic source inputs and belong in version control.

## Validate an example

From the repository root:

```bash
.controller-venv/bin/python controller/submit_job.py --spec examples/job.single.json --validate-only
```

For submission, preparation and queuing, follow the [submission guide](../docs/submission.md)
or [training examples walkthrough](../docs/training-examples.md).
