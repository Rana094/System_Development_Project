# Benchmarks and reference training

Run these commands from the repository root. For individual versus DDP timing,
see [distributed training](distributed-training.md#compare-individual-and-ddp-training-time).
For a list of scripts, see the [tools guide](../tools/README.md).

## Live checks and comparisons

The reboot runner intentionally reboots a selected training worker:

```bash
.controller-venv/bin/python -u tools/demos/reboot_recovery.py
```

With all workers idle and no dispatcher running, compare the reference workloads:

```bash
.controller-venv/bin/python -u tools/benchmarks/run_reference.py
```

The runner starts and stops its own dispatcher, constrains benchmarks to
pi02/pi03, and writes private results outside the repository. `--models` selects
any of `equation logistic numpy_mlp resnet18 tiny_cnn`. Images and pretrained
weights must be cached before measuring the image workloads. CIFAR-10 is stored
at `/tmp/cifar10_data`, matching the reference; it may need preparing after reboot.

Model accuracy, single-run compute speed and scheduler/recovery behavior are
separate measurements. Historical numbers from a different environment are
reference observations, not a controlled proof that one scheduler is faster.

## Full reference training and inference

The benchmark variants above differ from the friend's full training workers.
Run all five original training workflows on two workers with:

```bash
.controller-venv/bin/python -u tools/benchmarks/run_reference.py \
  --models equation logistic_train numpy_mlp_train resnet18_train tiny_cnn_train \
  --world-sizes 2 --output-dir "$HOME/distributed-dl-private/full-training"
```

This uses the original worker code and epoch counts. Logistic regression and
the NumPy MLP export independent rank models; the runner averages their parameters
using the same arithmetic as the original master scripts. ResNet-18 trains on
all 50,000 CIFAR-10 images for one epoch, and the CNN trains for five epochs with
the original augmentation. Both image workers synchronize gradients using DDP.
Their original inference scripts then evaluate the exported model on 10,000
test images. Model files are copied to the private run directory and checked
against the worker's SHA-256 digest.

Your controller replaces the friend's Slurm dispatch/master launch commands.
The original source folder is not edited. Fixed output paths expected by the
image workers are temporarily reserved; existing weights at those paths are
restored afterward. Full-run wall time includes inference and artifact
validation, so it must not be compared with training-only benchmark timers.
The `benchmark` job type hosts these reference workloads with zero retries;
it does not add checkpoint recovery to the friend's workers.

Validate a completed full-training run and generate private notes with:

```bash
.controller-venv/bin/python tools/benchmarks/verify_reference.py \
  "$HOME/distributed-dl-private/full-training/<run-directory>"
```

This checks completion and artifact hashes, verifies unchanged reference sources,
and evaluates the averaged logistic/MLP models on independent synthetic examples.
Successful execution and good model accuracy are reported separately.

