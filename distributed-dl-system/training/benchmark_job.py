"""Run a fixed reference workload without replacing the custom controller.

The original scripts remain unchanged in reference/. This adapter supplies rank
and rendezvous settings, captures their own timing markers, and writes a small
result for the Node Agent. Benchmark jobs do not support checkpoint retry.
"""

import argparse
import contextlib
import io
import hashlib
import json
import os
from pathlib import Path
import runpy
import re
import shutil
import sys
import tempfile
import time

MODELS = {
    "equation",
    "logistic",
    "numpy_mlp",
    "resnet18",
    "tiny_cnn",
    "logistic_train",
    "numpy_mlp_train",
    "resnet18_train",
    "tiny_cnn_train",
}


def run_image_training(source, args):
    """Preserve legacy output paths, retain fresh weights, then run original inference."""
    import torch

    filename = (
        "pretrained_weights.pt"
        if args.model == "resnet18_train"
        else "tinyyolo_distributed.pt"
    )
    legacy = Path("/tmp") / filename
    backup = args.output.with_suffix(".previous-weights.pt")
    artifact = args.output.with_suffix(".weights.pt")
    # Original workers write one fixed /tmp path on rank zero. Preserve an
    # existing file and give this invocation its own durable artifact name.
    had_previous = args.rank == 0 and legacy.exists()
    if had_previous:
        os.replace(legacy, backup)
    try:
        runpy.run_path(str(source), run_name="__main__")
        if args.rank != 0:
            return None
        state = torch.load(legacy, map_location="cpu", weights_only=True)
        if not state or not all(torch.isfinite(v).all().item() for v in state.values()):
            raise ValueError("Training did not produce finite model weights")
        shutil.copyfile(legacy, artifact)
        inference = source.with_name(args.model.replace("_train", "_inference") + ".py")
        # The ResNet inference script expects relative ./data and weight paths.
        # Supply those in an isolated directory without editing its source.
        cwd = Path.cwd()
        old_argv = sys.argv
        try:
            with tempfile.TemporaryDirectory(prefix="dl-inference-") as directory:
                directory = Path(directory)
                (directory / "data").symlink_to(
                    "/tmp/cifar10_data", target_is_directory=True
                )
                (directory / filename).symlink_to(artifact)
                os.chdir(directory)
                sys.argv = [str(inference), str(artifact)]
                print("[REFERENCE_INFERENCE_START]", flush=True)
                runpy.run_path(str(inference), run_name="__main__")
                print("[REFERENCE_INFERENCE_END]", flush=True)
        finally:
            os.chdir(cwd)
            sys.argv = old_argv
        return {
            "path": str(artifact),
            "sha256": hashlib.sha256(artifact.read_bytes()).hexdigest(),
            "tensor_count": len(state),
            "finite": True,
        }
    finally:
        if args.rank == 0:
            if had_previous:
                os.replace(backup, legacy)
            else:
                legacy.unlink(missing_ok=True)


class Capture(io.TextIOBase):
    """Keep reference output visible in the job log and available for parsing."""

    def __init__(self, stream):
        self.stream = stream
        self.parts = []

    def write(self, text):
        self.parts.append(text)
        self.stream.write(text)
        self.stream.flush()
        return len(text)

    def flush(self):
        self.stream.flush()


def main():
    """Set up a reference invocation, then persist results only on success."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", choices=sorted(MODELS), required=True)
    parser.add_argument("--samples", type=int, default=1000)
    parser.add_argument("--epochs", type=int, default=1)
    parser.add_argument("--rank", type=int, default=0)
    parser.add_argument("--world-size", type=int, default=1)
    parser.add_argument("--master-addr", default="127.0.0.1")
    parser.add_argument("--master-port", type=int, default=29651)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    os.environ.update(
        SLURM_PROCID=str(args.rank),
        SLURM_NTASKS=str(args.world_size),
        RANK=str(args.rank),
        WORLD_SIZE=str(args.world_size),
        MASTER_ADDR=args.master_addr,
        MASTER_PORT=str(args.master_port),
        GLOO_SOCKET_IFNAME="eth0",
    )
    args.output = args.output.resolve()
    source = Path(__file__).resolve().parent / "reference" / (args.model + ".py")
    argv = [str(source)]
    if args.model in {"logistic", "numpy_mlp"}:
        argv += ["--samples", str(args.samples)]
    elif args.model in {"resnet18", "tiny_cnn"}:
        argv += ["--epochs", str(args.epochs)]
        if args.model == "resnet18":
            argv += ["--samples", str(args.samples)]
        if args.world_size > 1:
            argv += ["--distributed"]
    elif args.model == "resnet18_train":
        argv += ["--epochs", str(args.epochs)]
    capture = Capture(sys.stdout)
    start = time.monotonic()
    old_argv = sys.argv
    artifact = None
    try:
        sys.argv = argv
        with contextlib.redirect_stdout(capture):
            if args.model in {"resnet18_train", "tiny_cnn_train"}:
                artifact = run_image_training(source, args)
            else:
                runpy.run_path(str(source), run_name="__main__")
    finally:
        sys.argv = old_argv
    elapsed = time.monotonic() - start
    output = "".join(capture.parts)
    accuracy = None
    if args.rank == 0 and args.model in {"resnet18_train", "tiny_cnn_train"}:
        match = re.search(
            r"(?:Final Test Accuracy|Overall Accuracy): ([0-9.]+)%", output
        )
        if match is None:
            raise RuntimeError("Original inference script did not report test accuracy")
        accuracy = float(match.group(1))
    timings = [
        line
        for line in output.splitlines()
        if line.startswith(("TIME|", "BENCHMARK_RESULT|"))
    ]
    result = {
        "model": args.model,
        "rank": args.rank,
        "world_size": args.world_size,
        "samples_per_invocation": args.samples,
        "epochs_argument": args.epochs,
        "wrapper_seconds": elapsed,
        "reference_compute_seconds": (
            float(timings[-1].split("|")[-1]) if timings else None
        ),
        "reference_output": output,
        "completed": True,
        "artifact": artifact,
        "test_accuracy_percent": accuracy,
    }
    temporary = args.output.with_suffix(".tmp")
    temporary.write_text(json.dumps(result, indent=2))
    os.replace(temporary, args.output)
    print(
        "[BENCHMARK_COMPLETED] "
        + json.dumps({k: v for k, v in result.items() if k != "reference_output"}),
        flush=True,
    )


if __name__ == "__main__":
    main()
