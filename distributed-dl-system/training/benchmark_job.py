"""Run a fixed reference workload without replacing the custom controller.

The original scripts remain unchanged in reference/. This adapter supplies rank
and rendezvous settings, captures their own timing markers, and writes a small
result for the Node Agent. Benchmark jobs do not support checkpoint retry.
"""

import argparse
import contextlib
import io
import json
import os
from pathlib import Path
import runpy
import sys
import time

MODELS = {"equation", "logistic", "numpy_mlp", "resnet18", "tiny_cnn"}


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
    source = Path(__file__).parent / "reference" / (args.model + ".py")
    argv = [str(source)]
    if args.model in {"logistic", "numpy_mlp"}:
        argv += ["--samples", str(args.samples)]
    elif args.model in {"resnet18", "tiny_cnn"}:
        argv += ["--epochs", str(args.epochs)]
        if args.model == "resnet18":
            argv += ["--samples", str(args.samples)]
        if args.world_size > 1:
            argv += ["--distributed"]
    capture = Capture(sys.stdout)
    start = time.monotonic()
    old_argv = sys.argv
    try:
        sys.argv = argv
        with contextlib.redirect_stdout(capture):
            runpy.run_path(str(source), run_name="__main__")
    finally:
        sys.argv = old_argv
    elapsed = time.monotonic() - start
    output = "".join(capture.parts)
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
