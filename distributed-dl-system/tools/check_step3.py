"""Terminal demonstration: submit, stage, prepare, verify and reuse worker caches."""

import argparse
import json
import os
import shlex
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]


def command(arguments):
    """Invoke the public controller commands exactly as a terminal user would."""
    result = subprocess.run(
        [sys.executable, *arguments],
        cwd=ROOT,
        env=dict(os.environ, PYTHONDONTWRITEBYTECODE="1"),
        stdout=subprocess.PIPE,
        text=True,
    )
    if result.stderr:
        print(result.stderr, end="", file=sys.stderr, flush=True)
    if result.returncode:
        print(result.stdout, end="", flush=True)
        raise SystemExit(result.returncode)
    return json.loads(result.stdout)


def main():
    """Use two real submissions to prove environment and dataset cache reuse."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--nodes", nargs="+", default=["pi01"])
    parser.add_argument(
        "--profile",
        choices=("isolated", "worker-base", "pytorch-cpu"),
        default="pytorch-cpu",
    )
    parser.add_argument("--owner", default="terminal-demo")
    args = parser.parse_args()
    nodes = list(dict.fromkeys(args.nodes))
    known = {
        node["id"]
        for node in json.loads((ROOT / "config/nodes.json").read_text())["nodes"]
    } | {"local"}
    if any(node not in known for node in nodes):
        parser.error("Use configured worker IDs or local")
    spec = (
        "examples/job.single.json"
        if len(nodes) == 1
        else "examples/job.distributed.json"
    )
    for number in (1, 2):
        receipt = command(
            ["controller/submit_job.py", "--spec", spec, "--owner", args.owner]
        )
        job_id = receipt["job_id"]
        print(
            f"\nJOB {number}: {job_id}\nStep 1: submission accepted\nStep 2: {receipt['status']} — input snapshot saved",
            flush=True,
        )
        command(["controller/stage_job.py", "--verify", job_id])
        prepared = command(
            [
                "controller/prepare_job.py",
                "--job-id",
                job_id,
                "--nodes",
                *nodes,
                "--profile",
                args.profile,
            ]
        )
        verified = command(
            [
                "controller/prepare_job.py",
                "--job-id",
                job_id,
                "--nodes",
                *nodes,
                "--verify",
                "--import-module",
                "torch",
            ]
        )
        for setup, check in zip(prepared, verified):
            print(
                f"  {check['node_id']}: {check['status']} | Python {check['record']['runtime']['python']} | {check['record']['runtime']['machine']}",
                flush=True,
            )
            print(
                f"  torch={check['imports']['torch']} | {check['pip_check']}",
                flush=True,
            )
            print(f"  cache hits: {setup['cache_hits']}", flush=True)
            if number == 2 and not all(setup["cache_hits"].values()):
                raise SystemExit("Second job did not reuse every expected cache")
        print(
            "Inspect: "
            + shlex.join(
                [sys.executable, str(ROOT / "controller/job_status.py"), job_id]
            ),
            flush=True,
        )
    print(
        "\nPASS: submission, storage, worker environments, imports and cache reuse verified.",
        flush=True,
    )
    print(
        "Jobs are READY. Use controller/queue_job.py for single-worker scheduling; see docs/scheduling.md.",
        flush=True,
    )


if __name__ == "__main__":
    main()
