"""Prepare staged projects on selected workers; no resource reservation or dispatch occurs."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import uuid

from job_storage import StorageError, verify_workspace
from job_store import get_job, record_environment, invalidate_environment

ROOT = Path(__file__).resolve().parents[1]
HELPER = ROOT / "agent/environment_worker.py"


class PreparationError(ValueError):
    """The selected worker could not prepare or verify a job's environment."""


class Worker:
    """Use the same worker protocol locally or through the existing SSH aliases."""

    def __init__(self, node):
        self.node = node
        self.local = node["id"] == "local"
        self.root = (
            str(
                Path(
                    os.environ.get(
                        "DL_LOCAL_WORKER_ROOT", ROOT / "storage/local-worker"
                    )
                ).resolve()
            )
            if self.local
            else node.get("worker_root", "distributed-dl-system")
        )
        self.python = (
            sys.executable
            if self.local
            else node.get("worker_python", ".venv/bin/python")
        )
        self.runtime = Path(self.root) / "runtime"

    def command(self, args):
        if self.local:
            return [self.python, str(self.runtime / "environment_worker.py"), *args]
        command = (
            "cd "
            + shlex.quote(self.root)
            + " && "
            + shlex.join([self.python, "runtime/environment_worker.py", *args])
        )
        return [
            "ssh",
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=5",
            self.node["id"],
            command,
        ]

    def mkdir(self, relative):
        if self.local:
            (Path(self.root) / relative).mkdir(parents=True, exist_ok=True)
        else:
            subprocess.run(
                [
                    "ssh",
                    "-o",
                    "BatchMode=yes",
                    self.node["id"],
                    "mkdir -p " + shlex.quote(self.root + "/" + relative),
                ],
                check=True,
                timeout=30,
            )

    def copy(self, source, relative, directory=False):
        destination = Path(self.root) / relative
        if self.local:
            if directory:
                shutil.copytree(source, destination)
            else:
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(source, destination)
        else:
            subprocess.run(
                [
                    "rsync",
                    "-a",
                    "--protect-args",
                    str(source) + ("/" if directory else ""),
                    self.node["id"]
                    + ":"
                    + str(destination)
                    + ("/" if directory else ""),
                ],
                check=True,
                timeout=1800,
            )

    def bootstrap(self):
        self.mkdir("runtime")
        self.copy(HELPER, "runtime/environment_worker.py")

    def request(self, action, extra):
        args = (
            [action, "--root", "worker-storage", *extra]
            if not self.local
            else [action, "--root", str(Path(self.root) / "worker-storage"), *extra]
        )
        result = subprocess.run(
            self.command(args), capture_output=True, text=True, timeout=1800
        )
        try:
            response = json.loads(result.stdout)
        except ValueError as error:
            raise PreparationError(
                "Invalid worker response: " + result.stderr + result.stdout
            ) from error
        if result.returncode or "error" in response:
            raise PreparationError(response.get("error", result.stderr))
        return response


def nodes_by_id():
    """Known SSH targets come from controller configuration, never from student commands."""
    nodes = {
        node["id"]: node
        for node in json.loads((ROOT / "config/nodes.json").read_text())["nodes"]
    }
    nodes["local"] = {"id": "local"}
    return nodes


def prepare_on_worker(job_id, node, profile):
    """Transfer cache misses and persist a successful worker record with the input identity."""
    job = get_job(job_id)
    if (
        job is None
        or job["job_type"] != "custom"
        or job["status"] not in ("STAGED", "READY")
    ):
        raise PreparationError("Prepare a STAGED student submission first")
    workspace = Path(job["storage_path"])
    manifest, fingerprint = verify_workspace(workspace, job)
    worker = Worker(node)
    worker.bootstrap()
    token = job_id + "-" + uuid.uuid4().hex[:12]
    relative = "worker-storage/incoming/" + token
    worker.mkdir(relative)
    descriptor = {
        "job_id": job_id,
        "profile": profile,
        "manifest": manifest,
        "manifest_sha256": fingerprint,
    }
    with tempfile.TemporaryDirectory(prefix="environment-request-") as directory:
        path = Path(directory) / "request.json"
        path.write_text(json.dumps(descriptor))
        worker.copy(path, relative + "/request.json")
    remote_request = (
        str(Path(worker.root) / relative / "request.json")
        if worker.local
        else relative + "/request.json"
    )
    plan = worker.request("plan", ["--request", remote_request])
    for kind in ("source", "data"):
        if not plan[kind + "_cached"]:
            worker.copy(workspace / kind, relative + "/" + kind, directory=True)
    prepared = worker.request("prepare", ["--request", remote_request])
    verified = worker.request("verify", ["--job-id", job_id])
    if verified["record"] != prepared["record"]:
        raise PreparationError("Prepared record changed before verification")
    state = record_environment(job_id, node["id"], prepared["record"])
    return {"node_id": node["id"], "status": state, **prepared}


def verify_on_worker(job_id, node, modules):
    """Probe a prepared environment in a new worker process and refresh its database record."""
    job = get_job(job_id)
    if job is None or job["status"] not in ("STAGED", "READY"):
        raise PreparationError(
            "Verification requires a staged or ready student submission"
        )
    verify_workspace(Path(job["storage_path"]), job)
    worker = Worker(node)
    worker.bootstrap()
    extra = ["--job-id", job_id]
    for module in modules:
        extra.extend(["--import-module", module])
    result = worker.request("verify", extra)
    state = record_environment(job_id, node["id"], result["record"])
    return {"node_id": node["id"], "status": state, **result}


def main():
    """Give students and operators a terminal-visible preparation and import check."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--job-id", required=True)
    parser.add_argument(
        "--nodes",
        nargs="+",
        required=True,
        help="Configured worker IDs, or local for a controller-side check.",
    )
    parser.add_argument(
        "--profile",
        choices=("isolated", "worker-base", "pytorch-cpu"),
        default="isolated",
    )
    parser.add_argument("--verify", action="store_true")
    parser.add_argument(
        "--summary", action="store_true", help="Print a compact terminal receipt."
    )
    parser.add_argument("--import-module", action="append", default=[])
    args = parser.parse_args()
    nodes = nodes_by_id()
    if any(
        node not in nodes or not re.fullmatch(r"[A-Za-z0-9_-]+", node)
        for node in args.nodes
    ):
        parser.error("Use configured worker IDs or local")
    if args.import_module and not args.verify:
        parser.error("--import-module requires --verify")
    results = []
    for node_id in dict.fromkeys(args.nodes):
        print(
            f"[ENVIRONMENT] {node_id}: {'verifying' if args.verify else 'preparing'} {args.job_id}",
            file=sys.stderr,
            flush=True,
        )
        try:
            result = (
                verify_on_worker(args.job_id, nodes[node_id], args.import_module)
                if args.verify
                else prepare_on_worker(args.job_id, nodes[node_id], args.profile)
            )
            results.append(result)
        except (
            OSError,
            ValueError,
            KeyError,
            RuntimeError,
            subprocess.SubprocessError,
        ) as error:
            invalidate_environment(args.job_id, node_id, str(error))
            results.append({"node_id": node_id, "error": str(error)})
    display = results
    if args.summary:
        display = []
        for result in results:
            if "error" in result:
                display.append(result)
                continue
            record = result["record"]
            display.append(
                {
                    "job_id": args.job_id,
                    "node_id": result["node_id"],
                    "status": result["status"],
                    "profile": record["profile"],
                    "python": record["runtime"]["python"],
                    "architecture": record["runtime"]["machine"],
                    "python_path": record["python_path"],
                    "dataset_path": record["dataset_path"],
                    **{
                        key: result[key]
                        for key in (
                            "prepared",
                            "verified",
                            "cache_hits",
                            "pip_check",
                            "imports",
                        )
                        if key in result
                    },
                }
            )
    print(json.dumps(display, indent=2))
    if any("error" in result for result in results):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
