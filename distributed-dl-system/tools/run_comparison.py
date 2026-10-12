"""Submit the reference benchmark matrix through the custom scheduler.

One- and two-node configurations match the friend's benchmark scripts. Rank
placement is constrained to pi02/pi03 for hardware comparability. The unchanged
reference code supplies compute timing; controller wall time is kept separately.
These benchmark jobs have no automatic retry or checkpoint recovery.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "controller"))
from job_store import add_job, get_job, get_jobs_by_status, get_attempts


def main():
    """Run each configuration alone, collect every rank, and clean up owned jobs."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path.home() / "distributed-dl-private" / "comparisons",
    )
    parser.add_argument(
        "--world-sizes",
        nargs="+",
        type=int,
        choices=[1, 2],
        default=[1, 2],
        help="Use two ranks for the original full training workflows.",
    )
    parser.add_argument(
        "--models",
        nargs="+",
        default=["equation", "logistic", "numpy_mlp", "resnet18", "tiny_cnn"],
    )
    args = parser.parse_args()
    os.chdir(ROOT)
    nodes = {
        n["id"]: n
        for n in json.loads((ROOT / "config/nodes.json").read_text())["nodes"]
    }
    for status in ("QUEUED", "SCHEDULED", "RUNNING"):
        assert not get_jobs_by_status(status), f"Existing {status} jobs"
    stamp = datetime.now(ZoneInfo("Asia/Dhaka")).strftime("%Y%m%d-%H%M%S")
    output = args.output_dir / stamp
    output.mkdir(parents=True)
    print("Private comparison output:", output, flush=True)
    session = requests.Session()
    session.trust_env = False

    def api(node, path, method="GET"):
        n = nodes[node]
        response = session.request(
            method, f"http://{n['ip']}:{n['port']}{path}", timeout=10
        )
        response.raise_for_status()
        return response.json()

    def save(name, data):
        (output / name).write_text(json.dumps(data, indent=2) + "\n")

    resources = {node: api(node, "/resources") for node in ("pi02", "pi03")}
    assert all(r["node_state"] == "IDLE" for r in resources.values())
    save("initial_resources.json", resources)
    rows = []
    owned = []
    log = open(output / "dispatcher.log", "w")
    dispatcher = subprocess.Popen(
        [sys.executable, "-u", "controller/dispatcher.py"],
        stdout=log,
        stderr=subprocess.STDOUT,
    )
    try:
        time.sleep(3)
        assert dispatcher.poll() is None, "Dispatcher already running or failed"
        for model in args.models:
            assert model in {
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
            for world_size in args.world_sizes:
                # Sample counts, epochs and batch sizes are inherited unchanged.
                samples = {
                    "equation": 5000,
                    "logistic": 30000 // world_size,
                    "numpy_mlp": 200000 // world_size,
                    "resnet18": 1000,
                    "tiny_cnn": 50000,
                    "logistic_train": 15000,
                    "numpy_mlp_train": 100000,
                    "resnet18_train": 50000,
                    "tiny_cnn_train": 50000,
                }[model]
                case = f"{model}-{world_size}node"
                started = time.monotonic()
                ids = []
                for rank in range(world_size):
                    jid = f"compare-{stamp}-{model}-{world_size}-{rank}"
                    config = {
                        "model": model,
                        "samples": samples,
                        "epochs": {
                            "logistic_train": 40,
                            "numpy_mlp_train": 500,
                            "tiny_cnn_train": 5,
                        }.get(model, 1),
                        "rank": rank,
                        "world_size": world_size,
                        "master_addr": nodes["pi02"]["ip"],
                        "master_port": 29651,
                        "allowed_nodes": ["pi02" if rank == 0 else "pi03"],
                    }
                    add_job(
                        jid, 1, max_retries=0, job_type="benchmark", job_config=config
                    )
                    ids.append(jid)
                    owned.append(jid)
                print("RUN", case, ids, flush=True)
                deadline = time.monotonic() + 3600
                last_message = 0
                while time.monotonic() < deadline:
                    jobs = [get_job(jid) for jid in ids]
                    if any(j["status"] == "FAILED" for j in jobs):
                        raise RuntimeError(jobs)
                    if all(j["status"] == "COMPLETED" for j in jobs):
                        break
                    if dispatcher.poll() is not None:
                        raise RuntimeError("Dispatcher exited")
                    if time.monotonic() - last_message > 30:
                        print(
                            case,
                            [(j["node_id"], j["status"]) for j in jobs],
                            flush=True,
                        )
                        last_message = time.monotonic()
                    time.sleep(1)
                else:
                    raise TimeoutError(case)
                wall = time.monotonic() - started
                rank_results = []
                for j in jobs:
                    remote = api(j["node_id"], f"/jobs/{j['job_id']}")["job"]
                    result = remote["benchmark_result"]
                    assert result["completed"]
                    rank_results.append(result)
                    if result.get("artifact"):
                        artifact = result["artifact"]
                        destination = output / (j["job_id"] + ".pt")
                        subprocess.run(
                            [
                                "scp",
                                "-q",
                                f"{j['node_id']}:{artifact['path']}",
                                str(destination),
                            ],
                            check=True,
                            timeout=180,
                        )
                        assert (
                            hashlib.sha256(destination.read_bytes()).hexdigest()
                            == artifact["sha256"]
                        )
                        result["controller_artifact"] = str(destination)
                    save(
                        j["job_id"] + ".json",
                        {
                            "job": j,
                            "attempts": get_attempts(j["job_id"]),
                            "result": result,
                        },
                    )
                compute = rank_results[0]["reference_compute_seconds"]
                row = {
                    "model": model,
                    "nodes": world_size,
                    "samples_per_rank_argument": samples,
                    "reference_compute_seconds": compute,
                    "controller_wall_seconds": wall,
                    "max_wrapper_seconds": max(
                        r["wrapper_seconds"] for r in rank_results
                    ),
                    "test_accuracy_percent": rank_results[0].get(
                        "test_accuracy_percent"
                    ),
                }
                if model in {"equation", "logistic_train", "numpy_mlp_train"}:
                    weights = []
                    for result in rank_results:
                        lines = result["reference_output"].splitlines()
                        for i, line in enumerate(lines):
                            if line.startswith("--- MODEL_EXPORT"):
                                weights.append(
                                    [
                                        float(x)
                                        for x in lines[i + 1]
                                        .replace("|", ",")
                                        .split(",")
                                    ]
                                )
                    assert len(weights) == world_size, "Missing rank model export"
                    assert len({len(w) for w in weights}) == 1
                    row["averaged_weights"] = [
                        sum(w[i] for w in weights) / len(weights)
                        for i in range(len(weights[0]))
                    ]
                rows.append(row)
                save("results.json", rows)
                print("DONE", case, row, flush=True)
        print("ALL COMPARISON RUNS COMPLETED", output, flush=True)
    finally:
        # A failed rank must not leave its DDP partner waiting indefinitely.
        for jid in owned:
            job = get_job(jid)
            if job["status"] == "RUNNING" and job.get("node_id"):
                try:
                    api(job["node_id"], f"/jobs/{jid}/stop", "POST")
                except requests.RequestException:
                    pass
        if dispatcher.poll() is None:
            time.sleep(3)
            dispatcher.terminate()
            dispatcher.wait(timeout=15)
        log.close()


if __name__ == "__main__":
    main()
