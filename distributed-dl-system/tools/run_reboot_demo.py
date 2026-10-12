"""Run the authorized Pi reboot demonstration and preserve machine-readable evidence.

Run from the controller with all three workers idle and passwordless sudo.
This intentionally reboots the worker selected for a new training job.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import requests
import torch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "controller"))
from job_store import get_job, get_jobs_by_status, get_attempts
from submit_job import create_pytorch_job
from recovery_metrics import log_event, get_events


def ssh(node, command, timeout=15):
    """Run a bounded noninteractive command and retain its output as test evidence."""
    return subprocess.run(
        ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5", node, command],
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def main():
    """Exercise placement, reboot, checkpoint restore, completion and worker rejoin."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--load-node", default="pi01")
    parser.add_argument("--epochs", type=int, default=60)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path.home() / "distributed-dl-private" / "runs",
    )
    args = parser.parse_args()
    os.chdir(ROOT)
    nodes = json.loads((ROOT / "config/nodes.json").read_text())["nodes"]
    by_id = {n["id"]: n for n in nodes}
    assert args.load_node in by_id
    for status in ("QUEUED", "SCHEDULED", "RUNNING"):
        assert not get_jobs_by_status(
            status
        ), f"Existing {status} jobs: finish them first"
    stamp = datetime.now(ZoneInfo("Asia/Dhaka")).strftime("%Y%m%d-%H%M%S")
    run_dir = args.output_dir / ("reboot-demo-" + stamp)
    run_dir.mkdir(parents=True)
    print("Evidence directory:", run_dir, flush=True)
    session = requests.Session()
    session.trust_env = False

    def api(node, route):
        n = by_id[node]
        response = session.get(f"http://{n['ip']}:{n['port']}{route}", timeout=4)
        response.raise_for_status()
        return response.json()

    def wait_for(description, fn, timeout=120):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            value = fn()
            if value:
                print(description, flush=True)
                return value
            time.sleep(1)
        raise TimeoutError(description)

    def save(name, value):
        (run_dir / name).write_text(json.dumps(value, indent=2))

    preflight = {}
    for node in by_id:
        resources = api(node, "/resources")
        assert resources["node_state"] == "IDLE", f"{node} is busy"
        result = ssh(
            node,
            "sudo -n true && systemctl is-enabled dl-node-agent && systemctl is-active dl-node-agent",
        )
        assert result.returncode == 0, result.stderr
        preflight[node] = resources
    save("preflight.json", preflight)
    dispatcher = None
    load_started = False
    dispatcher_log = open(run_dir / "dispatcher.log", "w")
    job_id = None
    try:
        # A bounded transient service owns all four CPU-load processes; cleanup
        # stops this service only, never unrelated processes on the worker.
        load_code = (
            "import multiprocessing,time\n"
            "def burn():\n"
            " end=time.monotonic()+180\n"
            " while time.monotonic()<end: pass\n"
            'if __name__=="__main__":\n'
            " ps=[multiprocessing.Process(target=burn) for _ in range(4)]\n"
            " for p in ps: p.start()\n"
            " for p in ps: p.join()\n"
        )
        command = shlex.join(
            [
                "sudo",
                "-n",
                "systemd-run",
                "--unit=dl-scheduler-demo",
                "--property=RuntimeMaxSec=190s",
                "/usr/bin/python3",
                "-c",
                load_code,
            ]
        )
        result = ssh(args.load_node, command)
        assert result.returncode == 0, result.stderr
        load_started = True
        time.sleep(3)
        before = {node: api(node, "/resources") for node in by_id}
        save("loaded_resources.json", before)
        assert (
            before[args.load_node]["cpu"]["usage_percent"] > 75
        ), "CPU load not established"
        dispatcher = subprocess.Popen(
            [sys.executable, "-u", "controller/dispatcher.py"],
            stdout=dispatcher_log,
            stderr=subprocess.STDOUT,
        )
        time.sleep(3)
        assert dispatcher.poll() is None, "Dispatcher failed to start; see log"
        job_id = create_pytorch_job(args.epochs, 128, 0.001, 1, 5, 0.05, 2)
        (run_dir / "job_id.txt").write_text(job_id + "\n")
        job = wait_for(
            "Training started",
            lambda: (j if (j := get_job(job_id))["status"] == "RUNNING" else None),
        )
        failed_node = job["node_id"]
        assert (
            failed_node != args.load_node
        ), "Scheduler selected deliberately loaded node"
        save(
            "placement.json",
            {
                "job_id": job_id,
                "loaded_node": args.load_node,
                "selected_node": failed_node,
            },
        )
        print(
            f"Scheduler avoided loaded {args.load_node}; selected {failed_node}",
            flush=True,
        )
        ssh(args.load_node, "sudo -n systemctl stop dl-scheduler-demo")
        load_started = False
        central = ROOT / "checkpoints" / (job_id + ".pt")

        def checkpoint_ready():
            if not central.exists():
                return None
            c = torch.load(central, map_location="cpu", weights_only=True)
            return c if c.get("global_step", 0) > 0 and not c.get("completed") else None

        checkpoint = wait_for("Central recovery checkpoint verified", checkpoint_ready)
        assert checkpoint["optimizer_state_dict"]["state"]
        assert checkpoint["save_reason"] == "time", "Time option did not reach trainer"
        assert api(failed_node, f"/jobs/{job_id}")["job"]["status"] == "RUNNING"
        old_boot = ssh(failed_node, "cat /proc/sys/kernel/random/boot_id")
        assert old_boot.returncode == 0, old_boot.stderr
        old_boot = old_boot.stdout.strip()
        shutil.copyfile(central, run_dir / "before_reboot.pt")
        original_log = ssh(
            failed_node, f"cat ~/distributed-dl-system/logs/{job_id}.log"
        )
        (run_dir / "original_before_reboot.log").write_text(original_log.stdout)
        failure_time = log_event(
            job_id,
            "FAILURE_INJECTED",
            failed_node,
            {"method": "sudo reboot", "boot_id_before": old_boot},
        )
        result = ssh(failed_node, "sudo -n reboot")
        save(
            "reboot_command.json",
            {
                "returncode": result.returncode,
                "stdout": result.stdout,
                "stderr": result.stderr,
                "failure_time": failure_time,
            },
        )
        assert result.returncode in (0, 255), result.stderr
        print(f"Reboot sent to {failed_node}; observing recovery", flush=True)

        def resumed():
            events = [e for e in get_events(job_id) if e["event_type"] == "RESUMED"]
            return events[-1] if events else None

        event = wait_for(
            "Training process confirmed model and optimizer restoration", resumed, 150
        )
        replacement = event["node_id"]
        assert replacement != failed_node
        info = json.loads(event["details_json"])
        # Match the trainer's restored bytes to a checkpoint the controller synced.
        syncs = [
            json.loads(e["details_json"])
            for e in get_events(job_id)
            if e["event_type"] == "CHECKPOINT_SYNC"
        ]
        assert any(s["sha256"] == info["checkpoint_sha256"] for s in syncs)
        assert info["restored_global_step"] >= checkpoint["global_step"]
        save("confirmed_resume.json", {"replacement": replacement, **info})

        def rejoined():
            result = ssh(
                failed_node,
                "cat /proc/sys/kernel/random/boot_id; systemctl is-active dl-node-agent",
                timeout=8,
            )
            if result.returncode:
                return None
            lines = result.stdout.strip().splitlines()
            if len(lines) < 2 or lines[0] == old_boot or lines[1] != "active":
                return None
            try:
                resources = api(failed_node, "/resources")
            except requests.RequestException:
                return None
            return {
                "boot_id_before": old_boot,
                "boot_id_after": lines[0],
                "resources": resources,
            }

        rejoin = wait_for("Original worker automatically rejoined", rejoined, 120)
        save("rejoin.json", rejoin)

        def done():
            j = get_job(job_id)
            if j["status"] == "FAILED":
                raise RuntimeError(j)
            return j if j["status"] == "COMPLETED" else None

        final_job = wait_for(
            "Replacement training completed; final checkpoint collected", done, 600
        )
        final = torch.load(central, map_location="cpu", weights_only=True)
        assert final["completed"] and final["hostname"] == replacement
        assert final["global_step"] == args.epochs * 47
        assert final["global_step"] > info["restored_global_step"]
        assert all(
            int(state["step"]) == final["global_step"]
            for state in final["optimizer_state_dict"]["state"].values()
        )
        attempts = get_attempts(job_id)
        assert (
            len(attempts) == 2
            and attempts[0]["status"] == "FAILED"
            and attempts[1]["status"] == "COMPLETED"
        )
        assert attempts[0]["node_id"] != attempts[1]["node_id"]
        shutil.copyfile(central, run_dir / "final.pt")
        for node in (failed_node, replacement):
            result = ssh(node, f"cat ~/distributed-dl-system/logs/{job_id}.log")
            (run_dir / f"{node}_training.log").write_text(result.stdout)
        save("attempts.json", attempts)
        save("events.json", get_events(job_id))
        save("job.json", final_job)
        save(
            "final_summary.json",
            {
                k: final.get(k)
                for k in (
                    "job_id",
                    "hostname",
                    "epoch",
                    "global_step",
                    "loss",
                    "completed",
                    "save_reason",
                )
            },
        )
        report = subprocess.run(
            [sys.executable, "controller/recovery_report.py", job_id],
            capture_output=True,
            text=True,
        )
        (run_dir / "recovery_report.txt").write_text(report.stdout)
        assert report.returncode == 0, report.stdout
        save(
            "result.json",
            {
                "passed": True,
                "job_id": job_id,
                "failed_node": failed_node,
                "replacement_node": replacement,
                "scheduler_avoided_loaded_node": True,
                "restored_step": info["restored_global_step"],
                "final_step": final["global_step"],
                "automatic_rejoin": True,
            },
        )
        print(
            "PASS: resource-aware placement, whole-node failover, confirmed restore, completion, rejoin",
            flush=True,
        )
    finally:
        if load_started:
            ssh(args.load_node, "sudo -n systemctl stop dl-scheduler-demo")
        # Never abandon active training if an evidence assertion failed.
        if dispatcher is not None and dispatcher.poll() is None:
            active = job_id and get_job(job_id)["status"] in (
                "RUNNING",
                "QUEUED",
                "SCHEDULED",
            )
            if active:
                print(
                    f"Job {job_id} still active; dispatcher PID {dispatcher.pid} left running. Evidence: {run_dir}",
                    flush=True,
                )
            else:
                dispatcher.terminate()
                dispatcher.wait(timeout=10)
        dispatcher_log.close()


if __name__ == "__main__":
    main()
