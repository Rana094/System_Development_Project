"""Isolated regression checks for real model equivalence, controller transitions, and agent launch behavior."""

import contextlib, hashlib, io, os, pathlib, shutil, subprocess, sys, tempfile
from unittest.mock import patch, Mock, MagicMock
import torch

ROOT = pathlib.Path(__file__).resolve().parents[1]
PYTHON = sys.executable


def same(a, b):
    """Recursively compare checkpoint tensors and optimizer metadata without floating-point tolerance."""
    if isinstance(a, torch.Tensor):
        return torch.equal(a, b)
    if isinstance(a, dict):
        return a.keys() == b.keys() and all(same(a[k], b[k]) for k in a)
    if isinstance(a, (list, tuple)):
        return len(a) == len(b) and all(same(x, y) for x, y in zip(a, b))
    return a == b


with tempfile.TemporaryDirectory(prefix="checkpoint-audit-") as td:
    root = pathlib.Path(td)
    for directory in ("training", "controller", "config", "agent"):
        (root / directory).mkdir()
        for p in (ROOT / directory).glob("*.py" if directory != "config" else "*.json"):
            shutil.copy2(p, root / directory / p.name)
    env = dict(os.environ, OMP_NUM_THREADS="1", MKL_NUM_THREADS="1")
    runner = root / "interrupt.py"
    runner.write_text("""import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'training'))
import pytorch_job as t
original=t.save_checkpoint
def save(**kw):
    result=original(**kw)
    if not kw['completed'] and (kw['next_batch_index']==5 or kw['save_reason']=='epoch'):
        os._exit(77)
    return result
t.save_checkpoint=save
t.main()
""")

    def run(path, interrupt=False, time_mode=False):
        args = [
            PYTHON,
            str(runner if interrupt else root / "training/pytorch_job.py"),
            "--job-id",
            "audit",
            "--epochs",
            "2",
            "--batch-size",
            "128",
            "--checkpoint",
            str(path),
            "--resume-status-file",
            str(path) + ".resume.json",
            "--checkpoint-every-seconds",
            "0.000001" if time_mode else "0",
        ]
        result = subprocess.run(
            args, env=env, capture_output=True, text=True, timeout=60
        )
        assert result.returncode == (77 if interrupt else 0), result.stderr
        return result.stdout

    baseline = root / "baseline.pt"
    run(baseline)
    expected = torch.load(baseline, weights_only=False)
    for mode in ("epoch", "mid_epoch"):
        cp = root / (mode + ".pt")
        run(cp, True, mode == "mid_epoch")
        stopped = torch.load(cp, weights_only=False)
        restored_hash = hashlib.sha256(cp.read_bytes()).hexdigest()
        run(cp, time_mode=mode == "mid_epoch")
        actual = torch.load(cp, weights_only=False)
        import json

        ack = json.loads(pathlib.Path(str(cp) + ".resume.json").read_text())
        assert ack["model_restored"] and ack["optimizer_restored"]
        assert ack["restored_global_step"] == stopped["global_step"]
        assert ack["checkpoint_sha256"] == restored_hash
        assert same(expected["model_state_dict"], actual["model_state_dict"])
        assert same(expected["optimizer_state_dict"], actual["optimizer_state_dict"])
        assert actual["global_step"] == expected["global_step"] == 94
        print(
            "PASS",
            mode,
            "resume: model and full Adam state bitwise equal; 94 steps; stopped at",
            stopped["epoch"],
            stopped["next_batch_index"],
        )
    sys.path.insert(0, str(root / "controller"))
    os.chdir(root)
    import job_store as store
    import migrate_pytorch_jobs as migration

    with contextlib.redirect_stdout(io.StringIO()):
        migration.main()
    import dispatcher as d
    import submit_job as submission
    import recovery_metrics as metrics

    observed = []
    original = submission.add_job

    def insert(*args, **kwargs):
        original(*args, **kwargs)
        observed.append(store.get_job(args[0]))

    with patch.object(
        submission, "add_job", side_effect=insert
    ), contextlib.redirect_stdout(io.StringIO()):
        jid = submission.create_pytorch_job(2, 128, 0.001, 1, 5, 0, 2)
    assert observed[0]["status"] == "QUEUED" and observed[0]["job_type"] == "pytorch"
    assert json.loads(observed[0]["job_config"])["checkpoint_every_seconds"] == 5
    print("PASS atomic PyTorch submission")
    response = Mock(status_code=200)
    response.json.return_value = {
        "job": {"started_at": 1, "pid": 123, "attempt_token": jid + ":0"}
    }
    select = {"node": {"id": "pi02", "ip": "127.0.0.2", "port": 5000}}
    with patch.object(d, "select_best_node", return_value=select), patch.object(
        d.requests, "post", side_effect=d.requests.Timeout
    ), contextlib.redirect_stdout(io.StringIO()):
        assert not d.dispatch_one_job(store.get_job(jid))
    assert store.get_job(jid)["status"] == "SCHEDULED"
    with patch.object(d.requests, "get", return_value=response):
        d.reconcile_scheduled_jobs()
    assert store.get_job(jid)["status"] == "RUNNING"
    assert len(store.get_attempts(jid)) == 1
    print("PASS uncertain start retained then reconciled without duplicate attempt")
    job = store.get_job(jid)
    d.record_confirmed_resume(job, response.json()["job"])
    assert not any(e["event_type"] == "RESUMED" for e in metrics.get_events(jid))
    info = {
        "job_id": jid,
        "pid": 123,
        "confirmed_at": 2,
        "model_restored": True,
        "optimizer_restored": True,
        "restored_global_step": 5,
    }
    worker = {**response.json()["job"], "resume": info}
    d.record_confirmed_resume(job, worker)
    d.record_confirmed_resume(job, worker)
    assert (
        len([e for e in metrics.get_events(jid) if e["event_type"] == "RESUMED"]) == 1
    )
    print("PASS restore acknowledgment required; event deduplicated")
    with patch.object(d, "sync_checkpoint", return_value=False), patch.object(
        d.requests, "get"
    ) as get:
        get.return_value.status_code = 200
        get.return_value.json.return_value = {
            "job": {"status": "COMPLETED", "return_code": 0, "finished_at": 2}
        }
        with contextlib.redirect_stdout(io.StringIO()):
            d.monitor_running_jobs()
    assert store.get_job(jid)["status"] == "RUNNING"
    dest = d.get_central_pytorch_checkpoint_path(jid)
    final = {**expected, "job_id": jid}
    torch.save(final, dest)
    # Corrupt download must never replace the known-good central model.
    before = dest.read_bytes()
    response = MagicMock()
    response.__enter__.return_value = response
    response.status_code = 200
    response.headers = {}
    response.iter_content.return_value = [b"not a checkpoint"]
    with patch.object(d.requests, "get", return_value=response):
        assert not d.sync_pytorch_checkpoint(job, force=True)
    assert dest.read_bytes() == before
    # Repeated identical content must not create new checkpoint events.
    response.iter_content.return_value = [before]
    with patch.object(d.requests, "get", return_value=response):
        assert not d.sync_pytorch_checkpoint(job, force=True)
    assert not any(
        e["event_type"] == "CHECKPOINT_SYNC" for e in metrics.get_events(jid)
    )
    print(
        "PASS corrupt transfer preserves central model; identical transfer deduplicated"
    )
    with patch.object(d, "sync_checkpoint", return_value=False), patch.object(
        d.requests, "get"
    ) as get:
        get.return_value.status_code = 200
        get.return_value.json.return_value = {
            "job": {"status": "COMPLETED", "return_code": 0, "finished_at": 2}
        }
        with contextlib.redirect_stdout(io.StringIO()):
            d.monitor_running_jobs()
    assert store.get_job(jid)["status"] == "COMPLETED"
    print("PASS completion requires valid final central checkpoint")
    # Agent route tests, with process creation mocked and temporary file paths.
    sys.path.insert(0, str(root / "agent"))
    import agent

    client = agent.app.test_client()
    process = Mock(pid=999)
    process.poll.return_value = None
    with patch.object(
        agent.subprocess, "Popen", return_value=process
    ) as launch, patch.object(agent.threading, "Thread"):
        result = client.post(
            "/jobs/start",
            json={
                "job_id": "agent-audit",
                "job_type": "pytorch",
                "attempt_token": "agent-audit:0",
                "checkpoint_every_seconds": 5,
            },
        )
        assert result.status_code == 200, result.json
        command = launch.call_args.args[0]
        assert command[command.index("--checkpoint-every-seconds") + 1] == "5.0"
        assert (
            client.post(
                "/jobs/start",
                json={
                    "job_id": "agent-audit",
                    "job_type": "pytorch",
                    "attempt_token": "agent-audit:0",
                },
            ).status_code
            == 200
        )
        assert launch.call_count == 1
        assert (
            client.post(
                "/jobs/start", json={"job_id": "other", "job_type": "pytorch"}
            ).status_code
            == 409
        )
        launch.call_args.kwargs["stdout"].close()
    agent.job_process = None
    agent.current_job = None
    assert (
        client.post(
            "/jobs/start",
            json={
                "job_id": "bad",
                "job_type": "pytorch",
                "checkpoint_every_seconds": -1,
            },
        ).status_code
        == 400
    )
    # Dummy support still launches the original script by default.
    with patch.object(
        agent.subprocess, "Popen", return_value=process
    ) as launch, patch.object(agent.threading, "Thread"):
        result = client.post(
            "/jobs/start", json={"job_id": "dummy-audit", "duration": 1}
        )
        assert result.status_code == 200 and result.json["job"]["job_type"] == "dummy"
        assert "demo_job.py" in launch.call_args.args[0][1]
        launch.call_args.kwargs["stdout"].close()
    print(
        "PASS agent forwards interval, rejects invalid timing/busy starts, deduplicates starts; dummy compatibility"
    )
    # Comparison jobs use the queue/API but must not enter model-checkpoint paths.
    bench = {
        "job_id": "benchmark-audit",
        "job_type": "benchmark",
        "job_config": {"model": "equation"},
    }
    assert d.build_start_payload(bench)["job_type"] == "benchmark"
    assert d.sync_checkpoint(bench) is False
    agent.job_process = None
    agent.current_job = None
    with patch.object(
        agent.subprocess, "Popen", return_value=process
    ) as launch, patch.object(agent.threading, "Thread"):
        response = client.post("/jobs/start", json=bench)
        assert response.status_code == 200, response.json
        command = launch.call_args.args[0]
        assert command[command.index("--model") + 1] == "equation"
        assert launch.call_args.kwargs["start_new_session"]
        launch.call_args.kwargs["stdout"].close()
    with patch.object(agent.os, "killpg") as kill:
        assert client.post("/jobs/another-job/stop").status_code == 404
        assert not kill.called
        assert client.post("/jobs/benchmark-audit/stop").status_code == 200
        kill.assert_called_once()
    agent.job_process = None
    assert (
        client.post(
            "/jobs/start",
            json={
                "job_id": "bad",
                "job_type": "benchmark",
                "job_config": {"model": "arbitrary-command"},
            },
        ).status_code
        == 400
    )
    import scheduler

    with patch.object(
        scheduler, "load_nodes", return_value=[{"id": "pi01"}, {"id": "pi02"}]
    ), patch.object(
        scheduler,
        "get_resources",
        return_value={
            "node_state": "IDLE",
            "cpu": {"usage_percent": 1},
            "memory": {"available_gb": 4},
            "temperature_c": 40,
        },
    ):
        assert (
            scheduler.select_best_node(allowed_nodes=["pi02"])["node"]["id"] == "pi02"
        )
    result_path = root / "equation-result.json"
    completed = subprocess.run(
        [
            PYTHON,
            str(ROOT / "training/benchmark_job.py"),
            "--model",
            "equation",
            "--output",
            str(result_path),
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert completed.returncode == 0, completed.stderr
    assert json.loads(result_path.read_text())["completed"]
    print(
        "PASS benchmark dispatch, allowlist, bounded stop, placement constraints and real reference execution"
    )
print("All checks passed; isolated temporary DB and checkpoints removed.")
