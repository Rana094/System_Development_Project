"""Worker HTTP API: report resources, launch one process at a time, and transfer checkpoints."""

import json
import math
import os
import re
import socket
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

import psutil
from flask import Flask, jsonify, request, send_file

app = Flask(__name__)
# Configuration
INTERFACE = "eth0"

BASE_DIR = Path(__file__).resolve().parent.parent
TRAINING_DIR = BASE_DIR / "training"
CHECKPOINT_DIR = BASE_DIR / "checkpoints"
LOG_DIR = BASE_DIR / "logs"

DUMMY_JOB_SCRIPT = TRAINING_DIR / "demo_job.py"
PYTORCH_JOB_SCRIPT = TRAINING_DIR / "pytorch_job.py"

CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)
# Runtime state
last_network = None
last_network_time = None

job_lock = threading.Lock()

current_job = None
job_process = None
job_history = {}


# Helpers
def valid_job_id(job_id):
    """
    Allow only safe filename-friendly job IDs.
    This prevents values such as ../../something from becoming paths.
    """
    return bool(
        isinstance(job_id, str)
        and job_id not in {".", ".."}
        and 1 <= len(job_id) <= 128
        and re.fullmatch(r"[A-Za-z0-9._-]+", job_id)
    )


def cancellation_path(job_id):
    return BASE_DIR / 'worker-storage' / 'cancellations' / (job_id + '.json')


def attempt_cancelled(job_id, token):
    """Read a durable tombstone so delayed or replayed HTTP starts cannot undo a stop."""
    path = cancellation_path(job_id)
    if not path.exists():
        return False
    return token in json.loads(path.read_text())['attempt_tokens']


def remember_cancellation(job_id, token):
    """The caller holds job_lock, serializing tombstones with the final launch check."""
    path = cancellation_path(job_id)
    path.parent.mkdir(parents=True,exist_ok=True)
    tokens = json.loads(path.read_text())['attempt_tokens'] if path.exists() else []
    if token not in tokens:
        write_json_atomic(path,{'attempt_tokens':[*tokens,token]})
        for directory in (path.parent,path.parent.parent):
            descriptor = os.open(directory,os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)


def checkpoint_path_for(job_id, job_type):
    """Select the local file extension used by the job type."""
    extension = ".pt" if job_type == "pytorch" else ".json"
    return CHECKPOINT_DIR / f"{job_id}{extension}"


def write_json_atomic(path, data):
    """
    Atomically replace a JSON checkpoint.
    """
    tmp_path = Path(str(path) + ".tmp")

    with open(tmp_path, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=2)
        file.flush()
        os.fsync(file.fileno())

    os.replace(tmp_path, path)


def write_binary_atomic(path, binary_stream=None, raw_bytes=None):
    """
    Atomically write a binary checkpoint uploaded by the controller.
    """
    tmp_path = Path(str(path) + ".upload.tmp")

    try:
        with open(tmp_path, "wb") as file:
            if binary_stream is not None:
                while True:
                    chunk = binary_stream.read(1024 * 1024)

                    if not chunk:
                        break

                    file.write(chunk)

            elif raw_bytes is not None:
                file.write(raw_bytes)

            else:
                raise ValueError("No checkpoint data provided")

            file.flush()
            os.fsync(file.fileno())

        os.replace(tmp_path, path)

    finally:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError:
                pass


def resolve_job_type(job_id):
    """
    Resolve the known type for a job, then fall back to existing files.
    """
    with job_lock:
        if current_job and current_job.get("job_id") == job_id:
            job_type = current_job.get("job_type")

            if job_type in {"dummy", "pytorch"}:
                return job_type

        history = job_history.get(job_id)

        if history:
            job_type = history.get("job_type")

            if job_type in {"dummy", "pytorch"}:
                return job_type

    if checkpoint_path_for(job_id, "pytorch").exists():
        return "pytorch"

    if checkpoint_path_for(job_id, "dummy").exists():
        return "dummy"

    return None


def get_ip_address():
    """Report the worker address observed by its outbound socket."""
    interfaces = psutil.net_if_addrs()

    if INTERFACE not in interfaces:
        return None

    for address in interfaces[INTERFACE]:
        if address.family == socket.AF_INET:
            return address.address

    return None


def get_temperature():
    """Read the Pi thermal sensor when available, otherwise report no temperature."""
    try:
        with open(
            "/sys/class/thermal/thermal_zone0/temp",
            "r",
            encoding="utf-8",
        ) as file:
            value = float(file.read().strip())

        return round(value / 1000, 2)

    except Exception:
        return None


def get_network_speed():
    """Convert counter differences since the previous sample into measured Mbps."""
    global last_network
    global last_network_time

    counters = psutil.net_io_counters(pernic=True)

    if INTERFACE not in counters:
        return {
            "upload_mbps": 0,
            "download_mbps": 0,
        }

    current = counters[INTERFACE]
    current_time = time.monotonic()

    if last_network is None or last_network_time is None:
        last_network = current
        last_network_time = current_time

        return {
            "upload_mbps": 0,
            "download_mbps": 0,
        }

    elapsed = current_time - last_network_time

    if elapsed <= 0:
        return {
            "upload_mbps": 0,
            "download_mbps": 0,
        }

    sent_difference = current.bytes_sent - last_network.bytes_sent
    received_difference = current.bytes_recv - last_network.bytes_recv

    upload_mbps = sent_difference * 8 / elapsed / 1_000_000

    download_mbps = received_difference * 8 / elapsed / 1_000_000

    last_network = current
    last_network_time = current_time

    return {
        "upload_mbps": round(upload_mbps, 3),
        "download_mbps": round(download_mbps, 3),
    }


def gpu_count():
    """Discover CUDA devices without importing a student's tensor library into Flask."""
    try:
        result = subprocess.run(['nvidia-smi','--query-gpu=index','--format=csv,noheader'],
                                capture_output=True,text=True,timeout=2)
        return len(result.stdout.strip().splitlines()) if result.returncode == 0 else 0
    except (OSError,subprocess.TimeoutExpired):
        return 0


GPU_COUNT = gpu_count()


# Job watcher
def watch_job(process, job_id, log_file):
    """Wait for the child to exit, then release the busy slot and preserve its final status."""
    global current_job
    global job_process
    global job_history

    process.wait()
    # A launcher can exit before a child that ignores SIGTERM. Clean up remaining
    # members of its process group before advertising the worker slot as idle.
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    log_file.close()

    with job_lock:
        status = "COMPLETED" if process.returncode == 0 else "FAILED"

        if (
            current_job
            and current_job.get("job_id") == job_id
            and current_job.get("pid") == process.pid
        ):
            current_job["status"] = status
            current_job["return_code"] = process.returncode
            current_job["finished_at"] = time.time()

            job_history[job_id] = current_job.copy()

        if job_process is process:
            job_process = None


# Health
@app.route("/health", methods=["GET"])
def health():
    """Provide a minimal liveness response that does not launch work."""
    return jsonify(
        {
            "node": socket.gethostname(),
            "status": "ONLINE",
        }
    )


# Resource information
@app.route("/resources", methods=["GET"])
def resources():
    """Return machine load and a lock-protected snapshot of current worker state."""
    memory = psutil.virtual_memory()
    disk = psutil.disk_usage("/")
    cpu_frequency = psutil.cpu_freq()
    network = get_network_speed()

    with job_lock:
        job_info = current_job.copy() if current_job else None

    node_status = (
        "BUSY" if (job_info and job_info.get("status") == "RUNNING") else "IDLE"
    )

    return jsonify(
        {
            "node": socket.gethostname(),
            "ip": get_ip_address(),
            "status": "ONLINE",
            "node_state": node_status,
            "slots": {"total":1,"available":int(node_status == "IDLE")},
            "allocation": (job_info.get("resources", {}) if node_status == "BUSY" else {}),
            "gpu": {"total":GPU_COUNT,"available":GPU_COUNT if node_status == "IDLE" else 0},
            "cpu": {
                "available_cores": len(os.sched_getaffinity(0)) if node_status == "IDLE" else 0,
                "usage_percent": psutil.cpu_percent(interval=0.2),
                "logical_cores": psutil.cpu_count(logical=True),
                "physical_cores": psutil.cpu_count(logical=False),
                "frequency_mhz": (
                    round(cpu_frequency.current, 2) if cpu_frequency else None
                ),
            },
            "memory": {
                "total_gb": round(
                    memory.total / (1024**3),
                    2,
                ),
                "available_gb": round(
                    memory.available / (1024**3),
                    2,
                ),
                "used_gb": round(
                    memory.used / (1024**3),
                    2,
                ),
                "usage_percent": memory.percent,
            },
            "disk": {
                "total_gb": round(
                    disk.total / (1024**3),
                    2,
                ),
                "free_gb": round(
                    disk.free / (1024**3),
                    2,
                ),
                "usage_percent": disk.percent,
            },
            "network": {
                "interface": INTERFACE,
                "upload_mbps": network["upload_mbps"],
                "download_mbps": network["download_mbps"],
            },
            "temperature_c": get_temperature(),
            "current_job": job_info,
            "timestamp": time.time(),
        }
    )


# Start job
@app.route("/jobs/start", methods=["POST"])
def start_job():
    """Validate supported workloads and launch a single tracked process group on this worker."""
    global current_job
    global job_process
    global job_history

    data = request.get_json(silent=True) or {}

    job_id = data.get("job_id")

    if not valid_job_id(job_id):
        return (
            jsonify(
                {
                    "error": (
                        "job_id is required and may contain only "
                        "letters, numbers, '.', '_' and '-'"
                    )
                }
            ),
            400,
        )

    job_type = str(data.get("job_type", "dummy")).strip().lower()

    if job_type not in {"dummy", "pytorch", "benchmark", "custom"}:
        return (
            jsonify(
                {
                    "error": (
                        "Unsupported job_type. "
                        "Use 'dummy', 'pytorch', 'benchmark' or 'custom'."
                    )
                }
            ),
            400,
        )

    if job_type == 'custom' and not re.fullmatch(re.escape(job_id) + r':[0-9]+',str(data.get('attempt_token',''))):
        return jsonify({'error':'Custom start requires a job_id:retry attempt token'}),400

    with job_lock:
        if attempt_cancelled(job_id,data.get('attempt_token')):
            return jsonify({'error':'Attempt has been cancelled'}),409
        previous = job_history.get(job_id)
        if previous and data.get("attempt_token") and previous.get("attempt_token") == data["attempt_token"]:
            return jsonify({"job":previous,"message":"Already started"}),200
        if job_process is not None and job_process.poll() is None:
            if (
                current_job.get("job_id") == job_id
                and data.get("attempt_token")
                and current_job.get("attempt_token") == data["attempt_token"]
            ):
                return jsonify({"job": current_job, "message": "Already started"}), 200
            return (
                jsonify({"error": "Node is already busy", "current_job": current_job}),
                409,
            )
    # Build dummy job
    if job_type == "dummy":
        try:
            duration = int(data.get("duration", 20))

            checkpoint_interval = int(
                data.get(
                    "checkpoint_interval",
                    5,
                )
            )

        except (TypeError, ValueError):
            return (
                jsonify(
                    {"error": ("duration and checkpoint_interval " "must be integers")}
                ),
                400,
            )

        if duration < 1 or duration > 300:
            return jsonify({"error": ("duration must be 1-300 seconds")}), 400

        if checkpoint_interval < 1:
            return jsonify({"error": ("checkpoint_interval must be >= 1")}), 400

        checkpoint_path = checkpoint_path_for(
            job_id,
            "dummy",
        )

        checkpoint_data = data.get("checkpoint")

        if checkpoint_data is not None:
            if not isinstance(
                checkpoint_data,
                dict,
            ):
                return (
                    jsonify({"error": ("dummy checkpoint must be " "a JSON object")}),
                    400,
                )

            try:
                write_json_atomic(
                    checkpoint_path,
                    checkpoint_data,
                )

            except Exception as error:
                return (
                    jsonify(
                        {"error": ("Failed to save dummy checkpoint: " f"{error}")}
                    ),
                    500,
                )

        command = [
            sys.executable,
            str(DUMMY_JOB_SCRIPT),
            "--job-id",
            job_id,
            "--duration",
            str(duration),
            "--checkpoint",
            str(checkpoint_path),
            "--checkpoint-interval",
            str(checkpoint_interval),
        ]

        job_details = {
            "duration": duration,
            "checkpoint_interval": checkpoint_interval,
        }
    elif job_type == "custom":
        try:
            directory = BASE_DIR / 'worker-storage' / 'jobs' / job_id
            prepared = json.loads((directory / 'prepared.json').read_text())
            descriptor = json.loads((directory / 'request.json').read_text())
            spec = descriptor['manifest']['execution_spec']
            if (prepared['manifest_sha256'] != data['manifest_sha256']
                or prepared['environment_key'] != data['environment_key']
                or prepared['job_id'] != job_id
                or descriptor['manifest_sha256'] != data['manifest_sha256']):
                raise ValueError('Prepared snapshot does not match controller placement')
            if spec['execution'] != {'mode':'single','workers':1} or spec['recovery']['mode'] not in ('restart','checkpoint'):
                raise ValueError('This execution mode is not available yet')
            resume=data.get('resume_checkpoint')
            if resume:
                from checkpoint_protocol import validate
                info=validate(directory/'checkpoints/resume.dlckpt',job_id,prepared['manifest_sha256'])
                if (spec['recovery']['mode']!='checkpoint' or info['compatibility_key']!=prepared['compatibility_key'] or info['sha256']!=resume['sha256']
                    or info['global_step']!=resume['global_step']):
                    raise ValueError('Uploaded resume checkpoint differs from start request')
            allocation = spec['resources_per_worker']
            if allocation != data['resources']:
                raise ValueError('Resource request does not match staged submission')
        except (OSError,ValueError,KeyError,TypeError) as error:
            return jsonify({'error':str(error)}),400
        checkpoint_path = directory / 'checkpoints'
        command = [sys.executable,str(BASE_DIR / 'agent/custom_runner.py'),
                   '--root',str(BASE_DIR / 'worker-storage'),'--job-id',job_id,
                   '--attempt-token',data['attempt_token']]
        resume=data.get('resume_checkpoint')
        if resume:
            command += ['--resume-sha256',resume['sha256']]
        job_details = {'resources':allocation,'environment_key':prepared['environment_key'],
                       'manifest_sha256':prepared['manifest_sha256'],'resume_checkpoint':resume}
    # Reference workloads continue to use their bundled launcher.

    elif job_type == "benchmark":
        config = data.get("job_config", {})
        if not isinstance(config, dict) or config.get("model") not in {
            "equation",
            "logistic",
            "numpy_mlp",
            "resnet18",
            "tiny_cnn",
            "logistic_train",
            "numpy_mlp_train",
            "resnet18_train",
            "tiny_cnn_train",
        }:
            return jsonify({"error": "Unknown benchmark model"}), 400
        try:
            values = {
                key: int(config.get(key, default))
                for key, default in (
                    ("samples", 1000),
                    ("epochs", 1),
                    ("rank", 0),
                    ("world_size", 1),
                    ("master_port", 29651),
                )
            }
            if not (
                1 <= values["samples"] <= 1000000
                and 1 <= values["epochs"] <= 1000
                and 1 <= values["world_size"] <= 3
                and 0 <= values["rank"] < values["world_size"]
                and 1024 <= values["master_port"] <= 65535
            ):
                raise ValueError("Invalid benchmark bounds")
        except (TypeError, ValueError):
            return jsonify({"error": "Invalid benchmark numeric settings"}), 400
        checkpoint_path = checkpoint_path_for(job_id, "dummy")
        command = [
            sys.executable,
            str(TRAINING_DIR / "benchmark_job.py"),
            "--model",
            config["model"],
            "--output",
            str(LOG_DIR / f"{job_id}.benchmark.json"),
            "--master-addr",
            str(config.get("master_addr", "127.0.0.1")),
        ]
        for key, value in values.items():
            command.extend(["--" + key.replace("_", "-"), str(value)])
        job_details = {"benchmark_config": config}

    # PyTorch jobs keep the existing recoverable training path.
    else:
        try:
            epochs = int(data.get("epochs", 20))

            batch_size = int(data.get("batch_size", 128))

            learning_rate = float(data.get("learning_rate", 0.001))

            checkpoint_interval = int(
                data.get(
                    "checkpoint_interval",
                    1,
                )
            )

            checkpoint_every_seconds = float(data.get("checkpoint_every_seconds", 0.0))

            batch_delay = float(data.get("batch_delay", 0.0))

        except (TypeError, ValueError):
            return (
                jsonify(
                    {
                        "error": (
                            "epochs, batch_size, learning_rate, "
                            "checkpoint_interval and batch_delay "
                            "must be numeric"
                        )
                    }
                ),
                400,
            )

        if not math.isfinite(checkpoint_every_seconds) or checkpoint_every_seconds < 0:
            return (
                jsonify({"error": "checkpoint_every_seconds must be finite and >= 0"}),
                400,
            )

        if epochs < 1:
            return jsonify({"error": "epochs must be >= 1"}), 400

        if batch_size < 1:
            return jsonify({"error": "batch_size must be >= 1"}), 400

        if learning_rate <= 0:
            return jsonify({"error": "learning_rate must be > 0"}), 400

        if checkpoint_interval < 1:
            return jsonify({"error": ("checkpoint_interval must be >= 1")}), 400

        if batch_delay < 0:
            return jsonify({"error": "batch_delay must be >= 0"}), 400

        checkpoint_path = checkpoint_path_for(
            job_id,
            "pytorch",
        )

        command = [
            sys.executable,
            str(PYTORCH_JOB_SCRIPT),
            "--job-id",
            job_id,
            "--epochs",
            str(epochs),
            "--batch-size",
            str(batch_size),
            "--learning-rate",
            str(learning_rate),
            "--checkpoint",
            str(checkpoint_path),
            "--checkpoint-interval",
            str(checkpoint_interval),
            "--checkpoint-every-seconds",
            str(checkpoint_every_seconds),
            "--resume-status-file",
            str(LOG_DIR / f"{job_id}.resume.json"),
            "--batch-delay",
            str(batch_delay),
        ]

        job_details = {
            "epochs": epochs,
            "batch_size": batch_size,
            "learning_rate": learning_rate,
            "checkpoint_interval": checkpoint_interval,
            "batch_delay": batch_delay,
            "checkpoint_every_seconds": checkpoint_every_seconds,
        }

    script_path = (
        BASE_DIR / 'agent/custom_runner.py' if job_type == 'custom' else
        TRAINING_DIR / "benchmark_job.py"
        if job_type == "benchmark"
        else DUMMY_JOB_SCRIPT if job_type == "dummy" else PYTORCH_JOB_SCRIPT
    )

    if not script_path.exists():
        return (
            jsonify({"error": (f"Training script not found: " f"{script_path}")}),
            500,
        )
    # Start process
    with job_lock:
        if attempt_cancelled(job_id,data.get('attempt_token')):
            return jsonify({'error':'Attempt has been cancelled'}),409
        if job_process is not None and job_process.poll() is None:
            return (
                jsonify(
                    {
                        "error": "Node is already busy",
                        "current_job": current_job,
                    }
                ),
                409,
            )

        if job_type == 'custom':
            # Recheck admission while holding the slot lock; the scheduler's earlier
            # snapshot may be stale. RAM is admission accounting, not a cgroup limit.
            if (allocation['cpu_cores'] > len(os.sched_getaffinity(0))
                or allocation['memory_gb'] > psutil.virtual_memory().available / 1024**3
                or allocation['gpus'] > GPU_COUNT):
                return jsonify({'error':'Insufficient CPU, RAM or GPU capacity'}),409

        if job_type == 'custom':
            for name in ('latest.dlckpt','resume-status.json'):
                (directory / 'checkpoints' / name).unlink(missing_ok=True)
        (LOG_DIR / f"{job_id}.resume.json").unlink(missing_ok=True)
        log_path = LOG_DIR / f"{job_id}.log"

        try:
            log_file = open(
                log_path,
                "w",
                encoding="utf-8",
                buffering=1,
            )

            job_process = subprocess.Popen(
                command,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                cwd=str(BASE_DIR),
                start_new_session=True,
            )

        except Exception as error:
            try:
                log_file.close()
            except Exception:
                pass

            job_process = None

            return jsonify({"error": ("Failed to start job: " f"{error}")}), 500

        current_job = {
            "job_id": job_id,
            "job_type": job_type,
            "status": "RUNNING",
            "pid": job_process.pid,
            "attempt_token": data.get("attempt_token"),
            "started_at": time.time(),
            "checkpoint": str(checkpoint_path),
            "log": str(log_path),
            **job_details,
        }

        job_history[job_id] = current_job.copy()

        watcher = threading.Thread(
            target=watch_job,
            args=(
                job_process,
                job_id,
                log_file,
            ),
            daemon=True,
        )

        watcher.start()

        return (
            jsonify(
                {
                    "message": "Job started",
                    "node": socket.gethostname(),
                    "job": current_job,
                }
            ),
            200,
        )


@app.route('/jobs/<job_id>/log', methods=['GET'])
def download_job_log(job_id):
    """Expose the tracked stdout/stderr file for terminal inspection and collection."""
    if not valid_job_id(job_id):
        return jsonify({'error':'Invalid job ID'}),400
    path = LOG_DIR / (job_id + '.log')
    if not path.is_file():
        return jsonify({'error':'Log not found'}),404
    return send_file(path, mimetype='text/plain', conditional=True)


# Job status
@app.route("/jobs/status", methods=["GET"])
def job_status():
    """Expose the current worker job without consulting the controller database."""
    with job_lock:
        if current_job is None:
            return jsonify({"current_job": None})

        return jsonify({"current_job": current_job})


def job_with_resume(job):
    """Attach trainer acknowledgments only when their process identity matches this attempt."""
    result = job.copy()
    try:
        info = json.loads((LOG_DIR / f"{job['job_id']}.resume.json").read_text())
        if (
            info.get("pid") == job.get("pid")
            and info.get("confirmed_at", 0) >= job["started_at"]
        ):
            result["resume"] = info
    except (OSError, ValueError, KeyError):
        pass
    if job.get('job_type') == 'custom':
        try:
            info=json.loads((BASE_DIR/'worker-storage/jobs'/job['job_id']/'checkpoints/resume-status.json').read_text())
            if (info.get('attempt_token')==job.get('attempt_token') and info.get('pid')==job.get('pid')
                and info.get('confirmed_at',0)>=job['started_at']):
                result['resume']=info
        except (OSError,ValueError,KeyError):
            pass
    if job.get("job_type") == "benchmark":
        try:
            result["benchmark_result"] = json.loads(
                (LOG_DIR / f"{job['job_id']}.benchmark.json").read_text()
            )
        except (OSError, ValueError):
            pass
    return result


@app.route(
    "/jobs/<job_id>",
    methods=["GET"],
)
def get_job(job_id):
    """Look up active or recent in-memory attempt state and attach trainer acknowledgment."""
    if not valid_job_id(job_id):
        return jsonify({"error": "Invalid job_id"}), 400

    with job_lock:
        if current_job and current_job.get("job_id") == job_id:
            return jsonify({"job": job_with_resume(current_job)})

        if job_id in job_history:
            return jsonify({"job": job_with_resume(job_history[job_id])})

    return jsonify({"error": "Job not found"}), 404


@app.route("/jobs/<job_id>/stop", methods=["POST"])
def stop_job(job_id):
    """Cancel a specific attempt durably and stop only its matching process group."""
    if not valid_job_id(job_id):
        return jsonify({'error':'Invalid job ID'}),400
    data = request.get_json(silent=True) or {}
    token = data.get('attempt_token')
    if token is not None and not re.fullmatch(re.escape(job_id) + r':[0-9]+',str(token)):
        return jsonify({'error':'Invalid attempt token'}),400
    if 'force' in data and type(data['force']) is not bool:
        return jsonify({'error':'force must be a boolean'}),400
    with job_lock:
        if not current_job or current_job['job_id'] != job_id:
            if token:
                remember_cancellation(job_id,token)
                return jsonify({'message':'Attempt cancelled before launch','job_id':job_id})
            return jsonify({'error':'Job not active on this worker'}),404
        if token and token != current_job.get('attempt_token'):
            return jsonify({'error':'Attempt token mismatch'}),409
        if token:
            remember_cancellation(job_id,token)
        if job_process is not None and job_process.poll() is None:
            try:
                os.killpg(job_process.pid,signal.SIGKILL if data.get('force') else signal.SIGTERM)
            except ProcessLookupError:
                pass
        return jsonify({'message':'Stop requested','job_id':job_id})


@app.route('/jobs/<job_id>/custom-checkpoint',methods=['GET','POST'])
def custom_checkpoint(job_id):
    """Transport validated SDK archives without importing student tensor libraries into Flask."""
    if not valid_job_id(job_id):
        return jsonify({'error':'Invalid job ID'}),400
    from checkpoint_protocol import validate
    directory=BASE_DIR/'worker-storage/jobs'/job_id/'checkpoints'
    try:
        record=json.loads((directory.parent/'prepared.json').read_text())
        manifest=record['manifest_sha256']
    except (OSError,ValueError,KeyError):
        return jsonify({'error':'Prepared job not found'}),404
    if request.method=='GET':
        path=directory/'latest.dlckpt'
        if not path.is_file(): return jsonify({'error':'Checkpoint not found'}),404
        try:
            validate(path,job_id,manifest,request.args.get('attempt_token'))
            return send_file(path,mimetype='application/octet-stream',conditional=True)
        except Exception as error:
            return jsonify({'error':str(error)}),422
    with job_lock:
        if job_process is not None and job_process.poll() is None:
            return jsonify({'error':'Worker is busy'}),409
        if request.headers.get('X-Manifest-SHA256')!=manifest or 'file' not in request.files:
            return jsonify({'error':'Snapshot identity and checkpoint file are required'}),400
        directory.mkdir(parents=True,exist_ok=True)
        import tempfile
        temporary=None
        try:
            with tempfile.NamedTemporaryFile(dir=directory,delete=False) as stream:
                temporary=Path(stream.name)
                while True:
                    chunk=request.files['file'].stream.read(1024*1024)
                    if not chunk: break
                    stream.write(chunk)
                stream.flush();os.fsync(stream.fileno())
            info=validate(temporary,job_id,manifest)
            if info['compatibility_key']!=record['compatibility_key']:
                raise ValueError('Checkpoint environment differs from prepared worker')
            if info['sha256']!=request.headers.get('X-Resume-SHA256'):
                raise ValueError('Uploaded checkpoint hash differs from the controller copy')
            os.replace(temporary,directory/'resume.dlckpt')
            descriptor=os.open(directory,os.O_RDONLY|os.O_DIRECTORY)
            try: os.fsync(descriptor)
            finally: os.close(descriptor)
            return jsonify({'checkpoint':info})
        except Exception as error:
            return jsonify({'error':str(error)}),422
        finally:
            if temporary: temporary.unlink(missing_ok=True)


# Checkpoints
@app.route(
    "/jobs/<job_id>/checkpoint",
    methods=["GET"],
)
def get_checkpoint(job_id):
    """
    Dummy job:
        returns the old JSON response:
        {"checkpoint": {...}}

    PyTorch job:
        returns the .pt file as binary.
    """
    if not valid_job_id(job_id):
        return jsonify({"error": "Invalid job_id"}), 400

    requested_type = request.args.get("job_type")

    if requested_type:
        requested_type = requested_type.strip().lower()
        if requested_type not in {"dummy", "pytorch"}:
            return jsonify({"error": "job_type must be 'dummy' or 'pytorch'"}), 400
        job_type = requested_type
    else:
        job_type = resolve_job_type(job_id)

    if job_type is None:
        return jsonify({"error": "Checkpoint not found"}), 404

    checkpoint_path = checkpoint_path_for(
        job_id,
        job_type,
    )

    if not checkpoint_path.exists():
        return jsonify({"error": "Checkpoint not found"}), 404

    if job_type == "pytorch":
        return send_file(
            checkpoint_path,
            mimetype="application/octet-stream",
            as_attachment=True,
            download_name=checkpoint_path.name,
            conditional=True,
        )

    try:
        with open(
            checkpoint_path,
            "r",
            encoding="utf-8",
        ) as file:
            data = json.load(file)

        return jsonify({"checkpoint": data})

    except Exception as error:
        return jsonify({"error": str(error)}), 500


@app.route(
    "/jobs/<job_id>/checkpoint",
    methods=["POST"],
)
def upload_checkpoint(job_id):
    """
    Upload a controller-held checkpoint to this worker
    before starting/resuming the replacement job.

    PyTorch recommended request:
        POST /jobs/<job_id>/checkpoint?job_type=pytorch
        multipart field: file

    Dummy jobs can send:
        {"checkpoint": {...}}
    """
    if not valid_job_id(job_id):
        return jsonify({"error": "Invalid job_id"}), 400

    with job_lock:
        if job_process is not None and job_process.poll() is None:
            return (
                jsonify(
                    {
                        "error": "Cannot upload checkpoint while node is busy",
                        "current_job": current_job,
                    }
                ),
                409,
            )

    requested_type = (
        request.args.get("job_type")
        or request.form.get("job_type")
        or request.headers.get("X-Job-Type")
    )

    if requested_type:
        job_type = requested_type.strip().lower()

    else:
        job_type = resolve_job_type(job_id)

        if job_type is None and "file" in request.files:
            filename = (request.files["file"].filename or "").lower()

            if filename.endswith((".pt", ".pth")):
                job_type = "pytorch"

            elif filename.endswith(".json"):
                job_type = "dummy"

        if job_type is None:
            if request.is_json:
                job_type = "dummy"

            elif request.mimetype == "application/octet-stream":
                job_type = "pytorch"

    if job_type not in {"dummy", "pytorch"}:
        return (
            jsonify(
                {
                    "error": (
                        "Unable to determine checkpoint type. "
                        "Specify ?job_type=dummy or "
                        "?job_type=pytorch."
                    )
                }
            ),
            400,
        )

    checkpoint_path = checkpoint_path_for(
        job_id,
        job_type,
    )

    try:
        if job_type == "dummy":
            payload = request.get_json(silent=True)

            if payload is None:
                return jsonify({"error": ("Dummy checkpoint must be JSON")}), 400

            checkpoint_data = payload.get(
                "checkpoint",
                payload,
            )

            if not isinstance(
                checkpoint_data,
                dict,
            ):
                return (
                    jsonify({"error": ("Dummy checkpoint must be " "a JSON object")}),
                    400,
                )

            write_json_atomic(
                checkpoint_path,
                checkpoint_data,
            )

        else:
            if "file" in request.files:
                uploaded_file = request.files["file"]

                write_binary_atomic(
                    checkpoint_path,
                    binary_stream=uploaded_file.stream,
                )

            else:
                raw_data = request.get_data(cache=False)

                if not raw_data:
                    return (
                        jsonify({"error": ("No PyTorch checkpoint " "data received")}),
                        400,
                    )

                write_binary_atomic(
                    checkpoint_path,
                    raw_bytes=raw_data,
                )

        return (
            jsonify(
                {
                    "message": "Checkpoint uploaded",
                    "job_id": job_id,
                    "job_type": job_type,
                    "checkpoint": str(checkpoint_path),
                    "size_bytes": checkpoint_path.stat().st_size,
                    "node": socket.gethostname(),
                }
            ),
            200,
        )

    except Exception as error:
        return jsonify({"error": ("Failed to save checkpoint: " f"{error}")}), 500


# Main
if __name__ == "__main__":
    print(f"Starting Node Agent: " f"{socket.gethostname()}")

    print(f"Base directory: {BASE_DIR}")
    print(f"Python: {sys.executable}")

    app.run(
        host="0.0.0.0",
        port=5000,
        debug=False,
        threaded=True,
    )
