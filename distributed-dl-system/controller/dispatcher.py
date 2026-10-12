"""Controller loop: place queued work, reconcile starts, collect checkpoints, and recover failed attempts."""

import hashlib

import json

import os

import sqlite3

import time

from pathlib import Path


import requests


from scheduler import select_best_node


from job_store import (
    get_jobs_by_status,
    get_job_environments,
    get_reservations,
    fair_queue,
    finish_cancellation,
    mark_scheduled,
    mark_running,
    mark_completed,
    requeue_after_failure,
    return_to_queue,
)


from checkpoint_store import (
    save_checkpoint,
    load_checkpoint,
)


from recovery_metrics import (
    log_event,
    get_events,
    init_table as init_recovery_table,
)

# Paths / configuration
CONTROLLER_DIR = Path(__file__).resolve().parent

PROJECT_DIR = CONTROLLER_DIR.parent

DB_PATH = CONTROLLER_DIR / "jobs.db"


CENTRAL_CHECKPOINT_DIR = PROJECT_DIR / "checkpoints"

CENTRAL_CHECKPOINT_DIR.mkdir(
    parents=True,
    exist_ok=True,
)


DISPATCH_INTERVAL = 2

FAILURE_THRESHOLD = 3

NODE_COOLDOWN = 20

CHECKPOINT_SYNC_INTERVAL = 1
# Runtime state
job_failure_counts = {}

unhealthy_nodes = {}

last_checkpoint_sync = {}
last_checkpoint_etag = {}
last_checkpoint_hash = {}
final_custom_checkpoint_failures = {}


# Helpers
def get_job_type(job):
    """Treat old database rows without a type as dummy jobs."""
    job_type = job.get("job_type") or "dummy"

    return str(job_type).lower()


def get_job_config(job):
    """Decode extensible JSON job settings while rejecting malformed definitions."""
    config = job.get("job_config")

    if config is None:

        return {}

    if isinstance(config, dict):

        return config

    if isinstance(config, str):

        config = config.strip()

        if not config:

            return {}

        try:

            parsed = json.loads(config)

        except json.JSONDecodeError as error:

            raise ValueError(
                f"Invalid job_config JSON for " f"{job['job_id']}: {error}"
            ) from error

        if not isinstance(parsed, dict):

            raise ValueError(
                f"job_config for {job['job_id']} " "must contain a JSON object"
            )

        return parsed

    raise ValueError(
        f"Unsupported job_config type for "
        f"{job['job_id']}: "
        f"{type(config).__name__}"
    )


def get_central_pytorch_checkpoint_path(job_id):
    """Locate the controller-owned checkpoint that survives an individual worker failure."""
    return CENTRAL_CHECKPOINT_DIR / f"{job_id}.pt"


def update_checkpoint_metadata(
    job_id, checkpoint_path, checkpoint_time, checkpoint_size
):
    """Record the location, size and sync time of the latest central checkpoint."""
    connection = sqlite3.connect(DB_PATH, timeout=5)

    try:

        connection.execute(
            """

            UPDATE jobs

            SET

                checkpoint_path = ?,

                checkpoint_updated_at = ?,

                checkpoint_size_bytes = ?,

                updated_at = ?

            WHERE job_id = ?

            """,
            (
                str(checkpoint_path),
                checkpoint_time,
                checkpoint_size,
                time.time(),
                job_id,
            ),
        )

        connection.commit()

    finally:

        connection.close()


def sha256_file(path):
    """Fingerprint checkpoint bytes so a transferred copy is not counted as new progress."""
    hasher = hashlib.sha256()

    with open(path, "rb") as file:
        while True:
            chunk = file.read(1024 * 1024)

            if not chunk:
                break

            hasher.update(chunk)

    return hasher.hexdigest()


def checkpoint_due(job_id, force=False):
    """Rate-limit checkpoint polling unless a final sync is explicitly requested."""
    if force:

        return True

    now = time.time()

    previous = last_checkpoint_sync.get(job_id, 0)

    return now - previous >= CHECKPOINT_SYNC_INTERVAL


# Dummy checkpoint sync
def sync_dummy_checkpoint(job, force=False):
    """Collect legacy JSON progress without changing dummy-job compatibility."""
    job_id = job["job_id"]

    if not checkpoint_due(job_id, force=force):

        return False

    node_ip = job.get("node_ip")

    if not node_ip:

        return False

    url = f"http://{node_ip}:5000/" f"jobs/{job_id}/checkpoint"

    try:

        response = requests.get(url, timeout=3)

        if response.status_code == 404:

            return False

        response.raise_for_status()

        checkpoint = response.json()["checkpoint"]

        save_checkpoint(job_id, checkpoint)

        now = time.time()

        last_checkpoint_sync[job_id] = now

        progress = checkpoint.get("progress")

        print(
            f"[CHECKPOINT] {job_id}: "
            f"dummy checkpoint synced "
            f"(progress={progress})"
        )

        return True

    except (
        requests.RequestException,
        ValueError,
        KeyError,
    ):

        return False


# PyTorch checkpoint sync
def sync_pytorch_checkpoint(job, force=False):
    """Download, deduplicate and validate bytes before replacing the last usable central model."""
    job_id = job["job_id"]

    if not checkpoint_due(job_id, force=force):
        return False

    node_ip = job.get("node_ip")

    if not node_ip:
        return False

    url = f"http://{node_ip}:5000/" f"jobs/{job_id}/checkpoint" f"?job_type=pytorch"

    destination = get_central_pytorch_checkpoint_path(job_id)

    temporary = Path(str(destination) + ".tmp")

    # Avoid downloading the same checkpoint repeatedly
    # while polling the worker every second.
    headers = {}

    previous_etag = last_checkpoint_etag.get(job_id)

    if previous_etag and not force:
        headers["If-None-Match"] = previous_etag

    try:
        with requests.get(
            url,
            timeout=5,
            stream=True,
            headers=headers,
        ) as response:

            # Worker has no checkpoint yet.
            if response.status_code == 404:
                return False

            # Same worker checkpoint as last poll.
            if response.status_code == 304:
                last_checkpoint_sync[job_id] = time.time()

                return False

            response.raise_for_status()

            response_etag = response.headers.get("ETag")

            response_last_modified = response.headers.get("Last-Modified")

            hasher = hashlib.sha256()

            with open(temporary, "wb") as file:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        file.write(chunk)
                        hasher.update(chunk)
                file.flush()
                os.fsync(file.fileno())

        size_bytes = temporary.stat().st_size

        if size_bytes <= 0:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass

            return False

        checkpoint_hash = hasher.hexdigest()

        previous_hash = last_checkpoint_hash.get(job_id)

        # Dispatcher may have restarted while a valid central
        # checkpoint already exists. Seed the in-memory hash
        # from that file before deciding whether this is new.
        if (
            previous_hash is None
            and destination.exists()
            and destination.stat().st_size > 0
        ):
            previous_hash = sha256_file(destination)

            last_checkpoint_hash[job_id] = previous_hash

        now = time.time()

        # A replacement worker may serve the exact same checkpoint
        # with a different HTTP ETag because it was uploaded there.
        # The SHA-256 check prevents that copy from being counted as
        # a newly-created training checkpoint.
        if previous_hash is not None and checkpoint_hash == previous_hash:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass

            last_checkpoint_sync[job_id] = now

            if response_etag:
                last_checkpoint_etag[job_id] = response_etag

            return False

        read_valid_checkpoint(temporary, job_id)

        # New checkpoint: atomically replace the controller copy.
        os.replace(temporary, destination)

        last_checkpoint_sync[job_id] = now

        last_checkpoint_hash[job_id] = checkpoint_hash

        if response_etag:
            last_checkpoint_etag[job_id] = response_etag

        update_checkpoint_metadata(
            job_id=job_id,
            checkpoint_path=destination,
            checkpoint_time=now,
            checkpoint_size=size_bytes,
        )

        log_event(
            job_id=job_id,
            event_type="CHECKPOINT_SYNC",
            node_id=job.get("node_id"),
            details={
                "checkpoint_path": str(destination),
                "checkpoint_size_bytes": size_bytes,
                "sha256": checkpoint_hash,
                "worker_etag": response_etag,
                "worker_last_modified": response_last_modified,
            },
            event_time=now,
        )

        print(
            f"[CHECKPOINT] {job_id}: "
            f"NEW PyTorch checkpoint synced "
            f"({size_bytes} bytes)"
        )

        return True

    except Exception as error:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass

        print(f"[CHECKPOINT ERROR] " f"{job_id}: {error}")

        return False


def sync_checkpoint(job, force=False):
    """Route recoverable job types to their checkpoint transport; benchmarks do not retry."""
    if get_job_type(job) == 'custom':
        config=get_job_config(job)
        if config['recovery']['mode']=='checkpoint' and checkpoint_due(job['job_id'],force):
            from custom_checkpoints import sync
            result=sync(job)
            last_checkpoint_sync[job['job_id']]=time.time()
            return result
        return False
    if get_job_type(job) == 'benchmark':
        return False

    if get_job_type(job) == "pytorch":

        return sync_pytorch_checkpoint(job, force=force)

    return sync_dummy_checkpoint(job, force=force)


# Node cooldown
def cleanup_unhealthy_nodes():
    """Expire temporary exclusions so recovered workers can become eligible again."""
    now = time.time()

    recovered = []

    for node_id, until in unhealthy_nodes.items():

        if now >= until:

            recovered.append(node_id)

    for node_id in recovered:

        del unhealthy_nodes[node_id]

        print(f"{node_id} removed " "from cooldown")


def get_excluded_nodes():
    """Return the currently cooling-down workers after expiring old entries."""
    cleanup_unhealthy_nodes()

    return set(unhealthy_nodes.keys())


def mark_node_unhealthy(node_id):
    """Temporarily avoid a worker immediately after a failure or uncertain request."""
    if not node_id:

        return

    unhealthy_nodes[node_id] = time.time() + NODE_COOLDOWN

    print(f"{node_id} marked unhealthy " f"for {NODE_COOLDOWN}s")


# PyTorch checkpoint restore
def upload_pytorch_checkpoint(job, node_ip, node_port):
    """Restore controller-owned bytes onto an idle replacement before starting training."""
    job_id = job["job_id"]

    checkpoint_path = get_central_pytorch_checkpoint_path(job_id)

    # First attempt: there is no central .pt yet.

    if not checkpoint_path.exists():

        print(
            f"[CHECKPOINT] {job_id}: "
            "no central PyTorch checkpoint; "
            "starting from epoch 1"
        )

        return True

    if checkpoint_path.stat().st_size <= 0:

        print(f"[CHECKPOINT] {job_id}: " "central checkpoint is empty")

        return False

    url = (
        f"http://{node_ip}:"
        f"{node_port}/jobs/"
        f"{job_id}/checkpoint"
        f"?job_type=pytorch"
    )

    try:

        with open(checkpoint_path, "rb") as checkpoint_file:

            files = {
                "file": (
                    checkpoint_path.name,
                    checkpoint_file,
                    "application/octet-stream",
                )
            }

            response = requests.post(
                url,
                files=files,
                timeout=10,
            )

        if response.status_code != 200:

            print(
                f"[CHECKPOINT] {job_id}: "
                f"replacement upload rejected "
                f"(HTTP {response.status_code})"
            )

            return False

        print(
            f"[CHECKPOINT] {job_id}: " f"uploaded central checkpoint " f"to {node_ip}"
        )

        return True

    except (
        requests.RequestException,
        OSError,
    ) as error:

        print(f"[CHECKPOINT] {job_id}: " f"upload failed: {error}")

        return False


# Build worker start payload
def build_start_payload(job):
    """Translate a stored job into the explicit fields accepted by the worker API."""
    job_id = job["job_id"]

    job_type = get_job_type(job)

    if job_type == "custom":
        config = get_job_config(job)
        if config['execution'] != {'mode':'single','workers':1} or config['recovery']['mode'] not in ('restart','checkpoint'):
            raise ValueError('Only single-worker submissions are supported')
        prepared = next(row['record'] for row in get_job_environments(job_id)
                        if row['node_id'] == job['node_id'])
        return {'job_id':job_id,'job_type':'custom',
                'manifest_sha256':job['storage_manifest_sha256'],
                'environment_key':prepared['environment_key'],
                'resources':config['resources_per_worker'],
                'resume_checkpoint':job.get('resume_checkpoint')}

    if job_type == "benchmark":
        return {
            "job_id": job_id,
            "job_type": "benchmark",
            "job_config": get_job_config(job),
        }

    if job_type == "dummy":

        checkpoint = load_checkpoint(job_id)

        return {
            "job_id": job_id,
            "job_type": "dummy",
            "duration": job["duration"],
            "checkpoint": checkpoint,
        }

    if job_type == "pytorch":

        config = get_job_config(job)

        return {
            "job_id": job_id,
            "job_type": "pytorch",
            "epochs": int(config.get("epochs", 30)),
            "batch_size": int(config.get("batch_size", 128)),
            "learning_rate": float(config.get("learning_rate", 0.001)),
            "checkpoint_interval": int(config.get("checkpoint_interval", 1)),
            "checkpoint_every_seconds": float(
                config.get("checkpoint_every_seconds", 0.0)
            ),
            "batch_delay": float(config.get("batch_delay", 0.0)),
        }

    raise ValueError(f"Unsupported job_type: {job_type}")


# Dispatch
def dispatch_one_job(job):
    """Select a worker, prepare recovery data, and persist the outcome of the start request."""
    config = get_job_config(job)
    custom = get_job_type(job) == 'custom'
    allowed = ([row['node_id'] for row in get_job_environments(job['job_id'])]
               if custom else config.get('allowed_nodes'))
    if custom and config['recovery']['mode']=='checkpoint':
        from custom_checkpoints import metadata
        try:
            saved=metadata(job)
        except Exception as error:
            print(f"[RESTORE_BLOCKED] {job['job_id']}: {error}")
            return False
        if saved:
            allowed=[row['node_id'] for row in get_job_environments(job['job_id'])
                     if row['record']['compatibility_key']==saved['compatibility_key']]
    requirements = config['resources_per_worker'] if custom else None
    selected = select_best_node(
        allowed_nodes=allowed,
        requirements=requirements,
        reserved_nodes={row['node_id'] for row in get_reservations()},
        excluded_nodes=(get_excluded_nodes()),
    )

    if selected is None:

        return False

    node = selected["node"]

    node_id = node["id"]

    node_ip = node["ip"]

    node_port = node["port"]

    job_id = job["job_id"]

    job_type = get_job_type(job)

    print()

    print(f"Scheduling {job_id} " f"({job_type}) on {node_id}")

    try:
        mark_scheduled(job_id, node_id, node_ip, requirements)
    except (sqlite3.IntegrityError, ValueError):
        return False
    # Build custom payloads from the selected prepared worker, not a stale queued row.
    previous_node = job.get('node_id')
    job = {**job, 'node_id':node_id, 'node_ip':node_ip}

    retry_count = int(job.get("retry_count") or 0)

    if custom and config['recovery']['mode']=='checkpoint':
        try:
            from custom_checkpoints import restore
            job['resume_checkpoint']=restore(job,node)
        except Exception as error:
            return_to_queue(job_id,'Could not restore custom checkpoint: '+str(error))
            return False
    # For a PyTorch retry, upload the controller's

    # latest .pt file before starting the worker.

    if job_type == "pytorch":

        uploaded = upload_pytorch_checkpoint(job, node_ip, node_port)

        if not uploaded:

            error = "Could not prepare PyTorch " "checkpoint on selected worker"

            return_to_queue(job_id, error)

            print(f"{job_id} returned " "to queue")

            return False

    # Record a reschedule only after the replacement node has
    # been selected and any PyTorch checkpoint preparation
    # succeeded.
    if retry_count > 0:
        log_event(
            job_id=job_id,
            event_type="RESCHEDULE",
            node_id=node_id,
            details={
                "retry_count": retry_count,
                "previous_node": previous_node,
            },
        )

    try:

        payload = build_start_payload(job)
        payload["attempt_token"] = attempt_token(job)

    except (
        ValueError,
        TypeError,
    ) as error:

        return_to_queue(job_id, str(error))

        print(f"{job_id}: invalid job " f"configuration: {error}")

        return False

    url = f"http://{node_ip}:" f"{node_port}/jobs/start"

    try:

        response = requests.post(
            url,
            json=payload,
            timeout=5,
        )

        if response.status_code == 200:

            data = response.json()

            mark_running(job_id, data["job"]["started_at"])

            job_failure_counts[job_id] = 0

            print(f"{job_id} RUNNING " f"on {node_id}")

            return True

        try:

            worker_error = response.json().get("error")

        except ValueError:

            worker_error = None

        error = f"Worker rejected: " f"HTTP " f"{response.status_code}"

        if worker_error:

            error += f" ({worker_error})"

        return_to_queue(job_id, error)

        print(f"{job_id} returned " "to queue")

        return False

    except requests.RequestException as error:

        mark_node_unhealthy(node_id)

        # Start may already have succeeded. Keep SCHEDULED until the original
        # agent confirms whether this attempt exists; never blindly duplicate it.
        print(f"Dispatch to " f"{node_id} failed.")

        return False


# Monitor running jobs
def monitor_running_jobs():
    """Poll worker attempts, collect progress and finalize or retry jobs according to observed state."""
    running_jobs = get_jobs_by_status("RUNNING")

    active_job_ids = set()

    for job in running_jobs:

        job_id = job["job_id"]

        active_job_ids.add(job_id)

        # Best-effort central checkpoint copy.

        sync_checkpoint(job)

        node_id = job.get("node_id")

        node_ip = job.get("node_ip")

        if not node_ip:

            continue

        url = f"http://{node_ip}:5000/" f"jobs/{job_id}"

        try:

            response = requests.get(url, timeout=2)

            # Agent alive but job missing.

            if response.status_code == 404:

                error = "Worker is alive but " "running job was lost"

                handle_running_job_failure(job, error)

                continue

            response.raise_for_status()

            data = response.json()

            worker_job = data["job"]

            record_confirmed_resume(job, worker_job)

            worker_status = worker_job["status"]

            # Successful response means heartbeat recovered.

            job_failure_counts[job_id] = 0

            if worker_status == "COMPLETED":

                # Force one final central copy, so the final

                # completed=True checkpoint is kept.

                sync_checkpoint(job, force=True)

                if get_job_type(job) == "pytorch":
                    try:
                        final = read_valid_checkpoint(
                            get_central_pytorch_checkpoint_path(job_id), job_id
                        )
                        if not final.get("completed"):
                            raise ValueError("Central checkpoint is not final yet")
                    except Exception as error:
                        print(f"[FINAL_CHECKPOINT_PENDING] {job_id}: {error}")
                        continue

                if get_job_type(job) == 'custom':
                    if get_job_config(job)['recovery']['mode']=='checkpoint':
                        from custom_checkpoints import metadata
                        try:
                            final=metadata(job)
                            if not final or not final['completed'] or final['attempt_token']!=attempt_token(job):
                                raise ValueError('Trainer exited without a validated final SDK checkpoint')
                        except Exception as error:
                            count=final_custom_checkpoint_failures.get(job_id,0)+1
                            final_custom_checkpoint_failures[job_id]=count
                            if count>=3:
                                final_custom_checkpoint_failures.pop(job_id,None)
                                handle_running_job_failure(job,str(error))
                            else:
                                print(f"[FINAL_CUSTOM_CHECKPOINT_PENDING] {job_id}: {error}")
                            continue
                        final_custom_checkpoint_failures.pop(job_id,None)
                    try:
                        from job_artifacts import collect
                        collect(job)
                    except Exception as error:
                        print(f"[ARTIFACTS_PENDING] {job_id}: {error}")
                        continue

                completed = mark_completed(
                    job_id,
                    worker_job.get("return_code", 0),
                    worker_job.get("finished_at"),
                )

                if completed is False:
                    continue
                log_event(
                    job_id=job_id,
                    event_type="COMPLETED",
                    node_id=node_id,
                    details={"return_code": worker_job.get("return_code", 0)},
                    event_time=(worker_job.get("finished_at") or time.time()),
                )

                job_failure_counts.pop(job_id, None)

                last_checkpoint_sync.pop(job_id, None)

                last_checkpoint_etag.pop(
                    job_id,
                    None,
                )

                last_checkpoint_hash.pop(
                    job_id,
                    None,
                )

                print(f"{job_id} " f"COMPLETED on " f"{node_id}")

            elif worker_status == "FAILED":

                # The worker is still reachable, so try one

                # final checkpoint sync before requeueing.

                sync_checkpoint(job, force=True)

                error = "Worker process " "returned FAILED"

                handle_running_job_failure(job, error)

        except requests.RequestException:

            failures = job_failure_counts.get(job_id, 0) + 1

            job_failure_counts[job_id] = failures

            print(
                f"{job_id}: "
                f"lost contact with "
                f"{node_id} "
                f"({failures}/"
                f"{FAILURE_THRESHOLD})"
            )

            # Hold custom allocations during lost contact until the worker confirms
            # an exit or missing attempt. Network-partition fencing arrives later.
            if failures >= FAILURE_THRESHOLD and get_job_type(job) != 'custom':

                error = f"Node {node_id} " "became unreachable"

                handle_running_job_failure(job, error)

    # Remove counters for jobs no longer running.

    stale_jobs = set(job_failure_counts.keys()) - active_job_ids

    for job_id in stale_jobs:

        job_failure_counts.pop(job_id, None)


# Failure handling
def handle_running_job_failure(job, error):
    """Record failure evidence, cool down the worker and apply the retry budget."""
    job_id = job["job_id"]

    node_id = job.get("node_id")

    print()

    print(f"[FAILURE_DETECTED] " f"{job_id}")

    print(f"Node: {node_id}")

    print(f"Reason: {error}")
    # T_detect
    detect_time = time.time()

    log_event(
        job_id=job_id,
        event_type="FAILURE_DETECTED",
        node_id=node_id,
        details={"reason": str(error)},
        event_time=detect_time,
    )

    if get_job_type(job)=='custom':
        try:
            from job_artifacts import collect
            collect(job)
        except Exception as error:
            print(f"[FAILED_ARTIFACTS_UNAVAILABLE] {job_id}: {error}")
    mark_node_unhealthy(node_id)

    new_state = requeue_after_failure(job_id, error)

    job_failure_counts.pop(job_id, None)

    last_checkpoint_sync.pop(job_id, None)

    print(f"{job_id} -> " f"{new_state}")


# Queue processing
def dispatch_queued_jobs():
    """Recompute fair turns after each placement as active counts and last-service change."""
    attempted = set()
    while True:
        queued = [job for job in fair_queue() if job['job_id'] not in attempted]
        if not queued:
            return
        job = queued[0]
        attempted.add(job['job_id'])
        dispatch_one_job(job)


def reconcile_cancellations():
    """Persist cancellation through controller/network failures until the worker stops."""
    from scheduler import load_nodes
    ports = {node['id']:node['port'] for node in load_nodes()}
    for job in get_jobs_by_status('CANCELLING'):
        url = f"http://{job['node_ip']}:{ports.get(job['node_id'],5000)}/jobs/{job['job_id']}"
        try:
            response = requests.get(url,timeout=2)
            if response.status_code == 404:
                # Install a worker tombstone before releasing an uncertain start's
                # slot; a delayed request must not be able to launch afterward.
                stopped = requests.post(url + '/stop',json={'attempt_token':attempt_token(job)},timeout=3)
                stopped.raise_for_status()
                finish_cancellation(job['job_id'])
                continue
            response.raise_for_status()
            worker = response.json()['job']
            if worker.get('attempt_token') != attempt_token(job):
                print(f"[CANCEL_PENDING] {job['job_id']}: original attempt identity not confirmed")
                continue
            if not job.get('started_at'):
                mark_running(job['job_id'],worker.get('started_at'))
            if worker['status'] in ('COMPLETED','FAILED'):
                try:
                    from job_artifacts import collect
                    collect(job)
                except Exception as error:
                    print(f"[CANCEL_ARTIFACTS_UNAVAILABLE] {job['job_id']}: {error}")
                finish_cancellation(job['job_id'],worker.get('return_code'))
                continue
            if worker['status'] == 'RUNNING':
                stopped = requests.post(url + '/stop',json={
                    'attempt_token':attempt_token(job),
                    'force':time.time() - job['cancel_requested_at'] >= 10},timeout=3)
                stopped.raise_for_status()
        except (requests.RequestException,KeyError,ValueError) as error:
            print(f"[CANCEL_PENDING] {job['job_id']}: {error}")


# Main
def attempt_token(job):
    """Identify a particular retry so a repeated HTTP start cannot launch it twice."""
    return f"{job['job_id']}:{int(job.get('retry_count') or 0)}"


def read_valid_checkpoint(path, job_id):
    """Load tensor-safe data and reject wrong-job or incomplete checkpoint structures."""
    import torch

    checkpoint = torch.load(path, map_location="cpu", weights_only=True)
    if checkpoint.get("job_id") != job_id:
        raise ValueError("Checkpoint job ID mismatch")
    if (
        not checkpoint.get("model_state_dict")
        or "optimizer_state_dict" not in checkpoint
    ):
        raise ValueError("Checkpoint missing model/optimizer state")
    return checkpoint


def record_confirmed_resume(job, worker_job):
    """Emit RESUMED only after the trainer confirms both state dictionaries were loaded."""
    info = worker_job.get("resume")
    if get_job_type(job)=='custom' and info:
        expected=worker_job.get('resume_checkpoint') or {}
        if (not expected or info.get('attempt_token')!=attempt_token(job) or not info.get('rng_restored')
            or info.get('checkpoint_sha256')!=expected.get('sha256')
            or info.get('restored_global_step')!=expected.get('global_step')):
            return
    if not info or not info.get("model_restored") or not info.get("optimizer_restored"):
        return
    if info.get("job_id") != job["job_id"] or info.get("pid") != worker_job.get("pid"):
        return
    started_at = worker_job.get("started_at")
    for event in get_events(job["job_id"]):
        if (
            event["event_type"] == "RESUMED"
            and json.loads(event["details_json"]).get("started_at") == started_at
        ):
            return
    log_event(
        job_id=job["job_id"],
        event_type="RESUMED",
        node_id=job["node_id"],
        details={
            **info,
            "started_at": started_at,
            "retry_count": job.get("retry_count", 0),
        },
    )
    print(
        f"[RESUMED] {job['job_id']} confirmed on {job['node_id']} at step {info['restored_global_step']}"
    )


def reconcile_scheduled_jobs():
    """Resolve uncertain starts with the original worker before risking a duplicate launch."""
    for job in get_jobs_by_status("SCHEDULED"):
        try:
            response = requests.get(
                f"http://{job['node_ip']}:5000/jobs/{job['job_id']}", timeout=2
            )
            if response.status_code == 404:
                return_to_queue(job["job_id"], "Agent confirms attempt was not started")
                continue
            response.raise_for_status()
            worker = response.json()["job"]
            if worker.get("attempt_token") == attempt_token(job):
                mark_running(job["job_id"], worker["started_at"])
                print(f"[RECONCILED] {job['job_id']} on {job['node_id']}")
        except (requests.RequestException, ValueError, KeyError) as error:
            print(
                f"[START_UNCERTAIN] {job['job_id']}: waiting for original worker: {error}"
            )


def main():
    """Hold a single-controller lock and repeatedly reconcile, monitor and dispatch work."""
    import fcntl

    lock = open(CONTROLLER_DIR / ".dispatcher.lock", "w")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        raise SystemExit("Another dispatcher is already running")

    init_recovery_table()

    print("=" * 70)

    print("FAILURE-AWARE " "DISTRIBUTED DL DISPATCHER")

    print("=" * 70)

    print(f"Dispatch interval: " f"{DISPATCH_INTERVAL}s")

    print(f"Failure threshold: " f"{FAILURE_THRESHOLD}")

    print(f"Node cooldown: " f"{NODE_COOLDOWN}s")

    print(f"Checkpoint sync: " f"{CHECKPOINT_SYNC_INTERVAL}s")

    print(f"Central checkpoint dir: " f"{CENTRAL_CHECKPOINT_DIR}")

    print()

    while True:

        reconcile_cancellations()
        reconcile_scheduled_jobs()
        monitor_running_jobs()

        dispatch_queued_jobs()

        time.sleep(DISPATCH_INTERVAL)


if __name__ == "__main__":

    main()
