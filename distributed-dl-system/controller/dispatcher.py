import time
import requests

from scheduler import (
    select_best_node
)

from job_store import (
    get_jobs_by_status,
    mark_scheduled,
    mark_running,
    mark_completed,
    requeue_after_failure,
    return_to_queue
)

from checkpoint_store import (
    save_checkpoint,
    load_checkpoint
)


DISPATCH_INTERVAL = 2

FAILURE_THRESHOLD = 3

NODE_COOLDOWN = 20

CHECKPOINT_SYNC_INTERVAL = 4


# Number of consecutive failures
# for each running job
job_failure_counts = {}


# node_id -> time until unhealthy
unhealthy_nodes = {}

last_checkpoint_sync = {}

def sync_checkpoint(job):

    job_id = job["job_id"]

    now = time.time()

    previous = (
        last_checkpoint_sync.get(
            job_id,
            0
        )
    )


    if (
        now - previous
        < CHECKPOINT_SYNC_INTERVAL
    ):
        return


    node_ip = job["node_ip"]

    if not node_ip:
        return


    url = (
        f"http://{node_ip}:5000/"
        f"jobs/{job_id}/checkpoint"
    )


    try:

        response = requests.get(
            url,
            timeout=2
        )


        if response.status_code == 404:
            return


        response.raise_for_status()


        checkpoint = (
            response.json()[
                "checkpoint"
            ]
        )


        save_checkpoint(
            job_id,
            checkpoint
        )


        last_checkpoint_sync[
            job_id
        ] = now


        print(
            f"{job_id}: checkpoint "
            f"synced "
            f"(progress="
            f"{checkpoint.get('progress')})"
        )


    except requests.RequestException:

        pass


def cleanup_unhealthy_nodes():

    now = time.time()

    recovered = []

    for node_id, until in (
        unhealthy_nodes.items()
    ):

        if now >= until:
            recovered.append(
                node_id
            )

    for node_id in recovered:

        del unhealthy_nodes[
            node_id
        ]

        print(
            f"{node_id} removed "
            "from cooldown"
        )


def get_excluded_nodes():

    cleanup_unhealthy_nodes()

    return set(
        unhealthy_nodes.keys()
    )


def mark_node_unhealthy(
    node_id
):

    unhealthy_nodes[
        node_id
    ] = (
        time.time()
        + NODE_COOLDOWN
    )

    print(
        f"{node_id} marked unhealthy "
        f"for {NODE_COOLDOWN}s"
    )


def dispatch_one_job(job):

    selected = select_best_node(
        excluded_nodes=
            get_excluded_nodes()
    )

    if selected is None:
        return False


    node = selected["node"]

    node_id = node["id"]
    node_ip = node["ip"]
    node_port = node["port"]


    print()

    print(
        f"Scheduling "
        f"{job['job_id']} "
        f"on {node_id}"
    )


    mark_scheduled(
        job["job_id"],
        node_id,
        node_ip
    )


    url = (
        f"http://{node_ip}:"
        f"{node_port}/jobs/start"
    )

    checkpoint = load_checkpoint(
    job["job_id"]
    )


    payload = {

        "job_id":
            job["job_id"],

        "duration":
            job["duration"],

        "checkpoint":
            checkpoint

    }


    try:

        response = requests.post(
            url,
            json=payload,
            timeout=3
        )


        if response.status_code == 200:

            data = response.json()

            mark_running(

                job["job_id"],

                data["job"][
                    "started_at"
                ]

            )


            job_failure_counts[
                job["job_id"]
            ] = 0


            print(
                f"{job['job_id']} "
                f"RUNNING on {node_id}"
            )

            return True


        error = (
            f"Worker rejected: "
            f"HTTP "
            f"{response.status_code}"
        )


        return_to_queue(
            job["job_id"],
            error
        )


        print(
            f"{job['job_id']} "
            "returned to queue"
        )


        return False


    except requests.RequestException as error:

        mark_node_unhealthy(
            node_id
        )

        return_to_queue(
            job["job_id"],
            str(error)
        )

        print(
            f"Dispatch to "
            f"{node_id} failed."
        )

        return False


def monitor_running_jobs():

    running_jobs = (
        get_jobs_by_status(
            "RUNNING"
        )
    )


    active_job_ids = set()


    for job in running_jobs:

        sync_checkpoint(job)

        job_id = job[
            "job_id"
        ]

        active_job_ids.add(
            job_id
        )

        node_id = job[
            "node_id"
        ]

        node_ip = job[
            "node_ip"
        ]


        if not node_ip:
            continue


        url = (
            f"http://{node_ip}:5000/"
            f"jobs/{job_id}"
        )


        try:

            response = requests.get(
                url,
                timeout=2
            )


            # Agent alive but job missing
            if response.status_code == 404:

                error = (
                    "Worker is alive but "
                    "running job was lost"
                )

                handle_running_job_failure(
                    job,
                    error
                )

                continue


            response.raise_for_status()

            data = response.json()

            worker_job = data[
                "job"
            ]

            worker_status = (
                worker_job[
                    "status"
                ]
            )


            # Successful response means
            # heartbeat recovered.
            job_failure_counts[
                job_id
            ] = 0


            if (
                worker_status
                == "COMPLETED"
            ):

                mark_completed(

                    job_id,

                    worker_job.get(
                        "return_code",
                        0
                    ),

                    worker_job.get(
                        "finished_at"
                    )

                )


                job_failure_counts.pop(
                    job_id,
                    None
                )


                print(
                    f"{job_id} "
                    f"COMPLETED on "
                    f"{node_id}"
                )


            elif (
                worker_status
                == "FAILED"
            ):

                error = (
                    "Worker process "
                    "returned FAILED"
                )

                handle_running_job_failure(
                    job,
                    error
                )


        except requests.RequestException:

            failures = (
                job_failure_counts.get(
                    job_id,
                    0
                )
                + 1
            )

            job_failure_counts[
                job_id
            ] = failures


            print(
                f"{job_id}: "
                f"lost contact with "
                f"{node_id} "
                f"({failures}/"
                f"{FAILURE_THRESHOLD})"
            )


            if (
                failures
                >= FAILURE_THRESHOLD
            ):

                error = (
                    f"Node {node_id} "
                    "became unreachable"
                )

                handle_running_job_failure(
                    job,
                    error
                )


    # Remove counters for jobs
    # no longer running
    stale_jobs = (
        set(
            job_failure_counts.keys()
        )
        - active_job_ids
    )

    for job_id in stale_jobs:

        job_failure_counts.pop(
            job_id,
            None
        )


def handle_running_job_failure(
    job,
    error
):

    job_id = job[
        "job_id"
    ]

    node_id = job[
        "node_id"
    ]


    print()
    print(
        f"FAILURE DETECTED: "
        f"{job_id}"
    )

    print(
        f"Node: {node_id}"
    )

    print(
        f"Reason: {error}"
    )


    mark_node_unhealthy(
        node_id
    )


    new_state = (
        requeue_after_failure(
            job_id,
            error
        )
    )


    job_failure_counts.pop(
        job_id,
        None
    )


    print(
        f"{job_id} -> "
        f"{new_state}"
    )


def dispatch_queued_jobs():

    queued_jobs = (
        get_jobs_by_status(
            "QUEUED"
        )
    )


    for job in queued_jobs:

        success = (
            dispatch_one_job(
                job
            )
        )


        if not success:

            # If no worker was available
            # during this cycle, stop.
            break


def main():

    print("=" * 70)

    print(
        "FAILURE-AWARE "
        "DISTRIBUTED DL DISPATCHER"
    )

    print("=" * 70)

    print(
        f"Dispatch interval: "
        f"{DISPATCH_INTERVAL}s"
    )

    print(
        f"Failure threshold: "
        f"{FAILURE_THRESHOLD}"
    )

    print(
        f"Node cooldown: "
        f"{NODE_COOLDOWN}s"
    )

    print()


    while True:

        monitor_running_jobs()

        dispatch_queued_jobs()

        time.sleep(
            DISPATCH_INTERVAL
        )


if __name__ == "__main__":
    main()