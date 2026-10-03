import json
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

import requests


CONFIG_FILE = "config/nodes.json"

POLL_INTERVAL = 3
FAILURE_THRESHOLD = 3


# ==========================================
# Load cluster configuration
# ==========================================

def load_nodes():

    with open(CONFIG_FILE, "r") as file:
        config = json.load(file)

    return config["nodes"]


nodes = load_nodes()


# ==========================================
# Runtime state of every node
# ==========================================

node_states = {}

for node in nodes:

    node_states[node["id"]] = {

        "status": "UNKNOWN",

        "failed_heartbeats": 0,

        "last_seen": None

    }


# ==========================================
# Query one Node Agent
# ==========================================

def check_node(node):

    node_id = node["id"]

    ip = node["ip"]

    port = node["port"]

    url = f"http://{ip}:{port}/resources"

    try:

        response = requests.get(
            url,
            timeout=1.5
        )

        response.raise_for_status()

        resources = response.json()


        # Successful heartbeat

        node_states[node_id][
            "failed_heartbeats"
        ] = 0

        node_states[node_id][
            "status"
        ] = "ONLINE"

        node_states[node_id][
            "last_seen"
        ] = datetime.now().strftime(
            "%H:%M:%S"
        )

        return node_id, resources


    except requests.RequestException:

        # Heartbeat failed

        node_states[node_id][
            "failed_heartbeats"
        ] += 1

        failures = node_states[node_id][
            "failed_heartbeats"
        ]

        if failures >= FAILURE_THRESHOLD:

            node_states[node_id][
                "status"
            ] = "OFFLINE"

        else:

            node_states[node_id][
                "status"
            ] = "SUSPECT"

        return node_id, None


# ==========================================
# Display cluster status
# ==========================================

def display_cluster(results):

    # Clear terminal

    print("\033[2J\033[H", end="")


    print("=" * 115)

    print(
        "DISTRIBUTED FAULT-TOLERANT "
        "DEEP LEARNING CLUSTER"
    )

    print("=" * 115)


    print(
        f"{'NODE':<8}"
        f"{'STATUS':<12}"
        f"{'CPU':<10}"
        f"{'RAM FREE':<13}"
        f"{'DISK FREE':<13}"
        f"{'TEMP':<10}"
        f"{'DOWNLOAD':<12}"
        f"{'UPLOAD':<12}"
        f"{'JOB':<10}"
    )


    print("-" * 115)


    for node_id, data in results:

        state = node_states[node_id]


        if data is not None:

            cpu = (
                f"{data['cpu']['usage_percent']}%"
            )

            ram = (
                f"{data['memory']['available_gb']} GB"
            )

            disk = (
                f"{data['disk']['free_gb']} GB"
            )


            if data["temperature_c"] is not None:

                temperature = (
                    f"{data['temperature_c']} C"
                )

            else:

                temperature = "-"


            download = (
                f"{data['network']['download_mbps']}"
            )

            upload = (
                f"{data['network']['upload_mbps']}"
            )


            if data["current_job"]:

                job = str(
                    data["current_job"]
                )

            else:

                job = "-"


        else:

            cpu = "-"
            ram = "-"
            disk = "-"
            temperature = "-"
            download = "-"
            upload = "-"
            job = "-"


        print(
            f"{node_id:<8}"
            f"{state['status']:<12}"
            f"{cpu:<10}"
            f"{ram:<13}"
            f"{disk:<13}"
            f"{temperature:<10}"
            f"{download:<12}"
            f"{upload:<12}"
            f"{job:<10}"
        )


    print("-" * 115)

    print(
        f"Polling every {POLL_INTERVAL}s | "
        f"OFFLINE after "
        f"{FAILURE_THRESHOLD} failed heartbeats"
    )

    print("=" * 115)


# ==========================================
# Main Controller
# ==========================================

def main():

    while True:

        with ThreadPoolExecutor(
            max_workers=len(nodes)
        ) as executor:

            results = list(
                executor.map(
                    check_node,
                    nodes
                )
            )

        display_cluster(results)

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":

    main()
