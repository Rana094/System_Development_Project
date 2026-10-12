"""Choose an eligible idle worker by lowest CPU usage, then greatest available RAM."""

import json
import requests

CONFIG_FILE = "config/nodes.json"


def load_nodes():
    """Read the configured worker inventory used by this process."""
    with open(CONFIG_FILE, "r") as file:
        return json.load(file)["nodes"]


def get_resources(node):
    """Fetch a worker snapshot; an unreachable worker is ineligible for this scheduling pass."""
    url = f"http://{node['ip']}:{node['port']}/resources"

    try:
        response = requests.get(url, timeout=1.5)
        response.raise_for_status()
        return response.json()

    except requests.RequestException:
        return None


def select_best_node(
    excluded_nodes=None,
    allowed_nodes=None,
):
    """Filter unavailable, busy or excluded nodes, then rank the remaining resources."""
    if excluded_nodes is None:
        excluded_nodes = set()

    nodes = load_nodes()
    candidates = []

    for node in nodes:
        # Optional placement constraints keep benchmark hardware comparable.
        if allowed_nodes is not None and node["id"] not in allowed_nodes:
            continue

        if node["id"] in excluded_nodes:

            print(f"{node['id']} skipped " "(temporarily unhealthy)")

            continue

        resources = get_resources(node)

        if resources is None:
            print(f"{node['id']} unavailable")
            continue

        if resources["node_state"] != "IDLE":
            print(f"{node['id']} skipped " f"({resources['node_state']})")
            continue

        print(
            f"[SCHEDULER] candidate={node['id']} "
            f"cpu={resources['cpu']['usage_percent']}% "
            f"available_ram={resources['memory']['available_gb']}GB"
        )
        candidates.append(
            {
                "node": node,
                "cpu": resources["cpu"]["usage_percent"],
                "ram": resources["memory"]["available_gb"],
                "temperature": resources["temperature_c"],
            }
        )

    if not candidates:
        return None

    candidates.sort(key=lambda item: (item["cpu"], -item["ram"]))

    return candidates[0]


if __name__ == "__main__":
    selected = select_best_node()

    if selected is None:
        print("No suitable node available.")

    else:
        node = selected["node"]

        print()
        print("Selected node:", node["id"])
        print("IP:", node["ip"])
        print("CPU usage:", selected["cpu"], "%")
        print("Available RAM:", selected["ram"], "GB")
        print("Temperature:", selected["temperature"], "C")
