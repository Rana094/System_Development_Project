import json
import requests


CONFIG_FILE = "config/nodes.json"


def load_nodes():
    with open(CONFIG_FILE, "r") as file:
        return json.load(file)["nodes"]


def get_resources(node):
    url = f"http://{node['ip']}:{node['port']}/resources"

    try:
        response = requests.get(url, timeout=1.5)
        response.raise_for_status()
        return response.json()

    except requests.RequestException:
        return None


def select_best_node(
    excluded_nodes=None
):

    if excluded_nodes is None:
        excluded_nodes = set()

    nodes = load_nodes()
    candidates = []

    for node in nodes:

        if node["id"] in excluded_nodes:

            print(
                f"{node['id']} skipped "
                "(temporarily unhealthy)"
            )

            continue
        
        resources = get_resources(node)

        if resources is None:
            print(f"{node['id']} unavailable")
            continue

        if resources["node_state"] != "IDLE":
            print(
                f"{node['id']} skipped "
                f"({resources['node_state']})"
            )
            continue

        candidates.append({
            "node": node,
            "cpu": resources["cpu"]["usage_percent"],
            "ram": resources["memory"]["available_gb"],
            "temperature": resources["temperature_c"]
        })

    if not candidates:
        return None

    candidates.sort(
        key=lambda item: (
            item["cpu"],
            -item["ram"]
        )
    )

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

