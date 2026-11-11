"""Choose an eligible idle worker by lowest CPU usage, then greatest available RAM."""

from pathlib import Path
import os
import json
import requests
from scheduler_metrics import begin_selection

CONFIG_FILE = Path(__file__).resolve().parents[1] / "config/nodes.json"
if os.environ.get('DL_NODES_CONFIG'):
    CONFIG_FILE=Path(os.environ['DL_NODES_CONFIG'])


def load_nodes():
    """Read the configured worker inventory used by this process."""
    if os.environ.get('DL_NODES_CONFIG'):
        from hybrid_storage.linux_boundary import protected_json
        value=protected_json(CONFIG_FILE)
        if set(value)!={'nodes'} or not isinstance(value['nodes'],list):raise ValueError('Invalid protected worker inventory')
        return value['nodes']
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


def resources_fit(snapshot, requested):
    """Check capacity as well as load; a low-load worker can still be too small."""
    return (
        snapshot.get("node_state") == "IDLE"
        and snapshot["cpu"].get("available_cores", snapshot["cpu"].get("logical_cores", 0)) >= requested.get("cpu_cores", 0)
        and snapshot["memory"]["available_gb"] >= requested.get("memory_gb", 0)
        and snapshot.get("gpu", {}).get("available", 0) >= requested.get("gpus", 0)
    )


def select_best_node(
    excluded_nodes=None,
    allowed_nodes=None,
    requirements=None,
    reserved_nodes=None,
    *,
    locality=None,
):
    """Filter unavailable, busy or excluded nodes, then rank the remaining resources."""
    if locality is not None:
        from hybrid_storage.locality import JobLocality
        if not isinstance(locality, JobLocality):raise TypeError('Expected explicit job-bound JobLocality')
        locality.reset()
    measurement=begin_selection('single')
    if excluded_nodes is None:
        excluded_nodes = set()

    requirements = requirements or {}
    reserved_nodes = reserved_nodes or set()
    nodes = load_nodes()
    candidates = []

    for node in nodes:
        if measurement is not None:measurement.examined+=1
        # Optional placement constraints keep benchmark hardware comparable.
        if allowed_nodes is not None and node["id"] not in allowed_nodes:
            if measurement is not None:measurement.skip('not_allowed',node['id'])
            continue

        if node["id"] in reserved_nodes:
            if measurement is not None:measurement.skip('reserved',node['id'])
            continue

        if node["id"] in excluded_nodes:
            if measurement is not None:measurement.skip('excluded',node['id'])

            print(f"{node['id']} skipped " "(temporarily unhealthy)")

            continue

        if measurement is not None:measurement.queries+=1
        resources = get_resources(node)

        if resources is None:
            if measurement is not None:measurement.skip('unavailable',node['id'])
            print(f"{node['id']} unavailable")
            continue

        if resources["node_state"] != "IDLE":
            if measurement is not None:measurement.skip('busy',node['id'])
            print(f"{node['id']} skipped " f"({resources['node_state']})")
            continue

        if not resources_fit(resources, requirements):
            if measurement is not None:measurement.skip('resources',node['id'])
            continue

        if measurement is not None:
            measurement.eligible+=1
            measurement.candidate(node['id'],resources['cpu']['usage_percent'],resources['memory']['available_gb'])

        print(
            f"[SCHEDULER] candidate={node['id']} "
            f"cpu={resources['cpu']['usage_percent']}% "
            f"available_ram={resources['memory']['available_gb']}GB"
        )
        candidates.append(
            {
                "node": node,
                "resources": resources,
                "cpu": resources["cpu"]["usage_percent"],
                "ram": resources["memory"]["available_gb"],
                "temperature": resources["temperature_c"],
            }
        )

    if not candidates:
        if locality is not None:locality.single([])
        if measurement is not None:measurement.finish([])
        return None

    candidates.sort(key=lambda item: (item["cpu"], -item["ram"]))

    selected = candidates[0] if locality is None else locality.single(candidates)
    if measurement is not None:
        measurement.finish([selected['node']],policy='resource' if locality is None else locality.snapshot().policy)
    return selected


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
