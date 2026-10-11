import argparse
import sqlite3
from pathlib import Path


CONTROLLER_DIR = Path(__file__).resolve().parent
DB_PATH = CONTROLLER_DIR / "jobs.db"


def get_connection():
    connection = sqlite3.connect(DB_PATH, timeout=5)
    connection.row_factory = sqlite3.Row
    return connection


def load_events(job_id):
    with get_connection() as connection:
        rows = connection.execute(
            """
            SELECT
                id,
                job_id,
                event_type,
                node_id,
                event_time,
                details_json
            FROM recovery_events
            WHERE job_id = ?
            ORDER BY event_time ASC, id ASC
            """,
            (job_id,),
        ).fetchall()

    return [dict(row) for row in rows]


def first_event_after(events, event_type, after_time):
    for event in events:
        if (
            event["event_type"] == event_type
            and event["event_time"] >= after_time
        ):
            return event
    return None


def last_event_before(events, event_type, before_time):
    matches = [
        event
        for event in events
        if (
            event["event_type"] == event_type
            and event["event_time"] <= before_time
        )
    ]

    if not matches:
        return None

    return matches[-1]


def print_timestamp(label, event):
    if event is None:
        print(f"{label:<24}: MISSING")
        return

    node = event.get("node_id") or "-"

    print(
        f"{label:<24}: "
        f"{event['event_time']:.6f} "
        f"(node={node})"
    )


def print_metric(label, start_event, end_event):
    if start_event is None or end_event is None:
        print(f"{label:<24}: unavailable")
        return None

    value = end_event["event_time"] - start_event["event_time"]

    print(
        f"{label:<24}: "
        f"{value:.3f} s"
    )

    return value


def build_report(job_id):
    events = load_events(job_id)

    if not events:
        print(f"No recovery events found for job {job_id}.")
        return 1

    failure_events = [
        event
        for event in events
        if event["event_type"] == "FAILURE_INJECTED"
    ]

    if not failure_events:
        print(
            f"No FAILURE_INJECTED event found for job {job_id}."
        )
        print(
            "Record the injected failure before stopping the worker."
        )
        return 1

    failure = failure_events[-1]
    failure_time = failure["event_time"]

    checkpoint = last_event_before(
        events,
        "CHECKPOINT_SYNC",
        failure_time,
    )

    detected = first_event_after(
        events,
        "FAILURE_DETECTED",
        failure_time,
    )

    search_after = (
        detected["event_time"]
        if detected
        else failure_time
    )

    rescheduled = first_event_after(
        events,
        "RESCHEDULE",
        search_after,
    )

    search_after = (
        rescheduled["event_time"]
        if rescheduled
        else search_after
    )

    resumed = first_event_after(
        events,
        "RESUMED",
        search_after,
    )

    search_after = (
        resumed["event_time"]
        if resumed
        else search_after
    )

    completed = first_event_after(
        events,
        "COMPLETED",
        search_after,
    )

    print()
    print("=" * 78)
    print("RECOVERY REPORT")
    print("=" * 78)
    print(f"Job ID                  : {job_id}")
    print()

    print("TIMESTAMPS")
    print("-" * 78)

    print_timestamp("T_checkpoint", checkpoint)
    print_timestamp("T_failure", failure)
    print_timestamp("T_detect", detected)
    print_timestamp("T_reschedule", rescheduled)
    print_timestamp("T_resume", resumed)
    print_timestamp("T_completed", completed)

    print()
    print("RECOVERY METRICS")
    print("-" * 78)

    print_metric(
        "Failure detection time",
        failure,
        detected,
    )

    print_metric(
        "Rescheduling delay",
        detected,
        rescheduled,
    )

    print_metric(
        "Resume delay",
        rescheduled,
        resumed,
    )

    print_metric(
        "Total recovery time",
        failure,
        resumed,
    )

    print_metric(
        "Checkpoint age",
        checkpoint,
        failure,
    )

    print_metric(
        "Post-failure finish time",
        failure,
        completed,
    )

    print()
    print("NODE TRANSITION")
    print("-" * 78)

    failed_node = failure.get("node_id") or "-"
    replacement_node = (
        resumed.get("node_id")
        if resumed
        else "-"
    )

    print(f"Failed node              : {failed_node}")
    print(f"Replacement node         : {replacement_node}")

    if (
        resumed is not None
        and failed_node != "-"
        and replacement_node != "-"
    ):
        print(
            f"Cross-node recovery      : "
            f"{'YES' if failed_node != replacement_node else 'NO'}"
        )

    print("=" * 78)

    missing = []

    required = {
        "CHECKPOINT_SYNC": checkpoint,
        "FAILURE_DETECTED": detected,
        "RESCHEDULE": rescheduled,
        "RESUMED": resumed,
        "COMPLETED": completed,
    }

    for name, event in required.items():
        if event is None:
            missing.append(name)

    if missing:
        print()
        print(
            "WARNING: missing event(s): "
            + ", ".join(missing)
        )
        return 2

    return 0


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Calculate fault-recovery metrics "
            "from controller recovery_events."
        )
    )

    parser.add_argument(
        "job_id",
        help="Job ID to analyse",
    )

    args = parser.parse_args()

    raise SystemExit(
        build_report(args.job_id)
    )


if __name__ == "__main__":
    main()
