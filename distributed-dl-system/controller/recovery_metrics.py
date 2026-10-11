import argparse
import json
import sqlite3
import time
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "jobs.db"


def get_connection():
    connection = sqlite3.connect(DB_PATH)
    connection.row_factory = sqlite3.Row
    return connection


def init_table():
    with get_connection() as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS recovery_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,

                job_id TEXT NOT NULL,

                event_type TEXT NOT NULL,

                node_id TEXT,

                event_time REAL NOT NULL,

                details_json TEXT NOT NULL DEFAULT '{}'
            )
            """
        )

        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_recovery_events_job
            ON recovery_events(job_id)
            """
        )

        connection.commit()


def log_event(
    job_id,
    event_type,
    node_id=None,
    details=None,
    event_time=None,
):
    init_table()

    if event_time is None:
        event_time = time.time()

    if details is None:
        details = {}

    with get_connection() as connection:
        connection.execute(
            """
            INSERT INTO recovery_events (
                job_id,
                event_type,
                node_id,
                event_time,
                details_json
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                job_id,
                event_type,
                node_id,
                event_time,
                json.dumps(details),
            ),
        )

        connection.commit()

    return event_time


def get_events(job_id):
    init_table()

    with get_connection() as connection:
        rows = connection.execute(
            """
            SELECT *
            FROM recovery_events
            WHERE job_id = ?
            ORDER BY event_time ASC
            """,
            (job_id,),
        ).fetchall()

    return [dict(row) for row in rows]


def show_events(job_id):
    events = get_events(job_id)

    print()
    print("=" * 90)
    print(f"RECOVERY EVENTS — {job_id}")
    print("=" * 90)

    if not events:
        print("No events found.")
        return

    for event in events:
        print(
            f"{event['event_time']:.6f}  "
            f"{event['event_type']:<22} "
            f"{event['node_id'] or '-':<8} "
            f"{event['details_json']}"
        )

    print("=" * 90)


def main():
    parser = argparse.ArgumentParser()

    subparsers = parser.add_subparsers(
        dest="command",
        required=True,
    )

    subparsers.add_parser("init")

    show_parser = subparsers.add_parser(
        "show"
    )

    show_parser.add_argument(
        "job_id"
    )

    record_parser = subparsers.add_parser(
        "record"
    )

    record_parser.add_argument(
        "job_id"
    )

    record_parser.add_argument(
        "event_type"
    )

    record_parser.add_argument(
        "--node"
    )

    args = parser.parse_args()

    if args.command == "init":
        init_table()

        print(
            "recovery_events table ready."
        )

    elif args.command == "show":
        show_events(
            args.job_id
        )

    elif args.command == "record":
        timestamp = log_event(
            args.job_id,
            args.event_type,
            node_id=args.node,
        )

        print(
            f"{args.event_type}: "
            f"{timestamp:.6f}"
        )


if __name__ == "__main__":
    main()
