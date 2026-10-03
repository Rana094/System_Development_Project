import sqlite3
import os
import time


BASE_DIR = os.path.dirname(
    os.path.dirname(
        os.path.abspath(__file__)
    )
)

DB_FILE = os.path.join(
    BASE_DIR,
    "controller",
    "jobs.db"
)


def get_connection():
    connection = sqlite3.connect(
        DB_FILE,
        timeout=10
    )

    connection.row_factory = sqlite3.Row

    connection.execute(
        "PRAGMA journal_mode=WAL"
    )

    connection.execute(
        "PRAGMA synchronous=NORMAL"
    )

    connection.execute(
        "PRAGMA foreign_keys=ON"
    )

    return connection


def column_exists(
    connection,
    table,
    column
):
    cursor = connection.cursor()

    cursor.execute(
        f"PRAGMA table_info({table})"
    )

    columns = [
        row["name"]
        for row in cursor.fetchall()
    ]

    return column in columns


def initialize_database():
    connection = get_connection()
    cursor = connection.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS jobs (
            job_id TEXT PRIMARY KEY,
            status TEXT NOT NULL,
            duration INTEGER NOT NULL,
            node_id TEXT,
            node_ip TEXT,
            created_at REAL NOT NULL,
            scheduled_at REAL,
            started_at REAL,
            finished_at REAL,
            return_code INTEGER,
            error TEXT
        )
        """
    )

    # Safe migration of old DB
    migrations = [
        (
            "retry_count",
            "INTEGER NOT NULL DEFAULT 0"
        ),
        (
            "max_retries",
            "INTEGER NOT NULL DEFAULT 2"
        ),
        (
            "last_error",
            "TEXT"
        ),
        (
            "updated_at",
            "REAL"
        )
    ]

    for column, definition in migrations:
        if not column_exists(
            connection,
            "jobs",
            column
        ):
            cursor.execute(
                f"""
                ALTER TABLE jobs
                ADD COLUMN {column} {definition}
                """
            )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS job_attempts (
            attempt_id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id TEXT NOT NULL,
            attempt_number INTEGER NOT NULL,
            node_id TEXT,
            node_ip TEXT,
            status TEXT NOT NULL,
            started_at REAL,
            finished_at REAL,
            return_code INTEGER,
            error TEXT,
            FOREIGN KEY(job_id)
                REFERENCES jobs(job_id)
        )
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS
        idx_jobs_status
        ON jobs(status)
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS
        idx_attempts_job
        ON job_attempts(job_id)
        """
    )

    connection.commit()
    connection.close()


def add_job(
    job_id,
    duration,
    max_retries=2
):
    now = time.time()

    connection = get_connection()
    cursor = connection.cursor()

    cursor.execute(
        """
        INSERT INTO jobs (
            job_id,
            status,
            duration,
            created_at,
            retry_count,
            max_retries,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            job_id,
            "QUEUED",
            duration,
            now,
            0,
            max_retries,
            now
        )
    )

    connection.commit()
    connection.close()


def get_job(job_id):
    connection = get_connection()
    cursor = connection.cursor()

    cursor.execute(
        """
        SELECT *
        FROM jobs
        WHERE job_id = ?
        """,
        (job_id,)
    )

    row = cursor.fetchone()
    connection.close()

    return (
        dict(row)
        if row
        else None
    )


def get_jobs_by_status(status):
    connection = get_connection()
    cursor = connection.cursor()

    cursor.execute(
        """
        SELECT *
        FROM jobs
        WHERE status = ?
        ORDER BY created_at ASC
        """,
        (status,)
    )

    rows = cursor.fetchall()
    connection.close()

    return [
        dict(row)
        for row in rows
    ]


def get_all_jobs():
    connection = get_connection()
    cursor = connection.cursor()

    cursor.execute(
        """
        SELECT *
        FROM jobs
        ORDER BY created_at DESC
        """
    )

    rows = cursor.fetchall()
    connection.close()

    return [
        dict(row)
        for row in rows
    ]


def mark_scheduled(
    job_id,
    node_id,
    node_ip
):
    now = time.time()

    connection = get_connection()
    cursor = connection.cursor()

    cursor.execute(
        """
        UPDATE jobs
        SET
            status = ?,
            node_id = ?,
            node_ip = ?,
            scheduled_at = ?,
            updated_at = ?
        WHERE job_id = ?
        """,
        (
            "SCHEDULED",
            node_id,
            node_ip,
            now,
            now,
            job_id
        )
    )

    connection.commit()
    connection.close()


def mark_running(
    job_id,
    started_at=None
):
    if started_at is None:
        started_at = time.time()

    job = get_job(job_id)

    attempt_number = (
        job["retry_count"] + 1
    )

    connection = get_connection()
    cursor = connection.cursor()

    cursor.execute(
        """
        UPDATE jobs
        SET
            status = ?,
            started_at = ?,
            finished_at = NULL,
            return_code = NULL,
            error = NULL,
            last_error = NULL,
            updated_at = ?
        WHERE job_id = ?
        """,
        (
            "RUNNING",
            started_at,
            time.time(),
            job_id
        )
    )

    cursor.execute(
        """
        INSERT INTO job_attempts (
            job_id,
            attempt_number,
            node_id,
            node_ip,
            status,
            started_at
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            job_id,
            attempt_number,
            job["node_id"],
            job["node_ip"],
            "RUNNING",
            started_at
        )
    )

    connection.commit()
    connection.close()


def mark_completed(
    job_id,
    return_code=0,
    finished_at=None
):
    if finished_at is None:
        finished_at = time.time()

    connection = get_connection()
    cursor = connection.cursor()

    cursor.execute(
        """
        UPDATE jobs
        SET
            status = ?,
            finished_at = ?,
            return_code = ?,
            updated_at = ?
        WHERE job_id = ?
        """,
        (
            "COMPLETED",
            finished_at,
            return_code,
            time.time(),
            job_id
        )
    )

    cursor.execute(
        """
        UPDATE job_attempts
        SET
            status = ?,
            finished_at = ?,
            return_code = ?
        WHERE attempt_id = (
            SELECT attempt_id
            FROM job_attempts
            WHERE job_id = ?
            ORDER BY attempt_id DESC
            LIMIT 1
        )
        """,
        (
            "COMPLETED",
            finished_at,
            return_code,
            job_id
        )
    )

    connection.commit()
    connection.close()


def requeue_after_failure(
    job_id,
    error
):
    job = get_job(job_id)

    new_retry_count = (
        job["retry_count"] + 1
    )

    connection = get_connection()
    cursor = connection.cursor()

    # Close current attempt
    cursor.execute(
        """
        UPDATE job_attempts
        SET
            status = ?,
            finished_at = ?,
            error = ?
        WHERE attempt_id = (
            SELECT attempt_id
            FROM job_attempts
            WHERE job_id = ?
            ORDER BY attempt_id DESC
            LIMIT 1
        )
        """,
        (
            "FAILED",
            time.time(),
            error,
            job_id
        )
    )

    if (
        new_retry_count
        <= job["max_retries"]
    ):
        cursor.execute(
            """
            UPDATE jobs
            SET
                status = ?,
                retry_count = ?,
                node_id = NULL,
                node_ip = NULL,
                scheduled_at = NULL,
                started_at = NULL,
                finished_at = NULL,
                return_code = NULL,
                error = NULL,
                last_error = ?,
                updated_at = ?
            WHERE job_id = ?
            """,
            (
                "QUEUED",
                new_retry_count,
                error,
                time.time(),
                job_id
            )
        )

        final_state = "QUEUED"

    else:
        cursor.execute(
            """
            UPDATE jobs
            SET
                status = ?,
                retry_count = ?,
                finished_at = ?,
                error = ?,
                last_error = ?,
                updated_at = ?
            WHERE job_id = ?
            """,
            (
                "FAILED",
                new_retry_count,
                time.time(),
                error,
                error,
                time.time(),
                job_id
            )
        )

        final_state = "FAILED"

    connection.commit()
    connection.close()

    return final_state


def return_to_queue(
    job_id,
    error
):
    connection = get_connection()
    cursor = connection.cursor()

    cursor.execute(
        """
        UPDATE jobs
        SET
            status = ?,
            node_id = NULL,
            node_ip = NULL,
            scheduled_at = NULL,
            last_error = ?,
            updated_at = ?
        WHERE job_id = ?
        """,
        (
            "QUEUED",
            error,
            time.time(),
            job_id
        )
    )

    connection.commit()
    connection.close()


def get_attempts(job_id):
    connection = get_connection()
    cursor = connection.cursor()

    cursor.execute(
        """
        SELECT *
        FROM job_attempts
        WHERE job_id = ?
        ORDER BY attempt_id ASC
        """,
        (job_id,)
    )

    rows = cursor.fetchall()
    connection.close()

    return [
        dict(row)
        for row in rows
    ]


initialize_database()