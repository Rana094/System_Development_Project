"""SQLite job and attempt state. Each transition commits its related updates together."""

import json
import sqlite3
import os
import time

from queue_policy import ACTIVE_STATES, PENDING_STATES, fair_order, identity, limits, load_policy

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DB_FILE = os.path.join(BASE_DIR, "controller", "jobs.db")


def get_connection():
    """Open the local database with the connection settings used by this module."""
    connection = sqlite3.connect(DB_FILE, timeout=10)

    connection.row_factory = sqlite3.Row

    connection.execute("PRAGMA journal_mode=WAL")

    connection.execute("PRAGMA synchronous=NORMAL")

    connection.execute("PRAGMA foreign_keys=ON")

    return connection


def column_exists(connection, table, column):
    """Check the schema before applying a backward-compatible migration."""
    cursor = connection.cursor()

    cursor.execute(f"PRAGMA table_info({table})")

    columns = [row["name"] for row in cursor.fetchall()]

    return column in columns


def initialize_database():
    """Create missing tables and columns without discarding existing jobs or attempts."""
    connection = get_connection()
    cursor = connection.cursor()

    cursor.execute("""
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
        """)

    # Safe migration of old DB
    migrations = [
        ("job_type", "TEXT NOT NULL DEFAULT 'dummy'"),
        ("job_config", "TEXT NOT NULL DEFAULT '{}'"),
        ("checkpoint_path", "TEXT"),
        ("checkpoint_updated_at", "REAL"),
        ("checkpoint_size_bytes", "INTEGER"),
        ("retry_count", "INTEGER NOT NULL DEFAULT 0"),
        ("max_retries", "INTEGER NOT NULL DEFAULT 2"),
        ("last_error", "TEXT"),
        ("updated_at", "REAL"),
        ("owner", "TEXT"),
        ("storage_path", "TEXT"),
        ("storage_manifest_sha256", "TEXT"),
        ("staged_at", "REAL"),
        ("queued_at", "REAL"),
        ("queue_reason", "TEXT"),
        ("cancel_requested_at", "REAL"),
    ]

    for column, definition in migrations:
        if not column_exists(connection, "jobs", column):
            cursor.execute(f"""
                ALTER TABLE jobs
                ADD COLUMN {column} {definition}
                """)

    cursor.execute("""
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
        """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS
        idx_jobs_status
        ON jobs(status)
        """)

    cursor.execute("""CREATE TABLE IF NOT EXISTS job_environments (
        job_id TEXT NOT NULL, node_id TEXT NOT NULL, profile TEXT NOT NULL,
        environment_key TEXT NOT NULL, record_json TEXT NOT NULL,
        prepared_at REAL NOT NULL, verified_at REAL NOT NULL,
        PRIMARY KEY(job_id,node_id), FOREIGN KEY(job_id) REFERENCES jobs(job_id)
    )""")

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS
        idx_attempts_job
        ON job_attempts(job_id)
        """)

    cursor.execute("""CREATE TABLE IF NOT EXISTS resource_reservations (
        node_id TEXT PRIMARY KEY, job_id TEXT NOT NULL UNIQUE,
        resources_json TEXT NOT NULL, created_at REAL NOT NULL,
        FOREIGN KEY(job_id) REFERENCES jobs(job_id)
    )""")
    cursor.execute("""CREATE TABLE IF NOT EXISTS owner_service (
        owner TEXT PRIMARY KEY, last_dispatch REAL NOT NULL
    )""")
    cursor.execute("UPDATE jobs SET queued_at=created_at WHERE status='QUEUED' AND queued_at IS NULL")
    # Reconstruct occupied slots when upgrading a database with active legacy jobs.
    cursor.execute("""INSERT OR IGNORE INTO resource_reservations
        SELECT node_id,job_id,'{}',COALESCE(scheduled_at,created_at)
        FROM jobs WHERE status IN ('SCHEDULED','RUNNING','CANCELLING') AND node_id IS NOT NULL""")
    connection.commit()
    connection.close()


def add_job(
    job_id,
    duration,
    max_retries=2,
    job_type="dummy",
    job_config=None,
    status="QUEUED",
    owner=None,
):
    """Publish a complete job atomically; general submissions wait for later staging."""
    if status not in {"QUEUED", "RECEIVED"}:
        raise ValueError("New jobs must be QUEUED or RECEIVED")
    if job_type == "custom" and status != "RECEIVED":
        raise ValueError(
            "Custom submissions must start RECEIVED; staging uses an explicit transition"
        )
    now = time.time()

    connection = get_connection()
    cursor = connection.cursor()
    connection.execute('BEGIN IMMEDIATE')
    if job_type == 'custom':
        try:
            quota = limits(load_policy(),owner)['max_pending_per_owner']
            count = connection.execute("SELECT COUNT(*) FROM jobs WHERE job_type='custom' AND owner=? AND status IN (?,?,?,?,?)",
                                       (owner,*PENDING_STATES)).fetchone()[0]
            if count >= quota:
                raise ValueError(f'Pending-job quota reached for {owner}: {count}/{quota}')
        except Exception:
            connection.rollback()
            connection.close()
            raise

    cursor.execute(
        """
        INSERT INTO jobs (
            job_id,
            status,
            duration,
            created_at,
            retry_count,
            max_retries,
            updated_at, job_type, job_config, owner, queued_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            job_id,
            status,
            duration,
            now,
            0,
            max_retries,
            now,
            job_type,
            json.dumps(job_config or {}),
            owner,
            now if status == "QUEUED" else None,
        ),
    )

    connection.commit()
    connection.close()


def get_job(job_id):
    """Return one persisted job, or None when its ID is unknown."""
    connection = get_connection()
    cursor = connection.cursor()

    cursor.execute(
        """
        SELECT *
        FROM jobs
        WHERE job_id = ?
        """,
        (job_id,),
    )

    row = cursor.fetchone()
    connection.close()

    return dict(row) if row else None


def mark_staged(job_id, storage_path, manifest_sha256, execution_spec):
    """Commit a published snapshot's identity and managed paths without queuing it."""
    with get_connection() as connection:
        connection.execute("BEGIN IMMEDIATE")
        job = connection.execute(
            "SELECT * FROM jobs WHERE job_id=?", (job_id,)
        ).fetchone()
        if (
            job is None
            or job["job_type"] != "custom"
            or job["status"] not in ("RECEIVED", "STAGED", "READY")
        ):
            raise ValueError("Only received or staged custom jobs can attach storage")
        if job["status"] in ("STAGED", "READY") and (
            job["storage_path"] != storage_path
            or job["storage_manifest_sha256"] != manifest_sha256
        ):
            raise ValueError("A staged job cannot replace its committed snapshot")
        connection.execute(
            """UPDATE jobs SET status=CASE WHEN status='READY' THEN 'READY' ELSE 'STAGED' END, storage_path=?,
            storage_manifest_sha256=?, staged_at=COALESCE(staged_at,?), job_config=?,
            last_error=NULL, updated_at=? WHERE job_id=?""",
            (
                storage_path,
                manifest_sha256,
                time.time(),
                json.dumps(execution_spec),
                time.time(),
                job_id,
            ),
        )
    connection.close()


def record_staging_error(job_id, error):
    """Keep failed copies retryable without recording a training failure or attempt."""
    with get_connection() as connection:
        connection.execute(
            "UPDATE jobs SET last_error=?, updated_at=? WHERE job_id=? AND job_type='custom' AND status='RECEIVED'",
            (error, time.time(), job_id),
        )
    connection.close()


def get_job_environments(job_id):
    """Return prepared candidate workers; preparation does not reserve their resources."""
    connection = get_connection()
    try:
        rows = connection.execute(
            "SELECT * FROM job_environments WHERE job_id=? ORDER BY node_id", (job_id,)
        ).fetchall()
        return [{**dict(row), "record": json.loads(row["record_json"])} for row in rows]
    finally:
        connection.close()


def record_environment(job_id, node_id, record):
    """READY requires enough compatible prepared candidates for the declared worker count."""
    connection = get_connection()
    try:
        with connection:
            connection.execute("BEGIN IMMEDIATE")
            job = connection.execute(
                "SELECT * FROM jobs WHERE job_id=?", (job_id,)
            ).fetchone()
            if (
                job is None
                or job["job_type"] != "custom"
                or job["status"] not in ("STAGED", "READY")
            ):
                raise ValueError(
                    "Environment preparation requires a staged student submission"
                )
            if (
                record["job_id"] != job_id
                or record["owner"] != job["owner"]
                or record["manifest_sha256"] != job["storage_manifest_sha256"]
            ):
                raise ValueError(
                    "Prepared environment belongs to a different input snapshot"
                )
            now = time.time()
            connection.execute(
                """INSERT INTO job_environments
                (job_id,node_id,profile,environment_key,record_json,prepared_at,verified_at)
                VALUES(?,?,?,?,?,?,?) ON CONFLICT(job_id,node_id) DO UPDATE SET
                profile=excluded.profile, environment_key=excluded.environment_key,
                record_json=excluded.record_json, prepared_at=excluded.prepared_at,
                verified_at=excluded.verified_at""",
                (
                    job_id,
                    node_id,
                    record["profile"],
                    record["environment_key"],
                    json.dumps(record),
                    now,
                    now,
                ),
            )
            groups = {}
            for row in connection.execute(
                "SELECT record_json FROM job_environments WHERE job_id=?", (job_id,)
            ):
                key = json.loads(row[0])["compatibility_key"]
                groups[key] = groups.get(key, 0) + 1
            required = json.loads(job["job_config"])["execution"]["workers"]
            state = "READY" if max(groups.values(), default=0) >= required else "STAGED"
            connection.execute(
                "UPDATE jobs SET status=?,last_error=NULL,updated_at=? WHERE job_id=?",
                (state, now, job_id),
            )
            return state
    finally:
        connection.close()


def invalidate_environment(job_id, node_id, error):
    """Failed verification removes a stale candidate rather than leaving it advertised READY."""
    connection = get_connection()
    try:
        with connection:
            connection.execute("BEGIN IMMEDIATE")
            job = connection.execute(
                "SELECT * FROM jobs WHERE job_id=?", (job_id,)
            ).fetchone()
            if (
                job is None
                or job["job_type"] != "custom"
                or job["status"] not in ("STAGED", "READY")
            ):
                return
            connection.execute(
                "DELETE FROM job_environments WHERE job_id=? AND node_id=?",
                (job_id, node_id),
            )
            groups = {}
            for row in connection.execute(
                "SELECT record_json FROM job_environments WHERE job_id=?", (job_id,)
            ):
                key = json.loads(row[0])["compatibility_key"]
                groups[key] = groups.get(key, 0) + 1
            needed = json.loads(job["job_config"])["execution"]["workers"]
            state = "READY" if max(groups.values(), default=0) >= needed else "STAGED"
            connection.execute(
                "UPDATE jobs SET status=?,last_error=?,updated_at=? WHERE job_id=?",
                (state, error, time.time(), job_id),
            )
    finally:
        connection.close()


def get_jobs_by_status(status):
    """Read jobs in FIFO submission order for the requested state."""
    connection = get_connection()
    cursor = connection.cursor()

    cursor.execute(
        """
        SELECT *
        FROM jobs
        WHERE status = ?
        ORDER BY created_at ASC
        """,
        (status,),
    )

    rows = cursor.fetchall()
    connection.close()

    return [dict(row) for row in rows]


def get_all_jobs():
    """Return newest jobs first for the status display."""
    connection = get_connection()
    cursor = connection.cursor()

    cursor.execute("""
        SELECT *
        FROM jobs
        ORDER BY created_at DESC
        """)

    rows = cursor.fetchall()
    connection.close()

    return [dict(row) for row in rows]


def get_reservations():
    """Durable occupied slots include uncertain starts, even when an agent reports idle."""
    connection = get_connection()
    try:
        return [dict(row) for row in connection.execute(
            "SELECT * FROM resource_reservations ORDER BY node_id"
        )]
    finally:
        connection.close()


def enqueue_custom_job(job_id, candidates):
    """Explicitly queue prepared single-worker restart jobs; later modes cannot slip through."""
    connection = get_connection()
    try:
        with connection:
            connection.execute("BEGIN IMMEDIATE")
            job = connection.execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone()
            if not job or job["job_type"] != "custom" or job["status"] != "READY":
                raise ValueError("Only READY custom jobs can be queued")
            spec = json.loads(job["job_config"])
            if spec["execution"] != {"mode": "single", "workers": 1}:
                raise ValueError("Distributed dispatch arrives in step 7")
            if spec["recovery"]["mode"] not in ('restart','checkpoint'):
                raise ValueError('Unsupported recovery mode')
            prepared = {row[0] for row in connection.execute(
                "SELECT node_id FROM job_environments WHERE job_id=?", (job_id,)
            )}
            if not prepared.intersection(candidates):
                raise ValueError("Prepare this job on at least one configured scheduler worker")
            connection.execute("UPDATE jobs SET status='QUEUED',queued_at=?,queue_reason=NULL,updated_at=? WHERE job_id=?",
                               (time.time(),time.time(),job_id))
    finally:
        connection.close()


def mark_scheduled(job_id, node_id, node_ip, resources=None):
    """Reserve a whole worker slot and persist placement atomically before sending HTTP."""
    connection = get_connection()
    try:
        with connection:
            connection.execute("BEGIN IMMEDIATE")
            job = connection.execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone()
            if not job or job["status"] != "QUEUED":
                raise ValueError("Placement requires a queued job")
            if job['job_type'] == 'custom':
                quota = limits(load_policy(),job['owner'])['max_active_per_owner']
                count = connection.execute("SELECT COUNT(*) FROM jobs WHERE job_type='custom' AND owner=? AND status IN (?,?,?)",
                                           (job['owner'],*ACTIVE_STATES)).fetchone()[0]
                if count >= quota:
                    raise ValueError('Owner active-job quota reached')
            connection.execute("INSERT INTO resource_reservations VALUES(?,?,?,?)",
                (node_id,job_id,json.dumps(resources or {}),time.time()))
            connection.execute("""UPDATE jobs SET status='SCHEDULED',node_id=?,node_ip=?,
                scheduled_at=?,updated_at=?,queue_reason=NULL WHERE job_id=?""",
                (node_id,node_ip,time.time(),time.time(),job_id))
            connection.execute("""INSERT INTO owner_service VALUES(?,?) ON CONFLICT(owner)
                DO UPDATE SET last_dispatch=excluded.last_dispatch""",(identity(dict(job)),time.time()))
    finally:
        connection.close()


def mark_running(job_id, started_at=None):
    """Commit the running state and open its attempt-history record together."""
    if started_at is None:
        started_at = time.time()

    connection = get_connection()
    cursor = connection.cursor()
    connection.execute('BEGIN IMMEDIATE')
    row = connection.execute('SELECT * FROM jobs WHERE job_id=?',(job_id,)).fetchone()
    if row is None or row['status'] not in ('SCHEDULED','CANCELLING'):
        connection.rollback()
        connection.close()
        return
    job = dict(row)
    if job['started_at'] is not None:
        connection.rollback()
        connection.close()
        return
    attempt_number = job['retry_count'] + 1

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
        ("CANCELLING" if job["status"] == "CANCELLING" else "RUNNING", started_at, time.time(), job_id),
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
        (job_id, attempt_number, job["node_id"], job["node_ip"], "RUNNING", started_at),
    )

    connection.commit()
    connection.close()


def mark_completed(job_id, return_code=0, finished_at=None):
    """Close the current attempt after the dispatcher verifies required final artifacts."""
    if finished_at is None:
        finished_at = time.time()

    connection = get_connection()
    cursor = connection.cursor()
    connection.execute('BEGIN IMMEDIATE')
    row = connection.execute('SELECT status FROM jobs WHERE job_id=?',(job_id,)).fetchone()
    if row is None or row[0] != 'RUNNING':
        connection.rollback()
        connection.close()
        return False

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
        ("COMPLETED", finished_at, return_code, time.time(), job_id),
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
        ("COMPLETED", finished_at, return_code, job_id),
    )

    cursor.execute("DELETE FROM resource_reservations WHERE job_id=?", (job_id,))
    connection.commit()
    connection.close()
    return True


def requeue_after_failure(job_id, error):
    """Close the failed attempt and either queue a retry or exhaust the retry budget."""
    connection = get_connection()
    cursor = connection.cursor()
    connection.execute('BEGIN IMMEDIATE')
    job = dict(connection.execute('SELECT * FROM jobs WHERE job_id=?',(job_id,)).fetchone())
    if job['status'] == 'CANCELLING':
        connection.rollback()
        connection.close()
        return 'CANCELLING'
    new_retry_count = job['retry_count'] + 1

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
        ("FAILED", time.time(), error, job_id),
    )

    if new_retry_count <= job["max_retries"]:
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
            ("QUEUED", new_retry_count, error, time.time(), job_id),
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
            ("FAILED", new_retry_count, time.time(), error, error, time.time(), job_id),
        )

        final_state = "FAILED"

    cursor.execute("DELETE FROM resource_reservations WHERE job_id=?", (job_id,))
    connection.commit()
    connection.close()

    return final_state


def return_to_queue(job_id, error):
    """Undo placement when a request is known not to have started the job."""
    connection = get_connection()
    cursor = connection.cursor()
    connection.execute('BEGIN IMMEDIATE')
    row = connection.execute('SELECT status FROM jobs WHERE job_id=?',(job_id,)).fetchone()
    if row is not None and row[0] == 'CANCELLING':
        connection.rollback()
        connection.close()
        finish_cancellation(job_id)
        return

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
        ("QUEUED", error, time.time(), job_id),
    )

    cursor.execute("DELETE FROM resource_reservations WHERE job_id=?", (job_id,))
    connection.commit()
    connection.close()


def fair_queue():
    """Read one consistent policy snapshot and expose quota wait reasons."""
    connection = get_connection()
    try:
        with connection:
            connection.execute('BEGIN IMMEDIATE')
            queued = [dict(row) for row in connection.execute("SELECT * FROM jobs WHERE status='QUEUED'")]
            active = [dict(row) for row in connection.execute("SELECT * FROM jobs WHERE status IN (?,?,?)",ACTIVE_STATES)]
            service = dict(connection.execute('SELECT owner,last_dispatch FROM owner_service'))
            policy = load_policy()
            for job in queued:
                if job['job_type'] == 'custom':
                    count = sum(identity(j)==identity(job) for j in active)
                    quota = limits(policy,job['owner'])['max_active_per_owner']
                    reason = f'Owner active-job limit: {count}/{quota}' if count >= quota else 'Waiting for eligible worker capacity'
                    connection.execute('UPDATE jobs SET queue_reason=? WHERE job_id=?',(reason,job['job_id']))
            return fair_order(queued,active,service,policy,time.time())
    finally:
        connection.close()


def control_job(job_id, owner, action):
    """Persist an owner-scoped hold, release or cancellation without racing placement."""
    connection = get_connection()
    try:
        with connection:
            connection.execute('BEGIN IMMEDIATE')
            job = connection.execute('SELECT * FROM jobs WHERE job_id=?',(job_id,)).fetchone()
            if not job or job['job_type'] != 'custom' or job['owner'] != owner:
                raise ValueError('Job does not belong to this owner')
            state = job['status']
            if action == 'hold':
                if state != 'QUEUED': raise ValueError('Only QUEUED jobs can be held')
                state, reason = 'HELD','Held by owner'
            elif action == 'release':
                if state != 'HELD': raise ValueError('Only HELD jobs can be released')
                state, reason = 'QUEUED','Released; waiting for scheduling'
            elif action == 'cancel':
                if state in ('COMPLETED','FAILED'): raise ValueError('Finished jobs cannot be cancelled')
                state = 'CANCELLING' if state in ACTIVE_STATES else 'CANCELLED'
                reason = 'Stop requested; waiting for worker confirmation' if state == 'CANCELLING' else 'Cancelled by owner'
                connection.execute('UPDATE jobs SET cancel_requested_at=COALESCE(cancel_requested_at,?) WHERE job_id=?',(time.time(),job_id))
            else: raise ValueError('Unknown control action')
            if state == 'CANCELLED':
                connection.execute('UPDATE jobs SET finished_at=COALESCE(finished_at,?) WHERE job_id=?',(time.time(),job_id))
            connection.execute('UPDATE jobs SET status=?,queue_reason=?,updated_at=? WHERE job_id=?',
                               (state,reason,time.time(),job_id))
            return state
    finally:
        connection.close()


def finish_cancellation(job_id, return_code=None):
    """Release a cancelled slot only after the original worker confirms the attempt ended."""
    connection = get_connection()
    try:
        with connection:
            connection.execute('BEGIN IMMEDIATE')
            changed = connection.execute("""UPDATE jobs SET status='CANCELLED',finished_at=?,return_code=?,
                queue_reason='Cancellation confirmed',updated_at=? WHERE job_id=? AND status='CANCELLING'""",
                (time.time(),return_code,time.time(),job_id)).rowcount
            if changed:
                connection.execute("UPDATE job_attempts SET status='CANCELLED',finished_at=?,return_code=? WHERE job_id=? AND status='RUNNING'",
                                   (time.time(),return_code,job_id))
                connection.execute('DELETE FROM resource_reservations WHERE job_id=?',(job_id,))
    finally:
        connection.close()


def get_attempts(job_id):
    """Return chronological worker attempts for a logical job."""
    connection = get_connection()
    cursor = connection.cursor()

    cursor.execute(
        """
        SELECT *
        FROM job_attempts
        WHERE job_id = ?
        ORDER BY attempt_id ASC
        """,
        (job_id,),
    )

    rows = cursor.fetchall()
    connection.close()

    return [dict(row) for row in rows]


initialize_database()
