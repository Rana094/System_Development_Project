"""Immutable per-job backend metadata; no DB connection, config or I/O on import.

Absence of a binding always means legacy, never today's operator default.
NFS is a retained submission intent, not an enabled execution path in Phase 3.3.
"""

import time

NFS_HELD_REASON = "NFS input staging/execution is not enabled; retained locally and held for later integration"


def validate_backend(backend):
    if type(backend) is not str or backend not in ("legacy", "nfs"):
        raise ValueError("storage_backend must be legacy or nfs")
    return backend


def create_schema(connection):
    """Add only metadata/immutability protection inside the caller's transaction."""
    connection.execute("""CREATE TABLE IF NOT EXISTS job_storage_bindings (
        job_id TEXT PRIMARY KEY,
        binding_version INTEGER NOT NULL CHECK(binding_version=1),
        backend TEXT NOT NULL CHECK(backend IN ('legacy','nfs')),
        selected_at REAL NOT NULL,
        FOREIGN KEY(job_id) REFERENCES jobs(job_id) ON DELETE CASCADE
    )""")
    connection.execute("""CREATE TRIGGER IF NOT EXISTS immutable_job_storage_binding
        BEFORE UPDATE ON job_storage_bindings
        BEGIN SELECT RAISE(ABORT,'Job storage selection is immutable'); END""")


def binding(connection, job_id):
    row = connection.execute(
        "SELECT binding_version,backend,selected_at FROM job_storage_bindings WHERE job_id=?", (job_id,)
    ).fetchone()
    if row is None:
        return {"binding_version": 1, "backend": "legacy", "origin": "implicit_legacy"}
    if row[0] != 1:
        raise ValueError("Unsupported job storage binding version")
    return {"binding_version": 1, "backend": validate_backend(row[1]), "origin": "pinned", "selected_at": row[2]}


def pin(connection, job_id, backend):
    backend = validate_backend(backend)
    # INSERT only: no update, replacement or automatic backfill of old jobs.
    connection.execute("INSERT INTO job_storage_bindings VALUES(?,1,?,?)", (job_id, backend, time.time()))


def require_execution(connection, job_id):
    if binding(connection, job_id)["backend"] != "legacy":
        raise ValueError(NFS_HELD_REASON)
