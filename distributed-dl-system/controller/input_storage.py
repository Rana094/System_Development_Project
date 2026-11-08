"""Explicit storage seam for trusted controller callers, not a student route.

Keep job_storage.stage_job/verify_workspace and all current upload/recovery APIs
unchanged. Future callers must authorize the exact job/manifest before building a
view. Imports do not select configuration, initialize SQLite, mount or copy data.
The explicit legacy-registration operation delegates only an authorized snapshot
copy into CAS; existing runtime routes never invoke it automatically.
"""

from pathlib import Path
import sys
from typing import Iterable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from hybrid_storage.backends import LegacyStorageBackend
from hybrid_storage.inputs import InputFile, LocalObjectStorageBackend, ProjectInputView


def local_objects(input_root: Path, *, staging_root: Path | None = None) -> LocalObjectStorageBackend:
    """Always use the controller-local view, including when inputs are exported.

    Both paths are trusted operator configuration. The publisher's protected
    roots/UID deployment contract must already be established. Publication also
    attests the actual opened parent/staging descriptors as supported local
    filesystems before writing; an absolute pathname alone does not grant trust.
    """
    return LocalObjectStorageBackend(input_root, staging_root=staging_root)


def project_inputs(managed_source_root: Path, files: Iterable[InputFile]) -> ProjectInputView:
    """Read only allowed files under an existing verified source snapshot.

    The caller derives this root from its owner-checked job and verifies the
    legacy manifest through job_storage.verify_workspace before constructing it.
    Never pass the raw project path or inventory from a student's request.
    """
    return ProjectInputView(LegacyStorageBackend(managed_source_root), files)


def register_legacy_inputs(registry, actor, job_id, *, managed_root: Path, verify_workspace):
    """Explicit reference-only enrollment; caller passes the existing verifier.

    No lazy import of job_storage/job_store: standalone imports must never run a
    database initializer. Trusted callers already using job_storage pass its
    verify_workspace function; root/actor are operator/authentication authority.
    """
    from hybrid_storage.legacy_registration import register_legacy_inputs as register
    return register(registry, actor, job_id, managed_root=managed_root, verify_workspace=verify_workspace)


def require_staged_attempt(connection, job_id):
    """Read staging barrier inside job_store's existing transition transaction.

    Never lifts the separate NFS execution gate or creates schema/state itself.
    """
    from hybrid_storage.staging_lifecycle import require_ready
    return require_ready(connection, job_id)
