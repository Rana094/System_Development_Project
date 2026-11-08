"""Explicit worker-service cache seam; not called by the legacy HTTP launcher.

No initialization on import/construction, no configuration activation, no DB or
filesystem side effects. Paths are operator configuration, never request input.
"""

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from hybrid_storage.worker_cache import WorkerCache


def local_cache(cache_root: Path, *, input_mount: Path,
                verify_after_seconds: float = 300, metrics=None) -> WorkerCache:
    """Return an inactive cache handle; the trusted service explicitly initializes.

    Runtime enablement stays gated pending authorized staging, independent
    trainer identity/isolation and safe terminal-path pin reconciliation.
    """
    return WorkerCache(cache_root, input_mount=input_mount,
                       verify_after_seconds=verify_after_seconds, metrics=metrics)


def nfs_cache_fetcher(cache: WorkerCache, *, source: str, quota_bytes: int, **limits):
    """Explicit inactive NFS fetch handle. No HTTP API or automatic runtime wiring.

    Only a trusted authorized staging service may call initialize/ensure; retain
    this handle and reconcile pending helpers until reaped, including after close.
    """
    from hybrid_storage.cache_fetch import CacheFetcher
    return CacheFetcher(cache, source=source, quota_bytes=quota_bytes, **limits)


def private_workspaces(cache: WorkerCache, workspace_root: Path, *, max_input_bytes: int, **limits):
    """Inactive private-copy/lifecycle seam; never enables the legacy launcher.

    A future isolated launcher must provide current exact authority and proof
    that the existing lease guardian and all descendants stopped before unpin.
    """
    from hybrid_storage.workspaces import WorkspaceManager
    return WorkspaceManager(cache, workspace_root, max_input_bytes=max_input_bytes, **limits)
