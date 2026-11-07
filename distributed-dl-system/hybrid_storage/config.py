"""Strict settings; legacy execution plus opt-in held NFS submission metadata."""

import argparse
from dataclasses import asdict, dataclass
import json
import os
from pathlib import Path
import stat

CONFIG_FILE = Path(__file__).resolve().parents[1] / "config/storage.json"
MAX_CONFIG_BYTES = 64 * 1024
FIELDS = frozenset(("storage_backend", "worker_cache_enabled", "storage_aware_staging", "scheduler_policy"))


class StorageConfigError(ValueError):
    """Invalid settings or a recognized but unavailable runtime feature."""


@dataclass(frozen=True)
class StorageConfig:
    storage_backend: str = "legacy"
    worker_cache_enabled: bool = False
    storage_aware_staging: bool = False
    scheduler_policy: str = "resource"

    def __post_init__(self):
        for name, allowed in (
            ("storage_backend", ("legacy", "nfs")),
            ("scheduler_policy", ("resource", "locality", "bandwidth")),
        ):
            if type(getattr(self, name)) is not str or getattr(self, name) not in allowed:
                raise StorageConfigError(f"{name} must be one of: {', '.join(allowed)}")
        for name in ("worker_cache_enabled", "storage_aware_staging"):
            if type(getattr(self, name)) is not bool:
                raise StorageConfigError(f"{name} must be a JSON boolean (true or false)")
        if self.storage_backend == "legacy" and (
            self.worker_cache_enabled or self.storage_aware_staging or self.scheduler_policy != "resource"
        ):
            raise StorageConfigError("legacy requires both new flags false and scheduler_policy=resource")
        if self.storage_backend == "nfs" and not self.storage_aware_staging:
            raise StorageConfigError("nfs requires storage_aware_staging=true; direct trainer NFS access is prohibited")
        if self.scheduler_policy != "resource" and not self.worker_cache_enabled:
            raise StorageConfigError("locality/bandwidth require worker_cache_enabled=true and verified inventory")


def validate_config(document: object) -> StorageConfig:
    """Fill missing keys with legacy defaults; never coerce or ignore unknown keys."""
    if not isinstance(document, dict) or any(type(key) is not str for key in document):
        raise StorageConfigError("Storage configuration must be a JSON object with string keys")
    unknown = set(document) - FIELDS
    if unknown:
        raise StorageConfigError("Unknown storage settings: " + ", ".join(sorted(unknown)))
    return StorageConfig(**document)


def _unique_object(pairs):
    document = {}
    for key, value in pairs:
        if key in document:
            raise StorageConfigError("Duplicate storage setting: " + key)
        document[key] = value
    return document


def _reject_constant(value):
    raise StorageConfigError("Nonfinite numbers are not valid storage settings")


def load_config(path: Path | None = None) -> StorageConfig:
    """Read local operator config only; missing file means legacy, other errors fail.

    No environment overrides, directory creation, DB imports or runtime selection.
    Final symlinks, nonregular files, duplicate fields and oversized JSON fail closed.
    """
    path = Path(path) if path is not None else CONFIG_FILE
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except FileNotFoundError:
        return StorageConfig()
    except OSError as error:
        raise StorageConfigError(f"Cannot open storage configuration {path}: {error.strerror}") from error
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            os.close(fd)
            raise StorageConfigError("Storage configuration must be a regular file")
        with os.fdopen(fd, "rb") as stream:
            raw = stream.read(MAX_CONFIG_BYTES + 1)
        if len(raw) > MAX_CONFIG_BYTES:
            raise StorageConfigError("Storage configuration exceeds 64 KiB")
        document = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique_object, parse_constant=_reject_constant)
    except (OSError, UnicodeError, json.JSONDecodeError, RecursionError) as error:
        raise StorageConfigError(f"Cannot read valid UTF-8 storage JSON from {path}: {type(error).__name__}") from error
    return validate_config(document)


def require_legacy_runtime(settings: StorageConfig) -> StorageConfig:
    """Do not enable unimplemented staging/cache/scheduler paths via configuration.

    This gate must be replaced only alongside tested hybrid runtime integration;
    parsing a future combination does not attest authentication or NFS readiness.
    """
    if not isinstance(settings, StorageConfig):
        raise StorageConfigError("Expected validated StorageConfig")
    if settings != StorageConfig():
        raise StorageConfigError(
            "Hybrid storage runtime is not implemented/enabled in Phase 1; use "
            "storage_backend=legacy, worker_cache_enabled=false, "
            "storage_aware_staging=false, scheduler_policy=resource"
        )
    return settings


def require_controller_config(settings: StorageConfig) -> StorageConfig:
    """Permit legacy execution and opt-in held NFS submissions, not NFS launch.

    Cache/locality/bandwidth remain unavailable. A new-submission default must
    never reinterpret or stop existing legacy jobs when the controller restarts.
    """
    if not isinstance(settings, StorageConfig):
        raise StorageConfigError("Expected validated StorageConfig")
    if settings.worker_cache_enabled or settings.scheduler_policy != "resource":
        raise StorageConfigError("Cache/locality/bandwidth execution is not implemented/enabled")
    return settings


def select_submission_backend(settings: StorageConfig, requested: str | None = None) -> str:
    settings = require_controller_config(settings)
    backend = settings.storage_backend if requested is None else requested
    if type(backend) is not str or backend not in ("legacy", "nfs"):
        raise StorageConfigError("storage_backend must be legacy or nfs")
    if backend == "nfs" and settings.storage_backend != "nfs":
        raise StorageConfigError("NFS submissions require operator storage_backend=nfs opt-in")
    return backend


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Check local storage configuration without starting services.")
    parser.add_argument("--config", type=Path, default=None, help="Config file; default: config/storage.json")
    args = parser.parse_args(argv)
    try:
        settings = require_controller_config(load_config(args.config))
    except StorageConfigError as error:
        parser.error(str(error))
    report = {"runtime": "legacy", "settings": asdict(settings)}
    if settings.storage_backend == "nfs":
        report.update(new_job_default="nfs", nfs_execution="blocked")
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
