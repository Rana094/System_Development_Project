"""Version-one student submissions: validate declarations without running their code."""

import json
import math
from pathlib import Path, PurePosixPath
import re
import zipfile


class JobSpecError(ValueError):
    """A submission is malformed or refers to an unavailable local input."""


def object_fields(value, field, required, optional=()):
    """Reject missing and misspelled fields rather than silently ignoring settings."""
    if not isinstance(value, dict):
        raise JobSpecError(f"{field}: expected an object")
    missing = set(required) - value.keys()
    unknown = value.keys() - set(required) - set(optional)
    if missing:
        raise JobSpecError(f"{field}: missing fields: {', '.join(sorted(missing))}")
    if unknown:
        raise JobSpecError(f"{field}: unknown fields: {', '.join(sorted(unknown))}")
    return value


def text(value, field, maximum=4096):
    """Require usable names and paths before they enter persistent job metadata."""
    if not isinstance(value, str) or not value.strip() or len(value) > maximum:
        raise JobSpecError(
            f"{field}: expected a nonempty string of at most {maximum} characters"
        )
    if any(c in value for c in ("\0", "\n", "\r")):
        raise JobSpecError(f"{field}: contains an invalid control character")
    return value


def integer(value, field, minimum, maximum=None):
    """Keep boolean values and fractional resource counts out of allocations."""
    if (
        type(value) is not int
        or value < minimum
        or (maximum is not None and value > maximum)
    ):
        bounds = (
            f"{minimum} or greater"
            if maximum is None
            else f"{minimum} through {maximum}"
        )
        raise JobSpecError(f"{field}: expected an integer {bounds}")
    return value


def positive_number(value, field):
    """Resource sizes and checkpoint periods must be positive finite numbers."""
    if type(value) not in (int, float):
        raise JobSpecError(f"{field}: expected a positive finite number")
    try:
        valid = math.isfinite(value) and value > 0
    except OverflowError:
        valid = False
    if not valid:
        raise JobSpecError(f"{field}: expected a positive finite number")
    return value


def validate_owner(owner):
    """Accept a stable student ID or email; authentication is a later interface step."""
    if not isinstance(owner, str) or not re.fullmatch(
        r"[A-Za-z0-9][A-Za-z0-9_.@-]{0,127}", owner
    ):
        raise JobSpecError("owner: use a student ID or email, up to 128 characters")
    return owner


def local_input(value, field, base_dir):
    """Resolve inputs relative to the manifest, so submission works from any directory."""
    try:
        path = Path(text(value, field)).expanduser()
        if not path.is_absolute():
            path = base_dir / path
        path = path.resolve(strict=True)
        if not (path.is_file() or path.is_dir()):
            raise JobSpecError(f"{field}: expected a regular file or directory")
    except (OSError, RuntimeError, UnicodeError) as error:
        raise JobSpecError(f"{field}: cannot access {value}: {error}") from error
    return path


def requirements_path(value, project):
    """Ensure dependencies are declared by a file inside the submitted project."""
    value = text(value, "environment.requirements")
    relative = PurePosixPath(value)
    if (
        relative.is_absolute()
        or ".." in relative.parts
        or "\\" in value
        or ":" in value
    ):
        raise JobSpecError(
            "environment.requirements: use a relative path inside the project"
        )
    normalized = str(relative)
    if project.is_dir():
        try:
            path = (project / normalized).resolve()
        except (OSError, RuntimeError, UnicodeError) as error:
            raise JobSpecError(
                f"environment.requirements: cannot resolve file: {error}"
            ) from error
        if not path.is_relative_to(project) or not path.is_file():
            raise JobSpecError(
                "environment.requirements: file is missing or outside the project"
            )
    else:
        try:
            with zipfile.ZipFile(project) as archive:
                member = archive.getinfo(normalized)
                if member.is_dir():
                    raise JobSpecError("environment.requirements: expected a file")
        except (zipfile.BadZipFile, KeyError, OSError) as error:
            raise JobSpecError(
                "environment.requirements: file is missing from the project ZIP"
            ) from error
    return normalized


def validate_spec(document, base_dir):
    """Return a normalized single-worker or distributed submission declaration."""
    doc = object_fields(
        document,
        "submission",
        (
            "schema_version",
            "name",
            "project",
            "entrypoint",
            "dataset",
            "execution",
            "resources_per_worker",
            "environment",
            "recovery",
        ),
    )
    if integer(doc["schema_version"], "schema_version", 1) != 1:
        raise JobSpecError("schema_version: only version 1 is supported")
    name = text(doc["name"], "name", 128).strip()
    project = local_input(doc["project"], "project", base_dir)
    if project.is_file() and (
        project.suffix.lower() != ".zip" or not zipfile.is_zipfile(project)
    ):
        raise JobSpecError("project: supply a project directory or a valid ZIP package")
    dataset = (
        None
        if doc["dataset"] is None
        else str(local_input(doc["dataset"], "dataset", base_dir))
    )

    entrypoint = doc["entrypoint"]
    if not isinstance(entrypoint, list) or not 1 <= len(entrypoint) <= 256:
        raise JobSpecError(
            'entrypoint: expected an argument list, e.g. ["python", "train.py"]'
        )
    for value in entrypoint:
        if not isinstance(value, str) or len(value) > 4096 or "\0" in value:
            raise JobSpecError(
                "entrypoint: arguments must be strings without NUL characters"
            )
    text(entrypoint[0], "entrypoint[0]")

    execution = object_fields(doc["execution"], "execution", ("mode", "workers"))
    if execution["mode"] not in ("single", "distributed"):
        raise JobSpecError("execution.mode: choose single or distributed")
    workers = integer(execution["workers"], "execution.workers", 1)
    if execution["mode"] == "single" and workers != 1:
        raise JobSpecError("execution.workers: single mode requires exactly one worker")
    if execution["mode"] == "distributed" and workers < 2:
        raise JobSpecError(
            "execution.workers: distributed mode requires at least two workers"
        )

    resources = object_fields(
        doc["resources_per_worker"],
        "resources_per_worker",
        ("cpu_cores", "memory_gb", "gpus"),
    )
    resources = {
        "cpu_cores": integer(
            resources["cpu_cores"], "resources_per_worker.cpu_cores", 1
        ),
        "memory_gb": positive_number(
            resources["memory_gb"], "resources_per_worker.memory_gb"
        ),
        "gpus": integer(resources["gpus"], "resources_per_worker.gpus", 0),
    }
    environment = object_fields(
        doc["environment"], "environment", ("requirements",), ("python",)
    )
    environment = {
        **environment,
        "requirements": requirements_path(environment["requirements"], project),
    }
    if "python" in environment and (
        not isinstance(environment["python"], str)
        or not re.fullmatch(r"3\.\d{1,2}", environment["python"])
    ):
        raise JobSpecError(
            "environment.python: use a Python 3 major.minor version, e.g. 3.12"
        )

    recovery = object_fields(
        doc["recovery"], "recovery", ("mode",), ("interval_seconds", "max_retries")
    )
    if recovery["mode"] not in ("restart", "checkpoint"):
        raise JobSpecError("recovery.mode: choose restart or checkpoint")
    recovery = {
        **recovery,
        "max_retries": integer(
            recovery.get("max_retries", 2), "recovery.max_retries", 0, 10
        ),
    }
    if recovery["mode"] == "checkpoint":
        recovery["interval_seconds"] = positive_number(
            recovery.get("interval_seconds"), "recovery.interval_seconds"
        )
    elif "interval_seconds" in recovery:
        raise JobSpecError("recovery.interval_seconds: only applies to checkpoint mode")
    return {
        "schema_version": 1,
        "name": name,
        "project": str(project),
        "entrypoint": list(entrypoint),
        "dataset": dataset,
        "execution": dict(execution),
        "resources_per_worker": resources,
        "environment": environment,
        "recovery": recovery,
    }


def load_spec(filename):
    """Read strict JSON and validate local inputs without extracting or importing code."""

    def unique_fields(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise JobSpecError(f"Duplicate JSON field: {key}")
            result[key] = value
        return result

    def invalid_number(value):
        raise JobSpecError(f"Invalid JSON number: {value}")

    try:
        path = Path(filename).expanduser().resolve()
        if path.stat().st_size > 1024 * 1024:
            raise JobSpecError("Submission file must be at most 1 MiB")
        document = json.loads(
            path.read_text(encoding="utf-8"),
            object_pairs_hook=unique_fields,
            parse_constant=invalid_number,
        )
    except JobSpecError:
        raise
    except (OSError, UnicodeError, ValueError, RecursionError, RuntimeError) as error:
        raise JobSpecError(f"Cannot read submission JSON: {error}") from error
    return validate_spec(document, path.parent)
