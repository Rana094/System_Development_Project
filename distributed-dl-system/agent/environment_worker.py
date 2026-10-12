"""Worker-local input caches and Python environments, independent of the job launcher."""

import argparse
from contextlib import contextmanager
import fcntl
import hashlib
import importlib.metadata as metadata
import json
import os
from pathlib import Path
import platform
import re
import shutil
import stat
import subprocess
import sys
import sysconfig
import tempfile
import time


class EnvironmentError(ValueError):
    """Preparation cannot produce a verified, compatible environment."""


def canonical(value):
    """Use the same inventory encoding as controller storage content versions."""
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        + "\n"
    ).encode()


def file_hash(path):
    result = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(chunk)
    return result.hexdigest()


def atomic_json(path, value):
    """Completion markers become visible only after their metadata is durable."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("wb") as stream:
        stream.write(canonical(value))
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


@contextmanager
def locked(root, key):
    """A reboot releases build locks; incomplete environments have no ready marker."""
    directory = root / ".locks"
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / (key + ".lock")).open("a") as stream:
        try:
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise EnvironmentError(f"Preparation already in progress: {key}") from error
        yield


def inventory(root):
    """Verify cached input bytes, directory layout and executable bits before reuse."""
    files, directories = [], []
    if root.is_symlink() or not root.is_dir():
        raise EnvironmentError(f"Input cache is missing or linked: {root}")

    def walk(directory):
        for path in sorted(directory.iterdir()):
            info = path.lstat()
            relative = path.relative_to(root).as_posix()
            if stat.S_ISDIR(info.st_mode):
                directories.append(relative)
                walk(path)
            elif stat.S_ISREG(info.st_mode):
                files.append(
                    {
                        "path": relative,
                        "size": info.st_size,
                        "sha256": file_hash(path),
                        "executable": bool(info.st_mode & 0o111),
                    }
                )
            else:
                raise EnvironmentError(
                    f"Unsupported cached link or special file: {path}"
                )

    walk(root)
    return {"directories": sorted(directories), "files": files}


def packages():
    """Resolve the effective version when an overlay shadows a baseline distribution."""
    names = {d.metadata["Name"] for d in metadata.distributions() if d.metadata["Name"]}
    return [
        list(item)
        for item in sorted(
            (name.lower().replace("_", "-"), metadata.version(name)) for name in names
        )
    ]


def runtime(profile):
    """Cache keys bind environments to the worker interpreter, ABI and architecture."""
    result = {
        "python": platform.python_version(),
        "machine": platform.machine(),
        "system": platform.system(),
        "platform": sysconfig.get_platform(),
        "abi": sys.implementation.cache_tag,
        "base_prefix": sys.base_prefix,
    }
    if profile == "worker-base":
        result["base_packages"] = packages()
        result["base_sites"] = sorted(
            {p for p in sys.path if p.endswith(("site-packages", "dist-packages"))}
        )
    return result


def read_request(path):
    """Only controller-validated snapshots with safe IDs and version keys are accepted."""
    request = json.loads(path.read_text())
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", request["job_id"]):
        raise EnvironmentError("Invalid job ID")
    if request["profile"] not in ("isolated", "worker-base", "pytorch-cpu"):
        raise EnvironmentError("Unknown environment profile")
    for kind in ("source", "data"):
        snapshot = request["manifest"][kind]
        expected = (
            None
            if snapshot["kind"] == "generated"
            else "sha256:"
            + hashlib.sha256(canonical(snapshot["inventory"])).hexdigest()
        )
        if snapshot["version"] != expected:
            raise EnvironmentError(f"Invalid {kind} fingerprint")
    requirement = request["manifest"]["execution_spec"]["environment"]["requirements"]
    if Path(requirement).is_absolute() or ".." in Path(requirement).parts:
        raise EnvironmentError("Dependency path escapes the project")
    return request


def input_location(root, request, kind):
    version = request["manifest"][kind]["version"]
    return (
        None
        if version is None
        else root / "inputs" / kind / version.split(":")[1] / "content"
    )


def check_input(path, expected):
    if inventory(path) != expected:
        raise EnvironmentError(f"Cached input failed its content check: {path}")


def plan(root, request):
    """Report verified cache hits before the controller transfers input bytes."""
    info = runtime(request["profile"])
    if (
        request["profile"] == "pytorch-cpu"
        and request["manifest"]["execution_spec"]["resources_per_worker"]["gpus"]
    ):
        raise EnvironmentError(
            "The pytorch-cpu profile cannot prepare a GPU-requesting job"
        )
    requested_python = request["manifest"]["execution_spec"]["environment"].get(
        "python"
    )
    if requested_python and requested_python != ".".join(info["python"].split(".")[:2]):
        raise EnvironmentError(
            f"Requested Python {requested_python}; worker provides {info['python']}"
        )
    key = hashlib.sha256(
        canonical(
            {
                "version": 1,
                "runtime": info,
                "profile": request["profile"],
                "source_version": request["manifest"]["source"]["version"],
                "requirements": request["manifest"]["execution_spec"]["environment"][
                    "requirements"
                ],
            }
        )
    ).hexdigest()
    result = {
        "environment_key": key,
        "runtime": info,
        "environment_cached": (root / "environments" / key / "ready.json").is_file(),
    }
    for kind in ("source", "data"):
        location = input_location(root, request, kind)
        cached = location is None or location.exists()
        if location is not None and cached:
            check_input(location, request["manifest"][kind]["inventory"])
        result[kind + "_cached"] = cached
    return result


def publish_input(root, request, kind, incoming):
    """One content version is published once; different jobs reuse its managed bytes."""
    location = input_location(root, request, kind)
    if location is None:
        return None
    with locked(root, kind + "-" + location.parent.name):
        if location.exists():
            check_input(location, request["manifest"][kind]["inventory"])
        else:
            check_input(incoming, request["manifest"][kind]["inventory"])
            location.parent.mkdir(parents=True, exist_ok=True)
            incoming.rename(location)
    return location


def clean_environment():
    """Job installation targets its venv and ignores an ambient PYTHONPATH or pip config."""
    env = dict(os.environ)
    for key in ("PYTHONPATH", "PYTHONHOME", "VIRTUAL_ENV"):
        env.pop(key, None)
    env.update(
        PIP_CONFIG_FILE=os.devnull,
        PIP_REQUIRE_VIRTUALENV="true",
        PIP_NO_INPUT="1",
        PYTHONDONTWRITEBYTECODE="1",
    )
    return env


PACKAGE_PROBE = """import importlib.metadata as m,json,platform,sys
names={d.metadata["Name"] for d in m.distributions() if d.metadata["Name"]}
print(json.dumps({"python":platform.python_version(),"machine":platform.machine(),"prefix":sys.prefix,"packages":sorted((n.lower().replace("_","-"),m.version(n)) for n in names)}))"""


def probe(python):
    result = subprocess.run(
        [str(python), "-I", "-c", PACKAGE_PROBE],
        env=clean_environment(),
        capture_output=True,
        text=True,
        check=True,
        timeout=60,
    )
    return json.loads(result.stdout)


def pip_check(python, log=None):
    """Dependency consistency is required both after installation and on cache reuse."""
    result = subprocess.run(
        [str(python), "-I", "-m", "pip", "--disable-pip-version-check", "check"],
        env=clean_environment(),
        capture_output=True,
        text=True,
        timeout=120,
    )
    if log is not None:
        log.write(result.stdout + result.stderr)
        log.flush()
    if result.returncode:
        raise EnvironmentError("pip check failed: " + result.stdout + result.stderr)
    return result.stdout.strip()


def verify_environment(root, request, expected):
    directory = root / "environments" / expected["environment_key"]
    marker = json.loads((directory / "ready.json").read_text())
    if (
        marker["environment_key"] != expected["environment_key"]
        or marker["runtime"] != expected["runtime"]
    ):
        raise EnvironmentError("Environment interpreter/profile identity changed")
    actual = probe(directory / "bin/python")
    if actual != marker["probe"] or actual["prefix"] != str(directory):
        raise EnvironmentError("Installed environment package inventory changed")
    pip_check(directory / "bin/python")
    return marker


def prepare(root, request, incoming):
    """Cache inputs, build a private environment, and publish a per-job execution record."""
    job_directory = root / "jobs" / request["job_id"]
    job_directory.mkdir(parents=True, exist_ok=True)
    with locked(root, "job-" + request["job_id"]):
        expected = plan(root, request)
        source = publish_input(root, request, "source", incoming / "source")
        data = publish_input(root, request, "data", incoming / "data")
        directory = root / "environments" / expected["environment_key"]
        with locked(root, "env-" + expected["environment_key"]):
            if (directory / "ready.json").exists():
                marker = verify_environment(root, request, expected)
            else:
                # Build at the final prefix because venv scripts embed that path.
                # The ready marker, rather than directory existence, gates reuse.
                if directory.exists():
                    shutil.rmtree(directory)
                directory.parent.mkdir(parents=True, exist_ok=True)
                with (job_directory / "preparation.log").open("w") as log:
                    result = subprocess.run(
                        [sys.executable, "-m", "venv", "--copies", str(directory)],
                        env=clean_environment(),
                        stdout=log,
                        stderr=subprocess.STDOUT,
                        timeout=180,
                    )
                    if result.returncode:
                        raise EnvironmentError(
                            f"venv creation failed; see {job_directory / 'preparation.log'}"
                        )
                    if request["profile"] == "worker-base":
                        site_result = subprocess.run(
                            [
                                str(directory / "bin/python"),
                                "-I",
                                "-c",
                                "import sysconfig;print(sysconfig.get_path('purelib'))",
                            ],
                            capture_output=True,
                            text=True,
                            check=True,
                        )
                        site_path = Path(site_result.stdout.strip())
                        (site_path / "worker_base.pth").write_text(
                            "\n".join(expected["runtime"]["base_sites"]) + "\n"
                        )
                    # Local wheels/build backends may write build files. Give pip
                    # its own stable source tree so cached student inputs stay intact.
                    build_source = directory / "dependency-source"
                    shutil.copytree(source, build_source)
                    requirements = (
                        build_source
                        / request["manifest"]["execution_spec"]["environment"][
                            "requirements"
                        ]
                    )
                    result = subprocess.run(
                        [
                            str(directory / "bin/python"),
                            "-I",
                            "-m",
                            "pip",
                            "--disable-pip-version-check",
                            "install",
                            *(
                                ["--index-url", "https://download.pytorch.org/whl/cpu"]
                                if request["profile"] == "pytorch-cpu"
                                else []
                            ),
                            "--cache-dir",
                            str(root / "pip-cache"),
                            "-r",
                            str(requirements),
                        ],
                        cwd=build_source,
                        env=clean_environment(),
                        stdout=log,
                        stderr=subprocess.STDOUT,
                        timeout=1200,
                    )
                    if result.returncode:
                        raise EnvironmentError(
                            f"Dependency installation failed; see {job_directory / 'preparation.log'}"
                        )
                    pip_check(directory / "bin/python", log)
                marker = {
                    "environment_key": expected["environment_key"],
                    "runtime": expected["runtime"],
                    "profile": request["profile"],
                    "probe": probe(directory / "bin/python"),
                    "created_at": time.time(),
                }
                atomic_json(directory / "ready.json", marker)
        if not (job_directory / "source").exists():
            temporary = Path(tempfile.mkdtemp(prefix=".source-", dir=job_directory))
            try:
                shutil.copytree(source, temporary / "content")
                (temporary / "content").rename(job_directory / "source")
            finally:
                shutil.rmtree(temporary)
        check_input(
            job_directory / "source", request["manifest"]["source"]["inventory"]
        )
        for name in ("checkpoints", "logs", "results"):
            (job_directory / name).mkdir(exist_ok=True)
        dataset_path = (
            None
            if data is None
            else (
                str(data / Path(request["manifest"]["execution_spec"]["dataset"]).name)
                if request["manifest"]["data"]["kind"] == "file"
                else str(data)
            )
        )
        record = {
            "job_id": request["job_id"],
            "owner": request["manifest"]["owner"],
            "manifest_sha256": request["manifest_sha256"],
            "environment_key": expected["environment_key"],
            "profile": request["profile"],
            "python_path": str(directory / "bin/python"),
            "source_path": str(job_directory / "source"),
            "dataset_path": dataset_path,
            "workspace_path": str(job_directory),
            "source_version": request["manifest"]["source"]["version"],
            "dataset_version": request["manifest"]["data"]["version"],
            "runtime": expected["runtime"],
            "packages": marker["probe"]["packages"],
        }
        compatibility = {
            "python": ".".join(record["runtime"]["python"].split(".")[:2]),
            "machine": record["runtime"]["machine"],
            "system": record["runtime"]["system"],
            "profile": record["profile"],
            "packages": [
                p
                for p in record["packages"]
                if p[0] not in ("pip", "setuptools", "wheel")
            ],
        }
        record["compatibility_key"] = hashlib.sha256(
            canonical(compatibility)
        ).hexdigest()
        atomic_json(job_directory / "request.json", request)
        atomic_json(job_directory / "prepared.json", record)
        return {
            "prepared": True,
            "record": record,
            "cache_hits": {
                k: expected[k]
                for k in ("source_cached", "data_cached", "environment_cached")
            },
        }


def verify(root, job_id, modules):
    """A fresh worker process verifies packages and input snapshots, then probes imports."""
    directory = root / "jobs" / job_id
    request = read_request(directory / "request.json")
    record = json.loads((directory / "prepared.json").read_text())
    expected = plan(root, request)
    marker = verify_environment(root, request, expected)
    if (
        record["environment_key"] != expected["environment_key"]
        or record["packages"] != marker["probe"]["packages"]
    ):
        raise EnvironmentError(
            "Prepared job record does not match the installed environment"
        )
    check_input(Path(record["source_path"]), request["manifest"]["source"]["inventory"])
    for module in modules:
        if not re.fullmatch(
            r"[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*", module
        ):
            raise EnvironmentError("Invalid import module name")
    code = """import contextlib,importlib,io,json,sys
out=io.StringIO(); imported={}
with contextlib.redirect_stdout(out):
 for name in sys.argv[1:]:
  m=importlib.import_module(name); imported[name]=str(getattr(m,"__version__","import succeeded"))
print(json.dumps({"imports":imported,"import_output":out.getvalue()}))"""
    result = subprocess.run(
        [record["python_path"], "-I", "-c", code, *modules],
        env=clean_environment(),
        capture_output=True,
        text=True,
        timeout=120,
    )
    if result.returncode:
        raise EnvironmentError("Environment import probe failed: " + result.stderr)
    return {
        "verified": True,
        "record": record,
        "pip_check": "No broken requirements found.",
        **json.loads(result.stdout),
    }


def main():
    """Controller protocol uses JSON on stdout; diagnostics remain on stderr/log files."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("plan", "prepare", "verify"))
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--request", type=Path)
    parser.add_argument("--job-id")
    parser.add_argument("--import-module", action="append", default=[])
    args = parser.parse_args()
    root = args.root.expanduser().resolve()
    try:
        if args.action == "verify":
            if not args.job_id or not re.fullmatch(
                r"[A-Za-z0-9_-]{1,128}", args.job_id
            ):
                raise EnvironmentError("A safe job ID is required")
            result = verify(root, args.job_id, args.import_module)
        else:
            request = read_request(args.request)
            result = (
                plan(root, request)
                if args.action == "plan"
                else prepare(root, request, args.request.parent)
            )
            if (
                args.action == "prepare"
                and args.request.resolve().parent.parent == root / "incoming"
            ):
                shutil.rmtree(args.request.resolve().parent)
        print(json.dumps(result))
    except (
        OSError,
        ValueError,
        KeyError,
        RuntimeError,
        subprocess.SubprocessError,
    ) as error:
        print(json.dumps({"error": str(error)}))
        raise SystemExit(1)


if __name__ == "__main__":
    main()
