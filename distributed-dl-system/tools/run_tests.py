"""Run each regression script in its own process to isolate controller imports."""

import argparse
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "tests", nargs="*", metavar="TEST_FILE",
        help="Optional test basenames, such as test_student_portal.py; default: all.",
    )
    parser.add_argument("--isolated", action="store_true",
                        help="Run from a temporary source copy, excluding live databases/runtime data.")
    args = parser.parse_args()
    available = {path.name: path for path in sorted((ROOT / "tests").glob("test_*.py"))}
    unknown = [name for name in args.tests if name not in available]
    if unknown:
        parser.error("Unknown test files: " + ", ".join(unknown))
    if args.isolated:
        with tempfile.TemporaryDirectory(prefix="distributed-ml-tests-") as temporary:
            sandbox = Path(temporary)
            for name in ("controller", "hybrid_storage", "tests", "config", "agent", "training", "examples", "tools", "docs"):
                source = ROOT / name
                if source.exists():
                    shutil.copytree(source, sandbox / name, ignore=shutil.ignore_patterns(
                        "*.db", "*.db-*", "*.sqlite*", "*.lock", "__pycache__", ".venv", "venv", "node_modules"))
            if (ROOT / "README.md").exists():
                shutil.copy2(ROOT / "README.md", sandbox / "README.md")
            env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")
            for name in ("DL_STORAGE_ROOT", "DL_LOCAL_WORKER_ROOT", "PYTHONPATH",
                         "DL_CONTROLLER_STATE_ROOT", "DL_AGENT_STATE_ROOT", "DL_NODES_CONFIG"):
                env.pop(name, None)
            print("Running from a temporary source copy; live databases/runtime roots excluded.", flush=True)
            return subprocess.run([sys.executable, str(sandbox / "tools/run_tests.py"), *args.tests],
                                  cwd=sandbox, env=env).returncode
    selected = [available[name] for name in args.tests] if args.tests else list(available.values())
    env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1", OMP_NUM_THREADS="1", MKL_NUM_THREADS="1")
    failed = []
    start = time.monotonic()
    for path in selected:
        print(f"\nRunning {path.name}", flush=True)
        result = subprocess.run([sys.executable, str(path)], cwd=ROOT, env=env)
        print(f"{'PASS' if result.returncode == 0 else 'FAIL'} {path.name}", flush=True)
        if result.returncode:
            failed.append(path.name)
    print(f"\n{len(selected) - len(failed)}/{len(selected)} test files passed in {time.monotonic() - start:.1f}s.")
    if failed:
        print("Failed: " + ", ".join(failed))
    return int(bool(failed))


if __name__ == "__main__":
    raise SystemExit(main())
