"""Real offline pip installation, environment separation and readiness transitions."""

import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
import uuid
import zipfile

ROOT = Path(__file__).resolve().parents[1]
ISOLATED = tempfile.TemporaryDirectory(prefix="environment-controller-")
COPY = Path(ISOLATED.name)
shutil.copytree(
    ROOT / "controller",
    COPY / "controller",
    ignore=shutil.ignore_patterns("jobs.db*", "*.lock", "__pycache__"),
)
shutil.copytree(
    ROOT / "agent", COPY / "agent", ignore=shutil.ignore_patterns("__pycache__")
)
shutil.copytree(ROOT / "config", COPY / "config")
sys.path.insert(0, str(COPY / "controller"))
import job_store as store
import job_storage as storage
import prepare_job as preparation
from job_spec import validate_spec


def wheel(path, version):
    """Supply two small real wheels so tests exercise pip without network downloads."""
    name = "step3_demo_dep"
    info = f"{name}-{version}.dist-info"
    contents = {
        f"{name}/__init__.py": f'__version__ = "{version}"\n',
        f"{info}/METADATA": f"Metadata-Version: 2.1\nName: step3-demo-dep\nVersion: {version}\n",
        f"{info}/WHEEL": "Wheel-Version: 1.0\nRoot-Is-Purelib: true\nTag: py3-none-any\n",
    }
    contents[f"{info}/RECORD"] = (
        "\n".join(f"{filename},," for filename in [*contents, f"{info}/RECORD"]) + "\n"
    )
    with zipfile.ZipFile(path / f"{name}-{version}-py3-none-any.whl", "w") as archive:
        for filename, data in contents.items():
            archive.writestr(filename, data)


class EnvironmentTests(unittest.TestCase):
    """Prepare actual venvs and verify cache reuse, imports and failure recovery."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="environment-inputs-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.inputs = self.root / "inputs"
        self.project = self.inputs / "project"
        (self.project / "wheels").mkdir(parents=True)
        wheel(self.project / "wheels", "1.0")
        wheel(self.project / "wheels", "2.0")
        (self.project / "train.py").write_text(
            "import step3_demo_dep\nprint(step3_demo_dep.__version__)\n"
        )
        (self.inputs / "data.csv").write_text("x,y\n1,2\n")
        self.env = patch.dict(
            os.environ,
            DL_STORAGE_ROOT=str(self.root / "storage"),
            DL_LOCAL_WORKER_ROOT=str(self.root / "worker"),
            PYTHONDONTWRITEBYTECODE="1",
        )
        self.env.start()
        self.addCleanup(self.env.stop)

    def submit(self, version="1.0", workers=1, python=None, gpus=0):
        (self.project / "requirements.txt").write_text(
            f"--no-index\n--find-links wheels\nstep3-demo-dep=={version}\n"
        )
        environment = {"requirements": "requirements.txt"}
        if python:
            environment["python"] = python
        spec = {
            "schema_version": 1,
            "name": "environment-test",
            "project": "project",
            "entrypoint": ["python", "train.py"],
            "dataset": "data.csv",
            "execution": {
                "mode": "single" if workers == 1 else "distributed",
                "workers": workers,
            },
            "resources_per_worker": {"cpu_cores": 1, "memory_gb": 1, "gpus": gpus},
            "environment": environment,
            "recovery": {"mode": "restart"},
        }
        jid = "env-" + uuid.uuid4().hex
        store.add_job(
            jid,
            1,
            job_type="custom",
            status="RECEIVED",
            owner="student",
            job_config=validate_spec(spec, self.inputs),
        )
        storage.stage_job(jid)
        return jid

    def prepare(self, jid, profile="isolated"):
        return preparation.prepare_on_worker(jid, {"id": "local"}, profile)

    def test_custom_runner_uses_prepared_interpreter_dataset_and_cpu_affinity(self):
        (self.project / 'train.py').write_text("""import json,os,sys
from pathlib import Path
import step3_demo_dep
out=Path(os.environ['DL_OUTPUT_DIR'])
out.joinpath('result.json').write_text(json.dumps({
 'job_id':os.environ['DL_JOB_ID'],'prefix':sys.prefix,
 'dependency':step3_demo_dep.__version__, 'cwd':os.getcwd(),
 'dataset':Path(os.environ['DL_DATASET_PATH']).read_text(),
 'cores':len(os.sched_getaffinity(0)),'threads':os.environ['OMP_NUM_THREADS']}))
""")
        jid = self.submit()
        record = self.prepare(jid)['record']
        result = subprocess.run([sys.executable,str(COPY / 'agent/custom_runner.py'),
            '--root',str(self.root / 'worker/worker-storage'),'--job-id',jid],
            capture_output=True,text=True,timeout=30)
        self.assertEqual(result.returncode,0,result.stderr)
        data = json.loads((Path(record['workspace_path']) / 'results/result.json').read_text())
        self.assertEqual(data['job_id'],jid)
        self.assertEqual(data['prefix'],str(Path(record['python_path']).parent.parent))
        self.assertEqual(data['dependency'],'1.0')
        self.assertEqual(data['cwd'],record['source_path'])
        self.assertEqual(data['dataset'],'x,y\n1,2\n')
        self.assertEqual(data['cores'],1)
        self.assertEqual(data['threads'],'1')
        # A tampered prepared source must fail before any submitted argv executes.
        Path(record['source_path']).joinpath('train.py').write_text('raise SystemExit(0)')
        rejected = subprocess.run([sys.executable,str(COPY / 'agent/custom_runner.py'),
            '--root',str(self.root / 'worker/worker-storage'),'--job-id',jid],
            capture_output=True,text=True,timeout=30)
        self.assertNotEqual(rejected.returncode,0)

    def test_real_installation_two_versions_isolation_and_cache_reuse(self):
        first = self.submit()
        a = self.prepare(first)
        verified = preparation.verify_on_worker(
            first, {"id": "local"}, ["step3_demo_dep"]
        )
        self.assertEqual(verified["imports"]["step3_demo_dep"], "1.0")
        self.assertEqual(store.get_job(first)["status"], "READY")
        second = self.submit()
        reused = self.prepare(second)
        self.assertTrue(all(reused["cache_hits"].values()))
        self.assertEqual(a["record"]["python_path"], reused["record"]["python_path"])
        third = self.submit("2.0")
        b = self.prepare(third)
        self.assertTrue(b["cache_hits"]["data_cached"])
        self.assertFalse(b["cache_hits"]["environment_cached"])
        self.assertNotEqual(a["record"]["python_path"], b["record"]["python_path"])
        self.assertEqual(
            preparation.verify_on_worker(third, {"id": "local"}, ["step3_demo_dep"])[
                "imports"
            ]["step3_demo_dep"],
            "2.0",
        )
        self.assertEqual(
            preparation.verify_on_worker(first, {"id": "local"}, ["step3_demo_dep"])[
                "imports"
            ]["step3_demo_dep"],
            "1.0",
        )
        self.assertFalse(list((self.root / "worker/worker-storage/incoming").iterdir()))
        # Fresh controller/worker processes can verify without the student's files.
        shutil.rmtree(self.inputs)
        result = subprocess.run(
            [
                sys.executable,
                str(COPY / "controller/prepare_job.py"),
                "--job-id",
                first,
                "--nodes",
                "local",
                "--verify",
                "--import-module",
                "step3_demo_dep",
            ],
            env=dict(os.environ),
            capture_output=True,
            text=True,
            timeout=60,
        )
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertTrue(json.loads(result.stdout)[0]["verified"])
        self.assertEqual(storage.stage_job(first)["status"], "READY")

    def test_incompatible_python_and_failed_install_never_become_ready(self):
        wrong = self.submit(python="3.99")
        with self.assertRaises(preparation.PreparationError):
            self.prepare(wrong)
        self.assertEqual(store.get_job(wrong)["status"], "STAGED")
        failed = self.submit(version="99.0")
        with self.assertRaises(preparation.PreparationError):
            self.prepare(failed)
        self.assertEqual(store.get_job(failed)["status"], "STAGED")
        self.assertFalse(store.get_job_environments(failed))
        markers = list(
            (self.root / "worker/worker-storage/environments").glob("*/ready.json")
        )
        self.assertFalse(markers)
        with self.assertRaises(preparation.PreparationError):
            self.prepare(failed)

    def test_distributed_readiness_counts_compatible_candidates_and_invalidates_stale_records(
        self,
    ):
        jid = self.submit(workers=2)
        result = self.prepare(jid)
        self.assertEqual(result["status"], "STAGED")
        second = dict(result["record"])
        second["compatibility_key"] = "incompatible"
        self.assertEqual(
            store.record_environment(jid, "other-worker", second), "STAGED"
        )
        self.assertEqual(
            store.record_environment(jid, "other-worker", result["record"]), "READY"
        )
        store.invalidate_environment(jid, "other-worker", "worker verification failed")
        self.assertEqual(store.get_job(jid)["status"], "STAGED")
        self.assertNotIn(
            "other-worker", [r["node_id"] for r in store.get_job_environments(jid)]
        )
        self.assertFalse(store.get_jobs_by_status("QUEUED"))

    def test_corrupt_cache_and_package_inventory_are_rejected(self):
        jid = self.submit()
        result = self.prepare(jid)
        marker = Path(result["record"]["python_path"]).parents[1] / "ready.json"
        previous = marker.read_text()
        value = json.loads(previous)
        value["probe"]["packages"].append(["invented-package", "9.0"])
        marker.write_text(json.dumps(value))
        with self.assertRaises(preparation.PreparationError):
            preparation.verify_on_worker(jid, {"id": "local"}, [])

        marker.write_text(previous)
        dataset = Path(result["record"]["dataset_path"])
        dataset.write_text("corrupted")
        with self.assertRaises(preparation.PreparationError):
            preparation.verify_on_worker(jid, {"id": "local"}, [])

    def test_cpu_profile_rejects_gpu_requests_before_installing(self):
        jid = self.submit(gpus=1)
        with self.assertRaises(preparation.PreparationError):
            self.prepare(jid, profile="pytorch-cpu")
        self.assertEqual(store.get_job(jid)["status"], "STAGED")
        self.assertFalse(store.get_job_environments(jid))


if __name__ == "__main__":
    try:
        unittest.main(verbosity=2)
    finally:
        ISOLATED.cleanup()
