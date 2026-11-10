"""Internal disposable case worker. Never accepts external hosts, paths or PIDs."""
import errno
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import unittest
from unittest.mock import patch
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / 'tools/benchmarks'))
sys.path.insert(0, str(ROOT))
from evaluation import runner


def serve_health(ready):
    # Import occurs only from a NEW per-case source copy, so agent module-level
    # logs/checkpoints creation cannot touch the real installation.
    spec = importlib.util.spec_from_file_location('disposable_agent', ROOT / 'agent/agent.py')
    module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
    module.node_id = lambda: 'disposable-evaluation-agent'
    def restricted(environ, start_response):
        if environ.get('PATH_INFO') != '/health' or environ.get('REQUEST_METHOD') != 'GET':
            start_response('404 Not Found', [('Content-Length', '0')]); return [b'']
        return module.app(environ, start_response)
    from werkzeug.serving import make_server
    with make_server('127.0.0.1', 0, restricted) as server:
        runner.write_json(ready, dict(port=server.server_port, pid=os.getpid()))
        server.serve_forever()


def agent_case(folder):
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    processes, logs = [], []
    def start(label):
        ready = folder / (label + '-ready.json'); log = runner.new_file(folder / (label + '-agent.log')); logs.append(log)
        process = subprocess.Popen([sys.executable, '-I', '-B', str(Path(__file__).resolve()), '--serve-health', str(ready)],
            cwd=folder, stdout=log, stderr=subprocess.STDOUT)
        processes.append(process); deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if process.poll() is not None: raise RuntimeError('Disposable agent failed to start')
            if ready.exists():
                info = json.loads(ready.read_text()); assert info['pid'] == process.pid
                url = f"http://127.0.0.1:{info['port']}/health"
                try:
                    with opener.open(url, timeout=0.25) as response: body = json.load(response)
                    if body['node'] == 'disposable-evaluation-agent' and body['status'] == 'ONLINE': return process, url
                except (OSError, urllib.error.URLError): pass
            time.sleep(0.025)
        raise TimeoutError('Disposable agent readiness deadline')
    try:
        first, url = start('first')
        injected = time.monotonic_ns(); first.kill()
        deadline = time.monotonic() + 5; observed = None
        while time.monotonic() < deadline:
            try:
                with opener.open(url, timeout=0.25) as response: response.read(4096)
            except (OSError, urllib.error.URLError) as error:
                observed = type(error).__name__; break
            time.sleep(0.01)
        if observed is None: raise AssertionError('Health loss never observed')
        detection = time.monotonic_ns() - injected
        assert first.wait(timeout=5) == -9
        restart = time.monotonic_ns(); second, _ = start('replacement'); restarted = time.monotonic_ns() - restart
        assert second.pid != first.pid
        return dict(detection_ns=detection, restart_ns=restarted, metric_scope='private loopback health-only agent process; not job recovery',
            details=dict(killed_owned_pid=first.pid, replacement_owned_pid=second.pid, failed_probe=observed, production_agent=False))
    finally:
        for process in processes:
            if process.poll() is None: process.kill()
            process.wait(timeout=5)
        for log in logs: log.close()


def cache_case(folder, corrupt):
    from evaluation import datasets, matrix_context
    from hybrid_storage.inputs import InputFile, object_key
    from hybrid_storage.worker_cache import CacheCorrupt
    recipe = datasets.generate(folder, 'regression', 12, 123, max_bytes=10000)
    context = matrix_context.Context(folder / 'component', None, library_root=ROOT)
    try:
        context.register(ROOT / 'examples/checkpoint_project', folder / recipe['path'], 1)
        cache, fetcher, _, _ = context.worker('slot-00')
        data = next(entry for entry in context.data.entries if entry.type == 'file')
        exact = (InputFile('dataset.csv', data.sha256, data.size_bytes),)
        source = context.root / 'objects' / object_key(data.sha256)
        original = source.read_bytes(); assert hashlib.sha256(original).hexdigest() == data.sha256
        def ensure(): return fetcher.ensure(exact, authorize=lambda items: items == exact)
        target = cache.root / 'objects' / object_key(data.sha256)
        if corrupt:
            assert not ensure()[0].cache_hit
            bad = b'X' * data.size_bytes
            target.chmod(0o600); target.write_bytes(bad); target.chmod(0o400)
            injected = time.monotonic_ns()
            try: cache.verify(data.sha256, data.size_bytes)
            except CacheCorrupt: detection = time.monotonic_ns() - injected
            else: raise AssertionError('Corrupted cache was accepted')
            started = time.monotonic_ns(); repaired = ensure()[0]; staged = time.monotonic_ns() - started
            assert not repaired.cache_hit and target.read_bytes() == original
            quarantined = list((cache.root / 'quarantine').iterdir())
            assert any(p.read_bytes() == bad for p in quarantined)
            assert ensure()[0].cache_hit and source.read_bytes() == original
            return dict(detection_ns=detection, staging_ns=staged, metric_scope='forced cache verification and subsequent repair ensure; no trainer restart',
                details=dict(refetched_bytes=data.size_bytes, quarantine_preserved=True, warm_followup_transferred_bytes=0, authoritative_sha256=data.sha256))

        # Interrupt a real helper read; only its kernel NFS identity is mocked.
        # Each helper records exact monotonic boundaries in this private folder.
        events = folder / 'copy-events.jsonl'
        program = matrix_context.LOCAL_COPY.replace(" print(json.dumps(copy_request", """
 from contextlib import contextmanager
 import errno,time
 from pathlib import Path
 from hybrid_storage.inputs import NFSObjectStorageBackend
 original=NFSObjectStorageBackend.open_artifact
 @contextmanager
 def interrupted(self,key):
  with original(self,key) as source:
   class Reader:
    started=False
    def fileno(self):return source.fileno()
    def read(self,count):
     if self.started:
      with Path(sys.argv[3]).open('a') as event:event.write(json.dumps(dict(event='injected_read_error',at_ns=time.monotonic_ns()))+'\\n')
      raise OSError(errno.ESTALE,'disposable interrupted source')
     self.started=True
     return source.read(min(count,3))
   yield Reader()
 NFSObjectStorageBackend.open_artifact=interrupted
 print(json.dumps(copy_request"""
        )
        spawn = fetcher._spawn; attempts = []
        def interrupted_spawn(request, output, lock):
            attempts.append(request['sha256'])
            if len(attempts) > 1:
                # Retry admission proves incomplete bytes were not published.
                assert not target.exists()
                with events.open('a') as event: event.write(json.dumps(dict(event='retry_started', at_ns=time.monotonic_ns())) + '\n')
                return spawn(request, output, lock)
            return subprocess.Popen([sys.executable, '-I', '-B', '-c', program, str(ROOT), json.dumps(request), str(events)],
                stdin=subprocess.DEVNULL, stdout=output, stderr=subprocess.DEVNULL, pass_fds=(request['output_fd'], lock))
        with patch.object(fetcher, '_spawn', side_effect=interrupted_spawn):
            started = time.monotonic_ns(); result = ensure()[0]; staged = time.monotonic_ns() - started
        history = [json.loads(line) for line in events.read_text().splitlines()]
        assert [event['event'] for event in history] == ['injected_read_error', 'retry_started']
        assert result.attempts == 2 and len(attempts) == 2 and target.read_bytes() == original
        assert not list((cache.root / 'parts').glob('*.part')) and source.read_bytes() == original
        return dict(detection_ns=history[1]['at_ns'] - history[0]['at_ns'], staging_ns=staged,
            metric_scope='injected helper read error to retry spawn (includes backoff); full ensure duration overlaps detection',
            details=dict(attempts=result.attempts, accepted_payload_bytes=data.size_bytes, interrupted_read_bytes=3, retry_started_without_final_object=True, partials_accepted=False))
    finally:
        context.close()


def main():
    if len(sys.argv) == 3 and sys.argv[1] == '--serve-health':
        serve_health(Path(sys.argv[2])); return 0
    from evaluation.faults import CASES, METRICS
    case, folder = sys.argv[1], Path(sys.argv[2])
    if case not in CASES or folder != Path.cwd(): raise ValueError('Invalid controlled case root')
    # This private test harness must retain every allowlisted child in the case
    # process group, including production cache helpers that normally setsid.
    # Reviewed helper/native-rank bodies do not themselves call setsid/setpgid.
    # The parent can therefore reap the entire case on timeout without finding
    # or signaling any unrelated host process. Never apply this to live agents.
    original_popen = subprocess.Popen
    def contained_popen(*args, **kwargs):
        kwargs['start_new_session'] = False
        kwargs.pop('process_group', None)
        return original_popen(*args, **kwargs)
    subprocess.Popen = contained_popen
    definition = CASES[case]
    result = dict(case=case, status='PASS', **{key: None for key in METRICS})
    if 'test' in definition:
        # Fixtures establish their own controller/agent import order. Adding
        # agent/ here would shadow the namespace package with agent/agent.py.
        sys.path.insert(0, str(ROOT / 'tests'))
        suite = unittest.defaultTestLoader.loadTestsFromName(definition['test'])
        outcome = unittest.TextTestRunner(verbosity=2).run(suite)
        if not outcome.wasSuccessful() or outcome.testsRun != 1 or outcome.skipped: return 1
        result.update(metric_scope='phase latencies not instrumented in reused regression fixture', details=dict(test=definition['test'], tests_run=outcome.testsRun, phase_latencies_missing=True))
    elif definition['worker'] == 'agent': result.update(agent_case(folder))
    else: result.update(cache_case(folder, definition['worker'] == 'corrupt'))
    runner.write_json(folder / 'measurements.json', result)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
