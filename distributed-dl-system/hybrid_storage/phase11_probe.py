"""Trusted hardware acceptance in NEW private state; never a production agent.

Only operator-authorized public fixtures and SSH stdin commands. No student code,
service installation, mounts, package downloads or arbitrary path deletion.
Inventory still uses the existing request-bound authenticated protocol.
"""
from concurrent.futures import ThreadPoolExecutor
import errno
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time

from .cache_eviction import CacheEvictor
from .cache_fetch import CacheFetcher, CacheCapacityError
from .input_manifests import SourceFile, SourceManifest
from .inputs import InputFile, object_key
from .inventory_protocol import Credential, MAX_BYTES
from .inventory_reporter import InventoryReporter
from .nfs_mount import require_input_mount
from .transfer_metrics import TransferMetrics
from .worker_cache import WorkerCache, CacheActivityBusy
from .workspaces import AttemptIdentity, WorkspaceManager, WorkspacePlan


def fixture_files(document):
    if type(document) is not dict or set(document) != {'files'}:
        raise ValueError('Exact public fixture manifest required')
    if type(document['files']) is not list or not 3 <= len(document['files']) <= 8:
        raise ValueError('Three to eight public test files required')
    files = tuple(InputFile(**row) for row in document['files'])
    if len({f.path for f in files}) != len(files) or len({f.sha256 for f in files}) != len(files):
        raise ValueError('Unique fixture paths/digests required')
    if not all(f.size_bytes > 0 for f in files) or sum(f.size_bytes for f in files) > 32*1024**2:
        raise ValueError('Public fixture size bound exceeded')
    return files


def _new(path):
    path.mkdir(mode=0o700)
    return path


class Probe:
    def __init__(self, state, *, worker, mount, source, files, credential):
        state, mount = Path(state), Path(mount)
        if (not state.is_absolute() or state.is_relative_to(mount) or mount.is_relative_to(state)
                or state.parent.stat().st_uid != os.geteuid()
                or state.parent.stat().st_mode & 0o077):
            raise ValueError('New state needs a private service-owned local parent, outside NFS')
        self.mount = require_input_mount(source, target=mount)
        self.state = _new(state); self.files = files
        self.allowed = {(f.sha256, f.size_bytes) for f in files}
        self.metrics = TransferMetrics(worker)
        self.cache = WorkerCache(_new(state/'cache'), input_mount=mount)
        self.fetch = CacheFetcher(self.cache, source=source, quota_bytes=64*1024**2,
            reserve_free_bytes=64*1024**2, io_timeout=30, transfer_metrics=self.metrics)
        self.fetch.initialize(); self.source = source
        self.reporter = InventoryReporter(self.cache, credential, transfer_metrics=self.metrics)
        self.reporter.initialize()
        self.results = {'worker':worker, 'mount':self.mount, 'checks':{}}

    def authorize(self, files):
        return all((f.sha256, f.size_bytes) in self.allowed for f in files)

    def ensure(self):
        return self.fetch.ensure(self.files, authorize=self.authorize)

    def bootstrap(self):
        started = time.perf_counter_ns(); cold = self.ensure(); elapsed = time.perf_counter_ns()-started
        assert all(not f.cache_hit for f in cold)
        cold_stats = self.metrics.prediction_snapshot(); warm = self.ensure()
        assert all(f.cache_hit for f in warm)
        assert self.metrics.prediction_snapshot()['sample_count'] == len(self.files)
        self.results.update(cold_bytes=sum(f.size_bytes for f in cold), cold_elapsed_ns=elapsed,
            cold_effective_mib_per_second=sum(f.size_bytes for f in cold)/2**20/(elapsed/1e9),
            cold_samples=cold_stats, warm_staged_bytes=0)
        self.results['checks']['cold_warm_actual_nfs'] = True
        first = self.files[0]; target = self.cache.root/'objects'/object_key(first.sha256)
        target.chmod(0o600)
        with target.open('r+b') as stream:
            byte = stream.read(1); stream.seek(0); stream.write(bytes([byte[0]^255]))
            stream.flush(); os.fsync(stream.fileno())
        target.chmod(0o400); repaired = self.ensure()
        assert sum(f.size_bytes for f in repaired if not f.cache_hit) == first.size_bytes
        assert hashlib.sha256(target.read_bytes()).hexdigest() == first.sha256
        self.results['checks']['corrupt_refetch'] = True
        cache = WorkerCache(_new(self.state/'concurrent-cache'), input_mount=self.cache.input_mount)
        handles = [CacheFetcher(cache, source=self.source, quota_bytes=64*1024**2,
                    reserve_free_bytes=0, io_timeout=30, transfer_metrics=self.metrics) for _ in range(2)]
        try:
            for handle in handles: handle.initialize()
            with ThreadPoolExecutor(max_workers=2) as pool:
                runs = list(pool.map(lambda h:h.ensure([first], authorize=self.authorize)[0], handles))
            assert sorted(r.cache_hit for r in runs) == [False, True]
        finally:
            for handle in handles: handle.close()
        self.results['checks']['duplicate_parallel_fetch_once'] = True
        small = WorkerCache(_new(self.state/'quota-cache'), input_mount=self.cache.input_mount)
        handle = CacheFetcher(small, source=self.source, quota_bytes=1, reserve_free_bytes=0)
        try:
            handle.initialize()
            try: handle.ensure([first], authorize=self.authorize)
            except CacheCapacityError as error: assert error.errno == errno.EDQUOT
            else: raise AssertionError('Quota should fail')
            assert list((small.root/'parts').iterdir()) == []
        finally: handle.close()
        self.results['checks']['quota_full_no_partial'] = True
        source = SourceManifest(tuple(SourceFile(f.path, f.sha256, f.size_bytes) for f in self.files))
        plan = WorkspacePlan(AttemptIdentity('public-phase11', 'public-phase11:0'), source.sha256, source)
        manager = WorkspaceManager(self.cache, _new(self.state/'workspaces'),
                    max_input_bytes=32*1024**2, reserve_free_bytes=0)
        manager.initialize(); receipt = manager.prepare(plan, authorize=lambda _:True)
        limited = CacheFetcher(self.cache, source=self.source, quota_bytes=1, reserve_free_bytes=0)
        gc = CacheEvictor(limited, protected_roots=(manager.root,))
        try:
            assert gc.plan().status == 'blocked_pins'
            with self.cache.open_verified(first.sha256, first.size_bytes):
                try: gc.plan()
                except CacheActivityBusy: pass
                else: raise AssertionError('Open reader did not exclude eviction')
            for f in self.files:
                path = receipt.source/f.path
                assert hashlib.sha256(path.read_bytes()).hexdigest() == f.sha256
                assert path.stat().st_ino != (self.cache.root/'objects'/object_key(f.sha256)).stat().st_ino
            self.results['checks']['private_workspace_pins_and_reader_exclusion'] = True
            self.results['isolation'] = self.isolation(receipt.source)
        finally:
            # No trainer was launched; the bounded isolation child was reaped.
            manager.finish(plan, 'CANCELLED', quiescent=lambda _:True); limited.close()
        self.results['checks']['pins_release_after_owned_probe_quiescence'] = True
        self.results['metrics'] = self.metrics.snapshot()
        return self.results

    def isolation(self, source):
        if os.geteuid() != 0:
            return {'tested':False, 'reason':'distinct-UID namespace fixture requires administrator'}
        view = _new(self.state/'execution-view')
        for item in self.files:
            target = view/item.path; target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source/item.path, target); target.chmod(0o444)
        view.chmod(0o555)
        program = """import errno,json,os,socket,sys
assert os.getuid()==65534
for path in sys.argv[1:]:
 try:open(path,'rb')
 except (FileNotFoundError,PermissionError):pass
 else:raise AssertionError('private path exposed')
try:open('/work/forbidden','wb')
except OSError as e:assert e.errno in (errno.EROFS,errno.EACCES)
else:raise AssertionError('input writable')
with socket.socket() as s:
 s.settimeout(1)
 try:s.connect(('192.168.10.214',2049))
 except OSError:pass
 else:raise AssertionError('host network exposed')
assert len(os.listdir('/work'))>=3
print(json.dumps({'tested':True,'uid':os.getuid(),'private_paths_hidden':True,'inputs_readonly':True,'network_isolated':True}))
"""
        command = ['/usr/bin/setpriv','--no-new-privs','/usr/bin/bwrap','--unshare-all','--die-with-parent','--new-session',
            '--clearenv','--setenv','PATH','/usr/bin:/bin','--setenv','LANG','C',
            '--uid','65534','--gid','65534','--cap-drop','ALL','--ro-bind','/usr','/usr',
            '--symlink','usr/bin','/bin','--proc','/proc','--dev','/dev','--tmpfs','/tmp',
            '--dir','/work','--ro-bind',str(view),'/work']
        for name in ('/lib','/lib64'):
            if Path(name).exists(): command += ['--ro-bind',name,name]
        command += ['--chdir','/work','--','/usr/bin/python3','-I','-B','-c',program,
            str(self.cache.root/'state/cache.sqlite3'),str(self.cache.input_mount/'sha256'),str(self.state.parent/'source')]
        result = subprocess.run(command, stdin=subprocess.DEVNULL, capture_output=True, text=True, timeout=15)
        if result.returncode: raise RuntimeError('Isolation negative test failed: '+result.stderr[:2048])
        return json.loads(result.stdout)

    def evict_crash(self):
        quota = sum(f.size_bytes for f in self.files)
        program = """import os,sys
from pathlib import Path
sys.path.insert(0,sys.argv[1])
from hybrid_storage.worker_cache import WorkerCache
from hybrid_storage.cache_fetch import CacheFetcher
from hybrid_storage.cache_eviction import CacheEvictor
c=WorkerCache(Path(sys.argv[2]),input_mount=Path(sys.argv[3]))
g=CacheEvictor(CacheFetcher(c,source=sys.argv[4],quota_bytes=int(sys.argv[5]),reserve_free_bytes=0))
p=g.plan()
assert p.status=='ready'
def crash(fd,name):
 os.unlink(name,dir_fd=fd);os._exit(42)
g._unlink=crash
g.apply(p,dry_run=False)
"""
        p = subprocess.run([sys.executable,'-I','-B','-c',program,str(Path(__file__).resolve().parents[1]),
            str(self.cache.root),str(self.cache.input_mount),self.source,str(quota)],
            capture_output=True, text=True, timeout=15)
        assert p.returncode == 42, p.stderr
        result = CacheEvictor(self.fetch, notify=self.reporter.request_full).reconcile()
        assert result['missing'] >= 1
        self.results['checks']['real_eviction_process_crash_and_reconciliation'] = True
        return result

    def restage(self):
        results = self.ensure(); staged = sum(f.size_bytes for f in results if not f.cache_hit)
        assert staged > 0; assert all(f.cache_hit for f in self.ensure())
        self.reporter.request_full()
        return {'staged_bytes':staged,'warm_staged_bytes':0,'metrics':self.metrics.snapshot()}

    def close(self):
        self.reporter.close(); self.fetch.close()


def main():
    config = json.loads(sys.stdin.buffer.readline(MAX_BYTES+1))
    files = fixture_files(config['manifest'])
    credential = Credential(config['worker'], config['key_id'], bytes.fromhex(config['secret_hex']))
    probe = Probe(config['state'], worker=config['worker'], mount=config['mount'],
                  source=config['source'], files=files, credential=credential)
    try:
        print(json.dumps({'ready':probe.bootstrap()}), flush=True)
        for raw in sys.stdin.buffer:
            if len(raw) > MAX_BYTES: raise ValueError('Probe command bound')
            command = json.loads(raw)
            if command['action'] == 'inventory':
                import base64
                body, mac = probe.reporter.reply(base64.b64decode(command['request']), command['mac'], command['key_id'])
                result = dict(body=base64.b64encode(body).decode(), mac=mac)
            elif command['action'] == 'evict-crash': result = probe.evict_crash()
            elif command['action'] == 'restage': result = probe.restage()
            elif command['action'] == 'close': break
            else: raise ValueError('Unknown trusted probe operation')
            print(json.dumps(result), flush=True)
    finally: probe.close()


if __name__ == '__main__': main()
