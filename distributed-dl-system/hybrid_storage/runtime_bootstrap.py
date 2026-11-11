"""Explicit hybrid service assembly. No I/O or automatic activation on import.

The caller supplies protected roots, worker keys and an enforceable launcher.
Schema provisioning is separate; normal legacy startup does not call this module.
"""
from contextlib import ExitStack
from dataclasses import dataclass
from pathlib import Path
import sqlite3
import threading
import ipaddress

from .backends import _directory, _root
from .worker_cache import WorkerCache, _protected
from .cache_fetch import CacheFetcher
from .workspaces import WorkspaceManager
from .inventory_protocol import Credential
from .inventory_reporter import InventoryReporter
from .config import require_staging_config


def _private_address(value):
    address = ipaddress.IPv4Address(value)
    if not any(address in ipaddress.IPv4Network(net) for net in ('10.0.0.0/8','172.16.0.0/12','192.168.0.0/16')):
        raise ValueError('Hybrid runtime requires explicitly configured private LAN endpoints')
    return address


@dataclass(frozen=True)
class WorkerPaths:
    worker: Path
    cache: Path
    workspaces: Path
    checkpoint_spool: Path
    inputs: Path

    def __post_init__(self):
        for name in self.__dataclass_fields__:
            object.__setattr__(self, name, _root(Path(getattr(self, name))))
        local = (self.worker, self.cache, self.workspaces, self.checkpoint_spool)
        for index, left in enumerate(local):
            for right in local[index+1:] + (self.inputs,):
                if left.is_relative_to(right) or right.is_relative_to(left):
                    raise ValueError('Worker state/cache/workspace/spool/input roots must be separate')


class LaunchBoundary:
    """Explicit launcher contract; LinuxBoundary is the production provider.

    A production CLI must select its concrete provider explicitly, never import a
    dotted name from student data or substitute a no-op callback.
    """
    def preflight(self):
        raise NotImplementedError

    def isolate(self, command, workspace, data):
        raise NotImplementedError

    def terminate(self, identity, record):
        raise NotImplementedError

    def quiescent(self, identity, record):
        raise NotImplementedError


class WorkerRuntime:
    def __init__(self, module, paths, credential, boundary, *, source,
                 cache_bytes, workspace_bytes, reserve_bytes=512*1024**2,
                 io_timeout=15, watchdog=True, environment_preparer=None):
        if (not isinstance(paths, WorkerPaths) or not isinstance(credential, Credential)
                or not isinstance(boundary, LaunchBoundary)):
            raise TypeError('Typed protected paths, credential and launch boundary required')
        if Path(module.BASE_DIR) != paths.worker or module.node_id() != credential.worker:
            raise ValueError('Runtime identity differs from actual agent')
        from .nfs_mount import validate_source
        source = validate_source(source)
        _private_address(source.split(':',1)[0])
        self.module, self.paths, self.credential, self.boundary = module, paths, credential, boundary
        self.source, self.cache_bytes, self.workspace_bytes = source, cache_bytes, workspace_bytes
        self.reserve_bytes, self.io_timeout, self.watchdog = reserve_bytes, io_timeout, watchdog
        self.environment_preparer = environment_preparer
        self._resources = ExitStack()
        self._started = False
        self._failed = False
        self._used = False
        self._cleanup_pending = False

    def start(self):
        if self._started or self._failed or self._used:
            raise ValueError('Worker runtime cannot be started again')
        if self.module.job_process is not None or self.module.app._got_first_request:
            raise ValueError('Worker must be idle and Flask setup still open')
        # Check the boundary before cache initialization, auth hooks or launch authority.
        if self.boundary.preflight() is not True:
            raise PermissionError('Linux launch boundary is not ready')
        for path in (self.paths.worker, self.paths.cache, self.paths.workspaces, self.paths.checkpoint_spool):
            with _directory(path) as fd:
                _protected(fd, directory=True)
        from .nfs_mount import require_input_mount
        require_input_mount(self.source, target=self.paths.inputs)
        # agent.py deliberately uses its sibling-module convention; importing
        # `agent.staging` breaks when the running entry point is agent/agent.py.
        from staging import StagingService, install_staging_service
        from checkpoint_service import CheckpointService, install_checkpoint_service
        from cache_storage import install_inventory_reporter
        try:
            from .transfer_metrics import TransferMetrics
            from .admin_observations import NFSHealthProbe
            self.transfers = TransferMetrics(self.credential.worker)
            self.cache = WorkerCache(self.paths.cache, input_mount=self.paths.inputs)
            self.fetcher = CacheFetcher(self.cache, source=self.source, quota_bytes=self.cache_bytes,
                                        reserve_free_bytes=self.reserve_bytes, io_timeout=self.io_timeout,
                                        transfer_metrics=self.transfers)
            self.fetcher.initialize()
            self._resources.callback(self.fetcher.close)
            self.workspaces = WorkspaceManager(self.cache, self.paths.workspaces,
                max_input_bytes=self.workspace_bytes, reserve_free_bytes=self.reserve_bytes)
            self.workspaces.initialize()
            self.reporter = InventoryReporter(self.cache, self.credential, transfer_metrics=self.transfers,
                nfs_health=NFSHealthProbe(self.source,target=self.paths.inputs))
            self.reporter.initialize()
            self._resources.callback(self.reporter.close)
            self.service = StagingService(self.fetcher, self.workspaces, self.credential,
                worker_root=self.paths.worker, ownership=self.module.ownership,
                cancelled=self.module.attempt_cancelled, isolate=self.boundary.isolate,
                quiescent=self.boundary.quiescent, terminate=self.boundary.terminate)
            self.checkpoints = CheckpointService(self.credential, worker_root=self.paths.worker,
                spool_root=self.paths.checkpoint_spool, reserve_free_bytes=self.reserve_bytes)
            # These existing installers enforce route ordering and domain-separated MACs.
            install_staging_service(self.module, self.service, watchdog=self.watchdog)
            install_checkpoint_service(self.module, self.checkpoints)
            install_inventory_reporter(self.module.app, self.reporter)
            if self.environment_preparer is not None:
                from .offline_prepare import install_preparer
                install_preparer(self.module,self.environment_preparer)
            self._started = True
            self._used = True
            return self
        except BaseException:
            # Flask setup is not reversible after partial installation. Never
            # continue serving that app as legacy; the process must exit.
            self._failed = True
            service = getattr(self, 'service', None)
            if service is not None and not service.close():
                self._cleanup_pending = True
                raise RuntimeError('Failed bootstrap still owns staging; terminate the service process')
            self._resources.close()
            raise

    def close(self):
        if self._cleanup_pending:
            if not self.service.close():
                return False
            self._cleanup_pending = False
        if not self._started:
            self._resources.close()
            return True
        if self.service.busy():
            return False  # Keep watchdog and durable ownership until cleanup proves quiescence.
        if not self.service.close():
            return False  # Retain fetch/reporter ownership while staging is active.
        self._resources.close()
        self._started = False
        return True


class ControllerRuntime:
    """Own the dedicated connection and singleton inventory in the dispatcher thread."""
    def __init__(self, registry, credentials, *, configured_workers, inventory_root,
                 checkpoint_root, settings, session=None, result_collector=None):
        require_staging_config(settings)
        from .registry import StorageRegistry
        if not isinstance(registry, StorageRegistry):
            raise TypeError('Explicit initialized LOCAL registry required')
        keys = list(credentials)
        if not keys or any(not isinstance(key, Credential) for key in keys):
            raise TypeError('Explicit worker credentials required')
        workers = tuple(configured_workers)
        if (len(set(workers)) != len(workers) or len({key.worker for key in keys}) != len(keys)
                or set(workers) != {key.worker for key in keys}):
            raise ValueError('Pilot workers and enrolled credentials must match exactly')
        self.registry, self.settings, self.credentials = registry, settings, {key.worker:key for key in keys}
        self.result_collector = result_collector
        self.workers, self.inventory_root, self.checkpoint_root = workers, Path(inventory_root), Path(checkpoint_root)
        self._owner = threading.get_ident()
        self._started = False
        self._endpoints = frozenset()
        self._own_session = session is None
        if session is None:
            import requests
            session = requests.Session()
        session.trust_env = False
        self.session = session

    def start(self):
        if self._started or threading.get_ident() != self._owner:
            raise ValueError('Start controller runtime once on its DB owner thread')
        from .cache_inventory import CacheInventory
        from .checkpoint_publication import LocalCheckpointPublisher
        from .checkpoint_transfer import CheckpointCollector
        from .checkpoint_restore import CheckpointRestorer
        from .staging_protocol import StagingClient
        from .cache_coverage import CoverageAPI
        from .bandwidth import TransferObservations
        from .admin_observations import AdminObservations
        from . import registry_schema, cache_registry_schema, staging_schema, cleanup_schema, checkpoint_schema, restore_schema
        from checkpoint_recovery import CheckpointDispatch
        from cache_inventory import InventoryHeartbeat
        registry_schema.require_schema(self.registry.db)
        cache_registry_schema.require_schema(self.registry.db)
        if not all((staging_schema._installed(self.registry.db), cleanup_schema.installed(self.registry.db),
                    checkpoint_schema.installed(self.registry.db), restore_schema.installed(self.registry.db))):
            raise ValueError('Explicit staging/cleanup/checkpoint/restore migrations required')
        for worker, key in self.credentials.items():
            row = self.registry.db.execute('SELECT auth_principal,auth_state,enabled FROM cache_registry_workers WHERE worker_id=?',
                                           (worker,)).fetchone()
            if row is None or tuple(row) != (key.principal, 'ENROLLED', 1):
                raise PermissionError('Explicit matching enabled worker enrollment required')
        self.publisher = LocalCheckpointPublisher(self.registry, self.checkpoint_root)
        # Validate the previously provisioned store without adopting an empty root.
        with self.publisher._store():
            pass
        main = Path(next(row[2] for row in self.registry.db.execute('PRAGMA database_list') if row[1]=='main'))
        def connect():
            # No create if the configured DB disappears; do not initialize schema here.
            from urllib.parse import quote
            db = sqlite3.connect('file:'+quote(str(main), safe='/')+'?mode=rw', uri=True, timeout=5)
            db.execute('PRAGMA foreign_keys=ON'); db.execute('PRAGMA synchronous=FULL')
            return db
        self.transfers = TransferObservations()
        self.observations = AdminObservations(self.inventory_root)
        self.inventory = CacheInventory(connect, list(self.credentials.values()),
            configured_workers=self.workers, state_root=self.inventory_root,
            transfer_observations=self.transfers, admin_observations=self.observations)
        self.inventory.start()
        try:
            self.collector = CheckpointCollector(self.publisher, self.credentials, get=self._get)
            self.restorer = CheckpointRestorer(self.publisher, self.credentials, post=self._post)
            self.client = StagingClient(self.credentials, post=self._post)
            self.coverage = CoverageAPI(self.registry, self.inventory)
            self.dispatch = CheckpointDispatch(self.registry, self.client, publisher=self.publisher,
                collector=self.collector, restorer=self.restorer, settings=self.settings, coverage=self.coverage)
            if self.result_collector is not None:
                self.dispatch.result_collector = self.result_collector
            self.heartbeat = InventoryHeartbeat(self.inventory, settings=self.settings)
            self._started = True
            return self
        except BaseException:
            self.inventory.close()
            raise

    def _request(self, method, url, **kwargs):
        from urllib.parse import urlsplit
        parsed = urlsplit(url)
        if (not self._started or parsed.scheme != 'http' or parsed.username is not None
                or parsed.password is not None or parsed.fragment
                or (parsed.hostname, parsed.port) not in self._endpoints):
            raise PermissionError('Runtime transport requires a validated selected-worker endpoint')
        # Signed POSTs must not follow a redirect with manifests/checkpoint bytes.
        kwargs['allow_redirects'] = False
        return getattr(self.session, method)(url, **kwargs)

    def _get(self, url, **kwargs):
        return self._request('get', url, **kwargs)

    def _post(self, url, **kwargs):
        return self._request('post', url, **kwargs)

    def poll(self, nodes):
        if not self._started or threading.get_ident() != self._owner:
            raise ValueError('Controller runtime must be polled on its DB owner thread')
        nodes = tuple(nodes)
        if len(nodes) != len(self.workers) or {node['id'] for node in nodes} != set(self.workers):
            raise ValueError('Runtime node scope differs from enrolled workers')
        for node in nodes:
            _private_address(node['ip'])
            if type(node['port']) is not int or not 1 <= node['port'] <= 65535:
                raise ValueError('Hybrid runtime requires explicitly configured private LAN endpoints')
        self._endpoints = frozenset((node['ip'], node['port']) for node in nodes)
        for node in nodes:
            self.heartbeat.poll(node)
        self.heartbeat.reconcile()

    def close(self):
        if threading.get_ident() != self._owner:
            raise ValueError('Close runtime on its DB owner thread')
        if self._started:
            self.heartbeat.close()
            self.inventory.close()
            self._started = False
            self._endpoints = frozenset()
        if self._own_session:
            self.session.close()


def dispatcher_cycle(module, runtime=None, *, admission=True):
    """Same ordering as legacy main; runtime supplied to every relevant seam."""
    if type(admission) is not bool:
        raise TypeError('Explicit admission boolean required')
    if runtime is None:
        module.reconcile_cancellations()
        module.reconcile_scheduled_jobs()
        module.monitor_running_jobs()
        if admission:
            module.dispatch_queued_jobs()
        return
    if not isinstance(runtime, ControllerRuntime) or not runtime._started:
        raise ValueError('Started owned controller runtime required')
    from scheduler import load_nodes
    runtime.poll(load_nodes())
    module.reconcile_cancellations(storage_runtime=runtime.dispatch)
    module.reconcile_scheduled_jobs(storage_runtime=runtime.dispatch)
    module.monitor_running_jobs(storage_runtime=runtime.dispatch)
    if admission:
        module.dispatch_queued_jobs(storage_runtime=runtime.dispatch)
