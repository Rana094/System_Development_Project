"""Explicit operator-owned runtime profile. Legacy entry points remain default.

Configuration/credentials are private LOCAL files. This module never provisions
users, firewall, schema, NFS or systemd; those are separate reviewed operations.
"""
import os
from pathlib import Path
import sys

from .inventory_protocol import fields,load_credential
from .linux_boundary import protected_json,LinuxConfig,LinuxBoundary
from .config import validate_config,require_staging_config
from .runtime_bootstrap import WorkerPaths,WorkerRuntime,ControllerRuntime,_private_address


def configuration(path):
    value=protected_json(Path(path))
    fields(value,'schema_version role state_root code_root bind_ip port settings nodes credentials paths boundary capsule')
    if value['schema_version']!=1 or value['role'] not in ('worker','controller','portal'):
        raise ValueError('Unsupported explicit runtime profile')
    require_staging_config(validate_config(value['settings']))
    _private_address(value['bind_ip'])
    if type(value['port']) is not int or not 1024<=value['port']<20000:raise ValueError('Invalid service port')
    state=Path(value['state_root']);code=Path(value['code_root'])
    if not state.is_absolute() or code!=Path(__file__).resolve().parents[1]:raise PermissionError('Installed source identity differs')
    # Validate protected LOCAL state before importing legacy modules with startup I/O.
    from .backends import _directory
    from .worker_cache import _protected
    with _directory(state) as fd:_protected(fd,directory=True)
    if set(value['credentials'])!={n['id'] for n in value['nodes']}:raise ValueError('Exact credential scope required')
    for node in value['nodes']:
        _private_address(node['ip'])
        if node['port']!=5001:raise ValueError('Reviewed pilot control port required')
    return value


def configure_environment(value):
    code=Path(value['code_root']);state=Path(value['state_root'])
    sys.path.insert(0,str(code/'agent'));sys.path.insert(0,str(code/'controller'))
    if value['role']=='worker':
        if os.geteuid()!=0:raise PermissionError('Trusted worker service must run as root')
        os.environ['DL_AGENT_STATE_ROOT']=str(state)
        os.environ['DL_NODE_ID']=value['paths']['worker_id']
        os.environ['DL_NODE_ADDRESS']=value['bind_ip'];os.environ['DL_NETWORK_INTERFACE']='eth0'
    else:
        os.environ['DL_CONTROLLER_STATE_ROOT']=str(state)
        os.environ['DL_STORAGE_ROOT']=value['paths']['jobs']
        os.environ['DL_NODES_CONFIG']=value['paths']['nodes']


def run(path):
    value=configuration(path);configure_environment(value)
    # Stable allowlisted storage events only. Queue insertion never waits for a
    # log sink and overload drops telemetry rather than slowing lease handling.
    import logging,logging.handlers,queue
    from . import events
    class BoundedHandler(logging.handlers.QueueHandler):
        def handleError(self,record):pass
    messages=queue.Queue(maxsize=1024)
    handler=BoundedHandler(messages);events.LOGGER.addHandler(handler)
    events.LOGGER.setLevel(logging.INFO);events.LOGGER.propagate=False
    listener=logging.handlers.QueueListener(messages,logging.StreamHandler(sys.stdout));listener.start()
    settings=validate_config(value['settings'])
    keys={w:load_credential(Path(p),expected_worker=w) for w,p in value['credentials'].items()}
    if value['role']=='worker':
        import agent
        from .offline_prepare import OfflinePreparer
        p=value['paths'];worker=p['worker_id']
        if set(keys)!={worker}:raise PermissionError('Worker may only receive its own key')
        boundary=LinuxBoundary(LinuxConfig(**value['boundary']))
        runtime=WorkerRuntime(agent,WorkerPaths(Path(value['state_root']),*(Path(p[k]) for k in ('cache','workspaces','spool','inputs'))),
            keys[worker],boundary,source=p['source'],cache_bytes=p['cache_bytes'],workspace_bytes=boundary.config.max_input_bytes,
            environment_preparer=OfflinePreparer(Path(value['state_root']),protected_json(Path(value['capsule'])))).start()
        try:
            from waitress import serve
            # Metadata routes retain their own 1-MiB authentication bound. The
            # existing authenticated raw restore route also needs the reviewed
            # checkpoint budget; do not truncate CNN/DDP restores to metadata size.
            serve(agent.app,host=value['bind_ip'],port=value['port'],threads=8,
                  max_request_body_size=max(1024**2,boundary.config.max_checkpoint_bytes))
        finally:runtime.close()
        return
    import job_store
    p=value['paths']
    if value['role']=='portal':
        from .portal_service import HybridPortal
        import student_portal
        service=HybridPortal(db_path=job_store.DB_FILE,objects=p['objects'],scratch=p['scratch'],checkpoints=p['checkpoints'],
            nodes=value['nodes'],credentials=keys,settings=settings)
        app=student_portal.create_app(Path(p['portal']),background=True,hybrid_service=service)
        app.config['SESSION_COOKIE_NAME']='dl_hybrid_pilot'
        from waitress import serve
        serve(app,host=value['bind_ip'],port=value['port'],threads=8,max_request_body_size=256*1024**2)
        return
    from .registry import StorageRegistry
    from .artifacts import ArtifactRegistrar
    from .protected_results import ProtectedCollector
    import sqlite3
    import dispatcher
    db=sqlite3.connect(Path(job_store.DB_FILE).as_uri()+'?mode=rw',uri=True,timeout=10)
    db.execute('PRAGMA foreign_keys=ON');db.execute('PRAGMA synchronous=FULL')
    runtime=ControllerRuntime(StorageRegistry(db,ArtifactRegistrar(Path(p['objects']),staging_root=Path(p['scratch']))),
        keys.values(),configured_workers=keys,inventory_root=p['inventory'],checkpoint_root=p['checkpoints'],settings=settings,
        result_collector=ProtectedCollector(Path(value['code_root'])/'tools/hybrid_result_bridge.py'))
    try:
        runtime.start();dispatcher.main(storage_runtime=runtime,storage_settings=settings)
    finally:runtime.close();db.close()
