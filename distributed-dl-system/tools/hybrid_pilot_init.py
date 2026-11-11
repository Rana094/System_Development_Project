"""Initialize only the new private pilot controller database, as its owner."""
from pathlib import Path
import sys
sys.dont_write_bytecode=True
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
if __name__=='__main__':
    from hybrid_storage.service_entry import configuration,configure_environment
    from hybrid_storage.config import validate_config
    config=configuration(Path(sys.argv[1]));configure_environment(config)
    if config['role']!='controller':raise ValueError('Controller profile required')
    import job_store,recovery_metrics,sqlite3,time
    from hybrid_storage.deployment_migrations import MIGRATIONS
    from importlib import import_module
    db=sqlite3.connect(job_store.DB_FILE);db.execute('PRAGMA foreign_keys=ON');db.execute('PRAGMA synchronous=FULL')
    for name in MIGRATIONS:import_module('hybrid_storage.'+name).migrate(db)
    recovery_metrics.init_table()
    from hybrid_storage.inventory_protocol import load_credential
    for worker,path in config['credentials'].items():
        key=load_credential(Path(path),expected_worker=worker);now=time.time_ns()
        with db:
            existing=db.execute('SELECT auth_principal,auth_state,enabled FROM cache_registry_workers WHERE worker_id=?',(worker,)).fetchone()
            if existing is not None and existing!=(key.principal,'ENROLLED',1):raise PermissionError('Existing enrollment differs')
            db.execute("INSERT OR IGNORE INTO cache_registry_workers(worker_id,auth_principal,auth_state,enabled,created_at_ns,updated_at_ns) VALUES (?,?,'ENROLLED',1,?,?)",(worker,key.principal,now,now))
    from hybrid_storage.artifacts import ArtifactRegistrar
    from hybrid_storage.registry import StorageRegistry
    from hybrid_storage.checkpoint_publication import LocalCheckpointPublisher
    p=config['paths']
    LocalCheckpointPublisher(StorageRegistry(db,ArtifactRegistrar(Path(p['objects']),staging_root=Path(p['scratch']))),Path(p['checkpoints'])).initialize()
    if db.execute('PRAGMA integrity_check').fetchone()[0]!='ok' or list(db.execute('PRAGMA foreign_key_check')):raise ValueError('Pilot DB validation failed')
    db.close();print('INITIALIZED_NEW_LOCAL_PILOT_DB_ONLY')
