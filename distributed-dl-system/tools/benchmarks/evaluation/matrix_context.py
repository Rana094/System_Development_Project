"""Private component context: real registry, signed inventory, cache and workspaces.

Logical workers share this host. No production DB/bootstrap, HTTP or SSH.
"""
from contextlib import ExitStack
from dataclasses import asdict
import importlib.util
import hashlib
import io
import json
import os
from pathlib import Path
import secrets
import sqlite3
import subprocess
import sys

from . import runner
from hybrid_storage import registry_schema,cache_registry_schema
from hybrid_storage.artifacts import ArtifactRegistrar
from hybrid_storage.cache_inventory import CacheInventory
from hybrid_storage.cache_coverage import CoverageAPI
from hybrid_storage.cache_fetch import CacheFetcher
from hybrid_storage.dataset_manifests import DatasetEntry
from hybrid_storage.input_manifests import SourceFile,SourceManifest,encoded
from hybrid_storage.inputs import InputFile,LocalObjectStorageBackend,object_key
from hybrid_storage.inventory_protocol import Credential
from hybrid_storage.inventory_reporter import InventoryReporter
from hybrid_storage.locality import JobLocality
from hybrid_storage.registry import StorageRegistry
from hybrid_storage.worker_cache import WorkerCache
from hybrid_storage.workspaces import AttemptIdentity,WorkspacePlan,WorkspaceManager

# Only this explicitly selected rehearsal helper substitutes kernel mount facts.
# Its records are never eligible to be labeled actual NFS measurements.
LOCAL_COPY='''
import json,sys
from unittest.mock import patch
sys.path.insert(0,sys.argv[1])
from hybrid_storage.cache_copy_worker import copy_request
with patch('hybrid_storage.inputs.require_input_mount',return_value={'mount_id':42}),patch('hybrid_storage.inputs.fd_mount_id',return_value=42):
 print(json.dumps(copy_request(**json.loads(sys.argv[2]))),flush=True)
'''


class SnapshotFetcher(CacheFetcher):
    def _spawn(self,request,output,lock):
        return subprocess.Popen([sys.executable,'-I','-B',str(self.library_root/'hybrid_storage/cache_copy_worker.py'),json.dumps(request)],
            stdin=subprocess.DEVNULL,stdout=output,stderr=subprocess.DEVNULL,
            pass_fds=(request['output_fd'],lock),start_new_session=True)


class RehearsalFetcher(SnapshotFetcher):
    def _spawn(self,request,output,lock):
        return subprocess.Popen([sys.executable,'-I','-B','-c',LOCAL_COPY,str(self.library_root),json.dumps(request)],
            stdin=subprocess.DEVNULL,stdout=output,stderr=subprocess.DEVNULL,
            pass_fds=(request['output_fd'],lock),start_new_session=True)


class Context:
    def __init__(self,root,source_root,*,workers=3,rehearsal=True,nfs_source=None,library_root=None):
        self.root=Path(root);self.root.mkdir(mode=0o700);self.stack=ExitStack()
        self.ids=tuple(f'slot-{i:02}' for i in range(workers));self.source_root=source_root
        self.rehearsal=rehearsal;self.nfs_source=nfs_source or '192.0.2.10:/srv/ml-shared/objects'
        self.library_root=Path(library_root or runner.ROOT)
        self.db_path=self.root/'registry.sqlite3';self.db=self.connect();self.stack.callback(self.db.close)
        with self.db:
            self.db.execute('CREATE TABLE jobs(job_id TEXT PRIMARY KEY,owner TEXT,status TEXT,job_type TEXT,job_config TEXT,storage_manifest_sha256 TEXT)')
            self.db.execute('CREATE TABLE job_storage_bindings(job_id TEXT PRIMARY KEY,backend TEXT)')
        registry_schema.migrate(self.db);cache_registry_schema.migrate(self.db)
        for name in ('objects','scratch'):(self.root/name).mkdir(mode=0o700)
        self.registry=StorageRegistry(self.db,ArtifactRegistrar(self.root/'objects',staging_root=self.root/'scratch'))
        self.credentials=tuple(Credential(w,secrets.token_hex(16),secrets.token_bytes(32)) for w in self.ids)
        self.inventory=CacheInventory(self.connect,self.credentials,configured_workers=self.ids,state_root=self.root)
        self.inventory.start();self.stack.callback(self.inventory.close)
        self.api=CoverageAPI(self.registry,self.inventory);self.workers={}

    def connect(self):
        db=sqlite3.connect(self.db_path);db.execute('PRAGMA foreign_keys=ON');db.execute('PRAGMA synchronous=FULL');return db

    def close(self):self.stack.close()

    def register(self,source,dataset,world):
        actor='evaluation';files=[]
        for name in ('requirements.txt','train.py'):
            raw=(source/name).read_bytes();artifact=self.registry.register_artifact(actor,io.BytesIO(raw),expected_size=len(raw))
            files.append(SourceFile(name,artifact.sha256,artifact.size_bytes,False))
        tree=SourceManifest(tuple(sorted(files,key=lambda f:f.path)),())
        self.registry.register_source(actor,tree)
        raw=Path(dataset).read_bytes();artifact=self.registry.register_artifact(actor,io.BytesIO(raw),expected_size=len(raw))
        data=self.registry.create_dataset(actor,(DatasetEntry('dataset.csv','file',artifact.sha256,artifact.size_bytes),))
        job='component-job';spec=dict(execution=dict(mode='distributed' if world>1 else 'single',workers=world),environment=dict(requirements='requirements.txt'))
        legacy=dict(storage_version=1,job_id=job,owner=actor,execution_spec=spec,
                    source=dict(inventory=tree.legacy_inventory()),data=dict(kind='file',inventory=dict(directories=[],files=[dict(path='dataset.csv',sha256=artifact.sha256,size=artifact.size_bytes)])))
        raw=encoded(legacy)
        with self.db:
            self.db.execute('INSERT INTO jobs VALUES(?,?,?,?,?,?)',(job,actor,'HELD','custom',json.dumps(spec),hashlib.sha256(raw).hexdigest()))
            self.db.execute('INSERT INTO job_storage_bindings VALUES(?,?)',(job,'nfs'))
        binding=self.registry.bind_job(actor,job,legacy_manifest_bytes=raw,source_sha256=tree.sha256,dataset_id=data.dataset_id,dataset_version=data.version)
        self.job,self.binding,self.tree,self.data=job,binding,tree,data
        self.template=WorkspacePlan(AttemptIdentity(job,job+':0'),binding.sha256,tree,data)
        return self.template

    def worker(self,worker):
        if worker in self.workers:return self.workers[worker]
        index=self.ids.index(worker);self.inventory.enroll(worker)
        root=self.root/worker;root.mkdir(mode=0o700)
        cache_root=root/'cache';cache_root.mkdir(mode=0o700)
        mount=Path(self.source_root or self.root/'objects')
        cache=WorkerCache(cache_root,input_mount=mount)
        fetcher=(RehearsalFetcher if self.rehearsal else SnapshotFetcher)(cache,source=self.nfs_source,
            quota_bytes=2*1024**3,reserve_free_bytes=64*1024**2,io_timeout=30)
        fetcher.library_root=self.library_root
        fetcher.initialize();self.stack.callback(fetcher.close)
        work_root=root/'workspaces';work_root.mkdir(mode=0o700)
        work=WorkspaceManager(cache,work_root,max_input_bytes=2*1024**3);work.initialize()
        reporter=InventoryReporter(cache,self.credentials[index]);reporter.initialize();self.stack.callback(reporter.close)
        self.workers[worker]=(cache,fetcher,work,reporter)
        return self.workers[worker]

    def refresh(self):
        for worker in self.ids:
            cache,fetcher,work,reporter=self.worker(worker)
            # Full signed reconciliation before each measured selection.
            # Request-bound HMAC and real verified local cache bytes, not fake inventory.
            reporter.request_full();request,headers=self.inventory.challenge(worker)
            body,signature=reporter.reply(request,headers['X-DL-Inventory-MAC'],headers['X-DL-Worker-Key'])
            self.inventory.accept(worker,request,body,signature)

    def choose(self,policy,world,*,refresh=True,observations=None):
        if refresh:self.refresh()
        # Fixed equal resource profile is a declared experiment control, NOT Pi
        # telemetry or controller eligibility. Use unchanged resource tie rules.
        preference=JobLocality(self.api,'evaluation',self.job,self.binding.sha256) if policy=='locality' else None
        sys.path.insert(0,str(runner.ROOT/'controller'))
        if world==1:
            spec=importlib.util.spec_from_file_location('evaluation_resource_selector',runner.ROOT/'controller/scheduler.py')
            module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
            module.load_nodes=lambda:[dict(id=w) for w in self.ids]
            module.get_resources=lambda _:dict(node_state='IDLE',cpu=dict(usage_percent=5,available_cores=4),memory=dict(available_gb=4),gpu=dict(available=0),temperature_c=40)
            from contextlib import redirect_stdout
            with redirect_stdout(io.StringIO()):choice=module.select_best_node(requirements=dict(cpu_cores=1,memory_gb=1,gpus=0),locality=preference)
            selected=[choice['node']['id']]
        else:
            # Already-compatible fixed resource candidates. Reuse the production
            # group locality tie-breaker, not the live reservation/lease launcher.
            candidates=[(5,-4,w,dict(id=w)) for w in self.ids];baseline=sorted(candidates)[:world]
            chosen=baseline if preference is None else preference.group(self.job,{'fixed-compatible':candidates},world,baseline)
            selected=[v[2] for v in chosen]
        coverage=observations if observations is not None else self.observe(world)
        return selected,dict(policy=policy,selected=selected,coverage=coverage,
            resource_profile='fixed_equal_local_component_control_not_live_telemetry',
            locality=None if preference is None else preference.snapshot().document())

    def observe(self,world):
        return [self.api.for_job('evaluation',w,self.job,rank=0 if world>1 else None).document() for w in self.ids]

    def stage(self,selected,attempt):
        receipts=[];plans=[];hits=transferred=0
        for rank,worker in enumerate(selected):
            cache,fetcher,manager,_=self.worker(worker)
            plan=WorkspacePlan(AttemptIdentity(self.job,self.job+':'+str(attempt),rank),self.binding.sha256,self.tree,self.data)
            exact=plan.inputs()
            results=fetcher.ensure(exact,authorize=lambda files:files==exact)
            hits+=sum(v.size_bytes for v in results if v.cache_hit)
            transferred+=sum(v.size_bytes for v in results if not v.cache_hit)
            receipt=manager.prepare(plan,authorize=lambda p:p==plan)
            receipts.append(dict(script=str(receipt.source/'train.py'),dataset=str(receipt.data/'dataset.csv')));plans.append((manager,plan))
        return receipts,plans,dict(cache_hit_bytes=hits,transferred_bytes=transferred)
