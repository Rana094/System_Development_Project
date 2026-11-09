"""Explicit trusted immutable publication, not a recovery launcher or activation.

Controller-local private store and SQLite only. No trainer filesystem privileges,
legacy latest replacement, automatic migration, retention or incomplete salvage.
"""
from contextlib import contextmanager
from dataclasses import dataclass
import json
import os
from pathlib import Path
import time
from typing import BinaryIO, Protocol

from . import checkpoint_schema as schema, registry_schema as base
from .artifacts import _rename_noreplace, _verify
from .backends import _root
from .checkpoint_files import private_root,regular,space,stream_copy,validate_fd
from .input_manifests import encoded,job_identity
from .inputs import object_key
from .inventory_protocol import hex_id,worker_id as valid_worker
from .registry import StorageRegistry
from .staging_lifecycle import require_running,_fence,_placements,_boot,_clock
from .worker_cache import _child,_protected


@dataclass(frozen=True)
class CommittedCheckpoint:
    publication_id: str
    job_id: str
    retry_count: int
    binding_sha256: str
    sha256: str
    size_bytes: int
    kind: str
    global_step: int
    completed: bool


class CheckpointPublisher(Protocol):
    def publish(self,actor:str,job_id:str,worker_id:str,source:BinaryIO,*,
                publication_id:str,expected_head:str|None,expected_sha256:str,
                expected_size:int)->CommittedCheckpoint: ...


def _row(cursor):
    row=cursor.fetchone()
    return None if row is None else dict(zip((item[0] for item in cursor.description),row))


class LocalCheckpointPublisher:
    """Caller is a trusted authenticated collector, never a student route.

    A publication_id is a server-generated idempotency key bound to ONE attempt
    and exact bytes. Rejected/interrupted IDs cannot be reused or auto-promoted.
    A new transfer can use a fresh key; only COMMITTED rows are recoverable.
    """
    def __init__(self,registry:StorageRegistry,root:Path,*,max_bytes=10*1024**3+16*1024**2,
                 reserve_free_bytes=256*1024**2):
        if not isinstance(registry,StorageRegistry):raise TypeError('Explicit trusted registry required')
        self.registry,self.db,self.root=registry,registry.db,_root(root)
        for other in (registry.publisher.root,registry.publisher.staging_root):
            if self.root.is_relative_to(other) or other.is_relative_to(self.root):
                raise ValueError('Private checkpoints must not overlap exported input objects/scratch')
        if (type(max_bytes) is not int or not 0<max_bytes<=2**63-1
                or type(reserve_free_bytes) is not int or not 0<=reserve_free_bytes<=2**63-1):
            raise ValueError('Bounded checkpoint size and free-space reserve required')
        self.max_bytes,self.reserve=max_bytes,reserve_free_bytes

    @contextmanager
    def _store(self,*,initialize=False):
        with private_root(self.root) as root:
            with _child(root,'pending',create=initialize) as pending, _child(root,'commits',create=initialize) as commits:
                value=os.fstat(root)
                identity=(str(self.root),value.st_dev,value.st_ino)
                if not initialize:
                    row=self.db.execute('SELECT root,device,inode FROM hybrid_checkpoint_store WHERE singleton=1').fetchone()
                    if row is None or tuple(row)!=identity:raise base.RegistryError('Checkpoint store identity changed')
                yield root,pending,commits,identity

    def initialize(self):
        """Explicit setup in an already provisioned protected LOCAL root."""
        with base.transaction(self.db):
            if not schema.installed(self.db):raise base.RegistryError('Explicit checkpoint migration required')
            with self._store(initialize=True) as (_,__,___,identity):
                row=self.db.execute('SELECT root,device,inode FROM hybrid_checkpoint_store WHERE singleton=1').fetchone()
                if row is None:self.db.execute('INSERT INTO hybrid_checkpoint_store VALUES(1,?,?,?)',identity)
                elif tuple(row)!=identity:raise base.RegistryError('Refuse a different/empty checkpoint store')

    def _authorize(self,actor,job_id,worker):
        if not schema.installed(self.db):raise base.RegistryError('Explicit checkpoint migration required')
        binding=self.registry._job_inputs(actor,job_id)
        if binding is None:raise PermissionError('Immutable owned input binding required')
        job=_row(self.db.execute('SELECT * FROM jobs WHERE job_id=?',(job_id,)))
        spec=json.loads(job['job_config'])
        selected=self.db.execute('SELECT backend FROM job_storage_bindings WHERE job_id=?',(job_id,)).fetchone()
        if (job['job_type']!='custom' or spec['recovery']['mode']!='checkpoint'
                or selected is None or selected[0]!='nfs'):
            raise PermissionError('Explicit hybrid checkpoint attempt required; legacy path stays unchanged')
        attempt=require_running(self.db,job_id)
        ranks=_placements(self.db,job,spec['execution']['workers'],running=True)
        if worker!=ranks[0][1]:raise PermissionError('Only the exact rank-zero producer can publish')
        fences={}
        for rank,node in ranks:
            _,lease=_fence(self.db,job,node,wall=time.time(),boot=_boot(),clock=_clock())
            fences[str(rank)]={key:lease[key] for key in ('node_id','generation','lease_id')}
        return job,binding,attempt,fences

    def _head(self,job_id):
        row=self.db.execute('SELECT publication_id FROM hybrid_checkpoint_heads WHERE job_id=?',(job_id,)).fetchone()
        return None if row is None else row[0]

    def _publication(self,identity):
        return _row(self.db.execute('SELECT * FROM hybrid_checkpoint_publications WHERE publication_id=?',(identity,)))

    @staticmethod
    def _receipt(row):
        return CommittedCheckpoint(**{key:bool(row[key]) if key=='completed' else row[key]
            for key in CommittedCheckpoint.__dataclass_fields__})

    def committed(self,actor,publication_id):
        """Verify an exact committed ID for acknowledgements; no recovery dispatch."""
        hex_id(publication_id)
        with base.transaction(self.db,write=False):
            if not schema.installed(self.db):raise base.RegistryError('Explicit checkpoint migration required')
            row=self._publication(publication_id)
            if row is None or row['owner']!=actor or row['phase']!='COMMITTED':
                raise PermissionError('Owned COMMITTED checkpoint required')
            binding=self.registry._job_inputs(actor,row['job_id'])
            if binding is None or binding.sha256!=row['binding_sha256']:raise PermissionError('Checkpoint input binding differs')
        # Immutable record is now frozen; do not hold even a read transaction
        # over a multi-GiB hash pass and delay unrelated cancellation writers.
        with self._store() as (_,__,commits,___):
            with regular(commits,publication_id+'.dlckpt',private=True) as stream:
                if os.fstat(stream.fileno()).st_mode&0o222:raise PermissionError('Committed snapshot is writable')
                _verify(stream,row['sha256'],row['size_bytes'])
        return self._receipt(row)

    def head(self,actor,job_id):
        job_identity(job_id)
        with base.transaction(self.db,write=False):
            self.registry._job(job_id,actor)
            if not schema.installed(self.db):raise base.RegistryError('Explicit checkpoint migration required')
            identity=self._head(job_id)
        return None if identity is None else self.committed(actor,identity)

    @contextmanager
    def open_committed(self,actor,publication_id):
        """Exact verified source descriptor, never a mutable latest filename."""
        receipt=self.committed(actor,publication_id)
        with self._store() as (_,__,commits,___):
            with regular(commits,publication_id+'.dlckpt',private=True) as stream:
                _verify(stream,receipt.sha256,receipt.size_bytes)
                stream.seek(0)
                yield stream

    def _metadata(self,job,kind,info,fences):
        if type(info.get('format_version')) is not int or info['format_version']!=(2 if kind=='ddp' else 1):
            raise ValueError('Wrong archive family')
        ranks=_placements(self.db,job,json.loads(job['job_config'])['execution']['workers'],running=True)
        prepared={row[0]:json.loads(row[1]) for row in self.db.execute(
            'SELECT node_id,record_json FROM job_environments WHERE job_id=?',(job['job_id'],))}
        if any(prepared.get(node,{}).get('compatibility_key')!=info['compatibility_key'] for _,node in ranks):
            raise ValueError('Checkpoint environment differs from reserved workers')
        if kind=='single':
            if info.get('lease_fence')!=fences['0']:raise PermissionError('Wrong single checkpoint producer fence')
        else:
            from checkpoint_protocol import group_identity
            group=self.db.execute('SELECT context_json FROM distributed_attempts WHERE job_id=? AND retry_count=?',
                                  (job['job_id'],job['retry_count'])).fetchone()
            if (info['world_size']!=len(ranks) or info.get('rank_fences')!=fences or group is None
                    or info['group_id']!=group_identity(job['job_id'],info['attempt_token'],job['storage_manifest_sha256'],
                                                      info['compatibility_key'],json.loads(group[0]))):
                raise PermissionError('Incomplete or differently fenced checkpoint group')

    def publish(self,actor,job_id,worker_id,source,*,publication_id,expected_head,
                expected_sha256,expected_size):
        job_identity(job_id);valid_worker(worker_id);hex_id(publication_id);object_key(expected_sha256)
        if expected_head is not None:hex_id(expected_head)
        if type(expected_size) is not int or not 0<expected_size<=self.max_bytes:
            raise ValueError('Declared checkpoint exceeds configured bound')
        previous=None
        with base.transaction(self.db,write=False):
            self.registry._job(job_id,actor)
            if not schema.installed(self.db):raise base.RegistryError('Explicit checkpoint migration required')
            existing=self._publication(publication_id)
            if existing is not None:
                if any(existing[key]!=value for key,value in dict(job_id=job_id,owner=actor,worker_id=worker_id,
                        expected_head=expected_head,sha256=expected_sha256,size_bytes=expected_size).items()):
                    raise base.RegistryConflict('Publication idempotency key identity differs')
                if existing['phase']!='COMMITTED':raise base.RegistryConflict('Interrupted/rejected publication retained; use a fresh key')
            else:
                previous=self._head(job_id)
        if existing is not None:return self.committed(actor,publication_id)
        if previous!=expected_head:raise base.RegistryConflict('Checkpoint head changed')
        if previous is not None:self.committed(actor,previous)
        with base.transaction(self.db):
            job,binding,attempt,fences=self._authorize(actor,job_id,worker_id)
            if self._head(job_id)!=expected_head:raise base.RegistryConflict('Checkpoint head changed')
            kind='ddp' if json.loads(job['job_config'])['execution']['mode']=='distributed' else 'single'
            now=time.time_ns()
            self.db.execute('''INSERT INTO hybrid_checkpoint_publications
                (publication_id,job_id,owner,retry_count,attempt_id,binding_sha256,worker_id,kind,source_fences,
                 expected_head,sha256,size_bytes,phase,created_at_ns,updated_at_ns)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?, 'RECEIVING',?,?)''',
                (publication_id,job_id,actor,job['retry_count'],attempt.attempt_id,binding.sha256,worker_id,kind,
                 encoded(fences).decode(),expected_head,expected_sha256,expected_size,now,now))
        # No DB write lock across network reads, hashing, archive validation or fsync.
        try:
            with self._store() as (_,pending,commits,___):
                with _child(pending,publication_id,create=True) as private:
                    space(private,expected_size,self.reserve)
                    fd=os.open('candidate',os.O_RDWR|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600,dir_fd=private)
                    with os.fdopen(fd,'w+b') as output:
                        _protected(output.fileno(),directory=False)
                        stream_copy(source,output,size=expected_size,sha256=expected_sha256)
                        info=validate_fd(output.fileno(),job_id,job['storage_manifest_sha256'],
                                         f'{job_id}:{job["retry_count"]}',kind)
                        with base.transaction(self.db):
                            current,current_binding,current_attempt,current_fences=self._authorize(actor,job_id,worker_id)
                            if (current_attempt!=attempt or current_binding.sha256!=binding.sha256 or current_fences!=fences):
                                raise PermissionError('Checkpoint producing attempt changed')
                            self._metadata(current,kind,info,fences)
                            self.db.execute("UPDATE hybrid_checkpoint_publications SET phase='VERIFIED',metadata_json=?,global_step=?,completed=?,updated_at_ns=? WHERE publication_id=?",
                                (encoded(info).decode(),info['global_step'],int(info['completed']),time.time_ns(),publication_id))
                        with base.transaction(self.db):
                            self.db.execute("UPDATE hybrid_checkpoint_publications SET phase='PUBLISHING',updated_at_ns=? WHERE publication_id=?",
                                            (time.time_ns(),publication_id))
                        os.fchmod(output.fileno(),0o400);os.fsync(output.fileno())
                        _rename_noreplace(private,'candidate',commits,publication_id+'.dlckpt')
                        os.fsync(commits);os.fsync(private)
                        with base.transaction(self.db):
                            current,current_binding,current_attempt,current_fences=self._authorize(actor,job_id,worker_id)
                            if (current_attempt!=attempt or current_binding.sha256!=binding.sha256 or current_fences!=fences):
                                raise PermissionError('Late checkpoint producer lost ownership')
                            if self._head(job_id)!=expected_head:raise base.RegistryConflict('A different checkpoint won the head')
                            old=None if expected_head is None else self._publication(expected_head)
                            if old and (info['global_step']<old['global_step'] or old['completed']>int(info['completed'])):
                                raise ValueError('Checkpoint progress/completion regressed')
                            self.db.execute("UPDATE hybrid_checkpoint_publications SET phase='COMMITTED',updated_at_ns=? WHERE publication_id=?",
                                            (time.time_ns(),publication_id))
                            if expected_head is None:self.db.execute('INSERT INTO hybrid_checkpoint_heads VALUES(?,?,1)',(job_id,publication_id))
                            else:self.db.execute('UPDATE hybrid_checkpoint_heads SET publication_id=?,revision=revision+1 WHERE job_id=?',(publication_id,job_id))
                            self.db.execute('''UPDATE jobs SET checkpoint_path=?,checkpoint_updated_at=?,checkpoint_size_bytes=?,updated_at=? WHERE job_id=?''',
                                (str(self.root/'commits'/(publication_id+'.dlckpt')),time.time(),expected_size,time.time(),job_id))
                            if self.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='recovery_events'").fetchone():
                                details={key:info[key] for key in ('global_step','sha256','completed','attempt_token')}
                                details['publication_id']=publication_id
                                for key in ('checkpoint_id','world_size','shared_state_sha256'):
                                    if key in info:details[key]=info[key]
                                self.db.execute('INSERT INTO recovery_events(job_id,event_type,node_id,event_time,details_json) VALUES(?,?,?,?,?)',
                                    (job_id,'DDP_CHECKPOINT_COMMITTED' if kind=='ddp' else 'CHECKPOINT_SYNC',worker_id,time.time(),encoded(details).decode()))
            return self.committed(actor,publication_id)
        except Exception:
            # Retain candidates/orphan immutable snapshots for explicit review;
            # they never become head merely because a final-looking file exists.
            with base.transaction(self.db):
                row=self._publication(publication_id)
                if row['phase'] not in ('COMMITTED','REJECTED'):
                    self.db.execute("UPDATE hybrid_checkpoint_publications SET phase='REJECTED',updated_at_ns=? WHERE publication_id=?",(time.time_ns(),publication_id))
            raise
