"""Cooperative PyTorch checkpoint saving for single workers and coordinated DDP groups."""
import json
import os
from pathlib import Path
import random
import tempfile
import time
import zipfile
import io
import hashlib
import uuid

from checkpoint_protocol import sha256, validate, validate_group, MAX_STATE_BYTES
from execution_lease import sdk_fence


def atomic_json(path, value):
    with tempfile.NamedTemporaryFile(mode='w',dir=path.parent,delete=False) as stream:
        temporary=Path(stream.name)
        try:
            json.dump(value,stream,allow_nan=False)
            stream.flush();os.fsync(stream.fileno())
            os.replace(temporary,path)
        finally:
            temporary.unlink(missing_ok=True)


def safe_state(value):
    """Keep custom state tensor-safe; arbitrary pickled Python objects are unsupported."""
    import torch
    if value is None or type(value) in (str,int,float,bool) or isinstance(value,torch.Tensor):
        return
    if isinstance(value,dict):
        for key,item in value.items():
            if type(key) not in (str,int): raise ValueError('Checkpoint dictionary keys must be strings or integers')
            safe_state(item)
        return
    if isinstance(value,(list,tuple)):
        for item in value: safe_state(item)
        return
    raise ValueError('Checkpoint state supports tensors and builtin values, not '+type(value).__name__)


class CheckpointManager:
    """Save after completed optimizer updates; progress describes the next work to run."""
    def __init__(self, *, distributed=False):
        if int(os.environ.get('WORLD_SIZE','1')) > 1 and not distributed:
            raise ValueError('DDP jobs must use DistributedCheckpointManager on every rank')
        self.job_id=os.environ['DL_JOB_ID']
        self.manifest=os.environ['DL_MANIFEST_SHA256']
        self.attempt=os.environ['DL_ATTEMPT_TOKEN']
        self.compatibility=os.environ['DL_COMPATIBILITY_KEY']
        self.directory=Path(os.environ['DL_CHECKPOINT_DIR'])
        self.directory.mkdir(parents=True,exist_ok=True)
        self.path=self.directory/'latest.dlckpt'
        self.interval=float(os.environ['DL_CHECKPOINT_INTERVAL_SECONDS'])
        self.last_save=0
        # Interrupted atomic saves leave only private temporary files, never latest.
        for pattern in ('.sdk-state-*','.sdk-archive-*'):
            for stale in self.directory.glob(pattern): stale.unlink()

    def save(self, model, optimizer, *, global_step, progress=None, scheduler=None,
             extra_state=None, completed=False, force=False):
        import torch
        if type(global_step) is not int or global_step<0 or type(completed) is not bool:
            raise ValueError('global_step must be nonnegative and completed must be boolean')
        progress={} if progress is None else progress
        if extra_state is not None and not isinstance(extra_state,dict): raise ValueError('extra_state must be a dictionary')
        if not isinstance(progress,dict): raise ValueError('progress must be a JSON object')
        json.dumps(progress,allow_nan=False)
        if not (force or completed) and time.monotonic()-self.last_save<self.interval:
            return False
        producer=sdk_fence()
        # Capture all states at a single safe boundary in the application's loop.
        if torch.cuda.is_available(): torch.cuda.synchronize()
        numpy_rng=None
        try:
            import numpy as np
            state=np.random.get_state()
            numpy_rng={'name':state[0],'keys':state[1].tolist(),'position':int(state[2]),
                       'has_gauss':int(state[3]),'cached_gauss':float(state[4])}
        except ImportError:
            pass
        state={'global_step':global_step,'progress':progress,'model':model.state_dict(),
               'optimizer':optimizer.state_dict(),'scheduler':scheduler.state_dict() if scheduler else None,
               'torch_rng':torch.get_rng_state(),'cuda_rng':torch.cuda.get_rng_state_all() if torch.cuda.is_available() else [],
               'python_rng':random.getstate(),'numpy_rng':numpy_rng,'extra_state':extra_state or {}}
        safe_state(state)
        state_tmp=archive_tmp=None
        try:
            with tempfile.NamedTemporaryFile(prefix='.sdk-state-',dir=self.directory,delete=False) as stream:
                state_tmp=Path(stream.name)
                torch.save(state,stream)
                stream.flush();os.fsync(stream.fileno())
            meta={'format_version':1,'job_id':self.job_id,'manifest_sha256':self.manifest,
                  'attempt_token':self.attempt,'compatibility_key':self.compatibility,'global_step':global_step,'completed':completed,
                  'state_sha256':sha256(state_tmp),'created_at':time.time()}
            if producer:meta['lease_fence']=producer
            with tempfile.NamedTemporaryFile(prefix='.sdk-archive-',dir=self.directory,delete=False) as stream:
                archive_tmp=Path(stream.name)
                with zipfile.ZipFile(stream,'w',compression=zipfile.ZIP_STORED) as archive:
                    archive.writestr('metadata.json',json.dumps(meta,allow_nan=False))
                    archive.write(state_tmp,'state.pt')
                stream.flush();os.fsync(stream.fileno())
            validate(archive_tmp,self.job_id,self.manifest,self.attempt)
            # Never expose a half-written checkpoint, even when the process is killed.
            if sdk_fence()!=producer:raise ValueError('Execution ownership changed during checkpoint saving')
            os.replace(archive_tmp,self.path)
            descriptor=os.open(self.directory,os.O_RDONLY|os.O_DIRECTORY)
            try: os.fsync(descriptor)
            finally: os.close(descriptor)
            self.last_save=time.monotonic()
            return True
        finally:
            for path in (state_tmp,archive_tmp):
                if path: path.unlink(missing_ok=True)

    def load(self, model, optimizer, *, scheduler=None):
        import torch
        resume=os.environ.get('DL_RESUME_CHECKPOINT')
        if not resume:
            return None
        meta=validate(resume,self.job_id,self.manifest)
        if meta['compatibility_key']!=self.compatibility:
            raise ValueError('Resume environment is not compatible with the saved checkpoint')
        if meta['sha256']!=os.environ['DL_RESUME_SHA256']:
            raise ValueError('Resume bytes differ from the controller checkpoint')
        with zipfile.ZipFile(resume) as archive, archive.open('state.pt') as stream:
            state=torch.load(stream,map_location='cpu',weights_only=True)
        if state['global_step']!=meta['global_step']:
            raise ValueError('Checkpoint tensor state and progress header disagree')
        if (state['scheduler'] is None)!=(scheduler is None):
            raise ValueError('Restore must supply the same learning-rate scheduler')
        model.load_state_dict(state['model'])
        optimizer.load_state_dict(state['optimizer'])
        if scheduler: scheduler.load_state_dict(state['scheduler'])
        # Restore RNG after constructors/loading, which may themselves consume randomness.
        torch.set_rng_state(state['torch_rng'])
        if state['cuda_rng']:
            if not torch.cuda.is_available() or len(state['cuda_rng'])!=torch.cuda.device_count():
                raise ValueError('CUDA RNG device count differs from the saved checkpoint')
            torch.cuda.set_rng_state_all(state['cuda_rng'])
        random.setstate(state['python_rng'])
        if state['numpy_rng'] is not None:
            import numpy as np
            rng=state['numpy_rng']
            np.random.set_state((rng['name'],np.array(rng['keys'],dtype=np.uint32),rng['position'],rng['has_gauss'],rng['cached_gauss']))
        info={'job_id':self.job_id,'attempt_token':self.attempt,'pid':int(os.environ['DL_LAUNCHER_PID']),
              'trainer_pid':os.getpid(),'confirmed_at':time.time(),'model_restored':True,
              'optimizer_restored':True,'scheduler_restored':scheduler is not None,'rng_restored':True,
              'restored_global_step':meta['global_step'],'checkpoint_sha256':meta['sha256']}
        atomic_json(Path(os.environ['DL_RESUME_STATUS_PATH']),info)
        return {'global_step':state['global_step'],'progress':state['progress'],'extra_state':state['extra_state']}


def cpu_tree(value):
    """Detach rank snapshots and normalize tensor storage before shared-state hashing."""
    import torch
    if isinstance(value,torch.Tensor):
        return value.detach().cpu().contiguous().clone()
    if isinstance(value,dict):
        return {key:cpu_tree(value[key]) for key in sorted(value,key=lambda k:(type(k).__name__,str(k)))}
    if isinstance(value,tuple):
        return tuple(cpu_tree(item) for item in value)
    if isinstance(value,list):
        return [cpu_tree(item) for item in value]
    return value


def tensor_bytes(value):
    import torch
    buffer = io.BytesIO()
    torch.save(value,buffer)
    return buffer.getvalue()


class DistributedCheckpointManager(CheckpointManager):
    """Collect rank snapshots and publish only after every rank acknowledges one cut."""
    def __init__(self):
        import torch.distributed as dist
        if not dist.is_initialized() or dist.get_world_size() < 2 or dist.get_backend() != 'gloo':
            raise ValueError('Initialize a CPU Gloo DDP group before creating its checkpoint manager')
        # PyTorch object collectives convert their transport tensors through NumPy.
        # Require it up front, rather than losing a peer halfway through a save.
        try:
            import numpy
        except ImportError as error:
            raise ValueError('DDP checkpoint saving requires numpy in the submitted requirements') from error
        super().__init__(distributed=True)
        self.rank,self.world_size = dist.get_rank(),dist.get_world_size()
        self.group_id = os.environ['DL_DDP_GROUP_ID']
        if self.rank != int(os.environ['RANK']) or self.world_size != int(os.environ['WORLD_SIZE']):
            raise ValueError('Checkpoint rank differs from the launcher group')
        if self.rank == 0:
            for path in self.directory.glob('.ddp-stage-*'):
                path.unlink()

    def _capture(self, model, optimizer, scheduler, step, progress, extra_state):
        import torch
        import random
        if type(step) is not int or step < 0 or not isinstance(progress,dict):
            raise ValueError('Invalid group progress')
        json.dumps(progress,allow_nan=False)
        if not isinstance(extra_state,dict):
            raise ValueError('extra_state must be a dictionary')
        underlying = model.module if hasattr(model,'module') else model
        numpy_rng = None
        try:
            import numpy as np
            rng = np.random.get_state()
            numpy_rng = {'name':rng[0],'keys':rng[1].tolist(),'position':int(rng[2]),
                         'has_gauss':int(rng[3]),'cached_gauss':float(rng[4])}
        except ImportError:
            pass
        state = cpu_tree({'rank':self.rank,'world_size':self.world_size,
                          'global_step':step,'progress':progress,
                          'model':underlying.state_dict(),'optimizer':optimizer.state_dict(),
                          'scheduler':scheduler.state_dict() if scheduler else None,
                          'torch_rng':torch.get_rng_state(),'python_rng':random.getstate(),
                          'numpy_rng':numpy_rng,'extra_state':extra_state})
        safe_state(state)
        # DDP parameters and optimizer/scheduler must agree. Model buffers and
        # RNG are rank-local and remain in each rank's complete snapshot.
        shared = cpu_tree({'parameters':dict(underlying.named_parameters()),
                           'optimizer':state['optimizer'],'scheduler':state['scheduler']})
        payload = tensor_bytes(state)
        if len(payload) > MAX_STATE_BYTES:
            raise ValueError('Rank checkpoint is oversized')
        return payload,hashlib.sha256(tensor_bytes(shared)).hexdigest()

    def _prepare(self, snapshots):
        identity = {key:snapshots[0][key] for key in
                    ('job_id','manifest_sha256','attempt_token','compatibility_key','group_id',
                     'world_size','global_step','completed','shared_state_sha256')}
        if any(snapshot.get('error') for snapshot in snapshots):
            raise ValueError('Rank snapshot failed: '+str([s.get('error') for s in snapshots]))
        if (sorted(s['rank'] for s in snapshots) != list(range(self.world_size))
                or any(any(snapshot[key] != value for key,value in identity.items()) for snapshot in snapshots)):
            raise ValueError('Ranks disagree about checkpoint boundary or shared training state')
        if sum(len(s['state']) for s in snapshots) > MAX_STATE_BYTES:
            raise ValueError('Group checkpoint is oversized')
        checkpoint_id = uuid.uuid4().hex
        hashes = {str(s['rank']):hashlib.sha256(s['state']).hexdigest() for s in snapshots}
        acknowledgments = [{'rank':rank,'checkpoint_id':checkpoint_id,
                            'state_sha256':hashes[str(rank)],'accepted':True} for rank in range(self.world_size)]
        meta = {**identity,'format_version':2,'kind':'ddp','checkpoint_id':checkpoint_id,
                'created_at':time.time(),'rank_state_sha256':hashes,
                'rank_acknowledgments':acknowledgments}
        producers={str(s['rank']):s.get('lease_fence') for s in snapshots}
        if any(producers.values()):
            if not all(producers.values()):raise ValueError('Only some checkpoint ranks have execution fences')
            meta['rank_fences']=producers
        with tempfile.NamedTemporaryFile(prefix='.ddp-stage-',dir=self.directory,delete=False) as stream:
            path = Path(stream.name)
            try:
                with zipfile.ZipFile(stream,'w',compression=zipfile.ZIP_STORED) as archive:
                    archive.writestr('metadata.json',json.dumps(meta,allow_nan=False))
                    for snapshot in snapshots:
                        archive.writestr(f"rank-{snapshot['rank']}.pt",snapshot['state'])
                stream.flush();os.fsync(stream.fileno())
                validate_group(path,self.job_id,self.manifest,self.attempt)
            except Exception:
                path.unlink(missing_ok=True)
                raise
        return path,meta

    def _acknowledge(self, proposal, state_sha256):
        return {'rank':self.rank,'checkpoint_id':proposal['checkpoint_id'],
                'state_sha256':state_sha256,
                'accepted':proposal['rank_state_sha256'].get(str(self.rank)) == state_sha256
                           and proposal['group_id'] == self.group_id}

    def save(self, model, optimizer, *, global_step, progress=None, scheduler=None,
             extra_state=None, completed=False, force=False):
        import torch.distributed as dist
        decision = [bool(force or completed or time.monotonic()-self.last_save >= self.interval)
                    if self.rank == 0 else None]
        dist.broadcast_object_list(decision,src=0)
        if not decision[0]:
            return False
        snapshot = {'rank':self.rank,'world_size':self.world_size,'job_id':self.job_id,
                    'manifest_sha256':self.manifest,'attempt_token':self.attempt,
                    'compatibility_key':self.compatibility,'group_id':self.group_id,
                    'global_step':global_step,'completed':completed}
        try:
            if type(completed) is not bool or type(force) is not bool:
                raise ValueError('Completion and force flags must be boolean')
            state,shared_hash = self._capture(model,optimizer,scheduler,global_step,
                                              {} if progress is None else progress,
                                              {} if extra_state is None else extra_state)
            snapshot.update(state=state,shared_state_sha256=shared_hash,lease_fence=sdk_fence())
        except Exception as error:
            snapshot.update(error=str(error),state=b'',shared_state_sha256='')
        snapshots = [None]*self.world_size if self.rank == 0 else None
        dist.gather_object(snapshot,snapshots,dst=0)
        staged = None
        proposal = [None]
        try:
            if self.rank == 0:
                try:
                    staged,proposal[0] = self._prepare(snapshots)
                except Exception as error:
                    proposal[0] = {'error':str(error)}
            dist.broadcast_object_list(proposal,src=0)
            if proposal[0].get('error'):
                raise ValueError(proposal[0]['error'])
            ack = self._acknowledge(proposal[0],hashlib.sha256(snapshot['state']).hexdigest())
            try:
                if sdk_fence()!=snapshot.get('lease_fence'):ack['accepted']=False
            except (ValueError,OSError,KeyError):ack['accepted']=False
            acknowledgments = [None]*self.world_size
            dist.all_gather_object(acknowledgments,ack)
            commit = [None]
            if self.rank == 0:
                try:
                    if acknowledgments != proposal[0]['rank_acknowledgments']:
                        raise ValueError('A rank did not acknowledge the prepared group checkpoint')
                    # The download endpoint sees only latest.dlckpt. Partial
                    # state files remain private until all acknowledgments agree.
                    if sdk_fence()!=snapshot.get('lease_fence'):raise ValueError('Checkpoint producer lost ownership')
                    os.replace(staged,self.path)
                    descriptor = os.open(self.directory,os.O_RDONLY|os.O_DIRECTORY)
                    try: os.fsync(descriptor)
                    finally: os.close(descriptor)
                    commit[0] = {'checkpoint_id':proposal[0]['checkpoint_id']}
                except Exception as error:
                    commit[0] = {'error':str(error)}
            dist.broadcast_object_list(commit,src=0)
            if commit[0].get('error'):
                raise ValueError(commit[0]['error'])
            self.last_save = time.monotonic()
            print(f"DDP_CHECKPOINT_COMMITTED rank={self.rank} step={global_step} id={commit[0]['checkpoint_id']}",flush=True)
            return True
        finally:
            if staged is not None:
                staged.unlink(missing_ok=True)

    def load(self, model, optimizer, *, scheduler=None, restore_extra=None):
        """Restore each logical rank, then acknowledge one common source checkpoint."""
        import torch
        import torch.distributed as dist
        resume = os.environ.get('DL_RESUME_CHECKPOINT')
        report = {'rank':self.rank,'resume':bool(resume)}
        state = meta = None
        try:
            if resume:
                meta = validate_group(resume,self.job_id,self.manifest)
                if (meta['compatibility_key'] != self.compatibility
                        or meta['world_size'] != self.world_size
                        or meta['sha256'] != os.environ['DL_RESUME_SHA256']):
                    raise ValueError('Resume identity, environment, worker count or bytes differ')
                if int(meta['attempt_token'].rsplit(':',1)[1]) >= int(self.attempt.rsplit(':',1)[1]):
                    raise ValueError('Resume must come from an earlier attempt')
                with zipfile.ZipFile(resume) as archive, archive.open(f'rank-{self.rank}.pt') as stream:
                    state = torch.load(stream,map_location='cpu',weights_only=True)
                if (state['rank'] != self.rank or state['world_size'] != self.world_size
                        or state['global_step'] != meta['global_step']):
                    raise ValueError('Rank tensor state differs from checkpoint metadata')
                if (state['scheduler'] is None) != (scheduler is None):
                    raise ValueError('Restore must supply the saved learning-rate scheduler')
                underlying = model.module if hasattr(model,'module') else model
                underlying.load_state_dict(state['model'])
                optimizer.load_state_dict(state['optimizer'])
                if scheduler is not None:
                    scheduler.load_state_dict(state['scheduler'])
                if state['extra_state'] and restore_extra is None:
                    raise ValueError('Supply restore_extra to restore application checkpoint state')
                if restore_extra is not None:
                    restore_extra(state['extra_state'],state['progress'])
                # Constructors and application callbacks can consume randomness.
                # Restore RNG last, before any resumed forward pass.
                torch.set_rng_state(state['torch_rng'])
                random.setstate(state['python_rng'])
                if state['numpy_rng'] is not None:
                    import numpy as np
                    rng = state['numpy_rng']
                    np.random.set_state((rng['name'],np.array(rng['keys'],dtype=np.uint32),
                                         rng['position'],rng['has_gauss'],rng['cached_gauss']))
                _,shared_hash = self._capture(model,optimizer,scheduler,state['global_step'],
                                               state['progress'],state['extra_state'])
                if shared_hash != meta['shared_state_sha256']:
                    raise ValueError('Restored shared training state differs from saved state')
                report.update(checkpoint_sha256=meta['sha256'],checkpoint_id=meta['checkpoint_id'],
                              restored_global_step=meta['global_step'],source_attempt=meta['attempt_token'],
                              shared_state_sha256=shared_hash)
        except Exception as error:
            report['error'] = str(error)
        reports = [None]*self.world_size
        dist.all_gather_object(reports,report)
        if any(item.get('error') for item in reports):
            raise ValueError('Group restore failed: '+str([item.get('error') for item in reports]))
        if not any(item['resume'] for item in reports):
            return None
        if (not all(item['resume'] for item in reports)
                or [item['rank'] for item in reports] != list(range(self.world_size))
                or any({key:value for key,value in item.items() if key!='rank'} !=
                       {key:value for key,value in report.items() if key!='rank'} for item in reports)):
            raise ValueError('Ranks restored different checkpoints or training boundaries')
        info = {**report,'job_id':self.job_id,'attempt_token':self.attempt,
                'pid':int(os.environ['DL_LAUNCHER_PID']),'trainer_pid':os.getpid(),
                'confirmed_at':time.time(),'world_size':self.world_size,'group_id':self.group_id,
                'model_restored':True,'optimizer_restored':True,
                'scheduler_restored':scheduler is not None,'rng_restored':True,
                'application_state_restored':True,'all_ranks_restored':True}
        ack_path = Path(os.environ['DL_RESUME_STATUS_PATH'])
        written = {'rank':self.rank}
        try:
            atomic_json(ack_path,info)
        except Exception as error:
            written['error'] = str(error)
        acknowledgments = [None]*self.world_size
        dist.all_gather_object(acknowledgments,written)
        if any(item.get('error') for item in acknowledgments):
            ack_path.unlink(missing_ok=True)
            raise ValueError('Group restore acknowledgment failed: '+str(acknowledgments))
        print(f"DDP_RESTORED rank={self.rank} step={meta['global_step']} id={meta['checkpoint_id']}",flush=True)
        return {'global_step':state['global_step'],'progress':state['progress'],
                'extra_state':state['extra_state']}
