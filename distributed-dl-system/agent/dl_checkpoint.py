"""Cooperative single-process PyTorch checkpoints for submitted training scripts."""
import json
import os
from pathlib import Path
import random
import tempfile
import time
import zipfile

from checkpoint_protocol import sha256, validate


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
    def __init__(self):
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
            with tempfile.NamedTemporaryFile(prefix='.sdk-archive-',dir=self.directory,delete=False) as stream:
                archive_tmp=Path(stream.name)
                with zipfile.ZipFile(stream,'w',compression=zipfile.ZIP_STORED) as archive:
                    archive.writestr('metadata.json',json.dumps(meta,allow_nan=False))
                    archive.write(state_tmp,'state.pt')
                stream.flush();os.fsync(stream.fileno())
            validate(archive_tmp,self.job_id,self.manifest,self.attempt)
            # Never expose a half-written checkpoint, even when the process is killed.
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
