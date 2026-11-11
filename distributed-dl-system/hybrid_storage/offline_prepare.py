"""Authenticated metadata preparation against an approved offline capsule.

Requirements must match an allowed immutable hash. Never install packages or
evaluate a student's dependency/build scripts in the privileged service.
"""
import hashlib
from pathlib import Path
import re
import threading

from .input_manifests import encoded, job_identity
from .inventory_protocol import fields
from .linux_boundary import protected_json, atomic_json
from .backends import _directory, _root
from .worker_cache import _protected


class OfflinePreparer:
    def __init__(self,worker_root,capsule):
        self.root=_root(Path(worker_root))/'worker-storage'
        self.capsule=dict(capsule)
        fields(self.capsule,'schema_version site_root runtime packages requirements_sha256 profiles')
        if self.capsule['schema_version']!=1:raise ValueError('Unknown offline capsule')
        self.site=_root(Path(self.capsule['site_root']))
        if not self.capsule['profiles'] or not set(self.capsule['profiles'])<={'pytorch-cpu','isolated'}:
            raise ValueError('Unsupported capsule profiles')
        if any(not re.fullmatch('[0-9a-f]{64}',x) for x in self.capsule['requirements_sha256']):raise ValueError('Invalid requirements hash')
        self._lock=threading.Lock()

    def prepare(self,request):
        fields(request,'job_id profile manifest manifest_sha256')
        job_id=job_identity(request['job_id'])
        if not re.fullmatch('[A-Za-z0-9_-]{1,128}',job_id):raise ValueError('Unsupported legacy SDK job ID')
        manifest=request['manifest'];spec=manifest['execution_spec'];profile=request['profile']
        if (profile not in self.capsule['profiles'] or manifest['job_id']!=job_id
                or hashlib.sha256(encoded(manifest)).hexdigest()!=request['manifest_sha256']):
            raise PermissionError('Metadata differs from immutable manifest/profile')
        requirement=spec['environment']['requirements']
        matches=[f for f in manifest['source']['inventory']['files'] if f['path']==requirement]
        if len(matches)!=1 or matches[0]['sha256'] not in self.capsule['requirements_sha256']:
            raise PermissionError('Requirements are not approved for the offline capsule')
        if spec['resources_per_worker']['gpus']!=0:raise ValueError('CPU capsule cannot run GPU submissions')
        requested=spec['environment'].get('python');runtime=self.capsule['runtime']
        if requested and requested!='.'.join(runtime['python'].split('.')[:2]):raise ValueError('Requested Python differs from capsule')
        from environment_worker import read_request, probe, pip_check
        key=hashlib.sha256(encoded(dict(version=1,profile=profile,capsule=self.capsule))).hexdigest()
        environment=self.root/'environments'/key;job=self.root/'jobs'/job_id
        with self._lock:
            with _directory(self.root) as fd:_protected(fd,directory=True)
            job.mkdir(parents=True,exist_ok=True,mode=0o700)
            with _directory(job) as fd:_protected(fd,directory=True)
            old=job/'request.json'
            if old.exists() and protected_json(old)!=request:raise PermissionError('Existing job metadata is immutable')
            environment.parent.mkdir(mode=0o700,exist_ok=True);marker=environment/'ready.json'
            if not environment.exists():
                environment.mkdir(mode=0o755);(environment/'bin').mkdir(mode=0o755)
                (environment/'bin/python').symlink_to('/usr/bin/python3')
                version='.'.join(runtime['python'].split('.')[:2])
                lib=environment/'lib'/('python'+version);lib.mkdir(parents=True,mode=0o755)
                (lib/'site-packages').symlink_to(self.site)
                (environment/'pyvenv.cfg').write_text('home = /usr/bin\ninclude-system-site-packages = false\nversion = '+runtime['python']+'\n')
                for directory in (environment,environment/'bin',environment/'lib',lib):directory.chmod(0o755)
                (environment/'pyvenv.cfg').chmod(0o644)
                actual=probe(environment/'bin/python');pip_check(environment/'bin/python')
                if actual['packages']!=self.capsule['packages'] or actual['prefix']!=str(environment):
                    raise PermissionError('Offline inventory differs from approved capsule')
                value=dict(environment_key=key,runtime=runtime,profile=profile,probe=actual,created_at=0)
                import json
                with marker.open('x') as f:json.dump(value,f)
                marker.chmod(0o444)
            elif not marker.is_file():
                raise PermissionError('Interrupted offline environment requires review; never reset it')
            else:
                import json,os,stat
                fd=os.open(marker,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
                with os.fdopen(fd,'rb') as saved:
                    info=os.fstat(saved.fileno())
                    if (info.st_uid!=os.geteuid() or info.st_mode&0o222 or not stat.S_ISREG(info.st_mode)
                            or info.st_nlink!=1 or info.st_size>1024**2):raise PermissionError('Unsafe offline environment marker')
                    value=json.load(saved)
                if (value.get('environment_key')!=key or value.get('runtime')!=runtime or value.get('profile')!=profile
                        or value.get('probe',{}).get('packages')!=self.capsule['packages']):
                    raise PermissionError('Offline environment marker differs from approved capsule')
            dataset=(None if manifest['data']['kind']=='generated' else
                     str(job/'data'/Path(spec['dataset']).name) if manifest['data']['kind']=='file' else str(job/'data'))
            record=dict(job_id=job_id,owner=manifest['owner'],manifest_sha256=request['manifest_sha256'],
                environment_key=key,profile=profile,python_path=str(environment/'bin/python'),
                source_path=str(job/'source'),dataset_path=dataset,workspace_path=str(job),
                source_version=manifest['source']['version'],dataset_version=manifest['data']['version'],
                runtime=runtime,packages=self.capsule['packages'])
            compatibility=dict(python='.'.join(runtime['python'].split('.')[:2]),machine=runtime['machine'],
                system=runtime['system'],profile=profile,
                packages=[p for p in record['packages'] if p[0] not in ('pip','setuptools','wheel')])
            record['compatibility_key']=hashlib.sha256(encoded(compatibility)).hexdigest()
            if not old.exists():atomic_json(old,request,new=True)
            if read_request(old)!=request:raise PermissionError('Descriptor validation disagrees')
            prepared=job/'prepared.json'
            if prepared.exists():
                if protected_json(prepared)!=record:raise PermissionError('Prepared identity differs')
            else:atomic_json(prepared,record,new=True)
            for name in ('results','checkpoints','logs'):(job/name).mkdir(mode=0o700,exist_ok=True)
            return dict(prepared=True,record=record,cache_hits=dict(source_cached=False,data_cached=False,environment_cached=True))


def install_preparer(module,preparer):
    from flask import Response,g
    from .inventory_protocol import encode
    from .staging_protocol import PREPARE_PATH
    if not isinstance(preparer,OfflinePreparer) or preparer.root!=module.BASE_DIR/'worker-storage':
        raise ValueError('Exact trusted offline worker root required')
    def prepare():
        try:
            request=g.staging_body
            with module.job_lock:
                active=module.current_job
                if active and active.get('job_id')==request.get('job_id') and module.job_process is not None:
                    raise PermissionError('Cannot prepare a running attempt')
                result=preparer.prepare(request)
            return Response(encode(result),mimetype='application/json')
        except (ValueError,OSError,KeyError,TypeError):
            return Response(encode(dict(error='Offline preparation rejected')),status=409,mimetype='application/json')
    module.app.add_url_rule(PREPARE_PATH,'hybrid_prepare',prepare,methods=['POST'])
