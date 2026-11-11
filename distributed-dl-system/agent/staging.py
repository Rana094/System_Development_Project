"""Explicit trusted worker staging service. Normal agent imports do not enable it.

Requires an operator-supplied isolation boundary. No unsafe same-UID fallback,
credential provisioning, mounts, dependency installation or artifact HTTP API.
"""

import threading

from hybrid_storage import events
from hybrid_storage.cache_fetch import CacheFetcher
from hybrid_storage.workspaces import WorkspaceManager
from hybrid_storage.inventory_protocol import Credential, fields, hex_id, encode
from hybrid_storage.staging_protocol import (STAGE_PATH, START_PATH, CONTROL_PATH, HEADER, read_request,
    parse_plan, receipt_document, receipt_hash, signature)


class StagingService:
    def __init__(self, fetcher, workspaces, credential, *, worker_root, ownership,
                 cancelled, isolate, quiescent=None, terminate=None):
        if (not isinstance(fetcher, CacheFetcher) or not isinstance(workspaces, WorkspaceManager)
                or workspaces.cache is not fetcher.cache or not isinstance(credential, Credential)
                or not callable(cancelled) or not callable(isolate)
                or any(value is not None and not callable(value) for value in (quiescent,terminate))):
            raise ValueError('Explicit cache, workspace, credential and isolated lifecycle required')
        from hybrid_storage.staging_journal import StagingJournal
        self.fetcher,self.workspaces,self.credential=fetcher,workspaces,credential
        self.root,self.ownership,self.cancelled,self.isolate=worker_root,ownership,cancelled,isolate
        self.quiescent,self.terminate=quiescent,terminate
        self.journal=StagingJournal(workspaces.root)
        self._lock=threading.Lock();self._ready={};self._active=None
        self._stop=threading.Event();self._watchdog=None;self.module=None
        self._orphan_busy=True

    def initialize(self,module):
        self.module=module;self.journal.initialize()
        # A restart NEVER restores launch authority from a READY file.
        for record in self.journal.records():
            if record['phase']!='TERMINAL':self.abort(record,'RESTART_ABORTED')
        self.tick()

    def start_watchdog(self):
        if self._watchdog is not None:raise ValueError('Watchdog already installed')
        def loop():
            while not self._stop.wait(0.25):
                try:self.tick()
                except Exception:self._orphan_busy=True  # Retain ownership/pins on uncertainty.
        self._watchdog=threading.Thread(target=loop,daemon=True)
        self._watchdog.start()

    def close(self):
        self._stop.set()
        if self._watchdog is not None:self._watchdog.join(timeout=1)
        # Do not release the single-service lock while a materializer owns it.
        if not self._lock.acquire(blocking=False):return False
        try:self.journal.close();return True
        finally:self._lock.release()

    def busy(self):
        try:return self._orphan_busy or any(r['phase']!='TERMINAL' for r in self.journal.records())
        except (OSError,ValueError,KeyError):return True

    def _record(self,stage_id):
        with self.journal.session() as state:return self.journal.read(state,stage_id)

    def _new(self,stage_id,plan,lease):
        return dict(version=1,stage_id=stage_id,plan=plan.document(),plan_sha256=plan.sha256,
                    lease=lease,boot_id=self.ownership.boot_id(),phase='STAGING',outcome=None,
                    launched=False,pins_released=False)

    def _matches(self,record,plan,lease):
        if (record['plan_sha256']!=plan.sha256 or record['lease']['job_id']!=lease['job_id']
                or record['lease']['attempt_token']!=lease['attempt_token']
                or self.ownership.fence(record['lease'])!=self.ownership.fence(lease)):
            raise PermissionError('Conflicting lifecycle identity')

    def authority(self,plan,lease,stage_id=None):
        if self._stop.is_set() or self.cancelled(plan.identity.job_id,plan.identity.attempt_token):
            raise PermissionError('Staging attempt stopped/cancelled')
        if stage_id is not None:
            record=self._record(stage_id)
            self._matches(record,plan,lease)
            if record['phase'] not in ('STAGING','READY','LAUNCHING','RUNNING'):
                raise PermissionError('Lifecycle no longer permits execution')
        self.ownership.assert_owned(self.root,plan.identity.job_id,plan.identity.attempt_token,
                                    lease['generation'],lease['lease_id'])
        return True

    def stage(self,body):
        fields(body,'stage_id plan lease')
        stage_id=hex_id(body['stage_id']);plan=parse_plan(body['plan']);lease=body['lease']
        if not self._lock.acquire(blocking=False):raise ValueError('Worker staging slot busy')
        try:
            for old in self.journal.records():
                if old['stage_id']!=stage_id and old['phase']!='TERMINAL':
                    raise ValueError('Previous attempt requires reconciliation')
            with self.journal.session() as state:
                record=self.journal.read(state,stage_id)
                if record is None:
                    record=self._new(stage_id,plan,lease);self.journal.write(state,record)
                self._matches(record,plan,lease)
                if record['phase'] not in ('STAGING','READY'):raise PermissionError('Attempt fenced')
            self._active=stage_id
            # Serialize durable authority updates with cancel/renew/launch; an
            # old cancellation must never overwrite a newer generation.
            with self.module.job_lock:
                with self.journal.session() as state:
                    record=self.journal.read(state,stage_id)
                    if record['phase'] not in ('STAGING','READY'):raise PermissionError('Attempt cancelled')
                    self.ownership.accept(self.root,lease,plan.identity.job_id,plan.identity.attempt_token,self.credential.worker)
                    record['lease']=lease;self.journal.write(state,record)
            authorize=lambda _:self.authority(plan,lease,stage_id)
            with events.observe_staging(plan,self.credential.worker,stage_id) as event:
                # Protect the fetch -> persistent workspace-pin gap as one activity.
                with self.fetcher.cache.activity(), self.fetcher.cache.metrics.observe() as observation:
                    event.counters=observation
                    results=self.fetcher.ensure(plan.inputs(),authorize=authorize)
                    self.workspaces.prepare(plan,authorize=authorize)
                from hybrid_storage.staging_metrics import measured
                metrics=measured(observation.snapshot(),results)
                self.authority(plan,lease,stage_id)
                with self.journal.session() as state:
                    record=self.journal.read(state,stage_id)
                    if record['phase'] not in ('STAGING','READY'):raise PermissionError('Attempt cancelled')
                    record['phase']='READY';self.journal.write(state,record)
                event.ready(metrics,results)
            expected=receipt_document(stage_id,plan,lease)
            self._ready[stage_id]=(plan,lease,expected)  # Diagnostic compatibility, not authority.
            return dict(receipt=expected,receipt_sha256=receipt_hash(expected),
                        metrics=metrics,
                        cache_hit_bytes=sum(r.size_bytes for r in results if r.cache_hit),
                        staged_bytes=sum(r.size_bytes for r in results if not r.cache_hit))
        except BaseException:
            record=self._record(stage_id)
            if record is not None:self.abort(record,'STAGING_FAILED')
            raise
        finally:
            self._active=None;self._lock.release()
            self.tick()

    def check_start(self,data):
        stage_id=hex_id(data.get('staging_id'));record=self._record(stage_id)
        if record is None:raise PermissionError('Unknown staged attempt')
        plan=parse_plan(record['plan']);lease=record['lease'];receipt=receipt_document(stage_id,plan,lease)
        if (record['phase'] not in ('READY','RUNNING') or data.get('job_type')!='custom'
                or data.get('job_id')!=plan.identity.job_id or data.get('attempt_token')!=plan.identity.attempt_token
                or data.get('staging_receipt')!=receipt_hash(receipt)
                or self.ownership.fence(data.get('lease'))!=self.ownership.fence(lease)
                or (data.get('ddp') or {}).get('rank',0)!=plan.identity.rank):
            raise PermissionError('Start differs from live staged attempt')
        self.authority(plan,lease,stage_id)
        publisher=self.module.app.extensions.get('checkpoint_publication')
        if publisher is not None:
            from checkpoint_restore_service import launch_path
            launch_path(publisher,self.module,data)
        return plan,lease

    def launch_inputs(self,data):
        if not callable(self.quiescent) or not callable(self.terminate):
            raise PermissionError('Reviewed containment termination/quiescence providers required')
        if not self._lock.acquire(blocking=False):raise ValueError('Materializer still owns workspace')
        try:
            plan,lease=self.check_start(data);stage_id=data['staging_id']
            with self.journal.session() as state:
                record=self.journal.read(state,stage_id)
                if record['phase']!='READY':raise PermissionError('Attempt already launched')
                # Ambiguous launch intent must survive death between Popen/attach.
                record.update(phase='LAUNCHING',launched=True);self.journal.write(state,record)
            return self.workspaces.mark_started(plan,authorize=lambda _:self.authority(plan,lease,stage_id))
        finally:self._lock.release()

    def launched(self,data):
        with self.journal.session() as state:
            record=self.journal.read(state,data['staging_id'])
            if record['phase']!='LAUNCHING':raise PermissionError('Launch intent fenced')
            record['phase']='RUNNING';record['lease']=data['lease'];self.journal.write(state,record)

    def revoke(self,record):
        lease=record['lease']
        for path in (self.ownership.lease_path(self.root,lease),self.ownership.authority_path(self.root)):
            try:held=self.ownership.read(path)
            except FileNotFoundError:continue
            if (self.ownership.fence(held['lease'])==self.ownership.fence(lease)
                    and held['lease']['job_id']==lease['job_id'] and held['lease']['attempt_token']==lease['attempt_token']):
                held.update(revoked=True,completed=False);self.ownership.atomic(path,held)

    def abort(self,record,outcome):
        plan=parse_plan(record['plan'])
        with self.module.job_lock:
            self.module.remember_cancellation(plan.identity.job_id,plan.identity.attempt_token)
            with self.journal.session() as state:
                current=self.journal.read(state,record['stage_id'])
                if current is None:current=record
                if current['phase']!='TERMINAL':
                    if current['phase']!='ABORTING':current.update(phase='ABORTING',outcome=outcome)
                    self.journal.write(state,current)
            self.revoke(current)
            active=self.module.current_job
            if (active and active.get('lease') and active.get('attempt_token')==plan.identity.attempt_token
                    and active.get('job_id')==plan.identity.job_id
                    and self.ownership.fence(active.get('lease'))==self.ownership.fence(record['lease'])):
                process=self.module.job_process
                if process is not None and process.poll() is None:
                    import os,signal
                    try:os.killpg(process.pid,signal.SIGKILL)
                    except ProcessLookupError:pass
        if self._active==record['stage_id']:self.fetcher.interrupt()
        if current['phase']=='ABORTING' and current['launched'] and callable(self.terminate):
            self.terminate(plan.identity,current)
        return current

    def note_exit(self,lease,returncode):
        for record in self.journal.records():
            if self.ownership.fence(record['lease'])!=self.ownership.fence(lease):continue
            plan=parse_plan(record['plan'])
            accepted=False
            try:accepted=returncode==0 and record['phase']=='RUNNING' and record['launched'] and self.authority(plan,lease,record['stage_id'])
            except (OSError,ValueError):pass
            with self.journal.session() as state:
                current=self.journal.read(state,record['stage_id'])
                if current['phase'] not in ('ABORTING','TERMINAL'):
                    current.update(phase='ABORTING',outcome='COMPLETED' if accepted else 'FAILED')
                    self.journal.write(state,current)
            return bool(accepted)
        return False

    def control(self,body):
        fields(body,'action stage_id plan lease')
        action=body['action'];stage_id=hex_id(body['stage_id']);plan=parse_plan(body['plan']);lease=body['lease']
        self.ownership.validate_fence(self.ownership.fence(lease))
        if (lease['node_id']!=self.credential.worker or lease['job_id']!=plan.identity.job_id
                or lease['attempt_token']!=plan.identity.attempt_token):raise PermissionError('Control identity differs')
        record=self._record(stage_id)
        if record is not None:self._matches(record,plan,lease)
        if action=='cancel':
            if record is None:record=self._new(stage_id,plan,lease)
            self.abort(record,'CANCELLED')
        elif action=='renew':
            if record is None:raise PermissionError('No durable stage to renew')
            with self.module.job_lock:
                self.authority(plan,record['lease'],stage_id)
                self.ownership.accept(self.root,lease,plan.identity.job_id,plan.identity.attempt_token,self.credential.worker)
                with self.journal.session() as state:
                    record=self.journal.read(state,stage_id)
                    if record['phase'] not in ('STAGING','READY','RUNNING'):raise PermissionError('Renewal fenced')
                    record['lease']=lease;self.journal.write(state,record)
        elif action!='status':raise ValueError('Unsupported lifecycle action')
        self.tick()
        record=self._record(stage_id)
        if record is None:return dict(stage_id=stage_id,plan_sha256=plan.sha256,phase='UNKNOWN',pins_released=False,outcome=None)
        result=dict(stage_id=stage_id,plan_sha256=record['plan_sha256'],phase=record['phase'],
                    pins_released=record['pins_released'],outcome=record['outcome'],
                    lease_id=record['lease']['lease_id'],generation=record['lease']['generation'])
        observed=self.module.current_job
        if observed is None or observed.get('job_id')!=plan.identity.job_id:
            observed=self.module.job_history.get(plan.identity.job_id)
        if (observed is not None and observed.get('attempt_token')==plan.identity.attempt_token
                and self.ownership.fence(observed.get('lease'))==self.ownership.fence(record['lease'])):
            result['job']=self.module.job_with_resume(observed)
        return result

    def tick(self):
        for record in self.journal.records():
            if record['phase'] not in ('ABORTING','TERMINAL'):
                plan=parse_plan(record['plan'])
                try:self.authority(plan,record['lease'],record['stage_id'])
                except (OSError,ValueError):record=self.abort(record,'LEASE_EXPIRED')
            if record['phase']=='TERMINAL':continue
            if record['phase']!='ABORTING' or not self._lock.acquire(blocking=False):continue
            try:
                if not self.fetcher.reconcile_orphans():continue
                plan=parse_plan(record['plan'])
                stopped=(not record['launched'] or record['boot_id']!=self.ownership.boot_id()
                         or (callable(self.quiescent) and self.quiescent(plan.identity,record) is True))
                process=self.module.job_process;active=self.module.current_job
                if (active and active.get('job_id')==plan.identity.job_id
                        and active.get('attempt_token')==plan.identity.attempt_token
                        and process is not None and process.poll() is None):stopped=False
                if not stopped:continue
                finished=self.workspaces.finish(plan,record['outcome'],quiescent=lambda _:True)
                if not finished['pins_released']:continue
                self.workspaces.cleanup_temporary(plan,quiescent=lambda _:True)
                with self.journal.session() as state:
                    current=self.journal.read(state,record['stage_id'])
                    current.update(phase='TERMINAL',pins_released=True);self.journal.write(state,current)
                self._ready.pop(record['stage_id'],None)
            finally:self._lock.release()
        self._orphan_busy=not self.fetcher.reconcile_orphans()


def install_staging_service(module, service, *, watchdog=True):
    """Trusted bootstrap only; dedicated hybrid worker, never alongside a trainer.

    All mutating legacy routes on this worker require authentication while this
    service is installed. The normal bootstrap remains unchanged/default OFF.
    isolate(command, receipt, data) MUST supply reviewed OS AND network isolation;
    it must reject unsupported execution modes. There is deliberately no default.
    """
    if not isinstance(service, StagingService) or service.root != module.BASE_DIR or service.credential.worker != module.node_id():
        raise ValueError('Stager identity/root differs from actual worker')
    if module.job_process is not None or 'hybrid_staging' in module.app.extensions:
        raise ValueError('Worker must be idle with no staging service installed')
    from flask import g, request, Response
    app = module.app

    @app.before_request
    def authenticate_control():
        if request.method != 'POST':
            return None
        # Do not expose unsigned stop/renew/upload mutations as an auth bypass.
        from hybrid_storage.staging_protocol import PREPARE_PATH
        if request.path not in (STAGE_PATH, START_PATH, CONTROL_PATH, PREPARE_PATH):
            if request.path == '/resources/cache-inventory':
                return None  # Has its own independently domain-separated HMAC.
            return Response('Hybrid control action unavailable', status=403)
        try:
            from hybrid_storage.inventory_protocol import MAX_BYTES
            if request.content_length is None or not 0 < request.content_length <= MAX_BYTES:
                raise ValueError('Bounded request body required')
            raw = request.stream.read(MAX_BYTES + 1)
            body = read_request(service.credential, request.path, raw, request.headers.get(HEADER))
            if request.path == START_PATH and not body.get('staging_id'):
                raise ValueError('Hybrid worker accepts staged starts only')
            if request.path == START_PATH:
                service.check_start(body)
            g.staging_raw = raw
            g.staging_body = body
        except (ValueError, OSError, KeyError, TypeError):
            return Response('Staging authorization rejected', status=403)

    @app.after_request
    def sign_reply(response):
        if hasattr(g, 'staging_raw'):
            response.headers[HEADER] = signature(service.credential, request.path,
                                                 response.get_data(), reply_to=g.staging_raw)
        return response

    def stage_route():
        try:
            with module.job_lock:
                if module.job_process is not None and module.job_process.poll() is None:
                    raise ValueError('Worker busy')
            result = service.stage(g.staging_body)
            return Response(encode(result), mimetype='application/json')
        except (ValueError, OSError, KeyError, TypeError):
            # No raw NFS paths, keys or other owners' names in HTTP failures.
            return Response(encode(dict(error='Staging failed; no launch authorized')), status=409,
                            mimetype='application/json')

    def control_route():
        try:return Response(encode(service.control(g.staging_body)),mimetype='application/json')
        except (OSError,ValueError,KeyError,TypeError):
            return Response(encode(dict(error='Lifecycle control rejected')),status=409,mimetype='application/json')

    app.add_url_rule(STAGE_PATH, 'hybrid_stage', stage_route, methods=['POST'])
    app.add_url_rule(CONTROL_PATH, 'hybrid_control', control_route, methods=['POST'])
    service.initialize(module)
    # Activate only after every route/auth hook registered successfully. Flask
    # rejects setup after its first request; that must never leave an enabled
    # agent launch hook without the authentication hook.
    app.extensions['hybrid_staging'] = service
    if watchdog:service.start_watchdog()
