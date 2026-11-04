"""Student web interface over the existing submission, queue and recovery services."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
import fcntl
import getpass
import io
import json
import os
from pathlib import Path
import secrets
import signal
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import zipfile

import requests
from flask import Flask, abort, jsonify, request, send_file, session
from werkzeug.exceptions import HTTPException
from werkzeug.security import check_password_hash, generate_password_hash
from werkzeug.utils import secure_filename

import job_store as store
from job_spec import load_spec, validate_owner, validate_spec
from job_storage import stage_job, unpack_zip, verify_workspace
from queue_policy import ACTIVE_STATES, fair_order, limits, load_policy
from scheduler import load_nodes
from submit_job import create_job_id
import execution_leases as ownership

ROOT = Path(__file__).resolve().parents[1]
EXAMPLES = {'regression': 'job.integration.json', 'binary': 'job.binary.json',
            'cnn': 'job.cnn.json', 'ddp': 'job.distributed-checkpoint.json'}
TERMINAL = ('COMPLETED', 'FAILED', 'CANCELLED')
LOG_BYTES = 64 * 1024


def rows(sql, params=()):
    # Read-only connections keep dashboard polling from changing scheduler state.
    with closing(sqlite3.connect(Path(store.DB_FILE).as_uri() + '?mode=ro', uri=True)) as db:
        db.row_factory = sqlite3.Row
        return [dict(row) for row in db.execute(sql, params)]


def configure_storage(directory):
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    secret = directory / 'session.key'
    try:
        fd = os.open(secret, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        pass
    else:
        with os.fdopen(fd, 'w') as output:
            output.write(secrets.token_hex(32))
    return secret.read_text().strip()


def add_user(directory, owner, password):
    """Only the controller operator can assign an existing submission owner a login."""
    validate_owner(owner)
    if len(password) < 10:
        raise ValueError('Use a password of at least 10 characters')
    configure_storage(directory)
    with (directory / '.users.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        path = directory / 'users.json'
        users = json.loads(path.read_text()) if path.exists() else {}
        users[owner] = generate_password_hash(password)
        fd, temporary = tempfile.mkstemp(dir=directory)
        try:
            with os.fdopen(fd, 'w') as output:
                json.dump(users, output)
            os.replace(temporary, path)
        finally:
            Path(temporary).unlink(missing_ok=True)


def public_spec(key):
    spec = json.loads((ROOT / 'examples' / EXAMPLES[key]).read_text())
    # Paths in downloadable templates refer to uploads, never controller folders.
    spec['project'] = 'project.zip'
    if spec['dataset'] is not None:
        spec['dataset'] = Path(spec['dataset']).name
    return spec


def checked_upload(upload, directory, name):
    """Bound ZIP expansion and use the existing traversal/link-safe extractor."""
    target = directory / name
    target.mkdir()
    filename = secure_filename(upload.filename or '')
    if not filename:
        raise ValueError('Choose a named upload')
    path = target / filename
    upload.save(path)
    if path.suffix.lower() == '.zip':
        with zipfile.ZipFile(path) as archive:
            members = archive.infolist()
            if len(members) > 10000 or sum(m.file_size for m in members) > 512 * 1024 * 1024:
                raise ValueError('ZIP exceeds 10,000 files or 512 MiB expanded size')
        extracted = target / 'unpacked'
        unpack_zip(path, extracted)
        path.unlink()
        return extracted
    if name == 'project':
        raise ValueError('Upload the project as a ZIP, with train.py and requirements.txt at its root')
    return path


class PreparationQueue:
    """Persist browser preparation requests so a portal restart can continue them."""
    def __init__(self):
        with closing(store.get_connection()) as db, db:
            db.execute('''CREATE TABLE IF NOT EXISTS portal_preparation (
                job_id TEXT PRIMARY KEY, profile TEXT NOT NULL, state TEXT NOT NULL,
                error TEXT, updated_at REAL NOT NULL, FOREIGN KEY(job_id) REFERENCES jobs(job_id))''')

    def enqueue(self, job_id, profile):
        if profile not in ('pytorch-cpu', 'isolated', 'worker-base'):
            raise ValueError('Unknown environment profile')
        with closing(store.get_connection()) as db, db:
            db.execute('BEGIN IMMEDIATE')
            job = db.execute('SELECT status FROM jobs WHERE job_id=?', (job_id,)).fetchone()
            if not job or job['status'] not in ('STAGED', 'READY'):
                raise ValueError('Only staged or ready jobs can be prepared')
            old = db.execute('SELECT state FROM portal_preparation WHERE job_id=?', (job_id,)).fetchone()
            if old and old['state'] in ('PENDING', 'PREPARING'):
                return
            db.execute('INSERT OR REPLACE INTO portal_preparation VALUES(?,?,?,NULL,?)',
                       (job_id, profile, 'PENDING', time.time()))

    def update(self, job_id, state, error=None):
        with closing(store.get_connection()) as db, db:
            db.execute('UPDATE portal_preparation SET state=?,error=?,updated_at=? WHERE job_id=?',
                       (state, error, time.time(), job_id))

    def run_one(self):
        tasks = rows("SELECT * FROM portal_preparation WHERE state='PENDING' ORDER BY updated_at LIMIT 1")
        if not tasks:
            return False
        task = tasks[0]
        job = store.get_job(task['job_id'])
        if job['status'] not in ('STAGED', 'READY'):
            self.update(job['job_id'], 'CANCELLED' if job['status']=='CANCELLED' else 'DONE')
            return True
        self.update(job['job_id'], 'PREPARING')
        process = None
        try:
            node_ids = [n['id'] for n in load_nodes()]
            if not node_ids:
                raise ValueError('No workers configured')
            # Reuse the CLI in a child process: pip/SSH output cannot block HTTP requests.
            command = [sys.executable, str(ROOT/'controller/prepare_job.py'), '--job-id', job['job_id'],
                       '--nodes', *node_ids, '--profile', task['profile'], '--summary']
            with (Path(job['storage_path'])/'logs/preparation.log').open('a') as log:
                process = subprocess.Popen(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT,
                                           start_new_session=True)
                deadline = time.monotonic()+3600
                while process.poll() is None:
                    if store.get_job(job['job_id'])['status']=='CANCELLED':
                        raise ValueError('Cancelled during preparation')
                    if time.monotonic()>deadline:
                        raise ValueError('Preparation exceeded one hour; check its log and retry')
                    time.sleep(0.5)
            current = store.get_job(job['job_id'])
            if current['status'] != 'READY':
                raise ValueError('Not enough compatible environments; see preparation log and retry')
            verify_workspace(Path(current['storage_path']), current)
            store.enqueue_custom_job(job['job_id'], set(node_ids))
            self.update(job['job_id'], 'DONE')
        except Exception as error:
            self.update(job['job_id'], 'CANCELLED' if store.get_job(job['job_id'])['status']=='CANCELLED' else 'ERROR', str(error))
        finally:
            if process and process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait()
        return True

    def start(self):
        with closing(store.get_connection()) as db, db:
            db.execute("UPDATE portal_preparation SET state='PENDING' WHERE state='PREPARING'")
        def work():
            while True:
                try:
                    if self.run_one():
                        continue
                except Exception as error:
                    print('Portal preparation:', error, flush=True)
                time.sleep(1)
        threading.Thread(target=work, daemon=True).start()


def dispatcher_running():
    path = ROOT/'controller/.dispatcher.lock'
    if not path.exists():
        return False
    with path.open('r') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        fcntl.flock(lock, fcntl.LOCK_UN)
    return False


class ClusterView:
    def __init__(self):
        self.lock = threading.Lock()
        self.updated = 0
        self.value = []

    def get(self):
        with self.lock:
            if time.monotonic()-self.updated < 3:
                return self.value
            def poll(node):
                try:
                    response = requests.get(f"http://{node['ip']}:{node['port']}/resources", timeout=2)
                    response.raise_for_status()
                    data = response.json()
                    # Show capacity without revealing another student's job identity.
                    return {'id': node['id'], 'state': data.get('node_state','ONLINE'),
                            **{k:data.get(k) for k in ('cpu','memory','disk','temperature_c','slots')}}
                except (requests.RequestException, ValueError):
                    return {'id': node['id'], 'state': 'UNREACHABLE'}
            with ThreadPoolExecutor(max_workers=8) as pool:
                self.value = list(pool.map(poll, load_nodes()))
            self.updated = time.monotonic()
            return self.value


def create_app(directory=None, background=False):
    directory = Path(directory or ROOT/'storage/portal')
    app = Flask(__name__, static_folder='portal_static', static_url_path='/static')
    app.secret_key = configure_storage(directory)
    app.config.update(MAX_CONTENT_LENGTH=256*1024*1024, SESSION_COOKIE_HTTPONLY=True,
                      SESSION_COOKIE_SAMESITE='Strict', PERMANENT_SESSION_LIFETIME=8*3600)
    preparation = PreparationQueue()
    app.extensions['preparation'] = preparation
    cluster = ClusterView()
    attempts = {}
    attempts_lock = threading.Lock()

    @app.before_request
    def access():
        if not request.path.startswith('/api/'):
            return
        if request.method != 'GET' and (not session.get('csrf') or
                not secrets.compare_digest(request.headers.get('X-CSRF-Token',''), session['csrf'])):
            abort(403, 'Refresh the page before trying again')
        if request.path not in ('/api/session','/api/login') and not session.get('owner'):
            abort(401, 'Sign in first')

    @app.after_request
    def headers(response):
        response.headers['Cache-Control'] = 'no-store'
        response.headers['X-Content-Type-Options'] = 'nosniff'
        response.headers['Content-Security-Policy'] = "default-src 'self'; script-src 'self'; style-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'"
        return response

    @app.errorhandler(Exception)
    def error_response(error):
        if isinstance(error, HTTPException):
            return jsonify(error=error.description), error.code
        if isinstance(error, (ValueError, OSError, zipfile.BadZipFile)):
            return jsonify(error=str(error)), 400
        app.logger.exception('Portal request failed')
        return jsonify(error='Request failed; check the controller portal log'), 500

    def owned(job_id):
        job = store.get_job(job_id)
        if not job or job.get('owner')!=session['owner'] or job['job_type']!='custom':
            abort(404, 'Job not found')
        return job

    @app.get('/')
    def index():
        return app.send_static_file('index.html')

    @app.get('/api/session')
    def session_info():
        session.setdefault('csrf', secrets.token_hex(24))
        return jsonify(owner=session.get('owner'), csrf=session['csrf'])

    @app.post('/api/login')
    def login():
        data = request.get_json()
        if not isinstance(data,dict): abort(400, 'Expected a JSON object')
        owner, password = data.get('owner',''), data.get('password','')
        validate_owner(owner)
        if not isinstance(password,str) or len(password)>1024:
            abort(400, 'Invalid password')
        key = request.remote_addr
        with attempts_lock:
            now = time.monotonic()
            for address in list(attempts):
                attempts[address] = [t for t in attempts[address] if now-t<60]
                if not attempts[address]: del attempts[address]
            if len(attempts.get(key,[]))>=10:
                abort(429, 'Too many sign-in attempts; wait one minute')
            attempts.setdefault(key,[]).append(now)
        users = json.loads((directory/'users.json').read_text()) if (directory/'users.json').exists() else {}
        if owner not in users or not check_password_hash(users[owner], password):
            abort(401, 'Incorrect student ID or password')
        session.clear()
        session.permanent = True
        session.update(owner=owner, csrf=secrets.token_hex(24))
        return jsonify(owner=owner, csrf=session['csrf'])

    @app.post('/api/logout')
    def logout():
        session.clear()
        return jsonify(ok=True)

    @app.get('/api/examples')
    def examples():
        return jsonify({key:public_spec(key) for key in EXAMPLES})

    @app.get('/api/examples/<key>/download')
    def example_download(key):
        if key not in EXAMPLES: abort(404)
        spec = load_spec(ROOT/'examples'/EXAMPLES[key])
        project = io.BytesIO()
        with zipfile.ZipFile(project,'w',zipfile.ZIP_DEFLATED) as archive:
            for path in sorted(Path(spec['project']).rglob('*')):
                if path.is_file() and '__pycache__' not in path.parts:
                    archive.write(path,path.relative_to(spec['project']))
        bundle = io.BytesIO()
        with zipfile.ZipFile(bundle,'w',zipfile.ZIP_DEFLATED) as archive:
            archive.writestr('project.zip',project.getvalue())
            archive.writestr('job.json',json.dumps(public_spec(key),indent=2))
            if spec['dataset']: archive.write(spec['dataset'],Path(spec['dataset']).name)
        bundle.seek(0)
        return send_file(bundle,as_attachment=True,download_name=key+'-submission.zip')

    @app.post('/api/jobs')
    def submit():
        profile = request.form.get('profile','pytorch-cpu')
        if profile not in ('pytorch-cpu','isolated','worker-base'):
            raise ValueError('Unknown environment profile')
        document = json.loads(request.form.get('spec','{}'))
        if not isinstance(document,dict): raise ValueError('Job definition must be a JSON object')
        example = request.form.get('example')
        with tempfile.TemporaryDirectory(prefix='portal-upload-') as temporary:
            base = Path(temporary)
            if example:
                if example not in EXAMPLES: raise ValueError('Unknown example')
                reference = load_spec(ROOT/'examples'/EXAMPLES[example])
                document.update(project=reference['project'], dataset=reference['dataset'])
            else:
                if 'project' not in request.files: raise ValueError('Choose a project ZIP')
                document['project'] = str(checked_upload(request.files['project'], base, 'project'))
                document['dataset'] = (str(checked_upload(request.files['dataset'],base,'dataset'))
                                       if request.files.get('dataset') else None)
            spec = validate_spec(document,base)
            # Built-in examples have known capabilities; reject incompatible forms before staging.
            if example in ('regression', 'binary', 'cnn') and spec['execution']['mode'] != 'single':
                raise ValueError('This example supports one worker. Choose the Distributed classifier (DDP) example for multiple workers.')
            if example == 'ddp':
                if spec['execution']['mode']=='single' and spec['recovery']['mode']=='checkpoint':
                    raise ValueError('This DDP example requires distributed execution for checkpoint recovery. Choose a single-worker example, or use restart recovery for its single-worker benchmark.')
                args = spec['entrypoint'][2:]
                batch = 64
                try:
                    for index, arg in enumerate(args):
                        if arg == '--global-batch-size': batch = int(args[index+1])
                        elif arg.startswith('--global-batch-size='): batch = int(arg.split('=',1)[1])
                except (ValueError, IndexError):
                    raise ValueError('Provide an integer after --global-batch-size') from None
                workers = spec['execution']['workers']
                if batch < workers or batch % workers:
                    raise ValueError(f'Global batch size {batch} must be divisible by {workers} workers. For 3 workers, use --global-batch-size 96.')
            if spec['execution']['workers']>len(load_nodes()):
                raise ValueError('Requested workers exceed the configured cluster size')
            job_id = create_job_id()
            store.add_job(job_id,1,spec['recovery']['max_retries'],job_type='custom',job_config=spec,
                          status='RECEIVED',owner=session['owner'])
            try:
                stage_job(job_id)
            except Exception:
                # Temporary inputs will be removed; do not leave an unresumable receipt pending.
                store.control_job(job_id,session['owner'],'cancel')
                raise
        preparation.enqueue(job_id,profile)
        return jsonify(job_id=job_id,status='STAGED'),202

    @app.get('/api/overview')
    def overview():
        all_jobs = rows('SELECT * FROM jobs')
        policy = load_policy()
        queued = fair_order([j for j in all_jobs if j['status']=='QUEUED'],
                            [j for j in all_jobs if j['status'] in ACTIVE_STATES],
                            {r['owner']:r['last_dispatch'] for r in rows('SELECT * FROM owner_service')},policy,time.time())
        positions = {j['job_id']:i+1 for i,j in enumerate(queued)}
        tasks = {t['job_id']:t for t in rows('SELECT * FROM portal_preparation')}
        jobs = []
        for job in sorted(all_jobs,key=lambda j:j['created_at'],reverse=True):
            if job['owner']!=session['owner'] or job['job_type']!='custom': continue
            spec = json.loads(job['job_config'])
            jobs.append({**{k:job[k] for k in ('job_id','status','created_at','node_id','retry_count','queue_reason','last_error')},
                         'name':spec.get('name',job['job_id']),'execution':spec.get('execution',{}),
                         'position':positions.get(job['job_id']),'preparation':tasks.get(job['job_id'])})
        return jsonify(jobs=jobs,nodes=cluster.get(),dispatcher_running=dispatcher_running(),
                       limits={k:limits(policy,session['owner'])[k] for k in ('max_active_per_owner','max_pending_per_owner')},
                       timestamp=time.time())

    @app.get('/api/jobs/<job_id>')
    def detail(job_id):
        job = owned(job_id)
        events = rows("SELECT * FROM recovery_events WHERE job_id=? AND event_type NOT IN ('CHECKPOINT_SYNC','DDP_CHECKPOINT_COMMITTED') ORDER BY event_time DESC,id DESC LIMIT 300",(job_id,))
        for event in events:
            event['details'] = json.loads(event.pop('details_json'))
        checkpoint = next(iter(rows("SELECT * FROM recovery_events WHERE job_id=? AND event_type IN ('CHECKPOINT_SYNC','DDP_CHECKPOINT_COMMITTED') ORDER BY event_time DESC,id DESC LIMIT 1",(job_id,))),None)
        if checkpoint:
            checkpoint['details'] = json.loads(checkpoint.pop('details_json'))
        # Only metadata already committed by the dispatcher is displayed; never unpickle training files.
        logs = sorted(p.name for p in (Path(job['storage_path'])/'logs').glob('*.log') if not p.is_symlink()) if job.get('storage_path') else []
        from job_timing import build_report
        timing = build_report(job, store.get_attempts(job_id)) if job.get('storage_path') and job['status'] in TERMINAL else None
        return jsonify(job=job,timing=timing,spec=json.loads(job['job_config']),attempts=store.get_attempts(job_id),
                       ranks=store.get_ranks(job_id),events=events,checkpoint=checkpoint,logs=logs,
                       leases=rows('SELECT * FROM execution_leases WHERE job_id=? ORDER BY retry_count,node_id',(job_id,)),
                       preparation=next(iter(rows('SELECT * FROM portal_preparation WHERE job_id=?',(job_id,))),None))

    @app.post('/api/jobs/<job_id>/<action>')
    def control(job_id,action):
        owned(job_id)
        if action=='prepare':
            preparation.enqueue(job_id,request.get_json().get('profile','pytorch-cpu'))
            return jsonify(status='PENDING'),202
        if action not in ('cancel','hold','release'): abort(404)
        return jsonify(status=store.control_job(job_id,session['owner'],action))

    @app.get('/api/jobs/<job_id>/logs')
    def logs(job_id):
        job = owned(job_id)
        name = request.args.get('file')
        if name:
            if Path(name).name!=name or not name.endswith('.log'): abort(400,'Invalid log name')
            path = Path(job['storage_path'])/'logs'/name
            if path.is_symlink() or not path.is_file(): abort(404,'Log not collected yet')
            with path.open('rb') as stream:
                stream.seek(max(0,path.stat().st_size-LOG_BYTES))
                data = stream.read(LOG_BYTES)
            return jsonify(text=data.decode(errors='replace'),source=name)
        if job['status'] not in ACTIVE_STATES:
            return jsonify(text='Choose a collected attempt log above.',source='controller')
        rank = request.args.get('rank',0,type=int)
        ranks = store.get_ranks(job_id,job['retry_count'])
        if ranks:
            selected = next((r for r in ranks if r['rank']==rank),None)
            if not selected: abort(404,'Rank not found')
            node_id = selected['node_id']
        else:
            node_id = job['node_id']
        node = next((n for n in load_nodes() if n['id']==node_id),None)
        if not node: abort(404,'Worker not allocated yet')
        try:
            url = f"http://{node['ip']}:{node['port']}/jobs/{job_id}/log"
            params = ownership.params(job,node_id)
            response = requests.get(url, params=params, headers={'Range':f'bytes=-{LOG_BYTES}'},
                                    stream=True, timeout=(2,3))
            if response.status_code == 416:
                # Werkzeug rejects suffix ranges larger than a short (or empty) file.
                # Retry without Range; the bounded reader still caps memory and transfer.
                response.close()
                response = requests.get(url, params=params, stream=True, timeout=(2,3))
            with response:
                response.raise_for_status()
                ownership.response(job,node_id,response.headers)
                data = bytearray()
                for chunk in response.iter_content(4096):
                    data.extend(chunk[:LOG_BYTES-len(data)])
                    if len(data)>=LOG_BYTES: break
            ownership.current(job,node_id)
            return jsonify(text=data.decode(errors='replace'),source=f"attempt {job['retry_count']+1} · {node_id} · rank {rank}")
        except (requests.RequestException,ValueError) as error:
            return jsonify(text='Live log temporarily unavailable. Collected logs remain available.',source=str(error)),200

    @app.get('/api/jobs/<job_id>/results')
    def download_results(job_id):
        job = owned(job_id)
        if job['status'] not in TERMINAL: abort(409,'Results are available when the job finishes')
        from job_timing import write_report
        write_report(job_id)  # Also supplies historical wall times for older completed jobs.
        root = Path(job['storage_path'])/'results'
        # Build on disk, not in RAM, and exclude links so results cannot expose controller files.
        output = tempfile.TemporaryFile()
        with zipfile.ZipFile(output,'w',zipfile.ZIP_DEFLATED) as archive:
            for path in sorted(root.rglob('*')):
                if path.is_file() and not path.is_symlink() and path.resolve().is_relative_to(root.resolve()):
                    archive.write(path,path.relative_to(root))
        output.seek(0)
        response = send_file(output,mimetype='application/zip',as_attachment=True,download_name=job_id+'-results.zip')
        response.call_on_close(output.close)
        return response

    if background:
        preparation.start()
    return app


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--host',default='127.0.0.1')
    parser.add_argument('--port',type=int,default=8080)
    parser.add_argument('--production',action='store_true',help='Serve with Waitress (used by installed services)')
    parser.add_argument('--create-user',metavar='STUDENT_ID',help='Create/reset a student password, then exit')
    args = parser.parse_args()
    directory = ROOT/'storage/portal'
    if args.create_user:
        password = getpass.getpass('New password (at least 10 characters): ')
        if password != getpass.getpass('Repeat password: '): parser.error('Passwords differ')
        add_user(directory,args.create_user,password)
        print('Student login ready:',args.create_user)
        return
    configure_storage(directory)
    lock = (directory/'.server.lock').open('a')
    try:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    except BlockingIOError:
        parser.error('Another student portal is already running')
    store.initialize_database()
    from recovery_metrics import init_table
    init_table()
    app = create_app(directory,background=True)
    if args.production:
        from waitress import serve
        # One process owns the durable preparation queue; threads serve browser requests.
        serve(app,host=args.host,port=args.port,threads=8,max_request_body_size=256*1024*1024)
    else:
        app.run(host=args.host,port=args.port,threaded=True,debug=False,use_reloader=False)


if __name__=='__main__':
    main()
