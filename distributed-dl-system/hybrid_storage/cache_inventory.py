"""Authenticated controller-local inventory ingestion, never scheduler authority.

Explicit operator enrollment, existing schema prerequisite, exclusive local owner.
The caller supplies fresh dedicated FULL/FK connections; no import/startup DB I/O.
"""

from contextlib import closing
import fcntl
import hashlib
import os
from pathlib import Path
import secrets
import sqlite3
import threading
import time

from . import cache_registry_schema as schema
from . import inventory_protocol as p
from . import registry_schema as base
from .backends import _directory
from .worker_cache import _private_file, _protected


class CacheInventory:
    def __init__(self, connect, credentials, *, configured_workers, state_root: Path,
                 freshness_seconds=90, heartbeat_seconds=15, reconcile_seconds=30, clock=time.time_ns):
        self.connect, self.state_root, self.clock = connect, Path(state_root), clock
        if not self.state_root.is_absolute(): raise ValueError('Expected controller-local absolute state root')
        self.credentials = {credential.worker: credential for credential in credentials}
        if (len(self.credentials) != len(credentials)
                or len({c.key_id for c in credentials}) != len(credentials)
                or len({c.secret for c in credentials}) != len(credentials)):
            raise p.InventoryError('Worker credentials must be unique per enrolled identity')
        self.configured = frozenset(p.worker_id(w) for w in configured_workers)
        if not set(self.credentials) <= self.configured:
            raise p.InventoryError('Credentials reference workers outside operator inventory')
        for value in (freshness_seconds, heartbeat_seconds):
            if type(value) is not int or not 1 <= value <= 3600: raise ValueError('Invalid freshness interval')
        self.freshness_ns, self.heartbeat_ns = freshness_seconds * 10**9, heartbeat_seconds * 10**9
        if type(reconcile_seconds) is not int or not 1 <= reconcile_seconds < freshness_seconds:
            raise ValueError('Full reconciliation interval must be shorter than inventory TTL')
        self.reconcile_ns = reconcile_seconds * 10**9
        self._lock, self._owner = threading.RLock(), None
        self._contexts, self._pending, self._accepted = {}, {}, {}

    def start(self):
        """Explicit singleton owner; fence pre-restart sessions, never migrate."""
        with self._lock:
            if self._owner is not None: raise p.InventoryError('Inventory owner already started')
            with _directory(self.state_root) as root:
                _protected(root, directory=True)
                owner = _private_file(root, 'cache-inventory.lock', create=True)
            try:
                fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
                with closing(self.connect()) as db:
                    schema.require_schema(db)
                    main = next(row[2] for row in db.execute('PRAGMA database_list') if row[1] == 'main')
                    if Path(main).parent != self.state_root:
                        raise p.InventoryError('Inventory owner lock must be beside the actual controller DB')
                    with base.transaction(db):
                        db.execute("UPDATE cache_registry_sessions SET state='STALE' WHERE state='ACTIVE'")
                self.controller_epoch = secrets.token_hex(16)
                self._database_path = Path(main)
                self._owner = owner
            except BaseException:
                os.close(owner)
                raise

    def close(self):
        with self._lock:
            if self._owner is not None:
                try:
                    with closing(self.connect()) as db, base.transaction(db):
                        db.execute("UPDATE cache_registry_sessions SET state='STALE' WHERE controller_epoch=? AND state='ACTIVE'",
                                   (self.controller_epoch,))
                finally:
                    os.close(self._owner); self._owner = None
                    self._contexts.clear(); self._pending.clear(); self._accepted.clear()

    def _ready(self, worker=None):
        if self._owner is None: raise p.InventoryError('Inventory owner not started')
        if worker is not None and worker not in self.credentials:
            raise p.AuthenticationError('Worker not in configured credential inventory')

    def enroll(self, worker):
        """Operator-only API, never exposed over HTTP; rotation invalidates sessions."""
        with self._lock:
            self._ready(worker)
            now = self.clock(); credential = self.credentials[worker]
            with closing(self.connect()) as db:
                schema.require_schema(db)
                with base.transaction(db):
                    db.execute('''INSERT INTO cache_registry_workers
                        (worker_id,auth_principal,auth_state,enabled,created_at_ns,updated_at_ns)
                        VALUES (?,?,'ENROLLED',1,?,?) ON CONFLICT(worker_id) DO UPDATE SET
                        auth_principal=excluded.auth_principal,auth_state='ENROLLED',enabled=1,
                        updated_at_ns=excluded.updated_at_ns''', (worker, credential.principal, now, now))

    def _eligible(self, db, worker, now, *, session_id=None):
        return db.execute('''SELECT s.session_id FROM cache_registry_sessions s
            JOIN cache_registry_workers w USING(worker_id)
            JOIN cache_registry_identities i ON i.cache_id=s.cache_id AND i.worker_id=s.worker_id
            WHERE s.worker_id=? AND s.controller_epoch=? AND s.state='ACTIVE'
            AND (? IS NULL OR s.session_id=?)
            AND w.enabled=1 AND w.auth_state='ENROLLED' AND w.auth_principal=?
            AND s.auth_principal=w.auth_principal AND i.retired=0 AND i.epoch_high_water=s.cache_epoch
            AND s.has_full_snapshot=1 AND s.fresh_until_ns>? AND s.inventory_updated_at_ns<=?
            AND s.last_seen_ns BETWEEN ? AND ? AND w.last_seen_ns BETWEEN ? AND ?''',
            (worker, self.controller_epoch, session_id, session_id, self.credentials[worker].principal, now, now,
             now-self.heartbeat_ns, now, now-self.heartbeat_ns, now)).fetchone() is not None

    def eligible(self, worker):
        """Advisory live/fresh/authenticated inventory only; not resource/lease eligibility."""
        with self._lock:
            self._ready(worker)
            if worker not in self._contexts or self._contexts[worker]['sequence'] == 0:
                return False
            try:
                with closing(self.connect()) as db, base.transaction(db, write=False):
                    return self._eligible(db, worker, self.clock())
            except (OSError, sqlite3.Error, base.RegistryError):
                return False

    def offline(self, worker):
        with self._lock:
            self._ready(worker)
            # Invalidate in-memory admission even if a full/read-only/unavailable
            # disk prevents recording the durable STALE transition right now.
            self._contexts.pop(worker, None); self._pending.pop(worker, None); self._accepted.pop(worker, None)
            with closing(self.connect()) as db, base.transaction(db):
                db.execute("UPDATE cache_registry_sessions SET state='STALE' WHERE worker_id=? AND state='ACTIVE'",
                           (worker,))

    def expire(self):
        """Periodic reconciliation of freshness even when no HTTP report arrives."""
        with self._lock:
            self._ready()
            for worker in self.credentials:
                if not self.eligible(worker): self.offline(worker)

    def challenge(self, worker):
        with self._lock:
            self._ready(worker)
            if worker in self._contexts and self._contexts[worker]['sequence'] > 0 and not self.eligible(worker):
                self.offline(worker)
            context = self._contexts.setdefault(worker, dict(session_id=secrets.token_hex(16), sequence=0))
            now = self.clock()
            full = context['sequence']==0 or now-context.get('full_at',0) >= self.reconcile_ns
            raw = p.encode(dict(version=1, worker_id=worker, session_id=context['session_id'],
                controller_epoch=self.controller_epoch, nonce=secrets.token_hex(16), sequence=context['sequence'],
                full=full, sent_at_ns=now))
            self._pending[worker] = raw
            credential = self.credentials[worker]
            return raw, {'X-DL-Worker-Key':credential.key_id,
                         'X-DL-Inventory-MAC':p.mac(credential, 'challenge', raw), 'Content-Type':'application/json'}

    def accept(self, worker, request, raw, signature):
        """Verify exact pending challenge + signature before any trusted DB update."""
        with self._lock:
            self._ready(worker)
            if self._pending.get(worker) != request:
                raise p.AuthenticationError('No matching outstanding inventory challenge')
            credential = self.credentials[worker]
            p.authenticate(credential, 'report', raw, signature, request=request)
            challenge, report = p.challenge(request), p.report(raw)
            now, digest = self.clock(), hashlib.sha256(raw).hexdigest()
            if not 0 <= now-challenge['sent_at_ns'] <= 10_000_000_000:
                raise p.AuthenticationError('Inventory response deadline expired')
            if self._accepted.get(worker) == (request, digest):
                return 'duplicate'  # No timestamp, disk, sequence or TTL update.
            if any(report[key] != challenge[key] for key in ('worker_id','session_id','controller_epoch')):
                raise p.AuthenticationError('Inventory identity/session mismatch')
            sequence, kind = report['sequence'], report['kind']
            if (report['base_sequence'] != challenge['sequence']
                    or sequence != challenge['sequence'] + int(kind != 'HEARTBEAT')
                    or (challenge['full'] and kind != 'FULL')):
                raise p.InventoryError('Out-of-order inventory or missing full snapshot')
            with closing(self.connect()) as db:
                schema.require_schema(db)
                with base.transaction(db):
                    enrolled = db.execute('''SELECT 1 FROM cache_registry_workers WHERE worker_id=?
                        AND enabled=1 AND auth_state='ENROLLED' AND auth_principal=?''',
                        (worker, credential.principal)).fetchone()
                    if not enrolled: raise p.AuthenticationError('Worker revoked or not operator-enrolled')
                    self._apply(db, worker, report, digest, now)
            self._contexts[worker]['sequence'] = sequence
            if kind == 'FULL': self._contexts[worker]['full_at'] = now
            self._accepted[worker] = request, digest
            return kind

    def _apply(self, db, worker, report, digest, now):
        sid, cache_id, epoch = report['session_id'], report['cache_id'], report['cache_epoch']
        kind, sequence = report['kind'], report['sequence']
        identity = db.execute('SELECT worker_id,epoch_high_water,retired FROM cache_registry_identities WHERE cache_id=?',
                              (cache_id,)).fetchone()
        if identity and (identity[0] != worker or identity[2] or epoch < identity[1]):
            raise p.AuthenticationError('Retired, spoofed or stale cache epoch')
        session = db.execute('''SELECT cache_id,cache_epoch,worker_boot_id,last_sequence,state,fresh_until_ns
            FROM cache_registry_sessions WHERE session_id=?''', (sid,)).fetchone()
        if session:
            if (tuple(session[:3]) != (cache_id, epoch, report['boot_id']) or session[3] != report['base_sequence']
                    or session[4] != 'ACTIVE' or not self._eligible(db, worker, now)):
                raise p.InventoryError('Stale inventory session; authenticated full reconnect required')
        elif kind != 'FULL' or sequence != 1:
            raise p.InventoryError('First report must be a full snapshot')
        for entry in report['entries']:
            if not db.execute('SELECT 1 FROM storage_artifacts WHERE sha256=? AND size_bytes=?',
                              (entry['sha256'],entry['size_bytes'])).fetchone():
                raise p.InventoryError('Unknown artifact or conflicting size; reconcile controller registry')
            if kind == 'CACHE_REMOVED' and not db.execute('SELECT 1 FROM cache_registry_entries WHERE session_id=? AND sha256=?',
                                                         (sid,entry['sha256'])).fetchone():
                raise p.InventoryError('Cannot remove unknown inventory entry')
        if not identity:
            db.execute('INSERT INTO cache_registry_identities VALUES (?,?,?,0,?,?)', (cache_id,worker,epoch,now,now))
        elif epoch > identity[1]:
            db.execute('UPDATE cache_registry_identities SET epoch_high_water=?,updated_at_ns=? WHERE cache_id=?',
                       (epoch,now,cache_id))
        principal = self.credentials[worker].principal
        if session is None:
            db.execute("UPDATE cache_registry_sessions SET state='STALE' WHERE worker_id=? AND state='ACTIVE'", (worker,))
            db.execute('''INSERT INTO cache_registry_sessions
                (session_id,worker_id,cache_id,cache_epoch,worker_boot_id,controller_epoch,
                 auth_principal,authenticated_at_ns,created_at_ns,updated_at_ns) VALUES (?,?,?,?,?,?,?,?,?,?)''',
                (sid,worker,cache_id,epoch,report['boot_id'],self.controller_epoch,principal,now,now,now))
        db.execute('UPDATE cache_registry_workers SET last_seen_ns=?,updated_at_ns=? WHERE worker_id=?', (now,now,worker))
        disk = report['disk']
        if kind == 'HEARTBEAT':
            db.execute('''UPDATE cache_registry_sessions SET last_seen_ns=?,updated_at_ns=?,disk_total_bytes=?,
                disk_free_bytes=?,disk_available_bytes=? WHERE session_id=?''',
                (now,now,disk['total'],disk['free'],disk['available'],sid))
            return
        db.execute('''UPDATE cache_registry_sessions SET state='ACTIVE',last_sequence=?,last_body_sha256=?,
            has_full_snapshot=1,inventory_updated_at_ns=?,fresh_until_ns=?,last_seen_ns=?,updated_at_ns=?,
            disk_total_bytes=?,disk_free_bytes=?,disk_available_bytes=? WHERE session_id=?''',
            (sequence,digest,now,now+self.freshness_ns,now,now,disk['total'],disk['free'],disk['available'],sid))
        if kind == 'FULL':
            db.execute('''UPDATE cache_registry_entries SET state='REMOVED',verified_bytes=0,
                inventory_sequence=?,updated_at_ns=? WHERE session_id=?''', (sequence,now,sid))
        for entry in report['entries']:
            removed = kind == 'CACHE_REMOVED'
            db.execute('''INSERT INTO cache_registry_entries
                (session_id,worker_id,cache_id,cache_epoch,sha256,size_bytes,state,verified_bytes,
                 inventory_sequence,last_verified_at_ns,updated_at_ns) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(session_id,sha256) DO UPDATE SET state=excluded.state,
                verified_bytes=excluded.verified_bytes,inventory_sequence=excluded.inventory_sequence,
                last_verified_at_ns=excluded.last_verified_at_ns,updated_at_ns=excluded.updated_at_ns''',
                (sid,worker,cache_id,epoch,entry['sha256'],entry['size_bytes'],'REMOVED' if removed else 'VERIFIED',
                 0 if removed else entry['size_bytes'],sequence,entry['last_verified_at_ns'],now))
        db.execute('INSERT INTO cache_registry_events VALUES (?,?,?,?,?,?,?)',
                   (sid,worker,sequence,kind,None if kind=='FULL' else report['base_sequence'],digest,now))
