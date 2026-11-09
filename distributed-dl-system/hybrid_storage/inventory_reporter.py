"""Trusted worker inventory responder; explicit lifecycle, no background threads.

Do not install in a service whose UID/files/environment student code can access.
Persistent identity/epoch is separate from the existing v1 cache DB/schema.
"""

import fcntl
import os
from pathlib import Path
import secrets
import threading
import time

from . import inventory_protocol as protocol
from .worker_cache import CacheMiss, _child, _private_file


class InventoryReporter:
    def __init__(self, cache, credential, *, full_interval_seconds=30, clock=time.time_ns,
                 monotonic=time.monotonic, transfer_metrics=None, nfs_health=None):
        if type(full_interval_seconds) not in (int, float) or not 1 <= full_interval_seconds <= 300:
            raise ValueError('Full reconciliation interval must be 1..300 seconds')
        self.cache, self.credential = cache, credential
        if transfer_metrics is not None:
            from .transfer_metrics import TransferMetrics
            if not isinstance(transfer_metrics, TransferMetrics) or transfer_metrics.worker != credential.worker:
                raise ValueError('Transfer collector must match authenticated worker')
        self.transfer_metrics = transfer_metrics
        if nfs_health is not None and not callable(nfs_health):raise TypeError('Expected trusted health probe')
        self.nfs_health=nfs_health
        self.clock, self.monotonic = clock, monotonic
        self.full_interval = full_interval_seconds
        self._lock = threading.Lock()
        self._owner = None
        self._context = None
        self._last_sent = 0
        self._previous = {}
        self._sequence = 0
        self._last_raw = None
        self._reply_cache = None
        self._force_full = False

    def initialize(self, *, boot_id=None):
        """Call once per trusted process startup; persist strictly newer epoch."""
        if self._owner is not None:
            raise protocol.InventoryError('Reporter already initialized')
        boot_id = boot_id or Path('/proc/sys/kernel/random/boot_id').read_text().strip()
        # Validate before changing durable state.
        import re
        if not re.fullmatch(r'[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}', boot_id):
            raise protocol.InventoryError('Invalid kernel boot identity')
        with self.cache._session() as (_, __, root), _child(root, 'state') as state:
            owner = _private_file(state, 'inventory.lock', create=True)
            try:
                fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
                try:
                    old_fd = _private_file(state, 'inventory-identity.json', create=False)
                except FileNotFoundError:
                    identity = dict(cache_id=secrets.token_hex(16), epoch=0)
                else:
                    try:
                        if os.fstat(old_fd).st_size > 1024:
                            raise protocol.InventoryError('Invalid durable inventory identity')
                        identity = protocol.fields(protocol.decode(os.read(old_fd, 1025)), 'cache_id epoch')
                        protocol.hex_id(identity['cache_id']); protocol.integer(identity['epoch'])
                    finally:
                        os.close(old_fd)
                identity['epoch'] += 1
                protocol.integer(identity['epoch'], 1)
                name = '.inventory-' + secrets.token_hex(16) + '.tmp'
                fd = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=state)
                try:
                    with os.fdopen(fd, 'wb') as stream:
                        stream.write(protocol.encode(identity)); stream.flush(); os.fsync(stream.fileno())
                    os.replace(name, 'inventory-identity.json', src_dir_fd=state, dst_dir_fd=state)
                    os.fsync(state)
                finally:
                    # Only this call's unique subsystem-owned temporary file.
                    try: os.unlink(name, dir_fd=state)
                    except FileNotFoundError: pass
                self.cache_id, self.epoch, self.boot_id = identity['cache_id'], identity['epoch'], boot_id
                self._context, self._last_sent, self._previous, self._sequence = None, 0, {}, 0
                self._last_raw, self._reply_cache = None, None
                self._owner = owner
            except BaseException:
                os.close(owner)
                raise

    def close(self):
        with self._lock:
            if self._owner is not None:
                os.close(self._owner)
                self._owner = None

    def request_full(self):
        """Trusted local maintenance notification; next fresh challenge is FULL.

        Existing cached/lost replies retain their original sequence/body. No
        network push or deletion acknowledgment is fabricated here.
        """
        with self._lock:
            if self._owner is None:raise protocol.InventoryError('Reporter not initialized')
            self._force_full=True

    def _scan(self, *, full):
        records = self.cache.inventory()
        if len(records) > protocol.MAX_ENTRIES:
            raise protocol.InventoryError('Cache inventory exceeds bounded reporting limit')
        entries = {}
        for record in records:
            try:
                verified = self.cache.verify(record.sha256, record.size_bytes, force=full)
            except (CacheMiss, PermissionError):
                continue
            entries[record.sha256] = dict(sha256=record.sha256, size_bytes=record.size_bytes,
                                         last_verified_at_ns=verified.last_verified_ns)
        return entries

    def reply(self, raw, signature, key_id):
        if key_id != self.credential.key_id:
            raise protocol.AuthenticationError('Unknown worker credential')
        protocol.authenticate(self.credential, 'challenge', raw, signature)
        request = protocol.challenge(raw)
        if request['worker_id'] != self.credential.worker:
            raise protocol.AuthenticationError('Worker identity mismatch')
        if abs(self.clock() - request['sent_at_ns']) > 30_000_000_000:
            raise protocol.AuthenticationError('Expired inventory challenge')
        if not self._lock.acquire(timeout=0.1):
            raise protocol.InventoryError('Inventory verification busy')
        try:
            if self._owner is None:
                raise protocol.InventoryError('Reporter not initialized')
            if self._reply_cache is not None and raw == self._reply_cache[0]:
                return self._reply_cache[1:]
            if request['sent_at_ns'] <= self._last_sent:
                raise protocol.AuthenticationError('Out-of-order inventory challenge')
            context = (request['controller_epoch'], request['session_id'])
            new = context != self._context
            if new and (request['sequence'] != 0 or not request['full']):
                raise protocol.InventoryError('Reconnect requires a full snapshot')
            if not new and request['sequence'] == self._sequence - 1 and self._last_raw is not None:
                response = self._last_raw  # Lost response retry, same body and sequence.
            else:
                if not new and request['sequence'] != self._sequence:
                    raise protocol.InventoryError('Inventory cursor mismatch; reconnect')
                full = new or request['full'] or self._force_full or self.monotonic() - self._full_at >= self.full_interval
                current = self._scan(full=full)
                previous = {} if new else self._previous
                removed = sorted(set(previous) - set(current))
                added = sorted(sha for sha in current if sha not in previous)
                if full:
                    kind, entries = 'FULL', list(current.values())
                elif removed:
                    kind, entries = 'CACHE_REMOVED', [previous[sha] for sha in removed]
                elif added:
                    kind, entries = 'CACHE_ADDED', [current[sha] for sha in added]
                else:
                    kind, entries = 'HEARTBEAT', []
                base = request['sequence']
                sequence = base if kind == 'HEARTBEAT' else base + 1
                space = self.cache.space()
                document = dict(version=1, worker_id=self.credential.worker,
                    session_id=request['session_id'], controller_epoch=request['controller_epoch'],
                    cache_id=self.cache_id, cache_epoch=self.epoch, boot_id=self.boot_id,
                    sequence=sequence, base_sequence=base, kind=kind, entries=entries,
                    disk=dict(total=space.total_bytes, free=space.free_bytes, available=space.available_bytes))
                if self.transfer_metrics is not None:
                    try:
                        transfer = self.transfer_metrics.prediction_snapshot()
                        transfer['generated_at_ns'] = self.clock()
                    except Exception:
                        transfer = None  # Optional measurements cannot break cache inventory.
                    document.update(version=2, transfer=transfer)
                if self.nfs_health is not None:
                    try:
                        from .admin_observations import health
                        nfs=health(self.nfs_health(),self.clock())
                    except Exception:nfs=None
                    document.update(version=3,transfer=document.get('transfer'),nfs=nfs)
                response = protocol.encode(document)
                protocol.report(response)
                self._context, self._sequence = context, sequence
                if full:
                    self._previous, self._full_at = current, self.monotonic()
                    self._force_full=False
                elif kind == 'CACHE_REMOVED':
                    for sha in removed: previous.pop(sha)
                    self._previous = previous
                elif kind == 'CACHE_ADDED':
                    previous.update({sha: current[sha] for sha in added})
                    self._previous = previous
                self._last_raw = response
            self._last_sent = request['sent_at_ns']
            signature = protocol.mac(self.credential, 'report', response, request=raw)
            self._reply_cache = raw, response, signature
            return response, signature
        finally:
            self._lock.release()
