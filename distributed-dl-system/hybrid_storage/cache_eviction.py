"""Explicit dry-run-first local cache maintenance; never automatic job-path GC.

Only unchanged, registered, unpinned replicas can be removed. Any persistent pin
blocks all eviction. Dedicated roots, upgraded trusted cache clients and isolation
from student code are prerequisites. No NFS/workspace/checkpoint traversal.
"""
from collections import OrderedDict
from contextlib import ExitStack, contextmanager
from dataclasses import asdict, dataclass
import os
from pathlib import Path
import secrets
import threading

from .backends import _root
from .cache_fetch import CacheFetcher, CacheFetchBusy
from .mount_identity import require_local_fd
from .worker_cache import (CacheError, CacheActivityBusy, CacheCorrupt, MAX_INTEGER,
                           _child, _signature)


class EvictionPlanStale(CacheError):
    """Re-plan and review; a stale or foreign plan is never deletion authority."""


@dataclass(frozen=True)
class EvictionCandidate:
    sha256: str
    size_bytes: int
    allocated_bytes: int
    last_used_ns: int | None
    fingerprint: str


@dataclass(frozen=True)
class EvictionPlan:
    token: str
    root_identity: tuple[int, int, int]
    max_cache_bytes: int
    reserve_free_bytes: int
    incoming_bytes: int
    used_bytes: int
    available_bytes: int
    pinned_references: int
    pinned_objects: int
    skipped_changed_or_missing: int
    status: str
    candidates: tuple[EvictionCandidate, ...]

    def document(self):
        return dict(schema_version=1, dry_run=True, **asdict(self),
                    selected_bytes=sum(c.size_bytes for c in self.candidates),
                    estimated_allocated_bytes=sum(c.allocated_bytes for c in self.candidates))


class CacheEvictor:
    """Trusted idle maintenance. Construct without I/O; initialize fetcher first.

    max_cache_bytes reuses fetcher.quota_bytes, including parts/quarantine; the
    free-space reserve is the same existing fetcher setting. No competing limit
    or global storage flag. At most eight in-process plans; no restart replay.
    notify may be an initialized InventoryReporter's request_full method. It is
    called AFTER releasing locks, including on partial failure, never on preview.
    """
    def __init__(self, fetcher, *, protected_roots=(), max_candidates=1024, notify=None):
        if not isinstance(fetcher, CacheFetcher):raise TypeError('Expected explicit CacheFetcher')
        if type(max_candidates) is not int or not 1 <= max_candidates <= 4096:
            raise ValueError('Candidate limit must be 1..4096')
        if notify is not None and not callable(notify):raise TypeError('Expected trusted notification callback')
        self.fetcher, self.cache = fetcher, fetcher.cache
        self.max_candidates, self.notify = max_candidates, notify
        self.protected_roots = tuple(_root(p) for p in protected_roots)
        for other in (self.cache.input_mount,*self.protected_roots):
            if self.cache.root.is_relative_to(other) or other.is_relative_to(self.cache.root):
                raise ValueError('Cache maintenance cannot overlap protected input/workspace/checkpoint roots')
        self._plans = OrderedDict()
        self._guard = threading.Lock()

    @contextmanager
    def _locked(self):
        with self.cache.activity(exclusive=True), ExitStack() as stack:
            try:root,objects,parts,quarantine,_ = self.fetcher._slot(stack)
            except CacheFetchBusy as error:raise CacheActivityBusy('A producer still owns this cache') from error
            identity = require_local_fd(root)['id']
            with _child(root,'state') as state:
                for fd in (objects,parts,quarantine,state):self._same_mount(fd,identity)
            yield root,objects,parts,quarantine,identity

    @staticmethod
    def _same_mount(fd, identity):
        if require_local_fd(fd)['id'] != identity:
            raise PermissionError('Cache maintenance refuses nested or replaced mounts')

    def _usage(self, objects, parts, quarantine):
        budget=[100000]
        identity=require_local_fd(objects)['id']
        return sum(self.fetcher._usage(fd,budget,mount_id=identity) for fd in (objects,parts,quarantine))

    @staticmethod
    def _same_root(actual, expected):
        if (os.fstat(actual).st_dev,os.fstat(actual).st_ino)!=(os.fstat(expected).st_dev,os.fstat(expected).st_ino):
            raise EvictionPlanStale('Cache root changed during maintenance')

    @staticmethod
    def _available(root):
        space=os.fstatvfs(root)
        return space.f_bavail*space.f_frsize

    @contextmanager
    def _candidate(self, objects, digest, identity):
        # Only typed DB digests enter this descriptor-relative path.
        from .inputs import object_key
        object_key(digest)
        with _child(objects,'sha256') as algorithm, _child(algorithm,digest[:2]) as prefix:
            self._same_mount(algorithm,identity);self._same_mount(prefix,identity)
            with self.cache._object(objects,digest) as stream:
                self._same_mount(stream.fileno(),identity)
                yield prefix,stream

    def _snapshot(self, opened, incoming, token):
        root,objects,parts,quarantine,identity=opened
        used=self._usage(objects,parts,quarantine);available=self._available(root)
        quota=self.fetcher.quota_bytes;reserve=self.fetcher.reserve_free_bytes
        with self.cache._session(read_only=True) as (db,_,dbroot):
            self._same_root(dbroot,root)
            pins=db.execute('SELECT COUNT(*),COUNT(DISTINCT sha256) FROM cache_pins').fetchone()
            rows=db.execute("SELECT sha256,size_bytes,last_used_ns,fingerprint FROM cache_objects WHERE state='verified' ORDER BY COALESCE(last_used_ns,0),sha256 LIMIT 100001").fetchall()
            if len(rows)>100000:raise CacheError('Maintenance inventory exceeds bounded scan')
            wanted_quota=max(0,used+incoming-quota)
            wanted_free=max(0,incoming+reserve-available)
            candidates=[];logical=allocated=skipped=0
            status='not_needed' if not (wanted_quota or wanted_free) else 'blocked_pins' if pins[0] else 'ready'
            if status=='ready':
                for digest,size,last_used,fingerprint in rows:
                    try:
                        with self._candidate(objects,digest,identity) as (_,stream):
                            info=os.fstat(stream.fileno())
                            if info.st_size!=size or _signature(info)!=fingerprint:
                                skipped+=1;continue
                            candidate=EvictionCandidate(digest,size,info.st_blocks*512,last_used,fingerprint)
                    except FileNotFoundError:
                        skipped+=1;continue
                    if not candidate.size_bytes and not candidate.allocated_bytes:continue
                    candidates.append(candidate);logical+=size;allocated+=candidate.allocated_bytes
                    if logical>=wanted_quota and allocated>=wanted_free:break
                    if len(candidates)>=self.max_candidates:break
                if logical<wanted_quota or allocated<wanted_free:status='insufficient_candidates'
        info=os.fstat(root)
        return EvictionPlan(token,(info.st_dev,info.st_ino,identity),quota,reserve,incoming,used,available,
                            pins[0],pins[1],skipped,status,tuple(candidates))

    def plan(self, *, incoming_bytes=0):
        """Read-only SQLite + OS inspection; no unlink, verification touch or notify.

        Allocated bytes are only an estimate (sparse/reflink/open-file effects).
        Apply remeasures space and never expands the reviewed victim set.
        """
        if type(incoming_bytes) is not int or not 0 <= incoming_bytes <= MAX_INTEGER:
            raise ValueError('Expected bounded incoming cache bytes')
        with self._locked() as opened:
            plan=self._snapshot(opened,incoming_bytes,secrets.token_hex(16))
        with self._guard:
            self._plans[plan.token]=plan
            while len(self._plans)>8:self._plans.popitem(last=False)
        return plan

    def _known(self, plan, *, consume=False):
        if not isinstance(plan,EvictionPlan):raise EvictionPlanStale('Expected a locally reviewed typed plan')
        with self._guard:
            if self._plans.get(plan.token)!=plan:raise EvictionPlanStale('Unknown, changed, consumed or pre-restart plan')
            if consume:self._plans.pop(plan.token)

    def _notify(self):
        if self.notify is None:return 'periodic_inventory_reconciliation'
        try:self.notify();return 'full_inventory_requested'
        except Exception:return 'notification_failed_periodic_reconciliation_required'

    @staticmethod
    def _unlink(prefix, digest):
        os.unlink(digest,dir_fd=prefix)
        os.fsync(prefix)

    def apply(self, plan, *, dry_run=True):
        """Default is preview. Explicit False consumes only this instance's plan.

        A durable missing observation precedes each unlink. Crash reconciliation
        observes actual bytes; it never replays deletion or drops pins. No remote
        action, recursive cleanup or unreviewed extra victim is possible here.
        """
        if type(dry_run) is not bool:raise ValueError('dry_run must be boolean')
        self._known(plan,consume=not dry_run)
        if dry_run:return plan.document()
        if plan.status not in ('ready','not_needed'):
            raise EvictionPlanStale('Plan is blocked or cannot meet its target; nothing evicted')
        changed=False
        try:
            with self._locked() as opened:
                root,objects,_,__,identity=opened
                current=self._snapshot(opened,plan.incoming_bytes,plan.token)
                if (current.root_identity!=plan.root_identity or current.max_cache_bytes!=plan.max_cache_bytes
                        or current.reserve_free_bytes!=plan.reserve_free_bytes or current.used_bytes!=plan.used_bytes
                        or current.status!=plan.status or current.pinned_references
                        or current.candidates!=plan.candidates):
                    raise EvictionPlanStale('Pins, budget, LRU, inode or pressure changed; review a new plan')
                removed=[]
                for candidate in plan.candidates:
                    with ExitStack() as retained:
                        with self.cache._session() as (db,_,dbroot):
                            self._same_root(dbroot,root)
                            if db.execute('SELECT 1 FROM cache_pins LIMIT 1').fetchone():
                                raise EvictionPlanStale('Persistent pin appeared; no further eviction')
                            row=db.execute('SELECT size_bytes,last_used_ns,fingerprint,state FROM cache_objects WHERE sha256=?',
                                           (candidate.sha256,)).fetchone()
                            if row!=(candidate.size_bytes,candidate.last_used_ns,candidate.fingerprint,'verified'):
                                raise EvictionPlanStale('Cache observation changed')
                            prefix,stream=retained.enter_context(self._candidate(objects,candidate.sha256,identity))
                            if _signature(os.fstat(stream.fileno()))!=candidate.fingerprint:
                                raise EvictionPlanStale('Candidate inode changed')
                            db.execute("UPDATE cache_objects SET state='missing',fingerprint=NULL,last_verified_ns=NULL WHERE sha256=?",
                                       (candidate.sha256,))
                        changed=True  # Committed intent BEFORE touching the directory entry.
                        actual=os.stat(candidate.sha256,dir_fd=prefix,follow_symlinks=False)
                        if _signature(actual)!=candidate.fingerprint:raise EvictionPlanStale('Candidate entry replaced')
                        self._unlink(prefix,candidate.sha256)
                    removed.append(candidate.sha256)
                    used=current.used_bytes-sum(c.size_bytes for c in plan.candidates[:len(removed)])
                    if used+plan.incoming_bytes<=plan.max_cache_bytes and self._available(root)>=plan.incoming_bytes+plan.reserve_free_bytes:
                        break
                used=self._usage(opened[1],opened[2],opened[3]);available=self._available(root)
                result=dict(dry_run=False,removed=removed,used_bytes=used,available_bytes=available,
                    status='satisfied' if used+plan.incoming_bytes<=plan.max_cache_bytes and available>=plan.incoming_bytes+plan.reserve_free_bytes
                           else 'insufficient_actual_space')
        finally:
            notice=self._notify() if changed else 'no_change'
        return dict(result,inventory_refresh=notice)

    def reconcile(self):
        """Explicit crash/startup audit of registered rows; NO deletions/unpins.

        Intent-before-unlink crash: retained file is reverified, never deleted on
        replay. Post-unlink crash: absent file stays missing. Unknown files remain
        untouched. Operator must arrange an idle maintenance interval for hashing.
        """
        counts=dict(verified=0,missing=0,corrupt=0);changed=False
        try:
            with self._locked() as opened:
                with self.cache._session(read_only=True) as (db,_,dbroot):
                    self._same_root(dbroot,opened[0])
                    rows=db.execute('SELECT sha256,size_bytes FROM cache_objects ORDER BY sha256 LIMIT 100001').fetchall()
                    if len(rows)>100000:raise CacheError('Reconciliation exceeds bounded scan')
                    pins_before=db.execute('SELECT COUNT(*) FROM cache_pins').fetchone()[0]
                for digest,size in rows:
                    with self.cache._session() as (db,_,dbroot):
                        self._same_root(dbroot,opened[0])
                        try:
                            with self._candidate(opened[1],digest,opened[4]) as (_,stream):
                                self.cache._check(db,stream,digest,size,force=True,touch=False)
                            state='verified'
                        except (OSError,CacheCorrupt) as error:
                            state='missing' if isinstance(error,FileNotFoundError) else 'corrupt'
                            db.execute('UPDATE cache_objects SET state=?,fingerprint=NULL,last_verified_ns=NULL WHERE sha256=?',
                                       (state,digest))
                    changed=True;counts[state]+=1
                with self.cache._session(read_only=True) as (db,_,__):
                    pins_after=db.execute('SELECT COUNT(*) FROM cache_pins').fetchone()[0]
        finally:
            notice=self._notify() if changed else 'no_change'
        return dict(counts,pins_before=pins_before,pins_after=pins_after,inventory_refresh=notice)
