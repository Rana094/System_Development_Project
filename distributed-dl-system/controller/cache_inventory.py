"""Opt-in authenticated metadata poll attached to the existing resource heartbeat.

No jobs, artifacts, checkpoint transfers or scheduler decisions occur here.
"""

import ipaddress
import sqlite3
import threading

import requests

from hybrid_storage import inventory_protocol as protocol
from hybrid_storage.config import StorageConfig


class InventoryHeartbeat:
    def __init__(self, registry, *, settings, transport=None):
        if not isinstance(settings, StorageConfig) or not settings.worker_cache_enabled:
            raise protocol.InventoryError('Inventory heartbeat requires explicit worker-cache opt-in')
        self.registry = registry
        self._locks = {worker:threading.Lock() for worker in registry.credentials}
        self.session = None
        if transport is None:
            self.session = requests.Session()
            self.session.trust_env = False
            transport = self._post
        self.transport = transport

    def _post(self, node, raw, headers):
        # Addresses come only from reviewed config/nodes.json, not worker reports.
        address = ipaddress.IPv4Address(node['ip'])
        if address.is_unspecified or address.is_multicast or type(node['port']) is not int or not 1 <= node['port'] <= 65535:
            raise protocol.InventoryError('Invalid configured worker endpoint')
        url = f'http://{address}:{node["port"]}' + protocol.PATH
        with self.session.post(url, data=raw, headers=headers, timeout=(1, 3),
                               allow_redirects=False, stream=True) as response:
            if response.status_code != 200:
                raise protocol.InventoryError('Inventory endpoint unavailable')
            data = bytearray()
            for chunk in response.iter_content(chunk_size=64*1024):
                data.extend(chunk)
                if len(data) > protocol.MAX_BYTES:
                    raise protocol.InventoryError('Inventory response exceeds limit')
            return bytes(data), response.headers.get('X-DL-Inventory-MAC', '')

    def poll(self, node):
        worker = node['id']
        if worker not in self._locks:
            return False  # An ordinary legacy worker is never auto-enrolled.
        if not self._locks[worker].acquire(blocking=False):
            return False
        try:
            raw, headers = self.registry.challenge(worker)
            response, signature = self.transport(node, raw, headers)
            self.registry.accept(worker, raw, response, signature)
            return self.registry.eligible(worker)
        except (protocol.InventoryError, requests.RequestException, OSError, ValueError, sqlite3.Error):
            try:
                self.registry.offline(worker)
            except (OSError, sqlite3.Error):
                pass  # DB unavailable: callers must treat unreadable inventory as ineligible.
            return False
        finally:
            self._locks[worker].release()

    def offline(self, worker):
        if worker in self._locks:
            try:
                self.registry.offline(worker)
            except (OSError, sqlite3.Error, ValueError):
                return False
        return True

    def reconcile(self):
        try:
            self.registry.expire()
            return True
        except (OSError, sqlite3.Error, ValueError):
            return False

    def close(self):
        if self.session is not None:
            self.session.close()
