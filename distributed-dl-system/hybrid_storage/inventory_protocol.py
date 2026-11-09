"""Bounded authenticated metadata on the existing worker HTTP channel.

Keys are operator provisioned, unique per worker, never student/job settings.
HMAC authenticates metadata, not confidentiality or the legacy job endpoints.
"""

from dataclasses import dataclass, field
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import stat

from .backends import _directory
from .mount_identity import require_local_fd

PATH = '/resources/cache-inventory'
MAX_BYTES = 1024 * 1024
MAX_ENTRIES = 4096
MAX_INT = 2**63 - 1


class InventoryError(ValueError):
    """Invalid, stale or unsupported inventory; do not count it as a cache hit."""


class AuthenticationError(InventoryError):
    """No current proof for the configured worker identity."""


def worker_id(value):
    if type(value) is not str or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,62}', value) or value == 'local':
        raise InventoryError('Invalid configured worker identity')
    return value


def hex_id(value, length=32):
    if type(value) is not str or not re.fullmatch('[0-9a-f]{%d}' % length, value):
        raise InventoryError('Invalid hexadecimal identity')
    return value


def integer(value, minimum=0):
    if type(value) is not int or not minimum <= value <= MAX_INT:
        raise InventoryError('Invalid integer range')
    return value


def fields(value, required):
    if type(value) is not dict or set(value) != set(required.split()):
        raise InventoryError('Unexpected inventory fields')
    return value


def encode(value):
    raw = json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()
    if len(raw) > MAX_BYTES:
        raise InventoryError('Inventory limit exceeded; no partial inventory accepted')
    return raw


def decode(raw):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise InventoryError('Duplicate inventory field')
            result[key] = value
        return result
    if type(raw) is not bytes or not 0 < len(raw) <= MAX_BYTES:
        raise InventoryError('Invalid inventory size')
    try:
        return json.loads(raw.decode('utf-8'), object_pairs_hook=unique,
                          parse_constant=lambda _: (_ for _ in ()).throw(InventoryError('Nonfinite JSON')))
    except (ValueError, UnicodeError, RecursionError) as error:
        raise InventoryError('Invalid inventory JSON') from error


@dataclass(frozen=True)
class Credential:
    worker: str
    key_id: str
    secret: bytes = field(repr=False)

    def __post_init__(self):
        worker_id(self.worker); hex_id(self.key_id)
        if type(self.secret) is not bytes or len(self.secret) != 32:
            raise InventoryError('Expected a 256-bit worker key')

    @property
    def principal(self):
        return 'hmac-sha256:' + self.key_id


def load_credential(path: Path, *, expected_worker: str):
    """Explicit read only: local, owned, single-link 0600 file; no creation."""
    path = Path(path)
    if not path.is_absolute():
        raise InventoryError('Credential path must be operator-configured and absolute')
    with _directory(path.parent) as parent:
        parent_info = os.fstat(parent)
        if parent_info.st_uid != os.geteuid() or parent_info.st_mode & 0o077:
            raise PermissionError('Credential directory must be private and service-owned')
        fd = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
        try:
            info = os.fstat(fd)
            require_local_fd(fd)
            if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
                    or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1 or info.st_size > 2048):
                raise PermissionError('Worker credential must be private and service-owned')
            raw = os.read(fd, 2049)
        finally:
            os.close(fd)
    document = fields(decode(raw), 'worker_id key_id secret_hex')
    if document['worker_id'] != expected_worker:
        raise AuthenticationError('Credential worker mismatch')
    return Credential(expected_worker, document['key_id'], bytes.fromhex(hex_id(document['secret_hex'], 64)))


def mac(credential, purpose, raw, *, request=b''):
    context = b'dl-cache-inventory-v1\x00POST\x00' + PATH.encode() + b'\x00'
    context += credential.worker.encode() + b'\x00' + credential.key_id.encode() + b'\x00' + purpose.encode() + b'\x00'
    return hmac.new(credential.secret, context + hashlib.sha256(request).digest() + raw, hashlib.sha256).hexdigest()


def authenticate(credential, purpose, raw, signature, *, request=b''):
    if (type(raw) is not bytes or len(raw) > MAX_BYTES or type(signature) is not str
            or not re.fullmatch(r'[0-9a-f]{64}', signature)
            or not hmac.compare_digest(mac(credential, purpose, raw, request=request), signature)):
        raise AuthenticationError('Invalid worker inventory authentication')


def challenge(raw):
    value = fields(decode(raw), 'version worker_id session_id controller_epoch nonce sequence full sent_at_ns')
    if type(value['version']) is not int or value['version'] != 1 or type(value['full']) is not bool:
        raise InventoryError('Unsupported inventory challenge')
    worker_id(value['worker_id'])
    for name in ('session_id', 'controller_epoch', 'nonce'):
        hex_id(value[name])
    integer(value['sequence']); integer(value['sent_at_ns'], 1)
    return value


def report(raw):
    value = decode(raw)
    if type(value) is not dict or type(value.get('version')) is not int or value['version'] not in (1, 2, 3):
        raise InventoryError('Unsupported inventory version')
    fields(value, 'version worker_id session_id controller_epoch cache_id cache_epoch boot_id sequence base_sequence kind entries disk'
           + (' transfer' if value['version'] >= 2 else '') + (' nfs' if value['version'] == 3 else ''))
    # Version 2 carries optional advisory measurements under the SAME request-
    # bound HMAC. Malformed telemetry is discarded separately, not execution data.
    # Explicit opt-in v3 adds advisory NFS mount identity. v1/v2 remain unchanged.
    worker_id(value['worker_id'])
    for name in ('session_id', 'controller_epoch', 'cache_id'):
        hex_id(value[name])
    integer(value['cache_epoch'], 1); integer(value['sequence']); integer(value['base_sequence'])
    if type(value['boot_id']) is not str or not re.fullmatch(r'[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}', value['boot_id']):
        raise InventoryError('Invalid boot identity')
    if value['kind'] not in ('FULL', 'CACHE_ADDED', 'CACHE_REMOVED', 'HEARTBEAT'):
        raise InventoryError('Unknown inventory event')
    disk = fields(value['disk'], 'total free available')
    for count in disk.values(): integer(count)
    if not disk['available'] <= disk['free'] <= disk['total']:
        raise InventoryError('Invalid disk capacity')
    entries = value['entries']
    if type(entries) is not list or len(entries) > MAX_ENTRIES:
        raise InventoryError('Inventory entry limit exceeded')
    seen = set()
    for entry in entries:
        fields(entry, 'sha256 size_bytes last_verified_at_ns')
        hex_id(entry['sha256'], 64); integer(entry['size_bytes'])
        if entry['last_verified_at_ns'] is not None: integer(entry['last_verified_at_ns'], 1)
        if value['kind'] in ('FULL', 'CACHE_ADDED') and entry['last_verified_at_ns'] is None:
            raise InventoryError('Unverified object cannot be advertised')
        if entry['sha256'] in seen: raise InventoryError('Duplicate inventory digest')
        seen.add(entry['sha256'])
    if value['kind'] == 'HEARTBEAT' and entries:
        raise InventoryError('Heartbeat cannot change inventory')
    return value
