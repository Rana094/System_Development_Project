"""Numeric-only, advisory measurements of one successful staging RPC.

No paths, hashes, credentials or worker supplied text enter the student view.
These observations never grant execution authority or represent wire throughput.
"""

from dataclasses import fields

from .metrics import MetricsSnapshot

NAMES = tuple(field.name for field in fields(MetricsSnapshot))
MAX_VALUE = 2**63 - 1


def validated(document, *, required_bytes=None):
    """Discard absent/malformed telemetry without weakening the receipt gate."""
    if (type(document) is not dict or set(document) != {'schema_version', *NAMES}
            or type(document['schema_version']) is not int or document['schema_version'] != 1
            or any(type(document[name]) is not int or not 0 <= document[name] <= MAX_VALUE
                   for name in NAMES)):
        return None
    if required_bytes is not None and document['cache_hit_bytes'] + document['staged_bytes'] != required_bytes:
        return None
    return dict(document)


def measured(snapshot, results):
    document = snapshot.document()
    # Unique fetched digests, not manifest paths; retries may repeat verification.
    document['cache_hit_bytes'] = sum(result.size_bytes for result in results if result.cache_hit)
    document['staged_bytes'] = sum(result.size_bytes for result in results if not result.cache_hit)
    return validated(document)
