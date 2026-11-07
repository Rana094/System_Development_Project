"""Opt-in hybrid storage components; importing this package changes no runtime state."""

from .backends import LegacyStorageBackend, NFSStorageBackend, StorageBackend

__all__ = ["StorageBackend", "LegacyStorageBackend", "NFSStorageBackend"]
