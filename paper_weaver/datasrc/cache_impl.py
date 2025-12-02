"""
DataSrc caching mechanism.

Provides a cache interface and an async pool with caching support.
The pool manages concurrent async operations with a semaphore and
deduplicates requests for the same cache key.
"""

from typing import Any
import asyncio
import time

from .cache import DataSrcCacheIface


class MemoryDataSrcCache(DataSrcCacheIface):
    """Simple in-memory cache implementation with expiration support."""

    def __init__(self):
        # Store tuples of (value, expire_at) where expire_at is None or timestamp
        self._data: dict[str, tuple[Any, float | None]] = {}
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> Any | None:
        async with self._lock:
            entry = self._data.get(key)
            if entry is None:
                return None
            
            value, expire_at = entry
            if expire_at is not None and time.monotonic() >= expire_at:
                # Entry has expired, remove it
                del self._data[key]
                return None
            
            return value

    async def set(self, key: str, value: Any, expire: float | None = None) -> None:
        async with self._lock:
            expire_at = None
            if expire is not None:
                expire_at = time.monotonic() + expire
            self._data[key] = (value, expire_at)
