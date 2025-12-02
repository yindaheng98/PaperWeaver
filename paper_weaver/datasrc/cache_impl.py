"""
DataSrc caching mechanism.

Provides a cache interface and an async pool with caching support.
The pool manages concurrent async operations with a semaphore and
deduplicates requests for the same cache key.
"""

from typing import Any
import asyncio

from .cache import DataSrcCacheIface


class MemoryDataSrcCache(DataSrcCacheIface):
    """Simple in-memory cache implementation."""

    def __init__(self):
        self._data: dict[str, Any] = {}
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> Any | None:
        async with self._lock:
            return self._data.get(key)

    async def set(self, key: str, value: Any) -> None:
        async with self._lock:
            self._data[key] = value
