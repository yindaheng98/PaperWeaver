"""
DataSrc caching mechanism.

Provides a cache interface and an async pool with caching support.
The pool manages concurrent async operations with a semaphore and
deduplicates requests for the same cache key.
"""

from abc import ABC, abstractmethod
from typing import TypeVar, Callable, Awaitable, Any
import asyncio


T = TypeVar('T')


class DataSrcCacheIface(ABC):
    """
    Abstract interface for caching DataSrc results.

    Implement this interface to provide different cache backends
    (e.g., memory, Redis, file-based, etc.).
    """

    @abstractmethod
    async def get(self, key: str) -> Any | None:
        """
        Get cached value by key.

        Args:
            key: Cache key

        Returns:
            Cached value or None if not found
        """
        raise NotImplementedError

    @abstractmethod
    async def set(self, key: str, value: Any) -> None:
        """
        Set cache value for key.

        Args:
            key: Cache key
            value: Value to cache
        """
        raise NotImplementedError


class CachedAsyncPool:
    """
    Async pool with caching support for DataSrc operations.

    This pool manages concurrent async operations with a semaphore
    and deduplicates requests for the same cache key. When multiple
    callers request the same key simultaneously, only one fetch
    operation runs and all callers receive the same result.
    """

    def __init__(self, cache: DataSrcCacheIface, max_concurrent: int = 10):
        """
        Initialize the cached async pool.

        Args:
            cache: Cache implementation (any subclass of DataSrcCacheIface)
            max_concurrent: Maximum number of concurrent fetch operations
        """
        self._cache = cache
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._pending: dict[str, asyncio.Task] = {}
        self._lock = asyncio.Lock()

    async def get_or_fetch(
        self,
        key: str,
        fetcher: Callable[[], Awaitable[T]]
    ) -> T | None:
        """
        Get value from cache or fetch using the provided callable.

        First checks cache for the key. If not found, runs the fetcher
        in the pool with concurrency control. If fetcher returns a non-None
        value, it's cached before returning. If fetcher returns None,
        returns None without caching.

        Deduplication: If multiple callers request the same key before
        the first fetch completes, they all wait for the same result.

        Args:
            key: Cache key
            fetcher: Async callable (lambda) that fetches the data if not cached

        Returns:
            Cached or fetched value, or None if fetch returns None
        """
        # Check cache first (fast path, no lock)
        cached = await self._cache.get(key)
        if cached is not None:
            return cached

        async with self._lock:
            # Double check cache after acquiring lock
            cached = await self._cache.get(key)
            if cached is not None:
                return cached

            # Check if already pending (deduplication)
            if key in self._pending:
                task = self._pending[key]
            else:
                # Create task for this key
                task = asyncio.create_task(self._fetch_and_cache(key, fetcher))
                self._pending[key] = task

        # Wait for result outside the lock
        return await task

    async def _fetch_and_cache(
        self,
        key: str,
        fetcher: Callable[[], Awaitable[T]]
    ) -> T | None:
        """
        Fetch data using the fetcher and cache if not None.

        Args:
            key: Cache key
            fetcher: Async callable that fetches the data

        Returns:
            Fetched value or None
        """
        try:
            async with self._semaphore:
                result = await fetcher()
                if result is not None:
                    await self._cache.set(key, result)
                return result
        finally:
            # Clean up pending entry after task completes
            async with self._lock:
                self._pending.pop(key, None)
