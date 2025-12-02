"""
DataSrc caching mechanism.

Provides a cache interface and an async pool with caching support.
The pool manages concurrent async operations with a semaphore and
deduplicates requests for the same cache key.
"""

from abc import ABC, abstractmethod
from typing import Any, Callable, Awaitable
import asyncio


class DataSrcCacheIface(ABC):
    """
    Abstract interface for caching DataSrc results.

    Implement this interface to provide different cache backends
    (e.g., memory, Redis, file-based, etc.).
    """

    @abstractmethod
    async def get(self, key: str) -> str | None:
        """
        Get cached value by key.

        Args:
            key: Cache key

        Returns:
            Cached string value or None if not found (or expired)
        """
        raise NotImplementedError

    @abstractmethod
    async def set(self, key: str, value: str, expire: int | None = None) -> None:
        """
        Set cache value for key with optional expiration.

        Args:
            key: Cache key
            value: String value to cache
            expire: Time-to-live in seconds (integer). None means no expiration.
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
        fetcher: Callable[[], Awaitable[str | None]],
        parser: Callable[[str], Any | None],
        expire: int | None = None
    ) -> Any | None:
        """
        Get value from cache or fetch using the provided callable.

        First checks cache for the key. If not found, runs the fetcher
        in the pool with concurrency control. If fetcher returns a non-None
        value, it's passed to the parser. Only when both fetcher and parser
        return non-None values, the raw fetched value is cached and parser's
        output is returned. If either returns None, returns None without caching.

        Deduplication: If multiple callers request the same key before
        the first fetch completes, they all wait for the same result.

        Args:
            key: Cache key
            fetcher: Async callable (lambda) that fetches the string data if not cached
            parser: Callable that parses fetched string data, returns Any or None
            expire: Time-to-live in seconds (integer) for cached value. None means no expiration.

        Returns:
            Parsed value from cache or fetcher, or None if fetch/parse returns None
        """
        # Check cache first (fast path, no lock)
        cached = await self._cache.get(key)
        if cached is not None:
            return parser(cached)

        async with self._lock:
            # Double check cache after acquiring lock
            cached = await self._cache.get(key)
            if cached is not None:
                return parser(cached)

            # Check if already pending (deduplication)
            if key in self._pending:
                task = self._pending[key]
            else:
                # Create task for this key
                task = asyncio.create_task(self._fetch_and_cache(key, fetcher, parser, expire))
                self._pending[key] = task

        # Wait for result outside the lock
        return await task

    async def _fetch_and_cache(
        self,
        key: str,
        fetcher: Callable[[], Awaitable[str | None]],
        parser: Callable[[str], Any | None],
        expire: int | None = None
    ) -> Any | None:
        """
        Fetch data using the fetcher, parse it, and cache if both are not None.

        Args:
            key: Cache key
            fetcher: Async callable that fetches the string data
            parser: Callable that parses fetched string data, returns Any or None
            expire: Time-to-live in seconds (integer) for cached value. None means no expiration.

        Returns:
            Parsed value or None
        """
        try:
            async with self._semaphore:
                result = await fetcher()
                if result is not None:
                    parsed = parser(result)
                    if parsed is not None:
                        await self._cache.set(key, result, expire)
                        return parsed
                return None
        finally:
            # Clean up pending entry after task completes
            async with self._lock:
                self._pending.pop(key, None)
