"""
DataSrc caching mechanism.

Provides cache implementations for DataSrc operations.
Includes both in-memory and Redis-based caching with expiration support.
"""

import asyncio
import time

from .cache import DataSrcCacheIface


class MemoryDataSrcCache(DataSrcCacheIface):
    """Simple in-memory cache implementation with expiration support."""

    def __init__(self):
        # Store tuples of (value, expire_at) where expire_at is None or timestamp
        self._data: dict[str, tuple[str, float | None]] = {}
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> str | None:
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

    async def set(self, key: str, value: str, expire: int | None = None) -> None:
        async with self._lock:
            expire_at = None
            if expire is not None:
                expire_at = time.monotonic() + expire
            self._data[key] = (value, expire_at)


class RedisDataSrcCache(DataSrcCacheIface):
    """
    Redis-based cache implementation with expiration support.

    Uses Redis native TTL for expiration. Values are stored as strings directly.
    """

    def __init__(
        self,
        redis_client,
        prefix: str = "datasrc_cache",
        default_expire: int | None = None
    ):
        """
        Initialize Redis cache.

        Args:
            redis_client: Redis async client (redis.asyncio.Redis)
            prefix: Key prefix for Redis keys to avoid collisions
            default_expire: Default TTL in seconds for keys when not specified
                           in set(). None means no expiration by default.
        """
        self._redis = redis_client
        self._prefix = prefix
        self._default_expire = default_expire

    def _make_key(self, key: str) -> str:
        """Create Redis key with prefix."""
        return f"{self._prefix}:{key}"

    async def get(self, key: str) -> str | None:
        """
        Get cached value by key.

        Args:
            key: Cache key

        Returns:
            Cached string value or None if not found (or expired)
        """
        result = await self._redis.get(self._make_key(key))
        if result is None:
            return None
        # Redis returns bytes, decode to string
        return result.decode() if isinstance(result, bytes) else result

    async def set(self, key: str, value: str, expire: int | None = None) -> None:
        """
        Set cache value for key with optional expiration.

        Args:
            key: Cache key
            value: String value to cache
            expire: Time-to-live in seconds (integer). None uses default_expire.
                   If both are None, no expiration is set.
        """
        redis_key = self._make_key(key)

        # Determine expiration: use provided expire, fall back to default
        ttl = expire if expire is not None else self._default_expire

        if ttl is not None:
            if ttl <= 0:
                # For zero or negative TTL, delete immediately (expired)
                await self._redis.delete(redis_key)
            else:
                await self._redis.setex(redis_key, ttl, value)
        else:
            await self._redis.set(redis_key, value)
