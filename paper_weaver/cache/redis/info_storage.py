"""
Info Storage - Stores entity information (dict data).

Separated from relationship storage for flexible composition.
"""

import json

from redis.asyncio import Redis

from ..info_storage import InfoStorageIface


class RedisInfoStorage(InfoStorageIface):
    """Redis info storage."""

    def __init__(self, redis_client: Redis, prefix: str = "info", expire: int | None = None):
        """
        Initialize Redis info storage.

        Args:
            redis_client: Redis async client
            prefix: Key prefix for Redis keys
            expire: TTL in seconds for keys, None means no expiration
        """
        self._redis = redis_client
        self._prefix = prefix
        self._expire = expire

    def _key(self, canonical_id: str) -> str:
        return f"{self._prefix}:{canonical_id}"

    async def get_info(self, canonical_id: str) -> dict | None:
        result = await self._redis.get(self._key(canonical_id))
        if result is None:
            return None
        data = result.decode() if isinstance(result, bytes) else result
        return json.loads(data)

    async def set_info(self, canonical_id: str, info: dict) -> None:
        if self._expire is not None:
            await self._redis.set(self._key(canonical_id), json.dumps(info), ex=self._expire)
        else:
            await self._redis.set(self._key(canonical_id), json.dumps(info))
