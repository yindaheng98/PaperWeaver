"""
Redis implementations for pending entity lists.
"""

import json

from redis.asyncio import Redis

from ..pending_storage import PendingListStorageIface


class RedisPendingListStorage(PendingListStorageIface):
    """Redis storage for pending entity lists using JSON."""

    def __init__(self, redis_client: Redis, prefix: str = "pending", expire: int | None = None):
        """
        Initialize Redis pending list storage.

        Args:
            redis_client: Redis async client
            prefix: Key prefix for Redis keys
            expire: TTL in seconds for keys, None means no expiration
        """
        self._redis = redis_client
        self._prefix = prefix
        self._expire = expire

    def _key(self, from_id: str) -> str:
        return f"{self._prefix}:{from_id}"

    async def get_pending_identifier_sets(self, from_id: str) -> list[set[str]] | None:
        result = await self._redis.get(self._key(from_id))
        if result is None:
            return None
        data = result.decode() if isinstance(result, bytes) else result
        items = json.loads(data)
        return [set(item) for item in items]

    async def set_pending_identifier_sets(self, from_id: str, items: list[set[str]]) -> None:
        # Convert sets to lists for JSON serialization
        data = [list(s) for s in items]
        if self._expire is not None:
            await self._redis.set(self._key(from_id), json.dumps(data), ex=self._expire)
        else:
            await self._redis.set(self._key(from_id), json.dumps(data))
