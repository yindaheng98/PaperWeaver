"""
Redis implementation for committed link tracking.
"""

from redis.asyncio import Redis

from ..link_storage import CommittedLinkStorageIface


class RedisCommittedLinkStorage(CommittedLinkStorageIface):
    """Redis storage for committed links using sets."""

    def __init__(self, redis_client: Redis, prefix: str = "committed", expire: int | None = None):
        """
        Initialize Redis committed link storage.

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

    async def commit_link(self, from_id: str, to_id: str) -> None:
        key = self._key(from_id)
        await self._redis.sadd(key, to_id)
        if self._expire is not None:
            await self._redis.expire(key, self._expire)

    async def is_link_committed(self, from_id: str, to_id: str) -> bool:
        return await self._redis.sismember(self._key(from_id), to_id)
