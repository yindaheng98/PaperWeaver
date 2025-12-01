"""
Redis implementation for committed link tracking.
"""


from ..link_storage import CommittedLinkStorageIface


class RedisCommittedLinkStorage(CommittedLinkStorageIface):
    """Redis storage for committed links using sets."""

    def __init__(self, redis_client, prefix: str = "committed"):
        self._redis = redis_client
        self._prefix = prefix

    def _key(self, from_id: str) -> str:
        return f"{self._prefix}:{from_id}"

    async def commit_link(self, from_id: str, to_id: str) -> None:
        await self._redis.sadd(self._key(from_id), to_id)

    async def is_link_committed(self, from_id: str, to_id: str) -> bool:
        return await self._redis.sismember(self._key(from_id), to_id)
