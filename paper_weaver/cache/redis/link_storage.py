"""
Redis implementations for link tracking and pending entity lists.
"""

from typing import Set, Optional, List

from ..link_storage import CommittedLinkStorageIface, PendingListStorageIface


class RedisCommittedLinkStorage(CommittedLinkStorageIface):
    """Redis storage for committed links using sets."""

    def __init__(self, redis_client, prefix: str = "committed"):
        self._redis = redis_client
        self._prefix = prefix

    def _key(self, from_id: str) -> str:
        return f"{self._prefix}:{from_id}"

    async def add_link(self, from_id: str, to_id: str) -> None:
        await self._redis.sadd(self._key(from_id), to_id)

    async def has_link(self, from_id: str, to_id: str) -> bool:
        return await self._redis.sismember(self._key(from_id), to_id)


class RedisPendingListStorage(PendingListStorageIface):
    """Redis storage for pending entity lists using JSON."""

    def __init__(self, redis_client, prefix: str = "pending"):
        self._redis = redis_client
        self._prefix = prefix

    def _key(self, from_id: str) -> str:
        return f"{self._prefix}:{from_id}"

    async def get_list(self, from_id: str) -> Optional[List[Set[str]]]:
        import json
        result = await self._redis.get(self._key(from_id))
        if result is None:
            return None
        data = result.decode() if isinstance(result, bytes) else result
        items = json.loads(data)
        return [set(item) for item in items]

    async def set_list(self, from_id: str, items: List[Set[str]]) -> None:
        import json
        # Convert sets to lists for JSON serialization
        data = [list(s) for s in items]
        await self._redis.set(self._key(from_id), json.dumps(data))
