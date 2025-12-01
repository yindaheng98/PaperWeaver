"""
Redis implementations for link tracking and pending entity lists.
"""

from typing import Set, Optional, List

from ..pending_storage import PendingListStorageIface


class RedisPendingListStorage(PendingListStorageIface):
    """Redis storage for pending entity lists using JSON."""

    def __init__(self, redis_client, prefix: str = "pending"):
        self._redis = redis_client
        self._prefix = prefix

    def _key(self, from_id: str) -> str:
        return f"{self._prefix}:{from_id}"

    async def get_pending_identifier_sets(self, from_id: str) -> Optional[List[Set[str]]]:
        import json
        result = await self._redis.get(self._key(from_id))
        if result is None:
            return None
        data = result.decode() if isinstance(result, bytes) else result
        items = json.loads(data)
        return [set(item) for item in items]

    async def set_pending_identifier_sets(self, from_id: str, items: List[Set[str]]) -> None:
        import json
        # Convert sets to lists for JSON serialization
        data = [list(s) for s in items]
        await self._redis.set(self._key(from_id), json.dumps(data))
