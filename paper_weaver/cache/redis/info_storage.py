"""
Info Storage - Stores entity information (dict data).

Separated from relationship storage for flexible composition.
"""

import json

from ..info_storage import InfoStorageIface


class RedisInfoStorage(InfoStorageIface):
    """Redis info storage."""

    def __init__(self, redis_client, prefix: str = "info"):
        self._redis = redis_client
        self._prefix = prefix

    def _key(self, canonical_id: str) -> str:
        return f"{self._prefix}:{canonical_id}"

    async def get_info(self, canonical_id: str) -> dict | None:
        result = await self._redis.get(self._key(canonical_id))
        if result is None:
            return None
        data = result.decode() if isinstance(result, bytes) else result
        return json.loads(data)

    async def set_info(self, canonical_id: str, info: dict) -> None:
        await self._redis.set(self._key(canonical_id), json.dumps(info))

    async def has_info(self, canonical_id: str) -> bool:
        return await self._redis.exists(self._key(canonical_id)) > 0
