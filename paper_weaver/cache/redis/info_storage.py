"""
Info Storage - Stores entity information (dict data).

Separated from relationship storage for flexible composition.
"""

import datetime
import json

from redis.asyncio import Redis

from ..info_storage import InfoStorageIface


class _TemporalEncoder(json.JSONEncoder):
    """Encodes datetime.datetime and datetime.date as tagged dicts."""

    def default(self, o):
        if isinstance(o, datetime.datetime):
            return {"__type": "datetime", "isoformat": o.isoformat()}
        if isinstance(o, datetime.date):
            return {"__type": "date", "isoformat": o.isoformat()}
        return super().default(o)


def _temporal_decoder_hook(d: dict):
    t = d.get("__type")
    if t == "datetime":
        return datetime.datetime.fromisoformat(d["isoformat"])
    if t == "date":
        return datetime.date.fromisoformat(d["isoformat"])
    return d


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
        return json.loads(data, object_hook=_temporal_decoder_hook)

    async def set_info(self, canonical_id: str, info: dict) -> None:
        payload = json.dumps(info, cls=_TemporalEncoder)
        if self._expire is not None:
            await self._redis.set(self._key(canonical_id), payload, ex=self._expire)
        else:
            await self._redis.set(self._key(canonical_id), payload)
