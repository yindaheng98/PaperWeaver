"""
Identifier Registry - Core component for managing object identity.

Two objects with any common identifier are considered the same object.
When objects are merged, their identifier sets are combined.
"""

import asyncio
from typing import AsyncIterator

from redis.asyncio import Redis

from ..identifier import IdentifierRegistryIface


class RedisIdentifierRegistry(IdentifierRegistryIface):
    """Redis implementation of identifier registry."""

    def __init__(self, redis_client: Redis, prefix: str = "idreg", expire: int | None = None):
        """
        Initialize Redis identifier registry.

        Args:
            redis_client: Redis async client
            prefix: Key prefix for Redis keys
            expire: TTL in seconds for keys, None means no expiration
        """
        self._redis = redis_client
        self._prefix = prefix
        self._expire = expire
        self._lock = asyncio.Lock()

    def _ident_key(self, identifier: str) -> str:
        return f"{self._prefix}:ident:{identifier}"

    def _canonical_key(self, canonical_id: str) -> str:
        return f"{self._prefix}:canonical:{canonical_id}"

    def _counter_key(self) -> str:
        return f"{self._prefix}:counter"

    def _all_canonicals_key(self) -> str:
        return f"{self._prefix}:all_canonicals"

    async def get_canonical_id(self, identifiers: set[str]) -> str | None:
        for ident in identifiers:
            result = await self._redis.get(self._ident_key(ident))
            if result:
                return result.decode() if isinstance(result, bytes) else result
        return None

    async def register(self, identifiers: set[str]) -> str:
        async with self._lock:
            # Find all existing canonical IDs
            existing_canonical_ids = set()
            for ident in identifiers:
                result = await self._redis.get(self._ident_key(ident))
                if result:
                    cid = result.decode() if isinstance(result, bytes) else result
                    existing_canonical_ids.add(cid)

            if not existing_canonical_ids:
                # Create new canonical ID
                counter = await self._redis.incr(self._counter_key())
                canonical_id = f"id_{counter}"

                # Store all identifiers
                pipe = self._redis.pipeline()
                for ident in identifiers:
                    if self._expire is not None:
                        pipe.set(self._ident_key(ident), canonical_id, ex=self._expire)
                    else:
                        pipe.set(self._ident_key(ident), canonical_id)
                pipe.sadd(self._canonical_key(canonical_id), *identifiers)
                if self._expire is not None:
                    pipe.expire(self._canonical_key(canonical_id), self._expire)
                pipe.sadd(self._all_canonicals_key(), canonical_id)
                if self._expire is not None:
                    pipe.expire(self._all_canonicals_key(), self._expire)
                await pipe.execute()
                return canonical_id

            # Merge into primary canonical
            canonical_ids_list = list(existing_canonical_ids)
            primary_canonical = canonical_ids_list[0]

            # Collect all identifiers
            all_identifiers = set(identifiers)
            for cid in canonical_ids_list:
                members = await self._redis.smembers(self._canonical_key(cid))
                for m in members:
                    all_identifiers.add(m.decode() if isinstance(m, bytes) else m)

            # Update mappings
            pipe = self._redis.pipeline()
            for ident in all_identifiers:
                if self._expire is not None:
                    pipe.set(self._ident_key(ident), primary_canonical, ex=self._expire)
                else:
                    pipe.set(self._ident_key(ident), primary_canonical)
            pipe.delete(self._canonical_key(primary_canonical))
            pipe.sadd(self._canonical_key(primary_canonical), *all_identifiers)
            if self._expire is not None:
                pipe.expire(self._canonical_key(primary_canonical), self._expire)

            # Remove merged canonical IDs
            for cid in canonical_ids_list[1:]:
                pipe.delete(self._canonical_key(cid))
                pipe.srem(self._all_canonicals_key(), cid)

            if self._expire is not None:
                pipe.expire(self._all_canonicals_key(), self._expire)

            await pipe.execute()
            return primary_canonical

    async def get_all_identifiers(self, canonical_id: str) -> set[str]:
        members = await self._redis.smembers(self._canonical_key(canonical_id))
        return {m.decode() if isinstance(m, bytes) else m for m in members}

    async def iterate_canonical_ids(self) -> AsyncIterator[str]:
        members = await self._redis.smembers(self._all_canonicals_key())
        for m in members:
            yield m.decode() if isinstance(m, bytes) else m
