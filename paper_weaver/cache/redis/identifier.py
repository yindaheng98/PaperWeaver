"""
Identifier Registry - Core component for managing object identity.

Two objects with any common identifier are considered the same object.
When objects are merged, their identifier sets are combined.
"""

from typing import Set, Optional
import asyncio

from ..identifier import IdentifierRegistryIface, T


class RedisIdentifierRegistry(IdentifierRegistryIface[T]):
    """Redis implementation of identifier registry."""

    def __init__(self, redis_client, prefix: str = "idreg"):
        self._redis = redis_client
        self._prefix = prefix
        self._lock = asyncio.Lock()

    def _ident_key(self, identifier: str) -> str:
        return f"{self._prefix}:ident:{identifier}"

    def _canonical_key(self, canonical_id: str) -> str:
        return f"{self._prefix}:canonical:{canonical_id}"

    def _counter_key(self) -> str:
        return f"{self._prefix}:counter"

    def _all_canonicals_key(self) -> str:
        return f"{self._prefix}:all_canonicals"

    async def get_canonical_id(self, identifiers: Set[str]) -> Optional[str]:
        for ident in identifiers:
            result = await self._redis.get(self._ident_key(ident))
            if result:
                return result.decode() if isinstance(result, bytes) else result
        return None

    async def register(self, identifiers: Set[str]) -> str:
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
                    pipe.set(self._ident_key(ident), canonical_id)
                pipe.sadd(self._canonical_key(canonical_id), *identifiers)
                pipe.sadd(self._all_canonicals_key(), canonical_id)
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
                pipe.set(self._ident_key(ident), primary_canonical)
            pipe.delete(self._canonical_key(primary_canonical))
            pipe.sadd(self._canonical_key(primary_canonical), *all_identifiers)

            # Remove merged canonical IDs
            for cid in canonical_ids_list[1:]:
                pipe.delete(self._canonical_key(cid))
                pipe.srem(self._all_canonicals_key(), cid)

            await pipe.execute()
            return primary_canonical

    async def get_all_identifiers(self, canonical_id: str) -> Set[str]:
        members = await self._redis.smembers(self._canonical_key(canonical_id))
        return {m.decode() if isinstance(m, bytes) else m for m in members}

    async def iterate_canonical_ids(self):
        members = await self._redis.smembers(self._all_canonicals_key())
        for m in members:
            yield m.decode() if isinstance(m, bytes) else m
