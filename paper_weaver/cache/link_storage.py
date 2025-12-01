"""
Link Storage - Stores relationships between entities.

Separated from info storage for flexible composition.
Relationships are stored using canonical IDs.
"""

from abc import ABCMeta, abstractmethod
from typing import Set, Optional, List
import asyncio


class LinkStorageIface(metaclass=ABCMeta):
    """
    Interface for storing directional links between entities.

    Links are stored as (from_id, to_id) pairs.
    """

    @abstractmethod
    async def add_link(self, from_id: str, to_id: str) -> None:
        """Add a link from one entity to another."""
        raise NotImplementedError

    @abstractmethod
    async def has_link(self, from_id: str, to_id: str) -> bool:
        """Check if a link exists."""
        raise NotImplementedError

    @abstractmethod
    async def get_targets(self, from_id: str) -> Optional[Set[str]]:
        """
        Get all target IDs linked from a source.
        Returns None if source has never been set (vs empty set if set but empty).
        """
        raise NotImplementedError

    @abstractmethod
    async def set_targets(self, from_id: str, to_ids: Set[str]) -> None:
        """Set all targets for a source (replaces existing)."""
        raise NotImplementedError


class MemoryLinkStorage(LinkStorageIface):
    """In-memory link storage using dict of sets."""

    def __init__(self):
        self._links: dict[str, Set[str]] = {}
        self._lock = asyncio.Lock()

    async def add_link(self, from_id: str, to_id: str) -> None:
        async with self._lock:
            if from_id not in self._links:
                self._links[from_id] = set()
            self._links[from_id].add(to_id)

    async def has_link(self, from_id: str, to_id: str) -> bool:
        async with self._lock:
            return from_id in self._links and to_id in self._links[from_id]

    async def get_targets(self, from_id: str) -> Optional[Set[str]]:
        async with self._lock:
            if from_id not in self._links:
                return None
            return set(self._links[from_id])

    async def set_targets(self, from_id: str, to_ids: Set[str]) -> None:
        async with self._lock:
            self._links[from_id] = set(to_ids)


class RedisLinkStorage(LinkStorageIface):
    """Redis link storage using sets."""

    def __init__(self, redis_client, prefix: str = "link"):
        self._redis = redis_client
        self._prefix = prefix

    def _key(self, from_id: str) -> str:
        return f"{self._prefix}:{from_id}"

    def _exists_key(self, from_id: str) -> str:
        return f"{self._prefix}:exists:{from_id}"

    async def add_link(self, from_id: str, to_id: str) -> None:
        pipe = self._redis.pipeline()
        pipe.sadd(self._key(from_id), to_id)
        pipe.set(self._exists_key(from_id), "1")
        await pipe.execute()

    async def has_link(self, from_id: str, to_id: str) -> bool:
        return await self._redis.sismember(self._key(from_id), to_id)

    async def get_targets(self, from_id: str) -> Optional[Set[str]]:
        exists = await self._redis.exists(self._exists_key(from_id))
        if not exists:
            return None
        members = await self._redis.smembers(self._key(from_id))
        return {m.decode() if isinstance(m, bytes) else m for m in members}

    async def set_targets(self, from_id: str, to_ids: Set[str]) -> None:
        pipe = self._redis.pipeline()
        pipe.delete(self._key(from_id))
        if to_ids:
            pipe.sadd(self._key(from_id), *to_ids)
        pipe.set(self._exists_key(from_id), "1")
        await pipe.execute()


class EntityListStorageIface(metaclass=ABCMeta):
    """
    Interface for storing ordered lists of entities associated with another entity.
    Used for: paper's authors, author's papers, paper's references, paper's citations.
    """

    @abstractmethod
    async def get_list(self, from_id: str) -> Optional[List[Set[str]]]:
        """
        Get list of identifier sets.
        Returns None if not set (vs empty list if explicitly set empty).
        """
        raise NotImplementedError

    @abstractmethod
    async def set_list(self, from_id: str, items: List[Set[str]]) -> None:
        """Set the list of identifier sets."""
        raise NotImplementedError


class MemoryEntityListStorage(EntityListStorageIface):
    """In-memory entity list storage."""

    def __init__(self):
        self._data: dict[str, List[Set[str]]] = {}
        self._lock = asyncio.Lock()

    async def get_list(self, from_id: str) -> Optional[List[Set[str]]]:
        async with self._lock:
            if from_id not in self._data:
                return None
            return [set(s) for s in self._data[from_id]]

    async def set_list(self, from_id: str, items: List[Set[str]]) -> None:
        async with self._lock:
            self._data[from_id] = [set(s) for s in items]


class RedisEntityListStorage(EntityListStorageIface):
    """Redis entity list storage using JSON."""

    def __init__(self, redis_client, prefix: str = "elist"):
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
