"""
Link Storage - Stores relationships between entities.

Separated from info storage for flexible composition.
Relationships are stored using canonical IDs.
"""

from typing import Set, Optional, List
import asyncio

from ..link_storage import LinkStorageIface, EntityListStorageIface


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
