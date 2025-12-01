"""
Memory implementations for link tracking and pending entity lists.
"""

from typing import Set, Optional, List
import asyncio

from ..link_storage import CommittedLinkStorageIface, PendingListStorageIface


class MemoryCommittedLinkStorage(CommittedLinkStorageIface):
    """In-memory storage for committed links."""

    def __init__(self):
        self._links: dict[str, Set[str]] = {}
        self._lock = asyncio.Lock()

    async def commit_link(self, from_id: str, to_id: str) -> None:
        async with self._lock:
            if from_id not in self._links:
                self._links[from_id] = set()
            self._links[from_id].add(to_id)

    async def is_link_committed(self, from_id: str, to_id: str) -> bool:
        async with self._lock:
            return from_id in self._links and to_id in self._links[from_id]


class MemoryPendingListStorage(PendingListStorageIface):
    """In-memory storage for pending entity lists."""

    def __init__(self):
        self._data: dict[str, List[Set[str]]] = {}
        self._lock = asyncio.Lock()

    async def get_pending_identifier_sets(self, from_id: str) -> Optional[List[Set[str]]]:
        async with self._lock:
            if from_id not in self._data:
                return None
            return [set(s) for s in self._data[from_id]]

    async def set_pending_identifier_sets(self, from_id: str, items: List[Set[str]]) -> None:
        async with self._lock:
            self._data[from_id] = [set(s) for s in items]
