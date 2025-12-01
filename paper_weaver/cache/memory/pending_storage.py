"""
Memory implementations for link tracking and pending entity lists.
"""

from typing import Set, Optional, List
import asyncio

from ..pending_storage import PendingListStorageIface


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
