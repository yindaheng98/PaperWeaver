"""
Memory implementations for pending entity lists.
"""

import asyncio

from ..pending_storage import PendingListStorageIface


class MemoryPendingListStorage(PendingListStorageIface):
    """In-memory storage for pending entity lists."""

    def __init__(self):
        self._data: dict[str, list[set[str]]] = {}
        self._lock = asyncio.Lock()

    async def get_pending_identifier_sets(self, from_id: str) -> list[set[str]] | None:
        async with self._lock:
            if from_id not in self._data:
                return None
            return [set(s) for s in self._data[from_id]]

    async def set_pending_identifier_sets(self, from_id: str, items: list[set[str]]) -> None:
        async with self._lock:
            self._data[from_id] = [set(s) for s in items]
