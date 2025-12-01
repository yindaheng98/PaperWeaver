"""
Memory implementation for committed link tracking.
"""

import asyncio

from ..link_storage import CommittedLinkStorageIface


class MemoryCommittedLinkStorage(CommittedLinkStorageIface):
    """In-memory storage for committed links."""

    def __init__(self):
        self._links: dict[str, set[str]] = {}
        self._lock = asyncio.Lock()

    async def commit_link(self, from_id: str, to_id: str) -> None:
        async with self._lock:
            if from_id not in self._links:
                self._links[from_id] = set()
            self._links[from_id].add(to_id)

    async def is_link_committed(self, from_id: str, to_id: str) -> bool:
        async with self._lock:
            return from_id in self._links and to_id in self._links[from_id]
