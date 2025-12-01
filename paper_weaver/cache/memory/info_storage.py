"""
Info Storage - Stores entity information (dict data).

Separated from relationship storage for flexible composition.
"""

import asyncio

from ..info_storage import InfoStorageIface


class MemoryInfoStorage(InfoStorageIface):
    """In-memory info storage using dict."""

    def __init__(self):
        self._data: dict[str, dict] = {}
        self._lock = asyncio.Lock()

    async def get_info(self, canonical_id: str) -> dict | None:
        async with self._lock:
            return self._data.get(canonical_id)

    async def set_info(self, canonical_id: str, info: dict) -> None:
        async with self._lock:
            self._data[canonical_id] = info
