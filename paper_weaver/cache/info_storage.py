"""
Info Storage - Stores entity information (dict data).

Separated from relationship storage for flexible composition.
"""

from abc import ABCMeta, abstractmethod

from .identifier import IdentifierRegistryIface


class InfoStorageIface(metaclass=ABCMeta):
    """Interface for storing entity info by canonical ID."""

    @abstractmethod
    async def get_info(self, canonical_id: str) -> dict | None:
        """Get info for a canonical ID. Returns None if not found."""
        raise NotImplementedError

    @abstractmethod
    async def set_info(self, canonical_id: str, info: dict) -> None:
        """Set info for a canonical ID."""
        raise NotImplementedError


class EntityInfoManager:
    """
    Manages entity info with identifier registry integration.

    Handles:
    - Identifier registration and merging
    - Info storage and retrieval
    - Keeping identifiers synchronized
    """

    def __init__(
        self,
        identifier_registry: IdentifierRegistryIface,
        info_storage: InfoStorageIface
    ):
        self._registry = identifier_registry
        self._storage = info_storage

    async def get_info(self, identifiers: set[str], merge_identifiers: bool = True) -> tuple[str | None, set[str], dict | None]:
        """
        Get info for an entity by its identifiers.

        Args:
            identifiers: Set of identifiers to look up
            merge_identifiers: If True, merge provided identifiers with existing ones

        Returns: (canonical_id, all_identifiers, info)
        - canonical_id: None if not registered
        - all_identifiers: All known identifiers (merged set)
        - info: None if not stored
        """
        canonical_id = await self._registry.get_canonical_id(identifiers)
        if canonical_id is None:
            return None, identifiers, None

        # Merge the provided identifiers with existing ones to keep them synchronized
        if merge_identifiers:
            canonical_id = await self._registry.register(identifiers)

        all_identifiers = await self._registry.get_all_identifiers(canonical_id)
        info = await self._storage.get_info(canonical_id)
        return canonical_id, all_identifiers, info

    async def set_info(self, identifiers: set[str], info: dict) -> tuple[str, set[str]]:
        """
        Set info for an entity.

        Returns: (canonical_id, all_identifiers)
        """
        canonical_id = await self._registry.register(identifiers)
        await self._storage.set_info(canonical_id, info)
        all_identifiers = await self._registry.get_all_identifiers(canonical_id)
        return canonical_id, all_identifiers

    async def register_identifiers(self, identifiers: set[str]) -> tuple[str, set[str]]:
        """
        Register identifiers without setting info.

        Returns: (canonical_id, all_identifiers)
        """
        canonical_id = await self._registry.register(identifiers)
        all_identifiers = await self._registry.get_all_identifiers(canonical_id)
        return canonical_id, all_identifiers

    async def iterate_entities(self):
        """Async iterator yielding (canonical_id, all_identifiers) for all registered entities."""
        async for canonical_id in self._registry.iterate_canonical_ids():
            all_identifiers = await self._registry.get_all_identifiers(canonical_id)
            yield canonical_id, all_identifiers
