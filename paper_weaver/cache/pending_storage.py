"""
Storage interface for pending entity lists.

PendingListStorageIface: Stores pending entity lists (may lack info, not yet in DataDst)

Manager classes:
- PendingListManager: Combines PendingListStorageIface with IdentifierRegistryIface
"""

from abc import ABCMeta, abstractmethod

from .identifier import IdentifierRegistryIface


class PendingListStorageIface(metaclass=ABCMeta):
    """
    Low-level interface for storing pending entity lists.

    Used for: paper's authors, author's papers, paper's references, paper's citations.
    These entities may not have info yet and are pending processing.
    Each entity in the list is represented by a set of identifiers.

    For high-level usage with identifier registration, use PendingListManager.
    """

    @abstractmethod
    async def get_pending_identifier_sets(self, from_id: str) -> list[set[str]] | None:
        """
        Get list of pending entity identifier sets.
        Returns None if not set (vs empty list if explicitly set empty).
        """
        raise NotImplementedError

    @abstractmethod
    async def set_pending_identifier_sets(self, from_id: str, items: list[set[str]]) -> None:
        """Set the list of pending entity identifier sets."""
        raise NotImplementedError


class PendingListManager:
    """
    Manages pending entity lists with identifier registry integration.

    Similar to EntityInfoManager, this handles:
    - Registering entities when they are added to pending lists
    - Merging identifiers when retrieving pending entities
    - Making entities discoverable via iteration

    Usage:
        manager = PendingListManager(paper_registry, pending_storage)

        # Add pending papers for an author
        registered_sets = await manager.add_pending_identifier_sets(author_cid, [p.identifiers for p in papers])

        # Get pending papers for an author
        id_sets = await manager.get_pending_identifier_sets(author_cid)
    """

    def __init__(
        self,
        entity_registry: IdentifierRegistryIface,
        pending_storage: PendingListStorageIface
    ):
        self._registry = entity_registry
        self._storage = pending_storage

    async def get_pending_canonical_id_identifier_set_dict(self, from_canonical_id: str) -> dict[str, set[str]] | None:
        """
        Get pending entity list in the form of a dictionary (canonical_id -> identifiers), merging identifiers for each entity.

        Each entity's identifiers are registered and merged with any existing
        identifiers for that entity.

        Args:
            from_canonical_id: The canonical ID of the source entity

        Returns:
            Dictionary mapping canonical_id to identifier sets, or None if not set
        """
        identifiers_list = await self._storage.get_pending_identifier_sets(from_canonical_id)
        if identifiers_list is None:
            return None

        result = {}
        for id_set in identifiers_list:
            canonical_id = await self._registry.register(id_set)
            all_identifiers = await self._registry.get_all_identifiers(canonical_id)
            result[canonical_id] = all_identifiers
        return result

    async def get_pending_identifier_sets(self, from_canonical_id: str) -> list[set[str]] | None:
        """
        Get pending entity list, merging identifiers for each entity.

        Each entity's identifiers are registered and merged with any existing
        identifiers for that entity.

        Args:
            from_canonical_id: The canonical ID of the source entity

        Returns:
            List of identifier sets (one per entity), or None if not set
        """
        result = await self.get_pending_canonical_id_identifier_set_dict(from_canonical_id)
        if result is None:
            return None
        return list(result.values())

    async def add_pending_identifier_sets(self, from_canonical_id: str, identifiers_list: list[set[str]]) -> list[set[str]]:
        """
        Add pending entities to the list, registering each entity and merging with existing entries.

        Each entity's identifiers are registered so subsequent lookups see the
        merged result. The stored pending list keeps its existing order, and any
        new canonical IDs are appended. The returned list mirrors the input order,
        but each identifier set is expanded with every known identifier.

        Args:
            from_canonical_id: The canonical ID of the source entity
            identifiers_list: List of identifier sets for each pending entity

        Returns:
            Updated identifier sets corresponding to identifiers_list (after merging)
        """
        result = await self.get_pending_canonical_id_identifier_set_dict(from_canonical_id)
        if result is None:
            result = {}
        updated_identifiers_list = []
        for id_set in identifiers_list:
            canonical_id = await self._registry.register(id_set)
            all_identifiers = await self._registry.get_all_identifiers(canonical_id)
            result[canonical_id] = all_identifiers
            updated_identifiers_list.append(all_identifiers)

        registered_sets = list(result.values())
        await self._storage.set_pending_identifier_sets(from_canonical_id, registered_sets)
        return updated_identifiers_list
