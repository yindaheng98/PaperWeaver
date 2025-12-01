"""
Storage interfaces for link tracking and pending entity lists.

Two types of storage:
1. CommittedLinkStorageIface: Tracks links that have been written to DataDst
2. PendingListStorageIface: Stores pending entity lists (may lack info, not yet in DataDst)

Manager classes:
- PendingListManager: Combines PendingListStorageIface with IdentifierRegistryIface
"""

from abc import ABCMeta, abstractmethod
from typing import Set, Optional, List

from .identifier import IdentifierRegistryIface


class CommittedLinkStorageIface(metaclass=ABCMeta):
    """
    Interface for tracking committed links (written to DataDst).

    Links are stored as (from_id, to_id) pairs.
    Used for quick link existence checks to avoid duplicate writes to DataDst.
    """

    @abstractmethod
    async def add_link(self, from_id: str, to_id: str) -> None:
        """Mark a link as committed."""
        raise NotImplementedError

    @abstractmethod
    async def has_link(self, from_id: str, to_id: str) -> bool:
        """Check if a link has been committed."""
        raise NotImplementedError


class PendingListStorageIface(metaclass=ABCMeta):
    """
    Low-level interface for storing pending entity lists.

    Used for: paper's authors, author's papers, paper's references, paper's citations.
    These entities may not have info yet and are pending processing.
    Each entity in the list is represented by a set of identifiers.
    Order is preserved (e.g., author order matters in papers).

    For high-level usage with identifier registration, use PendingListManager.
    """

    @abstractmethod
    async def get_list(self, from_id: str) -> Optional[List[Set[str]]]:
        """
        Get list of pending entity identifier sets.
        Returns None if not set (vs empty list if explicitly set empty).
        """
        raise NotImplementedError

    @abstractmethod
    async def set_list(self, from_id: str, items: List[Set[str]]) -> None:
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
        
        # Set pending papers for an author
        registered_sets = await manager.set_list(author_cid, [p.identifiers for p in papers])
        
        # Get pending papers for an author
        id_sets = await manager.get_list(author_cid)
    """

    def __init__(
        self,
        entity_registry: IdentifierRegistryIface,
        pending_storage: PendingListStorageIface
    ):
        self._registry = entity_registry
        self._storage = pending_storage

    async def get_list(self, from_canonical_id: str) -> Optional[List[Set[str]]]:
        """
        Get pending entity list, merging identifiers for each entity.

        Each entity's identifiers are registered and merged with any existing
        identifiers for that entity.

        Args:
            from_canonical_id: The canonical ID of the source entity

        Returns:
            List of identifier sets (one per entity), or None if not set
        """
        id_sets = await self._storage.get_list(from_canonical_id)
        if id_sets is None:
            return None

        result = []
        for id_set in id_sets:
            canonical_id = await self._registry.register(id_set)
            all_identifiers = await self._registry.get_all_identifiers(canonical_id)
            result.append(all_identifiers)
        return result

    async def set_list(self, from_canonical_id: str, id_sets: List[Set[str]]) -> List[Set[str]]:
        """
        Set pending entity list, registering each entity.

        Each entity's identifiers are registered, making them discoverable
        via the registry's iteration.

        Args:
            from_canonical_id: The canonical ID of the source entity
            id_sets: List of identifier sets for each pending entity

        Returns:
            List of merged identifier sets (with any existing identifiers)
        """
        registered_sets = []
        for id_set in id_sets:
            canonical_id = await self._registry.register(id_set)
            all_identifiers = await self._registry.get_all_identifiers(canonical_id)
            registered_sets.append(all_identifiers)

        await self._storage.set_list(from_canonical_id, registered_sets)
        return registered_sets
