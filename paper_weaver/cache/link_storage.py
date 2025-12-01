"""
Link Storage - Stores relationships between entities.

Separated from info storage for flexible composition.
Relationships are stored using canonical IDs.
"""

from abc import ABCMeta, abstractmethod
from typing import Set, Optional, List


class LinkStorageIface(metaclass=ABCMeta):
    """
    Interface for storing directional links between entities.

    Links are stored as (from_id, to_id) pairs.
    Used for quick link existence checks (e.g., is author linked to paper?).
    """

    @abstractmethod
    async def add_link(self, from_id: str, to_id: str) -> None:
        """Add a link from one entity to another."""
        raise NotImplementedError

    @abstractmethod
    async def has_link(self, from_id: str, to_id: str) -> bool:
        """Check if a link exists."""
        raise NotImplementedError


class EntityListStorageIface(metaclass=ABCMeta):
    """
    Interface for storing ordered lists of entities associated with another entity.
    Used for: paper's authors, author's papers, paper's references, paper's citations.

    Each entity in the list is represented by a set of identifiers.
    Order is preserved (e.g., author order matters in papers).
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
