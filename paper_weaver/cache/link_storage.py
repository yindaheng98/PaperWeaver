"""
Storage interfaces for link tracking and pending entity lists.

Two types of storage:
1. CommittedLinkStorageIface: Tracks links that have been written to DataDst
2. PendingListStorageIface: Stores pending entity lists (may lack info, not yet in DataDst)
"""

from abc import ABCMeta, abstractmethod
from typing import Set, Optional, List


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
    Interface for storing pending entity lists.

    Used for: paper's authors, author's papers, paper's references, paper's citations.
    These entities may not have info yet and are pending processing.
    Each entity in the list is represented by a set of identifiers.
    Order is preserved (e.g., author order matters in papers).
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
