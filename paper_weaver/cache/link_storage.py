"""
Storage interface for committed link tracking.

CommittedLinkStorageIface: Tracks links that have been written to DataDst.
Used for quick link existence checks to avoid duplicate writes.
"""

from abc import ABCMeta, abstractmethod


class CommittedLinkStorageIface(metaclass=ABCMeta):
    """
    Interface for tracking committed links (written to DataDst).

    Links are stored as (from_id, to_id) pairs.
    Used for quick link existence checks to avoid duplicate writes to DataDst.
    """

    @abstractmethod
    async def commit_link(self, from_id: str, to_id: str) -> None:
        """Mark a link as committed."""
        raise NotImplementedError

    @abstractmethod
    async def is_link_committed(self, from_id: str, to_id: str) -> bool:
        """Check if a link has been committed."""
        raise NotImplementedError
