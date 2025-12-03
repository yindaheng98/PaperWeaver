"""
Cache interfaces for tracking committed links.

These methods track which links have been formally written to DataDst.
Only objects with successfully fetched info reach this stage.
"""

from abc import ABCMeta, abstractmethod
from .dataclass import Paper, Author, Venue
from .iface import WeaverCacheIface


class AuthorLinkWeaverCacheIface(WeaverCacheIface, metaclass=ABCMeta):
    """Cache interface for paper-author link commitment tracking."""

    @abstractmethod
    async def is_author_link_committed(self, paper: Paper, author: Author) -> bool:
        """Check if paper-author link has been committed to DataDst."""
        raise NotImplementedError

    @abstractmethod
    async def commit_author_link(self, paper: Paper, author: Author) -> None:
        """Mark paper-author link as committed to DataDst."""
        raise NotImplementedError


class PaperLinkWeaverCacheIface(WeaverCacheIface, metaclass=ABCMeta):
    """Cache interface for paper-paper link commitment tracking (references/citations)."""

    @abstractmethod
    async def is_reference_link_committed(self, paper: Paper, reference: Paper) -> bool:
        """Check if paper-reference link has been committed to DataDst."""
        raise NotImplementedError

    @abstractmethod
    async def commit_reference_link(self, paper: Paper, reference: Paper) -> None:
        """Mark paper-reference link as committed to DataDst."""
        raise NotImplementedError

    async def is_citation_link_committed(self, paper: Paper, citation: Paper) -> bool:
        """Check if paper-citation link has been committed to DataDst."""
        # "paper is cited by citation" is the inverse of "citation references paper"
        return await self.is_reference_link_committed(citation, paper)

    async def commit_citation_link(self, paper: Paper, citation: Paper) -> None:
        """Mark paper-citation link as committed to DataDst."""
        # "paper is cited by citation" is the inverse of "citation references paper"
        return await self.commit_reference_link(citation, paper)


class VenueLinkWeaverCacheIface(WeaverCacheIface, metaclass=ABCMeta):
    """Cache interface for paper-venue link commitment tracking."""

    @abstractmethod
    async def is_venue_link_committed(self, paper: Paper, venue: Venue) -> bool:
        """Check if paper-venue link has been committed to DataDst."""
        raise NotImplementedError

    @abstractmethod
    async def commit_venue_link(self, paper: Paper, venue: Venue) -> None:
        """Mark paper-venue link as committed to DataDst."""
        raise NotImplementedError
