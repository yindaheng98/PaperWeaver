"""
Base Composable Cache - Foundation for all cache implementations.

Provides ComposableCacheBase with:
- Paper info management (get/set paper info, iterate papers)
- Author info management (get/set author info, iterate authors)
- Venue info management (get/set venue info, iterate venues)
- Identifier merging and canonical ID resolution

This is the base class that other cache implementations extend.
"""

from typing import AsyncIterator, Tuple

from ..dataclass import Paper, Author, Venue
from ..iface import WeaverCacheIface

from .identifier import IdentifierRegistryIface
from .info_storage import InfoStorageIface, EntityInfoManager


class ComposableCacheBase(WeaverCacheIface):
    """
    Base cache with composable info storage for papers, authors, and venues.

    Components:
    - paper_registry: Identifier registry for papers
    - paper_info_storage: Info storage for papers
    - author_registry: Identifier registry for authors
    - author_info_storage: Info storage for authors
    - venue_registry: Identifier registry for venues
    - venue_info_storage: Info storage for venues
    """

    def __init__(
        self,
        paper_registry: IdentifierRegistryIface,
        paper_info_storage: InfoStorageIface,
        author_registry: IdentifierRegistryIface,
        author_info_storage: InfoStorageIface,
        venue_registry: IdentifierRegistryIface,
        venue_info_storage: InfoStorageIface,
    ):
        self._paper_manager = EntityInfoManager(paper_registry, paper_info_storage)
        self._author_manager = EntityInfoManager(author_registry, author_info_storage)
        self._venue_manager = EntityInfoManager(venue_registry, venue_info_storage)

    # Paper info methods

    async def get_paper_info(self, paper: Paper) -> Tuple[Paper, dict | None]:
        """Return (updated paper with merged identifiers, info or None)."""
        canonical_id, all_identifiers, info = await self._paper_manager.get_info(paper.identifiers)
        paper.identifiers = all_identifiers
        return paper, info

    async def set_paper_info(self, paper: Paper, info: dict) -> None:
        """Set paper info, updating paper's identifiers with all known aliases."""
        canonical_id, all_identifiers = await self._paper_manager.set_info(paper.identifiers, info)
        paper.identifiers = all_identifiers

    # Author info methods

    async def get_author_info(self, author: Author) -> Tuple[Author, dict | None]:
        """Return (updated author with merged identifiers, info or None)."""
        canonical_id, all_identifiers, info = await self._author_manager.get_info(author.identifiers)
        author.identifiers = all_identifiers
        return author, info

    async def set_author_info(self, author: Author, info: dict) -> None:
        """Set author info, updating author's identifiers with all known aliases."""
        canonical_id, all_identifiers = await self._author_manager.set_info(author.identifiers, info)
        author.identifiers = all_identifiers

    # Venue info methods

    async def get_venue_info(self, venue: Venue) -> Tuple[Venue, dict | None]:
        """Return (updated venue with merged identifiers, info or None)."""
        canonical_id, all_identifiers, info = await self._venue_manager.get_info(venue.identifiers)
        venue.identifiers = all_identifiers
        return venue, info

    async def set_venue_info(self, venue: Venue, info: dict) -> None:
        """Set venue info, updating venue's identifiers with all known aliases."""
        canonical_id, all_identifiers = await self._venue_manager.set_info(venue.identifiers, info)
        venue.identifiers = all_identifiers

    # Iteration methods

    def iterate_papers(self) -> AsyncIterator[Paper]:
        """Iterate over all registered papers."""
        return self._iterate_papers_impl()

    async def _iterate_papers_impl(self) -> AsyncIterator[Paper]:
        async for canonical_id, identifiers in self._paper_manager.iterate_entities():
            yield Paper(identifiers=identifiers)

    def iterate_authors(self) -> AsyncIterator[Author]:
        """Iterate over all registered authors."""
        return self._iterate_authors_impl()

    async def _iterate_authors_impl(self) -> AsyncIterator[Author]:
        async for canonical_id, identifiers in self._author_manager.iterate_entities():
            yield Author(identifiers=identifiers)

    def iterate_venues(self) -> AsyncIterator[Venue]:
        """Iterate over all registered venues."""
        return self._iterate_venues_impl()

    async def _iterate_venues_impl(self) -> AsyncIterator[Venue]:
        async for canonical_id, identifiers in self._venue_manager.iterate_entities():
            yield Venue(identifiers=identifiers)
