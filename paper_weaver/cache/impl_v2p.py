"""
Venue to Papers Cache - Cache for venue -> papers relationships.

Provides Venue2PapersCache with:
- Pending papers management for venues
- Venue link tracking (inherited from VenueLinkCache)
"""


from ..dataclass import Paper, Venue
from ..iface_v2p import Venue2PapersWeaverCacheIface

from .identifier import IdentifierRegistryIface
from .info_storage import InfoStorageIface
from .link_storage import CommittedLinkStorageIface
from .pending_storage import PendingListStorageIface, PendingListManager

from .impl_link import VenueLinkCache


class Venue2PapersCache(VenueLinkCache, Venue2PapersWeaverCacheIface):
    """
    Cache for venue -> papers relationships.

    Additional components:
    - pending_papers_by_venue: PendingListManager for venue's pending papers
    """

    def __init__(
        self,
        paper_registry: IdentifierRegistryIface,
        paper_info_storage: InfoStorageIface,
        author_registry: IdentifierRegistryIface,
        author_info_storage: InfoStorageIface,
        venue_registry: IdentifierRegistryIface,
        venue_info_storage: InfoStorageIface,
        committed_venue_links: CommittedLinkStorageIface,
        pending_papers_by_venue: PendingListStorageIface,
    ):
        super().__init__(
            paper_registry, paper_info_storage,
            author_registry, author_info_storage,
            venue_registry, venue_info_storage,
            committed_venue_links
        )
        self._pending_papers_by_venue_manager = PendingListManager(
            self._paper_manager._registry, pending_papers_by_venue
        )

    async def get_pending_papers_for_venue(self, venue: Venue) -> list[Paper] | None:
        """Get pending papers for venue."""
        venue_cid = await self._get_venue_canonical_id(venue)
        id_sets = await self._pending_papers_by_venue_manager.get_pending_identifier_sets(venue_cid)
        if id_sets is None:
            return None
        return [Paper(identifiers=ids) for ids in id_sets]

    async def add_pending_papers_for_venue(self, venue: Venue, papers: list[Paper]) -> None:
        """Add pending papers for venue (registers them, merges with existing)."""
        venue_cid = await self._get_venue_canonical_id(venue)
        registered_sets = await self._pending_papers_by_venue_manager.add_pending_identifier_sets(
            venue_cid, [p.identifiers for p in papers]
        )
        # Update paper identifiers with merged sets
        for paper, ids in zip(papers, registered_sets):
            paper.identifiers = ids
