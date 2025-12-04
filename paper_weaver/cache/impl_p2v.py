"""
Paper to Venues Cache - Cache for paper -> venues relationships.

Provides Paper2VenuesCache with:
- Pending venues management for papers
- Venue link tracking (inherited from VenueLinkCache)
"""


from ..dataclass import Paper, Venue
from ..iface_p2v import Paper2VenuesWeaverCacheIface

from .identifier import IdentifierRegistryIface
from .info_storage import InfoStorageIface
from .link_storage import CommittedLinkStorageIface
from .pending_storage import PendingListStorageIface, PendingListManager

from .impl_link import VenueLinkCache


class Paper2VenuesCache(VenueLinkCache, Paper2VenuesWeaverCacheIface):
    """
    Cache for paper -> venues relationships.

    Additional components:
    - pending_venues_by_paper: PendingListManager for paper's pending venues
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
        pending_venues_by_paper: PendingListStorageIface,
    ):
        super().__init__(
            paper_registry, paper_info_storage,
            author_registry, author_info_storage,
            venue_registry, venue_info_storage,
            committed_venue_links
        )
        self._pending_venues_by_paper_manager = PendingListManager(
            self._venue_manager._registry, pending_venues_by_paper
        )

    async def get_pending_venues_for_paper(self, paper: Paper) -> list[Venue] | None:
        """Get pending venues for paper."""
        paper_cid = await self._get_paper_canonical_id(paper)
        id_sets = await self._pending_venues_by_paper_manager.get_pending_identifier_sets(paper_cid)
        if id_sets is None:
            return None
        return [Venue(identifiers=ids) for ids in id_sets]

    async def add_pending_venues_for_paper(self, paper: Paper, venues: list[Venue]) -> None:
        """Add pending venues for paper (registers them, merges with existing)."""
        paper_cid = await self._get_paper_canonical_id(paper)
        registered_sets = await self._pending_venues_by_paper_manager.add_pending_identifier_sets(
            paper_cid, [v.identifiers for v in venues]
        )
        # Update venue identifiers with merged sets
        for venue, ids in zip(venues, registered_sets):
            venue.identifiers = ids
