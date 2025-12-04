"""
Paper to Citations Cache - Cache for paper -> citations relationships.

Provides Paper2CitationsCache with:
- Pending citations management for papers
- Reference link tracking (inherited from PaperLinkCache)
"""


from ..dataclass import Paper
from ..iface_p2c import Paper2CitationsWeaverCacheIface

from .identifier import IdentifierRegistryIface
from .info_storage import InfoStorageIface
from .link_storage import CommittedLinkStorageIface
from .pending_storage import PendingListStorageIface, PendingListManager

from .impl_link import PaperLinkCache


class Paper2CitationsCache(PaperLinkCache, Paper2CitationsWeaverCacheIface):
    """
    Cache for paper -> citations relationships.

    Additional components:
    - pending_citations_by_paper: PendingListManager for paper's pending citations
    """

    def __init__(
        self,
        paper_registry: IdentifierRegistryIface,
        paper_info_storage: InfoStorageIface,
        author_registry: IdentifierRegistryIface,
        author_info_storage: InfoStorageIface,
        venue_registry: IdentifierRegistryIface,
        venue_info_storage: InfoStorageIface,
        committed_reference_links: CommittedLinkStorageIface,
        pending_citations_by_paper: PendingListStorageIface,
    ):
        super().__init__(
            paper_registry, paper_info_storage,
            author_registry, author_info_storage,
            venue_registry, venue_info_storage,
            committed_reference_links
        )
        self._pending_citations_by_paper_manager = PendingListManager(
            self._paper_manager._registry, pending_citations_by_paper
        )

    async def get_pending_citations_for_paper(self, paper: Paper) -> list[Paper] | None:
        """Get pending citations for paper."""
        paper_cid = await self._get_paper_canonical_id(paper)
        id_sets = await self._pending_citations_by_paper_manager.get_pending_identifier_sets(paper_cid)
        if id_sets is None:
            return None
        return [Paper(identifiers=ids) for ids in id_sets]

    async def add_pending_citations_for_paper(self, paper: Paper, citations: list[Paper]) -> None:
        """Add pending citations for paper (registers them, merges with existing)."""
        paper_cid = await self._get_paper_canonical_id(paper)
        registered_sets = await self._pending_citations_by_paper_manager.add_pending_identifier_sets(
            paper_cid, [c.identifiers for c in citations]
        )
        # Update citation identifiers with merged sets
        for cit, ids in zip(citations, registered_sets):
            cit.identifiers = ids
