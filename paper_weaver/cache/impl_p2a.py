"""
Paper to Authors Cache - Cache for paper -> authors relationships.

Provides Paper2AuthorsCache with:
- Pending authors management for papers
- Author-paper link tracking (inherited from AuthorLinkCache)
"""


from ..dataclass import Paper, Author
from ..iface_p2a import Paper2AuthorsWeaverCacheIface

from .identifier import IdentifierRegistryIface
from .info_storage import InfoStorageIface
from .link_storage import CommittedLinkStorageIface
from .pending_storage import PendingListStorageIface, PendingListManager

from .impl_link import AuthorLinkCache


class Paper2AuthorsCache(AuthorLinkCache, Paper2AuthorsWeaverCacheIface):
    """
    Cache for paper -> authors relationships.

    Additional components:
    - pending_authors_by_paper: PendingListManager for paper's pending authors
    """

    def __init__(
        self,
        paper_registry: IdentifierRegistryIface,
        paper_info_storage: InfoStorageIface,
        author_registry: IdentifierRegistryIface,
        author_info_storage: InfoStorageIface,
        venue_registry: IdentifierRegistryIface,
        venue_info_storage: InfoStorageIface,
        committed_author_links: CommittedLinkStorageIface,
        pending_authors_by_paper: PendingListStorageIface,
    ):
        super().__init__(
            paper_registry, paper_info_storage,
            author_registry, author_info_storage,
            venue_registry, venue_info_storage,
            committed_author_links
        )
        self._pending_authors_by_paper_manager = PendingListManager(
            self._author_manager._registry, pending_authors_by_paper
        )

    async def get_pending_authors_for_paper(self, paper: Paper) -> list[Author] | None:
        """Get pending authors for paper."""
        paper_cid = await self._get_paper_canonical_id(paper)
        id_sets = await self._pending_authors_by_paper_manager.get_pending_identifier_sets(paper_cid)
        if id_sets is None:
            return None
        return [Author(identifiers=ids) for ids in id_sets]

    async def add_pending_authors_for_paper(self, paper: Paper, authors: list[Author]) -> None:
        """Add pending authors for paper (registers them, merges with existing)."""
        paper_cid = await self._get_paper_canonical_id(paper)
        registered_sets = await self._pending_authors_by_paper_manager.add_pending_identifier_sets(
            paper_cid, [a.identifiers for a in authors]
        )
        # Update author identifiers with merged sets
        for author, ids in zip(authors, registered_sets):
            author.identifiers = ids
