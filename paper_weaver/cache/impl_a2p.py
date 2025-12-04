"""
Author to Papers Cache - Cache for author -> papers relationships.

Provides Author2PapersCache with:
- Pending papers management for authors
- Author-paper link tracking (inherited from AuthorLinkCache)
"""


from ..dataclass import Paper, Author
from ..iface_a2p import Author2PapersWeaverCacheIface

from .identifier import IdentifierRegistryIface
from .info_storage import InfoStorageIface
from .link_storage import CommittedLinkStorageIface
from .pending_storage import PendingListStorageIface, PendingListManager

from .impl_link import AuthorLinkCache


class Author2PapersCache(AuthorLinkCache, Author2PapersWeaverCacheIface):
    """
    Cache for author -> papers relationships.

    Additional components:
    - pending_papers_by_author: PendingListManager for author's pending papers
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
        pending_papers_by_author: PendingListStorageIface,
    ):
        super().__init__(
            paper_registry, paper_info_storage,
            author_registry, author_info_storage,
            venue_registry, venue_info_storage,
            committed_author_links
        )
        self._pending_papers_by_author_manager = PendingListManager(
            self._paper_manager._registry, pending_papers_by_author
        )

    async def get_pending_papers_for_author(self, author: Author) -> list[Paper] | None:
        """Get pending papers for author."""
        author_cid = await self._get_author_canonical_id(author)
        id_sets = await self._pending_papers_by_author_manager.get_pending_identifier_sets(author_cid)
        if id_sets is None:
            return None
        return [Paper(identifiers=ids) for ids in id_sets]

    async def add_pending_papers_for_author(self, author: Author, papers: list[Paper]) -> None:
        """Add pending papers for author (registers them, merges with existing)."""
        author_cid = await self._get_author_canonical_id(author)
        registered_sets = await self._pending_papers_by_author_manager.add_pending_identifier_sets(
            author_cid, [p.identifiers for p in papers]
        )
        # Update paper identifiers with merged sets
        for paper, ids in zip(papers, registered_sets):
            paper.identifiers = ids
