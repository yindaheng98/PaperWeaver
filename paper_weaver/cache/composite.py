"""
Composite Cache - Combines storage components into full cache implementations.

Allows flexible composition of different storage backends for:
- Identifier registry (memory/redis)
- Info storage (memory/redis)
- Committed link storage (memory/redis)
- Pending list storage (memory/redis)

Key concepts:
- Committed links: Links that have been written to DataDst
- Pending lists: Entity lists that may not have info yet, awaiting processing
"""


from ..dataclass import Paper
from ..iface_p2c import Paper2CitationsWeaverCacheIface
from ..iface_p2r import Paper2ReferencesWeaverCacheIface

from .identifier import IdentifierRegistryIface
from .info_storage import InfoStorageIface
from .link_storage import CommittedLinkStorageIface
from .pending_storage import PendingListStorageIface, PendingListManager

from .impl import ComposableCacheBase
from .impl_link import AuthorLinkCache, PaperLinkCache
from .impl_a2p import Author2PapersCache
from .impl_p2a import Paper2AuthorsCache


class Paper2ReferencesCache(PaperLinkCache, Paper2ReferencesWeaverCacheIface):
    """
    Cache for paper -> references relationships.

    Additional components:
    - pending_references: PendingListManager for paper's pending references
    """

    def __init__(
        self,
        paper_registry: IdentifierRegistryIface,
        paper_info_storage: InfoStorageIface,
        author_registry: IdentifierRegistryIface,
        author_info_storage: InfoStorageIface,
        committed_reference_links: CommittedLinkStorageIface,
        pending_references: PendingListStorageIface,
    ):
        super().__init__(
            paper_registry, paper_info_storage,
            author_registry, author_info_storage,
            committed_reference_links
        )
        self._pending_references_manager = PendingListManager(
            self._paper_manager._registry, pending_references
        )

    async def get_pending_references_for_paper(self, paper: Paper) -> list[Paper] | None:
        """Get pending references for paper."""
        paper_cid = await self._get_paper_canonical_id(paper)
        id_sets = await self._pending_references_manager.get_pending_identifier_sets(paper_cid)
        if id_sets is None:
            return None
        return [Paper(identifiers=ids) for ids in id_sets]

    async def add_pending_references_for_paper(self, paper: Paper, references: list[Paper]) -> None:
        """Add pending references for paper (registers them, merges with existing)."""
        paper_cid = await self._get_paper_canonical_id(paper)
        registered_sets = await self._pending_references_manager.add_pending_identifier_sets(
            paper_cid, [r.identifiers for r in references]
        )
        # Update reference identifiers with merged sets
        for ref, ids in zip(references, registered_sets):
            ref.identifiers = ids


class Paper2CitationsCache(PaperLinkCache, Paper2CitationsWeaverCacheIface):
    """
    Cache for paper -> citations relationships.

    Additional components:
    - pending_citations: PendingListManager for paper's pending citations
    """

    def __init__(
        self,
        paper_registry: IdentifierRegistryIface,
        paper_info_storage: InfoStorageIface,
        author_registry: IdentifierRegistryIface,
        author_info_storage: InfoStorageIface,
        committed_reference_links: CommittedLinkStorageIface,
        pending_citations: PendingListStorageIface,
    ):
        super().__init__(
            paper_registry, paper_info_storage,
            author_registry, author_info_storage,
            committed_reference_links
        )
        self._pending_citations_manager = PendingListManager(
            self._paper_manager._registry, pending_citations
        )

    async def get_pending_citations_for_paper(self, paper: Paper) -> list[Paper] | None:
        """Get pending citations for paper."""
        paper_cid = await self._get_paper_canonical_id(paper)
        id_sets = await self._pending_citations_manager.get_pending_identifier_sets(paper_cid)
        if id_sets is None:
            return None
        return [Paper(identifiers=ids) for ids in id_sets]

    async def add_pending_citations_for_paper(self, paper: Paper, citations: list[Paper]) -> None:
        """Add pending citations for paper (registers them, merges with existing)."""
        paper_cid = await self._get_paper_canonical_id(paper)
        registered_sets = await self._pending_citations_manager.add_pending_identifier_sets(
            paper_cid, [c.identifiers for c in citations]
        )
        # Update citation identifiers with merged sets
        for cit, ids in zip(citations, registered_sets):
            cit.identifiers = ids


class FullAuthorWeaverCache(Author2PapersCache, Paper2AuthorsCache):
    """
    Combined cache for AuthorWeaver with both author->papers and paper->authors.

    This cache can be used with AuthorWeaver.
    """

    def __init__(
        self,
        paper_registry: IdentifierRegistryIface,
        paper_info_storage: InfoStorageIface,
        author_registry: IdentifierRegistryIface,
        author_info_storage: InfoStorageIface,
        committed_author_links: CommittedLinkStorageIface,
        pending_papers: PendingListStorageIface,
        pending_authors: PendingListStorageIface,
    ):
        # Initialize base with shared components
        ComposableCacheBase.__init__(
            self,
            paper_registry, paper_info_storage,
            author_registry, author_info_storage
        )
        self._committed_author_links = committed_author_links
        self._pending_papers_manager = PendingListManager(
            self._paper_manager._registry, pending_papers
        )
        self._pending_authors_manager = PendingListManager(
            self._author_manager._registry, pending_authors
        )


class FullPaperWeaverCache(Paper2ReferencesCache, Paper2CitationsCache, Paper2AuthorsCache):
    """
    Combined cache for paper operations (references, citations, authors).
    """

    def __init__(
        self,
        paper_registry: IdentifierRegistryIface,
        paper_info_storage: InfoStorageIface,
        author_registry: IdentifierRegistryIface,
        author_info_storage: InfoStorageIface,
        committed_author_links: CommittedLinkStorageIface,
        committed_reference_links: CommittedLinkStorageIface,
        pending_authors: PendingListStorageIface,
        pending_references: PendingListStorageIface,
        pending_citations: PendingListStorageIface,
    ):
        ComposableCacheBase.__init__(
            self,
            paper_registry, paper_info_storage,
            author_registry, author_info_storage
        )
        self._committed_author_links = committed_author_links
        self._committed_reference_links = committed_reference_links
        self._pending_authors_manager = PendingListManager(
            self._author_manager._registry, pending_authors
        )
        self._pending_references_manager = PendingListManager(
            self._paper_manager._registry, pending_references
        )
        self._pending_citations_manager = PendingListManager(
            self._paper_manager._registry, pending_citations
        )
