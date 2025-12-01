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


from ..dataclass import Paper, Author
from ..iface_a2p import Author2PapersWeaverCacheIface
from ..iface_p2a import Paper2AuthorsWeaverCacheIface
from ..iface_p2c import Paper2CitationsWeaverCacheIface
from ..iface_p2r import Paper2ReferencesWeaverCacheIface

from .identifier import IdentifierRegistryIface
from .info_storage import InfoStorageIface
from .link_storage import CommittedLinkStorageIface
from .pending_storage import PendingListStorageIface, PendingListManager

from .impl import ComposableCacheBase
from .impl_link import AuthorLinkCache, PaperLinkCache


class Author2PapersCache(AuthorLinkCache, Author2PapersWeaverCacheIface):
    """
    Cache for author -> papers relationships.

    Additional components:
    - pending_papers: PendingListManager for author's pending papers
    """

    def __init__(
        self,
        paper_registry: IdentifierRegistryIface,
        paper_info_storage: InfoStorageIface,
        author_registry: IdentifierRegistryIface,
        author_info_storage: InfoStorageIface,
        committed_author_links: CommittedLinkStorageIface,
        pending_papers: PendingListStorageIface,
    ):
        super().__init__(
            paper_registry, paper_info_storage,
            author_registry, author_info_storage,
            committed_author_links
        )
        self._pending_papers_manager = PendingListManager(
            self._paper_manager._registry, pending_papers
        )

    async def get_pending_papers_for_author(self, author: Author) -> list[Paper] | None:
        """Get pending papers for author."""
        author_cid = await self._get_author_canonical_id(author)
        id_sets = await self._pending_papers_manager.get_pending_identifier_sets(author_cid)
        if id_sets is None:
            return None
        return [Paper(identifiers=ids) for ids in id_sets]

    async def add_pending_papers_for_author(self, author: Author, papers: list[Paper]) -> None:
        """Add pending papers for author (registers them, merges with existing)."""
        author_cid = await self._get_author_canonical_id(author)
        registered_sets = await self._pending_papers_manager.add_pending_identifier_sets(
            author_cid, [p.identifiers for p in papers]
        )
        # Update paper identifiers with merged sets
        for paper, ids in zip(papers, registered_sets):
            paper.identifiers = ids


class Paper2AuthorsCache(AuthorLinkCache, Paper2AuthorsWeaverCacheIface):
    """
    Cache for paper -> authors relationships.

    Additional components:
    - pending_authors: PendingListManager for paper's pending authors
    """

    def __init__(
        self,
        paper_registry: IdentifierRegistryIface,
        paper_info_storage: InfoStorageIface,
        author_registry: IdentifierRegistryIface,
        author_info_storage: InfoStorageIface,
        committed_author_links: CommittedLinkStorageIface,
        pending_authors: PendingListStorageIface,
    ):
        super().__init__(
            paper_registry, paper_info_storage,
            author_registry, author_info_storage,
            committed_author_links
        )
        self._pending_authors_manager = PendingListManager(
            self._author_manager._registry, pending_authors
        )

    async def get_pending_authors_for_paper(self, paper: Paper) -> list[Author] | None:
        """Get pending authors for paper."""
        paper_cid = await self._get_paper_canonical_id(paper)
        id_sets = await self._pending_authors_manager.get_pending_identifier_sets(paper_cid)
        if id_sets is None:
            return None
        return [Author(identifiers=ids) for ids in id_sets]

    async def add_pending_authors_for_paper(self, paper: Paper, authors: list[Author]) -> None:
        """Add pending authors for paper (registers them, merges with existing)."""
        paper_cid = await self._get_paper_canonical_id(paper)
        registered_sets = await self._pending_authors_manager.add_pending_identifier_sets(
            paper_cid, [a.identifiers for a in authors]
        )
        # Update author identifiers with merged sets
        for author, ids in zip(authors, registered_sets):
            author.identifiers = ids


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
