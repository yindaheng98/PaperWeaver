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

from typing import AsyncIterator, Tuple

from ..dataclass import Paper, Author
from ..iface import WeaverCacheIface
from ..iface_link import AuthorLinkWeaverCacheIface, PaperLinkWeaverCacheIface
from ..iface_a2p import Author2PapersWeaverCacheIface
from ..iface_p2a import Paper2AuthorsWeaverCacheIface
from ..iface_p2c import Paper2CitationsWeaverCacheIface
from ..iface_p2r import Paper2ReferencesWeaverCacheIface

from .identifier import IdentifierRegistryIface
from .info_storage import InfoStorageIface, EntityInfoManager
from .link_storage import CommittedLinkStorageIface, PendingListStorageIface, PendingListManager


class ComposableCacheBase(WeaverCacheIface):
    """
    Base cache with composable info storage for papers and authors.

    Components:
    - paper_registry: Identifier registry for papers
    - paper_info_storage: Info storage for papers
    - author_registry: Identifier registry for authors
    - author_info_storage: Info storage for authors
    """

    def __init__(
        self,
        paper_registry: IdentifierRegistryIface,
        paper_info_storage: InfoStorageIface,
        author_registry: IdentifierRegistryIface,
        author_info_storage: InfoStorageIface,
    ):
        self._paper_manager = EntityInfoManager(paper_registry, paper_info_storage)
        self._author_manager = EntityInfoManager(author_registry, author_info_storage)

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

    # Iteration methods

    def iterate_papers(self) -> AsyncIterator[Paper]:
        """Iterate over all papers without info."""
        return self._iterate_papers_impl()

    async def _iterate_papers_impl(self) -> AsyncIterator[Paper]:
        async for canonical_id, identifiers in self._paper_manager.iterate_entities():
            yield Paper(identifiers=identifiers)

    def iterate_authors(self) -> AsyncIterator[Author]:
        """Iterate over all authors without info."""
        return self._iterate_authors_impl()

    async def _iterate_authors_impl(self) -> AsyncIterator[Author]:
        async for canonical_id, identifiers in self._author_manager.iterate_entities():
            yield Author(identifiers=identifiers)


class AuthorLinkCache(ComposableCacheBase, AuthorLinkWeaverCacheIface):
    """
    Cache with author-paper committed link tracking.

    Additional components:
    - committed_author_links: Storage for tracking paper-author links written to DataDst
    """

    def __init__(
        self,
        paper_registry: IdentifierRegistryIface,
        paper_info_storage: InfoStorageIface,
        author_registry: IdentifierRegistryIface,
        author_info_storage: InfoStorageIface,
        committed_author_links: CommittedLinkStorageIface,
    ):
        super().__init__(paper_registry, paper_info_storage, author_registry, author_info_storage)
        self._committed_author_links = committed_author_links

    async def _get_paper_canonical_id(self, paper: Paper) -> str:
        """Get or create canonical ID for paper."""
        canonical_id, all_identifiers = await self._paper_manager.register_identifiers(paper.identifiers)
        paper.identifiers = all_identifiers
        return canonical_id

    async def _get_author_canonical_id(self, author: Author) -> str:
        """Get or create canonical ID for author."""
        canonical_id, all_identifiers = await self._author_manager.register_identifiers(author.identifiers)
        author.identifiers = all_identifiers
        return canonical_id

    async def is_author_link_committed(self, paper: Paper, author: Author) -> bool:
        """Check if paper-author link has been committed to DataDst."""
        paper_cid = await self._get_paper_canonical_id(paper)
        author_cid = await self._get_author_canonical_id(author)
        return await self._committed_author_links.is_link_committed(paper_cid, author_cid)

    async def commit_author_link(self, paper: Paper, author: Author) -> None:
        """Mark paper-author link as committed to DataDst."""
        paper_cid = await self._get_paper_canonical_id(paper)
        author_cid = await self._get_author_canonical_id(author)
        await self._committed_author_links.commit_link(paper_cid, author_cid)


class PaperLinkCache(ComposableCacheBase, PaperLinkWeaverCacheIface):
    """
    Cache with paper-paper committed link tracking (references/citations).

    Additional components:
    - committed_reference_links: Storage for tracking paper-reference links written to DataDst
    """

    def __init__(
        self,
        paper_registry: IdentifierRegistryIface,
        paper_info_storage: InfoStorageIface,
        author_registry: IdentifierRegistryIface,
        author_info_storage: InfoStorageIface,
        committed_reference_links: CommittedLinkStorageIface,
    ):
        super().__init__(paper_registry, paper_info_storage, author_registry, author_info_storage)
        self._committed_reference_links = committed_reference_links

    async def _get_paper_canonical_id(self, paper: Paper) -> str:
        """Get or create canonical ID for paper."""
        canonical_id, all_identifiers = await self._paper_manager.register_identifiers(paper.identifiers)
        paper.identifiers = all_identifiers
        return canonical_id

    async def is_reference_link_committed(self, paper: Paper, reference: Paper) -> bool:
        """Check if paper-reference link has been committed to DataDst."""
        paper_cid = await self._get_paper_canonical_id(paper)
        ref_cid = await self._get_paper_canonical_id(reference)
        return await self._committed_reference_links.is_link_committed(paper_cid, ref_cid)

    async def commit_reference_link(self, paper: Paper, reference: Paper) -> None:
        """Mark paper-reference link as committed to DataDst."""
        paper_cid = await self._get_paper_canonical_id(paper)
        ref_cid = await self._get_paper_canonical_id(reference)
        await self._committed_reference_links.commit_link(paper_cid, ref_cid)

    # is_citation_link_committed and commit_citation_link inherited from PaperLinkWeaverCacheIface


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

    async def get_pending_papers(self, author: Author) -> list[Paper] | None:
        """Get pending papers for author."""
        author_cid = await self._get_author_canonical_id(author)
        id_sets = await self._pending_papers_manager.get_pending_identifier_sets(author_cid)
        if id_sets is None:
            return None
        return [Paper(identifiers=ids) for ids in id_sets]

    async def set_pending_papers(self, author: Author, papers: list[Paper]) -> None:
        """Set pending papers for author (registers them for later processing)."""
        author_cid = await self._get_author_canonical_id(author)
        registered_sets = await self._pending_papers_manager.set_pending_identifier_sets(
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

    async def get_pending_authors(self, paper: Paper) -> list[Author] | None:
        """Get pending authors for paper."""
        paper_cid = await self._get_paper_canonical_id(paper)
        id_sets = await self._pending_authors_manager.get_pending_identifier_sets(paper_cid)
        if id_sets is None:
            return None
        return [Author(identifiers=ids) for ids in id_sets]

    async def set_pending_authors(self, paper: Paper, authors: list[Author]) -> None:
        """Set pending authors for paper (registers them for later processing)."""
        paper_cid = await self._get_paper_canonical_id(paper)
        registered_sets = await self._pending_authors_manager.set_pending_identifier_sets(
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

    async def get_pending_references(self, paper: Paper) -> list[Paper] | None:
        """Get pending references for paper."""
        paper_cid = await self._get_paper_canonical_id(paper)
        id_sets = await self._pending_references_manager.get_pending_identifier_sets(paper_cid)
        if id_sets is None:
            return None
        return [Paper(identifiers=ids) for ids in id_sets]

    async def set_pending_references(self, paper: Paper, references: list[Paper]) -> None:
        """Set pending references for paper (registers them for later processing)."""
        paper_cid = await self._get_paper_canonical_id(paper)
        registered_sets = await self._pending_references_manager.set_pending_identifier_sets(
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

    async def get_pending_citations(self, paper: Paper) -> list[Paper] | None:
        """Get pending citations for paper."""
        paper_cid = await self._get_paper_canonical_id(paper)
        id_sets = await self._pending_citations_manager.get_pending_identifier_sets(paper_cid)
        if id_sets is None:
            return None
        return [Paper(identifiers=ids) for ids in id_sets]

    async def set_pending_citations(self, paper: Paper, citations: list[Paper]) -> None:
        """Set pending citations for paper (registers them for later processing)."""
        paper_cid = await self._get_paper_canonical_id(paper)
        registered_sets = await self._pending_citations_manager.set_pending_identifier_sets(
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
