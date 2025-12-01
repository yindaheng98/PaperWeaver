"""
Composite Cache - Combines storage components into full cache implementations.

Allows flexible composition of different storage backends for:
- Identifier registry (memory/redis)
- Info storage (memory/redis)
- Link storage (memory/redis)
"""

from typing import AsyncIterator, List, Tuple, Optional

from ..dataclass import Paper, Author
from ..iface import WeaverCacheIface
from ..iface_link import AuthorLinkWeaverCacheIface, PaperLinkWeaverCacheIface
from ..iface_a2p import Author2PapersWeaverCacheIface
from ..iface_p2a import Paper2AuthorsWeaverCacheIface
from ..iface_p2c import Paper2CitationsWeaverCacheIface
from ..iface_p2r import Paper2ReferencesWeaverCacheIface

from .identifier import IdentifierRegistryIface
from .info_storage import InfoStorageIface, EntityInfoManager
from .link_storage import LinkStorageIface, EntityListStorageIface


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
    Cache with author-paper link storage.

    Additional components:
    - author_paper_links: Link storage for paper -> author relationships
    """

    def __init__(
        self,
        paper_registry: IdentifierRegistryIface,
        paper_info_storage: InfoStorageIface,
        author_registry: IdentifierRegistryIface,
        author_info_storage: InfoStorageIface,
        author_paper_links: LinkStorageIface,
    ):
        super().__init__(paper_registry, paper_info_storage, author_registry, author_info_storage)
        self._author_paper_links = author_paper_links

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

    async def is_link_author(self, paper: Paper, author: Author) -> bool:
        """Check if author is linked to paper."""
        paper_cid = await self._get_paper_canonical_id(paper)
        author_cid = await self._get_author_canonical_id(author)
        return await self._author_paper_links.has_link(paper_cid, author_cid)

    async def link_author(self, paper: Paper, author: Author) -> None:
        """Link author to paper."""
        paper_cid = await self._get_paper_canonical_id(paper)
        author_cid = await self._get_author_canonical_id(author)
        await self._author_paper_links.add_link(paper_cid, author_cid)


class PaperLinkCache(ComposableCacheBase, PaperLinkWeaverCacheIface):
    """
    Cache with paper-paper link storage (references/citations).

    Additional components:
    - paper_reference_links: Link storage for paper -> reference relationships
    """

    def __init__(
        self,
        paper_registry: IdentifierRegistryIface,
        paper_info_storage: InfoStorageIface,
        author_registry: IdentifierRegistryIface,
        author_info_storage: InfoStorageIface,
        paper_reference_links: LinkStorageIface,
    ):
        super().__init__(paper_registry, paper_info_storage, author_registry, author_info_storage)
        self._paper_reference_links = paper_reference_links

    async def _get_paper_canonical_id(self, paper: Paper) -> str:
        """Get or create canonical ID for paper."""
        canonical_id, all_identifiers = await self._paper_manager.register_identifiers(paper.identifiers)
        paper.identifiers = all_identifiers
        return canonical_id

    async def is_link_reference(self, paper: Paper, reference: Paper) -> bool:
        """Check if reference is linked to paper."""
        paper_cid = await self._get_paper_canonical_id(paper)
        ref_cid = await self._get_paper_canonical_id(reference)
        return await self._paper_reference_links.has_link(paper_cid, ref_cid)

    async def link_reference(self, paper: Paper, reference: Paper) -> None:
        """Link reference to paper."""
        paper_cid = await self._get_paper_canonical_id(paper)
        ref_cid = await self._get_paper_canonical_id(reference)
        await self._paper_reference_links.add_link(paper_cid, ref_cid)

    # is_link_citation and link_citation inherited from PaperLinkWeaverCacheIface


class Author2PapersCache(AuthorLinkCache, Author2PapersWeaverCacheIface):
    """
    Cache for author -> papers relationships.

    Additional components:
    - author_papers_list: Entity list storage for author's papers
    """

    def __init__(
        self,
        paper_registry: IdentifierRegistryIface,
        paper_info_storage: InfoStorageIface,
        author_registry: IdentifierRegistryIface,
        author_info_storage: InfoStorageIface,
        author_paper_links: LinkStorageIface,
        author_papers_list: EntityListStorageIface,
    ):
        super().__init__(
            paper_registry, paper_info_storage,
            author_registry, author_info_storage,
            author_paper_links
        )
        self._author_papers_list = author_papers_list

    async def get_papers_by_author(self, author: Author) -> list[Paper] | None:
        """Get papers by author, registering them if found."""
        author_cid = await self._get_author_canonical_id(author)
        paper_id_sets = await self._author_papers_list.get_list(author_cid)
        if paper_id_sets is None:
            return None

        papers = []
        for id_set in paper_id_sets:
            # Register each paper's identifiers to get merged set
            _, all_identifiers = await self._paper_manager.register_identifiers(id_set)
            papers.append(Paper(identifiers=all_identifiers))
        return papers

    async def set_papers_of_author(self, author: Author, papers: list[Paper]) -> None:
        """Set papers for author."""
        author_cid = await self._get_author_canonical_id(author)

        # Register each paper and store their identifier sets
        paper_id_sets = []
        for paper in papers:
            _, all_identifiers = await self._paper_manager.register_identifiers(paper.identifiers)
            paper.identifiers = all_identifiers
            paper_id_sets.append(all_identifiers)

        await self._author_papers_list.set_list(author_cid, paper_id_sets)


class Paper2AuthorsCache(AuthorLinkCache, Paper2AuthorsWeaverCacheIface):
    """
    Cache for paper -> authors relationships.

    Additional components:
    - paper_authors_list: Entity list storage for paper's authors
    """

    def __init__(
        self,
        paper_registry: IdentifierRegistryIface,
        paper_info_storage: InfoStorageIface,
        author_registry: IdentifierRegistryIface,
        author_info_storage: InfoStorageIface,
        author_paper_links: LinkStorageIface,
        paper_authors_list: EntityListStorageIface,
    ):
        super().__init__(
            paper_registry, paper_info_storage,
            author_registry, author_info_storage,
            author_paper_links
        )
        self._paper_authors_list = paper_authors_list

    async def get_authors_by_paper(self, paper: Paper) -> list[Author] | None:
        """Get authors by paper, registering them if found."""
        paper_cid = await self._get_paper_canonical_id(paper)
        author_id_sets = await self._paper_authors_list.get_list(paper_cid)
        if author_id_sets is None:
            return None

        authors = []
        for id_set in author_id_sets:
            _, all_identifiers = await self._author_manager.register_identifiers(id_set)
            authors.append(Author(identifiers=all_identifiers))
        return authors

    async def set_authors_of_paper(self, paper: Paper, authors: list[Author]) -> None:
        """Set authors for paper."""
        paper_cid = await self._get_paper_canonical_id(paper)

        author_id_sets = []
        for author in authors:
            _, all_identifiers = await self._author_manager.register_identifiers(author.identifiers)
            author.identifiers = all_identifiers
            author_id_sets.append(all_identifiers)

        await self._paper_authors_list.set_list(paper_cid, author_id_sets)


class Paper2ReferencesCache(PaperLinkCache, Paper2ReferencesWeaverCacheIface):
    """
    Cache for paper -> references relationships.

    Additional components:
    - paper_references_list: Entity list storage for paper's references
    """

    def __init__(
        self,
        paper_registry: IdentifierRegistryIface,
        paper_info_storage: InfoStorageIface,
        author_registry: IdentifierRegistryIface,
        author_info_storage: InfoStorageIface,
        paper_reference_links: LinkStorageIface,
        paper_references_list: EntityListStorageIface,
    ):
        super().__init__(
            paper_registry, paper_info_storage,
            author_registry, author_info_storage,
            paper_reference_links
        )
        self._paper_references_list = paper_references_list

    async def get_references_by_paper(self, paper: Paper) -> list[Paper] | None:
        """Get references by paper, registering them if found."""
        paper_cid = await self._get_paper_canonical_id(paper)
        ref_id_sets = await self._paper_references_list.get_list(paper_cid)
        if ref_id_sets is None:
            return None

        refs = []
        for id_set in ref_id_sets:
            _, all_identifiers = await self._paper_manager.register_identifiers(id_set)
            refs.append(Paper(identifiers=all_identifiers))
        return refs

    async def set_references_of_paper(self, paper: Paper, references: list[Paper]) -> None:
        """Set references for paper."""
        paper_cid = await self._get_paper_canonical_id(paper)

        ref_id_sets = []
        for ref in references:
            _, all_identifiers = await self._paper_manager.register_identifiers(ref.identifiers)
            ref.identifiers = all_identifiers
            ref_id_sets.append(all_identifiers)

        await self._paper_references_list.set_list(paper_cid, ref_id_sets)


class Paper2CitationsCache(PaperLinkCache, Paper2CitationsWeaverCacheIface):
    """
    Cache for paper -> citations relationships.

    Additional components:
    - paper_citations_list: Entity list storage for paper's citations
    """

    def __init__(
        self,
        paper_registry: IdentifierRegistryIface,
        paper_info_storage: InfoStorageIface,
        author_registry: IdentifierRegistryIface,
        author_info_storage: InfoStorageIface,
        paper_reference_links: LinkStorageIface,
        paper_citations_list: EntityListStorageIface,
    ):
        super().__init__(
            paper_registry, paper_info_storage,
            author_registry, author_info_storage,
            paper_reference_links
        )
        self._paper_citations_list = paper_citations_list

    async def get_citations_by_paper(self, paper: Paper) -> list[Paper] | None:
        """Get citations by paper, registering them if found."""
        paper_cid = await self._get_paper_canonical_id(paper)
        cit_id_sets = await self._paper_citations_list.get_list(paper_cid)
        if cit_id_sets is None:
            return None

        cits = []
        for id_set in cit_id_sets:
            _, all_identifiers = await self._paper_manager.register_identifiers(id_set)
            cits.append(Paper(identifiers=all_identifiers))
        return cits

    async def set_citations_of_paper(self, paper: Paper, citations: list[Paper]) -> None:
        """Set citations for paper."""
        paper_cid = await self._get_paper_canonical_id(paper)

        cit_id_sets = []
        for cit in citations:
            _, all_identifiers = await self._paper_manager.register_identifiers(cit.identifiers)
            cit.identifiers = all_identifiers
            cit_id_sets.append(all_identifiers)

        await self._paper_citations_list.set_list(paper_cid, cit_id_sets)


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
        author_paper_links: LinkStorageIface,
        author_papers_list: EntityListStorageIface,
        paper_authors_list: EntityListStorageIface,
    ):
        # Initialize base with shared components
        ComposableCacheBase.__init__(
            self,
            paper_registry, paper_info_storage,
            author_registry, author_info_storage
        )
        self._author_paper_links = author_paper_links
        self._author_papers_list = author_papers_list
        self._paper_authors_list = paper_authors_list


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
        author_paper_links: LinkStorageIface,
        paper_reference_links: LinkStorageIface,
        paper_authors_list: EntityListStorageIface,
        paper_references_list: EntityListStorageIface,
        paper_citations_list: EntityListStorageIface,
    ):
        ComposableCacheBase.__init__(
            self,
            paper_registry, paper_info_storage,
            author_registry, author_info_storage
        )
        self._author_paper_links = author_paper_links
        self._paper_reference_links = paper_reference_links
        self._paper_authors_list = paper_authors_list
        self._paper_references_list = paper_references_list
        self._paper_citations_list = paper_citations_list
