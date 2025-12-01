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


from .identifier import IdentifierRegistryIface
from .info_storage import InfoStorageIface
from .link_storage import CommittedLinkStorageIface
from .pending_storage import PendingListStorageIface, PendingListManager

from .impl import ComposableCacheBase
from .impl_a2p import Author2PapersCache
from .impl_p2a import Paper2AuthorsCache
from .impl_p2c import Paper2CitationsCache
from .impl_p2r import Paper2ReferencesCache


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
