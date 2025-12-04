"""
Full Combined Cache - Complete cache implementation combining all capabilities.

Provides:
- FullWeaverCache: Combines Author2Papers, Paper2Authors, Paper2References, 
  Paper2Citations, and Paper2Venues for complete weaver functionality.
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
from .impl_p2v import Paper2VenuesCache
from .impl_v2p import Venue2PapersCache


class FullWeaverCache(Author2PapersCache, Paper2AuthorsCache, Paper2ReferencesCache, Paper2CitationsCache, Paper2VenuesCache, Venue2PapersCache):
    """
    Combined cache for paper operations (references, citations, authors, venues).
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
        committed_reference_links: CommittedLinkStorageIface,
        committed_venue_links: CommittedLinkStorageIface,
        pending_papers_by_author: PendingListStorageIface,
        pending_authors_by_paper: PendingListStorageIface,
        pending_references_by_paper: PendingListStorageIface,
        pending_citations_by_paper: PendingListStorageIface,
        pending_venues_by_paper: PendingListStorageIface,
        pending_papers_by_venue: PendingListStorageIface,
    ):
        ComposableCacheBase.__init__(
            self,
            paper_registry, paper_info_storage,
            author_registry, author_info_storage,
            venue_registry, venue_info_storage
        )
        self._committed_author_links = committed_author_links
        self._committed_reference_links = committed_reference_links
        self._committed_venue_links = committed_venue_links
        self._pending_authors_by_paper_manager = PendingListManager(
            self._author_manager._registry, pending_authors_by_paper
        )
        self._pending_papers_by_author_manager = PendingListManager(
            self._paper_manager._registry, pending_papers_by_author
        )
        self._pending_references_by_paper_manager = PendingListManager(
            self._paper_manager._registry, pending_references_by_paper
        )
        self._pending_citations_by_paper_manager = PendingListManager(
            self._paper_manager._registry, pending_citations_by_paper
        )
        self._pending_venues_by_paper_manager = PendingListManager(
            self._venue_manager._registry, pending_venues_by_paper
        )
        self._pending_papers_by_venue_manager = PendingListManager(
            self._paper_manager._registry, pending_papers_by_venue
        )
