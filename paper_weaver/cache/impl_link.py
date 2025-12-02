"""
Link Cache - Caches with committed link tracking.

Provides:
- AuthorLinkCache: Tracks paper-author links committed to DataDst
- PaperLinkCache: Tracks paper-paper links (references/citations) committed to DataDst

Committed links represent relationships that have been written to DataDst.
"""


from ..dataclass import Paper, Author
from ..iface_link import AuthorLinkWeaverCacheIface, PaperLinkWeaverCacheIface

from .identifier import IdentifierRegistryIface
from .info_storage import InfoStorageIface
from .link_storage import CommittedLinkStorageIface

from .impl import ComposableCacheBase


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
