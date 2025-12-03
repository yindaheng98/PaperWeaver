"""
Unit tests for Author2PapersCache.
"""

import pytest

from paper_weaver.dataclass import Paper, Author
from paper_weaver.cache import (
    MemoryIdentifierRegistry,
    MemoryInfoStorage,
    MemoryCommittedLinkStorage,
    MemoryPendingListStorage,
    Author2PapersCache,
)


class TestAuthor2PapersCache:
    """Tests for Author2PapersCache."""

    @pytest.fixture
    def cache(self):
        return Author2PapersCache(
            paper_registry=MemoryIdentifierRegistry(),
            paper_info_storage=MemoryInfoStorage(),
            author_registry=MemoryIdentifierRegistry(),
            author_info_storage=MemoryInfoStorage(),
            venue_registry=MemoryIdentifierRegistry(),
            venue_info_storage=MemoryInfoStorage(),
            committed_author_links=MemoryCommittedLinkStorage(),
            pending_papers=MemoryPendingListStorage(),
        )

    @pytest.mark.asyncio
    async def test_get_pending_papers_not_set(self, cache):
        """Test getting pending papers that haven't been set."""
        author = Author(identifiers={"orcid:0001"})
        result = await cache.get_pending_papers_for_author(author)
        assert result is None

    @pytest.mark.asyncio
    async def test_add_and_get_pending_papers(self, cache):
        """Test adding and getting pending papers."""
        author = Author(identifiers={"orcid:0001"})
        papers = [
            Paper(identifiers={"doi:1"}),
            Paper(identifiers={"doi:2"}),
        ]

        await cache.add_pending_papers_for_author(author, papers)
        result = await cache.get_pending_papers_for_author(author)

        assert len(result) == 2
        assert any("doi:1" in p.identifiers for p in result)
        assert any("doi:2" in p.identifiers for p in result)

    @pytest.mark.asyncio
    async def test_pending_papers_are_registered(self, cache):
        """Test that pending papers are registered in the registry."""
        author = Author(identifiers={"orcid:0001"})
        papers = [Paper(identifiers={"doi:1"})]

        await cache.add_pending_papers_for_author(author, papers)

        # Paper should be discoverable via iteration
        found_papers = []
        async for paper in cache.iterate_papers():
            found_papers.append(paper)

        assert len(found_papers) == 1
        assert "doi:1" in found_papers[0].identifiers
