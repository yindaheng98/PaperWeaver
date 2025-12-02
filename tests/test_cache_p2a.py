"""
Unit tests for Paper2AuthorsCache.
"""

import pytest

from paper_weaver.dataclass import Paper, Author
from paper_weaver.cache import (
    MemoryIdentifierRegistry,
    MemoryInfoStorage,
    MemoryCommittedLinkStorage,
    MemoryPendingListStorage,
    Paper2AuthorsCache,
)


class TestPaper2AuthorsCache:
    """Tests for Paper2AuthorsCache."""

    @pytest.fixture
    def cache(self):
        return Paper2AuthorsCache(
            paper_registry=MemoryIdentifierRegistry(),
            paper_info_storage=MemoryInfoStorage(),
            author_registry=MemoryIdentifierRegistry(),
            author_info_storage=MemoryInfoStorage(),
            committed_author_links=MemoryCommittedLinkStorage(),
            pending_authors=MemoryPendingListStorage(),
        )

    @pytest.mark.asyncio
    async def test_get_pending_authors_not_set(self, cache):
        """Test getting pending authors that haven't been set."""
        paper = Paper(identifiers={"doi:123"})
        result = await cache.get_pending_authors_for_paper(paper)
        assert result is None

    @pytest.mark.asyncio
    async def test_add_and_get_pending_authors(self, cache):
        """Test adding and getting pending authors."""
        paper = Paper(identifiers={"doi:123"})
        authors = [
            Author(identifiers={"orcid:1"}),
            Author(identifiers={"orcid:2"}),
        ]
        
        await cache.add_pending_authors_for_paper(paper, authors)
        result = await cache.get_pending_authors_for_paper(paper)
        
        assert len(result) == 2
        assert any("orcid:1" in a.identifiers for a in result)
        assert any("orcid:2" in a.identifiers for a in result)

    @pytest.mark.asyncio
    async def test_pending_authors_are_registered(self, cache):
        """Test that pending authors are registered in the registry."""
        paper = Paper(identifiers={"doi:123"})
        authors = [Author(identifiers={"orcid:1"})]
        
        await cache.add_pending_authors_for_paper(paper, authors)
        
        # Author should be discoverable via iteration
        found_authors = []
        async for author in cache.iterate_authors():
            found_authors.append(author)
        
        assert len(found_authors) == 1
        assert "orcid:1" in found_authors[0].identifiers

