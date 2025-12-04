"""
Unit tests for Paper2CitationsCache.
"""

import pytest

from paper_weaver.dataclass import Paper
from paper_weaver.cache import (
    MemoryIdentifierRegistry,
    MemoryInfoStorage,
    MemoryCommittedLinkStorage,
    MemoryPendingListStorage,
    Paper2CitationsCache,
)


class TestPaper2CitationsCache:
    """Tests for Paper2CitationsCache."""

    @pytest.fixture
    def cache(self):
        return Paper2CitationsCache(
            paper_registry=MemoryIdentifierRegistry(),
            paper_info_storage=MemoryInfoStorage(),
            author_registry=MemoryIdentifierRegistry(),
            author_info_storage=MemoryInfoStorage(),
            venue_registry=MemoryIdentifierRegistry(),
            venue_info_storage=MemoryInfoStorage(),
            committed_reference_links=MemoryCommittedLinkStorage(),
            pending_citations_by_paper=MemoryPendingListStorage(),
        )

    @pytest.mark.asyncio
    async def test_get_pending_citations_not_set(self, cache):
        """Test getting pending citations that haven't been set."""
        paper = Paper(identifiers={"doi:123"})
        result = await cache.get_pending_citations_for_paper(paper)
        assert result is None

    @pytest.mark.asyncio
    async def test_add_and_get_pending_citations(self, cache):
        """Test adding and getting pending citations."""
        paper = Paper(identifiers={"doi:123"})
        citations = [
            Paper(identifiers={"doi:cit1"}),
            Paper(identifiers={"doi:cit2"}),
        ]

        await cache.add_pending_citations_for_paper(paper, citations)
        result = await cache.get_pending_citations_for_paper(paper)

        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_citation_link_commitment(self, cache):
        """Test citation link commitment."""
        paper = Paper(identifiers={"doi:123"})
        citation = Paper(identifiers={"doi:cit1"})

        await cache.commit_citation_link(paper, citation)
        assert await cache.is_citation_link_committed(paper, citation) is True
