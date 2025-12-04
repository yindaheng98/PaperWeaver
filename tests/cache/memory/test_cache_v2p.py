"""
Unit tests for Venue2PapersCache.
"""

import pytest

from paper_weaver.dataclass import Paper, Venue
from paper_weaver.cache import (
    MemoryIdentifierRegistry,
    MemoryInfoStorage,
    MemoryCommittedLinkStorage,
    MemoryPendingListStorage,
    Venue2PapersCache,
)


class TestVenue2PapersCache:
    """Tests for Venue2PapersCache."""

    @pytest.fixture
    def cache(self):
        return Venue2PapersCache(
            paper_registry=MemoryIdentifierRegistry(),
            paper_info_storage=MemoryInfoStorage(),
            author_registry=MemoryIdentifierRegistry(),
            author_info_storage=MemoryInfoStorage(),
            venue_registry=MemoryIdentifierRegistry(),
            venue_info_storage=MemoryInfoStorage(),
            committed_venue_links=MemoryCommittedLinkStorage(),
            pending_papers_by_venue=MemoryPendingListStorage(),
        )

    @pytest.mark.asyncio
    async def test_get_pending_papers_not_set(self, cache):
        """Test getting pending papers that haven't been set."""
        venue = Venue(identifiers={"issn:1234-5678"})
        result = await cache.get_pending_papers_for_venue(venue)
        assert result is None

    @pytest.mark.asyncio
    async def test_add_and_get_pending_papers(self, cache):
        """Test adding and getting pending papers."""
        venue = Venue(identifiers={"issn:1234-5678"})
        papers = [
            Paper(identifiers={"doi:123"}),
            Paper(identifiers={"doi:456"}),
        ]

        await cache.add_pending_papers_for_venue(venue, papers)
        result = await cache.get_pending_papers_for_venue(venue)

        assert len(result) == 2
        assert any("doi:123" in p.identifiers for p in result)
        assert any("doi:456" in p.identifiers for p in result)

    @pytest.mark.asyncio
    async def test_pending_papers_are_registered(self, cache):
        """Test that pending papers are registered in the registry."""
        venue = Venue(identifiers={"issn:1234-5678"})
        papers = [Paper(identifiers={"doi:123"})]

        await cache.add_pending_papers_for_venue(venue, papers)

        # Paper should be discoverable via iteration
        found_papers = []
        async for paper in cache.iterate_papers():
            found_papers.append(paper)

        assert len(found_papers) == 1
        assert "doi:123" in found_papers[0].identifiers

    @pytest.mark.asyncio
    async def test_venue_link_commitment(self, cache):
        """Test venue link commitment."""
        paper = Paper(identifiers={"doi:123"})
        venue = Venue(identifiers={"issn:1234-5678"})

        # Link should not be committed initially
        assert await cache.is_venue_link_committed(paper, venue) is False

        # Commit the link
        await cache.commit_venue_link(paper, venue)

        # Link should now be committed
        assert await cache.is_venue_link_committed(paper, venue) is True

    @pytest.mark.asyncio
    async def test_venue_link_works_with_merged_identifiers(self, cache):
        """Test that venue link checking works with merged identifiers."""
        paper = Paper(identifiers={"doi:123"})
        venue = Venue(identifiers={"issn:1234-5678"})

        await cache.commit_venue_link(paper, venue)

        # Check with additional identifiers
        paper2 = Paper(identifiers={"doi:123", "arxiv:456"})
        venue2 = Venue(identifiers={"issn:1234-5678", "dblp:conf/venue"})

        result = await cache.is_venue_link_committed(paper2, venue2)
        assert result is True

    @pytest.mark.asyncio
    async def test_paper_info_operations(self, cache):
        """Test paper info get/set operations."""
        paper = Paper(identifiers={"doi:123"})
        info = {"title": "Test Paper", "year": 2024}

        # Info should not exist initially
        paper, retrieved_info = await cache.get_paper_info(paper)
        assert retrieved_info is None

        # Set info
        await cache.set_paper_info(paper, info)

        # Info should now be retrievable
        paper, retrieved_info = await cache.get_paper_info(paper)
        assert retrieved_info == info

    @pytest.mark.asyncio
    async def test_paper_identifiers_merge_on_set(self, cache):
        """Test that paper identifiers are merged when setting info."""
        paper = Paper(identifiers={"doi:123", "arxiv:456"})
        await cache.set_paper_info(paper, {"title": "Test"})

        # Query with partial identifiers
        paper2 = Paper(identifiers={"doi:123"})
        paper2, info = await cache.get_paper_info(paper2)

        # Should have all identifiers
        assert "doi:123" in paper2.identifiers
        assert "arxiv:456" in paper2.identifiers

