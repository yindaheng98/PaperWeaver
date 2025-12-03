"""
Unit tests for Paper2VenuesCache.
"""

import pytest

from paper_weaver.dataclass import Paper, Venue
from paper_weaver.cache import (
    MemoryIdentifierRegistry,
    MemoryInfoStorage,
    MemoryCommittedLinkStorage,
    MemoryPendingListStorage,
    Paper2VenuesCache,
)


class TestPaper2VenuesCache:
    """Tests for Paper2VenuesCache."""

    @pytest.fixture
    def cache(self):
        return Paper2VenuesCache(
            paper_registry=MemoryIdentifierRegistry(),
            paper_info_storage=MemoryInfoStorage(),
            author_registry=MemoryIdentifierRegistry(),
            author_info_storage=MemoryInfoStorage(),
            venue_registry=MemoryIdentifierRegistry(),
            venue_info_storage=MemoryInfoStorage(),
            committed_venue_links=MemoryCommittedLinkStorage(),
            pending_venues=MemoryPendingListStorage(),
        )

    @pytest.mark.asyncio
    async def test_get_pending_venues_not_set(self, cache):
        """Test getting pending venues that haven't been set."""
        paper = Paper(identifiers={"doi:123"})
        result = await cache.get_pending_venues_for_paper(paper)
        assert result is None

    @pytest.mark.asyncio
    async def test_add_and_get_pending_venues(self, cache):
        """Test adding and getting pending venues."""
        paper = Paper(identifiers={"doi:123"})
        venues = [
            Venue(identifiers={"issn:1234-5678"}),
            Venue(identifiers={"issn:8765-4321"}),
        ]

        await cache.add_pending_venues_for_paper(paper, venues)
        result = await cache.get_pending_venues_for_paper(paper)

        assert len(result) == 2
        assert any("issn:1234-5678" in v.identifiers for v in result)
        assert any("issn:8765-4321" in v.identifiers for v in result)

    @pytest.mark.asyncio
    async def test_pending_venues_are_registered(self, cache):
        """Test that pending venues are registered in the registry."""
        paper = Paper(identifiers={"doi:123"})
        venues = [Venue(identifiers={"issn:1234-5678"})]

        await cache.add_pending_venues_for_paper(paper, venues)

        # Venue should be discoverable via iteration
        found_venues = []
        async for venue in cache.iterate_venues():
            found_venues.append(venue)

        assert len(found_venues) == 1
        assert "issn:1234-5678" in found_venues[0].identifiers

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
    async def test_venue_info_operations(self, cache):
        """Test venue info get/set operations."""
        venue = Venue(identifiers={"issn:1234-5678"})
        info = {"name": "Test Conference", "year": 2024}

        # Info should not exist initially
        venue, retrieved_info = await cache.get_venue_info(venue)
        assert retrieved_info is None

        # Set info
        await cache.set_venue_info(venue, info)

        # Info should now be retrievable
        venue, retrieved_info = await cache.get_venue_info(venue)
        assert retrieved_info == info

    @pytest.mark.asyncio
    async def test_venue_identifiers_merge_on_set(self, cache):
        """Test that venue identifiers are merged when setting info."""
        venue = Venue(identifiers={"issn:1234-5678", "dblp:conf/venue"})
        await cache.set_venue_info(venue, {"name": "Test"})

        # Query with partial identifiers
        venue2 = Venue(identifiers={"issn:1234-5678"})
        venue2, info = await cache.get_venue_info(venue2)

        # Should have all identifiers
        assert "issn:1234-5678" in venue2.identifiers
        assert "dblp:conf/venue" in venue2.identifiers

