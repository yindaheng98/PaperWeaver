"""
Unit tests for ComposableCacheBase.
"""

import pytest

from paper_weaver.dataclass import Paper, Author, Venue
from paper_weaver.cache import (
    MemoryIdentifierRegistry,
    MemoryInfoStorage,
    ComposableCacheBase,
)


class TestComposableCacheBase:
    """Tests for ComposableCacheBase."""

    @pytest.fixture
    def cache(self):
        return ComposableCacheBase(
            paper_registry=MemoryIdentifierRegistry(),
            paper_info_storage=MemoryInfoStorage(),
            author_registry=MemoryIdentifierRegistry(),
            author_info_storage=MemoryInfoStorage(),
            venue_registry=MemoryIdentifierRegistry(),
            venue_info_storage=MemoryInfoStorage(),
        )

    @pytest.mark.asyncio
    async def test_get_paper_info_not_set(self, cache):
        """Test getting paper info that hasn't been set."""
        paper = Paper(identifiers={"doi:123"})
        paper, info = await cache.get_paper_info(paper)
        assert info is None

    @pytest.mark.asyncio
    async def test_set_and_get_paper_info(self, cache):
        """Test setting and getting paper info."""
        paper = Paper(identifiers={"doi:123"})
        info = {"title": "Test Paper", "year": 2024}

        await cache.set_paper_info(paper, info)
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

    @pytest.mark.asyncio
    async def test_get_author_info_not_set(self, cache):
        """Test getting author info that hasn't been set."""
        author = Author(identifiers={"orcid:0000-0001"})
        author, info = await cache.get_author_info(author)
        assert info is None

    @pytest.mark.asyncio
    async def test_set_and_get_author_info(self, cache):
        """Test setting and getting author info."""
        author = Author(identifiers={"orcid:0000-0001"})
        info = {"name": "John Doe"}

        await cache.set_author_info(author, info)
        author, retrieved_info = await cache.get_author_info(author)

        assert retrieved_info == info

    @pytest.mark.asyncio
    async def test_iterate_papers(self, cache):
        """Test iterating over registered papers."""
        paper1 = Paper(identifiers={"doi:1"})
        paper2 = Paper(identifiers={"doi:2"})

        await cache.set_paper_info(paper1, {"title": "Paper 1"})
        await cache.set_paper_info(paper2, {"title": "Paper 2"})

        papers = []
        async for paper in cache.iterate_papers():
            papers.append(paper)

        assert len(papers) == 2

    @pytest.mark.asyncio
    async def test_iterate_authors(self, cache):
        """Test iterating over registered authors."""
        author1 = Author(identifiers={"orcid:1"})
        author2 = Author(identifiers={"orcid:2"})

        await cache.set_author_info(author1, {"name": "Author 1"})
        await cache.set_author_info(author2, {"name": "Author 2"})

        authors = []
        async for author in cache.iterate_authors():
            authors.append(author)

        assert len(authors) == 2

    @pytest.mark.asyncio
    async def test_get_venue_info_not_set(self, cache):
        """Test getting venue info that hasn't been set."""
        venue = Venue(identifiers={"issn:1234-5678"})
        venue, info = await cache.get_venue_info(venue)
        assert info is None

    @pytest.mark.asyncio
    async def test_set_and_get_venue_info(self, cache):
        """Test setting and getting venue info."""
        venue = Venue(identifiers={"issn:1234-5678"})
        info = {"name": "Test Conference", "year": 2024}

        await cache.set_venue_info(venue, info)
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

    @pytest.mark.asyncio
    async def test_iterate_venues(self, cache):
        """Test iterating over registered venues."""
        venue1 = Venue(identifiers={"issn:1111"})
        venue2 = Venue(identifiers={"issn:2222"})

        await cache.set_venue_info(venue1, {"name": "Venue 1"})
        await cache.set_venue_info(venue2, {"name": "Venue 2"})

        venues = []
        async for venue in cache.iterate_venues():
            venues.append(venue)

        assert len(venues) == 2
