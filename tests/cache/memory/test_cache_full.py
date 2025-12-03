"""
Unit tests for full cache implementations.

Tests: FullAuthorWeaverCache, FullPaperWeaverCache
"""

import pytest

from paper_weaver.dataclass import Paper, Author, Venue
from paper_weaver.cache import (
    create_memory_author_weaver_cache,
    create_memory_paper_weaver_cache,
)


class TestFullAuthorWeaverCache:
    """Tests for FullAuthorWeaverCache."""

    @pytest.fixture
    def cache(self):
        return create_memory_author_weaver_cache()

    @pytest.mark.asyncio
    async def test_paper_info_operations(self, cache):
        """Test paper info get/set operations."""
        paper = Paper(identifiers={"doi:123"})
        info = {"title": "Test Paper"}

        await cache.set_paper_info(paper, info)
        paper, retrieved = await cache.get_paper_info(paper)

        assert retrieved == info

    @pytest.mark.asyncio
    async def test_author_info_operations(self, cache):
        """Test author info get/set operations."""
        author = Author(identifiers={"orcid:0001"})
        info = {"name": "John Doe"}

        await cache.set_author_info(author, info)
        author, retrieved = await cache.get_author_info(author)

        assert retrieved == info

    @pytest.mark.asyncio
    async def test_author_to_papers_workflow(self, cache):
        """Test full author -> papers workflow."""
        author = Author(identifiers={"orcid:0001"})
        papers = [
            Paper(identifiers={"doi:1"}),
            Paper(identifiers={"doi:2"}),
        ]

        # Add pending papers
        await cache.add_pending_papers_for_author(author, papers)

        # Get pending papers
        result = await cache.get_pending_papers_for_author(author)
        assert len(result) == 2

        # Commit links
        for paper in papers:
            await cache.commit_author_link(paper, author)

        # Verify links committed
        for paper in papers:
            assert await cache.is_author_link_committed(paper, author) is True

    @pytest.mark.asyncio
    async def test_paper_to_authors_workflow(self, cache):
        """Test full paper -> authors workflow."""
        paper = Paper(identifiers={"doi:123"})
        authors = [
            Author(identifiers={"orcid:1"}),
            Author(identifiers={"orcid:2"}),
        ]

        # Add pending authors
        await cache.add_pending_authors_for_paper(paper, authors)

        # Get pending authors
        result = await cache.get_pending_authors_for_paper(paper)
        assert len(result) == 2

        # Commit links
        for author in authors:
            await cache.commit_author_link(paper, author)

        # Verify links committed
        for author in authors:
            assert await cache.is_author_link_committed(paper, author) is True


class TestFullPaperWeaverCache:
    """Tests for FullPaperWeaverCache."""

    @pytest.fixture
    def cache(self):
        return create_memory_paper_weaver_cache()

    @pytest.mark.asyncio
    async def test_paper_to_references_workflow(self, cache):
        """Test full paper -> references workflow."""
        paper = Paper(identifiers={"doi:123"})
        references = [
            Paper(identifiers={"doi:ref1"}),
            Paper(identifiers={"doi:ref2"}),
        ]

        # Add pending references
        await cache.add_pending_references_for_paper(paper, references)

        # Get pending references
        result = await cache.get_pending_references_for_paper(paper)
        assert len(result) == 2

        # Commit links
        for ref in references:
            await cache.commit_reference_link(paper, ref)

        # Verify links committed
        for ref in references:
            assert await cache.is_reference_link_committed(paper, ref) is True

    @pytest.mark.asyncio
    async def test_paper_to_citations_workflow(self, cache):
        """Test full paper -> citations workflow."""
        paper = Paper(identifiers={"doi:123"})
        citations = [
            Paper(identifiers={"doi:cit1"}),
            Paper(identifiers={"doi:cit2"}),
        ]

        # Add pending citations
        await cache.add_pending_citations_for_paper(paper, citations)

        # Get pending citations
        result = await cache.get_pending_citations_for_paper(paper)
        assert len(result) == 2

        # Commit links
        for cit in citations:
            await cache.commit_citation_link(paper, cit)

        # Verify links committed
        for cit in citations:
            assert await cache.is_citation_link_committed(paper, cit) is True

    @pytest.mark.asyncio
    async def test_paper_to_venues_workflow(self, cache):
        """Test full paper -> venues workflow."""
        paper = Paper(identifiers={"doi:123"})
        venues = [
            Venue(identifiers={"issn:1234-5678"}),
            Venue(identifiers={"issn:8765-4321"}),
        ]

        # Add pending venues
        await cache.add_pending_venues_for_paper(paper, venues)

        # Get pending venues
        result = await cache.get_pending_venues_for_paper(paper)
        assert len(result) == 2

        # Commit links
        for venue in venues:
            await cache.commit_venue_link(paper, venue)

        # Verify links committed
        for venue in venues:
            assert await cache.is_venue_link_committed(paper, venue) is True
