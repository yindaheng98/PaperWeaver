"""
Unit tests for link cache classes.

Tests: AuthorLinkCache, PaperLinkCache, VenueLinkCache
"""

import pytest

from paper_weaver.dataclass import Paper, Author, Venue
from paper_weaver.cache import (
    MemoryIdentifierRegistry,
    MemoryInfoStorage,
    MemoryCommittedLinkStorage,
    AuthorLinkCache,
    PaperLinkCache,
    VenueLinkCache,
)


class TestAuthorLinkCache:
    """Tests for AuthorLinkCache."""

    @pytest.fixture
    def cache(self):
        return AuthorLinkCache(
            paper_registry=MemoryIdentifierRegistry(),
            paper_info_storage=MemoryInfoStorage(),
            author_registry=MemoryIdentifierRegistry(),
            author_info_storage=MemoryInfoStorage(),
            venue_registry=MemoryIdentifierRegistry(),
            venue_info_storage=MemoryInfoStorage(),
            committed_author_links=MemoryCommittedLinkStorage(),
        )

    @pytest.mark.asyncio
    async def test_is_author_link_committed_not_set(self, cache):
        """Test checking uncommitted author link."""
        paper = Paper(identifiers={"doi:123"})
        author = Author(identifiers={"orcid:0001"})

        result = await cache.is_author_link_committed(paper, author)
        assert result is False

    @pytest.mark.asyncio
    async def test_commit_and_check_author_link(self, cache):
        """Test committing and checking author link."""
        paper = Paper(identifiers={"doi:123"})
        author = Author(identifiers={"orcid:0001"})

        await cache.commit_author_link(paper, author)
        result = await cache.is_author_link_committed(paper, author)

        assert result is True

    @pytest.mark.asyncio
    async def test_link_works_with_merged_identifiers(self, cache):
        """Test that link checking works with merged identifiers."""
        paper = Paper(identifiers={"doi:123"})
        author = Author(identifiers={"orcid:0001"})

        await cache.commit_author_link(paper, author)

        # Check with additional identifiers
        paper2 = Paper(identifiers={"doi:123", "arxiv:456"})
        author2 = Author(identifiers={"orcid:0001", "scopus:0001"})

        result = await cache.is_author_link_committed(paper2, author2)
        assert result is True


class TestPaperLinkCache:
    """Tests for PaperLinkCache."""

    @pytest.fixture
    def cache(self):
        return PaperLinkCache(
            paper_registry=MemoryIdentifierRegistry(),
            paper_info_storage=MemoryInfoStorage(),
            author_registry=MemoryIdentifierRegistry(),
            author_info_storage=MemoryInfoStorage(),
            venue_registry=MemoryIdentifierRegistry(),
            venue_info_storage=MemoryInfoStorage(),
            committed_reference_links=MemoryCommittedLinkStorage(),
        )

    @pytest.mark.asyncio
    async def test_is_reference_link_committed_not_set(self, cache):
        """Test checking uncommitted reference link."""
        paper = Paper(identifiers={"doi:123"})
        reference = Paper(identifiers={"doi:456"})

        result = await cache.is_reference_link_committed(paper, reference)
        assert result is False

    @pytest.mark.asyncio
    async def test_commit_and_check_reference_link(self, cache):
        """Test committing and checking reference link."""
        paper = Paper(identifiers={"doi:123"})
        reference = Paper(identifiers={"doi:456"})

        await cache.commit_reference_link(paper, reference)
        result = await cache.is_reference_link_committed(paper, reference)

        assert result is True

    @pytest.mark.asyncio
    async def test_citation_link_is_inverse_of_reference(self, cache):
        """Test that citation link is inverse of reference link."""
        paper = Paper(identifiers={"doi:123"})
        citation = Paper(identifiers={"doi:456"})

        # "paper is cited by citation" means "citation references paper"
        await cache.commit_citation_link(paper, citation)

        # Check: paper is cited by citation
        assert await cache.is_citation_link_committed(paper, citation) is True
        # Internally: citation references paper
        assert await cache.is_reference_link_committed(citation, paper) is True


class TestVenueLinkCache:
    """Tests for VenueLinkCache."""

    @pytest.fixture
    def cache(self):
        return VenueLinkCache(
            paper_registry=MemoryIdentifierRegistry(),
            paper_info_storage=MemoryInfoStorage(),
            author_registry=MemoryIdentifierRegistry(),
            author_info_storage=MemoryInfoStorage(),
            venue_registry=MemoryIdentifierRegistry(),
            venue_info_storage=MemoryInfoStorage(),
            committed_venue_links=MemoryCommittedLinkStorage(),
        )

    @pytest.mark.asyncio
    async def test_is_venue_link_committed_not_set(self, cache):
        """Test checking uncommitted venue link."""
        paper = Paper(identifiers={"doi:123"})
        venue = Venue(identifiers={"issn:1234-5678"})

        result = await cache.is_venue_link_committed(paper, venue)
        assert result is False

    @pytest.mark.asyncio
    async def test_commit_and_check_venue_link(self, cache):
        """Test committing and checking venue link."""
        paper = Paper(identifiers={"doi:123"})
        venue = Venue(identifiers={"issn:1234-5678"})

        await cache.commit_venue_link(paper, venue)
        result = await cache.is_venue_link_committed(paper, venue)

        assert result is True

    @pytest.mark.asyncio
    async def test_link_works_with_merged_identifiers(self, cache):
        """Test that link checking works with merged identifiers."""
        paper = Paper(identifiers={"doi:123"})
        venue = Venue(identifiers={"issn:1234-5678"})

        await cache.commit_venue_link(paper, venue)

        # Check with additional identifiers
        paper2 = Paper(identifiers={"doi:123", "arxiv:456"})
        venue2 = Venue(identifiers={"issn:1234-5678", "dblp:conf/venue"})

        result = await cache.is_venue_link_committed(paper2, venue2)
        assert result is True
