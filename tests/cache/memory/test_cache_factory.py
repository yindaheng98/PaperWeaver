"""
Unit tests for cache factory functions and builders.

Tests: HybridCacheBuilder, create_memory_weaver_cache, etc.
"""

import pytest

from paper_weaver.dataclass import Paper
from paper_weaver.cache import (
    HybridCacheBuilder,
)


class TestHybridCacheBuilder:
    """Tests for HybridCacheBuilder."""

    @pytest.mark.asyncio
    async def test_build_weaver_cache(self):
        """Test building weaver cache with defaults."""
        builder = HybridCacheBuilder()
        cache = builder.build_weaver_cache()

        # Verify it works
        paper = Paper(identifiers={"doi:123"})
        await cache.set_paper_info(paper, {"title": "Test"})
        paper, info = await cache.get_paper_info(paper)
        assert info["title"] == "Test"

    @pytest.mark.asyncio
    async def test_with_all_memory(self):
        """Test with_all_memory convenience method."""
        builder = HybridCacheBuilder().with_all_memory()
        cache = builder.build_weaver_cache()

        # Verify it works
        paper = Paper(identifiers={"doi:123"})
        await cache.set_paper_info(paper, {"title": "Test"})
        paper, info = await cache.get_paper_info(paper)
        assert info["title"] == "Test"

    @pytest.mark.asyncio
    async def test_build_author2papers_cache(self):
        """Test building Author2PapersCache."""
        builder = HybridCacheBuilder().with_all_memory()
        cache = builder.build_author2papers_cache()

        paper = Paper(identifiers={"doi:123"})
        await cache.set_paper_info(paper, {"title": "Test"})
        paper, info = await cache.get_paper_info(paper)
        assert info["title"] == "Test"

    @pytest.mark.asyncio
    async def test_build_paper2authors_cache(self):
        """Test building Paper2AuthorsCache."""
        builder = HybridCacheBuilder().with_all_memory()
        cache = builder.build_paper2authors_cache()

        paper = Paper(identifiers={"doi:123"})
        await cache.set_paper_info(paper, {"title": "Test"})
        paper, info = await cache.get_paper_info(paper)
        assert info["title"] == "Test"

    @pytest.mark.asyncio
    async def test_build_paper2references_cache(self):
        """Test building Paper2ReferencesCache."""
        builder = HybridCacheBuilder().with_all_memory()
        cache = builder.build_paper2references_cache()

        paper = Paper(identifiers={"doi:123"})
        await cache.set_paper_info(paper, {"title": "Test"})
        paper, info = await cache.get_paper_info(paper)
        assert info["title"] == "Test"

    @pytest.mark.asyncio
    async def test_build_paper2citations_cache(self):
        """Test building Paper2CitationsCache."""
        builder = HybridCacheBuilder().with_all_memory()
        cache = builder.build_paper2citations_cache()

        paper = Paper(identifiers={"doi:123"})
        await cache.set_paper_info(paper, {"title": "Test"})
        paper, info = await cache.get_paper_info(paper)
        assert info["title"] == "Test"

    @pytest.mark.asyncio
    async def test_build_paper2venues_cache(self):
        """Test building Paper2VenuesCache."""
        builder = HybridCacheBuilder().with_all_memory()
        cache = builder.build_paper2venues_cache()

        paper = Paper(identifiers={"doi:123"})
        await cache.set_paper_info(paper, {"title": "Test"})
        paper, info = await cache.get_paper_info(paper)
        assert info["title"] == "Test"

    @pytest.mark.asyncio
    async def test_build_venue2papers_cache(self):
        """Test building Venue2PapersCache."""
        builder = HybridCacheBuilder().with_all_memory()
        cache = builder.build_venue2papers_cache()

        paper = Paper(identifiers={"doi:123"})
        await cache.set_paper_info(paper, {"title": "Test"})
        paper, info = await cache.get_paper_info(paper)
        assert info["title"] == "Test"
