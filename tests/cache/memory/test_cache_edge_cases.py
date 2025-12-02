"""
Edge case tests for cache implementations.
"""

import pytest
import asyncio

from paper_weaver.dataclass import Paper, Author
from paper_weaver.cache import create_memory_author_weaver_cache


class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    @pytest.fixture
    def cache(self):
        return create_memory_author_weaver_cache()

    @pytest.mark.asyncio
    async def test_empty_identifiers_set(self, cache):
        """Test handling of empty identifiers set."""
        # Note: In practice, entities should always have at least one identifier
        # This tests the edge case behavior
        paper = Paper(identifiers=set())

        # Should handle gracefully
        paper, info = await cache.get_paper_info(paper)
        assert info is None

    @pytest.mark.asyncio
    async def test_identifier_merging_across_operations(self, cache):
        """Test that identifier merging works across different operations."""
        # Create paper with identifier A
        paper_a = Paper(identifiers={"doi:A"})
        await cache.set_paper_info(paper_a, {"title": "Paper A"})

        # Create paper with identifier B
        paper_b = Paper(identifiers={"doi:B"})
        await cache.set_paper_info(paper_b, {"title": "Paper B"})

        # Now query with both identifiers - should merge
        paper_ab = Paper(identifiers={"doi:A", "doi:B"})
        paper_ab, info = await cache.get_paper_info(paper_ab)

        # After merge, both identifiers should be present
        assert "doi:A" in paper_ab.identifiers
        assert "doi:B" in paper_ab.identifiers

    @pytest.mark.asyncio
    async def test_concurrent_access(self, cache):
        """Test that concurrent access is handled correctly."""
        async def add_paper(i):
            paper = Paper(identifiers={f"doi:{i}"})
            await cache.set_paper_info(paper, {"title": f"Paper {i}"})

        # Run many concurrent operations
        await asyncio.gather(*[add_paper(i) for i in range(100)])

        # Verify all papers were added
        papers = []
        async for paper in cache.iterate_papers():
            papers.append(paper)

        assert len(papers) == 100

    @pytest.mark.asyncio
    async def test_pending_list_with_duplicate_entities(self, cache):
        """Test pending list handling when same entity is added multiple times."""
        author = Author(identifiers={"orcid:0001"})

        # Add same paper twice (with same identifier)
        papers1 = [Paper(identifiers={"doi:123"})]
        papers2 = [Paper(identifiers={"doi:123"})]

        await cache.add_pending_papers_for_author(author, papers1)
        await cache.add_pending_papers_for_author(author, papers2)

        # Should only have one paper (deduplicated by canonical ID)
        result = await cache.get_pending_papers_for_author(author)
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_pending_list_merges_overlapping_identifiers(self, cache):
        """Test that pending list merges overlapping identifiers."""
        author = Author(identifiers={"orcid:0001"})

        # Add paper with one identifier
        papers1 = [Paper(identifiers={"doi:123"})]
        await cache.add_pending_papers_for_author(author, papers1)

        # Add same paper with additional identifier
        papers2 = [Paper(identifiers={"doi:123", "arxiv:456"})]
        await cache.add_pending_papers_for_author(author, papers2)

        # Should have one paper with merged identifiers
        result = await cache.get_pending_papers_for_author(author)
        assert len(result) == 1
        assert "doi:123" in result[0].identifiers
        assert "arxiv:456" in result[0].identifiers

    @pytest.mark.asyncio
    async def test_link_persists_after_identifier_merge(self, cache):
        """Test that committed links persist after identifier merging."""
        author = Author(identifiers={"orcid:0001"})
        paper = Paper(identifiers={"doi:123"})

        # Commit link
        await cache.commit_author_link(paper, author)

        # Query with different but overlapping identifiers
        paper2 = Paper(identifiers={"doi:123", "arxiv:456"})
        author2 = Author(identifiers={"orcid:0001", "scopus:0001"})

        # Link should still be found
        assert await cache.is_author_link_committed(paper2, author2) is True

    @pytest.mark.asyncio
    async def test_iterate_returns_merged_identifiers(self, cache):
        """Test that iteration returns entities with all merged identifiers."""
        # Add paper with one identifier
        paper1 = Paper(identifiers={"doi:123"})
        await cache.set_paper_info(paper1, {"title": "Test"})

        # Query with additional identifier to trigger merge
        paper2 = Paper(identifiers={"doi:123", "arxiv:456"})
        await cache.get_paper_info(paper2)

        # Iteration should return paper with merged identifiers
        found = False
        async for paper in cache.iterate_papers():
            if "doi:123" in paper.identifiers:
                assert "arxiv:456" in paper.identifiers
                found = True
        assert found

    @pytest.mark.asyncio
    async def test_info_overwrite_preserves_identifiers(self, cache):
        """Test that overwriting info preserves all identifiers."""
        paper = Paper(identifiers={"doi:123", "arxiv:456"})
        await cache.set_paper_info(paper, {"title": "v1"})

        paper2 = Paper(identifiers={"doi:123"})
        await cache.set_paper_info(paper2, {"title": "v2"})

        paper3 = Paper(identifiers={"arxiv:456"})
        paper3, info = await cache.get_paper_info(paper3)

        assert info["title"] == "v2"
        assert "doi:123" in paper3.identifiers
        assert "arxiv:456" in paper3.identifiers
