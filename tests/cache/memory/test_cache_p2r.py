"""
Unit tests for Paper2ReferencesCache.
"""

import pytest

from paper_weaver.dataclass import Paper
from paper_weaver.cache import (
    MemoryIdentifierRegistry,
    MemoryInfoStorage,
    MemoryCommittedLinkStorage,
    MemoryPendingListStorage,
    Paper2ReferencesCache,
)


class TestPaper2ReferencesCache:
    """Tests for Paper2ReferencesCache."""

    @pytest.fixture
    def cache(self):
        return Paper2ReferencesCache(
            paper_registry=MemoryIdentifierRegistry(),
            paper_info_storage=MemoryInfoStorage(),
            author_registry=MemoryIdentifierRegistry(),
            author_info_storage=MemoryInfoStorage(),
            venue_registry=MemoryIdentifierRegistry(),
            venue_info_storage=MemoryInfoStorage(),
            committed_reference_links=MemoryCommittedLinkStorage(),
            pending_references_by_paper=MemoryPendingListStorage(),
        )

    @pytest.mark.asyncio
    async def test_get_pending_references_not_set(self, cache):
        """Test getting pending references that haven't been set."""
        paper = Paper(identifiers={"doi:123"})
        result = await cache.get_pending_references_for_paper(paper)
        assert result is None

    @pytest.mark.asyncio
    async def test_add_and_get_pending_references(self, cache):
        """Test adding and getting pending references."""
        paper = Paper(identifiers={"doi:123"})
        references = [
            Paper(identifiers={"doi:ref1"}),
            Paper(identifiers={"doi:ref2"}),
        ]

        await cache.add_pending_references_for_paper(paper, references)
        result = await cache.get_pending_references_for_paper(paper)

        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_reference_link_commitment(self, cache):
        """Test reference link commitment."""
        paper = Paper(identifiers={"doi:123"})
        reference = Paper(identifiers={"doi:ref1"})

        await cache.commit_reference_link(paper, reference)
        assert await cache.is_reference_link_committed(paper, reference) is True
