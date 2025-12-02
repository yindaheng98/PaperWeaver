"""
Unit tests for memory storage components.

Tests: MemoryIdentifierRegistry, MemoryInfoStorage, 
       MemoryCommittedLinkStorage, MemoryPendingListStorage
"""

import pytest

from paper_weaver.cache import (
    MemoryIdentifierRegistry,
    MemoryInfoStorage,
    MemoryCommittedLinkStorage,
    MemoryPendingListStorage,
)


class TestMemoryIdentifierRegistry:
    """Tests for MemoryIdentifierRegistry."""

    @pytest.fixture
    def registry(self):
        return MemoryIdentifierRegistry()

    @pytest.mark.asyncio
    async def test_register_new_identifiers(self, registry):
        """Test registering new identifiers creates a canonical ID."""
        canonical_id = await registry.register({"doi:123", "arxiv:456"})
        assert canonical_id is not None
        assert canonical_id.startswith("id_")

    @pytest.mark.asyncio
    async def test_get_canonical_id_not_registered(self, registry):
        """Test getting canonical ID for unregistered identifiers returns None."""
        result = await registry.get_canonical_id({"unknown:999"})
        assert result is None

    @pytest.mark.asyncio
    async def test_get_canonical_id_after_registration(self, registry):
        """Test getting canonical ID after registration."""
        await registry.register({"doi:123"})
        canonical_id = await registry.get_canonical_id({"doi:123"})
        assert canonical_id is not None

    @pytest.mark.asyncio
    async def test_register_merges_overlapping_identifiers(self, registry):
        """Test that registering overlapping identifiers merges them."""
        cid1 = await registry.register({"doi:123"})
        cid2 = await registry.register({"arxiv:456"})
        # Now register with both - should merge
        cid3 = await registry.register({"doi:123", "arxiv:456"})

        # After merge, both should resolve to same canonical ID
        all_ids = await registry.get_all_identifiers(cid3)
        assert "doi:123" in all_ids
        assert "arxiv:456" in all_ids

    @pytest.mark.asyncio
    async def test_get_all_identifiers(self, registry):
        """Test getting all identifiers for a canonical ID."""
        await registry.register({"doi:123", "arxiv:456", "pmid:789"})
        canonical_id = await registry.get_canonical_id({"doi:123"})
        all_ids = await registry.get_all_identifiers(canonical_id)
        assert all_ids == {"doi:123", "arxiv:456", "pmid:789"}

    @pytest.mark.asyncio
    async def test_iterate_canonical_ids(self, registry):
        """Test iterating over all canonical IDs."""
        await registry.register({"doi:1"})
        await registry.register({"doi:2"})
        await registry.register({"doi:3"})

        canonical_ids = []
        async for cid in registry.iterate_canonical_ids():
            canonical_ids.append(cid)

        assert len(canonical_ids) == 3

    @pytest.mark.asyncio
    async def test_register_same_identifiers_returns_same_canonical_id(self, registry):
        """Test registering same identifiers returns same canonical ID."""
        cid1 = await registry.register({"doi:123"})
        cid2 = await registry.register({"doi:123"})
        assert cid1 == cid2

    @pytest.mark.asyncio
    async def test_merge_multiple_canonical_ids(self, registry):
        """Test merging multiple distinct canonical IDs into one."""
        cid1 = await registry.register({"id:A"})
        cid2 = await registry.register({"id:B"})
        cid3 = await registry.register({"id:C"})

        # Merge A, B, C by registering overlapping sets
        await registry.register({"id:A", "id:B"})
        await registry.register({"id:B", "id:C"})

        # All should now be under the same canonical ID
        final_cid = await registry.get_canonical_id({"id:C"})
        all_ids = await registry.get_all_identifiers(final_cid)
        assert {"id:A", "id:B", "id:C"}.issubset(all_ids)


class TestMemoryInfoStorage:
    """Tests for MemoryInfoStorage."""

    @pytest.fixture
    def storage(self):
        return MemoryInfoStorage()

    @pytest.mark.asyncio
    async def test_get_info_not_set(self, storage):
        """Test getting info that hasn't been set returns None."""
        result = await storage.get_info("cid_123")
        assert result is None

    @pytest.mark.asyncio
    async def test_set_and_get_info(self, storage):
        """Test setting and getting info."""
        info = {"title": "Test Paper", "year": 2024}
        await storage.set_info("cid_123", info)
        result = await storage.get_info("cid_123")
        assert result == info

    @pytest.mark.asyncio
    async def test_overwrite_info(self, storage):
        """Test overwriting existing info."""
        await storage.set_info("cid_123", {"title": "Old"})
        await storage.set_info("cid_123", {"title": "New"})
        result = await storage.get_info("cid_123")
        assert result["title"] == "New"


class TestMemoryCommittedLinkStorage:
    """Tests for MemoryCommittedLinkStorage."""

    @pytest.fixture
    def storage(self):
        return MemoryCommittedLinkStorage()

    @pytest.mark.asyncio
    async def test_is_link_committed_not_set(self, storage):
        """Test checking uncommitted link returns False."""
        result = await storage.is_link_committed("paper1", "author1")
        assert result is False

    @pytest.mark.asyncio
    async def test_commit_and_check_link(self, storage):
        """Test committing and checking a link."""
        await storage.commit_link("paper1", "author1")
        result = await storage.is_link_committed("paper1", "author1")
        assert result is True

    @pytest.mark.asyncio
    async def test_link_directionality(self, storage):
        """Test that links are directional (A->B != B->A)."""
        await storage.commit_link("paper1", "author1")
        # Forward direction should be committed
        assert await storage.is_link_committed("paper1", "author1") is True
        # Reverse direction should NOT be committed
        assert await storage.is_link_committed("author1", "paper1") is False

    @pytest.mark.asyncio
    async def test_multiple_links_from_same_source(self, storage):
        """Test multiple links from the same source."""
        await storage.commit_link("paper1", "author1")
        await storage.commit_link("paper1", "author2")
        await storage.commit_link("paper1", "author3")

        assert await storage.is_link_committed("paper1", "author1") is True
        assert await storage.is_link_committed("paper1", "author2") is True
        assert await storage.is_link_committed("paper1", "author3") is True
        assert await storage.is_link_committed("paper1", "author4") is False


class TestMemoryPendingListStorage:
    """Tests for MemoryPendingListStorage."""

    @pytest.fixture
    def storage(self):
        return MemoryPendingListStorage()

    @pytest.mark.asyncio
    async def test_get_pending_not_set(self, storage):
        """Test getting pending list that hasn't been set returns None."""
        result = await storage.get_pending_identifier_sets("author1")
        assert result is None

    @pytest.mark.asyncio
    async def test_set_and_get_pending(self, storage):
        """Test setting and getting pending list."""
        items = [{"doi:1"}, {"doi:2", "arxiv:2"}]
        await storage.set_pending_identifier_sets("author1", items)
        result = await storage.get_pending_identifier_sets("author1")
        assert len(result) == 2
        assert {"doi:1"} in result
        assert {"doi:2", "arxiv:2"} in result

    @pytest.mark.asyncio
    async def test_set_empty_list_vs_not_set(self, storage):
        """Test that empty list is different from not set."""
        await storage.set_pending_identifier_sets("author1", [])
        result = await storage.get_pending_identifier_sets("author1")
        assert result is not None
        assert result == []

    @pytest.mark.asyncio
    async def test_overwrite_pending(self, storage):
        """Test overwriting pending list."""
        await storage.set_pending_identifier_sets("author1", [{"doi:1"}])
        await storage.set_pending_identifier_sets("author1", [{"doi:2"}, {"doi:3"}])
        result = await storage.get_pending_identifier_sets("author1")
        assert len(result) == 2
