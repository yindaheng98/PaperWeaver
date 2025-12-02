"""
Unit tests for manager classes.

Tests: EntityInfoManager, PendingListManager
"""

import pytest

from paper_weaver.cache import (
    MemoryIdentifierRegistry,
    MemoryInfoStorage,
    MemoryPendingListStorage,
    EntityInfoManager,
    PendingListManager,
)


class TestEntityInfoManager:
    """Tests for EntityInfoManager."""

    @pytest.fixture
    def manager(self):
        registry = MemoryIdentifierRegistry()
        storage = MemoryInfoStorage()
        return EntityInfoManager(registry, storage)

    @pytest.mark.asyncio
    async def test_get_info_unregistered(self, manager):
        """Test getting info for unregistered entity."""
        canonical_id, all_ids, info = await manager.get_info({"doi:123"})
        assert canonical_id is None
        assert all_ids == {"doi:123"}
        assert info is None

    @pytest.mark.asyncio
    async def test_set_and_get_info(self, manager):
        """Test setting and getting info."""
        info = {"title": "Test Paper"}
        await manager.set_info({"doi:123"}, info)

        canonical_id, all_ids, retrieved_info = await manager.get_info({"doi:123"})
        assert canonical_id is not None
        assert "doi:123" in all_ids
        assert retrieved_info == info

    @pytest.mark.asyncio
    async def test_register_identifiers(self, manager):
        """Test registering identifiers without setting info."""
        canonical_id, all_ids = await manager.register_identifiers({"doi:123", "arxiv:456"})
        assert canonical_id is not None
        assert "doi:123" in all_ids
        assert "arxiv:456" in all_ids

    @pytest.mark.asyncio
    async def test_identifier_merging_on_get_info(self, manager):
        """Test that get_info merges identifiers."""
        await manager.set_info({"doi:123"}, {"title": "Test"})

        # Query with additional identifier
        canonical_id, all_ids, info = await manager.get_info({"doi:123", "arxiv:456"})

        # Both identifiers should now be associated
        assert "doi:123" in all_ids
        assert "arxiv:456" in all_ids

    @pytest.mark.asyncio
    async def test_iterate_entities(self, manager):
        """Test iterating over registered entities."""
        await manager.set_info({"doi:1"}, {"title": "Paper 1"})
        await manager.set_info({"doi:2"}, {"title": "Paper 2"})

        entities = []
        async for canonical_id, identifiers in manager.iterate_entities():
            entities.append((canonical_id, identifiers))

        assert len(entities) == 2


class TestPendingListManager:
    """Tests for PendingListManager."""

    @pytest.fixture
    def manager(self):
        registry = MemoryIdentifierRegistry()
        storage = MemoryPendingListStorage()
        return PendingListManager(registry, storage)

    @pytest.mark.asyncio
    async def test_get_pending_not_set(self, manager):
        """Test getting pending list that hasn't been set."""
        result = await manager.get_pending_identifier_sets("source_cid")
        assert result is None

    @pytest.mark.asyncio
    async def test_add_and_get_pending(self, manager):
        """Test adding and getting pending list."""
        items = [{"doi:1"}, {"doi:2"}]
        await manager.add_pending_identifier_sets("source_cid", items)
        result = await manager.get_pending_identifier_sets("source_cid")
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_add_pending_registers_entities(self, manager):
        """Test that adding pending list registers entities in registry."""
        items = [{"doi:1"}, {"doi:2"}]
        await manager.add_pending_identifier_sets("source_cid", items)

        # Entities should now be registered
        result = await manager.get_pending_canonical_id_identifier_set_dict("source_cid")
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_add_pending_merges_identifiers(self, manager):
        """Test that adding pending items merges with existing identifiers."""
        # First add
        await manager.add_pending_identifier_sets("source_cid", [{"doi:1"}])

        # Second add with overlapping identifier
        await manager.add_pending_identifier_sets("source_cid", [{"doi:1", "arxiv:1"}])

        result = await manager.get_pending_identifier_sets("source_cid")
        # Should still be 1 entity, but with merged identifiers
        assert len(result) == 1
        assert "doi:1" in result[0]
        assert "arxiv:1" in result[0]

    @pytest.mark.asyncio
    async def test_add_pending_appends_new_entities(self, manager):
        """Test that adding new pending items appends to list."""
        await manager.add_pending_identifier_sets("source_cid", [{"doi:1"}])
        await manager.add_pending_identifier_sets("source_cid", [{"doi:2"}])

        result = await manager.get_pending_identifier_sets("source_cid")
        assert len(result) == 2
