"""
Unit tests for Redis storage components.

These tests verify that Redis implementations have identical behavior
to Memory implementations. Uses real Redis server if available at localhost:6379,
otherwise falls back to fakeredis for testing.

Run with: pytest tests/test_redis_storage.py -v
Requires: pip install redis (and optionally fakeredis as fallback)
"""

import pytest
import pytest_asyncio

# Try to import redis library
try:
    import redis.asyncio as aioredis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

# Try to import fakeredis as fallback
try:
    import fakeredis.aioredis
    FAKEREDIS_AVAILABLE = True
except ImportError:
    FAKEREDIS_AVAILABLE = False

from paper_weaver.cache import (
    # Memory implementations
    MemoryIdentifierRegistry,
    MemoryInfoStorage,
    MemoryCommittedLinkStorage,
    MemoryPendingListStorage,
    # Redis implementations
    RedisIdentifierRegistry,
    RedisInfoStorage,
    RedisCommittedLinkStorage,
    RedisPendingListStorage,
)


def _check_real_redis_connection():
    """Check if real Redis server is available at localhost:6379."""
    if not REDIS_AVAILABLE:
        return False
    try:
        import redis
        client = redis.Redis(host='localhost', port=6379)
        client.ping()
        client.close()
        return True
    except Exception:
        return False


REAL_REDIS_AVAILABLE = _check_real_redis_connection()

# Skip tests if neither real Redis nor fakeredis is available
pytestmark = pytest.mark.skipif(
    not REAL_REDIS_AVAILABLE and not FAKEREDIS_AVAILABLE,
    reason="Neither real Redis server (localhost:6379) nor fakeredis is available"
)


# =============================================================================
# Fixtures
# =============================================================================

@pytest_asyncio.fixture
async def redis_client():
    """
    Create a Redis client for testing.
    Uses real Redis at localhost:6379 if available, otherwise falls back to fakeredis.
    """
    if REAL_REDIS_AVAILABLE:
        # Use real Redis server
        client = aioredis.Redis(host='localhost', port=6379, db=15)  # Use db=15 for testing
        # Clean the test database before use
        await client.flushdb()
        yield client
        # Clean up after test
        await client.flushdb()
        await client.aclose()
    else:
        # Fall back to fakeredis
        client = fakeredis.aioredis.FakeRedis()
        yield client


@pytest.fixture
def memory_identifier_registry():
    return MemoryIdentifierRegistry()


@pytest.fixture
def redis_identifier_registry(redis_client):
    return RedisIdentifierRegistry(redis_client, "test_idreg", expire=111)


@pytest.fixture
def memory_info_storage():
    return MemoryInfoStorage()


@pytest.fixture
def redis_info_storage(redis_client):
    return RedisInfoStorage(redis_client, "test_info", expire=112)


@pytest.fixture
def memory_link_storage():
    return MemoryCommittedLinkStorage()


@pytest.fixture
def redis_link_storage(redis_client):
    return RedisCommittedLinkStorage(redis_client, "test_links", expire=113)


@pytest.fixture
def memory_pending_storage():
    return MemoryPendingListStorage()


@pytest.fixture
def redis_pending_storage(redis_client):
    return RedisPendingListStorage(redis_client, "test_pending", expire=114)


# =============================================================================
# Test: IdentifierRegistry - Memory vs Redis behavior parity
# =============================================================================

class TestIdentifierRegistryParity:
    """
    Test that RedisIdentifierRegistry behaves identically to MemoryIdentifierRegistry.
    """

    @pytest.mark.asyncio
    async def test_register_returns_canonical_id(
        self, memory_identifier_registry, redis_identifier_registry
    ):
        """Both should return a canonical ID when registering."""
        mem_cid = await memory_identifier_registry.register({"doi:123"})
        redis_cid = await redis_identifier_registry.register({"doi:123"})

        assert mem_cid is not None
        assert redis_cid is not None
        # Both should start with "id_"
        assert mem_cid.startswith("id_")
        assert redis_cid.startswith("id_")

    @pytest.mark.asyncio
    async def test_get_canonical_id_unregistered_returns_none(
        self, memory_identifier_registry, redis_identifier_registry
    ):
        """Both should return None for unregistered identifiers."""
        mem_result = await memory_identifier_registry.get_canonical_id({"unknown:999"})
        redis_result = await redis_identifier_registry.get_canonical_id({"unknown:999"})

        assert mem_result is None
        assert redis_result is None

    @pytest.mark.asyncio
    async def test_get_canonical_id_after_register(
        self, memory_identifier_registry, redis_identifier_registry
    ):
        """Both should find canonical ID after registration."""
        await memory_identifier_registry.register({"doi:123"})
        await redis_identifier_registry.register({"doi:123"})

        mem_cid = await memory_identifier_registry.get_canonical_id({"doi:123"})
        redis_cid = await redis_identifier_registry.get_canonical_id({"doi:123"})

        assert mem_cid is not None
        assert redis_cid is not None

    @pytest.mark.asyncio
    async def test_register_same_identifiers_returns_same_id(
        self, memory_identifier_registry, redis_identifier_registry
    ):
        """Registering same identifiers should return same canonical ID."""
        mem_cid1 = await memory_identifier_registry.register({"doi:123"})
        mem_cid2 = await memory_identifier_registry.register({"doi:123"})

        redis_cid1 = await redis_identifier_registry.register({"doi:123"})
        redis_cid2 = await redis_identifier_registry.register({"doi:123"})

        assert mem_cid1 == mem_cid2
        assert redis_cid1 == redis_cid2

    @pytest.mark.asyncio
    async def test_get_all_identifiers(
        self, memory_identifier_registry, redis_identifier_registry
    ):
        """Both should return all identifiers for a canonical ID."""
        identifiers = {"doi:123", "arxiv:456", "pmid:789"}

        mem_cid = await memory_identifier_registry.register(identifiers)
        redis_cid = await redis_identifier_registry.register(identifiers)

        mem_all = await memory_identifier_registry.get_all_identifiers(mem_cid)
        redis_all = await redis_identifier_registry.get_all_identifiers(redis_cid)

        assert mem_all == identifiers
        assert redis_all == identifiers

    @pytest.mark.asyncio
    async def test_identifier_merging(
        self, memory_identifier_registry, redis_identifier_registry
    ):
        """Both should merge identifiers when registering overlapping sets."""
        # Register separate identifiers
        await memory_identifier_registry.register({"doi:A"})
        await memory_identifier_registry.register({"doi:B"})

        await redis_identifier_registry.register({"doi:A"})
        await redis_identifier_registry.register({"doi:B"})

        # Register overlapping set
        mem_cid = await memory_identifier_registry.register({"doi:A", "doi:B"})
        redis_cid = await redis_identifier_registry.register({"doi:A", "doi:B"})

        # Both should have merged identifiers
        mem_all = await memory_identifier_registry.get_all_identifiers(mem_cid)
        redis_all = await redis_identifier_registry.get_all_identifiers(redis_cid)

        assert "doi:A" in mem_all and "doi:B" in mem_all
        assert "doi:A" in redis_all and "doi:B" in redis_all

    @pytest.mark.asyncio
    async def test_iterate_canonical_ids(
        self, memory_identifier_registry, redis_identifier_registry
    ):
        """Both should iterate over all canonical IDs."""
        await memory_identifier_registry.register({"doi:1"})
        await memory_identifier_registry.register({"doi:2"})
        await memory_identifier_registry.register({"doi:3"})

        await redis_identifier_registry.register({"doi:1"})
        await redis_identifier_registry.register({"doi:2"})
        await redis_identifier_registry.register({"doi:3"})

        mem_cids = []
        async for cid in memory_identifier_registry.iterate_canonical_ids():
            mem_cids.append(cid)

        redis_cids = []
        async for cid in redis_identifier_registry.iterate_canonical_ids():
            redis_cids.append(cid)

        assert len(mem_cids) == 3
        assert len(redis_cids) == 3

    @pytest.mark.asyncio
    async def test_get_all_identifiers_empty_canonical(
        self, memory_identifier_registry, redis_identifier_registry
    ):
        """Both should return empty set for non-existent canonical ID."""
        mem_all = await memory_identifier_registry.get_all_identifiers("nonexistent")
        redis_all = await redis_identifier_registry.get_all_identifiers("nonexistent")

        assert mem_all == set()
        assert redis_all == set()


# =============================================================================
# Test: InfoStorage - Memory vs Redis behavior parity
# =============================================================================

class TestInfoStorageParity:
    """
    Test that RedisInfoStorage behaves identically to MemoryInfoStorage.
    """

    @pytest.mark.asyncio
    async def test_get_info_not_set_returns_none(
        self, memory_info_storage, redis_info_storage
    ):
        """Both should return None when info not set."""
        mem_result = await memory_info_storage.get_info("cid_123")
        redis_result = await redis_info_storage.get_info("cid_123")

        assert mem_result is None
        assert redis_result is None

    @pytest.mark.asyncio
    async def test_set_and_get_info(
        self, memory_info_storage, redis_info_storage
    ):
        """Both should store and retrieve info correctly."""
        info = {"title": "Test Paper", "year": 2024, "authors": ["Alice", "Bob"]}

        await memory_info_storage.set_info("cid_123", info)
        await redis_info_storage.set_info("cid_123", info)

        mem_result = await memory_info_storage.get_info("cid_123")
        redis_result = await redis_info_storage.get_info("cid_123")

        assert mem_result == info
        assert redis_result == info

    @pytest.mark.asyncio
    async def test_overwrite_info(
        self, memory_info_storage, redis_info_storage
    ):
        """Both should overwrite existing info."""
        await memory_info_storage.set_info("cid_123", {"title": "Old"})
        await memory_info_storage.set_info("cid_123", {"title": "New"})

        await redis_info_storage.set_info("cid_123", {"title": "Old"})
        await redis_info_storage.set_info("cid_123", {"title": "New"})

        mem_result = await memory_info_storage.get_info("cid_123")
        redis_result = await redis_info_storage.get_info("cid_123")

        assert mem_result["title"] == "New"
        assert redis_result["title"] == "New"

    @pytest.mark.asyncio
    async def test_complex_nested_info(
        self, memory_info_storage, redis_info_storage
    ):
        """Both should handle complex nested data structures."""
        info = {
            "title": "Test Paper",
            "metadata": {
                "keywords": ["ai", "ml"],
                "counts": {"citations": 42, "references": 10}
            },
            "authors": [
                {"name": "Alice", "affiliation": "MIT"},
                {"name": "Bob", "affiliation": "Stanford"}
            ]
        }

        await memory_info_storage.set_info("cid_123", info)
        await redis_info_storage.set_info("cid_123", info)

        mem_result = await memory_info_storage.get_info("cid_123")
        redis_result = await redis_info_storage.get_info("cid_123")

        assert mem_result == info
        assert redis_result == info

    @pytest.mark.asyncio
    async def test_empty_dict_info(
        self, memory_info_storage, redis_info_storage
    ):
        """Both should handle empty dict."""
        await memory_info_storage.set_info("cid_123", {})
        await redis_info_storage.set_info("cid_123", {})

        mem_result = await memory_info_storage.get_info("cid_123")
        redis_result = await redis_info_storage.get_info("cid_123")

        assert mem_result == {}
        assert redis_result == {}


# =============================================================================
# Test: CommittedLinkStorage - Memory vs Redis behavior parity
# =============================================================================

class TestCommittedLinkStorageParity:
    """
    Test that RedisCommittedLinkStorage behaves identically to MemoryCommittedLinkStorage.
    """

    @pytest.mark.asyncio
    async def test_is_link_committed_not_set_returns_false(
        self, memory_link_storage, redis_link_storage
    ):
        """Both should return False for uncommitted links."""
        mem_result = await memory_link_storage.is_link_committed("paper1", "author1")
        redis_result = await redis_link_storage.is_link_committed("paper1", "author1")

        assert not mem_result
        assert not redis_result

    @pytest.mark.asyncio
    async def test_commit_and_check_link(
        self, memory_link_storage, redis_link_storage
    ):
        """Both should commit and find links."""
        await memory_link_storage.commit_link("paper1", "author1")
        await redis_link_storage.commit_link("paper1", "author1")

        mem_result = await memory_link_storage.is_link_committed("paper1", "author1")
        redis_result = await redis_link_storage.is_link_committed("paper1", "author1")

        assert mem_result
        assert redis_result

    @pytest.mark.asyncio
    async def test_link_directionality(
        self, memory_link_storage, redis_link_storage
    ):
        """Both should respect link directionality."""
        await memory_link_storage.commit_link("A", "B")
        await redis_link_storage.commit_link("A", "B")

        # A->B should be committed
        assert await memory_link_storage.is_link_committed("A", "B")
        assert await redis_link_storage.is_link_committed("A", "B")

        # B->A should NOT be committed
        assert not await memory_link_storage.is_link_committed("B", "A")
        assert not await redis_link_storage.is_link_committed("B", "A")

    @pytest.mark.asyncio
    async def test_multiple_links_from_same_source(
        self, memory_link_storage, redis_link_storage
    ):
        """Both should handle multiple links from same source."""
        for target in ["T1", "T2", "T3"]:
            await memory_link_storage.commit_link("source", target)
            await redis_link_storage.commit_link("source", target)

        for target in ["T1", "T2", "T3"]:
            assert await memory_link_storage.is_link_committed("source", target)
            assert await redis_link_storage.is_link_committed("source", target)

        assert not await memory_link_storage.is_link_committed("source", "T4")
        assert not await redis_link_storage.is_link_committed("source", "T4")

    @pytest.mark.asyncio
    async def test_idempotent_commit(
        self, memory_link_storage, redis_link_storage
    ):
        """Both should handle idempotent commits."""
        # Commit same link multiple times
        for _ in range(3):
            await memory_link_storage.commit_link("A", "B")
            await redis_link_storage.commit_link("A", "B")

        assert await memory_link_storage.is_link_committed("A", "B")
        assert await redis_link_storage.is_link_committed("A", "B")


# =============================================================================
# Test: PendingListStorage - Memory vs Redis behavior parity
# =============================================================================

class TestPendingListStorageParity:
    """
    Test that RedisPendingListStorage behaves identically to MemoryPendingListStorage.
    """

    @pytest.mark.asyncio
    async def test_get_pending_not_set_returns_none(
        self, memory_pending_storage, redis_pending_storage
    ):
        """Both should return None when pending list not set."""
        mem_result = await memory_pending_storage.get_pending_identifier_sets("author1")
        redis_result = await redis_pending_storage.get_pending_identifier_sets("author1")

        assert mem_result is None
        assert redis_result is None

    @pytest.mark.asyncio
    async def test_set_and_get_pending(
        self, memory_pending_storage, redis_pending_storage
    ):
        """Both should store and retrieve pending lists correctly."""
        items = [{"doi:1"}, {"doi:2", "arxiv:2"}]

        await memory_pending_storage.set_pending_identifier_sets("author1", items)
        await redis_pending_storage.set_pending_identifier_sets("author1", items)

        mem_result = await memory_pending_storage.get_pending_identifier_sets("author1")
        redis_result = await redis_pending_storage.get_pending_identifier_sets("author1")

        assert len(mem_result) == 2
        assert len(redis_result) == 2

        # Check contents (order may vary)
        mem_sets = [frozenset(s) for s in mem_result]
        redis_sets = [frozenset(s) for s in redis_result]

        assert frozenset({"doi:1"}) in mem_sets
        assert frozenset({"doi:2", "arxiv:2"}) in mem_sets
        assert frozenset({"doi:1"}) in redis_sets
        assert frozenset({"doi:2", "arxiv:2"}) in redis_sets

    @pytest.mark.asyncio
    async def test_empty_list_vs_not_set(
        self, memory_pending_storage, redis_pending_storage
    ):
        """Both should distinguish between empty list and not set."""
        await memory_pending_storage.set_pending_identifier_sets("author1", [])
        await redis_pending_storage.set_pending_identifier_sets("author1", [])

        mem_result = await memory_pending_storage.get_pending_identifier_sets("author1")
        redis_result = await redis_pending_storage.get_pending_identifier_sets("author1")

        # Should be empty list, not None
        assert mem_result is not None
        assert redis_result is not None
        assert mem_result == []
        assert redis_result == []

    @pytest.mark.asyncio
    async def test_overwrite_pending(
        self, memory_pending_storage, redis_pending_storage
    ):
        """Both should overwrite existing pending lists."""
        await memory_pending_storage.set_pending_identifier_sets("author1", [{"doi:1"}])
        await memory_pending_storage.set_pending_identifier_sets("author1", [{"doi:2"}, {"doi:3"}])

        await redis_pending_storage.set_pending_identifier_sets("author1", [{"doi:1"}])
        await redis_pending_storage.set_pending_identifier_sets("author1", [{"doi:2"}, {"doi:3"}])

        mem_result = await memory_pending_storage.get_pending_identifier_sets("author1")
        redis_result = await redis_pending_storage.get_pending_identifier_sets("author1")

        assert len(mem_result) == 2
        assert len(redis_result) == 2

    @pytest.mark.asyncio
    async def test_single_item_list(
        self, memory_pending_storage, redis_pending_storage
    ):
        """Both should handle single item lists."""
        items = [{"doi:single"}]

        await memory_pending_storage.set_pending_identifier_sets("author1", items)
        await redis_pending_storage.set_pending_identifier_sets("author1", items)

        mem_result = await memory_pending_storage.get_pending_identifier_sets("author1")
        redis_result = await redis_pending_storage.get_pending_identifier_sets("author1")

        assert len(mem_result) == 1
        assert len(redis_result) == 1
        assert {"doi:single"} in mem_result
        assert {"doi:single"} in redis_result

    @pytest.mark.asyncio
    async def test_large_identifier_sets(
        self, memory_pending_storage, redis_pending_storage
    ):
        """Both should handle large identifier sets."""
        large_set = {f"id:{i}" for i in range(100)}
        items = [large_set]

        await memory_pending_storage.set_pending_identifier_sets("author1", items)
        await redis_pending_storage.set_pending_identifier_sets("author1", items)

        mem_result = await memory_pending_storage.get_pending_identifier_sets("author1")
        redis_result = await redis_pending_storage.get_pending_identifier_sets("author1")

        assert len(mem_result) == 1
        assert len(redis_result) == 1
        assert len(mem_result[0]) == 100
        assert len(redis_result[0]) == 100


# =============================================================================
# Run Tests
# =============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
