"""
Unit tests for DataSrc caching mechanism.

Tests: DataSrcCacheIface, MemoryDataSrcCache, CachedAsyncPool
"""

import pytest
import asyncio

from paper_weaver.datasrc.cache import DataSrcCacheIface, CachedAsyncPool
from paper_weaver.datasrc.cache_impl import MemoryDataSrcCache


class TestMemoryDataSrcCache:
    """Tests for MemoryDataSrcCache."""

    @pytest.fixture
    def cache(self):
        return MemoryDataSrcCache()

    @pytest.mark.asyncio
    async def test_get_not_set(self, cache):
        """Test getting a key that hasn't been set returns None."""
        result = await cache.get("nonexistent_key")
        assert result is None

    @pytest.mark.asyncio
    async def test_set_and_get(self, cache):
        """Test setting and getting a value."""
        await cache.set("key1", {"data": "value1"})
        result = await cache.get("key1")
        assert result == {"data": "value1"}

    @pytest.mark.asyncio
    async def test_overwrite_value(self, cache):
        """Test overwriting an existing value."""
        await cache.set("key1", "old_value")
        await cache.set("key1", "new_value")
        result = await cache.get("key1")
        assert result == "new_value"

    @pytest.mark.asyncio
    async def test_multiple_keys(self, cache):
        """Test storing and retrieving multiple keys."""
        await cache.set("key1", "value1")
        await cache.set("key2", "value2")
        await cache.set("key3", "value3")
        
        assert await cache.get("key1") == "value1"
        assert await cache.get("key2") == "value2"
        assert await cache.get("key3") == "value3"

    @pytest.mark.asyncio
    async def test_various_value_types(self, cache):
        """Test caching various value types."""
        # String
        await cache.set("str_key", "string_value")
        assert await cache.get("str_key") == "string_value"
        
        # Dict
        await cache.set("dict_key", {"nested": {"data": 123}})
        assert await cache.get("dict_key") == {"nested": {"data": 123}}
        
        # List
        await cache.set("list_key", [1, 2, 3, "four"])
        assert await cache.get("list_key") == [1, 2, 3, "four"]
        
        # Int
        await cache.set("int_key", 42)
        assert await cache.get("int_key") == 42

    @pytest.mark.asyncio
    async def test_concurrent_access(self, cache):
        """Test concurrent access to the cache."""
        async def write_value(key, value):
            await cache.set(key, value)
        
        async def read_value(key):
            return await cache.get(key)
        
        # Write multiple values concurrently
        await asyncio.gather(
            write_value("c1", "v1"),
            write_value("c2", "v2"),
            write_value("c3", "v3"),
        )
        
        # Read all values concurrently
        results = await asyncio.gather(
            read_value("c1"),
            read_value("c2"),
            read_value("c3"),
        )
        
        assert results == ["v1", "v2", "v3"]


class TestCachedAsyncPool:
    """Tests for CachedAsyncPool."""

    @pytest.fixture
    def cache(self):
        return MemoryDataSrcCache()

    @pytest.fixture
    def pool(self, cache):
        return CachedAsyncPool(cache, max_concurrent=3)

    @pytest.mark.asyncio
    async def test_get_or_fetch_cache_miss(self, pool):
        """Test get_or_fetch calls fetcher on cache miss."""
        fetch_count = 0
        
        async def fetcher():
            nonlocal fetch_count
            fetch_count += 1
            return {"data": "fetched_value"}
        
        result = await pool.get_or_fetch("key1", fetcher)
        
        assert result == {"data": "fetched_value"}
        assert fetch_count == 1

    @pytest.mark.asyncio
    async def test_get_or_fetch_cache_hit(self, cache, pool):
        """Test get_or_fetch returns cached value without calling fetcher."""
        # Pre-populate cache
        await cache.set("key1", "cached_value")
        
        fetch_count = 0
        
        async def fetcher():
            nonlocal fetch_count
            fetch_count += 1
            return "fresh_value"
        
        result = await pool.get_or_fetch("key1", fetcher)
        
        assert result == "cached_value"
        assert fetch_count == 0  # Fetcher should not be called

    @pytest.mark.asyncio
    async def test_get_or_fetch_caches_result(self, cache, pool):
        """Test get_or_fetch caches the fetched result."""
        async def fetcher():
            return "fetched_value"
        
        await pool.get_or_fetch("key1", fetcher)
        
        # Verify it's in the cache
        cached = await cache.get("key1")
        assert cached == "fetched_value"

    @pytest.mark.asyncio
    async def test_get_or_fetch_none_not_cached(self, cache, pool):
        """Test get_or_fetch does not cache None results."""
        fetch_count = 0
        
        async def fetcher():
            nonlocal fetch_count
            fetch_count += 1
            return None
        
        result = await pool.get_or_fetch("key1", fetcher)
        assert result is None
        
        # Verify None is not cached
        cached = await cache.get("key1")
        assert cached is None
        
        # Next call should fetch again
        result = await pool.get_or_fetch("key1", fetcher)
        assert result is None
        assert fetch_count == 2

    @pytest.mark.asyncio
    async def test_deduplication_same_key(self, pool):
        """Test that concurrent requests for same key deduplicate."""
        fetch_count = 0
        fetch_started = asyncio.Event()
        fetch_proceed = asyncio.Event()
        
        async def slow_fetcher():
            nonlocal fetch_count
            fetch_count += 1
            fetch_started.set()
            await fetch_proceed.wait()
            return f"value_{fetch_count}"
        
        # Start multiple concurrent requests for the same key
        task1 = asyncio.create_task(pool.get_or_fetch("key1", slow_fetcher))
        
        # Wait for first fetch to start
        await fetch_started.wait()
        
        # Start more requests while first is pending
        task2 = asyncio.create_task(pool.get_or_fetch("key1", slow_fetcher))
        task3 = asyncio.create_task(pool.get_or_fetch("key1", slow_fetcher))
        
        # Allow fetch to complete
        fetch_proceed.set()
        
        results = await asyncio.gather(task1, task2, task3)
        
        # All should get the same result
        assert results == ["value_1", "value_1", "value_1"]
        # Fetcher should only be called once
        assert fetch_count == 1

    @pytest.mark.asyncio
    async def test_different_keys_no_deduplication(self, pool):
        """Test that different keys don't deduplicate."""
        fetched_keys = []
        
        async def make_fetcher(key):
            async def fetcher():
                fetched_keys.append(key)
                await asyncio.sleep(0.01)
                return f"value_{key}"
            return fetcher
        
        fetcher1 = await make_fetcher("key1")
        fetcher2 = await make_fetcher("key2")
        fetcher3 = await make_fetcher("key3")
        
        results = await asyncio.gather(
            pool.get_or_fetch("key1", fetcher1),
            pool.get_or_fetch("key2", fetcher2),
            pool.get_or_fetch("key3", fetcher3),
        )
        
        # Each key should trigger its own fetch
        assert len(fetched_keys) == 3
        assert set(fetched_keys) == {"key1", "key2", "key3"}
        assert set(results) == {"value_key1", "value_key2", "value_key3"}

    @pytest.mark.asyncio
    async def test_semaphore_limits_concurrency(self):
        """Test that semaphore limits concurrent operations."""
        cache = MemoryDataSrcCache()
        pool = CachedAsyncPool(cache, max_concurrent=2)
        
        concurrent_count = 0
        max_concurrent = 0
        lock = asyncio.Lock()
        
        async def tracked_fetcher(key):
            nonlocal concurrent_count, max_concurrent
            async with lock:
                concurrent_count += 1
                max_concurrent = max(max_concurrent, concurrent_count)
            
            await asyncio.sleep(0.05)
            
            async with lock:
                concurrent_count -= 1
            
            return f"value_{key}"
        
        # Launch more tasks than semaphore allows
        tasks = [
            pool.get_or_fetch(f"key{i}", lambda i=i: tracked_fetcher(i))
            for i in range(5)
        ]
        
        await asyncio.gather(*tasks)
        
        # Max concurrent should be limited by semaphore
        assert max_concurrent <= 2

    @pytest.mark.asyncio
    async def test_pending_cleanup_after_completion(self, pool):
        """Test that pending entries are cleaned up after task completes."""
        async def fetcher():
            return "value"
        
        await pool.get_or_fetch("key1", fetcher)
        
        # After completion, pending should be empty
        assert "key1" not in pool._pending

    @pytest.mark.asyncio
    async def test_pending_cleanup_after_exception(self):
        """Test that pending entries are cleaned up even after exception."""
        cache = MemoryDataSrcCache()
        pool = CachedAsyncPool(cache, max_concurrent=3)
        
        async def failing_fetcher():
            raise ValueError("Fetch failed")
        
        with pytest.raises(ValueError):
            await pool.get_or_fetch("key1", failing_fetcher)
        
        # After exception, pending should be cleaned up
        assert "key1" not in pool._pending

    @pytest.mark.asyncio
    async def test_second_fetch_after_first_completes(self, pool):
        """Test fetching same key again after first fetch completes."""
        fetch_count = 0
        
        async def fetcher():
            nonlocal fetch_count
            fetch_count += 1
            return f"value_{fetch_count}"
        
        # First fetch
        result1 = await pool.get_or_fetch("key1", fetcher)
        assert result1 == "value_1"
        assert fetch_count == 1
        
        # Second fetch should use cache
        result2 = await pool.get_or_fetch("key1", fetcher)
        assert result2 == "value_1"  # Same cached value
        assert fetch_count == 1  # No additional fetch


class TestCachedAsyncPoolEdgeCases:
    """Edge case tests for CachedAsyncPool."""

    @pytest.mark.asyncio
    async def test_fetcher_with_delay(self):
        """Test fetcher that has internal delay."""
        cache = MemoryDataSrcCache()
        pool = CachedAsyncPool(cache, max_concurrent=5)
        
        async def delayed_fetcher():
            await asyncio.sleep(0.1)
            return "delayed_value"
        
        result = await pool.get_or_fetch("key1", delayed_fetcher)
        assert result == "delayed_value"

    @pytest.mark.asyncio
    async def test_empty_string_key(self):
        """Test with empty string as key."""
        cache = MemoryDataSrcCache()
        pool = CachedAsyncPool(cache, max_concurrent=3)
        
        async def fetcher():
            return "value_for_empty_key"
        
        result = await pool.get_or_fetch("", fetcher)
        assert result == "value_for_empty_key"
        
        # Should be cached
        cached = await cache.get("")
        assert cached == "value_for_empty_key"

    @pytest.mark.asyncio
    async def test_large_value(self):
        """Test caching large values."""
        cache = MemoryDataSrcCache()
        pool = CachedAsyncPool(cache, max_concurrent=3)
        
        large_data = {"items": list(range(10000))}
        
        async def fetcher():
            return large_data
        
        result = await pool.get_or_fetch("key1", fetcher)
        assert result == large_data

    @pytest.mark.asyncio
    async def test_concurrent_different_and_same_keys(self):
        """Test concurrent requests with mix of same and different keys."""
        cache = MemoryDataSrcCache()
        pool = CachedAsyncPool(cache, max_concurrent=5)
        
        fetch_calls = {}
        lock = asyncio.Lock()
        
        async def tracked_fetcher(key):
            async with lock:
                fetch_calls[key] = fetch_calls.get(key, 0) + 1
            await asyncio.sleep(0.02)
            return f"value_{key}"
        
        # Mix of same and different keys
        tasks = [
            pool.get_or_fetch("key1", lambda: tracked_fetcher("key1")),
            pool.get_or_fetch("key2", lambda: tracked_fetcher("key2")),
            pool.get_or_fetch("key1", lambda: tracked_fetcher("key1")),  # Duplicate
            pool.get_or_fetch("key3", lambda: tracked_fetcher("key3")),
            pool.get_or_fetch("key2", lambda: tracked_fetcher("key2")),  # Duplicate
        ]
        
        results = await asyncio.gather(*tasks)
        
        # key1 and key2 should only be fetched once each
        assert fetch_calls.get("key1", 0) == 1
        assert fetch_calls.get("key2", 0) == 1
        assert fetch_calls.get("key3", 0) == 1

    @pytest.mark.asyncio
    async def test_pool_reuse(self):
        """Test that pool can be reused for multiple operations."""
        cache = MemoryDataSrcCache()
        pool = CachedAsyncPool(cache, max_concurrent=3)
        
        async def make_fetcher(value):
            async def fetcher():
                return value
            return fetcher
        
        # First batch of operations
        for i in range(5):
            fetcher = await make_fetcher(f"v{i}")
            await pool.get_or_fetch(f"batch1_key{i}", fetcher)
        
        # Second batch of operations
        for i in range(5):
            fetcher = await make_fetcher(f"v{i}")
            result = await pool.get_or_fetch(f"batch2_key{i}", fetcher)
            assert result == f"v{i}"


class TestDataSrcCacheIfaceCompliance:
    """Tests to verify MemoryDataSrcCache complies with DataSrcCacheIface."""

    def test_is_subclass(self):
        """Test that MemoryDataSrcCache is a subclass of DataSrcCacheIface."""
        assert issubclass(MemoryDataSrcCache, DataSrcCacheIface)

    def test_instance_check(self):
        """Test that MemoryDataSrcCache instance is instance of DataSrcCacheIface."""
        cache = MemoryDataSrcCache()
        assert isinstance(cache, DataSrcCacheIface)

    def test_has_required_methods(self):
        """Test that MemoryDataSrcCache has all required methods."""
        cache = MemoryDataSrcCache()
        assert hasattr(cache, 'get')
        assert hasattr(cache, 'set')
        assert callable(cache.get)
        assert callable(cache.set)

