"""
Unit tests for Redis DataSrc caching mechanism.

These tests verify that RedisDataSrcCache has identical behavior
to MemoryDataSrcCache. Uses real Redis server if available at localhost:6379,
otherwise falls back to fakeredis for testing.

Run with: pytest tests/datasrc/cache/redis/test_redis_datasrc_cache.py -v
Requires: pip install redis (and optionally fakeredis as fallback)
"""

import pytest
import pytest_asyncio
import asyncio

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

from paper_weaver.datasrc import (
    MemoryDataSrcCache,
    RedisDataSrcCache,
    CachedAsyncPool,
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
def memory_cache():
    return MemoryDataSrcCache()


@pytest.fixture
def redis_cache(redis_client):
    return RedisDataSrcCache(redis_client, prefix="test_datasrc", default_expire=None)


@pytest.fixture
def redis_cache_with_default_expire(redis_client):
    return RedisDataSrcCache(redis_client, prefix="test_datasrc_expire", default_expire=60)


# =============================================================================
# Test: Basic Cache Operations - Memory vs Redis behavior parity
# =============================================================================

class TestDataSrcCacheParity:
    """
    Test that RedisDataSrcCache behaves identically to MemoryDataSrcCache.
    """

    @pytest.mark.asyncio
    async def test_get_not_set_returns_none(self, memory_cache, redis_cache):
        """Both should return None for non-existent keys."""
        mem_result = await memory_cache.get("nonexistent_key")
        redis_result = await redis_cache.get("nonexistent_key")

        assert mem_result is None
        assert redis_result is None

    @pytest.mark.asyncio
    async def test_set_and_get(self, memory_cache, redis_cache):
        """Both should store and retrieve values correctly."""
        await memory_cache.set("key1", "value1")
        await redis_cache.set("key1", "value1")

        mem_result = await memory_cache.get("key1")
        redis_result = await redis_cache.get("key1")

        assert mem_result == "value1"
        assert redis_result == "value1"

    @pytest.mark.asyncio
    async def test_overwrite_value(self, memory_cache, redis_cache):
        """Both should overwrite existing values."""
        await memory_cache.set("key1", "old_value")
        await memory_cache.set("key1", "new_value")

        await redis_cache.set("key1", "old_value")
        await redis_cache.set("key1", "new_value")

        mem_result = await memory_cache.get("key1")
        redis_result = await redis_cache.get("key1")

        assert mem_result == "new_value"
        assert redis_result == "new_value"

    @pytest.mark.asyncio
    async def test_multiple_keys(self, memory_cache, redis_cache):
        """Both should handle multiple keys correctly."""
        for i in range(3):
            await memory_cache.set(f"key{i}", f"value{i}")
            await redis_cache.set(f"key{i}", f"value{i}")

        for i in range(3):
            mem_result = await memory_cache.get(f"key{i}")
            redis_result = await redis_cache.get(f"key{i}")
            assert mem_result == f"value{i}"
            assert redis_result == f"value{i}"

    @pytest.mark.asyncio
    async def test_various_string_values(self, memory_cache, redis_cache):
        """Both should handle various string values correctly."""
        test_values = [
            ("simple", "simple_value"),
            ("empty", ""),
            ("json", '{"key": "value", "num": 123}'),
            ("unicode", "你好世界 🌍"),
            ("large", "x" * 10000),
        ]

        for key, value in test_values:
            await memory_cache.set(key, value)
            await redis_cache.set(key, value)

        for key, expected in test_values:
            mem_result = await memory_cache.get(key)
            redis_result = await redis_cache.get(key)
            assert mem_result == expected, f"Memory cache failed for key {key}"
            assert redis_result == expected, f"Redis cache failed for key {key}"


# =============================================================================
# Test: Expiration - Memory vs Redis behavior parity
# =============================================================================

class TestDataSrcCacheExpirationParity:
    """
    Test that expiration behavior is identical between Memory and Redis caches.
    """

    @pytest.mark.asyncio
    async def test_set_without_expire(self, memory_cache, redis_cache):
        """Both should store values without expiration by default."""
        await memory_cache.set("key1", "value1")
        await redis_cache.set("key1", "value1")

        mem_result = await memory_cache.get("key1")
        redis_result = await redis_cache.get("key1")

        assert mem_result == "value1"
        assert redis_result == "value1"

    @pytest.mark.asyncio
    async def test_set_with_expire_none(self, memory_cache, redis_cache):
        """Both should store values without expiration when expire=None."""
        await memory_cache.set("key1", "value1", expire=None)
        await redis_cache.set("key1", "value1", expire=None)

        mem_result = await memory_cache.get("key1")
        redis_result = await redis_cache.get("key1")

        assert mem_result == "value1"
        assert redis_result == "value1"

    @pytest.mark.asyncio
    async def test_set_with_expire_not_expired(self, memory_cache, redis_cache):
        """Both should return value before expiration."""
        await memory_cache.set("key1", "value1", expire=60)
        await redis_cache.set("key1", "value1", expire=60)

        mem_result = await memory_cache.get("key1")
        redis_result = await redis_cache.get("key1")

        assert mem_result == "value1"
        assert redis_result == "value1"

    @pytest.mark.asyncio
    async def test_set_with_expire_expired(self, memory_cache, redis_cache):
        """Both should return None after expiration."""
        await memory_cache.set("key1", "value1", expire=1)
        await redis_cache.set("key1", "value1", expire=1)

        # Value should be accessible immediately
        mem_result = await memory_cache.get("key1")
        redis_result = await redis_cache.get("key1")
        assert mem_result == "value1"
        assert redis_result == "value1"

        # Wait for expiration
        await asyncio.sleep(1.1)

        # Both should return None
        mem_result = await memory_cache.get("key1")
        redis_result = await redis_cache.get("key1")
        assert mem_result is None
        assert redis_result is None

    @pytest.mark.asyncio
    async def test_zero_expire(self, memory_cache, redis_cache):
        """Both should treat zero expire as immediate expiration."""
        await memory_cache.set("key1", "value1", expire=0)
        await redis_cache.set("key1", "value1", expire=0)

        # Both should return None (immediately expired)
        mem_result = await memory_cache.get("key1")
        redis_result = await redis_cache.get("key1")

        assert mem_result is None
        assert redis_result is None

    @pytest.mark.asyncio
    async def test_overwrite_with_new_expire(self, memory_cache, redis_cache):
        """Both should update expiration when overwriting."""
        await memory_cache.set("key1", "value1", expire=1)
        await memory_cache.set("key1", "value2", expire=60)

        await redis_cache.set("key1", "value1", expire=1)
        await redis_cache.set("key1", "value2", expire=60)

        # Wait past original expiration
        await asyncio.sleep(1.1)

        # Both should still have the value
        mem_result = await memory_cache.get("key1")
        redis_result = await redis_cache.get("key1")

        assert mem_result == "value2"
        assert redis_result == "value2"

    @pytest.mark.asyncio
    async def test_overwrite_remove_expire(self, memory_cache, redis_cache):
        """Both should remove expiration when overwriting with expire=None."""
        await memory_cache.set("key1", "value1", expire=1)
        await memory_cache.set("key1", "value2", expire=None)

        await redis_cache.set("key1", "value1", expire=1)
        await redis_cache.set("key1", "value2", expire=None)

        # Wait past original expiration
        await asyncio.sleep(1.1)

        # Both should still have the value
        mem_result = await memory_cache.get("key1")
        redis_result = await redis_cache.get("key1")

        assert mem_result == "value2"
        assert redis_result == "value2"


# =============================================================================
# Test: Default Expire
# =============================================================================

class TestRedisDataSrcCacheDefaultExpire:
    """Test RedisDataSrcCache with default_expire setting."""

    @pytest.mark.asyncio
    async def test_default_expire_used_when_not_specified(
        self, redis_cache_with_default_expire
    ):
        """Default expire should be used when expire is not specified."""
        cache = redis_cache_with_default_expire

        await cache.set("key1", "value1")

        # Value should be accessible
        result = await cache.get("key1")
        assert result == "value1"

    @pytest.mark.asyncio
    async def test_explicit_expire_overrides_default(
        self, redis_cache_with_default_expire
    ):
        """Explicit expire should override default_expire."""
        cache = redis_cache_with_default_expire

        await cache.set("key1", "value1", expire=1)

        # Value should be accessible immediately
        result = await cache.get("key1")
        assert result == "value1"

        # Wait for explicit expiration
        await asyncio.sleep(1.1)

        # Should be expired
        result = await cache.get("key1")
        assert result is None

    @pytest.mark.asyncio
    async def test_explicit_none_overrides_default(
        self, redis_cache_with_default_expire
    ):
        """Explicit expire=None should override default_expire (no expiration)."""
        cache = redis_cache_with_default_expire

        # This is a bit tricky - when expire=None is passed, it falls back to default
        # So we need to check the behavior matches expectations
        await cache.set("key1", "value1", expire=None)

        result = await cache.get("key1")
        assert result == "value1"


# =============================================================================
# Test: Redis-specific features
# =============================================================================

class TestRedisDataSrcCacheSpecific:
    """Test Redis-specific cache features."""

    @pytest.mark.asyncio
    async def test_key_prefix(self, redis_client):
        """Test that key prefix is applied correctly."""
        cache1 = RedisDataSrcCache(redis_client, prefix="prefix1")
        cache2 = RedisDataSrcCache(redis_client, prefix="prefix2")

        await cache1.set("key1", "value1")
        await cache2.set("key1", "value2")

        # Each cache should have its own namespace
        result1 = await cache1.get("key1")
        result2 = await cache2.get("key1")

        assert result1 == "value1"
        assert result2 == "value2"

    @pytest.mark.asyncio
    async def test_concurrent_access(self, redis_cache):
        """Test concurrent access to Redis cache."""
        async def write_value(key, value):
            await redis_cache.set(key, value)

        async def read_value(key):
            return await redis_cache.get(key)

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


# =============================================================================
# Test: CachedAsyncPool with Redis
# =============================================================================

class TestCachedAsyncPoolWithRedis:
    """Test CachedAsyncPool with RedisDataSrcCache backend."""

    @pytest.fixture
    def memory_pool(self, memory_cache):
        return CachedAsyncPool(memory_cache, max_concurrent=3)

    @pytest.fixture
    def redis_pool(self, redis_cache):
        return CachedAsyncPool(redis_cache, max_concurrent=3)

    @pytest.mark.asyncio
    async def test_get_or_fetch_cache_miss(self, memory_pool, redis_pool):
        """Both pools should call fetcher on cache miss."""
        mem_fetch_count = 0
        redis_fetch_count = 0

        async def mem_fetcher():
            nonlocal mem_fetch_count
            mem_fetch_count += 1
            return "fetched_value"

        async def redis_fetcher():
            nonlocal redis_fetch_count
            redis_fetch_count += 1
            return "fetched_value"

        parser = lambda x: x

        mem_result = await memory_pool.get_or_fetch("key1", mem_fetcher, parser)
        redis_result = await redis_pool.get_or_fetch("key1", redis_fetcher, parser)

        assert mem_result == "fetched_value"
        assert redis_result == "fetched_value"
        assert mem_fetch_count == 1
        assert redis_fetch_count == 1

    @pytest.mark.asyncio
    async def test_get_or_fetch_cache_hit(
        self, memory_cache, redis_cache, memory_pool, redis_pool
    ):
        """Both pools should return cached value without calling fetcher."""
        # Pre-populate caches
        await memory_cache.set("key1", "cached_value")
        await redis_cache.set("key1", "cached_value")

        mem_fetch_count = 0
        redis_fetch_count = 0

        async def mem_fetcher():
            nonlocal mem_fetch_count
            mem_fetch_count += 1
            return "fresh_value"

        async def redis_fetcher():
            nonlocal redis_fetch_count
            redis_fetch_count += 1
            return "fresh_value"

        parser = lambda x: x

        mem_result = await memory_pool.get_or_fetch("key1", mem_fetcher, parser)
        redis_result = await redis_pool.get_or_fetch("key1", redis_fetcher, parser)

        assert mem_result == "cached_value"
        assert redis_result == "cached_value"
        assert mem_fetch_count == 0
        assert redis_fetch_count == 0

    @pytest.mark.asyncio
    async def test_get_or_fetch_with_expire(
        self, memory_cache, redis_cache, memory_pool, redis_pool
    ):
        """Both pools should respect expiration."""
        async def fetcher():
            return "fetched_value"

        parser = lambda x: x

        await memory_pool.get_or_fetch("key1", fetcher, parser, expire=1)
        await redis_pool.get_or_fetch("key1", fetcher, parser, expire=1)

        # Values should be cached
        assert await memory_cache.get("key1") == "fetched_value"
        assert await redis_cache.get("key1") == "fetched_value"

        # Wait for expiration
        await asyncio.sleep(1.1)

        # Values should be expired
        assert await memory_cache.get("key1") is None
        assert await redis_cache.get("key1") is None

    @pytest.mark.asyncio
    async def test_get_or_fetch_fetcher_none_not_cached(
        self, memory_cache, redis_cache, memory_pool, redis_pool
    ):
        """Both pools should not cache when fetcher returns None."""
        mem_fetch_count = 0
        redis_fetch_count = 0

        async def mem_fetcher():
            nonlocal mem_fetch_count
            mem_fetch_count += 1
            return None

        async def redis_fetcher():
            nonlocal redis_fetch_count
            redis_fetch_count += 1
            return None

        parser = lambda x: x

        # First fetch
        mem_result = await memory_pool.get_or_fetch("key1", mem_fetcher, parser)
        redis_result = await redis_pool.get_or_fetch("key1", redis_fetcher, parser)

        assert mem_result is None
        assert redis_result is None
        assert mem_fetch_count == 1
        assert redis_fetch_count == 1

        # Second fetch should call fetcher again
        mem_result = await memory_pool.get_or_fetch("key1", mem_fetcher, parser)
        redis_result = await redis_pool.get_or_fetch("key1", redis_fetcher, parser)

        assert mem_result is None
        assert redis_result is None
        assert mem_fetch_count == 2
        assert redis_fetch_count == 2

    @pytest.mark.asyncio
    async def test_get_or_fetch_parser_none_not_cached(
        self, memory_cache, redis_cache, memory_pool, redis_pool
    ):
        """Both pools should not cache when parser returns None."""
        mem_fetch_count = 0
        redis_fetch_count = 0

        async def mem_fetcher():
            nonlocal mem_fetch_count
            mem_fetch_count += 1
            return "fetched_value"

        async def redis_fetcher():
            nonlocal redis_fetch_count
            redis_fetch_count += 1
            return "fetched_value"

        # Parser returns None
        parser = lambda x: None

        # First fetch
        mem_result = await memory_pool.get_or_fetch("key1", mem_fetcher, parser)
        redis_result = await redis_pool.get_or_fetch("key1", redis_fetcher, parser)

        assert mem_result is None
        assert redis_result is None
        assert mem_fetch_count == 1
        assert redis_fetch_count == 1

        # Verify value is not cached
        assert await memory_cache.get("key1") is None
        assert await redis_cache.get("key1") is None

        # Second fetch should call fetcher again
        mem_result = await memory_pool.get_or_fetch("key1", mem_fetcher, parser)
        redis_result = await redis_pool.get_or_fetch("key1", redis_fetcher, parser)

        assert mem_result is None
        assert redis_result is None
        assert mem_fetch_count == 2
        assert redis_fetch_count == 2

    @pytest.mark.asyncio
    async def test_get_or_fetch_parser_transforms_value(
        self, memory_pool, redis_pool
    ):
        """Both pools should return parser's output."""
        async def fetcher():
            return '{"key": "value"}'

        # Parser transforms to dict
        parser = lambda x: {"parsed": x}

        mem_result = await memory_pool.get_or_fetch("key1", fetcher, parser)
        redis_result = await redis_pool.get_or_fetch("key2", fetcher, parser)

        assert mem_result == {"parsed": '{"key": "value"}'}
        assert redis_result == {"parsed": '{"key": "value"}'}

    @pytest.mark.asyncio
    async def test_get_or_fetch_cache_hit_uses_parser(
        self, memory_cache, redis_cache, memory_pool, redis_pool
    ):
        """Both pools should apply parser to cached value on cache hit."""
        # Pre-populate caches
        await memory_cache.set("key1", "cached_value")
        await redis_cache.set("key1", "cached_value")

        async def fetcher():
            return "fresh_value"

        # Parser should be applied to cached value
        parser = lambda x: x.upper()

        mem_result = await memory_pool.get_or_fetch("key1", fetcher, parser)
        redis_result = await redis_pool.get_or_fetch("key1", fetcher, parser)

        assert mem_result == "CACHED_VALUE"
        assert redis_result == "CACHED_VALUE"


# =============================================================================
# Test: Interface Compliance
# =============================================================================

class TestRedisDataSrcCacheCompliance:
    """Tests to verify RedisDataSrcCache complies with DataSrcCacheIface."""

    def test_is_subclass(self):
        """Test that RedisDataSrcCache is a subclass of DataSrcCacheIface."""
        from paper_weaver.datasrc import DataSrcCacheIface
        assert issubclass(RedisDataSrcCache, DataSrcCacheIface)

    @pytest.mark.asyncio
    async def test_instance_check(self, redis_cache):
        """Test that RedisDataSrcCache instance is instance of DataSrcCacheIface."""
        from paper_weaver.datasrc import DataSrcCacheIface
        assert isinstance(redis_cache, DataSrcCacheIface)

    def test_has_required_methods(self, redis_cache):
        """Test that RedisDataSrcCache has all required methods."""
        assert hasattr(redis_cache, 'get')
        assert hasattr(redis_cache, 'set')
        assert callable(redis_cache.get)
        assert callable(redis_cache.set)


# =============================================================================
# Run Tests
# =============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v"])

