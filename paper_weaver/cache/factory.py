"""
Factory functions for creating cache instances with different backends.

Provides convenient functions to create fully configured cache instances
using either memory or Redis backends.
"""

from .memory import MemoryIdentifierRegistry, MemoryInfoStorage, MemoryCommittedLinkStorage, MemoryPendingListStorage
from .redis import RedisIdentifierRegistry, RedisInfoStorage, RedisCommittedLinkStorage, RedisPendingListStorage
from .impl_full import FullWeaverCache
from .impl_a2p import Author2PapersCache
from .impl_p2a import Paper2AuthorsCache
from .impl_p2r import Paper2ReferencesCache
from .impl_p2c import Paper2CitationsCache
from .impl_p2v import Paper2VenuesCache


def create_memory_weaver_cache() -> FullWeaverCache:
    """Create an in-memory cache for full weaver operations."""
    return FullWeaverCache(
        paper_registry=MemoryIdentifierRegistry(),
        paper_info_storage=MemoryInfoStorage(),
        author_registry=MemoryIdentifierRegistry(),
        author_info_storage=MemoryInfoStorage(),
        venue_registry=MemoryIdentifierRegistry(),
        venue_info_storage=MemoryInfoStorage(),
        committed_author_links=MemoryCommittedLinkStorage(),
        committed_reference_links=MemoryCommittedLinkStorage(),
        committed_venue_links=MemoryCommittedLinkStorage(),
        pending_papers=MemoryPendingListStorage(),
        pending_authors=MemoryPendingListStorage(),
        pending_references=MemoryPendingListStorage(),
        pending_citations=MemoryPendingListStorage(),
        pending_venues=MemoryPendingListStorage(),
    )


def create_redis_weaver_cache(
    redis_client, prefix: str = "pw", expire: int | None = None
) -> FullWeaverCache:
    """
    Create a Redis-backed cache for full weaver operations.

    Args:
        redis_client: An async Redis client (e.g., from redis.asyncio)
        prefix: Prefix for all Redis keys
        expire: TTL in seconds for keys, None means no expiration
    """
    return FullWeaverCache(
        paper_registry=RedisIdentifierRegistry(redis_client, f"{prefix}:paper_reg", expire),
        paper_info_storage=RedisInfoStorage(redis_client, f"{prefix}:paper_info", expire),
        author_registry=RedisIdentifierRegistry(redis_client, f"{prefix}:author_reg", expire),
        author_info_storage=RedisInfoStorage(redis_client, f"{prefix}:author_info", expire),
        venue_registry=RedisIdentifierRegistry(redis_client, f"{prefix}:venue_reg", expire),
        venue_info_storage=RedisInfoStorage(redis_client, f"{prefix}:venue_info", expire),
        committed_author_links=RedisCommittedLinkStorage(redis_client, f"{prefix}:committed_ap", expire),
        committed_reference_links=RedisCommittedLinkStorage(redis_client, f"{prefix}:committed_pr", expire),
        committed_venue_links=RedisCommittedLinkStorage(redis_client, f"{prefix}:committed_pv", expire),
        pending_papers=RedisPendingListStorage(redis_client, f"{prefix}:pending_a2p", expire),
        pending_authors=RedisPendingListStorage(redis_client, f"{prefix}:pending_p2a", expire),
        pending_references=RedisPendingListStorage(redis_client, f"{prefix}:pending_p2r", expire),
        pending_citations=RedisPendingListStorage(redis_client, f"{prefix}:pending_p2c", expire),
        pending_venues=RedisPendingListStorage(redis_client, f"{prefix}:pending_p2v", expire),
    )


# Hybrid cache builders for mixing memory and redis backends

class HybridCacheBuilder:
    """
    Builder for creating caches with mixed memory/redis backends.

    Allows fine-grained control over which components use which backend.

    Example:
        builder = HybridCacheBuilder(redis_client, expire=3600)
        cache = (builder
            .with_memory_paper_registry()
            .with_redis_paper_info("paper_info")
            .with_memory_author_registry()
            .with_redis_author_info("author_info")
            .with_memory_committed_author_links()
            .build_weaver_cache())
    """

    def __init__(self, redis_client=None, expire: int | None = None):
        """
        Initialize hybrid cache builder.

        Args:
            redis_client: Redis async client for Redis-backed components
            expire: Default TTL in seconds for Redis keys, None means no expiration
        """
        self._redis = redis_client
        self._expire = expire
        self._paper_registry = None
        self._paper_info = None
        self._author_registry = None
        self._author_info = None
        self._venue_registry = None
        self._venue_info = None
        self._committed_author_links = None
        self._committed_reference_links = None
        self._committed_venue_links = None
        self._pending_papers = None
        self._pending_authors = None
        self._pending_references = None
        self._pending_citations = None
        self._pending_venues = None

    # Paper registry

    def with_memory_paper_registry(self) -> "HybridCacheBuilder":
        self._paper_registry = MemoryIdentifierRegistry()
        return self

    def with_redis_paper_registry(self, prefix: str = "paper_reg", expire: int | None = None) -> "HybridCacheBuilder":
        self._paper_registry = RedisIdentifierRegistry(self._redis, prefix, expire if expire is not None else self._expire)
        return self

    # Paper info

    def with_memory_paper_info(self) -> "HybridCacheBuilder":
        self._paper_info = MemoryInfoStorage()
        return self

    def with_redis_paper_info(self, prefix: str = "paper_info", expire: int | None = None) -> "HybridCacheBuilder":
        self._paper_info = RedisInfoStorage(self._redis, prefix, expire if expire is not None else self._expire)
        return self

    # Author registry

    def with_memory_author_registry(self) -> "HybridCacheBuilder":
        self._author_registry = MemoryIdentifierRegistry()
        return self

    def with_redis_author_registry(self, prefix: str = "author_reg", expire: int | None = None) -> "HybridCacheBuilder":
        self._author_registry = RedisIdentifierRegistry(self._redis, prefix, expire if expire is not None else self._expire)
        return self

    # Author info

    def with_memory_author_info(self) -> "HybridCacheBuilder":
        self._author_info = MemoryInfoStorage()
        return self

    def with_redis_author_info(self, prefix: str = "author_info", expire: int | None = None) -> "HybridCacheBuilder":
        self._author_info = RedisInfoStorage(self._redis, prefix, expire if expire is not None else self._expire)
        return self

    # Venue registry

    def with_memory_venue_registry(self) -> "HybridCacheBuilder":
        self._venue_registry = MemoryIdentifierRegistry()
        return self

    def with_redis_venue_registry(self, prefix: str = "venue_reg", expire: int | None = None) -> "HybridCacheBuilder":
        self._venue_registry = RedisIdentifierRegistry(self._redis, prefix, expire if expire is not None else self._expire)
        return self

    # Venue info

    def with_memory_venue_info(self) -> "HybridCacheBuilder":
        self._venue_info = MemoryInfoStorage()
        return self

    def with_redis_venue_info(self, prefix: str = "venue_info", expire: int | None = None) -> "HybridCacheBuilder":
        self._venue_info = RedisInfoStorage(self._redis, prefix, expire if expire is not None else self._expire)
        return self

    # Committed links

    def with_memory_committed_author_links(self) -> "HybridCacheBuilder":
        self._committed_author_links = MemoryCommittedLinkStorage()
        return self

    def with_redis_committed_author_links(self, prefix: str = "committed_ap", expire: int | None = None) -> "HybridCacheBuilder":
        self._committed_author_links = RedisCommittedLinkStorage(self._redis, prefix, expire if expire is not None else self._expire)
        return self

    def with_memory_committed_reference_links(self) -> "HybridCacheBuilder":
        self._committed_reference_links = MemoryCommittedLinkStorage()
        return self

    def with_redis_committed_reference_links(self, prefix: str = "committed_pr", expire: int | None = None) -> "HybridCacheBuilder":
        self._committed_reference_links = RedisCommittedLinkStorage(self._redis, prefix, expire if expire is not None else self._expire)
        return self

    def with_memory_committed_venue_links(self) -> "HybridCacheBuilder":
        self._committed_venue_links = MemoryCommittedLinkStorage()
        return self

    def with_redis_committed_venue_links(self, prefix: str = "committed_pv", expire: int | None = None) -> "HybridCacheBuilder":
        self._committed_venue_links = RedisCommittedLinkStorage(self._redis, prefix, expire if expire is not None else self._expire)
        return self

    # Pending lists

    def with_memory_pending_papers(self) -> "HybridCacheBuilder":
        self._pending_papers = MemoryPendingListStorage()
        return self

    def with_redis_pending_papers(self, prefix: str = "pending_a2p", expire: int | None = None) -> "HybridCacheBuilder":
        self._pending_papers = RedisPendingListStorage(self._redis, prefix, expire if expire is not None else self._expire)
        return self

    def with_memory_pending_authors(self) -> "HybridCacheBuilder":
        self._pending_authors = MemoryPendingListStorage()
        return self

    def with_redis_pending_authors(self, prefix: str = "pending_p2a", expire: int | None = None) -> "HybridCacheBuilder":
        self._pending_authors = RedisPendingListStorage(self._redis, prefix, expire if expire is not None else self._expire)
        return self

    def with_memory_pending_references(self) -> "HybridCacheBuilder":
        self._pending_references = MemoryPendingListStorage()
        return self

    def with_redis_pending_references(self, prefix: str = "pending_p2r", expire: int | None = None) -> "HybridCacheBuilder":
        self._pending_references = RedisPendingListStorage(self._redis, prefix, expire if expire is not None else self._expire)
        return self

    def with_memory_pending_citations(self) -> "HybridCacheBuilder":
        self._pending_citations = MemoryPendingListStorage()
        return self

    def with_redis_pending_citations(self, prefix: str = "pending_p2c", expire: int | None = None) -> "HybridCacheBuilder":
        self._pending_citations = RedisPendingListStorage(self._redis, prefix, expire if expire is not None else self._expire)
        return self

    def with_memory_pending_venues(self) -> "HybridCacheBuilder":
        self._pending_venues = MemoryPendingListStorage()
        return self

    def with_redis_pending_venues(self, prefix: str = "pending_p2v", expire: int | None = None) -> "HybridCacheBuilder":
        self._pending_venues = RedisPendingListStorage(self._redis, prefix, expire if expire is not None else self._expire)
        return self

    # Convenience methods for setting all components at once

    def with_all_memory(self) -> "HybridCacheBuilder":
        """Set all components to use memory backend."""
        return (self
                .with_memory_paper_registry()
                .with_memory_paper_info()
                .with_memory_author_registry()
                .with_memory_author_info()
                .with_memory_venue_registry()
                .with_memory_venue_info()
                .with_memory_committed_author_links()
                .with_memory_committed_reference_links()
                .with_memory_committed_venue_links()
                .with_memory_pending_papers()
                .with_memory_pending_authors()
                .with_memory_pending_references()
                .with_memory_pending_citations()
                .with_memory_pending_venues())

    def with_all_redis(self, prefix: str = "pw", expire: int | None = None) -> "HybridCacheBuilder":
        """Set all components to use Redis backend with given prefix."""
        exp = expire if expire is not None else self._expire
        return (self
                .with_redis_paper_registry(f"{prefix}:paper_reg", exp)
                .with_redis_paper_info(f"{prefix}:paper_info", exp)
                .with_redis_author_registry(f"{prefix}:author_reg", exp)
                .with_redis_author_info(f"{prefix}:author_info", exp)
                .with_redis_venue_registry(f"{prefix}:venue_reg", exp)
                .with_redis_venue_info(f"{prefix}:venue_info", exp)
                .with_redis_committed_author_links(f"{prefix}:committed_ap", exp)
                .with_redis_committed_reference_links(f"{prefix}:committed_pr", exp)
                .with_redis_committed_venue_links(f"{prefix}:committed_pv", exp)
                .with_redis_pending_papers(f"{prefix}:pending_a2p", exp)
                .with_redis_pending_authors(f"{prefix}:pending_p2a", exp)
                .with_redis_pending_references(f"{prefix}:pending_p2r", exp)
                .with_redis_pending_citations(f"{prefix}:pending_p2c", exp)
                .with_redis_pending_venues(f"{prefix}:pending_p2v", exp))

    # Build methods

    def _ensure_defaults(self):
        """Ensure all components have defaults (memory)."""
        if self._paper_registry is None:
            self._paper_registry = MemoryIdentifierRegistry()
        if self._paper_info is None:
            self._paper_info = MemoryInfoStorage()
        if self._author_registry is None:
            self._author_registry = MemoryIdentifierRegistry()
        if self._author_info is None:
            self._author_info = MemoryInfoStorage()
        if self._venue_registry is None:
            self._venue_registry = MemoryIdentifierRegistry()
        if self._venue_info is None:
            self._venue_info = MemoryInfoStorage()
        if self._committed_author_links is None:
            self._committed_author_links = MemoryCommittedLinkStorage()
        if self._committed_reference_links is None:
            self._committed_reference_links = MemoryCommittedLinkStorage()
        if self._committed_venue_links is None:
            self._committed_venue_links = MemoryCommittedLinkStorage()
        if self._pending_papers is None:
            self._pending_papers = MemoryPendingListStorage()
        if self._pending_authors is None:
            self._pending_authors = MemoryPendingListStorage()
        if self._pending_references is None:
            self._pending_references = MemoryPendingListStorage()
        if self._pending_citations is None:
            self._pending_citations = MemoryPendingListStorage()
        if self._pending_venues is None:
            self._pending_venues = MemoryPendingListStorage()

    def build_weaver_cache(self) -> FullWeaverCache:
        """Build a FullWeaverCache with configured components."""
        self._ensure_defaults()
        return FullWeaverCache(
            paper_registry=self._paper_registry,
            paper_info_storage=self._paper_info,
            author_registry=self._author_registry,
            author_info_storage=self._author_info,
            venue_registry=self._venue_registry,
            venue_info_storage=self._venue_info,
            committed_author_links=self._committed_author_links,
            committed_reference_links=self._committed_reference_links,
            committed_venue_links=self._committed_venue_links,
            pending_papers=self._pending_papers,
            pending_authors=self._pending_authors,
            pending_references=self._pending_references,
            pending_citations=self._pending_citations,
            pending_venues=self._pending_venues,
        )

    def build_author2papers_cache(self) -> Author2PapersCache:
        """Build an Author2PapersCache with configured components."""
        self._ensure_defaults()
        return Author2PapersCache(
            paper_registry=self._paper_registry,
            paper_info_storage=self._paper_info,
            author_registry=self._author_registry,
            author_info_storage=self._author_info,
            venue_registry=self._venue_registry,
            venue_info_storage=self._venue_info,
            committed_author_links=self._committed_author_links,
            pending_papers=self._pending_papers,
        )

    def build_paper2authors_cache(self) -> Paper2AuthorsCache:
        """Build a Paper2AuthorsCache with configured components."""
        self._ensure_defaults()
        return Paper2AuthorsCache(
            paper_registry=self._paper_registry,
            paper_info_storage=self._paper_info,
            author_registry=self._author_registry,
            author_info_storage=self._author_info,
            venue_registry=self._venue_registry,
            venue_info_storage=self._venue_info,
            committed_author_links=self._committed_author_links,
            pending_authors=self._pending_authors,
        )

    def build_paper2references_cache(self) -> Paper2ReferencesCache:
        """Build a Paper2ReferencesCache with configured components."""
        self._ensure_defaults()
        return Paper2ReferencesCache(
            paper_registry=self._paper_registry,
            paper_info_storage=self._paper_info,
            author_registry=self._author_registry,
            author_info_storage=self._author_info,
            venue_registry=self._venue_registry,
            venue_info_storage=self._venue_info,
            committed_reference_links=self._committed_reference_links,
            pending_references=self._pending_references,
        )

    def build_paper2citations_cache(self) -> Paper2CitationsCache:
        """Build a Paper2CitationsCache with configured components."""
        self._ensure_defaults()
        return Paper2CitationsCache(
            paper_registry=self._paper_registry,
            paper_info_storage=self._paper_info,
            author_registry=self._author_registry,
            author_info_storage=self._author_info,
            venue_registry=self._venue_registry,
            venue_info_storage=self._venue_info,
            committed_reference_links=self._committed_reference_links,
            pending_citations=self._pending_citations,
        )

    def build_paper2venues_cache(self) -> Paper2VenuesCache:
        """Build a Paper2VenuesCache with configured components."""
        self._ensure_defaults()
        return Paper2VenuesCache(
            paper_registry=self._paper_registry,
            paper_info_storage=self._paper_info,
            author_registry=self._author_registry,
            author_info_storage=self._author_info,
            venue_registry=self._venue_registry,
            venue_info_storage=self._venue_info,
            committed_venue_links=self._committed_venue_links,
            pending_venues=self._pending_venues,
        )
