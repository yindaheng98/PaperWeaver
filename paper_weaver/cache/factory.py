"""
Factory functions for creating cache instances with different backends.

Provides convenient functions to create fully configured cache instances
using either memory or Redis backends.
"""

from .memory import MemoryIdentifierRegistry, MemoryInfoStorage, MemoryCommittedLinkStorage, MemoryPendingListStorage
from .redis import RedisIdentifierRegistry, RedisInfoStorage, RedisCommittedLinkStorage, RedisPendingListStorage
from .composite import (
    FullAuthorWeaverCache,
    FullPaperWeaverCache,
    Author2PapersCache,
    Paper2AuthorsCache,
    Paper2ReferencesCache,
    Paper2CitationsCache,
)


def create_memory_author_weaver_cache() -> FullAuthorWeaverCache:
    """Create an in-memory cache for AuthorWeaver."""
    return FullAuthorWeaverCache(
        paper_registry=MemoryIdentifierRegistry(),
        paper_info_storage=MemoryInfoStorage(),
        author_registry=MemoryIdentifierRegistry(),
        author_info_storage=MemoryInfoStorage(),
        committed_author_links=MemoryCommittedLinkStorage(),
        pending_papers=MemoryPendingListStorage(),
        pending_authors=MemoryPendingListStorage(),
    )


def create_redis_author_weaver_cache(redis_client, prefix: str = "pw") -> FullAuthorWeaverCache:
    """
    Create a Redis-backed cache for AuthorWeaver.

    Args:
        redis_client: An async Redis client (e.g., from redis.asyncio)
        prefix: Prefix for all Redis keys
    """
    return FullAuthorWeaverCache(
        paper_registry=RedisIdentifierRegistry(redis_client, f"{prefix}:paper_reg"),
        paper_info_storage=RedisInfoStorage(redis_client, f"{prefix}:paper_info"),
        author_registry=RedisIdentifierRegistry(redis_client, f"{prefix}:author_reg"),
        author_info_storage=RedisInfoStorage(redis_client, f"{prefix}:author_info"),
        committed_author_links=RedisCommittedLinkStorage(redis_client, f"{prefix}:committed_ap"),
        pending_papers=RedisPendingListStorage(redis_client, f"{prefix}:pending_a2p"),
        pending_authors=RedisPendingListStorage(redis_client, f"{prefix}:pending_p2a"),
    )


def create_memory_paper_weaver_cache() -> FullPaperWeaverCache:
    """Create an in-memory cache for full paper operations."""
    return FullPaperWeaverCache(
        paper_registry=MemoryIdentifierRegistry(),
        paper_info_storage=MemoryInfoStorage(),
        author_registry=MemoryIdentifierRegistry(),
        author_info_storage=MemoryInfoStorage(),
        committed_author_links=MemoryCommittedLinkStorage(),
        committed_reference_links=MemoryCommittedLinkStorage(),
        pending_authors=MemoryPendingListStorage(),
        pending_references=MemoryPendingListStorage(),
        pending_citations=MemoryPendingListStorage(),
    )


def create_redis_paper_weaver_cache(redis_client, prefix: str = "pw") -> FullPaperWeaverCache:
    """
    Create a Redis-backed cache for full paper operations.

    Args:
        redis_client: An async Redis client (e.g., from redis.asyncio)
        prefix: Prefix for all Redis keys
    """
    return FullPaperWeaverCache(
        paper_registry=RedisIdentifierRegistry(redis_client, f"{prefix}:paper_reg"),
        paper_info_storage=RedisInfoStorage(redis_client, f"{prefix}:paper_info"),
        author_registry=RedisIdentifierRegistry(redis_client, f"{prefix}:author_reg"),
        author_info_storage=RedisInfoStorage(redis_client, f"{prefix}:author_info"),
        committed_author_links=RedisCommittedLinkStorage(redis_client, f"{prefix}:committed_ap"),
        committed_reference_links=RedisCommittedLinkStorage(redis_client, f"{prefix}:committed_pr"),
        pending_authors=RedisPendingListStorage(redis_client, f"{prefix}:pending_p2a"),
        pending_references=RedisPendingListStorage(redis_client, f"{prefix}:pending_p2r"),
        pending_citations=RedisPendingListStorage(redis_client, f"{prefix}:pending_p2c"),
    )


# Hybrid cache builders for mixing memory and redis backends

class HybridCacheBuilder:
    """
    Builder for creating caches with mixed memory/redis backends.

    Allows fine-grained control over which components use which backend.

    Example:
        builder = HybridCacheBuilder(redis_client)
        cache = (builder
            .with_memory_paper_registry()
            .with_redis_paper_info("paper_info")
            .with_memory_author_registry()
            .with_redis_author_info("author_info")
            .with_memory_committed_author_links()
            .build_author_weaver_cache())
    """

    def __init__(self, redis_client=None):
        self._redis = redis_client
        self._paper_registry = None
        self._paper_info = None
        self._author_registry = None
        self._author_info = None
        self._committed_author_links = None
        self._committed_reference_links = None
        self._pending_papers = None
        self._pending_authors = None
        self._pending_references = None
        self._pending_citations = None

    # Paper registry

    def with_memory_paper_registry(self) -> "HybridCacheBuilder":
        self._paper_registry = MemoryIdentifierRegistry()
        return self

    def with_redis_paper_registry(self, prefix: str = "paper_reg") -> "HybridCacheBuilder":
        self._paper_registry = RedisIdentifierRegistry(self._redis, prefix)
        return self

    # Paper info

    def with_memory_paper_info(self) -> "HybridCacheBuilder":
        self._paper_info = MemoryInfoStorage()
        return self

    def with_redis_paper_info(self, prefix: str = "paper_info") -> "HybridCacheBuilder":
        self._paper_info = RedisInfoStorage(self._redis, prefix)
        return self

    # Author registry

    def with_memory_author_registry(self) -> "HybridCacheBuilder":
        self._author_registry = MemoryIdentifierRegistry()
        return self

    def with_redis_author_registry(self, prefix: str = "author_reg") -> "HybridCacheBuilder":
        self._author_registry = RedisIdentifierRegistry(self._redis, prefix)
        return self

    # Author info

    def with_memory_author_info(self) -> "HybridCacheBuilder":
        self._author_info = MemoryInfoStorage()
        return self

    def with_redis_author_info(self, prefix: str = "author_info") -> "HybridCacheBuilder":
        self._author_info = RedisInfoStorage(self._redis, prefix)
        return self

    # Committed links

    def with_memory_committed_author_links(self) -> "HybridCacheBuilder":
        self._committed_author_links = MemoryCommittedLinkStorage()
        return self

    def with_redis_committed_author_links(self, prefix: str = "committed_ap") -> "HybridCacheBuilder":
        self._committed_author_links = RedisCommittedLinkStorage(self._redis, prefix)
        return self

    def with_memory_committed_reference_links(self) -> "HybridCacheBuilder":
        self._committed_reference_links = MemoryCommittedLinkStorage()
        return self

    def with_redis_committed_reference_links(self, prefix: str = "committed_pr") -> "HybridCacheBuilder":
        self._committed_reference_links = RedisCommittedLinkStorage(self._redis, prefix)
        return self

    # Pending lists

    def with_memory_pending_papers(self) -> "HybridCacheBuilder":
        self._pending_papers = MemoryPendingListStorage()
        return self

    def with_redis_pending_papers(self, prefix: str = "pending_a2p") -> "HybridCacheBuilder":
        self._pending_papers = RedisPendingListStorage(self._redis, prefix)
        return self

    def with_memory_pending_authors(self) -> "HybridCacheBuilder":
        self._pending_authors = MemoryPendingListStorage()
        return self

    def with_redis_pending_authors(self, prefix: str = "pending_p2a") -> "HybridCacheBuilder":
        self._pending_authors = RedisPendingListStorage(self._redis, prefix)
        return self

    def with_memory_pending_references(self) -> "HybridCacheBuilder":
        self._pending_references = MemoryPendingListStorage()
        return self

    def with_redis_pending_references(self, prefix: str = "pending_p2r") -> "HybridCacheBuilder":
        self._pending_references = RedisPendingListStorage(self._redis, prefix)
        return self

    def with_memory_pending_citations(self) -> "HybridCacheBuilder":
        self._pending_citations = MemoryPendingListStorage()
        return self

    def with_redis_pending_citations(self, prefix: str = "pending_p2c") -> "HybridCacheBuilder":
        self._pending_citations = RedisPendingListStorage(self._redis, prefix)
        return self

    # Convenience methods for setting all components at once

    def with_all_memory(self) -> "HybridCacheBuilder":
        """Set all components to use memory backend."""
        return (self
                .with_memory_paper_registry()
                .with_memory_paper_info()
                .with_memory_author_registry()
                .with_memory_author_info()
                .with_memory_committed_author_links()
                .with_memory_committed_reference_links()
                .with_memory_pending_papers()
                .with_memory_pending_authors()
                .with_memory_pending_references()
                .with_memory_pending_citations())

    def with_all_redis(self, prefix: str = "pw") -> "HybridCacheBuilder":
        """Set all components to use Redis backend with given prefix."""
        return (self
                .with_redis_paper_registry(f"{prefix}:paper_reg")
                .with_redis_paper_info(f"{prefix}:paper_info")
                .with_redis_author_registry(f"{prefix}:author_reg")
                .with_redis_author_info(f"{prefix}:author_info")
                .with_redis_committed_author_links(f"{prefix}:committed_ap")
                .with_redis_committed_reference_links(f"{prefix}:committed_pr")
                .with_redis_pending_papers(f"{prefix}:pending_a2p")
                .with_redis_pending_authors(f"{prefix}:pending_p2a")
                .with_redis_pending_references(f"{prefix}:pending_p2r")
                .with_redis_pending_citations(f"{prefix}:pending_p2c"))

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
        if self._committed_author_links is None:
            self._committed_author_links = MemoryCommittedLinkStorage()
        if self._committed_reference_links is None:
            self._committed_reference_links = MemoryCommittedLinkStorage()
        if self._pending_papers is None:
            self._pending_papers = MemoryPendingListStorage()
        if self._pending_authors is None:
            self._pending_authors = MemoryPendingListStorage()
        if self._pending_references is None:
            self._pending_references = MemoryPendingListStorage()
        if self._pending_citations is None:
            self._pending_citations = MemoryPendingListStorage()

    def build_author_weaver_cache(self) -> FullAuthorWeaverCache:
        """Build a FullAuthorWeaverCache with configured components."""
        self._ensure_defaults()
        return FullAuthorWeaverCache(
            paper_registry=self._paper_registry,
            paper_info_storage=self._paper_info,
            author_registry=self._author_registry,
            author_info_storage=self._author_info,
            committed_author_links=self._committed_author_links,
            pending_papers=self._pending_papers,
            pending_authors=self._pending_authors,
        )

    def build_paper_weaver_cache(self) -> FullPaperWeaverCache:
        """Build a FullPaperWeaverCache with configured components."""
        self._ensure_defaults()
        return FullPaperWeaverCache(
            paper_registry=self._paper_registry,
            paper_info_storage=self._paper_info,
            author_registry=self._author_registry,
            author_info_storage=self._author_info,
            committed_author_links=self._committed_author_links,
            committed_reference_links=self._committed_reference_links,
            pending_authors=self._pending_authors,
            pending_references=self._pending_references,
            pending_citations=self._pending_citations,
        )

    def build_author2papers_cache(self) -> Author2PapersCache:
        """Build an Author2PapersCache with configured components."""
        self._ensure_defaults()
        return Author2PapersCache(
            paper_registry=self._paper_registry,
            paper_info_storage=self._paper_info,
            author_registry=self._author_registry,
            author_info_storage=self._author_info,
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
            committed_reference_links=self._committed_reference_links,
            pending_citations=self._pending_citations,
        )
