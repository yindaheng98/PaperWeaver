"""
Factory functions for creating cache instances with different backends.

Provides convenient functions to create fully configured cache instances
using either memory or Redis backends.
"""

from .memory import MemoryIdentifierRegistry, MemoryInfoStorage, MemoryLinkStorage, MemoryEntityListStorage
from .redis import RedisIdentifierRegistry, RedisInfoStorage, RedisLinkStorage, RedisEntityListStorage
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
        author_paper_links=MemoryLinkStorage(),
        author_papers_list=MemoryEntityListStorage(),
        paper_authors_list=MemoryEntityListStorage(),
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
        author_paper_links=RedisLinkStorage(redis_client, f"{prefix}:ap_link"),
        author_papers_list=RedisEntityListStorage(redis_client, f"{prefix}:a2p_list"),
        paper_authors_list=RedisEntityListStorage(redis_client, f"{prefix}:p2a_list"),
    )


def create_memory_paper_weaver_cache() -> FullPaperWeaverCache:
    """Create an in-memory cache for full paper operations."""
    return FullPaperWeaverCache(
        paper_registry=MemoryIdentifierRegistry(),
        paper_info_storage=MemoryInfoStorage(),
        author_registry=MemoryIdentifierRegistry(),
        author_info_storage=MemoryInfoStorage(),
        author_paper_links=MemoryLinkStorage(),
        paper_reference_links=MemoryLinkStorage(),
        paper_authors_list=MemoryEntityListStorage(),
        paper_references_list=MemoryEntityListStorage(),
        paper_citations_list=MemoryEntityListStorage(),
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
        author_paper_links=RedisLinkStorage(redis_client, f"{prefix}:ap_link"),
        paper_reference_links=RedisLinkStorage(redis_client, f"{prefix}:pr_link"),
        paper_authors_list=RedisEntityListStorage(redis_client, f"{prefix}:p2a_list"),
        paper_references_list=RedisEntityListStorage(redis_client, f"{prefix}:p2r_list"),
        paper_citations_list=RedisEntityListStorage(redis_client, f"{prefix}:p2c_list"),
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
            .with_memory_links()
            .build_author_weaver_cache())
    """

    def __init__(self, redis_client=None):
        self._redis = redis_client
        self._paper_registry = None
        self._paper_info = None
        self._author_registry = None
        self._author_info = None
        self._author_paper_links = None
        self._paper_reference_links = None
        self._author_papers_list = None
        self._paper_authors_list = None
        self._paper_references_list = None
        self._paper_citations_list = None

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

    # Links

    def with_memory_author_paper_links(self) -> "HybridCacheBuilder":
        self._author_paper_links = MemoryLinkStorage()
        return self

    def with_redis_author_paper_links(self, prefix: str = "ap_link") -> "HybridCacheBuilder":
        self._author_paper_links = RedisLinkStorage(self._redis, prefix)
        return self

    def with_memory_paper_reference_links(self) -> "HybridCacheBuilder":
        self._paper_reference_links = MemoryLinkStorage()
        return self

    def with_redis_paper_reference_links(self, prefix: str = "pr_link") -> "HybridCacheBuilder":
        self._paper_reference_links = RedisLinkStorage(self._redis, prefix)
        return self

    # Entity lists

    def with_memory_author_papers_list(self) -> "HybridCacheBuilder":
        self._author_papers_list = MemoryEntityListStorage()
        return self

    def with_redis_author_papers_list(self, prefix: str = "a2p_list") -> "HybridCacheBuilder":
        self._author_papers_list = RedisEntityListStorage(self._redis, prefix)
        return self

    def with_memory_paper_authors_list(self) -> "HybridCacheBuilder":
        self._paper_authors_list = MemoryEntityListStorage()
        return self

    def with_redis_paper_authors_list(self, prefix: str = "p2a_list") -> "HybridCacheBuilder":
        self._paper_authors_list = RedisEntityListStorage(self._redis, prefix)
        return self

    def with_memory_paper_references_list(self) -> "HybridCacheBuilder":
        self._paper_references_list = MemoryEntityListStorage()
        return self

    def with_redis_paper_references_list(self, prefix: str = "p2r_list") -> "HybridCacheBuilder":
        self._paper_references_list = RedisEntityListStorage(self._redis, prefix)
        return self

    def with_memory_paper_citations_list(self) -> "HybridCacheBuilder":
        self._paper_citations_list = MemoryEntityListStorage()
        return self

    def with_redis_paper_citations_list(self, prefix: str = "p2c_list") -> "HybridCacheBuilder":
        self._paper_citations_list = RedisEntityListStorage(self._redis, prefix)
        return self

    # Convenience methods for setting all components at once

    def with_all_memory(self) -> "HybridCacheBuilder":
        """Set all components to use memory backend."""
        return (self
                .with_memory_paper_registry()
                .with_memory_paper_info()
                .with_memory_author_registry()
                .with_memory_author_info()
                .with_memory_author_paper_links()
                .with_memory_paper_reference_links()
                .with_memory_author_papers_list()
                .with_memory_paper_authors_list()
                .with_memory_paper_references_list()
                .with_memory_paper_citations_list())

    def with_all_redis(self, prefix: str = "pw") -> "HybridCacheBuilder":
        """Set all components to use Redis backend with given prefix."""
        return (self
                .with_redis_paper_registry(f"{prefix}:paper_reg")
                .with_redis_paper_info(f"{prefix}:paper_info")
                .with_redis_author_registry(f"{prefix}:author_reg")
                .with_redis_author_info(f"{prefix}:author_info")
                .with_redis_author_paper_links(f"{prefix}:ap_link")
                .with_redis_paper_reference_links(f"{prefix}:pr_link")
                .with_redis_author_papers_list(f"{prefix}:a2p_list")
                .with_redis_paper_authors_list(f"{prefix}:p2a_list")
                .with_redis_paper_references_list(f"{prefix}:p2r_list")
                .with_redis_paper_citations_list(f"{prefix}:p2c_list"))

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
        if self._author_paper_links is None:
            self._author_paper_links = MemoryLinkStorage()
        if self._paper_reference_links is None:
            self._paper_reference_links = MemoryLinkStorage()
        if self._author_papers_list is None:
            self._author_papers_list = MemoryEntityListStorage()
        if self._paper_authors_list is None:
            self._paper_authors_list = MemoryEntityListStorage()
        if self._paper_references_list is None:
            self._paper_references_list = MemoryEntityListStorage()
        if self._paper_citations_list is None:
            self._paper_citations_list = MemoryEntityListStorage()

    def build_author_weaver_cache(self) -> FullAuthorWeaverCache:
        """Build a FullAuthorWeaverCache with configured components."""
        self._ensure_defaults()
        return FullAuthorWeaverCache(
            paper_registry=self._paper_registry,
            paper_info_storage=self._paper_info,
            author_registry=self._author_registry,
            author_info_storage=self._author_info,
            author_paper_links=self._author_paper_links,
            author_papers_list=self._author_papers_list,
            paper_authors_list=self._paper_authors_list,
        )

    def build_paper_weaver_cache(self) -> FullPaperWeaverCache:
        """Build a FullPaperWeaverCache with configured components."""
        self._ensure_defaults()
        return FullPaperWeaverCache(
            paper_registry=self._paper_registry,
            paper_info_storage=self._paper_info,
            author_registry=self._author_registry,
            author_info_storage=self._author_info,
            author_paper_links=self._author_paper_links,
            paper_reference_links=self._paper_reference_links,
            paper_authors_list=self._paper_authors_list,
            paper_references_list=self._paper_references_list,
            paper_citations_list=self._paper_citations_list,
        )

    def build_author2papers_cache(self) -> Author2PapersCache:
        """Build an Author2PapersCache with configured components."""
        self._ensure_defaults()
        return Author2PapersCache(
            paper_registry=self._paper_registry,
            paper_info_storage=self._paper_info,
            author_registry=self._author_registry,
            author_info_storage=self._author_info,
            author_paper_links=self._author_paper_links,
            author_papers_list=self._author_papers_list,
        )

    def build_paper2authors_cache(self) -> Paper2AuthorsCache:
        """Build a Paper2AuthorsCache with configured components."""
        self._ensure_defaults()
        return Paper2AuthorsCache(
            paper_registry=self._paper_registry,
            paper_info_storage=self._paper_info,
            author_registry=self._author_registry,
            author_info_storage=self._author_info,
            author_paper_links=self._author_paper_links,
            paper_authors_list=self._paper_authors_list,
        )

    def build_paper2references_cache(self) -> Paper2ReferencesCache:
        """Build a Paper2ReferencesCache with configured components."""
        self._ensure_defaults()
        return Paper2ReferencesCache(
            paper_registry=self._paper_registry,
            paper_info_storage=self._paper_info,
            author_registry=self._author_registry,
            author_info_storage=self._author_info,
            paper_reference_links=self._paper_reference_links,
            paper_references_list=self._paper_references_list,
        )

    def build_paper2citations_cache(self) -> Paper2CitationsCache:
        """Build a Paper2CitationsCache with configured components."""
        self._ensure_defaults()
        return Paper2CitationsCache(
            paper_registry=self._paper_registry,
            paper_info_storage=self._paper_info,
            author_registry=self._author_registry,
            author_info_storage=self._author_info,
            paper_reference_links=self._paper_reference_links,
            paper_citations_list=self._paper_citations_list,
        )
