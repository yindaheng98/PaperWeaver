"""
PaperWeaver Cache Module

Provides modular cache implementations with support for:
- In-memory storage (Python built-in data structures)
- Redis storage

Key concepts:
- Identifier Registry: Manages object identity, merging objects with common identifiers
- Info Storage: Stores entity information (dict data)
- Committed Link Storage: Tracks links that have been written to DataDst
- Pending List Storage: Stores entity lists that may lack info, awaiting processing

Usage:
    # Simple in-memory cache
    from paper_weaver.cache import create_memory_author_weaver_cache
    cache = create_memory_author_weaver_cache()

    # Redis-backed cache
    import redis.asyncio as redis
    from paper_weaver.cache import create_redis_author_weaver_cache
    r = redis.Redis()
    cache = create_redis_author_weaver_cache(r)

    # Hybrid cache with builder
    from paper_weaver.cache import HybridCacheBuilder
    cache = (HybridCacheBuilder(redis_client)
        .with_memory_paper_registry()
        .with_redis_paper_info("paper_info")
        .with_memory_author_registry()
        .with_redis_author_info("author_info")
        .with_memory_committed_author_links()
        .with_memory_pending_papers()
        .with_memory_pending_authors()
        .build_author_weaver_cache())
"""

# Identifier Registry
from .identifier import (
    IdentifierRegistryIface,
)

# Info Storage
from .info_storage import (
    InfoStorageIface,
    EntityInfoManager,
)

# Link Storage
from .link_storage import (
    CommittedLinkStorageIface,
    PendingListStorageIface,
    PendingListManager,
)

from .memory import (  # noqa: F401
    MemoryIdentifierRegistry,
    MemoryInfoStorage,
    MemoryCommittedLinkStorage,
    MemoryPendingListStorage,
)

from .redis import (  # noqa: F401
    RedisIdentifierRegistry,
    RedisInfoStorage,
    RedisCommittedLinkStorage,
    RedisPendingListStorage,
)

# Composite Cache
from .composite import (
    ComposableCacheBase,
    AuthorLinkCache,
    PaperLinkCache,
    Author2PapersCache,
    Paper2AuthorsCache,
    Paper2ReferencesCache,
    Paper2CitationsCache,
    FullAuthorWeaverCache,
    FullPaperWeaverCache,
)

# Factory functions
from .factory import (
    create_memory_author_weaver_cache,
    create_redis_author_weaver_cache,
    create_memory_paper_weaver_cache,
    create_redis_paper_weaver_cache,
    HybridCacheBuilder,
)

__all__ = [
    # Identifier Registry
    "IdentifierRegistryIface",
    "MemoryIdentifierRegistry",
    "RedisIdentifierRegistry",
    # Info Storage
    "InfoStorageIface",
    "MemoryInfoStorage",
    "RedisInfoStorage",
    "EntityInfoManager",
    # Committed Link Storage
    "CommittedLinkStorageIface",
    "MemoryCommittedLinkStorage",
    "RedisCommittedLinkStorage",
    # Pending List Storage
    "PendingListStorageIface",
    "PendingListManager",
    "MemoryPendingListStorage",
    "RedisPendingListStorage",
    # Composite Cache
    "ComposableCacheBase",
    "AuthorLinkCache",
    "PaperLinkCache",
    "Author2PapersCache",
    "Paper2AuthorsCache",
    "Paper2ReferencesCache",
    "Paper2CitationsCache",
    "FullAuthorWeaverCache",
    "FullPaperWeaverCache",
    # Factory
    "create_memory_author_weaver_cache",
    "create_redis_author_weaver_cache",
    "create_memory_paper_weaver_cache",
    "create_redis_paper_weaver_cache",
    "HybridCacheBuilder",
]
