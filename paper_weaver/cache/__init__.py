"""
PaperWeaver Cache Module

Provides modular cache implementations with support for:
- In-memory storage (Python built-in data structures)
- Redis storage

Key concepts:
- Identifier Registry: Manages object identity, merging objects with common identifiers
- Info Storage: Stores entity information (dict data)
- Link Storage: Stores relationships between entities
- Composite Cache: Combines storage components into full cache implementations

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
        .with_memory_author_paper_links()
        .with_memory_author_papers_list()
        .with_memory_paper_authors_list()
        .build_author_weaver_cache())
"""

# Identifier Registry
from .identifier import (
    IdentifierRegistryIface,
    MemoryIdentifierRegistry,
    RedisIdentifierRegistry,
)

# Info Storage
from .info_storage import (
    InfoStorageIface,
    MemoryInfoStorage,
    RedisInfoStorage,
    EntityInfoManager,
)

# Link Storage
from .link_storage import (
    LinkStorageIface,
    MemoryLinkStorage,
    RedisLinkStorage,
    EntityListStorageIface,
    MemoryEntityListStorage,
    RedisEntityListStorage,
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
    # Link Storage
    "LinkStorageIface",
    "MemoryLinkStorage",
    "RedisLinkStorage",
    "EntityListStorageIface",
    "MemoryEntityListStorage",
    "RedisEntityListStorage",
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
