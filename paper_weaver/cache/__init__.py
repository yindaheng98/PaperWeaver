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
    from paper_weaver.cache import create_memory_weaver_cache
    cache = create_memory_weaver_cache()

    # Redis-backed cache
    import redis.asyncio as redis
    from paper_weaver.cache import create_redis_weaver_cache
    r = redis.Redis()
    cache = create_redis_weaver_cache(r)

    # Hybrid cache with builder
    from paper_weaver.cache import HybridCacheBuilder
    cache = (HybridCacheBuilder(redis_client)
        .with_memory_paper_registry()
        .with_redis_paper_info("paper_info")
        .with_memory_author_registry()
        .with_redis_author_info("author_info")
        .with_memory_committed_author_links()
        .with_memory_pending_papers_by_author()   # for author's pending papers
        .with_memory_pending_authors_by_paper()  # for paper's pending authors
        .build_weaver_cache())
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
)

# Pending Storage
from .pending_storage import (
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
from .impl_full import (
    FullWeaverCache,
)
from .impl import (  # noqa: F401
    ComposableCacheBase,
)
from .impl_link import (  # noqa: F401
    AuthorLinkCache,
    PaperLinkCache,
    VenueLinkCache,
)
from .impl_a2p import (  # noqa: F401
    Author2PapersCache,
)
from .impl_p2a import (  # noqa: F401
    Paper2AuthorsCache,
)
from .impl_p2r import (  # noqa: F401
    Paper2ReferencesCache,
)
from .impl_p2c import (  # noqa: F401
    Paper2CitationsCache,
)
from .impl_p2v import (  # noqa: F401
    Paper2VenuesCache,
)
from .impl_v2p import (  # noqa: F401
    Venue2PapersCache,
)

# Factory functions
from .factory import (
    create_memory_weaver_cache,
    create_redis_weaver_cache,
    HybridCacheBuilder,
)

# For command-line argument parsing
from .argparse import (  # noqa: F401
    add_cache_args,
    create_cache_from_args,
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
    "VenueLinkCache",
    "Author2PapersCache",
    "Paper2AuthorsCache",
    "Paper2ReferencesCache",
    "Paper2CitationsCache",
    "Paper2VenuesCache",
    "Venue2PapersCache",
    "FullWeaverCache",
    # Factory
    "create_memory_weaver_cache",
    "create_redis_weaver_cache",
    "HybridCacheBuilder",
    # Argparse
    "add_cache_args",
    "create_cache_from_args",
]
