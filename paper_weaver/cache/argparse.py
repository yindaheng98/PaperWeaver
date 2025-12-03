"""
Command-line argument configuration for PaperWeaver cache.

Supports:
- Memory mode: Simple in-memory cache
- Redis mode: Redis-backed cache with 4 redis clients (reg, info, committed, pending)
"""

import argparse

from .factory import HybridCacheBuilder, FullWeaverCache


def add_cache_args(parser: argparse.ArgumentParser) -> None:
    """Add cache-related command-line arguments."""
    parser.add_argument("--cache-mode", choices=["memory", "redis"], default="memory", help="Cache backend mode (default: memory)")
    parser.add_argument("--cache-redis-prefix", default="pw", help="Redis key prefix (default: pw)")

    # Redis connection - 4 clients
    parser.add_argument("--cache-redis-url", default="redis://localhost:6379",
                        help="Default Redis URL for all storages")
    parser.add_argument("--cache-redis-reg-url", help="Redis URL for registry storage")
    parser.add_argument("--cache-redis-info-url", help="Redis URL for info storage")
    parser.add_argument("--cache-redis-committed-url", help="Redis URL for committed storage")
    parser.add_argument("--cache-redis-pending-url", help="Redis URL for pending storage")

    # TTL for *_info (3 items)
    parser.add_argument("--cache-paper-info-expire", type=int, help="TTL seconds for paper_info")
    parser.add_argument("--cache-author-info-expire", type=int, help="TTL seconds for author_info")
    parser.add_argument("--cache-venue-info-expire", type=int, help="TTL seconds for venue_info")

    # TTL for pending_* (5 items)
    parser.add_argument("--cache-pending-papers-expire", type=int, help="TTL seconds for pending_papers")
    parser.add_argument("--cache-pending-authors-expire", type=int, help="TTL seconds for pending_authors")
    parser.add_argument("--cache-pending-references-expire", type=int, help="TTL seconds for pending_references")
    parser.add_argument("--cache-pending-citations-expire", type=int, help="TTL seconds for pending_citations")
    parser.add_argument("--cache-pending-venues-expire", type=int, help="TTL seconds for pending_venues")


def create_cache_from_args(args: argparse.Namespace) -> FullWeaverCache:
    """Create a FullWeaverCache from parsed command-line arguments."""
    builder = HybridCacheBuilder()

    if args.cache_mode == "memory":
        return builder.with_all_memory().build_weaver_cache()

    # Redis mode
    import redis.asyncio as redis

    default_url = args.cache_redis_url
    reg_client = redis.from_url(args.cache_redis_reg_url or default_url)
    info_client = redis.from_url(args.cache_redis_info_url or default_url)
    committed_client = redis.from_url(args.cache_redis_committed_url or default_url)
    pending_client = redis.from_url(args.cache_redis_pending_url or default_url)

    prefix = args.cache_redis_prefix

    # Registry (permanent)
    builder.with_redis_paper_registry(f"{prefix}:paper_reg", None, reg_client)
    builder.with_redis_author_registry(f"{prefix}:author_reg", None, reg_client)
    builder.with_redis_venue_registry(f"{prefix}:venue_reg", None, reg_client)

    # Info (configurable expire)
    builder.with_redis_paper_info(f"{prefix}:paper_info", args.cache_paper_info_expire, info_client)
    builder.with_redis_author_info(f"{prefix}:author_info", args.cache_author_info_expire, info_client)
    builder.with_redis_venue_info(f"{prefix}:venue_info", args.cache_venue_info_expire, info_client)

    # Committed (permanent)
    builder.with_redis_committed_author_links(f"{prefix}:committed_ap", None, committed_client)
    builder.with_redis_committed_reference_links(f"{prefix}:committed_pr", None, committed_client)
    builder.with_redis_committed_venue_links(f"{prefix}:committed_pv", None, committed_client)

    # Pending (configurable expire)
    builder.with_redis_pending_papers(f"{prefix}:pending_a2p", args.cache_pending_papers_expire, pending_client)
    builder.with_redis_pending_authors(f"{prefix}:pending_p2a", args.cache_pending_authors_expire, pending_client)
    builder.with_redis_pending_references(f"{prefix}:pending_p2r", args.cache_pending_references_expire, pending_client)
    builder.with_redis_pending_citations(f"{prefix}:pending_p2c", args.cache_pending_citations_expire, pending_client)
    builder.with_redis_pending_venues(f"{prefix}:pending_p2v", args.cache_pending_venues_expire, pending_client)

    return builder.build_weaver_cache()
