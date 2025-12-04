"""
Command-line argument configuration for PaperWeaver DataSrc.

Supports:
- SemanticScholar: Semantic Scholar API
- DBLP: DBLP API
"""

import argparse

from ..dataclass import DataSrc
from .cache_impl import MemoryDataSrcCache, RedisDataSrcCache
from .semanticscholar import SemanticScholarDataSrc
from .dblp import DBLPDataSrc


def add_datasrc_args(parser: argparse.ArgumentParser) -> None:
    """Add DataSrc-related command-line arguments."""
    parser.add_argument("--datasrc-type", choices=["semanticscholar", "dblp"], default="dblp", help="DataSrc type (default: dblp)")

    # DataSrc cache
    parser.add_argument("--datasrc-cache-mode", choices=["memory", "redis"], default="memory", help="DataSrc cache backend (default: memory)")
    parser.add_argument("--datasrc-redis-url", default="redis://localhost:6379", help="Redis URL for DataSrc cache (default: redis://localhost:6379)")
    parser.add_argument("--datasrc-redis-prefix", default="paper-weaver-datasrc-cache", help="Redis key prefix for DataSrc cache (default: paper-weaver-datasrc-cache)")

    # Common HTTP settings
    parser.add_argument("--datasrc-max-concurrent", type=int, default=10, help="Maximum concurrent requests (default: 10)")
    parser.add_argument("--datasrc-http-proxy", help="HTTP proxy URL")
    parser.add_argument("--datasrc-http-timeout", type=int, default=30, help="HTTP timeout in seconds (default: 30)")

    # SemanticScholar specific
    parser.add_argument("--datasrc-ss-cache-ttl", type=int, default=604800, help="SemanticScholar cache TTL in seconds (default: 604800 = 7 days, API data is relatively stable)")
    parser.add_argument("--datasrc-ss-api-key", help="SemanticScholar API key")

    # DBLP specific
    parser.add_argument("--datasrc-dblp-record-ttl", type=int, help="DBLP record cache TTL seconds (default: None, permanent, publication records rarely change)")
    parser.add_argument("--datasrc-dblp-person-ttl", type=int, default=604800, help="DBLP person cache TTL seconds (default: 604800 = 7 days, person info may be updated)")
    parser.add_argument("--datasrc-dblp-venue-ttl", type=int, default=604800, help="DBLP venue cache TTL seconds (default: 604800 = 7 days, venue info may be updated)")


def create_datasrc_from_args(args: argparse.Namespace) -> DataSrc:
    """Create a DataSrc from parsed command-line arguments."""
    # Create cache
    match args.datasrc_cache_mode:
        case "memory":
            cache = MemoryDataSrcCache()
        case "redis":
            import redis.asyncio as redis
            client = redis.from_url(args.datasrc_redis_url)
            cache = RedisDataSrcCache(client, args.datasrc_redis_prefix)
        case _:
            raise ValueError(f"Unknown datasrc cache mode: {args.datasrc_cache_mode}")

    # Create datasrc
    match args.datasrc_type:
        case "semanticscholar":
            http_headers = {"x-api-key": args.datasrc_ss_api_key} if args.datasrc_ss_api_key else None
            return SemanticScholarDataSrc(
                cache=cache,
                max_concurrent=args.datasrc_max_concurrent,
                cache_ttl=args.datasrc_ss_cache_ttl,
                http_headers=http_headers,
                http_proxy=args.datasrc_http_proxy,
                http_timeout=args.datasrc_http_timeout
            )
        case "dblp":
            return DBLPDataSrc(
                cache=cache,
                max_concurrent=args.datasrc_max_concurrent,
                record_cache_ttl=args.datasrc_dblp_record_ttl,
                person_cache_ttl=args.datasrc_dblp_person_ttl,
                venue_cache_ttl=args.datasrc_dblp_venue_ttl,
                http_proxy=args.datasrc_http_proxy,
                http_timeout=args.datasrc_http_timeout
            )
        case _:
            raise ValueError(f"Unknown datasrc type: {args.datasrc_type}")
