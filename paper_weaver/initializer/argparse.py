"""
Command-line argument configuration for PaperWeaver Initializers.

Supports:
- DBLP: Initialize with DBLP record keys, person IDs, venue keys, or venue index keys
- CrossRef: Initialize with DOIs
- arXiv: Initialize with arXiv search query
"""

import argparse

from ..iface_init import WeaverInitializerIface
from .dblp import (
    DBLPPapersInitializer,
    DBLPAuthorsInitializer,
    DBLPVenuesInitializer,
    DBLPVenueIndexInitializer,
)
from .crossref import CrossRefPapersInitializer
from .crossref import CrossRefNeo4JPapersInitializer
from .arxiv import ArxivPapersInitializer
from ..datasrc.argparse import create_datasrc_from_args


def add_initializer_args(parser: argparse.ArgumentParser) -> None:
    """Add initializer-related command-line arguments."""
    parser.add_argument(
        "--init",
        choices=[
            "dblp-papers",
            "dblp-authors",
            "dblp-venues",
            "dblp-venue-index",
            "crossref-papers",
            "crossref-neo4j-papers",
            "arxiv-query",
        ],
        default="dblp-authors",
        help="Initializer mode (default: dblp-authors)"
    )

    # DBLP specific
    parser.add_argument(
        "--init-dblp-record-keys",
        nargs="+",
        default=[],
        help="DBLP record keys to initialize papers (e.g., conf/cvpr/HeZRS16 journals/pami/HeZRS16)"
    )
    parser.add_argument(
        "--init-dblp-pids",
        nargs="+",
        default=[],
        help="DBLP person IDs to initialize authors (e.g., h/KaimingHe 74/1552)"
    )
    parser.add_argument(
        "--init-dblp-venue-keys",
        nargs="+",
        default=[],
        help="DBLP venue keys to initialize venues (e.g., db/conf/cvpr/cvpr2016 db/journals/pami/pami45)"
    )
    parser.add_argument(
        "--init-dblp-venue-index-keys",
        nargs="+",
        default=[],
        help="DBLP venue index keys to initialize venues (e.g., db/conf/cvpr db/journals/tpds)"
    )

    # CrossRef specific
    parser.add_argument(
        "--init-crossref-dois",
        nargs="+",
        default=[],
        help="DOIs to initialize papers (e.g., 10.1109/CVPR.2016.90 https://doi.org/10.1000/xyz123)"
    )
    parser.add_argument(
        "--init-crossref-neo4j-patterns",
        nargs="+",
        default=[],
        help="Cypher patterns that bind Paper node as `paper` for Neo4j-based CrossRef initialization"
    )

    # arXiv specific
    parser.add_argument(
        "--init-arxiv-query",
        type=str,
        default="",
        help="arXiv search query (e.g., 'all:machine learning' 'cat:cs.CV')"
    )
    parser.add_argument(
        "--init-arxiv-pages",
        type=int,
        default=1,
        help="Number of query pages to fetch per query (default: 1)"
    )
    parser.add_argument(
        "--init-arxiv-page-size",
        type=int,
        default=10,
        help="Results per page (default: 10)"
    )


def create_initializer_from_args(args: argparse.Namespace) -> WeaverInitializerIface:
    """Create an initializer from parsed command-line arguments."""
    match args.init:
        case "dblp-papers":
            return DBLPPapersInitializer(list(args.init_dblp_record_keys))
        case "dblp-authors":
            return DBLPAuthorsInitializer(list(args.init_dblp_pids))
        case "dblp-venues":
            return DBLPVenuesInitializer(list(args.init_dblp_venue_keys))
        case "dblp-venue-index":
            return DBLPVenueIndexInitializer(list(args.init_dblp_venue_index_keys))
        case "crossref-papers":
            return CrossRefPapersInitializer(list(args.init_crossref_dois))
        case "crossref-neo4j-papers":
            return CrossRefNeo4JPapersInitializer(
                patterns=list(args.init_crossref_neo4j_patterns),
                uri=args.datadst_neo4j_uri,
                user=args.datadst_neo4j_user,
                password=args.datadst_neo4j_password,
                database=args.datadst_neo4j_database,
            )
        case "arxiv-query":
            datasrc = create_datasrc_from_args(args)
            return ArxivPapersInitializer(
                datasrc=datasrc,
                query=args.init_arxiv_query,
                pages=args.init_arxiv_pages,
                page_size=args.init_arxiv_page_size,
            )
        case _:
            raise ValueError(f"Unknown initializer: {args.init}")
