"""
Command-line argument configuration for PaperWeaver Initializers.

Supports:
- DBLP: Initialize with DBLP record keys, person IDs, venue keys, or venue index keys
- CrossRef: Initialize with DOIs
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
        case _:
            raise ValueError(f"Unknown initializer: {args.init}")
