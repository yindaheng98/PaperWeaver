"""
Command-line argument configuration for PaperWeaver Initializers.

Supports:
- DBLP: Initialize with DBLP record keys, person IDs, or venue keys
"""

import argparse

from ..iface_init import (
    PapersWeaverInitializerIface,
    AuthorsWeaverInitializerIface,
    VenuesWeaverInitializerIface,
)
from .dblp import (
    DBLPPapersInitializer,
    DBLPAuthorsInitializer,
    DBLPVenuesInitializer,
)


def add_initializer_args(parser: argparse.ArgumentParser) -> None:
    """Add initializer-related command-line arguments."""
    parser.add_argument(
        "--init-type",
        choices=["dblp"],
        default="dblp",
        help="Initializer type (default: dblp)"
    )

    # DBLP specific
    parser.add_argument(
        "--dblp-record-keys",
        nargs="+",
        default=[],
        help="DBLP record keys to initialize papers (e.g., conf/cvpr/HeZRS16 journals/pami/HeZRS16)"
    )
    parser.add_argument(
        "--dblp-pids",
        nargs="+",
        default=[],
        help="DBLP person IDs to initialize authors (e.g., h/KaimingHe 74/1552)"
    )
    parser.add_argument(
        "--dblp-venue-keys",
        nargs="+",
        default=[],
        help="DBLP venue keys to initialize venues (e.g., db/conf/cvpr/cvpr2016 db/journals/pami/pami45)"
    )


def create_papers_initializer_from_args(args: argparse.Namespace) -> PapersWeaverInitializerIface:
    """Create a papers initializer from parsed command-line arguments."""
    match args.init_type:
        case "dblp":
            return DBLPPapersInitializer(list(args.dblp_record_keys))
        case _:
            raise ValueError(f"Unknown initializer type: {args.init_type}")


def create_authors_initializer_from_args(args: argparse.Namespace) -> AuthorsWeaverInitializerIface:
    """Create an authors initializer from parsed command-line arguments."""
    match args.init_type:
        case "dblp":
            return DBLPAuthorsInitializer(list(args.dblp_pids))
        case _:
            raise ValueError(f"Unknown initializer type: {args.init_type}")


def create_venues_initializer_from_args(args: argparse.Namespace) -> VenuesWeaverInitializerIface:
    """Create a venues initializer from parsed command-line arguments."""
    match args.init_type:
        case "dblp":
            return DBLPVenuesInitializer(list(args.dblp_venue_keys))
        case _:
            raise ValueError(f"Unknown initializer type: {args.init_type}")
