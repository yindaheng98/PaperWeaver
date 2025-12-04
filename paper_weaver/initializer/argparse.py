"""
Command-line argument configuration for PaperWeaver Initializers.

Supports:
- DBLP: Initialize with DBLP record keys, person IDs, or venue keys
"""

import argparse

from ..iface_init import WeaverInitializerIface
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
    parser.add_argument(
        "--init-mode",
        choices=["papers", "authors", "venues"],
        default="authors",
        help="Initialization mode: papers, authors, or venues (default: authors)"
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


def create_initializer_from_args(args: argparse.Namespace) -> WeaverInitializerIface:
    """Create an initializer from parsed command-line arguments."""
    match args.init_type:
        case "dblp":
            match args.init_mode:
                case "papers":
                    return DBLPPapersInitializer(list(args.init_dblp_record_keys))
                case "authors":
                    return DBLPAuthorsInitializer(list(args.init_dblp_pids))
                case "venues":
                    return DBLPVenuesInitializer(list(args.init_dblp_venue_keys))
                case _:
                    raise ValueError(f"Unknown init mode: {args.init_mode}")
        case _:
            raise ValueError(f"Unknown initializer type: {args.init_type}")
