"""
Command-line argument configuration for DBLP Initializers.

Supports:
- DBLPPapersInitializer: Initialize with DBLP record keys
- DBLPAuthorsInitializer: Initialize with DBLP person IDs (pids)
- DBLPVenuesInitializer: Initialize with DBLP venue keys
"""

import argparse

from ...iface_init import (
    PapersWeaverInitializerIface,
    AuthorsWeaverInitializerIface,
    VenuesWeaverInitializerIface,
)
from .initializer import (
    DBLPPapersInitializer,
    DBLPAuthorsInitializer,
    DBLPVenuesInitializer,
)


def add_dblp_papers_initializer_args(parser: argparse.ArgumentParser) -> None:
    """Add DBLP papers initializer command-line arguments."""
    parser.add_argument(
        "--dblp-record-keys",
        nargs="+",
        default=[],
        help="DBLP record keys to initialize papers (e.g., conf/cvpr/HeZRS16 journals/pami/HeZRS16)"
    )


def add_dblp_authors_initializer_args(parser: argparse.ArgumentParser) -> None:
    """Add DBLP authors initializer command-line arguments."""
    parser.add_argument(
        "--dblp-pids",
        nargs="+",
        default=[],
        help="DBLP person IDs to initialize authors (e.g., h/KaimingHe 74/1552)"
    )


def add_dblp_venues_initializer_args(parser: argparse.ArgumentParser) -> None:
    """Add DBLP venues initializer command-line arguments."""
    parser.add_argument(
        "--dblp-venue-keys",
        nargs="+",
        default=[],
        help="DBLP venue keys to initialize venues (e.g., db/conf/cvpr/cvpr2016 db/journals/pami/pami45)"
    )


def create_dblp_papers_initializer_from_args(args: argparse.Namespace) -> PapersWeaverInitializerIface:
    """Create a DBLP papers initializer from parsed command-line arguments."""
    return DBLPPapersInitializer(list(args.dblp_record_keys))


def create_dblp_authors_initializer_from_args(args: argparse.Namespace) -> AuthorsWeaverInitializerIface:
    """Create a DBLP authors initializer from parsed command-line arguments."""
    return DBLPAuthorsInitializer(list(args.dblp_pids))


def create_dblp_venues_initializer_from_args(args: argparse.Namespace) -> VenuesWeaverInitializerIface:
    """Create a DBLP venues initializer from parsed command-line arguments."""
    return DBLPVenuesInitializer(list(args.dblp_venue_keys))
