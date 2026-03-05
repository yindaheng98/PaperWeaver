"""
Command-line argument configuration for PaperWeaver Weaver.

Supports:
- a2p2v: Author2Paper2VenueWeaver
- p2r2a: Paper2Reference2AuthorWeaver
- p-only: PaperOnlyWeaver (init-only)
"""

import argparse

from .dataclass import DataSrc, DataDst
from .cache import FullWeaverCache
from .iface_init import WeaverInitializerIface
from .weaver_a2p2v import Author2Paper2VenueWeaver
from .weaver_p2r2a import Paper2Reference2AuthorWeaver
from .weaver_p_only import PaperOnlyWeaver


def add_weaver_args(parser: argparse.ArgumentParser) -> None:
    """Add Weaver-related command-line arguments."""
    parser.add_argument("--weaver-type", choices=["a2p2v", "p2r2a", "p-only"], default="a2p2v", help="Weaver type (default: a2p2v)")


def create_weaver_from_args(
    args: argparse.Namespace,
    src: DataSrc,
    dst: DataDst,
    cache: FullWeaverCache,
    initializer: WeaverInitializerIface
):
    """Create a Weaver from parsed command-line arguments."""
    match args.weaver_type:
        case "a2p2v":
            return Author2Paper2VenueWeaver(
                src=src,
                dst=dst,
                cache=cache,
                initializer=initializer
            )
        case "p2r2a":
            return Paper2Reference2AuthorWeaver(
                src=src,
                dst=dst,
                cache=cache,
                initializer=initializer
            )
        case "p-only":
            return PaperOnlyWeaver(
                src=src,
                dst=dst,
                cache=cache,
                initializer=initializer
            )
        case _:
            raise ValueError(f"Unknown weaver type: {args.weaver_type}")
