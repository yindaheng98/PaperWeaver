"""
PaperWeaver command-line tool.

Usage:
    python -m paper_weaver [options]
"""

import argparse
import asyncio
import logging

from .argparse import add_weaver_args, create_weaver_from_args
from .cache.argparse import add_cache_args, create_cache_from_args
from .datasrc.argparse import add_datasrc_args, create_datasrc_from_args
from .datadst.argparse import add_datadst_args, create_datadst_from_args
from .initializer.argparse import add_initializer_args, create_initializer_from_args


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="paper_weaver",
        description="PaperWeaver: Weave academic paper data from various sources"
    )

    # Weaver args
    add_weaver_args(parser)
    add_cache_args(parser)
    add_datasrc_args(parser)
    add_datadst_args(parser)
    add_initializer_args(parser)

    # Run mode
    parser.add_argument("--max-iterations", "-n", type=int, default=0, help="Max BFS iterations (0 = until no new data)")
    parser.add_argument("-v", "--verbose", action="count", default=0, help="Increase verbosity (-v: INFO, -vv: DEBUG)")

    return parser


def setup_logging(verbosity: int) -> None:
    if verbosity >= 2:
        level = logging.DEBUG
    elif verbosity >= 1:
        level = logging.INFO
    else:
        level = logging.WARNING
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


async def run(args: argparse.Namespace) -> None:
    cache = create_cache_from_args(args)
    datasrc = create_datasrc_from_args(args)
    datadst, driver = create_datadst_from_args(args)
    initializer = create_initializer_from_args(args)
    weaver = create_weaver_from_args(args, datasrc, datadst, cache, initializer)

    try:
        max_iter = args.max_iterations if args.max_iterations > 0 else 10000
        total = await weaver.bfs(max_iterations=max_iter)
        print(f"[Done] Total {total} items processed.")
    finally:
        await driver.close()


def main():
    parser = create_parser()
    args = parser.parse_args()
    setup_logging(args.verbose)
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
