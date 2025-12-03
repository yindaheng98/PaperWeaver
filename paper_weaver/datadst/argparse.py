"""
Command-line argument configuration for PaperWeaver DataDst.

Supports:
- Neo4j: Neo4j graph database
"""

import argparse

from ..dataclass import DataDst
from .neo4j import Neo4jDataDst


def add_datadst_args(parser: argparse.ArgumentParser) -> None:
    """Add DataDst-related command-line arguments."""
    parser.add_argument("--datadst-type", choices=["neo4j"], default="neo4j", help="DataDst type (default: neo4j)")
    parser.add_argument("--datadst-neo4j-uri", default="bolt://localhost:7687", help="Neo4j connection URI (default: bolt://localhost:7687)")
    parser.add_argument("--datadst-neo4j-user", default="neo4j", help="Neo4j username (default: neo4j)")
    parser.add_argument("--datadst-neo4j-password", default="neo4j", help="Neo4j password (default: neo4j)")
    parser.add_argument("--datadst-neo4j-database", default="neo4j", help="Neo4j database name (default: neo4j)")


def create_datadst_from_args(args: argparse.Namespace) -> tuple[DataDst, object]:
    """
    Create a DataDst from parsed command-line arguments.

    Returns:
        (DataDst instance, driver to close later)
    """
    match args.datadst_type:
        case "neo4j":
            from neo4j import AsyncGraphDatabase

            driver = AsyncGraphDatabase.driver(
                args.datadst_neo4j_uri,
                auth=(args.datadst_neo4j_user, args.datadst_neo4j_password)
            )
            session = driver.session(database=args.datadst_neo4j_database)
            return Neo4jDataDst(session), driver
        case _:
            raise ValueError(f"Unknown datadst type: {args.datadst_type}")
