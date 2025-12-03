"""
DataDst module.

Provides data destination implementations for storing Paper, Author, and Venue data.
"""

from .neo4j import Neo4jDataDst

from .argparse import (  # noqa: F401
    add_datadst_args,
    create_datadst_from_args,
)

__all__ = [
    "Neo4jDataDst",
    "add_datadst_args",
    "create_datadst_from_args",
]
