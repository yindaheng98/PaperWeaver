"""
DataDst module.

Provides data destination implementations for storing Paper, Author, and Venue data.
"""

from .neo4j import Neo4jDataDst

__all__ = [
    "Neo4jDataDst",
]

