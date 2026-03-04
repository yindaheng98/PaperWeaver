"""
CrossRef Initializer module.

Provides initializer implementations for seeding weavers with CrossRef entities.
"""

from .initializer import CrossRefPapersInitializer
from .neo4j import CrossRefNeo4JPapersInitializer

__all__ = [
    "CrossRefPapersInitializer",
    "CrossRefNeo4JPapersInitializer",
]
