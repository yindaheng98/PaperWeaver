"""
Neo4j DataDst module.

Provides DataDst implementation for Neo4j graph database.
"""

from .datadst import Neo4jDataDst
from .utils import (
    find_nodes_by_identifiers,
    merge_nodes_into_one,
    create_node,
    save_node,
    create_relationship
)
from .paper import save_paper, link_paper_citation, link_paper_reference
from .author import save_author, link_author_to_paper
from .venue import save_venue, link_paper_to_venue

__all__ = [
    # Main class
    "Neo4jDataDst",
    # Utils
    "find_nodes_by_identifiers",
    "merge_nodes_into_one",
    "create_node",
    "save_node",
    "create_relationship",
    # Paper
    "save_paper",
    "link_paper_citation",
    "link_paper_reference",
    # Author
    "save_author",
    "link_author_to_paper",
    # Venue
    "save_venue",
    "link_paper_to_venue",
]
