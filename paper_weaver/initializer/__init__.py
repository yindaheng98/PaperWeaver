"""
PaperWeaver Initializer module.

Provides initializer implementations for seeding weavers with initial entities.
"""

from .dblp import (
    DBLPPapersInitializer,
    DBLPAuthorsInitializer,
    DBLPVenuesInitializer,
)
from .argparse import (
    add_initializer_args,
    create_papers_initializer_from_args,
    create_authors_initializer_from_args,
    create_venues_initializer_from_args,
)

__all__ = [
    # DBLP Initializers
    "DBLPPapersInitializer",
    "DBLPAuthorsInitializer",
    "DBLPVenuesInitializer",
    # Argparse
    "add_initializer_args",
    "create_papers_initializer_from_args",
    "create_authors_initializer_from_args",
    "create_venues_initializer_from_args",
]
