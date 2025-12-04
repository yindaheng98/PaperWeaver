"""
DBLP DataSrc module.

Provides data source implementation for DBLP API.
"""

from .initializer import DBLPPapersInitializer, DBLPAuthorsInitializer, DBLPVenuesInitializer
from .argparse import (
    add_dblp_papers_initializer_args,
    add_dblp_authors_initializer_args,
    add_dblp_venues_initializer_args,
    create_dblp_papers_initializer_from_args,
    create_dblp_authors_initializer_from_args,
    create_dblp_venues_initializer_from_args,
)

__all__ = [
    # Initializers
    "DBLPPapersInitializer",
    "DBLPAuthorsInitializer",
    "DBLPVenuesInitializer",
    # Argparse
    "add_dblp_papers_initializer_args",
    "add_dblp_authors_initializer_args",
    "add_dblp_venues_initializer_args",
    "create_dblp_papers_initializer_from_args",
    "create_dblp_authors_initializer_from_args",
    "create_dblp_venues_initializer_from_args",
]
