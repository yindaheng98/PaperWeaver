"""
PaperWeaver Initializer module.

Provides initializer implementations for seeding weavers with initial entities.
"""

from .dblp import (
    DBLPPapersInitializer,
    DBLPAuthorsInitializer,
    DBLPVenuesInitializer,
    DBLPVenueIndexInitializer,
)
from .argparse import (
    add_initializer_args,
    create_initializer_from_args,
)

__all__ = [
    # DBLP Initializers
    "DBLPPapersInitializer",
    "DBLPAuthorsInitializer",
    "DBLPVenuesInitializer",
    "DBLPVenueIndexInitializer",
    # Argparse
    "add_initializer_args",
    "create_initializer_from_args",
]
