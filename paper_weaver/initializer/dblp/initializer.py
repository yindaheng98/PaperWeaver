"""
DBLP Initializers for PaperWeaver.

Provides initializer implementations for seeding the weaver with DBLP entities:
- DBLPPapersInitializer: Initialize with DBLP record keys
- DBLPAuthorsInitializer: Initialize with DBLP person IDs (pids)
- DBLPVenuesInitializer: Initialize with DBLP venue keys
"""

from typing import AsyncIterator

from ...dataclass import Paper, Author, Venue
from ...iface_init import (
    PapersWeaverInitializerIface,
    AuthorsWeaverInitializerIface,
    VenuesWeaverInitializerIface,
)


class DBLPPapersInitializer(PapersWeaverInitializerIface):
    """
    Initialize weaver with papers from DBLP record keys.

    Record keys are in format like "conf/cvpr/HeZRS16" or "journals/pami/HeZRS16".
    """

    def __init__(self, record_keys: list[str]):
        """
        Initialize with DBLP record keys.

        Args:
            record_keys: List of DBLP record keys (e.g., ["conf/cvpr/HeZRS16", "journals/pami/HeZRS16"])
        """
        self._record_keys = record_keys

    async def fetch_papers(self) -> AsyncIterator[Paper]:
        """Yield Paper objects for each record key."""
        for key in self._record_keys:
            yield Paper(identifiers={f"dblp:key:{key}"})  # datasrc.dblp.record.paper_to_dblp_key


class DBLPAuthorsInitializer(AuthorsWeaverInitializerIface):
    """
    Initialize weaver with authors from DBLP person IDs.

    Person IDs (pids) are in format like "h/KaimingHe" or "74/1552".
    """

    def __init__(self, pids: list[str]):
        """
        Initialize with DBLP person IDs.

        Args:
            pids: List of DBLP person IDs (e.g., ["h/KaimingHe", "74/1552"])
        """
        self._pids = pids

    async def fetch_authors(self) -> AsyncIterator[Author]:
        """Yield Author objects for each person ID."""
        for pid in self._pids:
            yield Author(identifiers={f"dblp:pid:{pid}"})  # datasrc.dblp.person.author_to_dblp_pid


class DBLPVenuesInitializer(VenuesWeaverInitializerIface):
    """
    Initialize weaver with venues from DBLP venue keys.

    Venue keys are in format like "db/conf/cvpr/cvpr2016" or "db/journals/pami/pami45".
    """

    def __init__(self, venue_keys: list[str]):
        """
        Initialize with DBLP venue keys.

        Args:
            venue_keys: List of DBLP venue keys (e.g., ["db/conf/cvpr/cvpr2016", "db/journals/pami/pami45"])
        """
        self._venue_keys = venue_keys

    async def fetch_venues(self) -> AsyncIterator[Venue]:
        """Yield Venue objects for each venue key."""
        for key in self._venue_keys:
            yield Venue(identifiers={f"dblp:key:{key}"})  # datasrc.dblp.venue.venue_to_dblp_key
