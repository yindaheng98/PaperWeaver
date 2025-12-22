"""
DBLP Initializers for PaperWeaver.

Provides initializer implementations for seeding the weaver with DBLP entities:
- DBLPPapersInitializer: Initialize with DBLP record keys
- DBLPAuthorsInitializer: Initialize with DBLP person IDs (pids)
- DBLPVenuesInitializer: Initialize with DBLP venue keys
- DBLPVenueIndexInitializer: Initialize with DBLP venue index keys (expands to individual venues)
"""

import re
from typing import AsyncIterator
from xml.etree import ElementTree

from ...datasrc.dblp import fetch_xml
from ...dataclass import Venue
from ...iface_init import VenuesWeaverInitializerIface

DBLP_BASE_URL = "https://dblp.org"


async def _get_venue_keys(dblp_key: str) -> list[str]:
    """
    Given a DBLP key (e.g., 'db/conf/cvpr'), fetch the index.xml and return all venue keys.

    Args:
        dblp_key: A DBLP venue directory key like 'db/conf/cvpr' or 'db/journals/tpds'

    Returns:
        List of venue keys like ['db/conf/cvpr/cvpr2025', 'db/conf/cvpr/cvpr2024', ...]
    """
    url = f"{DBLP_BASE_URL}/{dblp_key}/index.xml"
    data = ElementTree.fromstring(await fetch_xml(url))
    urls = [re.sub(r"\.html$", "", li.attrib["href"]) for li in data.findall('./ul/li/ref')]
    for proceedings in data.findall('./dblpcites/r/proceedings'):
        if proceedings.find('./url') is None:
            continue  # skip those not in dblp
        urls.append(re.sub(r"\.html$", "", proceedings.find('./url').text))
    return urls


class DBLPVenueIndexInitializer(VenuesWeaverInitializerIface):
    """
    Initialize weaver with venues from DBLP venue index keys.

    Venue index keys are in format like "db/conf/cvpr" or "db/journals/tpds".
    This initializer expands each index key to all individual venue keys within that index.
    """

    def __init__(self, venue_index_keys: list[str]):
        """
        Initialize with DBLP venue index keys.

        Args:
            venue_index_keys: List of DBLP venue index keys (e.g., ["db/conf/cvpr", "db/journals/tpds"])
        """
        self._venue_index_keys = venue_index_keys

    async def fetch_venues(self) -> AsyncIterator[Venue]:
        """Yield Venue objects for each venue within the index keys."""
        for index_key in self._venue_index_keys:
            venue_keys = await _get_venue_keys(index_key)
            for key in venue_keys:
                yield Venue(identifiers={f"dblp:key:{key}"})  # datasrc.dblp.venue.venue_to_dblp_key
