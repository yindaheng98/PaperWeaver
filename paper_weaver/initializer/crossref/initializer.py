"""
CrossRef Initializers for PaperWeaver.

Provides initializer implementations for seeding the weaver with CrossRef entities:
- CrossRefPapersInitializer: Initialize with DOIs
"""

from typing import AsyncIterator

from ...dataclass import Paper
from ...iface_init import PapersWeaverInitializerIface


class CrossRefPapersInitializer(PapersWeaverInitializerIface):
    """
    Initialize weaver with papers from DOIs.

    DOIs are in format like "10.1109/CVPR.2016.90".
    Both bare DOIs and full URLs (https://doi.org/...) are accepted.
    """

    def __init__(self, dois: list[str]):
        """
        Initialize with DOIs.

        Args:
            dois: List of DOIs (e.g., ["10.1109/CVPR.2016.90", "https://doi.org/10.1000/xyz123"])
        """
        self._dois = dois

    async def fetch_papers(self) -> AsyncIterator[Paper]:
        """Yield Paper objects for each DOI."""
        for doi in self._dois:
            url = doi if doi.startswith("https://doi.org/") else f"https://doi.org/{doi}"
            yield Paper(identifiers={url})  # datasrc.crossref.record.paper_to_doi
