"""
DBLP DataSrc implementation.

Important DBLP API limitation:
- Author pages (pid/xxx.xml) contain publications WITH author pid attributes
- Paper pages (rec/xxx.xml) do NOT contain author pid attributes

Therefore, get_authors_by_paper only works from cache populated by
get_author_info or get_papers_by_author.
"""

from typing import Tuple

from ..cache import CachedAsyncPool, DataSrcCacheIface
from ...dataclass import DataSrc, Paper, Author, Venue

from .parser import RecordPageParser, PersonPageParser, VenuePageParser
from .record import paper_to_dblp_key, author_from_record_author, record_to_paper, record_to_info
from .person import author_to_dblp_pid, person_to_author, person_to_info
from .venue import venue_to_dblp_key, venue_from_paper_info, venue_to_venue, venue_to_info
from .utils import fetch_xml


class DBLPDataSrc(CachedAsyncPool, DataSrc):
    """
    DataSrc implementation for DBLP API.

    Uses CachedAsyncPool for caching and concurrency control.
    Identifiers use prefixes:
    - Paper: "dblp:{key}", "dblp-url:{url}"
    - Author: "dblp-author:{pid}", "dblp-author-name:{name}"
    - Venue: "dblp-venue:{key}", "dblp-venue-title:{title}"
    """

    # Default cache TTL in seconds (7 days)
    DEFAULT_CACHE_TTL = 7 * 24 * 60 * 60

    def __init__(
        self,
        cache: DataSrcCacheIface,
        max_concurrent: int = 10,
        cache_ttl: int | None = None,
        http_proxy: str | None = None,
        http_timeout: int = 30
    ):
        """
        Initialize DBLPDataSrc.

        Args:
            cache: Cache implementation
            max_concurrent: Maximum concurrent requests
            cache_ttl: Cache time-to-live in seconds (None = no expiration)
            http_proxy: Optional HTTP proxy URL
            http_timeout: HTTP request timeout in seconds
        """
        CachedAsyncPool.__init__(self, cache, max_concurrent)
        self._cache_ttl = cache_ttl if cache_ttl is not None else self.DEFAULT_CACHE_TTL
        self._http_proxy = http_proxy
        self._http_timeout = http_timeout

    # ==================== Venue Methods ====================

    async def _fetch_venue(self, venue: Venue) -> VenuePageParser | None:
        """Fetch and parse venue page by venue."""
        venue_key = venue_to_dblp_key(venue)
        if venue_key is None:
            raise ValueError("No valid DBLP identifier found for venue")

        url = f"https://dblp.org/db/{venue_key}.xml"

        venue_page = await self.get_or_fetch(
            url,
            lambda: fetch_xml(url, self._http_proxy, self._http_timeout),
            VenuePageParser,
            self._cache_ttl
        )

        return venue_page

    async def get_venue_info(self, venue: Venue) -> Tuple[Venue, dict]:
        """Get venue information from DBLP."""
        venue_page = await self._fetch_venue(venue)

        updated_venue = venue_to_venue(venue_page)
        updated_venue.identifiers.update(venue.identifiers)
        info = venue_to_info(venue_page)
        return updated_venue, info

    async def get_papers_by_venue(self, venue: Venue) -> list[Paper]:
        """Get papers from a venue from DBLP."""
        venue_page = await self._fetch_venue(venue)

        papers = []
        for publication in venue_page.publications:
            paper = record_to_paper(publication)
            if paper.identifiers:
                papers.append(paper)
        return papers
