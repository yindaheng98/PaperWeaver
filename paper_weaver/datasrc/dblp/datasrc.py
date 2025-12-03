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
from .venue import venue_to_dblp_key, venue_key_from_paper, venue_to_venue, venue_to_info
from .utils import fetch_xml


class DBLPDataSrc(CachedAsyncPool, DataSrc):
    """
    DataSrc implementation for DBLP API.

    Uses CachedAsyncPool for caching and concurrency control.
    Identifiers use format {type}:{info_key}:{value} matching info dict keys:
    - Paper: "paper:dblp:key:{key}", "paper:dblp:url:{url}"
    - Author: "author:dblp:pid:{pid}", "author:name:{name}", "author:orcid:{orcid}"
    - Venue: "venue:dblp:key:{key}", "venue:title:{title}", "venue:proceedings_title:{title}"
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

    # ==================== Paper Methods ====================

    async def _fetch_record_page_by_key(self, key: str) -> RecordPageParser | None:
        """Fetch and parse record page by paper key."""

        url = f"https://dblp.org/rec/{key}.xml"

        record_page = await self.get_or_fetch(
            url,
            lambda: fetch_xml(url, self._http_proxy, self._http_timeout),
            RecordPageParser,
            self._cache_ttl
        )
        if record_page is None:
            raise ValueError("Failed to fetch record page for paper")

        return record_page

    async def _fetch_record_page(self, paper: Paper) -> RecordPageParser | None:
        """Fetch and parse record page by paper key."""

        paper_key = paper_to_dblp_key(paper)

        if paper_key is None:
            raise ValueError("No valid DBLP identifier found for paper")

        return await self._fetch_record_page_by_key(paper_key)

    async def get_paper_info(self, paper: Paper) -> Tuple[Paper, dict]:
        """Get paper information from DBLP."""
        record_page = await self._fetch_record_page(paper)

        updated_paper = record_to_paper(record_page)
        updated_paper.identifiers.update(paper.identifiers)
        info = record_to_info(record_page)
        return updated_paper, info

    async def get_venues_by_paper(self, paper: Paper) -> list[Venue]:
        """Get venues for a paper from DBLP."""
        updated_paper, info = await self.get_paper_info(paper)

        # Extract venue key from dblp:url or paper:dblp:url: identifier
        venue_key = venue_key_from_paper(updated_paper, info)
        if venue_key is None:
            raise ValueError("No valid DBLP venue key found for paper")

        # Fetch full venue page (cached)
        venue = Venue(identifiers={f"venue:dblp:key:{venue_key}"})
        updated_venue, _ = await self.get_venue_info(venue)
        return [updated_venue]

    async def get_authors_by_paper(self, paper: Paper) -> list[Author]:
        """
        Get authors for a paper from DBLP.

        Note: DBLP paper pages (rec/xxx.xml) do NOT contain author pid attributes.
        Authors returned will only have dblp-author-name:{name} identifiers
        unless the record was fetched from an author page (which includes pids).
        """
        record_page = await self._fetch_record_page(paper)

        authors = []
        for record_author in record_page.authors:
            author = author_from_record_author(record_author)
            if author is not None:
                authors.append(author)
        return authors

    async def get_references_by_paper(self, paper: Paper) -> list[Paper]:
        """
        Get references (papers cited by this paper) from DBLP.

        Note: DBLP API does not provide reference information.
        This method always raises NotImplementedError.
        """
        raise NotImplementedError(
            "DBLP API does not provide reference information"
        )

    async def get_citations_by_paper(self, paper: Paper) -> list[Paper]:
        """
        Get citations (papers that cite this paper) from DBLP.

        Note: DBLP API does not provide citation information.
        This method always raises NotImplementedError.
        """
        raise NotImplementedError(
            "DBLP API does not provide citation information"
        )

    # ==================== Author Methods ====================

    async def _fetch_person_page_by_id(self, pid: str) -> PersonPageParser | None:
        """Fetch and parse person page by author."""

        url = f"https://dblp.org/pid/{pid}.xml"

        person_page = await self.get_or_fetch(
            url,
            lambda: fetch_xml(url, self._http_proxy, self._http_timeout),
            PersonPageParser,
            self._cache_ttl
        )
        if person_page is None:
            raise ValueError("Failed to fetch person page for author")

        return person_page

    async def _fetch_person_page(self, author: Author) -> PersonPageParser | None:
        """Fetch and parse person page by author."""

        author_pid = author_to_dblp_pid(author)

        if author_pid is None:
            raise ValueError("No valid DBLP identifier found for author")

        return await self._fetch_person_page_by_id(author_pid)

    async def get_author_info(self, author: Author) -> Tuple[Author, dict]:
        """Get author information from DBLP."""
        person_page = await self._fetch_person_page(author)

        updated_author = person_to_author(person_page)
        updated_author.identifiers.update(author.identifiers)
        info = person_to_info(person_page)
        return updated_author, info

    async def get_papers_by_author(self, author: Author) -> list[Paper]:
        """Get papers by an author from DBLP."""
        person_page = await self._fetch_person_page(author)

        papers = []
        for publication in person_page.publications:
            paper = record_to_paper(publication)
            if paper.identifiers:
                papers.append(paper)
        return papers

    # ==================== Venue Methods ====================

    async def _fetch_venue_page_by_key(self, key: str) -> VenuePageParser | None:
        """Fetch and parse venue page by venue key."""

        url = f"https://dblp.org/db/{key}.xml"

        venue_page = await self.get_or_fetch(
            url,
            lambda: fetch_xml(url, self._http_proxy, self._http_timeout),
            VenuePageParser,
            self._cache_ttl
        )
        if venue_page is None:
            raise ValueError("Failed to fetch venue page for venue")

        return venue_page

    async def _fetch_venue_page(self, venue: Venue) -> VenuePageParser | None:
        """Fetch and parse venue page by venue."""

        venue_key = venue_to_dblp_key(venue)

        if venue_key is None:
            raise ValueError("No valid DBLP identifier found for venue")

        return await self._fetch_venue_page_by_key(venue_key)

    async def get_venue_info(self, venue: Venue) -> Tuple[Venue, dict]:
        """Get venue information from DBLP."""
        venue_page = await self._fetch_venue_page(venue)

        updated_venue = venue_to_venue(venue_page)
        updated_venue.identifiers.update(venue.identifiers)
        info = venue_to_info(venue_page)
        return updated_venue, info

    async def get_papers_by_venue(self, venue: Venue) -> list[Paper]:
        """Get papers from a venue from DBLP."""
        venue_page = await self._fetch_venue_page(venue)

        papers = []
        for publication in venue_page.publications:
            paper = record_to_paper(publication)
            if paper.identifiers:
                papers.append(paper)
        return papers
