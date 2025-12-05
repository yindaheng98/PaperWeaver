"""
DBLP DataSrc implementation.

Note on author PIDs:
- Author pages (pid/xxx.xml) contain publications WITH author pid attributes
- Paper pages (rec/xxx.xml) do NOT contain author pid attributes
- Venue pages (db/xxx/xxx.xml) contain publications WITH author pid attributes

get_authors_by_paper fetches the venue page to obtain author PIDs when they
are not available from the record page.
"""

from typing import Tuple

from ..cache import CachedAsyncPool, DataSrcCacheIface
from ...dataclass import DataSrc, Paper, Author, Venue

from dblp_webxml_parser import RecordPageParser, PersonPageParser, VenuePageParser
from .record import paper_to_dblp_key, record_to_paper, record_to_info
from .person import author_from_record_author
from .person import author_to_dblp_pid, person_page_to_author, person_page_to_info
from .venue import venue_to_dblp_key, venue_key_from_paper, venue_page_to_venue, venue_page_to_info
from .utils import fetch_xml


class DBLPDataSrc(CachedAsyncPool, DataSrc):
    """
    DataSrc implementation for DBLP API.

    Uses CachedAsyncPool for caching and concurrency control.
    Identifiers use format {info_key}:{value} matching info dict keys:
    - Paper: "dblp:key:{key}", "dblp:url:{url}"
    - Author: "dblp:pid:{pid}", "dblp:name:{name}", "orcid:{orcid}"
    - Venue: "dblp:key:{key}", "title:{title}", "proceedings_title:{title}"
    """

    def __init__(
        self,
        cache: DataSrcCacheIface,
        max_concurrent: int = 10,
        record_cache_ttl: int | None = None,
        person_cache_ttl: int | None = None,
        venue_cache_ttl: int | None = None,
        http_proxy: str | None = None,
        http_timeout: int = 30
    ):
        """
        Initialize DBLPDataSrc.

        Args:
            cache: Cache implementation
            max_concurrent: Maximum concurrent requests
            record_cache_ttl: Cache TTL for record pages in seconds (None = no expiration)
            person_cache_ttl: Cache TTL for person pages in seconds (None = no expiration)
            venue_cache_ttl: Cache TTL for venue pages in seconds (None = no expiration)
            http_proxy: Optional HTTP proxy URL
            http_timeout: HTTP request timeout in seconds
        """
        CachedAsyncPool.__init__(self, cache, max_concurrent)
        self._record_cache_ttl = record_cache_ttl
        self._person_cache_ttl = person_cache_ttl
        self._venue_cache_ttl = venue_cache_ttl
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
            self._record_cache_ttl
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

        # Extract venue key from dblp:url identifier
        venue_key = venue_key_from_paper(updated_paper, info)
        if venue_key is None:
            raise ValueError("No valid DBLP venue key found for paper")

        # Fetch full venue page (cached)
        venue_page = await self._fetch_venue_page_by_key(venue_key)
        return [venue_page_to_venue(venue_page)]

    async def get_authors_by_paper(self, paper: Paper) -> list[Author]:
        """
        Get authors for a paper from DBLP.

        Strategy:
        1. First fetch the record page to get basic paper info
        2. If authors don't have PIDs, fetch venue page and find the paper there
           (venue pages contain author PIDs, unlike record pages)
        """
        record_page = await self._fetch_record_page(paper)

        # Check if authors have PIDs
        authors_have_pids = any(
            record_author.pid for record_author in record_page.authors
        )

        if not authors_have_pids:
            # Try getting authors from venue page (which has PIDs)
            try:
                paper_info = record_to_paper(record_page)
                paper_info.identifiers.update(paper.identifiers)
                info = record_to_info(record_page)

                venue_key = venue_key_from_paper(paper_info, info)
                if venue_key:
                    venue_page = await self._fetch_venue_page_by_key(venue_key)

                    # Find this paper in venue page by key
                    paper_key = record_page.key
                    for publication in venue_page.publications:
                        if publication.key == paper_key:
                            # Found it! Get authors from venue page
                            authors = []
                            for record_author in publication.authors:
                                author = author_from_record_author(record_author)
                                if author is not None:
                                    authors.append(author)
                            if authors:
                                return authors
                            break
            except Exception:
                pass  # Fall through to use record page authors

        # Fallback: use record page authors (may not have PIDs)
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
            self._person_cache_ttl
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

        updated_author = person_page_to_author(person_page)
        updated_author.identifiers.update(author.identifiers)
        info = person_page_to_info(person_page)
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

        url = f"https://dblp.org/{key}.xml"

        venue_page = await self.get_or_fetch(
            url,
            lambda: fetch_xml(url, self._http_proxy, self._http_timeout),
            VenuePageParser,
            self._venue_cache_ttl
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

        updated_venue = venue_page_to_venue(venue_page)
        updated_venue.identifiers.update(venue.identifiers)
        info = venue_page_to_info(venue_page)
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
