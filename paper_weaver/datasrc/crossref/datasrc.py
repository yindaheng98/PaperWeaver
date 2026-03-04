"""
CrossRef DataSrc implementation.

Fetches paper metadata from the CrossRef REST API.
Supports: get_paper_info, get_authors_by_paper, get_references_by_paper.
Does NOT support: get_citations_by_paper, author/venue methods (CrossRef lacks these).

Cache keys use the request URL directly.
"""

import json
from typing import Tuple

from ..cache import CachedAsyncPool, DataSrcCacheIface
from ...dataclass import DataSrc, Paper, Author, Venue
from .record import (
    paper_to_doi,
    work_json_to_paper,
    work_json_to_info,
    work_json_to_authors,
    work_json_to_references,
)
from .utils import fetch_json

CROSSREF_API_BASE = "https://api.crossref.org"


class CrossRefDataSrc(CachedAsyncPool, DataSrc):
    """
    DataSrc implementation for CrossRef REST API.

    Uses CachedAsyncPool for caching and concurrency control.
    Cache keys are the API path after the base URL, e.g. "works/doi/10.1109/...".

    Identifiers:
    - Paper: "https://doi.org/{doi}", "title:{title}", "title_hash:{hash} year:{year}"
    - Author: "orcid:{orcid}", "crossref:name:{name}"
    """

    def __init__(
        self,
        cache: DataSrcCacheIface,
        max_concurrent: int = 10,
        cache_ttl: int | None = None,
        mailto: str | None = None,
    ):
        """
        Initialize CrossRefDataSrc.

        Proxy is automatically read from environment variables (HTTP_PROXY, HTTPS_PROXY)
        via aiohttp's trust_env=True setting.

        Args:
            cache: Cache implementation
            max_concurrent: Maximum concurrent requests
            cache_ttl: Cache TTL in seconds (None = use default 7 days)
            mailto: Contact email for CrossRef polite pool (recommended)
        """
        CachedAsyncPool.__init__(self, cache, max_concurrent)
        self._cache_ttl = cache_ttl
        self._mailto = mailto

    # ==================== Paper Methods ====================

    async def _fetch_work_json_by_doi(self, doi: str) -> dict | None:
        """Fetch a single work by DOI with caching."""
        url = f"{CROSSREF_API_BASE}/works/doi/{doi}"

        work_json = await self.get_or_fetch(
            url,
            lambda: fetch_json(
                url=url,
                mailto=self._mailto
            ),
            lambda text: json.loads(text).get("message"),
            self._cache_ttl
        )
        if work_json is None:
            raise ValueError(f"Failed to fetch work JSON from CrossRef: {doi}")

        return work_json

    async def _fetch_work_json(self, paper: Paper) -> dict | None:
        """Fetch a CrossRef work by DOI identifier from paper."""

        work_doi = paper_to_doi(paper)

        if work_doi is None:
            raise ValueError("No valid DOI identifier found for paper")

        return await self._fetch_work_json_by_doi(work_doi)

    async def get_paper_info(self, paper: Paper) -> Tuple[Paper, dict]:
        """Get paper information from CrossRef."""
        work_json = await self._fetch_work_json(paper)

        updated_paper = work_json_to_paper(work_json)
        updated_paper.identifiers.update(paper.identifiers)
        info = work_json_to_info(work_json)
        return updated_paper, info

    async def get_authors_by_paper(self, paper: Paper) -> list[Author]:
        """Get authors for a paper from CrossRef."""
        work_json = await self._fetch_work_json(paper)
        return work_json_to_authors(work_json)

    async def get_references_by_paper(self, paper: Paper) -> list[Paper]:
        """Get references (cited papers) from CrossRef."""
        work_json = await self._fetch_work_json(paper)
        return work_json_to_references(work_json)

    # ==================== Unsupported Methods ====================

    async def get_venues_by_paper(self, paper: Paper) -> list[Venue]:
        raise NotImplementedError(
            "CrossRef API does not provide a dedicated venue endpoint"
        )

    async def get_citations_by_paper(self, paper: Paper) -> list[Paper]:
        raise NotImplementedError(
            "CrossRef API does not provide citation information"
        )

    async def get_author_info(self, author: Author) -> Tuple[Author, dict]:
        raise NotImplementedError(
            "CrossRef API does not provide author profile lookup"
        )

    async def get_papers_by_author(self, author: Author) -> list[Paper]:
        raise NotImplementedError(
            "CrossRef API does not provide author-to-papers lookup"
        )

    async def get_venue_info(self, venue: Venue) -> Tuple[Venue, dict]:
        raise NotImplementedError(
            "CrossRef API does not provide venue profile lookup"
        )

    async def get_papers_by_venue(self, venue: Venue) -> list[Paper]:
        raise NotImplementedError(
            "CrossRef API does not provide venue-to-papers lookup"
        )
