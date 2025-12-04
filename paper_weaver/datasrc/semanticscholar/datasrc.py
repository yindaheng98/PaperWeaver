"""
Semantic Scholar DataSrc implementation.

This module provides a DataSrc implementation that fetches data from
the Semantic Scholar API with caching and concurrency control provided
by CachedAsyncPool.
"""

import json
import os
from typing import Tuple

import aiohttp

from ..cache import CachedAsyncPool, DataSrcCacheIface
from ...dataclass import DataSrc, Paper, Author, Venue


class SemanticScholarDataSrc(CachedAsyncPool, DataSrc):
    """
    DataSrc implementation for Semantic Scholar API.

    Uses CachedAsyncPool for caching and concurrency control.
    Identifiers use prefixes:
    - Paper: "ss:<paperId>", "doi:<doi>"
    - Author: "ss-author:<authorId>"
    """

    # API field constants (matching crawlers/ss.py)
    FIELDS_PAPER = "title,abstract,year,publicationDate,authors.externalIds,authors.name,authors.affiliations,authors.homepage,externalIds,publicationTypes,journal"
    FIELDS_AUTHORS = "externalIds,name,affiliations,homepage"
    FIELDS_REFERENCES = "title,abstract,year,authors,externalIds,publicationTypes,journal"
    FIELDS_CITATIONS = "title,abstract,year,authors,externalIds,publicationTypes,journal"
    FIELDS_AUTHOR_PAPERS = "paperId,title"

    # Default cache TTL in seconds (7 days)
    DEFAULT_CACHE_TTL = 7 * 24 * 60 * 60

    def __init__(
        self,
        cache: DataSrcCacheIface,
        max_concurrent: int = 10,
        cache_ttl: int | None = None,
        http_headers: dict | None = None,
        http_proxy: str | None = None,
        http_timeout: int = 30
    ):
        """
        Initialize SemanticScholarDataSrc.

        Args:
            cache: Cache implementation
            max_concurrent: Maximum concurrent requests
            cache_ttl: Cache time-to-live in seconds (None = no expiration)
            http_headers: Optional HTTP headers for requests
            http_proxy: Optional HTTP proxy URL
            http_timeout: HTTP request timeout in seconds
        """
        CachedAsyncPool.__init__(self, cache, max_concurrent)
        self._cache_ttl = cache_ttl if cache_ttl is not None else self.DEFAULT_CACHE_TTL
        self._http_headers = http_headers or {}
        self._http_proxy = http_proxy or os.getenv("HTTP_PROXY")
        self._http_timeout = http_timeout

    # ==================== Helper Methods ====================

    def _extract_ss_paper_id(self, paper: Paper) -> str | None:
        """Extract Semantic Scholar paperId from paper identifiers."""
        for ident in paper.identifiers:
            if ident.startswith("ss:"):
                return ident[3:]  # Remove "ss:" prefix
            if ident.startswith("doi:"):
                return ident  # DOI can be used directly as paper ID
        return None

    def _extract_ss_author_id(self, author: Author) -> str | None:
        """Extract Semantic Scholar authorId from author identifiers."""
        for ident in author.identifiers:
            if ident.startswith("ss-author:"):
                return ident[10:]  # Remove "ss-author:" prefix
        return None

    def _paper_from_ss_data(self, data: dict) -> Paper:
        """Create Paper from Semantic Scholar API data."""
        identifiers = set()
        if 'paperId' in data and data['paperId']:
            identifiers.add(f"ss:{data['paperId']}")
        if 'externalIds' in data and data['externalIds']:
            if 'DOI' in data['externalIds'] and data['externalIds']['DOI']:
                identifiers.add(f"doi:{data['externalIds']['DOI']}")
            if 'DBLP' in data['externalIds'] and data['externalIds']['DBLP']:
                identifiers.add(f"dblp:{data['externalIds']['DBLP']}")
            if 'ArXiv' in data['externalIds'] and data['externalIds']['ArXiv']:
                identifiers.add(f"arxiv:{data['externalIds']['ArXiv']}")
        return Paper(identifiers=identifiers)

    def _author_from_ss_data(self, data: dict) -> Author:
        """Create Author from Semantic Scholar API data."""
        identifiers = set()
        if 'authorId' in data and data['authorId']:
            identifiers.add(f"ss-author:{data['authorId']}")
        if 'externalIds' in data and data['externalIds']:
            if 'DBLP' in data['externalIds'] and data['externalIds']['DBLP']:
                for dblp_name in data['externalIds']['DBLP']:
                    identifiers.add(f"dblp-author:{dblp_name}")
            if 'ORCID' in data['externalIds'] and data['externalIds']['ORCID']:
                identifiers.add(f"orcid:{data['externalIds']['ORCID']}")
        return Author(identifiers=identifiers)

    def _venue_from_ss_data(self, data: dict) -> Venue:
        """Create Venue from Semantic Scholar API data."""
        identifiers = set()
        if 'journal' in data and data['journal']:
            journal = data['journal']
            if 'name' in journal and journal['name']:
                identifiers.add(f"ss-venue:{journal['name']}")
        if 'venue' in data and data['venue']:
            identifiers.add(f"ss-venue:{data['venue']}")
        return Venue(identifiers=identifiers)

    async def _fetch_json(self, url: str) -> str | None:
        """Fetch JSON data from URL."""
        try:
            connector = aiohttp.TCPConnector(ssl=False)
            async with aiohttp.ClientSession(
                connector=connector,
                headers=self._http_headers
            ) as session:
                async with session.get(
                    url,
                    proxy=self._http_proxy,
                    timeout=aiohttp.ClientTimeout(total=self._http_timeout)
                ) as response:
                    if response.status == 200:
                        return await response.text()
        except Exception:
            pass
        return None

    # ==================== Paper Methods ====================

    async def get_paper_info(self, paper: Paper) -> Tuple[Paper, dict]:
        """Get paper information from Semantic Scholar."""
        paper_id = self._extract_ss_paper_id(paper)
        if paper_id is None:
            raise ValueError("No valid Semantic Scholar identifier found for paper")

        paper_id_lower = paper_id.lower()
        url = f"https://api.semanticscholar.org/graph/v1/paper/{paper_id}?fields={self.FIELDS_PAPER}"
        cache_key = f"ss:paper:{paper_id_lower}"

        def parser(text: str) -> dict | None:
            try:
                data = json.loads(text)
                if 'paperId' in data and data['paperId']:
                    return data
            except json.JSONDecodeError:
                pass
            return None

        data = await self.get_or_fetch(
            cache_key,
            lambda: self._fetch_json(url),
            parser,
            self._cache_ttl
        )

        if data is None:
            raise ValueError(f"Failed to fetch paper: {paper_id}")

        updated_paper = self._paper_from_ss_data(data)
        updated_paper.identifiers.update(paper.identifiers)
        return updated_paper, data

    async def get_authors_by_paper(self, paper: Paper) -> list[Author]:
        """Get authors for a paper from Semantic Scholar."""
        paper_id = self._extract_ss_paper_id(paper)
        if paper_id is None:
            raise ValueError("No valid Semantic Scholar identifier found for paper")

        paper_id_lower = paper_id.lower()
        url = f"https://api.semanticscholar.org/graph/v1/paper/{paper_id}/authors?fields={self.FIELDS_AUTHORS}"
        cache_key = f"ss:paper-authors:{paper_id_lower}"

        def parser(text: str) -> list | None:
            try:
                data = json.loads(text)
                if 'data' in data:
                    return data['data']
            except json.JSONDecodeError:
                pass
            return None

        data = await self.get_or_fetch(
            cache_key,
            lambda: self._fetch_json(url),
            parser,
            self._cache_ttl
        )

        if data is None:
            raise ValueError(f"Failed to fetch authors for paper: {paper_id}")

        authors = []
        for author_data in data:
            if 'authorId' in author_data and author_data['authorId']:
                authors.append(self._author_from_ss_data(author_data))
        return authors

    async def get_venues_by_paper(self, paper: Paper) -> list[Venue]:
        """Get venues for a paper from Semantic Scholar."""
        # Semantic Scholar returns venue info in paper data
        # We need to fetch paper info first
        paper_id = self._extract_ss_paper_id(paper)
        if paper_id is None:
            raise ValueError("No valid Semantic Scholar identifier found for paper")

        _, data = await self.get_paper_info(paper)

        venues = []
        venue = self._venue_from_ss_data(data)
        if venue.identifiers:
            venues.append(venue)
        return venues

    async def get_references_by_paper(self, paper: Paper) -> list[Paper]:
        """Get references (papers cited by this paper) from Semantic Scholar."""
        paper_id = self._extract_ss_paper_id(paper)
        if paper_id is None:
            raise ValueError("No valid Semantic Scholar identifier found for paper")

        paper_id_lower = paper_id.lower()
        url = f"https://api.semanticscholar.org/graph/v1/paper/{paper_id}/references?fields={self.FIELDS_REFERENCES}"
        cache_key = f"ss:paper-references:{paper_id_lower}"

        def parser(text: str) -> list | None:
            try:
                data = json.loads(text)
                if 'data' in data:
                    return data['data']
            except json.JSONDecodeError:
                pass
            return None

        data = await self.get_or_fetch(
            cache_key,
            lambda: self._fetch_json(url),
            parser,
            self._cache_ttl
        )

        if data is None:
            raise ValueError(f"Failed to fetch references for paper: {paper_id}")

        papers = []
        for ref_data in data:
            if 'citedPaper' in ref_data and ref_data['citedPaper']:
                cited = ref_data['citedPaper']
                if 'paperId' in cited and cited['paperId']:
                    papers.append(self._paper_from_ss_data(cited))
        return papers

    async def get_citations_by_paper(self, paper: Paper) -> list[Paper]:
        """Get citations (papers that cite this paper) from Semantic Scholar."""
        paper_id = self._extract_ss_paper_id(paper)
        if paper_id is None:
            raise ValueError("No valid Semantic Scholar identifier found for paper")

        paper_id_lower = paper_id.lower()
        url = f"https://api.semanticscholar.org/graph/v1/paper/{paper_id}/citations?fields={self.FIELDS_CITATIONS}"
        cache_key = f"ss:paper-citations:{paper_id_lower}"

        def parser(text: str) -> list | None:
            try:
                data = json.loads(text)
                if 'data' in data:
                    return data['data']
            except json.JSONDecodeError:
                pass
            return None

        data = await self.get_or_fetch(
            cache_key,
            lambda: self._fetch_json(url),
            parser,
            self._cache_ttl
        )

        if data is None:
            raise ValueError(f"Failed to fetch citations for paper: {paper_id}")

        papers = []
        for cite_data in data:
            if 'citingPaper' in cite_data and cite_data['citingPaper']:
                citing = cite_data['citingPaper']
                if 'paperId' in citing and citing['paperId']:
                    papers.append(self._paper_from_ss_data(citing))
        return papers

    # ==================== Author Methods ====================

    async def get_author_info(self, author: Author) -> Tuple[Author, dict]:
        """Get author information from Semantic Scholar."""
        author_id = self._extract_ss_author_id(author)
        if author_id is None:
            raise ValueError("No valid Semantic Scholar identifier found for author")

        author_id_lower = author_id.lower()
        url = f"https://api.semanticscholar.org/graph/v1/author/{author_id}?fields={self.FIELDS_AUTHORS}"
        cache_key = f"ss:author:{author_id_lower}"

        def parser(text: str) -> dict | None:
            try:
                data = json.loads(text)
                if 'authorId' in data and data['authorId']:
                    return data
            except json.JSONDecodeError:
                pass
            return None

        data = await self.get_or_fetch(
            cache_key,
            lambda: self._fetch_json(url),
            parser,
            self._cache_ttl
        )

        if data is None:
            raise ValueError(f"Failed to fetch author: {author_id}")

        updated_author = self._author_from_ss_data(data)
        updated_author.identifiers.update(author.identifiers)
        return updated_author, data

    async def get_papers_by_author(self, author: Author) -> list[Paper]:
        """Get papers by an author from Semantic Scholar."""
        author_id = self._extract_ss_author_id(author)
        if author_id is None:
            raise ValueError("No valid Semantic Scholar identifier found for author")

        author_id_lower = author_id.lower()
        url = f"https://api.semanticscholar.org/graph/v1/author/{author_id}/papers?fields={self.FIELDS_AUTHOR_PAPERS}&limit=100"
        cache_key = f"ss:author-papers:{author_id_lower}"

        def parser(text: str) -> list | None:
            try:
                data = json.loads(text)
                if 'data' in data:
                    return data['data']
            except json.JSONDecodeError:
                pass
            return None

        data = await self.get_or_fetch(
            cache_key,
            lambda: self._fetch_json(url),
            parser,
            self._cache_ttl
        )

        if data is None:
            raise ValueError(f"Failed to fetch papers for author: {author_id}")

        papers = []
        for paper_data in data:
            if 'paperId' in paper_data and paper_data['paperId']:
                papers.append(Paper(identifiers={f"ss:{paper_data['paperId']}"}))
        return papers

    # ==================== Venue Methods ====================

    async def get_venue_info(self, venue: Venue) -> Tuple[Venue, dict]:
        """
        Get venue information from Semantic Scholar.

        Note: Semantic Scholar API doesn't have a direct venue endpoint.
        This returns the venue with its identifiers and an empty info dict.
        """
        # Semantic Scholar doesn't have a dedicated venue API endpoint
        # Return the venue as-is with empty info
        return venue, {}

    async def get_papers_by_venue(self, venue: Venue) -> list[Paper]:
        """
        Get papers from a venue from Semantic Scholar.

        Note: Semantic Scholar API doesn't support direct venue-to-papers lookup.
        This raises NotImplementedError as it's not supported by the API.
        """
        raise NotImplementedError(
            "Semantic Scholar API does not support direct venue-to-papers lookup"
        )
