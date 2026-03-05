"""
arXiv DataSrc implementation.
"""

from typing import Tuple

import feedparser

from ..cache import CachedAsyncPool, DataSrcCacheIface
from ...dataclass import DataSrc, Paper, Author, Venue
from .record import paper_to_arxiv_id, entry_to_paper, entry_to_info
from .utils import fetch_xml

ARXIV_QUERY_BASE = "https://export.arxiv.org/api/query"


class ArxivDataSrc(CachedAsyncPool, DataSrc):
    """
    DataSrc for arXiv XML API.

    Supported:
    - get_paper_info

    Partially supported:
    - get_venues_by_paper returns []
    """

    def __init__(
        self,
        cache: DataSrcCacheIface,
        max_concurrent: int = 10,
        cache_ttl: int | None = None,
    ):
        CachedAsyncPool.__init__(self, cache, max_concurrent)
        self._cache_ttl = cache_ttl

    async def _fetch_entry(self, paper: Paper) -> dict:
        arxiv_id = paper_to_arxiv_id(paper)
        if arxiv_id is None:
            raise ValueError("No valid arXiv identifier found for paper")
        url = f"{ARXIV_QUERY_BASE}?id_list={arxiv_id}"

        entry = await self.get_or_fetch(
            url,
            lambda: fetch_xml(url),
            lambda text: feedparser.parse(text).entries[0],
            self._cache_ttl,
        )
        if entry is None:
            raise ValueError(f"Failed to fetch arXiv entry: {arxiv_id}")
        return entry

    async def get_paper_info(self, paper: Paper) -> Tuple[Paper, dict]:
        entry = await self._fetch_entry(paper)

        updated_paper = entry_to_paper(entry)
        updated_paper.identifiers.update(paper.identifiers)
        info = entry_to_info(entry)
        return updated_paper, info

    async def get_venues_by_paper(self, paper: Paper) -> list[Venue]:
        _ = paper
        return []

    async def get_authors_by_paper(self, paper: Paper) -> list[Author]:
        raise NotImplementedError("arXiv DataSrc does not provide author entities")

    async def get_references_by_paper(self, paper: Paper) -> list[Paper]:
        raise NotImplementedError("arXiv DataSrc does not provide references endpoint")

    async def get_citations_by_paper(self, paper: Paper) -> list[Paper]:
        raise NotImplementedError("arXiv DataSrc does not provide citations endpoint")

    async def get_author_info(self, author: Author) -> Tuple[Author, dict]:
        raise NotImplementedError("arXiv DataSrc does not provide author profile lookup")

    async def get_papers_by_author(self, author: Author) -> list[Paper]:
        raise NotImplementedError("arXiv DataSrc does not provide author-to-papers lookup")

    async def get_venue_info(self, venue: Venue) -> Tuple[Venue, dict]:
        raise NotImplementedError("arXiv DataSrc does not provide venue profile lookup")

    async def get_papers_by_venue(self, venue: Venue) -> list[Paper]:
        raise NotImplementedError("arXiv DataSrc does not provide venue-to-papers lookup")
