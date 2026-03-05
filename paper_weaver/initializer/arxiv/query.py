"""
arXiv query-based initializer.
"""

from typing import AsyncIterator

from ...dataclass import Paper
from ...iface_init import PapersWeaverInitializerIface
from ...datasrc.arxiv.datasrc import ArxivDataSrc


class ArxivPapersInitializer(PapersWeaverInitializerIface):
    """
    Initialize weaver with papers fetched from arXiv search query.
    Uses ArxivDataSrc.preload_search_cache to populate the datasrc cache
    so that subsequent get_paper_info calls hit cached data.
    """

    def __init__(self, datasrc: ArxivDataSrc, query: str, pages: int = 1, page_size: int = 10):
        self._datasrc = datasrc
        self._query = query
        self._pages = pages
        self._page_size = page_size

    async def fetch_papers(self) -> AsyncIterator[Paper]:
        for page_idx in range(self._pages):
            start = page_idx * self._page_size
            query_str = f"search_query={self._query}&start={start}&max_results={self._page_size}"
            count = 0
            async for paper in self._datasrc.preload_search_cache(query_str):
                yield paper
                count += 1
            if count < self._page_size:
                break
