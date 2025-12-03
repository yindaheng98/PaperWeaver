from abc import abstractmethod
import asyncio
import logging
from .dataclass import DataSrc, DataDst
from .iface import SimpleWeaver
from .iface_a2p import Author2PapersWeaverIface, Author2PapersWeaverCacheIface
from .iface_p2a import Paper2AuthorsWeaverIface, Paper2AuthorsWeaverCacheIface
from .iface_p2c import Paper2CitationsWeaverIface, Paper2CitationsWeaverCacheIface
from .iface_p2r import Paper2ReferencesWeaverIface, Paper2ReferencesWeaverCacheIface
from .iface_p2v import Paper2VenuesWeaverIface, Paper2VenuesWeaverCacheIface


class Author2PaperWeaver(Author2PapersWeaverIface):
    logger = logging.getLogger("Author2PaperWeaver")

    @property
    @abstractmethod
    def cache(self) -> Author2PapersWeaverCacheIface:
        raise ValueError("Cache is not set")

    async def all_author_to_papers(self) -> int:
        tasks = []
        async for author in self.cache.iterate_authors():
            tasks.append(self.author_to_papers(author))
        self.logger.info(f"Fetching papers from {len(tasks)} new authors")
        state = await asyncio.gather(*tasks)
        author_succ_count, author_fail_count = sum([1 for s in state if s is not None]), sum([1 for s in state if s is None])
        paper_succ_count, paper_fail_count = sum([s[0] for s in state if s is not None]), sum([s[1] for s in state if s is not None])
        self.logger.info(f"Found {paper_succ_count} new papers from {author_succ_count} authors. {paper_fail_count} papers fetch failed. {author_fail_count} authors fetch failed.")
        return paper_succ_count


class Paper2AuthorWeaver(Paper2AuthorsWeaverIface):
    logger = logging.getLogger("Paper2AuthorWeaver")

    @property
    @abstractmethod
    def cache(self) -> Paper2AuthorsWeaverCacheIface:
        raise ValueError("Cache is not set")

    async def all_paper_to_authors(self) -> int:
        tasks = []
        async for paper in self.cache.iterate_papers():
            tasks.append(self.paper_to_authors(paper))
        self.logger.info(f"Fetching authors from {len(tasks)} new papers")
        state = await asyncio.gather(*tasks)
        paper_succ_count, paper_fail_count = sum([1 for s in state if s is not None]), sum([1 for s in state if s is None])
        author_succ_count, author_fail_count = sum([s[0] for s in state if s is not None]), sum([s[1] for s in state if s is not None])
        self.logger.info(f"Found {author_succ_count} new authors from {paper_succ_count} papers. {author_fail_count} authors fetch failed. {paper_fail_count} papers fetch failed.")
        return author_succ_count


class Paper2CitationWeaver(Paper2CitationsWeaverIface):
    logger = logging.getLogger("Paper2CitationWeaver")

    @property
    @abstractmethod
    def cache(self) -> Paper2CitationsWeaverCacheIface:
        raise ValueError("Cache is not set")

    async def all_paper_to_citations(self) -> int:
        tasks = []
        async for paper in self.cache.iterate_papers():
            tasks.append(self.paper_to_citations(paper))
        self.logger.info(f"Fetching citations from {len(tasks)} new papers")
        state = await asyncio.gather(*tasks)
        paper_succ_count, paper_fail_count = sum([1 for s in state if s is not None]), sum([1 for s in state if s is None])
        citation_succ_count, citation_fail_count = sum([s[0] for s in state if s is not None]), sum([s[1] for s in state if s is not None])
        self.logger.info(f"Found {citation_succ_count} new citations from {paper_succ_count} papers. {citation_fail_count} citations fetch failed. {paper_fail_count} papers fetch failed.")
        return citation_succ_count


class Paper2ReferenceWeaver(Paper2ReferencesWeaverIface):
    logger = logging.getLogger("Paper2ReferenceWeaver")

    @property
    @abstractmethod
    def cache(self) -> Paper2ReferencesWeaverCacheIface:
        raise ValueError("Cache is not set")

    async def all_paper_to_references(self) -> int:
        tasks = []
        async for paper in self.cache.iterate_papers():
            tasks.append(self.paper_to_references(paper))
        self.logger.info(f"Fetching references from {len(tasks)} new papers")
        state = await asyncio.gather(*tasks)
        paper_succ_count, paper_fail_count = sum([1 for s in state if s is not None]), sum([1 for s in state if s is None])
        reference_succ_count, reference_fail_count = sum([s[0] for s in state if s is not None]), sum([s[1] for s in state if s is not None])
        self.logger.info(f"Found {reference_succ_count} new references from {paper_succ_count} papers. {reference_fail_count} references fetch failed. {paper_fail_count} papers fetch failed.")
        return reference_succ_count


class Paper2VenueWeaver(Paper2VenuesWeaverIface):
    logger = logging.getLogger("Paper2VenueWeaver")

    @property
    @abstractmethod
    def cache(self) -> Paper2VenuesWeaverCacheIface:
        raise ValueError("Cache is not set")

    async def all_paper_to_venues(self) -> int:
        tasks = []
        async for paper in self.cache.iterate_papers():
            tasks.append(self.paper_to_venues(paper))
        self.logger.info(f"Fetching venues from {len(tasks)} new papers")
        state = await asyncio.gather(*tasks)
        paper_succ_count, paper_fail_count = sum([1 for s in state if s is not None]), sum([1 for s in state if s is None])
        venue_succ_count, venue_fail_count = sum([s[0] for s in state if s is not None]), sum([s[1] for s in state if s is not None])
        self.logger.info(f"Found {venue_succ_count} new venues from {paper_succ_count} papers. {venue_fail_count} venues fetch failed. {paper_fail_count} papers fetch failed.")
        return venue_succ_count
