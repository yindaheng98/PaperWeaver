from abc import ABCMeta, abstractmethod
import asyncio
import logging
from .iface_a2p import Author2PapersWeaverIface, Author2PapersWeaverCacheIface
from .iface_p2a import Paper2AuthorsWeaverIface, Paper2AuthorsWeaverCacheIface
from .iface_p2c import Paper2CitationsWeaverIface, Paper2CitationsWeaverCacheIface
from .iface_p2r import Paper2ReferencesWeaverIface, Paper2ReferencesWeaverCacheIface
from .iface_p2v import Paper2VenuesWeaverIface, Paper2VenuesWeaverCacheIface


class Weaver(metaclass=ABCMeta):
    @property
    def logger(self) -> logging.Logger:
        return logging.getLogger(self.__class__.__name__)

    @abstractmethod
    async def bfs_once(self) -> int:
        """Perform one BFS iteration, return number of new entities fetched."""
        raise NotImplementedError

    async def bfs(self, max_iterations: int = 10) -> int:
        """Perform BFS for a number of iterations, return total number of new entities fetched."""
        total_new = 0
        for iteration in range(max_iterations):
            self.logger.info(f"Starting BFS iteration {iteration + 1}")
            new_count = await self.bfs_once()
            if new_count == 0:
                self.logger.info("No new entities fetched, stopping BFS.")
                break
            total_new += new_count
        self.logger.info(f"BFS completed with total {total_new} new entities fetched.")
        return total_new


class Author2PaperWeaver(Author2PapersWeaverIface, Weaver):

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

    async def bfs_once(self) -> int:
        return await self.all_author_to_papers()


class Paper2AuthorWeaver(Paper2AuthorsWeaverIface, Weaver):

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

    async def bfs_once(self) -> int:
        return await self.all_paper_to_authors()


class Paper2CitationWeaver(Paper2CitationsWeaverIface, Weaver):

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

    async def bfs_once(self) -> int:
        return await self.all_paper_to_citations()


class Paper2ReferenceWeaver(Paper2ReferencesWeaverIface, Weaver):

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

    async def bfs_once(self) -> int:
        return await self.all_paper_to_references()


class Paper2VenueWeaver(Paper2VenuesWeaverIface, Weaver):

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

    async def bfs_once(self) -> int:
        return await self.all_paper_to_venues()
