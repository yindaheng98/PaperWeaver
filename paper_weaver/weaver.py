from abc import ABCMeta, abstractmethod
import asyncio
import logging
from typing import AsyncIterator, Tuple
from .dataclass import Paper, Author, DataSrc, DataDst


class AuthorWeaverCache(metaclass=ABCMeta):
    @abstractmethod
    async def get_author_info(self, author: Author) -> Tuple[Author, dict | None]:
        """return (updated author, info or None if not in cache)"""
        raise NotImplementedError

    @abstractmethod
    async def set_author_info(self, author: Author, info: dict) -> None:
        raise NotImplementedError

    @abstractmethod
    async def get_paper_info(self, paper: Paper) -> Tuple[Paper, dict | None]:
        """return (updated paper, info or None if not in cache)"""
        raise NotImplementedError

    @abstractmethod
    async def set_paper_info(self, paper: Paper, info: dict) -> None:
        raise NotImplementedError

    @abstractmethod
    async def get_authors_by_paper(self, paper: Paper) -> list[Author]:
        raise NotImplementedError

    @abstractmethod
    async def set_authors_of_paper(self, paper: Paper, authors: list[Author]) -> None:
        raise NotImplementedError

    @abstractmethod
    async def get_papers_by_author(self, author: Author) -> list[Paper]:
        raise NotImplementedError

    @abstractmethod
    async def set_papers_of_author(self, author: Author, papers: list[Paper]) -> None:
        raise NotImplementedError

    @abstractmethod
    def iterate_authors(self) -> AsyncIterator[Author, None]:
        raise NotImplementedError

    @abstractmethod
    def iterate_papers(self) -> AsyncIterator[Paper, None]:
        raise NotImplementedError


class AuthorWeaver:
    logger = logging.getLogger("AuthorWeaver")

    def __init__(self, src: DataSrc, dst: DataDst, cache: AuthorWeaverCache):
        self.src = src
        self.dst = dst
        self.cache = cache

    async def _fetch_author_info(self, author: Author) -> Author | None:
        author, info = await self.cache.get_author_info(author)  # try cache first
        if info is None:  # not in cache
            author, info = await author.get_info(self.src)  # fetch from source
            if info is None:  # failed to fetch
                return None
            # save to cache and dst
            await self.dst.save_author_info(author, info)
            await self.cache.set_author_info(author, info)
        return author

    async def _fetch_papers_of_author(self, author: Author) -> list[Paper]:
        papers = await self.cache.get_papers_by_author(author)  # try cache first
        if len(papers) == 0:  # not in cache
            papers = await author.get_papers(self.src)  # fetch from source
            if len(papers) == 0:  # failed to fetch
                return []  # skip
            # save to cache and dst
            await self.cache.set_papers_of_author(author, papers)
        return papers

    async def _fetch_author_info_and_papers(self, author: Author) -> int:
        author = await self._fetch_author_info(author)
        if author is None:
            return 0  # skip

        papers = await self._fetch_papers_of_author(author)
        return len(papers)

    async def _fetch_paper_info(self, paper: Paper) -> Paper | None:
        paper, info = await self.cache.get_paper_info(paper)  # try cache first
        if info is None:  # not in cache
            paper, info = await paper.get_info(self.src)  # fetch from source
            if info is None:  # failed to fetch
                return None
            # save to cache and dst
            await self.dst.save_paper_info(paper, info)
            await self.cache.set_paper_info(paper, info)
        return paper

    async def _fetch_authors_of_paper(self, paper: Paper) -> list[Author]:
        authors = await self.cache.get_authors_by_paper(paper)  # try cache first
        if len(authors) == 0:  # not in cache
            authors = await paper.get_authors(self.src)  # fetch from source
            if len(authors) == 0:  # failed to fetch
                return []  # skip
            # save to cache and dst
            await self.cache.set_authors_of_paper(paper, authors)
        return authors

    async def _fetch_paper_info_and_authors(self, paper: Paper) -> int:
        paper = await self._fetch_paper_info(paper)
        if paper is None:
            return 0  # skip

        authors = await self._fetch_authors_of_paper(paper)
        return len(authors)

    async def bfs_once(self):
        tasks = []
        async for author in self.cache.iterate_authors():
            tasks.append(self._fetch_author_info_and_papers(author))
        self.logger.info(f"Fetching papers from {len(tasks)} new authors")
        success = await asyncio.gather(*tasks)
        succ_count = sum([1 for s in success if s > 0])
        fail_count = sum([1 for s in success if s <= 0])
        fetched_count = sum(success)
        self.logger.info(f"Found {fetched_count} papers from {succ_count} authors. {fail_count} authors do not have new papers.")

        tasks = []
        async for paper in self.cache.iterate_papers():
            tasks.append(self._fetch_paper_info_and_authors(paper))
        self.logger.info(f"Fetching authors from {len(tasks)} new papers")
        success = await asyncio.gather(*tasks)
        succ_count = sum([1 for s in success if s > 0])
        fail_count = sum([1 for s in success if s <= 0])
        fetched_count = sum(success)
        self.logger.info(f"Found {fetched_count} authors from {succ_count} papers. {fail_count} papers do not have new authors.")
