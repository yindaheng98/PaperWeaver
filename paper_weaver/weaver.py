from abc import ABCMeta, abstractmethod
import asyncio
import logging
from typing import AsyncIterator, Tuple
from .dataclass import Paper, Author, DataSrc, DataDst


class AuthorWeaverCache(metaclass=ABCMeta):
    @abstractmethod
    async def find_author_info_and_identifiers_by_identifiers(self, identifiers: set[str]) -> Tuple[set[str], dict | None]:
        raise NotImplementedError

    async def get_author_info(self, author: Author) -> Tuple[Author, dict | None]:
        identifiers, info = await self.find_author_info_and_identifiers_by_identifiers(author.identifiers)
        return Author(identifiers=identifiers.union(author.identifiers)), info  # merge identifiers

    @abstractmethod
    async def set_author_info(self, author: Author, info: dict) -> None:
        raise NotImplementedError

    @abstractmethod
    async def find_paper_info_and_identifiers_by_identifiers(self, identifiers: set[str]) -> Tuple[set[str], dict | None]:
        raise NotImplementedError

    async def get_paper_info(self, paper: Paper) -> Tuple[Paper, dict | None]:
        identifiers, info = await self.find_paper_info_and_identifiers_by_identifiers(paper.identifiers)
        return Paper(identifiers=identifiers.union(paper.identifiers)), info  # merge identifiers

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

    async def _load_author_info_and_papers(self, author: Author) -> bool:
        author, info = await author.get_info(self.src)
        if info is None:
            return False
        await self.cache.set_author_info(author, info)
        await self.dst.save_author_info(author, info)
        return True

    async def _load_paper_info_and_authors(self, paper: Paper) -> bool:
        paper, info = await paper.get_info(self.src)
        if info is None:
            return False
        await self.cache.set_paper_info(paper, info)
        await self.dst.save_paper_info(paper, info)
        return True

    async def bfs_once(self):
        self.logger.info("Fetching authors from paper ...")
        tasks = []
        async for author in self.cache.iterate_authors():
            papers = await self.cache.get_papers_by_author(author)
            if len(papers) == 0:
                continue
            for paper in papers:
                tasks.append(self._load_paper_info_and_authors(paper))

        self.logger.info("Fetching papers from author ...")
        async for paper in self.cache.iterate_papers():
            authors = await self.cache.get_authors_by_paper(paper)
            if len(authors) == 0:
                continue
            for author in authors:
                tasks.append(self._load_author_info_and_papers(author))
