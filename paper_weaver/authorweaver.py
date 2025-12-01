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
    async def get_authors_by_paper(self, paper: Paper) -> list[Author] | None:
        raise NotImplementedError

    @abstractmethod
    async def set_authors_of_paper(self, paper: Paper, authors: list[Author]) -> None:
        raise NotImplementedError

    @abstractmethod
    async def get_papers_by_author(self, author: Author) -> list[Paper] | None:
        raise NotImplementedError

    @abstractmethod
    async def set_papers_of_author(self, author: Author, papers: list[Paper]) -> None:
        raise NotImplementedError

    @abstractmethod
    async def is_link_author(self, paper: Paper, author: Author) -> bool:
        raise NotImplementedError

    @abstractmethod
    async def link_author(self, paper: Paper, author: Author) -> None:
        raise NotImplementedError

    @abstractmethod
    def iterate_authors(self) -> AsyncIterator[Author, None]:
        raise NotImplementedError

    @abstractmethod
    def iterate_papers(self) -> AsyncIterator[Paper, None]:
        raise NotImplementedError


class WeaverIface(metaclass=ABCMeta):

    @property
    @abstractmethod
    def src(self) -> DataSrc:
        raise ValueError("Model is not set")

    @property
    @abstractmethod
    def dst(self) -> DataDst:
        raise ValueError("Model is not set")


class Author2PapersWeaverIface(WeaverIface, metaclass=ABCMeta):

    @property
    @abstractmethod
    def cache(self) -> AuthorWeaverCache:
        raise ValueError("Model is not set")

    async def author_to_papers(self, author: Author) -> Tuple[int, int] | None:
        """Process one author: fetch info and papers, write to cache and dst. Return number of new papers fetched and number of failed papers, or None if failed."""
        # Step 1: Fetch and save author info
        author, author_info = await self.cache.get_author_info(author)  # fetch from cache
        if author_info is None:  # not in cache
            author, author_info = await author.get_info(self.src)  # fetch from source
            if author_info is None:  # failed to fetch
                return None  # no new author, no new papers
            # Write author info if fetched
            await self.dst.save_author_info(author, author_info)
            await self.cache.set_author_info(author, author_info)

        # Step 2: Fetch and save papers of this author
        papers = await self.cache.get_papers_by_author(author)  # fetch from cache
        if papers is None:  # not in cache
            papers = await author.get_papers(self.src)  # fetch from source
            if papers is None:  # failed to fetch
                return None  # no new papers
            # Write papers if fetched
            await self.cache.set_papers_of_author(author, papers)

        # Step 3: Fetch and save info for all papers of this author
        async def process_paper(paper):
            n_new = 0
            paper, paper_info = await self.cache.get_paper_info(paper)  # fetch from cache
            if paper_info is None:  # not in cache
                paper, paper_info = await paper.get_info(self.src)  # fetch from source
                if paper_info is None:  # failed to fetch
                    return None  # no new paper
                # Write paper info if fetched
                await self.dst.save_paper_info(paper, paper_info)
                await self.cache.set_paper_info(paper, paper_info)
                n_new = 1

            # Step 4: Link authors to paper if not already linked
            if not await self.cache.is_link_author(paper, author):  # check link in cache
                await self.dst.link_author(paper, author)  # link in dst
                await self.cache.link_author(paper, author)  # link in cache

            return n_new

        results = await asyncio.gather(*[process_paper(paper) for paper in papers])
        n_new_papers = sum([r for r in results if r is not None])
        n_failed = sum([1 for r in results if r is None])

        return n_new_papers, n_failed


class Paper2AuthorsWeaverIface(WeaverIface, metaclass=ABCMeta):

    @property
    @abstractmethod
    def cache(self) -> AuthorWeaverCache:
        raise ValueError("Model is not set")

    async def paper_to_authors(self, paper: Paper) -> Tuple[int, int] | None:
        """Process one paper: fetch info and authors, write to cache and dst. Return number of new authors fetched and number of failed authors, or None if failed."""
        # Step 1: Fetch and save paper info
        paper, paper_info = await self.cache.get_paper_info(paper)  # fetch from cache
        if paper_info is None:  # not in cache
            paper, paper_info = await paper.get_info(self.src)  # fetch from source
            if paper_info is None:  # failed to fetch
                return None  # no new paper, no new authors
            # Write paper info if fetched
            await self.dst.save_paper_info(paper, paper_info)
            await self.cache.set_paper_info(paper, paper_info)

        # Step 2: Fetch and save authors of this paper
        authors = await self.cache.get_authors_by_paper(paper)  # fetch from cache
        if authors is None:  # not in cache
            authors = await paper.get_authors(self.src)  # fetch from source
            if authors is None:  # failed to fetch
                return None  # no new authors
            # Write authors if fetched
            await self.cache.set_authors_of_paper(paper, authors)

        # Step 3: Fetch and save info for all authors of this paper
        async def process_author(author):
            n_new = 0
            author, author_info = await self.cache.get_author_info(author)  # fetch from cache
            if author_info is None:  # not in cache
                author, author_info = await author.get_info(self.src)  # fetch from source
                if author_info is None:  # failed to fetch
                    return None  # no new author
                # Write author info if fetched
                await self.dst.save_author_info(author, author_info)
                await self.cache.set_author_info(author, author_info)
                n_new = 1

            # Step 4: Link authors to paper if not already linked
            if not await self.cache.is_link_author(paper, author):  # check link in cache
                await self.dst.link_author(paper, author)  # link in dst
                await self.cache.link_author(paper, author)  # link in cache

            return n_new

        results = await asyncio.gather(*[process_author(author) for author in authors])
        n_new_authors = sum([r for r in results if r is not None])
        n_failed = sum([1 for r in results if r is None])

        return n_new_authors, n_failed


class AuthorWeaver(Author2PapersWeaverIface, Paper2AuthorsWeaverIface):
    logger = logging.getLogger("AuthorWeaver")

    def __init__(self, src: DataSrc, dst: DataDst, cache: AuthorWeaverCache):
        self._src = src
        self._dst = dst
        self._cache = cache

    @property
    def src(self) -> DataSrc:
        return self._src

    @property
    def dst(self) -> DataDst:
        return self._dst

    @property
    def cache(self) -> AuthorWeaverCache:
        return self._cache

    async def bfs_once(self):
        tasks = []
        async for author in self.cache.iterate_authors():
            tasks.append(self.author_to_papers(author))
        self.logger.info(f"Fetching papers from {len(tasks)} new authors")
        state = await asyncio.gather(*tasks)
        author_succ_count, author_fail_count = sum([1 for s in state if s is not None]), sum([1 for s in state if s is None])
        paper_succ_count, paper_fail_count = sum([s[0] for s in state if s is not None]), sum([s[1] for s in state if s is not None])
        self.logger.info(f"Found {paper_succ_count} new papers from {author_succ_count} authors. {paper_fail_count} papers fetch failed. {author_fail_count} authors fetch failed.")

        tasks = []
        async for paper in self.cache.iterate_papers():
            tasks.append(self.paper_to_authors(paper))
        self.logger.info(f"Fetching authors from {len(tasks)} new papers")
        state = await asyncio.gather(*tasks)
        paper_succ_count, paper_fail_count = sum([1 for s in state if s is not None]), sum([1 for s in state if s is None])
        author_succ_count, author_fail_count = sum([s[0] for s in state if s is not None]), sum([s[1] for s in state if s is not None])
        self.logger.info(f"Found {author_succ_count} new authors from {paper_succ_count} papers. {author_fail_count} authors fetch failed. {paper_fail_count} papers fetch failed.")
