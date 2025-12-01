from abc import ABCMeta, abstractmethod
import asyncio
from typing import Tuple
from .dataclass import Paper, Author
from .iface import WeaverIface
from .iface_link import AuthorLinkWeaverCacheIface


class Author2PapersWeaverCacheIface(AuthorLinkWeaverCacheIface, metaclass=ABCMeta):

    @abstractmethod
    async def get_papers_by_author(self, author: Author) -> list[Paper] | None:
        raise NotImplementedError

    @abstractmethod
    async def set_papers_of_author(self, author: Author, papers: list[Paper]) -> None:
        raise NotImplementedError


class Author2PapersWeaverIface(WeaverIface, metaclass=ABCMeta):

    @property
    @abstractmethod
    def cache(self) -> Author2PapersWeaverCacheIface:
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
