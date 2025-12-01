from abc import ABCMeta, abstractmethod
import asyncio
from typing import Tuple
from .dataclass import Paper, Author
from .iface import WeaverIface
from .iface_link import AuthorLinkWeaverCacheIface


class Paper2AuthorsWeaverCacheIface(AuthorLinkWeaverCacheIface, metaclass=ABCMeta):

    @abstractmethod
    async def get_authors_by_paper(self, paper: Paper) -> list[Author] | None:
        raise NotImplementedError

    @abstractmethod
    async def add_authors_of_paper(self, paper: Paper, authors: list[Author]) -> None:
        raise NotImplementedError


class Paper2AuthorsWeaverIface(WeaverIface, metaclass=ABCMeta):

    @property
    @abstractmethod
    def cache(self) -> Paper2AuthorsWeaverCacheIface:
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
            await self.cache.add_authors_of_paper(paper, authors)

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
