"""
Paper to Authors weaver interface.

Workflow:
1. Get/set pending authors (objects may lack info, not written to DataDst)
2. Process each author to fetch info
3. Commit link to DataDst once info is fetched
"""

from abc import ABCMeta, abstractmethod
import asyncio
from typing import Tuple
from .dataclass import Paper, Author
from .iface import WeaverIface
from .iface_link import AuthorLinkWeaverCacheIface


class Paper2AuthorsWeaverCacheIface(AuthorLinkWeaverCacheIface, metaclass=ABCMeta):
    """
    Cache interface for paper -> authors relationship.

    Pending authors: Temporarily cached authors that may not have info yet.
    These are discoverable via iterate_authors() for later processing.
    """

    @abstractmethod
    async def get_pending_authors(self, paper: Paper) -> list[Author] | None:
        """Get pending authors for paper. Returns None if not yet fetched."""
        raise NotImplementedError

    @abstractmethod
    async def set_pending_authors(self, paper: Paper, authors: list[Author]) -> None:
        """Set pending authors for paper (registers them for later processing)."""
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

        # Step 2: Get or fetch pending authors (not yet written to DataDst)
        authors = await self.cache.get_pending_authors(paper)  # fetch from cache
        if authors is None:  # not in cache
            authors = await paper.get_authors(self.src)  # fetch from source
            if authors is None:  # failed to fetch
                return None  # no new authors
            # Cache pending authors (makes them discoverable via iterate_authors)
            await self.cache.set_pending_authors(paper, authors)

        # Step 3: Process each author - fetch info and commit link
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

            # Step 4: Commit link to DataDst if not already committed
            if not await self.cache.is_author_link_committed(paper, author):
                await self.dst.link_author(paper, author)  # write to DataDst
                await self.cache.commit_author_link(paper, author)  # mark as committed

            return n_new

        results = await asyncio.gather(*[process_author(author) for author in authors])
        n_new_authors = sum([r for r in results if r is not None])
        n_failed = sum([1 for r in results if r is None])

        return n_new_authors, n_failed
