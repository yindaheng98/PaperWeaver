"""
Paper to Authors weaver interface.

Workflow:
1. Get/add pending authors (objects may lack info, not written to DataDst)
2. Process each author to fetch info
3. Commit link to DataDst once info is fetched
"""

from abc import ABCMeta, abstractmethod
import asyncio
from typing import Tuple
from .dataclass import Paper, Author
from .iface import WeaverIface
from .iface_link import AuthorLinkWeaverCacheIface
from .iface_init import PapersWeaverInitializerIface
from .bfs import bfs_cached_step


class Paper2AuthorsWeaverCacheIface(AuthorLinkWeaverCacheIface, metaclass=ABCMeta):
    """
    Cache interface for paper -> authors relationship.

    Pending authors: Temporarily cached authors that may not have info yet.
    When added, authors are registered and become discoverable via iterate_authors().
    """

    @abstractmethod
    async def get_pending_authors_for_paper(self, paper: Paper) -> list[Author] | None:
        """Get pending authors for paper. Returns None if not yet fetched."""
        raise NotImplementedError

    @abstractmethod
    async def add_pending_authors_for_paper(self, paper: Paper, authors: list[Author]) -> None:
        """Add pending authors for paper (registers them, merges with existing)."""
        raise NotImplementedError


class Paper2AuthorsWeaverIface(WeaverIface, metaclass=ABCMeta):

    @property
    @abstractmethod
    def cache(self) -> Paper2AuthorsWeaverCacheIface:
        raise ValueError("Cache is not set")

    async def paper_to_authors(self, paper: Paper) -> Tuple[int, int] | None:
        """Process one paper: fetch info and authors, write to cache and dst. Return number of new authors fetched and number of failed authors, or None if failed."""
        return await bfs_cached_step(
            parent=paper,
            load_parent_info=lambda p: p.get_info(self.src),
            save_parent_info=self.dst.save_paper_info,
            cache_get_parent_info=self.cache.get_paper_info,
            cache_set_parent_info=self.cache.set_paper_info,
            load_pending_children_from_parent=lambda p: p.get_authors(self.src),
            cache_get_pending_children=self.cache.get_pending_authors_for_paper,
            cache_add_pending_children=self.cache.add_pending_authors_for_paper,
            load_child_info=lambda c: c.get_info(self.src),
            save_child_info=self.dst.save_author_info,
            cache_get_child_info=self.cache.get_author_info,
            cache_set_child_info=self.cache.set_author_info,
            save_link=self.dst.link_author,
            is_link_committed=self.cache.is_author_link_committed,
            commit_link=self.cache.commit_author_link,
            logger=self.logger,
        )

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

    @property
    @abstractmethod
    def initializer(self) -> PapersWeaverInitializerIface:
        """Initializer called before BFS starts."""
        pass

    async def init(self) -> int:
        tasks = []
        async for paper in self.initializer.fetch_papers():
            tasks.append(self.paper_to_authors(paper))
        self.logger.info(f"Fetching authors from {len(tasks)} new papers")
        state = await asyncio.gather(*tasks)
        paper_succ_count, paper_fail_count = sum([1 for s in state if s is not None]), sum([1 for s in state if s is None])
        author_succ_count, author_fail_count = sum([s[0] for s in state if s is not None]), sum([s[1] for s in state if s is not None])
        self.logger.info(f"Found {author_succ_count} new authors from {paper_succ_count} papers. {author_fail_count} authors fetch failed. {paper_fail_count} papers fetch failed.")
        return author_succ_count
