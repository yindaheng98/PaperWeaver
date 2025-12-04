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

    async def paper_to_authors(self, paper: Paper) -> Tuple[int, int, int] | None:
        """Process one paper: fetch info and authors, write to cache and dst. Return (n_new_authors, n_new_links, n_failed) or None if failed."""
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
        self.logger.info(f"[P2A] Processing {len(tasks)} papers")
        results = await asyncio.gather(*tasks)
        n_paper_succ = sum(1 for r in results if r is not None)
        n_paper_fail = sum(1 for r in results if r is None)
        n_new_author = sum(r[0] for r in results if r is not None)
        n_new_link = sum(r[1] for r in results if r is not None)
        n_author_fail = sum(r[2] for r in results if r is not None)
        self.logger.info(f"[P2A] Done: {n_paper_succ} papers OK, {n_paper_fail} papers failed | {n_new_author} new authors, {n_new_link} new links, {n_author_fail} authors failed")
        return n_new_author

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
        self.logger.info(f"[P2A Init] Processing {len(tasks)} papers")
        results = await asyncio.gather(*tasks)
        n_paper_succ = sum(1 for r in results if r is not None)
        n_paper_fail = sum(1 for r in results if r is None)
        n_new_author = sum(r[0] for r in results if r is not None)
        n_new_link = sum(r[1] for r in results if r is not None)
        n_author_fail = sum(r[2] for r in results if r is not None)
        self.logger.info(f"[P2A Init] Done: {n_paper_succ} papers OK, {n_paper_fail} papers failed | {n_new_author} new authors, {n_new_link} new links, {n_author_fail} authors failed")
        return n_new_author
