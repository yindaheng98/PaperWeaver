"""
Author to Papers weaver interface.

Workflow:
1. Get/add pending papers (objects may lack info, not written to DataDst)
2. Process each paper to fetch info
3. Commit link to DataDst once info is fetched
"""

from abc import ABCMeta, abstractmethod
import asyncio
from typing import Tuple
from .dataclass import Paper, Author
from .iface import WeaverIface
from .iface_link import AuthorLinkWeaverCacheIface
from .iface_init import AuthorsWeaverInitializerIface
from .bfs import bfs_cached_step


class Author2PapersWeaverCacheIface(AuthorLinkWeaverCacheIface, metaclass=ABCMeta):
    """
    Cache interface for author -> papers relationship.

    Pending papers: Temporarily cached papers that may not have info yet.
    When added, papers are registered and become discoverable via iterate_papers().
    """

    @abstractmethod
    async def get_pending_papers_for_author(self, author: Author) -> list[Paper] | None:
        """Get pending papers for author. Returns None if not yet fetched."""
        raise NotImplementedError

    @abstractmethod
    async def add_pending_papers_for_author(self, author: Author, papers: list[Paper]) -> None:
        """Add pending papers for author (registers them, merges with existing)."""
        raise NotImplementedError


class Author2PapersWeaverIface(WeaverIface, metaclass=ABCMeta):

    @property
    @abstractmethod
    def cache(self) -> Author2PapersWeaverCacheIface:
        raise ValueError("Cache is not set")

    async def author_to_papers(self, author: Author) -> Tuple[int, int, int] | None:
        """Process one author: fetch info and papers, write to cache and dst. Return (n_new_papers, n_new_links, n_failed) or None if failed."""
        return await bfs_cached_step(
            parent=author,
            load_parent_info=lambda a: a.get_info(self.src),
            save_parent_info=self.dst.save_author_info,
            cache_get_parent_info=self.cache.get_author_info,
            cache_set_parent_info=self.cache.set_author_info,
            load_pending_children_from_parent=lambda a: a.get_papers(self.src),
            cache_get_pending_children=self.cache.get_pending_papers_for_author,
            cache_add_pending_children=self.cache.add_pending_papers_for_author,
            load_child_info=lambda p: p.get_info(self.src),
            save_child_info=self.dst.save_paper_info,
            cache_get_child_info=self.cache.get_paper_info,
            cache_set_child_info=self.cache.set_paper_info,
            # Note: link functions take (paper, author) order, so we swap (parent=author, child=paper)
            save_link=lambda author, paper: self.dst.link_author(paper, author),
            is_link_committed=lambda author, paper: self.cache.is_author_link_committed(paper, author),
            commit_link=lambda author, paper: self.cache.commit_author_link(paper, author),
            logger=self.logger,
        )

    async def all_author_to_papers(self) -> int:
        tasks = []
        async for author in self.cache.iterate_authors():
            tasks.append(self.author_to_papers(author))
        self.logger.info(f"[A2P] Processing {len(tasks)} authors")
        results = await asyncio.gather(*tasks)
        n_author_succ = sum(1 for r in results if r is not None)
        n_author_fail = sum(1 for r in results if r is None)
        n_new_paper = sum(r[0] for r in results if r is not None)
        n_new_link = sum(r[1] for r in results if r is not None)
        n_paper_fail = sum(r[2] for r in results if r is not None)
        self.logger.info(f"[A2P] Done: {n_author_succ} authors OK, {n_author_fail} authors failed | {n_new_paper} new papers, {n_new_link} new links, {n_paper_fail} papers failed")
        return n_new_paper

    async def bfs_once(self) -> int:
        return await self.all_author_to_papers()

    @property
    @abstractmethod
    def initializer(self) -> AuthorsWeaverInitializerIface:
        """Initializer called before BFS starts."""
        pass

    async def init(self) -> int:
        tasks = []
        async for author in self.initializer.fetch_authors():
            tasks.append(self.author_to_papers(author))
        self.logger.info(f"[A2P Init] Processing {len(tasks)} authors")
        results = await asyncio.gather(*tasks)
        n_author_succ = sum(1 for r in results if r is not None)
        n_author_fail = sum(1 for r in results if r is None)
        n_new_paper = sum(r[0] for r in results if r is not None)
        n_new_link = sum(r[1] for r in results if r is not None)
        n_paper_fail = sum(r[2] for r in results if r is not None)
        self.logger.info(f"[A2P Init] Done: {n_author_succ} authors OK, {n_author_fail} authors failed | {n_new_paper} new papers, {n_new_link} new links, {n_paper_fail} papers failed")
        return n_new_paper
