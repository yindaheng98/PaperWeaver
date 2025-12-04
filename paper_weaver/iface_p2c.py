"""
Paper to Citations weaver interface.

Workflow:
1. Get/add pending citations (objects may lack info, not written to DataDst)
2. Process each citation to fetch info
3. Commit link to DataDst once info is fetched
"""

from abc import ABCMeta, abstractmethod
import asyncio
from typing import Tuple
from .dataclass import Paper
from .iface import WeaverIface
from .iface_link import PaperLinkWeaverCacheIface
from .iface_init import PapersWeaverInitializerIface
from .bfs import bfs_cached_step


class Paper2CitationsWeaverCacheIface(PaperLinkWeaverCacheIface, metaclass=ABCMeta):
    """
    Cache interface for paper -> citations relationship.

    Pending citations: Temporarily cached citations that may not have info yet.
    When added, citations are registered and become discoverable via iterate_papers().
    """

    @abstractmethod
    async def get_pending_citations_for_paper(self, paper: Paper) -> list[Paper] | None:
        """Get pending citations for paper. Returns None if not yet fetched."""
        raise NotImplementedError

    @abstractmethod
    async def add_pending_citations_for_paper(self, paper: Paper, citations: list[Paper]) -> None:
        """Add pending citations for paper (registers them, merges with existing)."""
        raise NotImplementedError


class Paper2CitationsWeaverIface(WeaverIface, metaclass=ABCMeta):

    @property
    @abstractmethod
    def cache(self) -> Paper2CitationsWeaverCacheIface:
        raise ValueError("Cache is not set")

    async def paper_to_citations(self, paper: Paper) -> Tuple[int, int, int] | None:
        """Process one paper: fetch info and citations, write to cache and dst. Return (n_new_citations, n_new_links, n_failed) or None if failed."""
        return await bfs_cached_step(
            parent=paper,
            load_parent_info=lambda p: p.get_info(self.src),
            save_parent_info=self.dst.save_paper_info,
            cache_get_parent_info=self.cache.get_paper_info,
            cache_set_parent_info=self.cache.set_paper_info,
            load_pending_children_from_parent=lambda p: p.get_citations(self.src),
            cache_get_pending_children=self.cache.get_pending_citations_for_paper,
            cache_add_pending_children=self.cache.add_pending_citations_for_paper,
            load_child_info=lambda c: c.get_info(self.src),
            save_child_info=self.dst.save_paper_info,
            cache_get_child_info=self.cache.get_paper_info,
            cache_set_child_info=self.cache.set_paper_info,
            save_link=self.dst.link_citation,
            is_link_committed=self.cache.is_citation_link_committed,
            commit_link=self.cache.commit_citation_link,
            logger=self.logger,
        )

    async def all_paper_to_citations(self) -> int:
        tasks = []
        async for paper in self.cache.iterate_papers():
            tasks.append(self.paper_to_citations(paper))
        self.logger.info(f"[P2C] Processing {len(tasks)} papers")
        results = await asyncio.gather(*tasks)
        n_paper_succ = sum(1 for r in results if r is not None)
        n_paper_fail = sum(1 for r in results if r is None)
        n_new_cite = sum(r[0] for r in results if r is not None)
        n_new_link = sum(r[1] for r in results if r is not None)
        n_cite_fail = sum(r[2] for r in results if r is not None)
        self.logger.info(f"[P2C] Done: {n_paper_succ} papers OK, {n_paper_fail} papers failed | {n_new_cite} new cites, {n_new_link} new links, {n_cite_fail} cites failed")
        return n_new_cite

    async def bfs_once(self) -> int:
        return await self.all_paper_to_citations()

    @property
    @abstractmethod
    def initializer(self) -> PapersWeaverInitializerIface:
        """Initializer called before BFS starts."""
        pass

    async def init(self) -> int:
        tasks = []
        async for paper in self.initializer.fetch_papers():
            tasks.append(self.paper_to_citations(paper))
        self.logger.info(f"[P2C Init] Processing {len(tasks)} papers")
        results = await asyncio.gather(*tasks)
        n_paper_succ = sum(1 for r in results if r is not None)
        n_paper_fail = sum(1 for r in results if r is None)
        n_new_cite = sum(r[0] for r in results if r is not None)
        n_new_link = sum(r[1] for r in results if r is not None)
        n_cite_fail = sum(r[2] for r in results if r is not None)
        self.logger.info(f"[P2C Init] Done: {n_paper_succ} papers OK, {n_paper_fail} papers failed | {n_new_cite} new cites, {n_new_link} new links, {n_cite_fail} cites failed")
        return n_new_cite
