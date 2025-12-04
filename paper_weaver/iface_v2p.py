"""
Venue to Papers weaver interface.

Workflow:
1. Get/add pending papers (objects may lack info, not written to DataDst)
2. Process each paper to fetch info
3. Commit link to DataDst once info is fetched
"""

from abc import ABCMeta, abstractmethod
import asyncio
from typing import Tuple
from .dataclass import Paper, Venue
from .iface import WeaverIface
from .iface_link import VenueLinkWeaverCacheIface
from .iface_init import VenuesWeaverInitializerIface
from .bfs import bfs_cached_step


class Venue2PapersWeaverCacheIface(VenueLinkWeaverCacheIface, metaclass=ABCMeta):
    """
    Cache interface for venue -> papers relationship.

    Pending papers: Temporarily cached papers that may not have info yet.
    When added, papers are registered and become discoverable via iterate_papers().
    """

    @abstractmethod
    async def get_pending_papers_for_venue(self, venue: Venue) -> list[Paper] | None:
        """Get pending papers for venue. Returns None if not yet fetched."""
        raise NotImplementedError

    @abstractmethod
    async def add_pending_papers_for_venue(self, venue: Venue, papers: list[Paper]) -> None:
        """Add pending papers for venue (registers them, merges with existing)."""
        raise NotImplementedError


class Venue2PapersWeaverIface(WeaverIface, metaclass=ABCMeta):

    @property
    @abstractmethod
    def cache(self) -> Venue2PapersWeaverCacheIface:
        raise ValueError("Cache is not set")

    async def venue_to_papers(self, venue: Venue) -> Tuple[int, int] | None:
        """Process one venue: fetch info and papers, write to cache and dst. Return number of new papers fetched and number of failed papers, or None if failed."""
        return await bfs_cached_step(
            parent=venue,
            load_parent_info=lambda v: v.get_info(self.src),
            save_parent_info=self.dst.save_venue_info,
            cache_get_parent_info=self.cache.get_venue_info,
            cache_set_parent_info=self.cache.set_venue_info,
            load_pending_children_from_parent=lambda v: v.get_papers(self.src),
            cache_get_pending_children=self.cache.get_pending_papers_for_venue,
            cache_add_pending_children=self.cache.add_pending_papers_for_venue,
            load_child_info=lambda p: p.get_info(self.src),
            save_child_info=self.dst.save_paper_info,
            cache_get_child_info=self.cache.get_paper_info,
            cache_set_child_info=self.cache.set_paper_info,
            # Note: link functions take (paper, venue) order, so we swap (parent=venue, child=paper)
            save_link=lambda venue, paper: self.dst.link_venue(paper, venue),
            is_link_committed=lambda venue, paper: self.cache.is_venue_link_committed(paper, venue),
            commit_link=lambda venue, paper: self.cache.commit_venue_link(paper, venue),
        )

    async def all_venue_to_papers(self) -> int:
        tasks = []
        async for venue in self.cache.iterate_venues():
            tasks.append(self.venue_to_papers(venue))
        self.logger.info(f"Fetching papers from {len(tasks)} new venues")
        state = await asyncio.gather(*tasks)
        venue_succ_count, venue_fail_count = sum([1 for s in state if s is not None]), sum([1 for s in state if s is None])
        paper_succ_count, paper_fail_count = sum([s[0] for s in state if s is not None]), sum([s[1] for s in state if s is not None])
        self.logger.info(f"Found {paper_succ_count} new papers from {venue_succ_count} venues. {paper_fail_count} papers fetch failed. {venue_fail_count} venues fetch failed.")
        return paper_succ_count

    async def bfs_once(self) -> int:
        return await self.all_venue_to_papers()

    @property
    @abstractmethod
    def initializer(self) -> VenuesWeaverInitializerIface:
        """Initializer called before BFS starts."""
        pass

    async def init(self) -> int:
        tasks = []
        async for venue in self.initializer.fetch_venues():
            tasks.append(self.venue_to_papers(venue))
        self.logger.info(f"Fetching papers from {len(tasks)} new venues")
        state = await asyncio.gather(*tasks)
        venue_succ_count, venue_fail_count = sum([1 for s in state if s is not None]), sum([1 for s in state if s is None])
        paper_succ_count, paper_fail_count = sum([s[0] for s in state if s is not None]), sum([s[1] for s in state if s is not None])
        self.logger.info(f"Found {paper_succ_count} new papers from {venue_succ_count} venues. {paper_fail_count} papers fetch failed. {venue_fail_count} venues fetch failed.")
        return paper_succ_count
