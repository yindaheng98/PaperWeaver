"""
Paper to Venues weaver interface.

Workflow:
1. Get/add pending venues (objects may lack info, not written to DataDst)
2. Process each venue to fetch info
3. Commit link to DataDst once info is fetched
"""

from abc import ABCMeta, abstractmethod
import asyncio
from typing import Tuple
from .dataclass import Paper, Venue
from .iface import WeaverIface
from .iface_link import VenueLinkWeaverCacheIface
from .iface_init import PapersWeaverInitializerIface
from .bfs import bfs_cached_step


class Paper2VenuesWeaverCacheIface(VenueLinkWeaverCacheIface, metaclass=ABCMeta):
    """
    Cache interface for paper -> venues relationship.

    Pending venues: Temporarily cached venues that may not have info yet.
    When added, venues are registered and become discoverable via iterate_venues().
    """

    @abstractmethod
    async def get_pending_venues_for_paper(self, paper: Paper) -> list[Venue] | None:
        """Get pending venues for paper. Returns None if not yet fetched."""
        raise NotImplementedError

    @abstractmethod
    async def add_pending_venues_for_paper(self, paper: Paper, venues: list[Venue]) -> None:
        """Add pending venues for paper (registers them, merges with existing)."""
        raise NotImplementedError


class Paper2VenuesWeaverIface(WeaverIface, metaclass=ABCMeta):

    @property
    @abstractmethod
    def cache(self) -> Paper2VenuesWeaverCacheIface:
        raise ValueError("Cache is not set")

    async def paper_to_venues(self, paper: Paper) -> Tuple[int, int] | None:
        """Process one paper: fetch info and venues, write to cache and dst. Return number of new venues fetched and number of failed venues, or None if failed."""
        return await bfs_cached_step(
            parent=paper,
            load_parent_info=lambda p: p.get_info(self.src),
            save_parent_info=self.dst.save_paper_info,
            cache_get_parent_info=self.cache.get_paper_info,
            cache_set_parent_info=self.cache.set_paper_info,
            load_pending_children_from_parent=lambda p: p.get_venues(self.src),
            cache_get_pending_children=self.cache.get_pending_venues_for_paper,
            cache_add_pending_children=self.cache.add_pending_venues_for_paper,
            load_child_info=lambda c: c.get_info(self.src),
            save_child_info=self.dst.save_venue_info,
            cache_get_child_info=self.cache.get_venue_info,
            cache_set_child_info=self.cache.set_venue_info,
            save_link=self.dst.link_venue,
            is_link_committed=self.cache.is_venue_link_committed,
            commit_link=self.cache.commit_venue_link,
            logger=self.logger,
        )

    async def all_paper_to_venues(self) -> int:
        tasks = []
        async for paper in self.cache.iterate_papers():
            tasks.append(self.paper_to_venues(paper))
        self.logger.info(f"Fetching venues from {len(tasks)} new papers")
        state = await asyncio.gather(*tasks)
        paper_succ_count, paper_fail_count = sum([1 for s in state if s is not None]), sum([1 for s in state if s is None])
        venue_succ_count, venue_fail_count = sum([s[0] for s in state if s is not None]), sum([s[1] for s in state if s is not None])
        self.logger.info(f"Found {venue_succ_count} new venues from {paper_succ_count} papers. {venue_fail_count} venues fetch failed. {paper_fail_count} papers fetch failed.")
        return venue_succ_count

    async def bfs_once(self) -> int:
        return await self.all_paper_to_venues()

    @property
    @abstractmethod
    def initializer(self) -> PapersWeaverInitializerIface:
        """Initializer called before BFS starts."""
        pass

    async def init(self) -> int:
        tasks = []
        async for paper in self.initializer.fetch_papers():
            tasks.append(self.paper_to_venues(paper))
        self.logger.info(f"Fetching venues from {len(tasks)} new papers")
        state = await asyncio.gather(*tasks)
        paper_succ_count, paper_fail_count = sum([1 for s in state if s is not None]), sum([1 for s in state if s is None])
        venue_succ_count, venue_fail_count = sum([s[0] for s in state if s is not None]), sum([s[1] for s in state if s is not None])
        self.logger.info(f"Found {venue_succ_count} new venues from {paper_succ_count} papers. {venue_fail_count} venues fetch failed. {paper_fail_count} papers fetch failed.")
        return venue_succ_count
