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
        # Step 1: Fetch and save paper info
        paper, paper_info = await self.cache.get_paper_info(paper)  # fetch from cache
        if paper_info is None:  # not in cache
            paper, paper_info = await paper.get_info(self.src)  # fetch from source
            if paper_info is None:  # failed to fetch
                return None  # no new paper, no new venues
            # Write paper info if fetched
            await self.dst.save_paper_info(paper, paper_info)
            await self.cache.set_paper_info(paper, paper_info)

        # Step 2: Get or fetch pending venues (not yet written to DataDst)
        venues = await self.cache.get_pending_venues_for_paper(paper)  # fetch from cache
        if venues is None:  # not in cache
            venues = await paper.get_venues(self.src)  # fetch from source
            if venues is None:  # failed to fetch
                return None  # no new venues
            # Cache pending venues (registers them, discoverable via iterate_venues)
            await self.cache.add_pending_venues_for_paper(paper, venues)

        # Step 3: Process each venue - fetch info and commit link
        async def process_venue(venue):
            n_new = 0
            venue, venue_info = await self.cache.get_venue_info(venue)  # fetch from cache
            if venue_info is None:  # not in cache
                venue, venue_info = await venue.get_info(self.src)  # fetch from source
                if venue_info is None:  # failed to fetch
                    return None  # no new venue
                # Write venue info if fetched
                await self.dst.save_venue_info(venue, venue_info)
                await self.cache.set_venue_info(venue, venue_info)
                n_new = 1

            # Step 4: Commit link to DataDst if not already committed
            if not await self.cache.is_venue_link_committed(paper, venue):
                await self.dst.link_venue(paper, venue)  # write to DataDst
                await self.cache.commit_venue_link(paper, venue)  # mark as committed

            return n_new

        results = await asyncio.gather(*[process_venue(venue) for venue in venues])
        n_new_venues = sum([r for r in results if r is not None])
        n_failed = sum([1 for r in results if r is None])

        return n_new_venues, n_failed

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
