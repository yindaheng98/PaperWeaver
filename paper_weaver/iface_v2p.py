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
        # Step 1: Fetch and save venue info
        venue, venue_info = await self.cache.get_venue_info(venue)  # fetch from cache
        if venue_info is None:  # not in cache
            venue, venue_info = await venue.get_info(self.src)  # fetch from source
            if venue_info is None:  # failed to fetch
                return None  # no new venue, no new papers
            # Write venue info if fetched
            await self.dst.save_venue_info(venue, venue_info)
            await self.cache.set_venue_info(venue, venue_info)

        # Step 2: Get or fetch pending papers (not yet written to DataDst)
        papers = await self.cache.get_pending_papers_for_venue(venue)  # fetch from cache
        if papers is None:  # not in cache
            papers = await venue.get_papers(self.src)  # fetch from source
            if papers is None:  # failed to fetch
                return None  # no new papers
            # Cache pending papers (registers them, discoverable via iterate_papers)
            await self.cache.add_pending_papers_for_venue(venue, papers)

        # Step 3: Process each paper - fetch info and commit link
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

            # Step 4: Commit link to DataDst if not already committed
            if not await self.cache.is_venue_link_committed(paper, venue):
                await self.dst.link_venue(paper, venue)  # write to DataDst
                await self.cache.commit_venue_link(paper, venue)  # mark as committed

            return n_new

        results = await asyncio.gather(*[process_paper(paper) for paper in papers])
        n_new_papers = sum([r for r in results if r is not None])
        n_failed = sum([1 for r in results if r is None])

        return n_new_papers, n_failed

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
