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

    async def paper_to_citations(self, paper: Paper) -> Tuple[int, int] | None:
        """Process one paper: fetch info and citations, write to cache and dst. Return number of new citations fetched and number of failed citations, or None if failed."""
        # Step 1: Fetch and save paper info
        paper, paper_info = await self.cache.get_paper_info(paper)  # fetch from cache
        if paper_info is None:  # not in cache
            paper, paper_info = await paper.get_info(self.src)  # fetch from source
            if paper_info is None:  # failed to fetch
                return None  # no new paper, no new citations
            # Write paper info if fetched
            await self.dst.save_paper_info(paper, paper_info)
            await self.cache.set_paper_info(paper, paper_info)

        # Step 2: Get or fetch pending citations (not yet written to DataDst)
        citations = await self.cache.get_pending_citations_for_paper(paper)  # fetch from cache
        if citations is None:  # not in cache
            citations = await paper.get_citations(self.src)  # fetch from source
            if citations is None:  # failed to fetch
                return None  # no new citations
            # Cache pending citations (registers them, discoverable via iterate_papers)
            await self.cache.add_pending_citations_for_paper(paper, citations)

        # Step 3: Process each citation - fetch info and commit link
        async def process_citation(citation):
            n_new = 0
            citation, citation_info = await self.cache.get_paper_info(citation)  # fetch from cache
            if citation_info is None:  # not in cache
                citation, citation_info = await citation.get_info(self.src)  # fetch from source
                if citation_info is None:  # failed to fetch
                    return None  # no new citation
                # Write citation info if fetched
                await self.dst.save_paper_info(citation, citation_info)
                await self.cache.set_paper_info(citation, citation_info)
                n_new = 1

            # Step 4: Commit link to DataDst if not already committed
            if not await self.cache.is_citation_link_committed(paper, citation):
                await self.dst.link_citation(paper, citation)  # write to DataDst
                await self.cache.commit_citation_link(paper, citation)  # mark as committed

            return n_new

        results = await asyncio.gather(*[process_citation(citation) for citation in citations])
        n_new_citations = sum([r for r in results if r is not None])
        n_failed = sum([1 for r in results if r is None])

        return n_new_citations, n_failed

    async def all_paper_to_citations(self) -> int:
        tasks = []
        async for paper in self.cache.iterate_papers():
            tasks.append(self.paper_to_citations(paper))
        self.logger.info(f"Fetching citations from {len(tasks)} new papers")
        state = await asyncio.gather(*tasks)
        paper_succ_count, paper_fail_count = sum([1 for s in state if s is not None]), sum([1 for s in state if s is None])
        citation_succ_count, citation_fail_count = sum([s[0] for s in state if s is not None]), sum([s[1] for s in state if s is not None])
        self.logger.info(f"Found {citation_succ_count} new citations from {paper_succ_count} papers. {citation_fail_count} citations fetch failed. {paper_fail_count} papers fetch failed.")
        return citation_succ_count

    async def bfs_once(self) -> int:
        return await self.all_paper_to_citations()
