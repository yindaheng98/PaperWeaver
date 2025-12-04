"""
Paper to References weaver interface.

Workflow:
1. Get/add pending references (objects may lack info, not written to DataDst)
2. Process each reference to fetch info
3. Commit link to DataDst once info is fetched
"""

from abc import ABCMeta, abstractmethod
import asyncio
from typing import Tuple
from .dataclass import Paper
from .iface import WeaverIface
from .iface_link import PaperLinkWeaverCacheIface
from .iface_init import PapersWeaverInitializerIface


class Paper2ReferencesWeaverCacheIface(PaperLinkWeaverCacheIface, metaclass=ABCMeta):
    """
    Cache interface for paper -> references relationship.

    Pending references: Temporarily cached references that may not have info yet.
    When added, references are registered and become discoverable via iterate_papers().
    """

    @abstractmethod
    async def get_pending_references_for_paper(self, paper: Paper) -> list[Paper] | None:
        """Get pending references for paper. Returns None if not yet fetched."""
        raise NotImplementedError

    @abstractmethod
    async def add_pending_references_for_paper(self, paper: Paper, references: list[Paper]) -> None:
        """Add pending references for paper (registers them, merges with existing)."""
        raise NotImplementedError


class Paper2ReferencesWeaverIface(WeaverIface, metaclass=ABCMeta):

    @property
    @abstractmethod
    def cache(self) -> Paper2ReferencesWeaverCacheIface:
        raise ValueError("Cache is not set")

    async def paper_to_references(self, paper: Paper) -> Tuple[int, int] | None:
        """Process one paper: fetch info and references, write to cache and dst. Return number of new references fetched and number of failed references, or None if failed."""
        # Step 1: Fetch and save paper info
        paper, paper_info = await self.cache.get_paper_info(paper)  # fetch from cache
        if paper_info is None:  # not in cache
            paper, paper_info = await paper.get_info(self.src)  # fetch from source
            if paper_info is None:  # failed to fetch
                return None  # no new paper, no new references
            # Write paper info if fetched
            await self.dst.save_paper_info(paper, paper_info)
            await self.cache.set_paper_info(paper, paper_info)

        # Step 2: Get or fetch pending references (not yet written to DataDst)
        references = await self.cache.get_pending_references_for_paper(paper)  # fetch from cache
        if references is None:  # not in cache
            references = await paper.get_references(self.src)  # fetch from source
            if references is None:  # failed to fetch
                return None  # no new references
            # Cache pending references (registers them, discoverable via iterate_papers)
            await self.cache.add_pending_references_for_paper(paper, references)

        # Step 3: Process each reference - fetch info and commit link
        async def process_reference(reference):
            n_new = 0
            reference, reference_info = await self.cache.get_paper_info(reference)  # fetch from cache
            if reference_info is None:  # not in cache
                reference, reference_info = await reference.get_info(self.src)  # fetch from source
                if reference_info is None:  # failed to fetch
                    return None  # no new reference
                # Write reference info if fetched
                await self.dst.save_paper_info(reference, reference_info)
                await self.cache.set_paper_info(reference, reference_info)
                n_new = 1

            # Step 4: Commit link to DataDst if not already committed
            if not await self.cache.is_reference_link_committed(paper, reference):
                await self.dst.link_reference(paper, reference)  # write to DataDst
                await self.cache.commit_reference_link(paper, reference)  # mark as committed

            return n_new

        results = await asyncio.gather(*[process_reference(reference) for reference in references])
        n_new_references = sum([r for r in results if r is not None])
        n_failed = sum([1 for r in results if r is None])

        return n_new_references, n_failed

    async def all_paper_to_references(self) -> int:
        tasks = []
        async for paper in self.cache.iterate_papers():
            tasks.append(self.paper_to_references(paper))
        self.logger.info(f"Fetching references from {len(tasks)} new papers")
        state = await asyncio.gather(*tasks)
        paper_succ_count, paper_fail_count = sum([1 for s in state if s is not None]), sum([1 for s in state if s is None])
        reference_succ_count, reference_fail_count = sum([s[0] for s in state if s is not None]), sum([s[1] for s in state if s is not None])
        self.logger.info(f"Found {reference_succ_count} new references from {paper_succ_count} papers. {reference_fail_count} references fetch failed. {paper_fail_count} papers fetch failed.")
        return reference_succ_count

    async def bfs_once(self) -> int:
        return await self.all_paper_to_references()

    @property
    @abstractmethod
    def initializer(self) -> PapersWeaverInitializerIface:
        """Initializer called before BFS starts."""
        pass

    async def init(self) -> int:
        tasks = []
        async for paper in self.initializer.fetch_papers():
            tasks.append(self.paper_to_references(paper))
        self.logger.info(f"Fetching references from {len(tasks)} new papers")
        state = await asyncio.gather(*tasks)
        paper_succ_count, paper_fail_count = sum([1 for s in state if s is not None]), sum([1 for s in state if s is None])
        reference_succ_count, reference_fail_count = sum([s[0] for s in state if s is not None]), sum([s[1] for s in state if s is not None])
        self.logger.info(f"Found {reference_succ_count} new references from {paper_succ_count} papers. {reference_fail_count} references fetch failed. {paper_fail_count} papers fetch failed.")
        return reference_succ_count
