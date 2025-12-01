from abc import ABCMeta, abstractmethod
import asyncio
from typing import Tuple
from .dataclass import Paper
from .iface import WeaverIface
from .iface_link import PaperLinkWeaverCacheIface


class Paper2ReferencesWeaverCacheIface(PaperLinkWeaverCacheIface, metaclass=ABCMeta):

    @abstractmethod
    async def get_references_by_paper(self, paper: Paper) -> list[Paper] | None:
        raise NotImplementedError

    @abstractmethod
    async def add_references_of_paper(self, paper: Paper, references: list[Paper]) -> None:
        raise NotImplementedError


class Paper2ReferencesWeaverIface(WeaverIface, metaclass=ABCMeta):

    @property
    @abstractmethod
    def cache(self) -> Paper2ReferencesWeaverCacheIface:
        raise ValueError("Model is not set")

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

        # Step 2: Fetch and save references of this paper
        references = await self.cache.get_references_by_paper(paper)  # fetch from cache
        if references is None:  # not in cache
            references = await paper.get_references(self.src)  # fetch from source
            if references is None:  # failed to fetch
                return None  # no new references
            # Write references if fetched
            await self.cache.add_references_of_paper(paper, references)

        # Step 3: Fetch and save info for all references of this paper
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

            # Step 4: Link references to paper if not already linked
            if not await self.cache.is_link_reference(paper, reference):  # check link in cache
                await self.dst.link_reference(paper, reference)  # link in dst
                await self.cache.link_reference(paper, reference)  # link in cache

            return n_new

        results = await asyncio.gather(*[process_reference(reference) for reference in references])
        n_new_references = sum([r for r in results if r is not None])
        n_failed = sum([1 for r in results if r is None])

        return n_new_references, n_failed
