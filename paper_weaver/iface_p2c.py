from abc import ABCMeta, abstractmethod
import asyncio
from typing import Tuple
from .dataclass import Paper
from .iface import WeaverIface
from .iface_link import PaperLinkWeaverCacheIface


class Paper2CitationsWeaverCacheIface(PaperLinkWeaverCacheIface, metaclass=ABCMeta):

    @abstractmethod
    async def get_citations_by_paper(self, paper: Paper) -> list[Paper] | None:
        raise NotImplementedError

    @abstractmethod
    async def add_citations_of_paper(self, paper: Paper, citations: list[Paper]) -> None:
        raise NotImplementedError


class Paper2CitationsWeaverIface(WeaverIface, metaclass=ABCMeta):

    @property
    @abstractmethod
    def cache(self) -> Paper2CitationsWeaverCacheIface:
        raise ValueError("Model is not set")

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

        # Step 2: Fetch and save citations of this paper
        citations = await self.cache.get_citations_by_paper(paper)  # fetch from cache
        if citations is None:  # not in cache
            citations = await paper.get_citations(self.src)  # fetch from source
            if citations is None:  # failed to fetch
                return None  # no new citations
            # Write citations if fetched
            await self.cache.add_citations_of_paper(paper, citations)

        # Step 3: Fetch and save info for all citations of this paper
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

            # Step 4: Link citations to paper if not already linked
            if not await self.cache.is_link_citation(paper, citation):  # check link in cache
                await self.dst.link_citation(paper, citation)  # link in dst
                await self.cache.link_citation(paper, citation)  # link in cache

            return n_new

        results = await asyncio.gather(*[process_citation(citation) for citation in citations])
        n_new_citations = sum([r for r in results if r is not None])
        n_failed = sum([1 for r in results if r is None])

        return n_new_citations, n_failed
