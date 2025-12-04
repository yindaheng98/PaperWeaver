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

    async def author_to_papers(self, author: Author) -> Tuple[int, int] | None:
        """Process one author: fetch info and papers, write to cache and dst. Return number of new papers fetched and number of failed papers, or None if failed."""
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
        )

    async def all_author_to_papers(self) -> int:
        tasks = []
        async for author in self.cache.iterate_authors():
            tasks.append(self.author_to_papers(author))
        self.logger.info(f"Fetching papers from {len(tasks)} new authors")
        state = await asyncio.gather(*tasks)
        author_succ_count, author_fail_count = sum([1 for s in state if s is not None]), sum([1 for s in state if s is None])
        paper_succ_count, paper_fail_count = sum([s[0] for s in state if s is not None]), sum([s[1] for s in state if s is not None])
        self.logger.info(f"Found {paper_succ_count} new papers from {author_succ_count} authors. {paper_fail_count} papers fetch failed. {author_fail_count} authors fetch failed.")
        return paper_succ_count

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
        self.logger.info(f"Fetching papers from {len(tasks)} new authors")
        state = await asyncio.gather(*tasks)
        author_succ_count, author_fail_count = sum([1 for s in state if s is not None]), sum([1 for s in state if s is None])
        paper_succ_count, paper_fail_count = sum([s[0] for s in state if s is not None]), sum([s[1] for s in state if s is not None])
        self.logger.info(f"Found {paper_succ_count} new papers from {author_succ_count} authors. {paper_fail_count} papers fetch failed. {author_fail_count} authors fetch failed.")
        return paper_succ_count
