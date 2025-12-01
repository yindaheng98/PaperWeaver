from abc import ABCMeta
import asyncio
import logging
from .dataclass import DataSrc, DataDst
from .iface_a2p import Author2PapersWeaverIface, Author2PapersWeaverCacheIface
from .iface_p2a import Paper2AuthorsWeaverIface, Paper2AuthorsWeaverCacheIface


class AuthorWeaverCache(Author2PapersWeaverCacheIface, Paper2AuthorsWeaverCacheIface, metaclass=ABCMeta):
    pass


class AuthorWeaver(Author2PapersWeaverIface, Paper2AuthorsWeaverIface):
    logger = logging.getLogger("AuthorWeaver")

    def __init__(self, src: DataSrc, dst: DataDst, cache: AuthorWeaverCache):
        self._src = src
        self._dst = dst
        self._cache = cache

    @property
    def src(self) -> DataSrc:
        return self._src

    @property
    def dst(self) -> DataDst:
        return self._dst

    @property
    def cache(self) -> AuthorWeaverCache:
        return self._cache

    async def bfs_once(self):
        tasks = []
        async for author in self.cache.iterate_authors():
            tasks.append(self.author_to_papers(author))
        self.logger.info(f"Fetching papers from {len(tasks)} new authors")
        state = await asyncio.gather(*tasks)
        author_succ_count, author_fail_count = sum([1 for s in state if s is not None]), sum([1 for s in state if s is None])
        paper_succ_count, paper_fail_count = sum([s[0] for s in state if s is not None]), sum([s[1] for s in state if s is not None])
        self.logger.info(f"Found {paper_succ_count} new papers from {author_succ_count} authors. {paper_fail_count} papers fetch failed. {author_fail_count} authors fetch failed.")

        tasks = []
        async for paper in self.cache.iterate_papers():
            tasks.append(self.paper_to_authors(paper))
        self.logger.info(f"Fetching authors from {len(tasks)} new papers")
        state = await asyncio.gather(*tasks)
        paper_succ_count, paper_fail_count = sum([1 for s in state if s is not None]), sum([1 for s in state if s is None])
        author_succ_count, author_fail_count = sum([s[0] for s in state if s is not None]), sum([s[1] for s in state if s is not None])
        self.logger.info(f"Found {author_succ_count} new authors from {paper_succ_count} papers. {author_fail_count} authors fetch failed. {paper_fail_count} papers fetch failed.")
