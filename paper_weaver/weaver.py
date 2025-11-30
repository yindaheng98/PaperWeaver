from abc import ABCMeta, abstractmethod
import asyncio
import logging
from typing import AsyncIterator, Tuple
from .dataclass import Paper, Author, Venue, DataSrc, DataDst


class AuthorWeaverCache(metaclass=ABCMeta):
    @abstractmethod
    async def find_author_info_and_identifiers_by_identifiers(self, identifiers: set[str]) -> Tuple[set[str], dict | None]:
        raise NotImplementedError

    async def get_author_info(self, author: Author) -> Tuple[Author, dict | None]:
        identifiers, info = await self.find_author_info_and_identifiers_by_identifiers(author.identifiers)
        return Author(identifiers=identifiers.union(author.identifiers)), info

    @abstractmethod
    async def set_author_info(self, author: Author, info: dict) -> None:
        raise NotImplementedError

    @abstractmethod
    def iterate_authors(self) -> AsyncIterator[Author, None]:
        raise NotImplementedError


class AuthorWeaver:
    logger = logging.getLogger("AuthorWeaver")

    def __init__(self, src: DataSrc, dst: DataDst, cache: AuthorWeaverCache):
        self.src = src
        self.dst = dst
        self.cache = cache

    async def _load_author_info(self, author: Author) -> bool:
        author, info = await author.get_info(self.src)
        if info is None:
            return False
        await self.cache.set_author_info(author, info)
        return True

    async def _init_authors_info(self, author: Author) -> bool:
        # init author info for those identifiers without info
        tasks = []
        async for author in self.cache.iterate_authors():
            if await self.cache.get_author_info(author) is None:
                tasks.append(self._load_author_info(author))
        self.logger.info("Initializing %d authors " % len(tasks))
        success = await asyncio.gather(*tasks)
        succ_count = sum([1 for s in success if s])
        fail_count = sum([1 for s in success if not s])
        return succ_count, fail_count

    async def bfs_once(self):
        total_author_succ_count, total_author_fail_count = 0, 0

        # init author info for those identifiers but no info (failed in last loop)
        self.logger.info("Initializing authors failed in last loop")
        succ_count, fail_count = await self._init_authors_info()
        self.logger.info("%d authors failed in last loop initialized, %d still fetch failed" % (succ_count, fail_count))
        total_author_succ_count += succ_count
        total_author_fail_count += fail_count
