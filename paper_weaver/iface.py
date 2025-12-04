from abc import ABCMeta, abstractmethod
import logging
from typing import Tuple, AsyncIterator
from .dataclass import Paper, Author, Venue, DataSrc, DataDst
from .iface_init import WeaverInitializerIface


class WeaverCacheIface(metaclass=ABCMeta):

    @abstractmethod
    async def get_author_info(self, author: Author) -> Tuple[Author, dict | None]:
        """return (updated author, info or None if not in cache)"""
        raise NotImplementedError

    @abstractmethod
    async def set_author_info(self, author: Author, info: dict) -> None:
        raise NotImplementedError

    @abstractmethod
    async def get_paper_info(self, paper: Paper) -> Tuple[Paper, dict | None]:
        """return (updated paper, info or None if not in cache)"""
        raise NotImplementedError

    @abstractmethod
    async def set_paper_info(self, paper: Paper, info: dict) -> None:
        raise NotImplementedError

    @abstractmethod
    async def get_venue_info(self, venue: Venue) -> Tuple[Venue, dict | None]:
        """return (updated venue, info or None if not in cache)"""
        raise NotImplementedError

    @abstractmethod
    async def set_venue_info(self, venue: Venue, info: dict) -> None:
        raise NotImplementedError

    @abstractmethod
    def iterate_authors(self) -> AsyncIterator[Author]:
        """Iterate over all registered authors."""
        raise NotImplementedError

    @abstractmethod
    def iterate_papers(self) -> AsyncIterator[Paper]:
        """Iterate over all registered papers."""
        raise NotImplementedError

    @abstractmethod
    def iterate_venues(self) -> AsyncIterator[Venue]:
        """Iterate over all registered venues."""
        raise NotImplementedError


class WeaverIface(metaclass=ABCMeta):

    @property
    @abstractmethod
    def src(self) -> DataSrc:
        raise ValueError("DataSrc is not set")

    @property
    @abstractmethod
    def dst(self) -> DataDst:
        raise ValueError("DataDst is not set")

    @property
    @abstractmethod
    def cache(self) -> WeaverCacheIface:
        raise ValueError("Cache is not set")

    @property
    @abstractmethod
    def initializer(self) -> WeaverInitializerIface:
        raise ValueError("Initializer is not set")

    @property
    def logger(self) -> logging.Logger:
        return logging.getLogger(self.__class__.__name__)

    @abstractmethod
    async def init(self) -> int:
        """Initialize the weaver before BFS starts. Return number of new entities fetched."""
        raise NotImplementedError

    @abstractmethod
    async def bfs_once(self) -> int:
        """Perform one BFS iteration, return number of new entities fetched."""
        raise NotImplementedError

    async def bfs(self, max_iterations: int = 10) -> int:
        """Perform BFS for a number of iterations, return total number of new entities fetched."""
        total_new = await self.init()
        self.logger.info(f"Initialization completed with {total_new} new entities fetched.")
        for iteration in range(max_iterations):
            self.logger.info(f"Starting BFS iteration {iteration + 1}")
            new_count = await self.bfs_once()
            if new_count == 0:
                self.logger.info("No new entities fetched, stopping BFS.")
                break
            total_new += new_count
        self.logger.info(f"BFS completed with total {total_new} new entities fetched.")
        return total_new


class SimpleWeaver(WeaverIface):
    """A simple WeaverIface implementation with given src, dst, cache, initializer."""

    def __init__(
        self,
        src: DataSrc,
        dst: DataDst,
        cache: WeaverCacheIface,
        initializer: WeaverInitializerIface
    ):
        self._src = src
        self._dst = dst
        self._cache = cache
        self._initializer = initializer

    @property
    def src(self) -> DataSrc:
        return self._src

    @property
    def dst(self) -> DataDst:
        return self._dst

    @property
    def cache(self) -> WeaverCacheIface:
        return self._cache

    @property
    def initializer(self) -> WeaverInitializerIface:
        return self._initializer
