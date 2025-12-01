from abc import ABCMeta, abstractmethod
from typing import Tuple, AsyncIterator
from .dataclass import Paper, Author, DataSrc, DataDst


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
    def iterate_authors(self) -> AsyncIterator[Author]:
        """Iterate over all registered authors."""
        raise NotImplementedError

    @abstractmethod
    def iterate_papers(self) -> AsyncIterator[Paper]:
        """Iterate over all registered papers."""
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


class SimpleWeaver(WeaverIface):
    """A simple WeaverIface implementation with given src, dst, cache."""

    def __init__(self, src: DataSrc, dst: DataDst, cache: WeaverCacheIface):
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
    def cache(self) -> WeaverCacheIface:
        return self._cache
