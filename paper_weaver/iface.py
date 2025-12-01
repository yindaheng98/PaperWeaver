from abc import ABCMeta, abstractmethod
from typing import Tuple
from .dataclass import Paper, Author, DataSrc, DataDst


class WeaverIface(metaclass=ABCMeta):

    @property
    @abstractmethod
    def src(self) -> DataSrc:
        raise ValueError("Model is not set")

    @property
    @abstractmethod
    def dst(self) -> DataDst:
        raise ValueError("Model is not set")


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
    async def is_link_author(self, paper: Paper, author: Author) -> bool:
        raise NotImplementedError

    @abstractmethod
    async def link_author(self, paper: Paper, author: Author) -> None:
        raise NotImplementedError
