from abc import ABCMeta, abstractmethod
from .dataclass import Paper, Author
from .iface import WeaverCacheIface


class AuthorLinkWeaverCacheIface(WeaverCacheIface, metaclass=ABCMeta):

    @abstractmethod
    async def is_link_author(self, paper: Paper, author: Author) -> bool:
        raise NotImplementedError

    @abstractmethod
    async def link_author(self, paper: Paper, author: Author) -> None:
        raise NotImplementedError
