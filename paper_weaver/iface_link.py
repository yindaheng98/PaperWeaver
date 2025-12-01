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


class PaperLinkWeaverCacheIface(WeaverCacheIface, metaclass=ABCMeta):

    @abstractmethod
    async def is_link_reference(self, paper: Paper, reference: Paper) -> bool:
        raise NotImplementedError

    @abstractmethod
    async def link_reference(self, paper: Paper, reference: Paper) -> None:
        raise NotImplementedError

    async def is_link_citation(self, paper: Paper, citation: Paper) -> bool:
        return await self.is_link_reference(citation, paper)

    async def link_citation(self, paper: Paper, citation: Paper) -> None:
        return await self.link_reference(citation, paper)
