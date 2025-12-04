from abc import ABCMeta, abstractmethod
from typing import AsyncIterator, Union
from .dataclass import Paper, Author, Venue


class PapersWeaverInitializerIface(metaclass=ABCMeta):
    @abstractmethod
    def fetch_papers(self) -> AsyncIterator[Paper]:
        """Fetch initial papers to seed the weaver."""
        raise NotImplementedError


class AuthorsWeaverInitializerIface(metaclass=ABCMeta):
    @abstractmethod
    def fetch_authors(self) -> AsyncIterator[Author]:
        """Fetch initial authors to seed the weaver."""
        raise NotImplementedError


class VenuesWeaverInitializerIface(metaclass=ABCMeta):
    @abstractmethod
    def fetch_venues(self) -> AsyncIterator[Venue]:
        """Fetch initial venues to seed the weaver."""
        raise NotImplementedError


# Unified type alias for all initializer interfaces
WeaverInitializerIface = Union[
    PapersWeaverInitializerIface,
    AuthorsWeaverInitializerIface,
    VenuesWeaverInitializerIface,
]
