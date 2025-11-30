from abc import ABCMeta, abstractmethod
from dataclasses import dataclass


@dataclass
class Paper:
    id: str
    title: str
    identifiers: dict[str, str]  # example: {"doi": "10.1000/xyz123"}

    async def get_authors(self, src: "DataSrc") -> list["Author"]:
        return await src.get_authors_by_paper(self)

    async def get_venue(self, src: "DataSrc") -> list["Venue"]:
        return await src.get_venue_by_paper(self)

    async def get_references(self, src: "DataSrc") -> list["Paper"]:
        return await src.get_references_by_paper(self)

    async def get_citations(self, src: "DataSrc") -> list["Paper"]:
        return await src.get_citations_by_paper(self)


@dataclass
class Author:
    id: str
    name: str
    identifiers: dict[str, str]  # example: {"orcid": "0000-0001-2345-6789"}

    async def get_papers(self, src: "DataSrc") -> list[Paper]:
        return await src.get_papers_by_author(self)


@dataclass
class Venue:
    id: str
    name: str
    identifiers: dict[str, str]  # example: {"issn": "1234-5678"}

    async def get_papers(self, src: "DataSrc") -> list[Paper]:
        return await src.get_papers_by_venue(self)


class DataSrc(metaclass=ABCMeta):
    @abstractmethod
    async def get_papers_by_author(self, author: Author) -> list[Paper]:
        raise NotImplementedError

    @abstractmethod
    async def get_papers_by_venue(self, venue: Venue) -> list[Paper]:
        raise NotImplementedError

    @abstractmethod
    async def get_authors_by_paper(self, paper: Paper) -> list[Author]:
        raise NotImplementedError

    @abstractmethod
    async def get_venue_by_paper(self, paper: Paper) -> list[Venue]:
        raise NotImplementedError

    @abstractmethod
    async def get_references_by_paper(self, paper: Paper) -> list[Paper]:
        raise NotImplementedError

    @abstractmethod
    async def get_citations_by_paper(self, paper: Paper) -> list[Paper]:
        raise NotImplementedError
