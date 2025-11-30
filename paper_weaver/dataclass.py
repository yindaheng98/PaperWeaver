from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from typing import Tuple


@dataclass
class Paper:
    identifiers: set[str]  # example: ["doi:10.1000/xyz123"]

    async def get_info(self, src: "DataSrc") -> Tuple["Paper", dict]:
        identifiers, info = await src.get_paper_info(self)
        self.identifiers.update(identifiers=self.identifiers.union(identifiers))
        return self, info

    async def get_authors(self, src: "DataSrc") -> list["Author"]:
        return await src.get_authors_by_paper(self)

    async def get_venues(self, src: "DataSrc") -> list["Venue"]:
        return await src.get_venues_by_paper(self)

    async def get_references(self, src: "DataSrc") -> list["Paper"]:
        return await src.get_references_by_paper(self)

    async def get_citations(self, src: "DataSrc") -> list["Paper"]:
        return await src.get_citations_by_paper(self)


@dataclass
class Author:
    identifiers: set[str]  # example: ["orcid:0000-0001-2345-6789"]

    async def get_info(self, src: "DataSrc") -> Tuple["Author", dict]:
        identifiers, info = await src.get_author_info(self)
        self.identifiers.update(identifiers=self.identifiers.union(identifiers))
        return self, info

    async def get_papers(self, src: "DataSrc") -> list[Paper]:
        return await src.get_papers_by_author(self)


@dataclass
class Venue:
    identifiers: set[str]  # example: ["issn:1234-5678"]

    async def get_info(self, src: "DataSrc") -> Tuple["Venue", dict]:
        identifiers, info = await src.get_venue_info(self)
        self.identifiers.update(identifiers=self.identifiers.union(identifiers))
        return self, info

    async def get_papers(self, src: "DataSrc") -> list[Paper]:
        return await src.get_papers_by_venue(self)


class DataSrc(metaclass=ABCMeta):
    @abstractmethod
    async def get_paper_info(self, paper: Paper) -> Tuple[set[str], dict]:
        """return (identifiers, info)"""
        raise NotImplementedError

    @abstractmethod
    async def get_authors_by_paper(self, paper: Paper) -> list[Author]:
        raise NotImplementedError

    @abstractmethod
    async def get_venues_by_paper(self, paper: Paper) -> list[Venue]:
        raise NotImplementedError

    @abstractmethod
    async def get_references_by_paper(self, paper: Paper) -> list[Paper]:
        raise NotImplementedError

    @abstractmethod
    async def get_citations_by_paper(self, paper: Paper) -> list[Paper]:
        raise NotImplementedError

    @abstractmethod
    async def get_author_info(self, author: Author) -> Tuple[set[str], dict]:
        """return (identifiers, info)"""
        raise NotImplementedError

    @abstractmethod
    async def get_papers_by_author(self, author: Author) -> list[Paper]:
        raise NotImplementedError

    @abstractmethod
    async def get_venue_info(self, venue: Venue) -> Tuple[set[str], dict]:
        """return (identifiers, info)"""
        raise NotImplementedError

    @abstractmethod
    async def get_papers_by_venue(self, venue: Venue) -> list[Paper]:
        raise NotImplementedError


class DataDst(metaclass=ABCMeta):
    @abstractmethod
    async def save_paper_info(self, paper: Paper, info: dict) -> None:
        raise NotImplementedError

    @abstractmethod
    async def link_venue(self, paper: Paper, venue: Venue) -> None:
        raise NotImplementedError

    @abstractmethod
    async def link_author(self, paper: Paper, author: Author) -> None:
        raise NotImplementedError

    @abstractmethod
    async def link_citation(self, paper: Paper, citation: Paper) -> None:
        raise NotImplementedError

    @abstractmethod
    async def link_reference(self, paper: Paper, reference: Paper) -> None:
        raise NotImplementedError

    @abstractmethod
    async def save_author_info(self, author: Author, info: dict) -> None:
        raise NotImplementedError

    async def link_paper_to_author(self, author: Author, paper: Paper) -> None:
        return await self.link_author(paper, author)

    @abstractmethod
    async def save_venue_info(self, venue: Venue, info: dict) -> None:
        raise NotImplementedError

    async def link_paper_to_venue(self, venue: Venue, paper: Paper) -> None:
        return await self.link_venue(paper, venue)
