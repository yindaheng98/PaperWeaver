from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from typing import Tuple


@dataclass
class Paper:
    identifiers: set[str]  # example: ["doi:10.1000/xyz123"]

    def __repr__(self) -> str:
        ids = sorted(self.identifiers)[:2]
        suffix = ", ..." if len(self.identifiers) > 2 else ""
        return f"Paper({', '.join(ids)}{suffix})"

    async def get_info(self, src: "DataSrc") -> Tuple["Paper", dict | None]:
        paper, info = await src.get_paper_info_no_exception(self)
        if info is None:
            return self, None
        self.identifiers.update(paper.identifiers)
        return self, info

    async def get_authors(self, src: "DataSrc") -> list["Author"] | None:
        return await src.get_authors_by_paper_no_exception(self)

    async def get_venues(self, src: "DataSrc") -> list["Venue"] | None:
        return await src.get_venues_by_paper_no_exception(self)

    async def get_references(self, src: "DataSrc") -> list["Paper"] | None:
        return await src.get_references_by_paper_no_exception(self)

    async def get_citations(self, src: "DataSrc") -> list["Paper"] | None:
        return await src.get_citations_by_paper_no_exception(self)


@dataclass
class Author:
    identifiers: set[str]  # example: ["orcid:0000-0001-2345-6789"]

    def __repr__(self) -> str:
        ids = sorted(self.identifiers)[:2]
        suffix = ", ..." if len(self.identifiers) > 2 else ""
        return f"Author({', '.join(ids)}{suffix})"

    async def get_info(self, src: "DataSrc") -> Tuple["Author", dict | None]:
        author, info = await src.get_author_info_no_exception(self)
        if info is None:
            return self, None
        self.identifiers.update(author.identifiers)
        return self, info

    async def get_papers(self, src: "DataSrc") -> list[Paper] | None:
        return await src.get_papers_by_author_no_exception(self)


@dataclass
class Venue:
    identifiers: set[str]  # example: ["issn:1234-5678"]

    def __repr__(self) -> str:
        ids = sorted(self.identifiers)[:2]
        suffix = ", ..." if len(self.identifiers) > 2 else ""
        return f"Venue({', '.join(ids)}{suffix})"

    async def get_info(self, src: "DataSrc") -> Tuple["Venue", dict | None]:
        venue, info = await src.get_venue_info_no_exception(self)
        if info is None:
            return self, None
        self.identifiers.update(venue.identifiers)
        return self, info

    async def get_papers(self, src: "DataSrc") -> list[Paper] | None:
        return await src.get_papers_by_venue_no_exception(self)


class DataSrc(metaclass=ABCMeta):

    # Paper related methods

    @abstractmethod
    async def get_paper_info(self, paper: Paper) -> Tuple[Paper, dict]:
        """return (updated paper, info)"""
        raise NotImplementedError

    async def get_paper_info_no_exception(self, paper: Paper) -> Tuple[Paper, dict | None]:
        """return (updated paper, None) if exception occurs"""
        try:
            return await self.get_paper_info(paper)
        except Exception:
            return paper, None

    @abstractmethod
    async def get_authors_by_paper(self, paper: Paper) -> list[Author]:
        raise NotImplementedError

    async def get_authors_by_paper_no_exception(self, paper: Paper) -> list[Author] | None:
        """return None if exception occurs"""
        try:
            return await self.get_authors_by_paper(paper)
        except Exception:
            return None

    @abstractmethod
    async def get_venues_by_paper(self, paper: Paper) -> list[Venue]:
        raise NotImplementedError

    async def get_venues_by_paper_no_exception(self, paper: Paper) -> list[Venue] | None:
        """return None if exception occurs"""
        try:
            return await self.get_venues_by_paper(paper)
        except Exception:
            return None

    @abstractmethod
    async def get_references_by_paper(self, paper: Paper) -> list[Paper]:
        raise NotImplementedError

    async def get_references_by_paper_no_exception(self, paper: Paper) -> list[Paper] | None:
        """return None if exception occurs"""
        try:
            return await self.get_references_by_paper(paper)
        except Exception:
            return None

    @abstractmethod
    async def get_citations_by_paper(self, paper: Paper) -> list[Paper]:
        raise NotImplementedError

    async def get_citations_by_paper_no_exception(self, paper: Paper) -> list[Paper] | None:
        """return None if exception occurs"""
        try:
            return await self.get_citations_by_paper(paper)
        except Exception:
            return None

    # Author related methods

    @abstractmethod
    async def get_author_info(self, author: Author) -> Tuple[Author, dict]:
        """return (updated author, info)"""
        raise NotImplementedError

    async def get_author_info_no_exception(self, author: Author) -> Tuple[Author, dict | None]:
        """return (updated author, None) if exception occurs"""
        try:
            return await self.get_author_info(author)
        except Exception:
            return author, None

    @abstractmethod
    async def get_papers_by_author(self, author: Author) -> list[Paper]:
        raise NotImplementedError

    async def get_papers_by_author_no_exception(self, author: Author) -> list[Paper] | None:
        """return None if exception occurs"""
        try:
            return await self.get_papers_by_author(author)
        except Exception:
            return None

    # Venue related methods

    @abstractmethod
    async def get_venue_info(self, venue: Venue) -> Tuple[Venue, dict]:
        """return (updated venue, info)"""
        raise NotImplementedError

    async def get_venue_info_no_exception(self, venue: Venue) -> Tuple[Venue, dict | None]:
        """return (updated venue, None) if exception occurs"""
        try:
            return await self.get_venue_info(venue)
        except Exception:
            return venue, None

    @abstractmethod
    async def get_papers_by_venue(self, venue: Venue) -> list[Paper]:
        raise NotImplementedError

    async def get_papers_by_venue_no_exception(self, venue: Venue) -> list[Paper] | None:
        """return None if exception occurs"""
        try:
            return await self.get_papers_by_venue(venue)
        except Exception:
            return None


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
