"""
DBLP Record/Paper XML parsing.

Handles parsing of:
1. Record items (article, inproceedings, etc.) - individual publication elements
2. Record pages (https://dblp.org/rec/xxx.xml) - pages containing a single record item

Example record page: https://dblp.org/rec/conf/cvpr/HeZRS16.xml
"""

import re
import xml.etree.ElementTree as ElementTree
from typing import Iterator
from urllib.parse import urlparse


def _url2doi(url: str) -> str | None:
    """Extract DOI from a URL."""
    u = urlparse(url)
    if u.netloc != "doi.org":
        return None
    return re.sub(r"^/+", "", u.path)


def _parse_xml(text: str) -> ElementTree.Element | None:
    """Parse XML text to ElementTree Element."""
    try:
        return ElementTree.fromstring(text)
    except ElementTree.ParseError:
        return None


class RecordAuthor:
    """Author information from a record item."""

    def __init__(self, data: ElementTree.Element):
        """
        Initialize RecordAuthor from author XML element.

        Args:
            data: Author XML element
        """
        assert data.tag == "author", "Should be xml of an author element!"
        self.data = data

    @property
    def name(self) -> str | None:
        """Get author name."""
        return self.data.text

    @property
    def pid(self) -> str | None:
        """Get DBLP person ID (only available from person pages)."""
        return self.data.attrib.get("pid")

    @property
    def orcid(self) -> str | None:
        """Get ORCID identifier."""
        return self.data.attrib.get("orcid")

    def __repr__(self) -> str:
        return f"RecordAuthor(name={self.name!r}, pid={self.pid!r})"

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        result = {}
        if self.name:
            result["name"] = self.name
        if self.pid:
            result["pid"] = self.pid
        if self.orcid:
            result["orcid"] = self.orcid
        return result


class RecordParser:
    """
    Parser for a record item (publication element).

    A record item is an individual publication element like <article>, 
    <inproceedings>, <proceedings>, etc.

    Initialized from an ElementTree.Element directly.
    Use RecordPageParser to parse from XML string.
    """

    def __init__(self, data: ElementTree.Element):
        """
        Initialize RecordParser from XML element.

        Args:
            data: Publication XML element (article, inproceedings, etc.)
        """
        self.data = data

    @property
    def key(self) -> str | None:
        """Get DBLP paper key."""
        return self.data.attrib.get("key")

    @property
    def type(self) -> str:
        """Get publication type (article, inproceedings, etc.)."""
        return self.data.tag

    @property
    def mdate(self) -> str | None:
        """Get modification date."""
        return self.data.attrib.get("mdate")

    @property
    def title(self) -> str | None:
        """Get publication title."""
        for child in self.data:
            if child.tag == "title":
                return " ".join(t for t in child.itertext())
        return None

    @property
    def year(self) -> int | None:
        """Get publication year."""
        for child in self.data:
            if child.tag == "year" and child.text:
                try:
                    return int(child.text)
                except ValueError:
                    return None
        return None

    @property
    def venue(self) -> str | None:
        """Get venue name (journal/booktitle/series)."""
        tag_map = {
            'inproceedings': 'booktitle',
            'proceedings': 'booktitle',
            'article': 'journal',
            'incollection': 'booktitle',
            'book': 'series'
        }
        target_tag = tag_map.get(self.data.tag)
        if target_tag:
            for child in self.data:
                if child.tag == target_tag:
                    return child.text
        return None

    @property
    def venue_type(self) -> str | None:
        """Get venue type (journal/booktitle/series)."""
        tag_map = {
            'inproceedings': 'booktitle',
            'proceedings': 'booktitle',
            'article': 'journal',
            'incollection': 'booktitle',
            'book': 'series'
        }
        return tag_map.get(self.data.tag)

    @property
    def doi(self) -> str | None:
        """Get DOI."""
        for ee in self._ee():
            doi = _url2doi(ee)
            if doi:
                return doi
        return None

    def _ee(self) -> Iterator[str]:
        """Iterate over ee (electronic edition) URLs."""
        for child in self.data:
            if child.tag == "ee" and child.text:
                yield child.text

    @property
    def pages(self) -> str | None:
        """Get pages."""
        for child in self.data:
            if child.tag == "pages" and child.text:
                return child.text
        return None

    @property
    def volume(self) -> str | None:
        """Get volume."""
        for child in self.data:
            if child.tag == "volume" and child.text:
                return child.text
        return None

    @property
    def number(self) -> str | None:
        """Get number."""
        for child in self.data:
            if child.tag == "number" and child.text:
                return child.text
        return None

    @property
    def authors(self) -> Iterator[RecordAuthor]:
        """Iterate over authors."""
        for child in self.data:
            if child.tag == "author":
                yield RecordAuthor(child)

    @property
    def author_names(self) -> list[str]:
        """Get list of author names."""
        return [a.name for a in self.authors if a.name]

    def __dict__(self) -> dict:
        """
        Convert to dictionary (excluding authors list).

        Returns:
            Dict with record info (key, type, title, year, venue, doi, etc.)
        """
        result = {}

        if self.key:
            result["dblp_key"] = self.key
        if self.type:
            result["type"] = self.type
        if self.mdate:
            result["mdate"] = self.mdate
        if self.title:
            result["title"] = self.title
        if self.year:
            result["year"] = self.year
        if self.venue:
            result["venue"] = self.venue
        if self.venue_type:
            result["venue_type"] = self.venue_type
        if self.doi:
            result["doi"] = self.doi
        if self.pages:
            result["pages"] = self.pages
        if self.volume:
            result["volume"] = self.volume
        if self.number:
            result["number"] = self.number

        return result

    def __repr__(self) -> str:
        return f"RecordParser(key={self.key!r}, title={self.title!r})"


class RecordPageParser(RecordParser):
    """
    Parser for a record page (https://dblp.org/rec/xxx.xml).

    A record page contains a <dblp> root with a single publication element inside.
    This class extracts the publication element and passes it to RecordParser.
    """

    def __init__(self, xml_text: str):
        """
        Initialize RecordPageParser from record page XML string.

        Args:
            xml_text: XML string from record page (https://dblp.org/rec/xxx.xml)

        Raises:
            ValueError: If XML is invalid or doesn't contain a publication
        """
        root = _parse_xml(xml_text)
        if root is None:
            raise ValueError("Invalid record page XML")

        if root.tag != "dblp":
            raise ValueError(f"Expected <dblp> root element, got <{root.tag}>")

        if len(root) == 0:
            raise ValueError("Record page contains no publication element")

        # Pass the publication element to parent
        super().__init__(root[0])
