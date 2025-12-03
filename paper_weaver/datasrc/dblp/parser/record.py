"""
DBLP Record/Paper XML parsing.

Handles parsing of:
1. Record items (article, inproceedings, etc.) - individual publication elements
2. Record pages (https://dblp.org/rec/xxx.xml) - pages containing a single record item

Example record page: https://dblp.org/rec/conf/cvpr/HeZRS16.xml
"""

import xml.etree.ElementTree as ElementTree
from typing import Iterator


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
        """Get ORCID (if available)."""
        return self.data.attrib.get("orcid")

    def __dict__(self) -> dict:
        """
        Convert to dictionary.

        Returns:
            Dict with author info (name, pid)
        """
        result = {}
        if self.name:
            result["name"] = self.name
        if self.pid:
            result["pid"] = self.pid
        if self.orcid:
            result["orcid"] = self.orcid
        return result

    def __repr__(self) -> str:
        return f"RecordAuthor(name={self.name!r}, pid={self.pid!r})"


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
        for title in self.data.findall("title"):
            return " ".join(t for t in title.itertext())

    @property
    def pages(self) -> str | None:
        """Get pages."""
        for pages in self.data.findall("pages"):
            if pages.text:
                return pages.text

    @property
    def year(self) -> int | None:
        """Get publication year."""
        for year in self.data.findall("year"):
            if year.text:
                try:
                    return int(year.text)
                except ValueError:
                    return None

    @property
    def month(self) -> int | None:
        """Get publication month."""
        for year in self.data.findall("month"):
            if year.text:
                try:
                    return int(year.text)
                except ValueError:
                    return None

    @property
    def volume(self) -> str | None:
        """Get volume."""
        for volume in self.data.findall("volume"):
            if volume.text:
                return volume.text

    @property
    def series(self) -> str | None:
        """Get series."""
        for series in self.data.findall("series"):
            if series.text:
                return series.text

    @property
    def booktitle(self) -> str | None:
        """Get booktitle."""
        for booktitle in self.data.findall("booktitle"):
            if booktitle.text:
                return booktitle.text

    @property
    def journal(self) -> str | None:
        """Get journal."""
        for journal in self.data.findall("journal"):
            if journal.text:
                return journal.text

    @property
    def number(self) -> str | None:
        """Get number."""
        for number in self.data.findall("number"):
            if number.text:
                return number.text

    @property
    def ees(self) -> Iterator[str]:
        """Iterate over ee (electronic edition) URLs."""
        for ee in self.data.findall("ee"):
            if ee.text:
                yield ee.text

    @property
    def crossref(self) -> str | None:
        """Get crossref."""
        for crossref in self.data.findall("crossref"):
            if crossref.text:
                return crossref.text

    @property
    def url(self) -> str | None:
        """Get url."""
        for url in self.data.findall("url"):
            if url.text:
                return url.text

    @property
    def stream(self) -> str | None:
        """Get stream."""
        for url in self.data.findall("stream"):
            if url.text:
                return url.text

    @property
    def venue(self) -> str | None:
        """Get venue name (journal/booktitle/series)."""
        if self.type in ["article"]:
            return self.journal
        if self.type in ["proceedings", "inproceedings", "incollection"]:
            return self.booktitle
        if self.type in ["book"]:
            return self.series

    @property
    def venue_type(self) -> str | None:
        """Get venue type (journal/proceedings/book)."""
        if self.type in ["article"]:
            return "journal"
        if self.type in ["proceedings", "inproceedings", "incollection"]:
            return "proceedings"
        if self.type in ["book"]:
            return "book"

    @property
    def authors(self) -> Iterator[RecordAuthor]:
        """Iterate over authors."""
        for author in self.data.findall("author"):
            yield RecordAuthor(author)


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
