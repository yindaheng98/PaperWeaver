"""
DBLP Venue/Index page XML parsing.

Handles parsing of venue index pages: https://dblp.org/db/xxx/index.xml

Venue index pages contain:
- Venue title
- Volume references (sub-pages for different years/volumes)
- Papers directly listed in that volume

Example: https://dblp.org/db/conf/cvpr/cvpr2016.xml
"""

import xml.etree.ElementTree as ElementTree
from typing import Iterator

from .record import RecordParser, _parse_xml


class VenuePageParser:
    """
    Parser for a venue index page (https://dblp.org/db/xxx/index.xml).

    A venue page contains venue information and either:
    - Volume references to sub-pages (for top-level venue pages)
    - Direct paper listings (for specific volume pages)
    """

    def __init__(self, xml_text: str):
        """
        Parse venue index page XML.

        Args:
            xml_text: XML text from venue index page

        Raises:
            ValueError: If XML is invalid
        """
        self.data = _parse_xml(xml_text)

        if self.data is None:
            raise ValueError("Invalid venue page XML")

        if self.data.tag != "bht":
            raise ValueError(f"Expected <bht> root element, got <{self.data.tag}>")

    @property
    def key(self) -> str | None:
        """Get venue title."""
        key = self.data.attrib.get("key")
        if key is not None and key.endswith(".bht"):
            return key[:-4]  # Remove .bht suffix

    @property
    def title(self) -> str | None:
        """Get venue title."""
        return self.data.attrib.get("title")

    @property
    def href(self) -> str | None:
        """
        Get volume reference keys.
        """
        for ref in self.data.findall(".//ref"):
            href = ref.attrib.get("href", "")
            if href:
                return href

    @property
    def ref(self) -> str | None:
        """
        Get volume reference name.
        """
        for ref in self.data.findall(".//ref"):
            return ref.text

    @property
    def h2(self) -> str | None:
        """
        Get h2.
        """
        for h in self.data.findall(".//h2"):
            return h.text

    @property
    def h3(self) -> str | None:
        """
        Get h3.
        """
        for h in self.data.findall(".//h3"):
            return h.text

    @property
    def _proceedings_element(self) -> ElementTree.Element | None:
        """Get the proceedings element."""
        for proceedings in self.data.findall(".//dblpcites/r/proceedings"):
            return proceedings

    @property
    def proceedings_title(self) -> str | None:
        """Get proceedings title."""
        proceedings = self._proceedings_element
        if proceedings is not None:
            for title in proceedings.findall("title"):
                return title.text

    @property
    def proceedings_booktitle(self) -> str | None:
        """Get proceedings booktitle."""
        proceedings = self._proceedings_element
        if proceedings is not None:
            for booktitle in proceedings.findall("booktitle"):
                return booktitle.text

    @property
    def proceedings_publisher(self) -> str | None:
        """Get proceedings publisher."""
        proceedings = self._proceedings_element
        if proceedings is not None:
            for publisher in proceedings.findall("publisher"):
                return publisher.text

    @property
    def proceedings_isbn(self) -> str | None:
        """Get proceedings ISBN."""
        proceedings = self._proceedings_element
        if proceedings is not None:
            for isbn in proceedings.findall("isbn"):
                return isbn.text

    @property
    def proceedings_url(self) -> str | None:
        """Get proceedings URL."""
        proceedings = self._proceedings_element
        if proceedings is not None:
            for url in proceedings.findall("url"):
                return url.text

    @property
    def proceedings_year(self) -> int | None:
        """Get proceedings year."""
        proceedings = self._proceedings_element
        if proceedings is not None:
            for year in proceedings.findall("year"):
                if year.text:
                    try:
                        return int(year.text)
                    except ValueError:
                        return None

    @property
    def proceedings_ees(self) -> Iterator[str]:
        """Iterate over proceedings ees (electronic editions)."""
        proceedings = self._proceedings_element
        if proceedings is not None:
            for ee in proceedings.findall("ee"):
                if ee.text:
                    yield ee.text

    @property
    def publications(self) -> Iterator[RecordParser]:
        """
        Iterate over publications directly listed in this venue page.

        Note: Author information in venue pages includes pid attributes,
        unlike individual record pages.
        """
        for r_elem in self.data.findall(".//dblpcites/r"):
            if len(r_elem) > 0:
                yield RecordParser(r_elem[0])
