"""
DBLP Venue/Index page XML parsing.

Handles parsing of venue index pages: https://dblp.org/db/xxx/index.xml

Venue index pages contain:
- Venue title
- Volume references (sub-pages for different years/volumes)
- Papers directly listed in that volume

Example: https://dblp.org/db/conf/cvpr/cvpr2016.xml
"""

import re
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
    def title(self) -> str | None:
        """Get venue title."""
        return self.data.attrib.get("title")

    @property
    def volume_refs(self) -> Iterator[str]:
        """
        Iterate over volume reference keys.

        These are paths to sub-pages (e.g., individual years/volumes).
        """
        for ref in self.data.findall(".//ref"):
            href = ref.attrib.get("href", "")
            if href:
                # Remove .html extension
                vol_key = re.sub(r"\.html$", "", href)
                yield vol_key

    # TODO: more information in ".//dblpcites/r/proceedings" elements

    @property
    def publications(self) -> Iterator[RecordParser]:
        """
        Iterate over publications directly listed in this venue page.

        Note: Author information in venue pages typically does not include
        pid attributes.
        """
        for r_elem in self.data.findall(".//dblpcites/r"):
            if len(r_elem) > 0:
                yield RecordParser(r_elem[0])

    # TODO: more information in ".//dblpcites/r/inproceedings" is papers, but ".//dblpcites/r/proceedings" is proceedings info

    def __dict__(self) -> dict:
        """
        Convert to dictionary (excluding publications list).

        Returns:
            Dict with venue info (title, volume_refs)
        """
        result = {}

        if self.title:
            result["title"] = self.title

        volume_refs = list(self.volume_refs)
        if volume_refs:
            result["volume_refs"] = volume_refs

        return result

    def __repr__(self) -> str:
        return f"VenuePageParser(title={self.title!r})"
