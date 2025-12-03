"""
DBLP Person page XML parsing.

Handles parsing of person pages: https://dblp.org/pid/xxx.xml

Person pages contain:
- Author info (name, affiliations)
- List of publications WITH author pid attributes

Example: https://dblp.org/pid/34/7659.xml
"""

import xml.etree.ElementTree as ElementTree
from typing import Iterator

from .record import RecordParser, _parse_xml


class PersonPageParser:
    """
    Parser for a person page (https://dblp.org/pid/xxx.xml).

    A person page contains author information and their publication list.
    Publications in person pages include author pid attributes, unlike record pages.
    """

    def __init__(self, xml_text: str):
        """
        Parse person page XML.

        Args:
            xml_text: XML text from person page

        Raises:
            ValueError: If XML is invalid
        """
        self.data = _parse_xml(xml_text)

        if self.data is None:
            raise ValueError("Invalid person page XML")

        if self.data.tag != "dblpperson":
            raise ValueError(f"Expected <dblpperson> root element, got <{self.data.tag}>")

    @property
    def pid(self) -> str | None:
        """Get DBLP person ID."""
        return self.data.attrib.get("pid")

    @property
    def name(self) -> str | None:
        """Get person name."""
        return self.data.attrib.get("name")

    @property
    def _person_element(self) -> ElementTree.Element | None:
        """Get the person element."""
        for person in self.data.findall("person"):
            return person

    @property
    def uname(self) -> str | None:
        """Get username."""
        person = self._person_element
        if person is not None:
            for note in person.findall("note[@type='uname']"):
                if note.text:
                    return note.text

    @property
    def affiliations(self) -> Iterator[str]:
        """Iterate over affiliations."""
        person = self._person_element
        if person is not None:
            for note in person.findall("note[@type='affiliation']"):
                if note.text:
                    yield note.text

    @property
    def urls(self) -> Iterator[str]:
        """Iterate over URLs."""
        person = self._person_element
        if person is not None:
            for url in person.findall("url"):
                if url.text:
                    yield url.text

    @property
    def orcid(self) -> str | None:
        """Get ORCID."""
        for url in self.urls:
            if url.startswith("https://orcid.org/"):
                return url[19:]  # Remove prefix

    @property
    def publications(self) -> Iterator[RecordParser]:
        """
        Iterate over publications.

        Each publication is a RecordParser with full author information
        including pid attributes.
        """
        for r_elem in self.data.findall("r"):
            if len(r_elem) > 0:
                yield RecordParser(r_elem[0])
