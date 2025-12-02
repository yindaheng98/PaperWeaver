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
        for child in self.data:
            if child.tag == "person":
                return child
        return None

    @property
    def affiliations(self) -> Iterator[str]:
        """Iterate over affiliations."""
        person = self._person_element
        if person is not None:
            for note in person:
                if note.tag == "note" and note.attrib.get("type") == "affiliation" and note.text:
                    yield note.text

    @property
    def uname(self) -> str | None:
        person = self._person_element
        if person is not None:
            for note in person:
                if note.tag == "note" and note.attrib.get("type") == "uname" and note.text:
                    return note.text

    @property
    def urls(self) -> Iterator[str]:
        person = self._person_element
        if person is not None:
            for note in person:
                if note.tag == "url" and note.text:
                    yield note.text

    @property
    def publications(self) -> Iterator[RecordParser]:
        """
        Iterate over publications.

        Each publication is a RecordParser with full author information
        including pid attributes.
        """
        for child in self.data:
            if child.tag == "r" and len(child) > 0:
                yield RecordParser(child[0])

    def __dict__(self) -> dict:
        """
        Convert to dictionary (excluding publications list).

        Returns:
            Dict with person info (pid, name, affiliations)
        """
        result = {}

        if self.pid:
            result["pid"] = self.pid
        if self.name:
            result["name"] = self.name

        affiliations = list(self.affiliations)
        if affiliations:
            result["affiliations"] = affiliations

        uname = self.uname
        if uname:
            result["uname"] = uname

        urls = list(self.urls)
        if urls:
            result["urls"] = urls

        return result

    def __repr__(self) -> str:
        return f"PersonPageParser(pid={self.pid!r}, name={self.name!r})"
