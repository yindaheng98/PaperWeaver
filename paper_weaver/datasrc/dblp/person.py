"""
DBLP Person/Author page utilities.

Provides functions to convert PersonPageParser to Author and info dict.
"""

from ...dataclass import Author
from .parser import PersonPageParser


def person_to_author(person: PersonPageParser) -> Author:
    """
    Convert PersonPageParser to Author with identifiers.

    Identifiers extracted:
    - dblp-author:{pid} - DBLP person ID
    - dblp-author-name:{name} - Author name
    - {url} - Author URLs
    """
    identifiers = set()

    if person.pid:
        identifiers.add(f"dblp-author:{person.pid}")

    if person.name:
        identifiers.add(f"dblp-author-name:{person.name}")

    for url in person.urls:
        identifiers.add(url)

    return Author(identifiers=identifiers)


def person_to_info(person: PersonPageParser) -> dict:
    """
    Convert PersonPageParser to info dict.

    Keys with "dblp:" prefix (to avoid conflicts):
    - dblp:pid, dblp:uname

    Common keys (unlikely to conflict):
    - name, affiliations, urls
    """
    info = {}

    # Keys with dblp: prefix
    if person.pid:
        info["dblp:pid"] = person.pid
    if person.uname:
        info["dblp:uname"] = person.uname

    # Common keys
    if person.name:
        info["name"] = person.name
    affiliations = list(person.affiliations)
    if affiliations:
        info["affiliations"] = affiliations
    urls = list(person.urls)
    if urls:
        info["urls"] = urls

    return info
