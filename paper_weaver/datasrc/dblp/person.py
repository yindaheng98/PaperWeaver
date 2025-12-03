"""
DBLP Person/Author page utilities.

Provides functions to convert PersonPageParser to Author and info dict,
and extract DBLP identifiers from Author objects.
"""

from ...dataclass import Author
from .parser import PersonPageParser


def author_to_dblp_pid(author: Author) -> str | None:
    """
    Extract DBLP person ID from Author identifiers.

    Args:
        author: Author object with identifiers

    Returns:
        DBLP person ID (e.g., "h/KaimingHe") or None if not found
    """
    for ident in author.identifiers:
        if ident.startswith("author:dblp:pid:"):
            return ident[16:]  # Remove "author:dblp:pid:" prefix
    return None


def person_page_to_author(person: PersonPageParser) -> Author:
    """
    Convert PersonPageParser to Author with identifiers.

    Identifiers extracted (format: author:{info_key}:{value}):
    - author:dblp:pid:{pid} - DBLP person ID (matches info["dblp:pid"])
    - author:name:{name} - Author name (matches info["name"])
    - author:orcid:{orcid} - ORCID (matches info["orcid"])
    - {url} - Author URLs
    """
    identifiers = set()

    if person.pid:
        identifiers.add(f"author:dblp:pid:{person.pid}")

    if person.name:
        identifiers.add(f"author:name:{person.name}")

    if person.orcid:
        identifiers.add(f"author:orcid:{person.orcid}")

    for url in person.urls:
        identifiers.add(url)

    return Author(identifiers=identifiers)


def person_page_to_info(person: PersonPageParser) -> dict:
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
    if person.orcid:
        info["orcid"] = person.orcid

    return info
