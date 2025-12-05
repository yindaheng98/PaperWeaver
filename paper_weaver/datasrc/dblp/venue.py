"""
DBLP Venue/Index page utilities.

Provides functions to convert VenuePageParser to Venue and info dict,
and extract DBLP identifiers from Venue objects.
"""

from ...dataclass import Paper, Venue
from ..title_hash import title_hash
from dblp_webxml_parser import VenuePageParser


def venue_to_dblp_key(venue: Venue) -> str | None:
    """
    Extract DBLP venue key from Venue identifiers.

    Args:
        venue: Venue object with identifiers

    Returns:
        DBLP venue key (e.g., "conf/cvpr/cvpr2016") or None if not found
    """
    for ident in venue.identifiers:
        if ident.startswith("dblp:key:"):
            return ident[9:]  # Remove "dblp:key:" prefix
    return None


def venue_key_from_paper(paper: Paper, info: dict) -> str | None:
    """
    Extract venue key from paper URL.

    Tries dblp:url from info first, then dblp:url: from paper identifiers.
    URL format: db/conf/cvpr/cvpr2016.html#HeZRS16
    Returns: db/conf/cvpr/cvpr2016 (for fetching https://dblp.org/db/conf/cvpr/cvpr2016.xml)
    """
    # Collect candidate URLs
    urls = []
    if dblp_url := info.get("dblp:url"):
        urls.append(dblp_url)
    for ident in paper.identifiers:
        if ident.startswith("dblp:url:"):
            urls.append(ident[9:])

    # Try each URL path (format: db/conf/cvpr/cvpr2016.html#HeZRS16)
    for url in urls:
        # Remove fragment (#HeZRS16)
        path = url.split("#")[0]
        # Remove .html extension
        if path.endswith(".html"):
            path = path[:-5]
        if path:
            return path

    return None


def venue_page_to_venue(parser: VenuePageParser) -> Venue:
    """
    Convert VenuePageParser to Venue with identifiers.

    Identifiers extracted:
    - dblp:key:{key} - DBLP venue key (matches info["dblp:key"])
    - title:{title} - Venue title (matches info["title"])
    - proceedings_title:{proceedings_title} - Proceedings title (matches info["proceedings_title"])
    - {ee} - All proceedings ee URLs as identifiers
    """
    identifiers = set()

    if parser.key:
        identifiers.add(f"dblp:key:{parser.key}")

    if parser.title:
        identifiers.add(f"title:{parser.title}")
        for method, h in title_hash(parser.title).items():
            identifiers.add(f"title_hash:{h}")

    if parser.proceedings_title:
        identifiers.add(f"proceedings_title:{parser.proceedings_title}")
        identifiers.add(f"title:{parser.proceedings_title}")
        for method, h in title_hash(parser.proceedings_title).items():
            identifiers.add(f"proceedings_title_hash:{h}")
            identifiers.add(f"title_hash:{h}")

    for ee in parser.proceedings_ees:
        identifiers.add(ee)

    return Venue(identifiers=identifiers)


def venue_page_to_info(parser: VenuePageParser) -> dict:
    """
    Convert VenuePageParser to info dict.

    Keys with "dblp:" prefix (to avoid conflicts):
    - dblp:key, dblp:href, dblp:ref, dblp:h2, dblp:h3, dblp:proceedings_url

    Common keys (unlikely to conflict):
    - title, proceedings_title, proceedings_booktitle, proceedings_publisher,
      proceedings_isbn, proceedings_year, proceedings_ees
    """
    info = {}

    # Keys with dblp: prefix
    if parser.key:
        info["dblp:key"] = parser.key
    if parser.href:
        info["dblp:href"] = parser.href
    if parser.ref:
        info["dblp:ref"] = parser.ref
    if parser.h2:
        info["dblp:h2"] = parser.h2
    if parser.h3:
        info["dblp:h3"] = parser.h3
    if parser.proceedings_url:
        info["dblp:proceedings_url"] = parser.proceedings_url

    # Common keys
    if parser.title:
        info["title"] = parser.title
    if parser.proceedings_title:
        info["proceedings_title"] = parser.proceedings_title
    if parser.proceedings_booktitle:
        info["proceedings_booktitle"] = parser.proceedings_booktitle
    if parser.proceedings_publisher:
        info["proceedings_publisher"] = parser.proceedings_publisher
    if parser.proceedings_isbn:
        info["proceedings_isbn"] = parser.proceedings_isbn
    if parser.proceedings_year:
        info["proceedings_year"] = parser.proceedings_year
    ees = list(parser.proceedings_ees)
    if ees:
        info["urls"] = ees

    return info
