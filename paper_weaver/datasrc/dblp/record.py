"""
DBLP Record/Paper page utilities.

Provides functions to convert RecordParser/RecordPageParser to Paper and info dict,
and extract DBLP identifiers from Paper/Author objects.
"""

from ...dataclass import Paper, Author
from .parser import RecordParser, RecordAuthor


def paper_to_dblp_key(paper: Paper) -> str | None:
    """
    Extract DBLP paper key from Paper identifiers.

    Args:
        paper: Paper object with identifiers

    Returns:
        DBLP paper key (e.g., "conf/cvpr/HeZRS16") or None if not found
    """
    for ident in paper.identifiers:
        if ident.startswith("paper:dblp:key:"):
            return ident[15:]  # Remove "paper:dblp:key:" prefix
    return None


def author_from_record_author(record_author: RecordAuthor) -> Author | None:
    """
    Create Author from RecordAuthor (with pid).

    Only returns Author if pid is available (from person pages).

    Args:
        record_author: RecordAuthor from parser

    Returns:
        Author with identifiers or None if no pid
    """
    identifiers = set()
    if record_author.pid:
        identifiers = {f"author:dblp:pid:{record_author.pid}"}
    if record_author.name:
        identifiers.add(f"author:name:{record_author.name}")
    return Author(identifiers=identifiers) if identifiers else None


def record_to_paper(record: RecordParser) -> Paper:
    """
    Convert RecordParser to Paper with identifiers.

    Identifiers extracted (format: paper:{info_key}:{value}):
    - paper:dblp:key:{key} - DBLP paper key (matches info["dblp:key"])
    - paper:dblp:url:{url} - DBLP URL (matches info["dblp:url"])
    - {ee} - All ee URLs as identifiers
    """
    identifiers = set()

    if record.key:
        identifiers.add(f"paper:dblp:key:{record.key}")

    if record.url:
        identifiers.add(f"paper:dblp:url:{record.url}")

    for ee in record.ees:
        identifiers.add(f"{ee}")

    return Paper(identifiers=identifiers)


def record_to_info(record: RecordParser) -> dict:
    """
    Convert RecordParser to info dict.

    Keys with "dblp:" prefix (to avoid conflicts):
    - dblp:key, dblp:type, dblp:mdate, dblp:url, dblp:crossref

    Common keys (unlikely to conflict):
    - title, pages, year, month, volume, series, booktitle, journal, number, ees, stream, venue, venue_type
    """
    info = {}

    # Keys with dblp: prefix
    if record.key:
        info["dblp:key"] = record.key
    if record.type:
        info["dblp:type"] = record.type
    if record.mdate:
        info["dblp:mdate"] = record.mdate
    if record.url:
        info["dblp:url"] = record.url
    if record.crossref:
        info["dblp:crossref"] = record.crossref
    if record.stream:
        info["dblp:stream"] = record.stream
    if record.venue:
        info["dblp:venue"] = record.venue
    if record.venue_type:
        info["dblp:venue_type"] = record.venue_type

    # Common keys
    if record.title:
        info["title"] = record.title
    if record.pages:
        info["pages"] = record.pages
    if record.year:
        info["year"] = record.year
    if record.month:
        info["month"] = record.month
    if record.volume:
        info["volume"] = record.volume
    if record.series:
        info["series"] = record.series
    if record.booktitle:
        info["booktitle"] = record.booktitle
    if record.journal:
        info["journal"] = record.journal
    if record.number:
        info["number"] = record.number
    ees = list(record.ees)
    if ees:
        info["urls"] = ees

    return info
