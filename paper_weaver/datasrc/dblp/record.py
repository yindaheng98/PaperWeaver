"""
DBLP Record/Paper page utilities.

Provides functions to convert RecordParser/RecordPageParser to Paper and info dict.
"""

from ...dataclass import Paper
from .parser import RecordParser


def record_to_paper(record: RecordParser) -> Paper:
    """
    Convert RecordParser to Paper with identifiers.

    Identifiers extracted:
    - dblp:{key} - DBLP paper key
    - dblp-url:{url} - DBLP URL
    - {ee} - All ee URLs as identifiers
    """
    identifiers = set()

    if record.key:
        identifiers.add(f"dblp:{record.key}")

    if record.url:
        identifiers.add(f"dblp-url:{record.url}")

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
