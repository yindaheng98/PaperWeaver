"""
DBLP DataSrc module.

Provides data source implementation for DBLP API.
"""

from .datasrc import DBLPDataSrc
from .utils import fetch_xml
from .person import author_to_dblp_pid, person_page_to_author, person_page_to_info
from .record import paper_to_dblp_key, record_to_paper, record_to_info
from .person import author_from_record_author
from .venue import venue_to_dblp_key, venue_key_from_paper, venue_page_to_venue, venue_page_to_info
from dblp_webxml_parser import RecordParser, RecordAuthor, RecordPageParser, PersonPageParser, VenuePageParser

__all__ = [
    "DBLPDataSrc",
    # Utils
    "fetch_xml",
    # Person/Author
    "author_to_dblp_pid",
    "person_page_to_author",
    "person_page_to_info",
    "PersonPageParser",
    # Record/Paper
    "paper_to_dblp_key",
    "author_from_record_author",
    "record_to_paper",
    "record_to_info",
    "RecordParser",
    "RecordAuthor",
    "RecordPageParser",
    # Venue
    "venue_to_dblp_key",
    "venue_key_from_paper",
    "venue_page_to_venue",
    "venue_page_to_info",
    "VenuePageParser",
]
