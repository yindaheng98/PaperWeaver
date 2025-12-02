"""
DBLP XML Parsers.

This module provides parsers for different DBLP XML page types:

- RecordParser: Parse publication element (from ElementTree.Element)
- RecordPageParser: Parse record page (https://dblp.org/rec/xxx.xml)
- PersonPageParser: Parse person page (https://dblp.org/pid/xxx.xml)  
- VenuePageParser: Parse venue page (https://dblp.org/db/xxx/index.xml)

Usage:
    from paper_weaver.datasrc.dblp.parser import (
        RecordParser, RecordPageParser, RecordAuthor,
        PersonPageParser,
        VenuePageParser,
    )
    
    # Parse a record page from XML string
    record = RecordPageParser(xml_text)
    print(record.title, record.year)
    
    # Parse a person page
    person = PersonPageParser(xml_text)
    for pub in person.publications:  # pub is RecordParser
        print(pub.key)
"""

from .record import (
    RecordParser,
    RecordPageParser,
    RecordAuthor,
)
from .person import (
    PersonPageParser,
)
from .venue import (
    VenuePageParser,
)

__all__ = [
    # Record parsing
    "RecordParser",
    "RecordPageParser",
    "RecordAuthor",
    # Person page parsing
    "PersonPageParser",
    # Venue page parsing
    "VenuePageParser",
]
