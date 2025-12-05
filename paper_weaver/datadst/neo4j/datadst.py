"""
Neo4j DataDst implementation.

Provides a DataDst implementation that stores Paper, Author, and Venue
nodes in a Neo4j graph database.

Features:
- Stores identifiers as separate nodes (e.g., PaperIdentifier, AuthorIdentifier, VenueIdentifier)
  connected via HAS_ID relationships (allows indexing, which Neo4j cannot do on list properties)
- Merges nodes with overlapping identifiers into one
- Updates existing nodes by adding new identifiers and merging info
- Creates relationships between nodes (AUTHORED, PUBLISHED_IN, CITES)
- Ensures session operations are serialized via asyncio.Lock
"""

import asyncio

from neo4j import AsyncSession

from ...dataclass import DataDst, Paper, Author, Venue
from .paper import save_paper, link_paper_citation, link_paper_reference
from .author import save_author, link_author_to_paper
from .venue import save_venue, link_paper_to_venue


class Neo4jDataDst(DataDst):
    """
    DataDst implementation for Neo4j graph database.

    Stores Paper, Author, and Venue as nodes with their info as properties.
    Identifiers are stored as separate nodes connected via HAS_ID relationships:
    - Paper -[:HAS_ID]-> PaperIdentifier
    - Author -[:HAS_ID]-> AuthorIdentifier
    - Venue -[:HAS_ID]-> VenueIdentifier

    This structure allows creating indexes on identifier values
    (Neo4j cannot create indexes on list properties).

    When saving:
    - Finds all nodes with any matching identifier (via identifier nodes)
    - Merges all matching nodes into one
    - Creates new identifier nodes for new identifiers
    - Updates properties: new info values override existing values for same keys

    Relationships:
    - Author -[AUTHORED]-> Paper
    - Paper -[PUBLISHED_IN]-> Venue
    - Paper -[CITES]-> Paper (for both citations and references)
    """

    def __init__(self, session: AsyncSession):
        """
        Initialize Neo4jDataDst.

        Args:
            session: Neo4j async session for database operations
        """
        self._session = session
        self._lock = asyncio.Lock()

    @property
    def session(self) -> AsyncSession:
        """Get the Neo4j session."""
        return self._session

    @property
    def lock(self) -> asyncio.Lock:
        """Get the session lock for concurrency control."""
        return self._lock

    # ==================== Paper Methods ====================

    async def save_paper_info(self, paper: Paper, info: dict) -> None:
        """
        Save paper information to Neo4j.

        Creates or updates a Paper node. If nodes with matching identifiers
        exist, they are merged into one node.

        Args:
            paper: Paper object with identifiers
            info: Info dict to store as node properties
        """
        async with self._lock:
            await save_paper(self._session, paper, info)

    async def link_citation(self, paper: Paper, citation: Paper) -> None:
        """
        Link a citation to a paper (citation cites this paper).

        Creates a CITES relationship: citation -[CITES]-> paper

        Args:
            paper: The paper being cited
            citation: The paper that cites this paper
        """
        async with self._lock:
            await link_paper_citation(self._session, paper, citation)

    async def link_reference(self, paper: Paper, reference: Paper) -> None:
        """
        Link a reference to a paper (paper cites the reference).

        Creates a CITES relationship: paper -[CITES]-> reference

        Args:
            paper: The paper that cites
            reference: The paper being cited (referenced)
        """
        async with self._lock:
            await link_paper_reference(self._session, paper, reference)

    # ==================== Author Methods ====================

    async def save_author_info(self, author: Author, info: dict) -> None:
        """
        Save author information to Neo4j.

        Creates or updates an Author node. If nodes with matching identifiers
        exist, they are merged into one node.

        Args:
            author: Author object with identifiers
            info: Info dict to store as node properties
        """
        async with self._lock:
            await save_author(self._session, author, info)

    async def link_author(self, paper: Paper, author: Author) -> None:
        """
        Link an author to a paper.

        Creates an AUTHORED relationship: author -[AUTHORED]-> paper

        Args:
            paper: The paper
            author: The author who wrote the paper
        """
        async with self._lock:
            await link_author_to_paper(self._session, paper, author)

    # ==================== Venue Methods ====================

    async def save_venue_info(self, venue: Venue, info: dict) -> None:
        """
        Save venue information to Neo4j.

        Creates or updates a Venue node. If nodes with matching identifiers
        exist, they are merged into one node.

        Args:
            venue: Venue object with identifiers
            info: Info dict to store as node properties
        """
        async with self._lock:
            await save_venue(self._session, venue, info)

    async def link_venue(self, paper: Paper, venue: Venue) -> None:
        """
        Link a paper to a venue.

        Creates a PUBLISHED_IN relationship: paper -[PUBLISHED_IN]-> venue

        Args:
            paper: The paper
            venue: The venue where the paper was published
        """
        async with self._lock:
            await link_paper_to_venue(self._session, paper, venue)
