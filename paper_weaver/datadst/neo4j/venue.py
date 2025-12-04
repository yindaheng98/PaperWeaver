"""
Neo4j Venue node operations.

Provides functions for saving Venue nodes and creating relationships
between papers and venues.
"""

from neo4j import AsyncSession

from ...dataclass import Paper, Venue
from .utils import save_node, create_relationship


async def save_venue(
    session: AsyncSession,
    venue: Venue,
    info: dict
) -> None:
    """
    Save a Venue node to Neo4j.

    If nodes with matching identifiers exist, they are merged into one.
    New info values override existing values for the same keys.

    Args:
        session: Neo4j async session
        venue: Venue object with identifiers
        info: Info dict to store as node properties
    """
    async def _save(tx):
        await save_node(tx, "Venue", venue.identifiers, info)

    await session.execute_write(_save)


async def link_paper_to_venue(
    session: AsyncSession,
    paper: Paper,
    venue: Venue
) -> None:
    """
    Create a PUBLISHED_IN relationship: paper -> venue.

    Args:
        session: Neo4j async session
        paper: The paper
        venue: The venue where the paper was published
    """
    async def _link(tx):
        await create_relationship(
            tx,
            "Paper", paper.identifiers,
            "Venue", venue.identifiers,
            "PUBLISHED_IN"
        )

    await session.execute_write(_link)
