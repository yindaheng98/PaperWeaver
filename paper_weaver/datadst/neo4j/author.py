"""
Neo4j Author node operations.

Provides functions for saving Author nodes and creating relationships
between authors and papers.
"""

from neo4j import AsyncSession

from ...dataclass import Paper, Author
from .utils import save_node, create_relationship


async def save_author(
    session: AsyncSession,
    author: Author,
    info: dict
) -> None:
    """
    Save an Author node to Neo4j.

    If nodes with matching identifiers exist, they are merged into one.
    New info values override existing values for the same keys.

    Args:
        session: Neo4j async session
        author: Author object with identifiers
        info: Info dict to store as node properties
    """
    async def _save(tx):
        await save_node(tx, "Author", author.identifiers, info)

    await session.execute_write(_save)


async def link_author_to_paper(
    session: AsyncSession,
    paper: Paper,
    author: Author
) -> None:
    """
    Create an AUTHORED relationship: author -> paper.

    Args:
        session: Neo4j async session
        paper: The paper
        author: The author who wrote the paper
    """
    async def _link(tx):
        await create_relationship(
            tx,
            "Author", author.identifiers,
            "Paper", paper.identifiers,
            "AUTHORED"
        )

    await session.execute_write(_link)
