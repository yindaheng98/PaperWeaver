"""
Neo4j Paper node operations.

Provides functions for saving Paper nodes and creating relationships
between papers (citations/references).
"""

from neo4j import AsyncSession

from ...dataclass import Paper
from .utils import save_node, create_relationship


async def save_paper(
    session: AsyncSession,
    paper: Paper,
    info: dict
) -> None:
    """
    Save a Paper node to Neo4j.

    If nodes with matching identifiers exist, they are merged into one.
    New info values override existing values for the same keys.

    Args:
        session: Neo4j async session
        paper: Paper object with identifiers
        info: Info dict to store as node properties
    """
    async def _save(tx):
        await save_node(tx, "Paper", paper.identifiers, info)

    await session.execute_write(_save)


async def link_paper_citation(
    session: AsyncSession,
    paper: Paper,
    citation: Paper
) -> None:
    """
    Create a CITES relationship: citation -> paper (citation cites this paper).

    Args:
        session: Neo4j async session
        paper: The paper being cited
        citation: The paper that cites this paper
    """
    async def _link(tx):
        await create_relationship(
            tx,
            "Paper", citation.identifiers,
            "Paper", paper.identifiers,
            "CITES"
        )

    await session.execute_write(_link)


async def link_paper_reference(
    session: AsyncSession,
    paper: Paper,
    reference: Paper
) -> None:
    """
    Create a CITES relationship: paper -> reference (paper cites the reference).

    Args:
        session: Neo4j async session
        paper: The paper that cites
        reference: The paper being cited (referenced)
    """
    async def _link(tx):
        await create_relationship(
            tx,
            "Paper", paper.identifiers,
            "Paper", reference.identifiers,
            "CITES"
        )

    await session.execute_write(_link)
