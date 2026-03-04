"""
CrossRef initializer that seeds papers from Neo4j.

This initializer executes user-provided Cypher patterns that match `Paper`
nodes, then appends a DOI extraction expression to each pattern:
- Match `(paper)-[:HAS_ID]->(id:PaperIdentifier)`
- Keep identifiers whose `value` starts with `doi` (case-insensitive)
- Return the identifier `value`
"""

from typing import AsyncIterator

from neo4j import AsyncGraphDatabase

from ...dataclass import Paper
from ...iface_init import PapersWeaverInitializerIface


class CrossRefNeo4JPapersInitializer(PapersWeaverInitializerIface):
    """
    Initialize papers from Neo4j by executing Cypher patterns.

    Notes:
    - Each pattern must bind the matched Paper node to variable name `paper`.
    - The initializer appends DOI extraction Cypher after each pattern.
    """

    def __init__(
        self,
        patterns: list[str],
        uri: str = "bolt://localhost:7687",
        user: str = "neo4j",
        password: str = "neo4j",
        database: str = "neo4j",
    ):
        """
        Initialize with Neo4j query patterns and connection settings.

        Args:
            patterns: Cypher pattern snippets that match Paper nodes as `paper`.
            uri: Neo4j connection URI.
            user: Neo4j username.
            password: Neo4j password.
            database: Neo4j database name.
        """
        self._patterns = patterns
        self._uri = uri
        self._user = user
        self._password = password
        self._database = database

    @staticmethod
    def _pattern_to_query(pattern: str) -> str:
        """Append DOI extraction expression after a Paper-matching pattern."""
        suffix = """
MATCH (paper)-[:HAS_ID]->(paper_identifier:PaperIdentifier)
WHERE toLower(paper_identifier.value) STARTS WITH 'https://doi.org/'
RETURN DISTINCT paper_identifier.value AS doi_identifier
"""
        return f"{pattern.rstrip()}\n{suffix}"

    async def fetch_papers(self) -> AsyncIterator[Paper]:
        """Yield papers initialized from DOI-like PaperIdentifier values in Neo4j."""
        seen_dois: set[str] = set()

        driver = AsyncGraphDatabase.driver(
            self._uri,
            auth=(self._user, self._password),
        )
        async with driver:
            async with driver.session(database=self._database) as session:
                for pattern in self._patterns:
                    query = self._pattern_to_query(pattern)
                    result = await session.run(query)
                    async for record in result:
                        doi_identifier = record.get("doi_identifier")
                        if not doi_identifier or doi_identifier in seen_dois:
                            continue
                        seen_dois.add(doi_identifier)
                        yield Paper(identifiers={doi_identifier})
