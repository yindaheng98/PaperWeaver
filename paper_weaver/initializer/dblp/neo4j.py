from typing import AsyncIterator

from ...dataclass import Paper, Author, Venue
from ...iface_init import (
    PapersWeaverInitializerIface,
    AuthorsWeaverInitializerIface,
    VenuesWeaverInitializerIface,
)


class Neo4JDBLPPapersInitializer(PapersWeaverInitializerIface):
    pass


class Neo4JDBLPAuthorsInitializer(AuthorsWeaverInitializerIface):
    pass


class Neo4JDBLPVenuesInitializer(VenuesWeaverInitializerIface):
    pass

# TODO: Match and return DBLP keys from Neo4J like paper_weaver/initializer/crossref/neo4j.py
