# PaperWeaver

[![PyPI version](https://badge.fury.io/py/paper-weaver.svg)](https://badge.fury.io/py/paper-weaver)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)

**PaperWeaver** is a tool for weaving academic paper data from various sources (DBLP, Semantic Scholar) into graph databases (Neo4j). It uses BFS traversal to explore and collect papers, authors, venues, citations, and references, building a comprehensive academic knowledge graph.

## Features

- **Multiple Data Sources**
  - DBLP API - bibliographic information
  - Semantic Scholar API - citations and references

- **Graph Database Output**
  - Neo4j - store papers, authors, venues and their relationships

- **Flexible Caching**
  - In-memory cache for simple use cases
  - Redis cache for distributed and persistent caching

- **BFS Traversal**
  - Start from authors, papers, or venues
  - Automatically discover related entities through citations, references, and authorship

## Installation

```bash
pip install paper-weaver
```

Or install from source:

```bash
git clone https://github.com/yindaheng98/PaperWeaver.git
cd PaperWeaver
pip install -e .
```

## Quick Start

### Basic Usage

Start from an author and explore their papers and related venues:

```bash
paper-weaver \
  --init-mode authors \
  --init-dblp-pids h/KaimingHe \
  --datadst-neo4j-uri bolt://localhost:7687 \
  --datadst-neo4j-user neo4j \
  --datadst-neo4j-password your-password \
  -n 10 -v
```

### Start from Papers

```bash
paper-weaver \
  --init-mode papers \
  --init-dblp-record-keys conf/cvpr/HeZRS16 journals/pami/HeZRS16 \
  --datadst-neo4j-uri bolt://localhost:7687 \
  -n 5 -v
```

### Start from Venues

```bash
paper-weaver \
  --init-mode venues \
  --init-dblp-venue-keys db/conf/cvpr/cvpr2016 \
  --datadst-neo4j-uri bolt://localhost:7687 \
  -n 5 -v
```

## Command-Line Options

### Weaver Options

| Option | Default | Description |
|--------|---------|-------------|
| `--weaver-type` | `a2p2v` | Weaver type |
| `-n, --max-iterations` | `0` | Max BFS iterations (0 = until no new data) |
| `-v, --verbose` | - | Increase verbosity (-v: INFO, -vv: DEBUG) |

### Initialization Options

| Option | Default | Description |
|--------|---------|-------------|
| `--init-type` | `dblp` | Initializer type |
| `--init-mode` | `authors` | Initialization mode: `papers`, `authors`, or `venues` |
| `--init-dblp-record-keys` | - | DBLP record keys (e.g., `conf/cvpr/HeZRS16`) |
| `--init-dblp-pids` | - | DBLP person IDs (e.g., `h/KaimingHe`) |
| `--init-dblp-venue-keys` | - | DBLP venue keys (e.g., `db/conf/cvpr/cvpr2016`) |

### Data Source Options

| Option | Default | Description |
|--------|---------|-------------|
| `--datasrc-type` | `dblp` | Data source: `dblp` or `semanticscholar` |
| `--datasrc-cache-mode` | `memory` | Cache backend: `memory` or `redis` |
| `--datasrc-redis-url` | `redis://localhost:6379` | Redis URL for data source cache |
| `--datasrc-max-concurrent` | `10` | Maximum concurrent HTTP requests |
| `--datasrc-http-proxy` | - | HTTP proxy URL |
| `--datasrc-http-timeout` | `30` | HTTP timeout in seconds |
| `--datasrc-ss-api-key` | - | Semantic Scholar API key |

### Cache Options

| Option | Default | Description |
|--------|---------|-------------|
| `--cache-mode` | `memory` | Cache backend: `memory` or `redis` |
| `--cache-redis-url` | `redis://localhost:6379` | Default Redis URL |
| `--cache-redis-prefix` | `paper-weaver-cache` | Redis key prefix |

### Neo4j Options

| Option | Default | Description |
|--------|---------|-------------|
| `--datadst-neo4j-uri` | `bolt://localhost:7687` | Neo4j connection URI |
| `--datadst-neo4j-user` | `neo4j` | Neo4j username |
| `--datadst-neo4j-password` | `neo4j` | Neo4j password |
| `--datadst-neo4j-database` | `neo4j` | Neo4j database name |

## Using with Redis Cache

For large-scale crawling, use Redis for persistent caching:

```bash
paper-weaver \
  --init-mode authors \
  --init-dblp-pids h/KaimingHe \
  --cache-mode redis \
  --cache-redis-url redis://localhost:6379 \
  --datasrc-cache-mode redis \
  --datasrc-redis-url redis://localhost:6379 \
  --datadst-neo4j-uri bolt://localhost:7687 \
  -v
```

## Graph Schema

PaperWeaver creates the following nodes and relationships in Neo4j:

### Nodes

- **Paper**: `title`, `year`, `venue`, `doi`, etc.
- **Author**: `name`, `pid`, `orcid`, etc.
- **Venue**: `name`, `type` (journal/proceedings/book)

### Relationships

- `(Author)-[:AUTHORED]->(Paper)`
- `(Paper)-[:PUBLISHED_IN]->(Venue)`
- `(Paper)-[:CITES]->(Paper)`
- `(Paper)-[:REFERENCES]->(Paper)`

## Python API

```python
import asyncio
from paper_weaver import Author2Paper2VenueWeaver
from paper_weaver.datasrc.dblp import DBLPDataSrc
from paper_weaver.datadst.neo4j import Neo4jDataDst
from paper_weaver.cache import HybridCacheBuilder
from paper_weaver.initializer.dblp import DBLPAuthorsInitializer

async def main():
    # Setup components
    datasrc = DBLPDataSrc()
    cache = HybridCacheBuilder().with_all_memory().build_weaver_cache()
    initializer = DBLPAuthorsInitializer(["h/KaimingHe"])
    
    # Setup Neo4j
    from neo4j import AsyncGraphDatabase
    driver = AsyncGraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"))
    session = driver.session(database="neo4j")
    datadst = Neo4jDataDst(session)
    
    # Create weaver and run
    weaver = Author2Paper2VenueWeaver(
        src=datasrc,
        dst=datadst,
        cache=cache,
        initializer=initializer
    )
    
    total = await weaver.bfs(max_iterations=10)
    print(f"Processed {total} items")
    
    await driver.close()

asyncio.run(main())
```

## Requirements

- Python 3.10+
- Neo4j 4.0+ (for graph storage)
- Redis (optional, for distributed caching)

## License

MIT License
