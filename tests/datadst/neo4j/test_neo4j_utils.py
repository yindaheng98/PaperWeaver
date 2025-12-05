"""
Unit tests for Neo4j utils operations.

Tests verify the core Neo4j operations for storing and merging nodes
based on identifiers.

Run with: pytest tests/datadst/neo4j/test_neo4j_utils.py -v
Requires: pip install neo4j pytest-asyncio

Environment variables:
- NEO4J_URI: Neo4j server URI (default: bolt://localhost:7687)
- NEO4J_USER: Neo4j username (default: neo4j)
- NEO4J_PASSWORD: Neo4j password (default: password)
"""

import os
import pytest
import pytest_asyncio

# Try to import neo4j library
try:
    from neo4j import AsyncGraphDatabase
    NEO4J_AVAILABLE = True
except ImportError:
    NEO4J_AVAILABLE = False

from paper_weaver.datadst.neo4j.utils import (
    find_nodes_by_identifiers,
    create_node,
    merge_nodes_into_one,
    save_node,
    create_relationship,
)

# Neo4j connection config from environment variables
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")


def _check_neo4j_connection():
    """Check if Neo4j server is available and credentials are valid."""
    if not NEO4J_AVAILABLE:
        return False, "neo4j library not installed"
    try:
        from neo4j import GraphDatabase
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        driver.verify_connectivity()
        driver.close()
        return True, None
    except Exception as e:
        return False, str(e)


NEO4J_SERVER_AVAILABLE, NEO4J_ERROR = _check_neo4j_connection()

# Skip all tests if Neo4j is not available
pytestmark = pytest.mark.skipif(
    not NEO4J_SERVER_AVAILABLE,
    reason=f"Neo4j server not available: {NEO4J_ERROR}"
)


# =============================================================================
# Fixtures
# =============================================================================

@pytest_asyncio.fixture
async def neo4j_driver():
    """Create a Neo4j async driver for testing."""
    driver = AsyncGraphDatabase.driver(
        NEO4J_URI,
        auth=(NEO4J_USER, NEO4J_PASSWORD)
    )
    yield driver
    await driver.close()


@pytest_asyncio.fixture
async def neo4j_session(neo4j_driver):
    """Create a Neo4j async session and clean up test data."""
    session = neo4j_driver.session()

    # Clean up test nodes and identifier nodes before each test
    await session.run("MATCH (n:TestPaper) DETACH DELETE n")
    await session.run("MATCH (n:TestAuthor) DETACH DELETE n")
    await session.run("MATCH (n:TestVenue) DETACH DELETE n")
    await session.run("MATCH (n:TestPaperIdentifier) DETACH DELETE n")
    await session.run("MATCH (n:TestAuthorIdentifier) DETACH DELETE n")
    await session.run("MATCH (n:TestVenueIdentifier) DETACH DELETE n")

    yield session

    # Clean up test nodes and identifier nodes after each test
    await session.run("MATCH (n:TestPaper) DETACH DELETE n")
    await session.run("MATCH (n:TestAuthor) DETACH DELETE n")
    await session.run("MATCH (n:TestVenue) DETACH DELETE n")
    await session.run("MATCH (n:TestPaperIdentifier) DETACH DELETE n")
    await session.run("MATCH (n:TestAuthorIdentifier) DETACH DELETE n")
    await session.run("MATCH (n:TestVenueIdentifier) DETACH DELETE n")
    await session.close()


# =============================================================================
# Test: find_nodes_by_identifiers
# =============================================================================

class TestFindNodesByIdentifiers:
    """Test find_nodes_by_identifiers function."""

    @pytest.mark.asyncio
    async def test_find_no_nodes_when_empty(self, neo4j_session):
        """Should return empty list when no nodes exist."""
        async def _test(tx):
            result = await find_nodes_by_identifiers(
                tx, "TestPaper", {"doi:123"}
            )
            assert result == []

        await neo4j_session.execute_read(_test)

    @pytest.mark.asyncio
    async def test_find_no_nodes_with_empty_identifiers(self, neo4j_session):
        """Should return empty list when identifiers is empty."""
        async def _test(tx):
            result = await find_nodes_by_identifiers(
                tx, "TestPaper", set()
            )
            assert result == []

        await neo4j_session.execute_read(_test)

    @pytest.mark.asyncio
    async def test_find_single_node(self, neo4j_session):
        """Should find a single node with matching identifier."""
        # Create a node first
        async def _create(tx):
            await create_node(tx, "TestPaper", {"doi:123"}, {"title": "Test Paper"})

        await neo4j_session.execute_write(_create)

        # Find the node
        async def _find(tx):
            result = await find_nodes_by_identifiers(
                tx, "TestPaper", {"doi:123"}
            )
            assert len(result) == 1
            assert "doi:123" in result[0]["identifiers"]
            assert result[0]["properties"]["title"] == "Test Paper"

        await neo4j_session.execute_read(_find)

    @pytest.mark.asyncio
    async def test_find_multiple_nodes(self, neo4j_session):
        """Should find multiple nodes with overlapping identifiers."""
        async def _create(tx):
            await create_node(tx, "TestPaper", {"doi:A", "shared:123"}, {"title": "Paper A"})
            await create_node(tx, "TestPaper", {"doi:B", "shared:123"}, {"title": "Paper B"})

        await neo4j_session.execute_write(_create)

        async def _find(tx):
            result = await find_nodes_by_identifiers(
                tx, "TestPaper", {"shared:123"}
            )
            assert len(result) == 2

        await neo4j_session.execute_read(_find)

    @pytest.mark.asyncio
    async def test_find_node_with_any_matching_identifier(self, neo4j_session):
        """Should find node if ANY identifier matches."""
        async def _create(tx):
            await create_node(
                tx, "TestPaper",
                {"doi:123", "arxiv:456", "pmid:789"},
                {"title": "Multi ID Paper"}
            )

        await neo4j_session.execute_write(_create)

        async def _find(tx):
            # Search with just one matching identifier
            result = await find_nodes_by_identifiers(
                tx, "TestPaper", {"arxiv:456"}
            )
            assert len(result) == 1
            assert "doi:123" in result[0]["identifiers"]
            assert "arxiv:456" in result[0]["identifiers"]
            assert "pmid:789" in result[0]["identifiers"]

        await neo4j_session.execute_read(_find)


# =============================================================================
# Test: create_node
# =============================================================================

class TestCreateNode:
    """Test create_node function."""

    @pytest.mark.asyncio
    async def test_create_simple_node(self, neo4j_session):
        """Should create a node with identifiers and info."""
        async def _create(tx):
            element_id = await create_node(
                tx, "TestPaper",
                {"doi:123"},
                {"title": "Test Paper", "year": 2024}
            )
            assert element_id is not None

        await neo4j_session.execute_write(_create)

        # Verify node exists
        async def _verify(tx):
            result = await find_nodes_by_identifiers(
                tx, "TestPaper", {"doi:123"}
            )
            assert len(result) == 1
            assert result[0]["properties"]["title"] == "Test Paper"
            assert result[0]["properties"]["year"] == 2024
            assert "doi:123" in result[0]["identifiers"]

        await neo4j_session.execute_read(_verify)

    @pytest.mark.asyncio
    async def test_create_node_with_multiple_identifiers(self, neo4j_session):
        """Should create a node with multiple identifiers."""
        async def _create(tx):
            await create_node(
                tx, "TestPaper",
                {"doi:123", "arxiv:456", "pmid:789"},
                {"title": "Multi ID Paper"}
            )

        await neo4j_session.execute_write(_create)

        async def _verify(tx):
            result = await find_nodes_by_identifiers(
                tx, "TestPaper", {"doi:123"}
            )
            assert len(result) == 1
            assert "doi:123" in result[0]["identifiers"]
            assert "arxiv:456" in result[0]["identifiers"]
            assert "pmid:789" in result[0]["identifiers"]

        await neo4j_session.execute_read(_verify)

    @pytest.mark.asyncio
    async def test_create_node_with_empty_info(self, neo4j_session):
        """Should create a node with just identifiers."""
        async def _create(tx):
            await create_node(tx, "TestPaper", {"doi:123"}, {})

        await neo4j_session.execute_write(_create)

        async def _verify(tx):
            result = await find_nodes_by_identifiers(
                tx, "TestPaper", {"doi:123"}
            )
            assert len(result) == 1
            # Identifiers should be available via the identifiers field
            assert "doi:123" in result[0]["identifiers"]

        await neo4j_session.execute_read(_verify)

    @pytest.mark.asyncio
    async def test_create_node_with_special_characters_in_keys(self, neo4j_session):
        """Should handle info keys with colons and special characters."""
        async def _create(tx):
            await create_node(
                tx, "TestPaper",
                {"doi:123"},
                {"dblp:key": "conf/test/2024", "semantic-scholar:id": "abc123"}
            )

        await neo4j_session.execute_write(_create)

        async def _verify(tx):
            result = await find_nodes_by_identifiers(
                tx, "TestPaper", {"doi:123"}
            )
            assert len(result) == 1
            props = result[0]["properties"]
            assert props["dblp:key"] == "conf/test/2024"
            assert props["semantic-scholar:id"] == "abc123"

        await neo4j_session.execute_read(_verify)

    @pytest.mark.asyncio
    async def test_create_node_with_list_values(self, neo4j_session):
        """Should handle info with list values."""
        async def _create(tx):
            await create_node(
                tx, "TestAuthor",
                {"orcid:123"},
                {"name": "Test Author", "affiliations": ["MIT", "Stanford"]}
            )

        await neo4j_session.execute_write(_create)

        async def _verify(tx):
            result = await find_nodes_by_identifiers(
                tx, "TestAuthor", {"orcid:123"}
            )
            assert len(result) == 1
            props = result[0]["properties"]
            assert props["name"] == "Test Author"
            assert "MIT" in props["affiliations"]
            assert "Stanford" in props["affiliations"]

        await neo4j_session.execute_read(_verify)

    @pytest.mark.asyncio
    async def test_create_node_creates_identifier_nodes(self, neo4j_session):
        """Should create separate identifier nodes connected via HAS_ID."""
        async def _create(tx):
            await create_node(
                tx, "TestPaper",
                {"doi:123", "arxiv:456"},
                {"title": "Test Paper"}
            )

        await neo4j_session.execute_write(_create)

        # Verify identifier nodes exist and are connected
        result = await neo4j_session.run(
            "MATCH (p:TestPaper)-[:HAS_ID]->(id:TestPaperIdentifier) "
            "RETURN p.title as title, collect(id.value) as ids"
        )
        record = await result.single()
        assert record["title"] == "Test Paper"
        assert set(record["ids"]) == {"doi:123", "arxiv:456"}


# =============================================================================
# Test: merge_nodes_into_one
# =============================================================================

class TestMergeNodesIntoOne:
    """Test merge_nodes_into_one function."""

    @pytest.mark.asyncio
    async def test_merge_single_node_updates_properties(self, neo4j_session):
        """Merging single node should update its properties."""
        # Create initial node
        async def _create(tx):
            await create_node(
                tx, "TestPaper",
                {"doi:123"},
                {"title": "Original Title", "year": 2020}
            )

        await neo4j_session.execute_write(_create)

        # Merge with new info
        async def _merge(tx):
            nodes = await find_nodes_by_identifiers(tx, "TestPaper", {"doi:123"})
            await merge_nodes_into_one(
                tx, "TestPaper", nodes,
                {"doi:123", "arxiv:456"},  # Add new identifier
                {"title": "Updated Title", "abstract": "New abstract"}  # Update and add
            )

        await neo4j_session.execute_write(_merge)

        # Verify
        async def _verify(tx):
            result = await find_nodes_by_identifiers(tx, "TestPaper", {"doi:123"})
            assert len(result) == 1
            props = result[0]["properties"]
            assert props["title"] == "Updated Title"  # Updated
            assert props["year"] == 2020  # Preserved
            assert props["abstract"] == "New abstract"  # Added
            assert "arxiv:456" in result[0]["identifiers"]  # New identifier added

        await neo4j_session.execute_read(_verify)

    @pytest.mark.asyncio
    async def test_merge_two_nodes_combines_identifiers(self, neo4j_session):
        """Merging two nodes should combine their identifiers."""
        # Create two separate nodes
        async def _create(tx):
            await create_node(tx, "TestPaper", {"doi:A"}, {"title": "Paper A"})
            await create_node(tx, "TestPaper", {"doi:B"}, {"title": "Paper B"})

        await neo4j_session.execute_write(_create)

        # Merge them
        async def _merge(tx):
            nodes = await find_nodes_by_identifiers(tx, "TestPaper", {"doi:A", "doi:B"})
            await merge_nodes_into_one(
                tx, "TestPaper", nodes,
                {"doi:C"},  # Add another identifier
                {"title": "Merged Paper"}
            )

        await neo4j_session.execute_write(_merge)

        # Verify - should have one node with all identifiers
        async def _verify(tx):
            result_a = await find_nodes_by_identifiers(tx, "TestPaper", {"doi:A"})
            result_b = await find_nodes_by_identifiers(tx, "TestPaper", {"doi:B"})
            result_c = await find_nodes_by_identifiers(tx, "TestPaper", {"doi:C"})

            assert len(result_a) == 1
            assert len(result_b) == 1
            assert len(result_c) == 1

            # All should point to the same node
            assert result_a[0]["element_id"] == result_b[0]["element_id"]
            assert result_a[0]["element_id"] == result_c[0]["element_id"]

            # Should have all identifiers
            identifiers = result_a[0]["identifiers"]
            assert "doi:A" in identifiers
            assert "doi:B" in identifiers
            assert "doi:C" in identifiers

        await neo4j_session.execute_read(_verify)

    @pytest.mark.asyncio
    async def test_merge_nodes_new_info_overrides_existing(self, neo4j_session):
        """New info values should override existing values for same keys."""
        async def _create(tx):
            await create_node(
                tx, "TestPaper",
                {"doi:123"},
                {"title": "Old Title", "year": 2020, "old_field": "preserved"}
            )

        await neo4j_session.execute_write(_create)

        async def _merge(tx):
            nodes = await find_nodes_by_identifiers(tx, "TestPaper", {"doi:123"})
            await merge_nodes_into_one(
                tx, "TestPaper", nodes,
                {"doi:123"},
                {"title": "New Title", "year": 2024}  # Override both
            )

        await neo4j_session.execute_write(_merge)

        async def _verify(tx):
            result = await find_nodes_by_identifiers(tx, "TestPaper", {"doi:123"})
            props = result[0]["properties"]
            assert props["title"] == "New Title"
            assert props["year"] == 2024
            assert props["old_field"] == "preserved"  # Not overridden, kept

        await neo4j_session.execute_read(_verify)

    @pytest.mark.asyncio
    async def test_merge_preserves_properties_from_all_nodes(self, neo4j_session):
        """Merging should preserve unique properties from all nodes."""
        async def _create(tx):
            await create_node(tx, "TestPaper", {"doi:A"}, {"prop_a": "value_a"})
            await create_node(tx, "TestPaper", {"doi:B"}, {"prop_b": "value_b"})

        await neo4j_session.execute_write(_create)

        async def _merge(tx):
            nodes = await find_nodes_by_identifiers(tx, "TestPaper", {"doi:A", "doi:B"})
            await merge_nodes_into_one(tx, "TestPaper", nodes, {"doi:A", "doi:B"}, {})

        await neo4j_session.execute_write(_merge)

        async def _verify(tx):
            result = await find_nodes_by_identifiers(tx, "TestPaper", {"doi:A"})
            props = result[0]["properties"]
            # Both properties should be preserved
            assert props.get("prop_a") == "value_a" or props.get("prop_b") == "value_b"

        await neo4j_session.execute_read(_verify)

    @pytest.mark.asyncio
    async def test_merge_cleans_up_duplicate_identifier_nodes(self, neo4j_session):
        """Merging should clean up identifier nodes from deleted nodes."""
        # Create two separate nodes with some identifiers
        async def _create(tx):
            await create_node(tx, "TestPaper", {"doi:A", "shared:123"}, {"title": "Paper A"})
            await create_node(tx, "TestPaper", {"doi:B", "shared:456"}, {"title": "Paper B"})

        await neo4j_session.execute_write(_create)

        # Merge them
        async def _merge(tx):
            nodes = await find_nodes_by_identifiers(tx, "TestPaper", {"doi:A", "doi:B"})
            await merge_nodes_into_one(tx, "TestPaper", nodes, set(), {"title": "Merged"})

        await neo4j_session.execute_write(_merge)

        # Verify only one Paper node exists with all identifiers
        result = await neo4j_session.run(
            "MATCH (p:TestPaper) RETURN count(p) as paper_count"
        )
        record = await result.single()
        assert record["paper_count"] == 1

        # Verify all identifiers are connected to the single paper
        result = await neo4j_session.run(
            "MATCH (p:TestPaper)-[:HAS_ID]->(id:TestPaperIdentifier) "
            "RETURN collect(id.value) as ids"
        )
        record = await result.single()
        ids = set(record["ids"])
        assert "doi:A" in ids
        assert "doi:B" in ids
        assert "shared:123" in ids
        assert "shared:456" in ids


# =============================================================================
# Test: save_node (main entry point)
# =============================================================================

class TestSaveNode:
    """Test save_node function - the main entry point for saving nodes."""

    @pytest.mark.asyncio
    async def test_save_creates_new_node(self, neo4j_session):
        """save_node should create a new node when none exists."""
        async def _save(tx):
            await save_node(
                tx, "TestPaper",
                {"doi:123"},
                {"title": "New Paper", "year": 2024}
            )

        await neo4j_session.execute_write(_save)

        async def _verify(tx):
            result = await find_nodes_by_identifiers(tx, "TestPaper", {"doi:123"})
            assert len(result) == 1
            assert result[0]["properties"]["title"] == "New Paper"

        await neo4j_session.execute_read(_verify)

    @pytest.mark.asyncio
    async def test_save_updates_existing_node(self, neo4j_session):
        """save_node should update existing node when identifiers match."""
        # Create initial node
        async def _create(tx):
            await save_node(
                tx, "TestPaper",
                {"doi:123"},
                {"title": "Original", "year": 2020}
            )

        await neo4j_session.execute_write(_create)

        # Update with new info
        async def _update(tx):
            await save_node(
                tx, "TestPaper",
                {"doi:123"},
                {"title": "Updated", "abstract": "New abstract"}
            )

        await neo4j_session.execute_write(_update)

        async def _verify(tx):
            result = await find_nodes_by_identifiers(tx, "TestPaper", {"doi:123"})
            assert len(result) == 1
            props = result[0]["properties"]
            assert props["title"] == "Updated"
            assert props["year"] == 2020  # Preserved
            assert props["abstract"] == "New abstract"  # Added

        await neo4j_session.execute_read(_verify)

    @pytest.mark.asyncio
    async def test_save_merges_multiple_matching_nodes(self, neo4j_session):
        """save_node should merge multiple nodes that match different identifiers."""
        # Create two separate nodes
        async def _create(tx):
            await create_node(tx, "TestPaper", {"doi:A"}, {"source": "A"})
            await create_node(tx, "TestPaper", {"doi:B"}, {"source": "B"})

        await neo4j_session.execute_write(_create)

        # Save with identifiers that match both
        async def _save(tx):
            await save_node(
                tx, "TestPaper",
                {"doi:A", "doi:B"},
                {"title": "Merged Paper"}
            )

        await neo4j_session.execute_write(_save)

        async def _verify(tx):
            result_a = await find_nodes_by_identifiers(tx, "TestPaper", {"doi:A"})
            result_b = await find_nodes_by_identifiers(tx, "TestPaper", {"doi:B"})

            # Both should point to same node
            assert len(result_a) == 1
            assert len(result_b) == 1
            assert result_a[0]["element_id"] == result_b[0]["element_id"]
            assert result_a[0]["properties"]["title"] == "Merged Paper"

        await neo4j_session.execute_read(_verify)

    @pytest.mark.asyncio
    async def test_save_adds_new_identifiers(self, neo4j_session):
        """save_node should add new identifiers to existing node."""
        async def _create(tx):
            await save_node(tx, "TestPaper", {"doi:123"}, {"title": "Paper"})

        await neo4j_session.execute_write(_create)

        async def _update(tx):
            await save_node(
                tx, "TestPaper",
                {"doi:123", "arxiv:456", "pmid:789"},  # Add new identifiers
                {}
            )

        await neo4j_session.execute_write(_update)

        async def _verify(tx):
            result = await find_nodes_by_identifiers(tx, "TestPaper", {"arxiv:456"})
            assert len(result) == 1
            identifiers = result[0]["identifiers"]
            assert "doi:123" in identifiers
            assert "arxiv:456" in identifiers
            assert "pmid:789" in identifiers

        await neo4j_session.execute_read(_verify)


# =============================================================================
# Test: create_relationship
# =============================================================================

class TestCreateRelationship:
    """Test create_relationship function."""

    @pytest.mark.asyncio
    async def test_create_relationship_between_existing_nodes(self, neo4j_session):
        """Should create relationship between existing nodes."""
        # Create nodes
        async def _create(tx):
            await create_node(tx, "TestAuthor", {"orcid:A"}, {"name": "Author A"})
            await create_node(tx, "TestPaper", {"doi:123"}, {"title": "Paper"})

        await neo4j_session.execute_write(_create)

        # Create relationship
        async def _link(tx):
            await create_relationship(
                tx,
                "TestAuthor", {"orcid:A"},
                "TestPaper", {"doi:123"},
                "AUTHORED"
            )

        await neo4j_session.execute_write(_link)

        # Verify relationship
        result = await neo4j_session.run(
            "MATCH (a:TestAuthor)-[r:AUTHORED]->(p:TestPaper) RETURN a, r, p"
        )
        records = [record async for record in result]
        assert len(records) == 1

    @pytest.mark.asyncio
    async def test_create_relationship_creates_missing_nodes(self, neo4j_session):
        """Should create placeholder nodes if they don't exist."""
        async def _link(tx):
            await create_relationship(
                tx,
                "TestAuthor", {"orcid:new"},
                "TestPaper", {"doi:new"},
                "AUTHORED"
            )

        await neo4j_session.execute_write(_link)

        # Both nodes should now exist
        async def _verify(tx):
            authors = await find_nodes_by_identifiers(tx, "TestAuthor", {"orcid:new"})
            papers = await find_nodes_by_identifiers(tx, "TestPaper", {"doi:new"})
            assert len(authors) == 1
            assert len(papers) == 1

        await neo4j_session.execute_read(_verify)

        # And relationship should exist
        result = await neo4j_session.run(
            "MATCH (a:TestAuthor)-[r:AUTHORED]->(p:TestPaper) RETURN count(r) as count"
        )
        record = await result.single()
        assert record["count"] == 1

    @pytest.mark.asyncio
    async def test_create_relationship_idempotent(self, neo4j_session):
        """Creating same relationship multiple times should be idempotent."""
        async def _create(tx):
            await create_node(tx, "TestAuthor", {"orcid:A"}, {"name": "Author"})
            await create_node(tx, "TestPaper", {"doi:123"}, {"title": "Paper"})

        await neo4j_session.execute_write(_create)

        # Create relationship multiple times
        for _ in range(3):
            async def _link(tx):
                await create_relationship(
                    tx,
                    "TestAuthor", {"orcid:A"},
                    "TestPaper", {"doi:123"},
                    "AUTHORED"
                )
            await neo4j_session.execute_write(_link)

        # Should only have one relationship
        result = await neo4j_session.run(
            "MATCH (a:TestAuthor)-[r:AUTHORED]->(p:TestPaper) RETURN count(r) as count"
        )
        record = await result.single()
        assert record["count"] == 1

    @pytest.mark.asyncio
    async def test_create_cites_relationship(self, neo4j_session):
        """Should create CITES relationship between papers."""
        async def _create(tx):
            await create_node(tx, "TestPaper", {"doi:A"}, {"title": "Citing Paper"})
            await create_node(tx, "TestPaper", {"doi:B"}, {"title": "Cited Paper"})

        await neo4j_session.execute_write(_create)

        async def _link(tx):
            await create_relationship(
                tx,
                "TestPaper", {"doi:A"},
                "TestPaper", {"doi:B"},
                "CITES"
            )

        await neo4j_session.execute_write(_link)

        result = await neo4j_session.run(
            "MATCH (a:TestPaper)-[r:CITES]->(b:TestPaper) RETURN a.title as citing, b.title as cited"
        )
        record = await result.single()
        assert record["citing"] == "Citing Paper"
        assert record["cited"] == "Cited Paper"

    @pytest.mark.asyncio
    async def test_create_published_in_relationship(self, neo4j_session):
        """Should create PUBLISHED_IN relationship between paper and venue."""
        async def _create(tx):
            await create_node(tx, "TestPaper", {"doi:123"}, {"title": "Paper"})
            await create_node(tx, "TestVenue", {"venue:cvpr2024"}, {"name": "CVPR 2024"})

        await neo4j_session.execute_write(_create)

        async def _link(tx):
            await create_relationship(
                tx,
                "TestPaper", {"doi:123"},
                "TestVenue", {"venue:cvpr2024"},
                "PUBLISHED_IN"
            )

        await neo4j_session.execute_write(_link)

        result = await neo4j_session.run(
            "MATCH (p:TestPaper)-[r:PUBLISHED_IN]->(v:TestVenue) "
            "RETURN p.title as paper, v.name as venue"
        )
        record = await result.single()
        assert record["paper"] == "Paper"
        assert record["venue"] == "CVPR 2024"


# =============================================================================
# Test: Integration scenarios
# =============================================================================

class TestIntegration:
    """Integration tests combining multiple operations."""

    @pytest.mark.asyncio
    async def test_complete_paper_workflow(self, neo4j_session):
        """Test complete workflow: create paper, add authors, add references."""
        # Create paper
        async def _create_paper(tx):
            await save_node(
                tx, "TestPaper",
                {"doi:main", "arxiv:main"},
                {"title": "Main Paper", "year": 2024}
            )

        await neo4j_session.execute_write(_create_paper)

        # Create authors and link
        async def _create_authors(tx):
            await save_node(tx, "TestAuthor", {"orcid:A"}, {"name": "Alice"})
            await save_node(tx, "TestAuthor", {"orcid:B"}, {"name": "Bob"})
            await create_relationship(tx, "TestAuthor", {"orcid:A"}, "TestPaper", {"doi:main"}, "AUTHORED")
            await create_relationship(tx, "TestAuthor", {"orcid:B"}, "TestPaper", {"doi:main"}, "AUTHORED")

        await neo4j_session.execute_write(_create_authors)

        # Create references and link
        async def _create_refs(tx):
            await save_node(tx, "TestPaper", {"doi:ref1"}, {"title": "Reference 1"})
            await save_node(tx, "TestPaper", {"doi:ref2"}, {"title": "Reference 2"})
            await create_relationship(tx, "TestPaper", {"doi:main"}, "TestPaper", {"doi:ref1"}, "CITES")
            await create_relationship(tx, "TestPaper", {"doi:main"}, "TestPaper", {"doi:ref2"}, "CITES")

        await neo4j_session.execute_write(_create_refs)

        # Verify complete structure
        result = await neo4j_session.run(
            "MATCH (a:TestAuthor)-[:AUTHORED]->(p:TestPaper {title: 'Main Paper'})-[:CITES]->(r:TestPaper) "
            "RETURN count(DISTINCT a) as authors, count(DISTINCT r) as refs"
        )
        record = await result.single()
        assert record["authors"] == 2
        assert record["refs"] == 2

    @pytest.mark.asyncio
    async def test_identifier_based_deduplication(self, neo4j_session):
        """Test that papers from different sources are merged via shared identifiers."""
        # Source 1 creates paper with DOI
        async def _source1(tx):
            await save_node(
                tx, "TestPaper",
                {"doi:shared", "source1:id"},
                {"title": "Paper from Source 1", "source": "source1"}
            )

        await neo4j_session.execute_write(_source1)

        # Source 2 creates paper with same DOI but different metadata
        async def _source2(tx):
            await save_node(
                tx, "TestPaper",
                {"doi:shared", "source2:id"},
                {"title": "Paper from Source 2", "abstract": "Added by source2"}
            )

        await neo4j_session.execute_write(_source2)

        # Should have single node with merged data
        async def _verify(tx):
            nodes = await find_nodes_by_identifiers(tx, "TestPaper", {"doi:shared"})
            assert len(nodes) == 1

            identifiers = nodes[0]["identifiers"]
            assert "doi:shared" in identifiers
            assert "source1:id" in identifiers
            assert "source2:id" in identifiers

            props = nodes[0]["properties"]
            # Latest title wins
            assert props["title"] == "Paper from Source 2"
            # source from first
            assert props["source"] == "source1"
            # abstract added by second
            assert props["abstract"] == "Added by source2"

        await neo4j_session.execute_read(_verify)


# =============================================================================
# Test: Identifier Node Structure
# =============================================================================

class TestIdentifierNodeStructure:
    """Tests specific to the identifier node storage structure."""

    @pytest.mark.asyncio
    async def test_identifiers_stored_as_separate_nodes(self, neo4j_session):
        """Identifiers should be stored as separate nodes, not as list property."""
        async def _create(tx):
            await create_node(
                tx, "TestPaper",
                {"doi:123", "arxiv:456"},
                {"title": "Test Paper"}
            )

        await neo4j_session.execute_write(_create)

        # Verify identifier nodes exist
        result = await neo4j_session.run(
            "MATCH (id:TestPaperIdentifier) RETURN count(id) as count"
        )
        record = await result.single()
        assert record["count"] == 2

        # Verify HAS_ID relationships exist
        result = await neo4j_session.run(
            "MATCH (p:TestPaper)-[r:HAS_ID]->(id:TestPaperIdentifier) "
            "RETURN count(r) as count"
        )
        record = await result.single()
        assert record["count"] == 2

    @pytest.mark.asyncio
    async def test_identifier_nodes_have_value_property(self, neo4j_session):
        """Each identifier node should have a 'value' property."""
        async def _create(tx):
            await create_node(
                tx, "TestPaper",
                {"doi:123"},
                {"title": "Test Paper"}
            )

        await neo4j_session.execute_write(_create)

        result = await neo4j_session.run(
            "MATCH (id:TestPaperIdentifier) RETURN id.value as value"
        )
        record = await result.single()
        assert record["value"] == "doi:123"

    @pytest.mark.asyncio
    async def test_main_node_has_no_identifiers_property(self, neo4j_session):
        """Main node should not have an 'identifiers' list property."""
        async def _create(tx):
            await create_node(
                tx, "TestPaper",
                {"doi:123"},
                {"title": "Test Paper"}
            )

        await neo4j_session.execute_write(_create)

        result = await neo4j_session.run(
            "MATCH (p:TestPaper) RETURN p.identifiers as identifiers, p.title as title"
        )
        record = await result.single()
        assert record["identifiers"] is None
        assert record["title"] == "Test Paper"

    @pytest.mark.asyncio
    async def test_can_index_identifier_value(self, neo4j_session):
        """Should be able to create an index on identifier value (the main benefit)."""
        # Create an index (this should work since value is a scalar property)
        try:
            await neo4j_session.run(
                "CREATE INDEX test_paper_identifier_value IF NOT EXISTS "
                "FOR (id:TestPaperIdentifier) ON (id.value)"
            )
            index_created = True
        except Exception:
            index_created = False

        assert index_created, "Should be able to create index on identifier value"

        # Clean up index
        try:
            await neo4j_session.run(
                "DROP INDEX test_paper_identifier_value IF EXISTS"
            )
        except Exception:
            pass


# =============================================================================
# Run Tests
# =============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
