"""
Unit tests for cache implementations.

Tests the memory-based cache implementation to verify correctness
against the upper layer interface requirements.
"""

import pytest
import asyncio
from typing import Tuple

from paper_weaver.dataclass import Paper, Author, DataSrc, DataDst
from paper_weaver.cache import (
    # Memory storage components
    MemoryIdentifierRegistry,
    MemoryInfoStorage,
    MemoryCommittedLinkStorage,
    MemoryPendingListStorage,
    # Manager classes
    EntityInfoManager,
    PendingListManager,
    # Composite caches
    ComposableCacheBase,
    AuthorLinkCache,
    PaperLinkCache,
    Author2PapersCache,
    Paper2AuthorsCache,
    Paper2ReferencesCache,
    Paper2CitationsCache,
    FullAuthorWeaverCache,
    FullPaperWeaverCache,
    # Factory
    create_memory_author_weaver_cache,
    create_memory_paper_weaver_cache,
    HybridCacheBuilder,
)


# =============================================================================
# Test: MemoryIdentifierRegistry
# =============================================================================

class TestMemoryIdentifierRegistry:
    """Tests for MemoryIdentifierRegistry."""

    @pytest.fixture
    def registry(self):
        return MemoryIdentifierRegistry()

    @pytest.mark.asyncio
    async def test_register_new_identifiers(self, registry):
        """Test registering new identifiers creates a canonical ID."""
        canonical_id = await registry.register({"doi:123", "arxiv:456"})
        assert canonical_id is not None
        assert canonical_id.startswith("id_")

    @pytest.mark.asyncio
    async def test_get_canonical_id_not_registered(self, registry):
        """Test getting canonical ID for unregistered identifiers returns None."""
        result = await registry.get_canonical_id({"unknown:999"})
        assert result is None

    @pytest.mark.asyncio
    async def test_get_canonical_id_after_registration(self, registry):
        """Test getting canonical ID after registration."""
        await registry.register({"doi:123"})
        canonical_id = await registry.get_canonical_id({"doi:123"})
        assert canonical_id is not None

    @pytest.mark.asyncio
    async def test_register_merges_overlapping_identifiers(self, registry):
        """Test that registering overlapping identifiers merges them."""
        cid1 = await registry.register({"doi:123"})
        cid2 = await registry.register({"arxiv:456"})
        # Now register with both - should merge
        cid3 = await registry.register({"doi:123", "arxiv:456"})
        
        # After merge, both should resolve to same canonical ID
        all_ids = await registry.get_all_identifiers(cid3)
        assert "doi:123" in all_ids
        assert "arxiv:456" in all_ids

    @pytest.mark.asyncio
    async def test_get_all_identifiers(self, registry):
        """Test getting all identifiers for a canonical ID."""
        await registry.register({"doi:123", "arxiv:456", "pmid:789"})
        canonical_id = await registry.get_canonical_id({"doi:123"})
        all_ids = await registry.get_all_identifiers(canonical_id)
        assert all_ids == {"doi:123", "arxiv:456", "pmid:789"}

    @pytest.mark.asyncio
    async def test_iterate_canonical_ids(self, registry):
        """Test iterating over all canonical IDs."""
        await registry.register({"doi:1"})
        await registry.register({"doi:2"})
        await registry.register({"doi:3"})
        
        canonical_ids = []
        async for cid in registry.iterate_canonical_ids():
            canonical_ids.append(cid)
        
        assert len(canonical_ids) == 3

    @pytest.mark.asyncio
    async def test_register_same_identifiers_returns_same_canonical_id(self, registry):
        """Test registering same identifiers returns same canonical ID."""
        cid1 = await registry.register({"doi:123"})
        cid2 = await registry.register({"doi:123"})
        assert cid1 == cid2

    @pytest.mark.asyncio
    async def test_merge_multiple_canonical_ids(self, registry):
        """Test merging multiple distinct canonical IDs into one."""
        cid1 = await registry.register({"id:A"})
        cid2 = await registry.register({"id:B"})
        cid3 = await registry.register({"id:C"})
        
        # Merge A, B, C by registering overlapping sets
        await registry.register({"id:A", "id:B"})
        await registry.register({"id:B", "id:C"})
        
        # All should now be under the same canonical ID
        final_cid = await registry.get_canonical_id({"id:C"})
        all_ids = await registry.get_all_identifiers(final_cid)
        assert {"id:A", "id:B", "id:C"}.issubset(all_ids)


# =============================================================================
# Test: MemoryInfoStorage
# =============================================================================

class TestMemoryInfoStorage:
    """Tests for MemoryInfoStorage."""

    @pytest.fixture
    def storage(self):
        return MemoryInfoStorage()

    @pytest.mark.asyncio
    async def test_get_info_not_set(self, storage):
        """Test getting info that hasn't been set returns None."""
        result = await storage.get_info("cid_123")
        assert result is None

    @pytest.mark.asyncio
    async def test_set_and_get_info(self, storage):
        """Test setting and getting info."""
        info = {"title": "Test Paper", "year": 2024}
        await storage.set_info("cid_123", info)
        result = await storage.get_info("cid_123")
        assert result == info

    @pytest.mark.asyncio
    async def test_overwrite_info(self, storage):
        """Test overwriting existing info."""
        await storage.set_info("cid_123", {"title": "Old"})
        await storage.set_info("cid_123", {"title": "New"})
        result = await storage.get_info("cid_123")
        assert result["title"] == "New"


# =============================================================================
# Test: MemoryCommittedLinkStorage
# =============================================================================

class TestMemoryCommittedLinkStorage:
    """Tests for MemoryCommittedLinkStorage."""

    @pytest.fixture
    def storage(self):
        return MemoryCommittedLinkStorage()

    @pytest.mark.asyncio
    async def test_is_link_committed_not_set(self, storage):
        """Test checking uncommitted link returns False."""
        result = await storage.is_link_committed("paper1", "author1")
        assert result is False

    @pytest.mark.asyncio
    async def test_commit_and_check_link(self, storage):
        """Test committing and checking a link."""
        await storage.commit_link("paper1", "author1")
        result = await storage.is_link_committed("paper1", "author1")
        assert result is True

    @pytest.mark.asyncio
    async def test_link_directionality(self, storage):
        """Test that links are directional (A->B != B->A)."""
        await storage.commit_link("paper1", "author1")
        # Forward direction should be committed
        assert await storage.is_link_committed("paper1", "author1") is True
        # Reverse direction should NOT be committed
        assert await storage.is_link_committed("author1", "paper1") is False

    @pytest.mark.asyncio
    async def test_multiple_links_from_same_source(self, storage):
        """Test multiple links from the same source."""
        await storage.commit_link("paper1", "author1")
        await storage.commit_link("paper1", "author2")
        await storage.commit_link("paper1", "author3")
        
        assert await storage.is_link_committed("paper1", "author1") is True
        assert await storage.is_link_committed("paper1", "author2") is True
        assert await storage.is_link_committed("paper1", "author3") is True
        assert await storage.is_link_committed("paper1", "author4") is False


# =============================================================================
# Test: MemoryPendingListStorage
# =============================================================================

class TestMemoryPendingListStorage:
    """Tests for MemoryPendingListStorage."""

    @pytest.fixture
    def storage(self):
        return MemoryPendingListStorage()

    @pytest.mark.asyncio
    async def test_get_pending_not_set(self, storage):
        """Test getting pending list that hasn't been set returns None."""
        result = await storage.get_pending_identifier_sets("author1")
        assert result is None

    @pytest.mark.asyncio
    async def test_set_and_get_pending(self, storage):
        """Test setting and getting pending list."""
        items = [{"doi:1"}, {"doi:2", "arxiv:2"}]
        await storage.set_pending_identifier_sets("author1", items)
        result = await storage.get_pending_identifier_sets("author1")
        assert len(result) == 2
        assert {"doi:1"} in result
        assert {"doi:2", "arxiv:2"} in result

    @pytest.mark.asyncio
    async def test_set_empty_list_vs_not_set(self, storage):
        """Test that empty list is different from not set."""
        await storage.set_pending_identifier_sets("author1", [])
        result = await storage.get_pending_identifier_sets("author1")
        assert result is not None
        assert result == []

    @pytest.mark.asyncio
    async def test_overwrite_pending(self, storage):
        """Test overwriting pending list."""
        await storage.set_pending_identifier_sets("author1", [{"doi:1"}])
        await storage.set_pending_identifier_sets("author1", [{"doi:2"}, {"doi:3"}])
        result = await storage.get_pending_identifier_sets("author1")
        assert len(result) == 2


# =============================================================================
# Test: EntityInfoManager
# =============================================================================

class TestEntityInfoManager:
    """Tests for EntityInfoManager."""

    @pytest.fixture
    def manager(self):
        registry = MemoryIdentifierRegistry()
        storage = MemoryInfoStorage()
        return EntityInfoManager(registry, storage)

    @pytest.mark.asyncio
    async def test_get_info_unregistered(self, manager):
        """Test getting info for unregistered entity."""
        canonical_id, all_ids, info = await manager.get_info({"doi:123"})
        assert canonical_id is None
        assert all_ids == {"doi:123"}
        assert info is None

    @pytest.mark.asyncio
    async def test_set_and_get_info(self, manager):
        """Test setting and getting info."""
        info = {"title": "Test Paper"}
        await manager.set_info({"doi:123"}, info)
        
        canonical_id, all_ids, retrieved_info = await manager.get_info({"doi:123"})
        assert canonical_id is not None
        assert "doi:123" in all_ids
        assert retrieved_info == info

    @pytest.mark.asyncio
    async def test_register_identifiers(self, manager):
        """Test registering identifiers without setting info."""
        canonical_id, all_ids = await manager.register_identifiers({"doi:123", "arxiv:456"})
        assert canonical_id is not None
        assert "doi:123" in all_ids
        assert "arxiv:456" in all_ids

    @pytest.mark.asyncio
    async def test_identifier_merging_on_get_info(self, manager):
        """Test that get_info merges identifiers."""
        await manager.set_info({"doi:123"}, {"title": "Test"})
        
        # Query with additional identifier
        canonical_id, all_ids, info = await manager.get_info({"doi:123", "arxiv:456"})
        
        # Both identifiers should now be associated
        assert "doi:123" in all_ids
        assert "arxiv:456" in all_ids

    @pytest.mark.asyncio
    async def test_iterate_entities(self, manager):
        """Test iterating over registered entities."""
        await manager.set_info({"doi:1"}, {"title": "Paper 1"})
        await manager.set_info({"doi:2"}, {"title": "Paper 2"})
        
        entities = []
        async for canonical_id, identifiers in manager.iterate_entities():
            entities.append((canonical_id, identifiers))
        
        assert len(entities) == 2


# =============================================================================
# Test: PendingListManager
# =============================================================================

class TestPendingListManager:
    """Tests for PendingListManager."""

    @pytest.fixture
    def manager(self):
        registry = MemoryIdentifierRegistry()
        storage = MemoryPendingListStorage()
        return PendingListManager(registry, storage)

    @pytest.mark.asyncio
    async def test_get_pending_not_set(self, manager):
        """Test getting pending list that hasn't been set."""
        result = await manager.get_pending_identifier_sets("source_cid")
        assert result is None

    @pytest.mark.asyncio
    async def test_add_and_get_pending(self, manager):
        """Test adding and getting pending list."""
        items = [{"doi:1"}, {"doi:2"}]
        await manager.add_pending_identifier_sets("source_cid", items)
        result = await manager.get_pending_identifier_sets("source_cid")
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_add_pending_registers_entities(self, manager):
        """Test that adding pending list registers entities in registry."""
        items = [{"doi:1"}, {"doi:2"}]
        await manager.add_pending_identifier_sets("source_cid", items)
        
        # Entities should now be registered
        result = await manager.get_pending_canonical_id_identifier_set_dict("source_cid")
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_add_pending_merges_identifiers(self, manager):
        """Test that adding pending items merges with existing identifiers."""
        # First add
        await manager.add_pending_identifier_sets("source_cid", [{"doi:1"}])
        
        # Second add with overlapping identifier
        await manager.add_pending_identifier_sets("source_cid", [{"doi:1", "arxiv:1"}])
        
        result = await manager.get_pending_identifier_sets("source_cid")
        # Should still be 1 entity, but with merged identifiers
        assert len(result) == 1
        assert "doi:1" in result[0]
        assert "arxiv:1" in result[0]

    @pytest.mark.asyncio
    async def test_add_pending_appends_new_entities(self, manager):
        """Test that adding new pending items appends to list."""
        await manager.add_pending_identifier_sets("source_cid", [{"doi:1"}])
        await manager.add_pending_identifier_sets("source_cid", [{"doi:2"}])
        
        result = await manager.get_pending_identifier_sets("source_cid")
        assert len(result) == 2


# =============================================================================
# Test: ComposableCacheBase
# =============================================================================

class TestComposableCacheBase:
    """Tests for ComposableCacheBase."""

    @pytest.fixture
    def cache(self):
        return ComposableCacheBase(
            paper_registry=MemoryIdentifierRegistry(),
            paper_info_storage=MemoryInfoStorage(),
            author_registry=MemoryIdentifierRegistry(),
            author_info_storage=MemoryInfoStorage(),
        )

    @pytest.mark.asyncio
    async def test_get_paper_info_not_set(self, cache):
        """Test getting paper info that hasn't been set."""
        paper = Paper(identifiers={"doi:123"})
        paper, info = await cache.get_paper_info(paper)
        assert info is None

    @pytest.mark.asyncio
    async def test_set_and_get_paper_info(self, cache):
        """Test setting and getting paper info."""
        paper = Paper(identifiers={"doi:123"})
        info = {"title": "Test Paper", "year": 2024}
        
        await cache.set_paper_info(paper, info)
        paper, retrieved_info = await cache.get_paper_info(paper)
        
        assert retrieved_info == info

    @pytest.mark.asyncio
    async def test_paper_identifiers_merge_on_set(self, cache):
        """Test that paper identifiers are merged when setting info."""
        paper = Paper(identifiers={"doi:123", "arxiv:456"})
        await cache.set_paper_info(paper, {"title": "Test"})
        
        # Query with partial identifiers
        paper2 = Paper(identifiers={"doi:123"})
        paper2, info = await cache.get_paper_info(paper2)
        
        # Should have all identifiers
        assert "doi:123" in paper2.identifiers
        assert "arxiv:456" in paper2.identifiers

    @pytest.mark.asyncio
    async def test_get_author_info_not_set(self, cache):
        """Test getting author info that hasn't been set."""
        author = Author(identifiers={"orcid:0000-0001"})
        author, info = await cache.get_author_info(author)
        assert info is None

    @pytest.mark.asyncio
    async def test_set_and_get_author_info(self, cache):
        """Test setting and getting author info."""
        author = Author(identifiers={"orcid:0000-0001"})
        info = {"name": "John Doe"}
        
        await cache.set_author_info(author, info)
        author, retrieved_info = await cache.get_author_info(author)
        
        assert retrieved_info == info

    @pytest.mark.asyncio
    async def test_iterate_papers(self, cache):
        """Test iterating over registered papers."""
        paper1 = Paper(identifiers={"doi:1"})
        paper2 = Paper(identifiers={"doi:2"})
        
        await cache.set_paper_info(paper1, {"title": "Paper 1"})
        await cache.set_paper_info(paper2, {"title": "Paper 2"})
        
        papers = []
        async for paper in cache.iterate_papers():
            papers.append(paper)
        
        assert len(papers) == 2

    @pytest.mark.asyncio
    async def test_iterate_authors(self, cache):
        """Test iterating over registered authors."""
        author1 = Author(identifiers={"orcid:1"})
        author2 = Author(identifiers={"orcid:2"})
        
        await cache.set_author_info(author1, {"name": "Author 1"})
        await cache.set_author_info(author2, {"name": "Author 2"})
        
        authors = []
        async for author in cache.iterate_authors():
            authors.append(author)
        
        assert len(authors) == 2


# =============================================================================
# Test: AuthorLinkCache
# =============================================================================

class TestAuthorLinkCache:
    """Tests for AuthorLinkCache."""

    @pytest.fixture
    def cache(self):
        return AuthorLinkCache(
            paper_registry=MemoryIdentifierRegistry(),
            paper_info_storage=MemoryInfoStorage(),
            author_registry=MemoryIdentifierRegistry(),
            author_info_storage=MemoryInfoStorage(),
            committed_author_links=MemoryCommittedLinkStorage(),
        )

    @pytest.mark.asyncio
    async def test_is_author_link_committed_not_set(self, cache):
        """Test checking uncommitted author link."""
        paper = Paper(identifiers={"doi:123"})
        author = Author(identifiers={"orcid:0001"})
        
        result = await cache.is_author_link_committed(paper, author)
        assert result is False

    @pytest.mark.asyncio
    async def test_commit_and_check_author_link(self, cache):
        """Test committing and checking author link."""
        paper = Paper(identifiers={"doi:123"})
        author = Author(identifiers={"orcid:0001"})
        
        await cache.commit_author_link(paper, author)
        result = await cache.is_author_link_committed(paper, author)
        
        assert result is True

    @pytest.mark.asyncio
    async def test_link_works_with_merged_identifiers(self, cache):
        """Test that link checking works with merged identifiers."""
        paper = Paper(identifiers={"doi:123"})
        author = Author(identifiers={"orcid:0001"})
        
        await cache.commit_author_link(paper, author)
        
        # Check with additional identifiers
        paper2 = Paper(identifiers={"doi:123", "arxiv:456"})
        author2 = Author(identifiers={"orcid:0001", "scopus:0001"})
        
        result = await cache.is_author_link_committed(paper2, author2)
        assert result is True


# =============================================================================
# Test: PaperLinkCache
# =============================================================================

class TestPaperLinkCache:
    """Tests for PaperLinkCache."""

    @pytest.fixture
    def cache(self):
        return PaperLinkCache(
            paper_registry=MemoryIdentifierRegistry(),
            paper_info_storage=MemoryInfoStorage(),
            author_registry=MemoryIdentifierRegistry(),
            author_info_storage=MemoryInfoStorage(),
            committed_reference_links=MemoryCommittedLinkStorage(),
        )

    @pytest.mark.asyncio
    async def test_is_reference_link_committed_not_set(self, cache):
        """Test checking uncommitted reference link."""
        paper = Paper(identifiers={"doi:123"})
        reference = Paper(identifiers={"doi:456"})
        
        result = await cache.is_reference_link_committed(paper, reference)
        assert result is False

    @pytest.mark.asyncio
    async def test_commit_and_check_reference_link(self, cache):
        """Test committing and checking reference link."""
        paper = Paper(identifiers={"doi:123"})
        reference = Paper(identifiers={"doi:456"})
        
        await cache.commit_reference_link(paper, reference)
        result = await cache.is_reference_link_committed(paper, reference)
        
        assert result is True

    @pytest.mark.asyncio
    async def test_citation_link_is_inverse_of_reference(self, cache):
        """Test that citation link is inverse of reference link."""
        paper = Paper(identifiers={"doi:123"})
        citation = Paper(identifiers={"doi:456"})
        
        # "paper is cited by citation" means "citation references paper"
        await cache.commit_citation_link(paper, citation)
        
        # Check: paper is cited by citation
        assert await cache.is_citation_link_committed(paper, citation) is True
        # Internally: citation references paper
        assert await cache.is_reference_link_committed(citation, paper) is True


# =============================================================================
# Test: Author2PapersCache
# =============================================================================

class TestAuthor2PapersCache:
    """Tests for Author2PapersCache."""

    @pytest.fixture
    def cache(self):
        return Author2PapersCache(
            paper_registry=MemoryIdentifierRegistry(),
            paper_info_storage=MemoryInfoStorage(),
            author_registry=MemoryIdentifierRegistry(),
            author_info_storage=MemoryInfoStorage(),
            committed_author_links=MemoryCommittedLinkStorage(),
            pending_papers=MemoryPendingListStorage(),
        )

    @pytest.mark.asyncio
    async def test_get_pending_papers_not_set(self, cache):
        """Test getting pending papers that haven't been set."""
        author = Author(identifiers={"orcid:0001"})
        result = await cache.get_pending_papers_for_author(author)
        assert result is None

    @pytest.mark.asyncio
    async def test_add_and_get_pending_papers(self, cache):
        """Test adding and getting pending papers."""
        author = Author(identifiers={"orcid:0001"})
        papers = [
            Paper(identifiers={"doi:1"}),
            Paper(identifiers={"doi:2"}),
        ]
        
        await cache.add_pending_papers_for_author(author, papers)
        result = await cache.get_pending_papers_for_author(author)
        
        assert len(result) == 2
        assert any("doi:1" in p.identifiers for p in result)
        assert any("doi:2" in p.identifiers for p in result)

    @pytest.mark.asyncio
    async def test_pending_papers_are_registered(self, cache):
        """Test that pending papers are registered in the registry."""
        author = Author(identifiers={"orcid:0001"})
        papers = [Paper(identifiers={"doi:1"})]
        
        await cache.add_pending_papers_for_author(author, papers)
        
        # Paper should be discoverable via iteration
        found_papers = []
        async for paper in cache.iterate_papers():
            found_papers.append(paper)
        
        assert len(found_papers) == 1
        assert "doi:1" in found_papers[0].identifiers


# =============================================================================
# Test: Paper2AuthorsCache
# =============================================================================

class TestPaper2AuthorsCache:
    """Tests for Paper2AuthorsCache."""

    @pytest.fixture
    def cache(self):
        return Paper2AuthorsCache(
            paper_registry=MemoryIdentifierRegistry(),
            paper_info_storage=MemoryInfoStorage(),
            author_registry=MemoryIdentifierRegistry(),
            author_info_storage=MemoryInfoStorage(),
            committed_author_links=MemoryCommittedLinkStorage(),
            pending_authors=MemoryPendingListStorage(),
        )

    @pytest.mark.asyncio
    async def test_get_pending_authors_not_set(self, cache):
        """Test getting pending authors that haven't been set."""
        paper = Paper(identifiers={"doi:123"})
        result = await cache.get_pending_authors_for_paper(paper)
        assert result is None

    @pytest.mark.asyncio
    async def test_add_and_get_pending_authors(self, cache):
        """Test adding and getting pending authors."""
        paper = Paper(identifiers={"doi:123"})
        authors = [
            Author(identifiers={"orcid:1"}),
            Author(identifiers={"orcid:2"}),
        ]
        
        await cache.add_pending_authors_for_paper(paper, authors)
        result = await cache.get_pending_authors_for_paper(paper)
        
        assert len(result) == 2
        assert any("orcid:1" in a.identifiers for a in result)
        assert any("orcid:2" in a.identifiers for a in result)

    @pytest.mark.asyncio
    async def test_pending_authors_are_registered(self, cache):
        """Test that pending authors are registered in the registry."""
        paper = Paper(identifiers={"doi:123"})
        authors = [Author(identifiers={"orcid:1"})]
        
        await cache.add_pending_authors_for_paper(paper, authors)
        
        # Author should be discoverable via iteration
        found_authors = []
        async for author in cache.iterate_authors():
            found_authors.append(author)
        
        assert len(found_authors) == 1
        assert "orcid:1" in found_authors[0].identifiers


# =============================================================================
# Test: Paper2ReferencesCache
# =============================================================================

class TestPaper2ReferencesCache:
    """Tests for Paper2ReferencesCache."""

    @pytest.fixture
    def cache(self):
        return Paper2ReferencesCache(
            paper_registry=MemoryIdentifierRegistry(),
            paper_info_storage=MemoryInfoStorage(),
            author_registry=MemoryIdentifierRegistry(),
            author_info_storage=MemoryInfoStorage(),
            committed_reference_links=MemoryCommittedLinkStorage(),
            pending_references=MemoryPendingListStorage(),
        )

    @pytest.mark.asyncio
    async def test_get_pending_references_not_set(self, cache):
        """Test getting pending references that haven't been set."""
        paper = Paper(identifiers={"doi:123"})
        result = await cache.get_pending_references_for_paper(paper)
        assert result is None

    @pytest.mark.asyncio
    async def test_add_and_get_pending_references(self, cache):
        """Test adding and getting pending references."""
        paper = Paper(identifiers={"doi:123"})
        references = [
            Paper(identifiers={"doi:ref1"}),
            Paper(identifiers={"doi:ref2"}),
        ]
        
        await cache.add_pending_references_for_paper(paper, references)
        result = await cache.get_pending_references_for_paper(paper)
        
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_reference_link_commitment(self, cache):
        """Test reference link commitment."""
        paper = Paper(identifiers={"doi:123"})
        reference = Paper(identifiers={"doi:ref1"})
        
        await cache.commit_reference_link(paper, reference)
        assert await cache.is_reference_link_committed(paper, reference) is True


# =============================================================================
# Test: Paper2CitationsCache
# =============================================================================

class TestPaper2CitationsCache:
    """Tests for Paper2CitationsCache."""

    @pytest.fixture
    def cache(self):
        return Paper2CitationsCache(
            paper_registry=MemoryIdentifierRegistry(),
            paper_info_storage=MemoryInfoStorage(),
            author_registry=MemoryIdentifierRegistry(),
            author_info_storage=MemoryInfoStorage(),
            committed_reference_links=MemoryCommittedLinkStorage(),
            pending_citations=MemoryPendingListStorage(),
        )

    @pytest.mark.asyncio
    async def test_get_pending_citations_not_set(self, cache):
        """Test getting pending citations that haven't been set."""
        paper = Paper(identifiers={"doi:123"})
        result = await cache.get_pending_citations_for_paper(paper)
        assert result is None

    @pytest.mark.asyncio
    async def test_add_and_get_pending_citations(self, cache):
        """Test adding and getting pending citations."""
        paper = Paper(identifiers={"doi:123"})
        citations = [
            Paper(identifiers={"doi:cit1"}),
            Paper(identifiers={"doi:cit2"}),
        ]
        
        await cache.add_pending_citations_for_paper(paper, citations)
        result = await cache.get_pending_citations_for_paper(paper)
        
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_citation_link_commitment(self, cache):
        """Test citation link commitment."""
        paper = Paper(identifiers={"doi:123"})
        citation = Paper(identifiers={"doi:cit1"})
        
        await cache.commit_citation_link(paper, citation)
        assert await cache.is_citation_link_committed(paper, citation) is True


# =============================================================================
# Test: FullAuthorWeaverCache
# =============================================================================

class TestFullAuthorWeaverCache:
    """Tests for FullAuthorWeaverCache."""

    @pytest.fixture
    def cache(self):
        return create_memory_author_weaver_cache()

    @pytest.mark.asyncio
    async def test_paper_info_operations(self, cache):
        """Test paper info get/set operations."""
        paper = Paper(identifiers={"doi:123"})
        info = {"title": "Test Paper"}
        
        await cache.set_paper_info(paper, info)
        paper, retrieved = await cache.get_paper_info(paper)
        
        assert retrieved == info

    @pytest.mark.asyncio
    async def test_author_info_operations(self, cache):
        """Test author info get/set operations."""
        author = Author(identifiers={"orcid:0001"})
        info = {"name": "John Doe"}
        
        await cache.set_author_info(author, info)
        author, retrieved = await cache.get_author_info(author)
        
        assert retrieved == info

    @pytest.mark.asyncio
    async def test_author_to_papers_workflow(self, cache):
        """Test full author -> papers workflow."""
        author = Author(identifiers={"orcid:0001"})
        papers = [
            Paper(identifiers={"doi:1"}),
            Paper(identifiers={"doi:2"}),
        ]
        
        # Add pending papers
        await cache.add_pending_papers_for_author(author, papers)
        
        # Get pending papers
        result = await cache.get_pending_papers_for_author(author)
        assert len(result) == 2
        
        # Commit links
        for paper in papers:
            await cache.commit_author_link(paper, author)
        
        # Verify links committed
        for paper in papers:
            assert await cache.is_author_link_committed(paper, author) is True

    @pytest.mark.asyncio
    async def test_paper_to_authors_workflow(self, cache):
        """Test full paper -> authors workflow."""
        paper = Paper(identifiers={"doi:123"})
        authors = [
            Author(identifiers={"orcid:1"}),
            Author(identifiers={"orcid:2"}),
        ]
        
        # Add pending authors
        await cache.add_pending_authors_for_paper(paper, authors)
        
        # Get pending authors
        result = await cache.get_pending_authors_for_paper(paper)
        assert len(result) == 2
        
        # Commit links
        for author in authors:
            await cache.commit_author_link(paper, author)
        
        # Verify links committed
        for author in authors:
            assert await cache.is_author_link_committed(paper, author) is True


# =============================================================================
# Test: FullPaperWeaverCache
# =============================================================================

class TestFullPaperWeaverCache:
    """Tests for FullPaperWeaverCache."""

    @pytest.fixture
    def cache(self):
        return create_memory_paper_weaver_cache()

    @pytest.mark.asyncio
    async def test_paper_to_references_workflow(self, cache):
        """Test full paper -> references workflow."""
        paper = Paper(identifiers={"doi:123"})
        references = [
            Paper(identifiers={"doi:ref1"}),
            Paper(identifiers={"doi:ref2"}),
        ]
        
        # Add pending references
        await cache.add_pending_references_for_paper(paper, references)
        
        # Get pending references
        result = await cache.get_pending_references_for_paper(paper)
        assert len(result) == 2
        
        # Commit links
        for ref in references:
            await cache.commit_reference_link(paper, ref)
        
        # Verify links committed
        for ref in references:
            assert await cache.is_reference_link_committed(paper, ref) is True

    @pytest.mark.asyncio
    async def test_paper_to_citations_workflow(self, cache):
        """Test full paper -> citations workflow."""
        paper = Paper(identifiers={"doi:123"})
        citations = [
            Paper(identifiers={"doi:cit1"}),
            Paper(identifiers={"doi:cit2"}),
        ]
        
        # Add pending citations
        await cache.add_pending_citations_for_paper(paper, citations)
        
        # Get pending citations
        result = await cache.get_pending_citations_for_paper(paper)
        assert len(result) == 2
        
        # Commit links
        for cit in citations:
            await cache.commit_citation_link(paper, cit)
        
        # Verify links committed
        for cit in citations:
            assert await cache.is_citation_link_committed(paper, cit) is True


# =============================================================================
# Test: HybridCacheBuilder
# =============================================================================

class TestHybridCacheBuilder:
    """Tests for HybridCacheBuilder."""

    @pytest.mark.asyncio
    async def test_build_author_weaver_cache(self):
        """Test building author weaver cache with defaults."""
        builder = HybridCacheBuilder()
        cache = builder.build_author_weaver_cache()
        
        # Verify it works
        paper = Paper(identifiers={"doi:123"})
        await cache.set_paper_info(paper, {"title": "Test"})
        paper, info = await cache.get_paper_info(paper)
        assert info["title"] == "Test"

    @pytest.mark.asyncio
    async def test_build_paper_weaver_cache(self):
        """Test building paper weaver cache with defaults."""
        builder = HybridCacheBuilder()
        cache = builder.build_paper_weaver_cache()
        
        # Verify it works
        paper = Paper(identifiers={"doi:123"})
        await cache.set_paper_info(paper, {"title": "Test"})
        paper, info = await cache.get_paper_info(paper)
        assert info["title"] == "Test"

    @pytest.mark.asyncio
    async def test_with_all_memory(self):
        """Test with_all_memory convenience method."""
        builder = HybridCacheBuilder().with_all_memory()
        cache = builder.build_author_weaver_cache()
        
        # Verify it works
        paper = Paper(identifiers={"doi:123"})
        await cache.set_paper_info(paper, {"title": "Test"})
        paper, info = await cache.get_paper_info(paper)
        assert info["title"] == "Test"


# =============================================================================
# Integration Tests: Simulating Upper Layer Workflows
# =============================================================================

class TestIntegrationAuthor2PapersWorkflow:
    """
    Integration tests simulating the Author2PapersWeaverIface workflow.
    
    Verifies the cache behaves correctly for the author_to_papers method.
    """

    @pytest.fixture
    def cache(self):
        return create_memory_author_weaver_cache()

    @pytest.mark.asyncio
    async def test_complete_author_to_papers_cycle(self, cache):
        """
        Test complete author -> papers workflow as expected by iface_a2p.py.
        
        Steps:
        1. Get author info from cache (returns None initially)
        2. Set author info after fetching from source
        3. Get pending papers (returns None initially)
        4. Add pending papers after fetching from source
        5. Get pending papers again (should return papers)
        6. For each paper: get info, set info, commit link
        """
        author = Author(identifiers={"orcid:0001"})
        
        # Step 1 & 2: Author info
        author, info = await cache.get_author_info(author)
        assert info is None
        
        author_info = {"name": "John Doe", "affiliation": "MIT"}
        await cache.set_author_info(author, author_info)
        
        author, info = await cache.get_author_info(author)
        assert info == author_info
        
        # Step 3 & 4: Pending papers
        papers = await cache.get_pending_papers_for_author(author)
        assert papers is None
        
        pending_papers = [
            Paper(identifiers={"doi:paper1"}),
            Paper(identifiers={"doi:paper2"}),
        ]
        await cache.add_pending_papers_for_author(author, pending_papers)
        
        # Step 5: Get pending papers again
        papers = await cache.get_pending_papers_for_author(author)
        assert len(papers) == 2
        
        # Step 6: Process each paper
        for paper in papers:
            paper, paper_info = await cache.get_paper_info(paper)
            assert paper_info is None  # Not set yet
            
            # Simulate fetching info
            new_info = {"title": f"Paper {paper.identifiers}"}
            await cache.set_paper_info(paper, new_info)
            
            # Verify link not committed
            assert await cache.is_author_link_committed(paper, author) is False
            
            # Commit link
            await cache.commit_author_link(paper, author)
            assert await cache.is_author_link_committed(paper, author) is True

    @pytest.mark.asyncio
    async def test_idempotent_operations(self, cache):
        """Test that operations are idempotent."""
        author = Author(identifiers={"orcid:0001"})
        paper = Paper(identifiers={"doi:123"})
        
        # Set info multiple times
        await cache.set_author_info(author, {"name": "v1"})
        await cache.set_author_info(author, {"name": "v2"})
        author, info = await cache.get_author_info(author)
        assert info["name"] == "v2"
        
        # Commit link multiple times should not error
        await cache.commit_author_link(paper, author)
        await cache.commit_author_link(paper, author)
        assert await cache.is_author_link_committed(paper, author) is True


class TestIntegrationPaper2AuthorsWorkflow:
    """
    Integration tests simulating the Paper2AuthorsWeaverIface workflow.
    """

    @pytest.fixture
    def cache(self):
        return create_memory_author_weaver_cache()

    @pytest.mark.asyncio
    async def test_complete_paper_to_authors_cycle(self, cache):
        """Test complete paper -> authors workflow."""
        paper = Paper(identifiers={"doi:123"})
        
        # Get paper info (None initially)
        paper, info = await cache.get_paper_info(paper)
        assert info is None
        
        # Set paper info
        await cache.set_paper_info(paper, {"title": "Test Paper"})
        
        # Get pending authors (None initially)
        authors = await cache.get_pending_authors_for_paper(paper)
        assert authors is None
        
        # Add pending authors
        pending_authors = [
            Author(identifiers={"orcid:1"}),
            Author(identifiers={"orcid:2"}),
        ]
        await cache.add_pending_authors_for_paper(paper, pending_authors)
        
        # Get pending authors
        authors = await cache.get_pending_authors_for_paper(paper)
        assert len(authors) == 2
        
        # Process each author
        for author in authors:
            await cache.set_author_info(author, {"name": "Author"})
            await cache.commit_author_link(paper, author)
            assert await cache.is_author_link_committed(paper, author) is True


class TestIntegrationPaper2ReferencesWorkflow:
    """
    Integration tests simulating the Paper2ReferencesWeaverIface workflow.
    """

    @pytest.fixture
    def cache(self):
        return create_memory_paper_weaver_cache()

    @pytest.mark.asyncio
    async def test_complete_paper_to_references_cycle(self, cache):
        """Test complete paper -> references workflow."""
        paper = Paper(identifiers={"doi:123"})
        
        # Set paper info
        await cache.set_paper_info(paper, {"title": "Main Paper"})
        
        # Add pending references
        references = [
            Paper(identifiers={"doi:ref1"}),
            Paper(identifiers={"doi:ref2"}),
        ]
        await cache.add_pending_references_for_paper(paper, references)
        
        # Get and process references
        refs = await cache.get_pending_references_for_paper(paper)
        for ref in refs:
            await cache.set_paper_info(ref, {"title": "Reference"})
            await cache.commit_reference_link(paper, ref)
            assert await cache.is_reference_link_committed(paper, ref) is True


class TestIntegrationPaper2CitationsWorkflow:
    """
    Integration tests simulating the Paper2CitationsWeaverIface workflow.
    """

    @pytest.fixture
    def cache(self):
        return create_memory_paper_weaver_cache()

    @pytest.mark.asyncio
    async def test_complete_paper_to_citations_cycle(self, cache):
        """Test complete paper -> citations workflow."""
        paper = Paper(identifiers={"doi:123"})
        
        # Set paper info
        await cache.set_paper_info(paper, {"title": "Main Paper"})
        
        # Add pending citations
        citations = [
            Paper(identifiers={"doi:cit1"}),
            Paper(identifiers={"doi:cit2"}),
        ]
        await cache.add_pending_citations_for_paper(paper, citations)
        
        # Get and process citations
        cits = await cache.get_pending_citations_for_paper(paper)
        for cit in cits:
            await cache.set_paper_info(cit, {"title": "Citation"})
            await cache.commit_citation_link(paper, cit)
            assert await cache.is_citation_link_committed(paper, cit) is True


# =============================================================================
# Edge Case Tests
# =============================================================================

class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    @pytest.fixture
    def cache(self):
        return create_memory_author_weaver_cache()

    @pytest.mark.asyncio
    async def test_empty_identifiers_set(self, cache):
        """Test handling of empty identifiers set."""
        # Note: In practice, entities should always have at least one identifier
        # This tests the edge case behavior
        paper = Paper(identifiers=set())
        
        # Should handle gracefully
        paper, info = await cache.get_paper_info(paper)
        assert info is None

    @pytest.mark.asyncio
    async def test_identifier_merging_across_operations(self, cache):
        """Test that identifier merging works across different operations."""
        # Create paper with identifier A
        paper_a = Paper(identifiers={"doi:A"})
        await cache.set_paper_info(paper_a, {"title": "Paper A"})
        
        # Create paper with identifier B
        paper_b = Paper(identifiers={"doi:B"})
        await cache.set_paper_info(paper_b, {"title": "Paper B"})
        
        # Now query with both identifiers - should merge
        paper_ab = Paper(identifiers={"doi:A", "doi:B"})
        paper_ab, info = await cache.get_paper_info(paper_ab)
        
        # After merge, both identifiers should be present
        assert "doi:A" in paper_ab.identifiers
        assert "doi:B" in paper_ab.identifiers

    @pytest.mark.asyncio
    async def test_concurrent_access(self, cache):
        """Test that concurrent access is handled correctly."""
        async def add_paper(i):
            paper = Paper(identifiers={f"doi:{i}"})
            await cache.set_paper_info(paper, {"title": f"Paper {i}"})
        
        # Run many concurrent operations
        await asyncio.gather(*[add_paper(i) for i in range(100)])
        
        # Verify all papers were added
        papers = []
        async for paper in cache.iterate_papers():
            papers.append(paper)
        
        assert len(papers) == 100

    @pytest.mark.asyncio
    async def test_pending_list_with_duplicate_entities(self, cache):
        """Test pending list handling when same entity is added multiple times."""
        author = Author(identifiers={"orcid:0001"})
        
        # Add same paper twice (with same identifier)
        papers1 = [Paper(identifiers={"doi:123"})]
        papers2 = [Paper(identifiers={"doi:123"})]
        
        await cache.add_pending_papers_for_author(author, papers1)
        await cache.add_pending_papers_for_author(author, papers2)
        
        # Should only have one paper (deduplicated by canonical ID)
        result = await cache.get_pending_papers_for_author(author)
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_pending_list_merges_overlapping_identifiers(self, cache):
        """Test that pending list merges overlapping identifiers."""
        author = Author(identifiers={"orcid:0001"})
        
        # Add paper with one identifier
        papers1 = [Paper(identifiers={"doi:123"})]
        await cache.add_pending_papers_for_author(author, papers1)
        
        # Add same paper with additional identifier
        papers2 = [Paper(identifiers={"doi:123", "arxiv:456"})]
        await cache.add_pending_papers_for_author(author, papers2)
        
        # Should have one paper with merged identifiers
        result = await cache.get_pending_papers_for_author(author)
        assert len(result) == 1
        assert "doi:123" in result[0].identifiers
        assert "arxiv:456" in result[0].identifiers


# =============================================================================
# Run Tests
# =============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v"])

