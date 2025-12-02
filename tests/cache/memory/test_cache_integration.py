"""
Integration tests simulating upper layer workflows.

Tests the cache implementation against the expected behavior
from iface_a2p.py, iface_p2a.py, iface_p2r.py, iface_p2c.py.
"""

import pytest

from paper_weaver.dataclass import Paper, Author
from paper_weaver.cache import (
    create_memory_author_weaver_cache,
    create_memory_paper_weaver_cache,
)


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
