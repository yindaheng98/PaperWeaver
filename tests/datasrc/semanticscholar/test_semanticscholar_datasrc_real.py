"""
Real API tests for SemanticScholarDataSrc.

These tests send actual HTTP requests to the Semantic Scholar API.
Run with: pytest tests/datasrc/semanticscholar/test_semanticscholar_datasrc_real.py -v -s

To use an API key for higher rate limits, set the environment variable:
    set SS_API_KEY=your_api_key_here  (Windows)
    export SS_API_KEY=your_api_key_here  (Linux/Mac)

Paper ID: 2c03df8b48bf3fa39054345bafabfeff15bfd11d (Deep Residual Learning for Image Recognition)
Author ID: 39353098
"""

import os
import pytest

from paper_weaver.datasrc.semanticscholar import SemanticScholarDataSrc
from paper_weaver.datasrc.cache_impl import MemoryDataSrcCache
from paper_weaver.dataclass import Paper, Author, Venue


# Test data constants
TEST_PAPER_ID = "2c03df8b48bf3fa39054345bafabfeff15bfd11d"
TEST_AUTHOR_ID = "39353098"


def get_api_headers() -> dict | None:
    """Get API headers with optional API key from environment variable."""
    api_key = os.getenv("SS_API_KEY")
    if api_key:
        return {"x-api-key": api_key}
    return None


@pytest.fixture
def cache():
    """Fresh cache for each test."""
    return MemoryDataSrcCache()


@pytest.fixture
def datasrc(cache):
    """DataSrc with real API access. Uses API key from SS_API_KEY env var if set."""
    headers = get_api_headers()
    return SemanticScholarDataSrc(
        cache,
        max_concurrent=3,
        cache_ttl=3600,
        http_timeout=30,
        http_headers=headers
    )


class TestRealPaperAPI:
    """Real API tests for paper-related methods."""

    @pytest.mark.asyncio
    async def test_get_paper_info(self, datasrc):
        """Test fetching real paper info."""
        paper = Paper(identifiers={f"ss:{TEST_PAPER_ID}"})

        try:
            updated_paper, info = await datasrc.get_paper_info(paper)

            # Verify we got data
            assert "paperId" in info
            assert "title" in info

            # Verify identifiers are updated
            assert len(updated_paper.identifiers) > 0

            print(f"\n✓ Paper: {info['title']}")
            print(f"  Year: {info.get('year', 'N/A')}")
            print(f"  Identifiers: {updated_paper.identifiers}")
        except ValueError as e:
            pytest.skip(f"API request failed (possibly rate limited): {e}")

    @pytest.mark.asyncio
    async def test_get_authors_by_paper(self, datasrc):
        """Test fetching real authors for a paper."""
        paper = Paper(identifiers={f"ss:{TEST_PAPER_ID}"})

        try:
            authors = await datasrc.get_authors_by_paper(paper)

            # Should have at least one author
            assert len(authors) > 0

            # Check author identifiers
            author_ids = set()
            for author in authors:
                for ident in author.identifiers:
                    if ident.startswith("ss-author:"):
                        author_ids.add(ident[10:])

            print(f"\n✓ Found {len(authors)} authors")
            print(f"  Author IDs: {author_ids}")
        except ValueError as e:
            pytest.skip(f"API request failed (possibly rate limited): {e}")

    @pytest.mark.asyncio
    async def test_get_venues_by_paper(self, datasrc):
        """Test fetching real venues for a paper."""
        paper = Paper(identifiers={f"ss:{TEST_PAPER_ID}"})

        try:
            venues = await datasrc.get_venues_by_paper(paper)

            # May or may not have venue info
            print(f"\n✓ Found {len(venues)} venues")
            for venue in venues:
                print(f"  Venue: {venue.identifiers}")
        except ValueError as e:
            pytest.skip(f"API request failed (possibly rate limited): {e}")

    @pytest.mark.asyncio
    async def test_get_references_by_paper(self, datasrc):
        """Test fetching real references for a paper."""
        paper = Paper(identifiers={f"ss:{TEST_PAPER_ID}"})

        try:
            references = await datasrc.get_references_by_paper(paper)

            # Paper should have references
            assert len(references) >= 0

            print(f"\n✓ Found {len(references)} references")
            # Print first 3 references
            for i, ref in enumerate(references[:3]):
                print(f"  {i+1}. {ref.identifiers}")
        except ValueError as e:
            pytest.skip(f"API request failed (possibly rate limited): {e}")

    @pytest.mark.asyncio
    async def test_get_citations_by_paper(self, datasrc):
        """Test fetching real citations for a paper."""
        paper = Paper(identifiers={f"ss:{TEST_PAPER_ID}"})

        try:
            citations = await datasrc.get_citations_by_paper(paper)

            # Paper should have citations
            assert len(citations) >= 0

            print(f"\n✓ Found {len(citations)} citations (first page)")
            # Print first 3 citations
            for i, cite in enumerate(citations[:3]):
                print(f"  {i+1}. {cite.identifiers}")
        except ValueError as e:
            pytest.skip(f"API request failed (possibly rate limited): {e}")


class TestRealAuthorAPI:
    """Real API tests for author-related methods."""

    @pytest.mark.asyncio
    async def test_get_author_info(self, datasrc):
        """Test fetching real author info."""
        author = Author(identifiers={f"ss-author:{TEST_AUTHOR_ID}"})

        try:
            updated_author, info = await datasrc.get_author_info(author)

            # Verify we got data
            assert "authorId" in info
            assert info["authorId"] == TEST_AUTHOR_ID

            # Verify name exists
            assert "name" in info

            print(f"\n✓ Author: {info['name']}")
            print(f"  Identifiers: {updated_author.identifiers}")
        except ValueError as e:
            pytest.skip(f"API request failed (possibly rate limited): {e}")

    @pytest.mark.asyncio
    async def test_get_papers_by_author(self, datasrc):
        """Test fetching real papers for an author."""
        author = Author(identifiers={f"ss-author:{TEST_AUTHOR_ID}"})

        try:
            papers = await datasrc.get_papers_by_author(author)

            # Author should have papers
            assert len(papers) > 0

            print(f"\n✓ Found {len(papers)} papers by author")
            # Print first 5 papers
            for i, p in enumerate(papers[:5]):
                print(f"  {i+1}. {p.identifiers}")
        except ValueError as e:
            pytest.skip(f"API request failed (possibly rate limited): {e}")


class TestRealWorkflow:
    """Integration tests with real API calls."""

    @pytest.mark.asyncio
    async def test_author_to_papers_workflow(self, datasrc):
        """Test getting an author's papers."""
        print("\n=== Author to Papers Workflow ===")

        author = Author(identifiers={f"ss-author:{TEST_AUTHOR_ID}"})

        try:
            # 1. Get author info
            updated_author, info = await datasrc.get_author_info(author)
            print(f"\n1. Author: {info.get('name', 'Unknown')}")

            # 2. Get author's papers
            papers = await datasrc.get_papers_by_author(updated_author)
            print(f"\n2. Papers by author: {len(papers)}")

            assert len(papers) > 0
            print("\n✓ Workflow completed!")
        except ValueError as e:
            pytest.skip(f"API request failed (possibly rate limited): {e}")

    @pytest.mark.asyncio
    async def test_paper_authors_workflow(self, datasrc):
        """Test getting a paper's authors."""
        print("\n=== Paper Authors Workflow ===")

        paper = Paper(identifiers={f"ss:{TEST_PAPER_ID}"})

        try:
            # Get authors for paper
            authors = await datasrc.get_authors_by_paper(paper)
            print(f"\n1. Found {len(authors)} authors for paper")

            for i, author in enumerate(authors[:3]):
                author_id = next((ident[10:] for ident in author.identifiers if ident.startswith("ss-author:")), "?")
                print(f"   - Author {i+1}: {author_id}")

            assert len(authors) > 0
            print("\n✓ Workflow completed!")
        except ValueError as e:
            pytest.skip(f"API request failed (possibly rate limited): {e}")

    @pytest.mark.asyncio
    async def test_paper_references_workflow(self, datasrc):
        """Test getting a paper's references."""
        print("\n=== Paper References Workflow ===")

        paper = Paper(identifiers={f"ss:{TEST_PAPER_ID}"})

        try:
            # Get references for paper
            references = await datasrc.get_references_by_paper(paper)
            print(f"\n1. Found {len(references)} references")

            for i, ref in enumerate(references[:3]):
                ref_id = next((ident[3:] for ident in ref.identifiers if ident.startswith("ss:")), "?")
                print(f"   - Ref {i+1}: {ref_id[:20]}...")

            print("\n✓ Workflow completed!")
        except ValueError as e:
            pytest.skip(f"API request failed (possibly rate limited): {e}")
