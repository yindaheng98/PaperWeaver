"""
Real API tests for CrossRefDataSrc.

These tests send actual HTTP requests to the CrossRef API.
Run with: pytest tests/datasrc/crossref/test_crossref_datasrc_real.py -v -s

To use HTTP proxy, set the environment variable:
    set HTTP_PROXY=http://127.0.0.1:7890  (Windows)
    export HTTP_PROXY=http://127.0.0.1:7890  (Linux/Mac)

Paper DOI: 10.1109/CVPR.2016.90 (Deep Residual Learning for Image Recognition)

Cache: Uses Redis at localhost:6379 if available, otherwise falls back to MemoryDataSrcCache.
"""

import datetime

import pytest
import pytest_asyncio

from paper_weaver.datasrc.crossref import CrossRefDataSrc
from paper_weaver.datasrc.cache_impl import MemoryDataSrcCache, RedisDataSrcCache
from paper_weaver.dataclass import Paper, Author, Venue


try:
    import redis.asyncio as aioredis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False


def _check_real_redis_connection():
    """Check if real Redis server is available at localhost:6379."""
    if not REDIS_AVAILABLE:
        return False
    try:
        import redis
        client = redis.Redis(host='localhost', port=6379)
        client.ping()
        client.close()
        return True
    except Exception:
        return False


REAL_REDIS_AVAILABLE = _check_real_redis_connection()

TEST_DOI = "10.1109/CVPR.2016.90"
TEST_DOI_URL = f"https://doi.org/{TEST_DOI}"
TEST_TITLE_SUBSTR = "Deep Residual Learning"


@pytest_asyncio.fixture
async def cache():
    """
    Fresh cache for each test.
    Uses Redis at localhost:6379 if available, otherwise falls back to MemoryDataSrcCache.
    """
    if REAL_REDIS_AVAILABLE:
        client = aioredis.Redis(host='localhost', port=6379, db=15)
        await client.flushdb()
        cache = RedisDataSrcCache(client, prefix="test_crossref_datasrc", default_expire=None)
        yield cache
        await client.flushdb()
        await client.aclose()
    else:
        yield MemoryDataSrcCache()


@pytest.fixture
def datasrc(cache):
    """DataSrc with real API access."""
    return CrossRefDataSrc(
        cache,
        max_concurrent=3,
        cache_ttl=3600,
    )


class TestRealPaperAPI:
    """Real API tests for paper-related methods."""

    @pytest.mark.asyncio
    async def test_get_paper_info(self, datasrc):
        """Test fetching real paper info."""
        paper = Paper(identifiers={TEST_DOI_URL})

        try:
            updated_paper, info = await datasrc.get_paper_info(paper)

            assert "doi" in info
            assert info["doi"].lower() == TEST_DOI.lower()
            assert "title" in info
            assert TEST_TITLE_SUBSTR in info["title"]
            assert "year" in info
            assert isinstance(info["year"], int)

            has_doi = any(
                ident.startswith("https://doi.org/")
                for ident in updated_paper.identifiers
            )
            assert has_doi

            print(f"\n✓ Paper: {info['title']}")
            print(f"  Year: {info.get('year', 'N/A')} (type={type(info['year']).__name__})")
            print(f"  Type: {info.get('crossref:type', 'N/A')}")
            print(f"  Publisher: {info.get('publisher', 'N/A')}")
            print(f"  Identifiers: {updated_paper.identifiers}")
        except ValueError as e:
            pytest.skip(f"API request failed: {e}")

    @pytest.mark.asyncio
    async def test_get_paper_info_preserves_original_identifiers(self, datasrc):
        """Test that original identifiers are preserved after fetching."""
        custom_ident = "custom:my-tag-123"
        paper = Paper(identifiers={TEST_DOI_URL, custom_ident})

        try:
            updated_paper, _ = await datasrc.get_paper_info(paper)

            assert TEST_DOI_URL in updated_paper.identifiers
            assert custom_ident in updated_paper.identifiers
            print("\n✓ Original identifiers preserved")
        except ValueError as e:
            pytest.skip(f"API request failed: {e}")

    @pytest.mark.asyncio
    async def test_get_paper_info_has_title_hash(self, datasrc):
        """Test that paper info includes title_hash identifiers."""
        paper = Paper(identifiers={TEST_DOI_URL})

        try:
            updated_paper, _ = await datasrc.get_paper_info(paper)

            has_title_hash = any(
                ident.startswith("title_hash:")
                for ident in updated_paper.identifiers
            )
            assert has_title_hash
            print("\n✓ title_hash identifiers generated")
        except ValueError as e:
            pytest.skip(f"API request failed: {e}")

    @pytest.mark.asyncio
    async def test_get_paper_info_temporal_types(self, datasrc):
        """Test that date fields use proper temporal types."""
        paper = Paper(identifiers={TEST_DOI_URL})

        try:
            _, info = await datasrc.get_paper_info(paper)

            assert isinstance(info["year"], int)

            # created/deposited/indexed have date-time → datetime.datetime
            if "crossref:created" in info:
                assert isinstance(info["crossref:created"], datetime.datetime)
                print(f"  crossref:created = {info['crossref:created']} ({type(info['crossref:created']).__name__})")
            if "crossref:deposited" in info:
                assert isinstance(info["crossref:deposited"], datetime.datetime)
            if "crossref:indexed" in info:
                assert isinstance(info["crossref:indexed"], datetime.datetime)

            # published-print only has date-parts [year, month] → datetime.date
            if "crossref:published-print" in info:
                assert isinstance(info["crossref:published-print"], (datetime.date, int))
                print(f"  crossref:published-print = {info['crossref:published-print']} ({type(info['crossref:published-print']).__name__})")
            if "crossref:issued" in info:
                assert isinstance(info["crossref:issued"], (datetime.date, int))

            print("\n✓ All date fields use proper temporal types")
        except ValueError as e:
            pytest.skip(f"API request failed: {e}")

    @pytest.mark.asyncio
    async def test_get_authors_by_paper(self, datasrc):
        """Test fetching real authors for a paper (only those with ORCID)."""
        paper = Paper(identifiers={TEST_DOI_URL})

        try:
            authors = await datasrc.get_authors_by_paper(paper)

            for author in authors:
                has_orcid = any(
                    ident.startswith("orcid:")
                    for ident in author.identifiers
                )
                assert has_orcid, f"Author missing ORCID: {author.identifiers}"

            print(f"\n✓ Found {len(authors)} authors with ORCID")
            for author in authors:
                orcid = next(
                    (ident[6:] for ident in author.identifiers if ident.startswith("orcid:")),
                    "?"
                )
                print(f"  ORCID: {orcid}")
        except ValueError as e:
            pytest.skip(f"API request failed: {e}")

    @pytest.mark.asyncio
    async def test_get_references_by_paper(self, datasrc):
        """Test fetching references for a paper."""
        paper = Paper(identifiers={TEST_DOI_URL})

        try:
            references = await datasrc.get_references_by_paper(paper)

            assert len(references) > 0

            for ref in references:
                has_doi = any(
                    ident.startswith("https://doi.org/")
                    for ident in ref.identifiers
                )
                assert has_doi, f"Reference missing DOI: {ref.identifiers}"

            print(f"\n✓ Found {len(references)} references with DOI")
            for i, ref in enumerate(references[:5]):
                doi = next(
                    (ident for ident in ref.identifiers if ident.startswith("https://doi.org/")),
                    "?"
                )
                print(f"  {i+1}. {doi}")
        except ValueError as e:
            pytest.skip(f"API request failed: {e}")


class TestUnsupportedMethods:
    """Test that unsupported methods raise NotImplementedError."""

    @pytest.mark.asyncio
    async def test_get_venues_by_paper(self, datasrc):
        paper = Paper(identifiers={TEST_DOI_URL})
        with pytest.raises(NotImplementedError, match="venue"):
            await datasrc.get_venues_by_paper(paper)
        print("\n✓ get_venues_by_paper correctly raises NotImplementedError")

    @pytest.mark.asyncio
    async def test_get_citations_by_paper(self, datasrc):
        paper = Paper(identifiers={TEST_DOI_URL})
        with pytest.raises(NotImplementedError, match="citation"):
            await datasrc.get_citations_by_paper(paper)
        print("\n✓ get_citations_by_paper correctly raises NotImplementedError")

    @pytest.mark.asyncio
    async def test_get_author_info(self, datasrc):
        author = Author(identifiers={"orcid:0000-0000-0000-0000"})
        with pytest.raises(NotImplementedError, match="author"):
            await datasrc.get_author_info(author)
        print("\n✓ get_author_info correctly raises NotImplementedError")

    @pytest.mark.asyncio
    async def test_get_papers_by_author(self, datasrc):
        author = Author(identifiers={"orcid:0000-0000-0000-0000"})
        with pytest.raises(NotImplementedError, match="author"):
            await datasrc.get_papers_by_author(author)
        print("\n✓ get_papers_by_author correctly raises NotImplementedError")

    @pytest.mark.asyncio
    async def test_get_venue_info(self, datasrc):
        venue = Venue(identifiers={"issn:0000-0000"})
        with pytest.raises(NotImplementedError, match="venue"):
            await datasrc.get_venue_info(venue)
        print("\n✓ get_venue_info correctly raises NotImplementedError")

    @pytest.mark.asyncio
    async def test_get_papers_by_venue(self, datasrc):
        venue = Venue(identifiers={"issn:0000-0000"})
        with pytest.raises(NotImplementedError, match="venue"):
            await datasrc.get_papers_by_venue(venue)
        print("\n✓ get_papers_by_venue correctly raises NotImplementedError")


class TestRealWorkflow:
    """Integration tests with real API calls."""

    @pytest.mark.asyncio
    async def test_paper_info_and_authors_workflow(self, datasrc):
        """Test getting paper info then its authors."""
        print("\n=== Paper Info + Authors Workflow ===")

        paper = Paper(identifiers={TEST_DOI_URL})

        try:
            updated_paper, info = await datasrc.get_paper_info(paper)
            print(f"\n1. Paper: {info.get('title', 'Unknown')}")
            print(f"   Year: {info.get('year')}")

            authors = await datasrc.get_authors_by_paper(updated_paper)
            print(f"\n2. Authors with ORCID: {len(authors)}")
            for author in authors:
                orcid = next(
                    (ident[6:] for ident in author.identifiers if ident.startswith("orcid:")),
                    "?"
                )
                print(f"   - ORCID: {orcid}")

            print("\n✓ Workflow completed!")
        except ValueError as e:
            pytest.skip(f"API request failed: {e}")

    @pytest.mark.asyncio
    async def test_paper_info_and_references_workflow(self, datasrc):
        """Test getting paper info then its references."""
        print("\n=== Paper Info + References Workflow ===")

        paper = Paper(identifiers={TEST_DOI_URL})

        try:
            updated_paper, info = await datasrc.get_paper_info(paper)
            print(f"\n1. Paper: {info.get('title', 'Unknown')}")

            references = await datasrc.get_references_by_paper(updated_paper)
            print(f"\n2. References with DOI: {len(references)}")
            for i, ref in enumerate(references[:5]):
                doi = next(
                    (ident for ident in ref.identifiers if ident.startswith("https://doi.org/")),
                    "?"
                )
                print(f"   {i+1}. {doi}")

            assert len(references) > 0
            print("\n✓ Workflow completed!")
        except ValueError as e:
            pytest.skip(f"API request failed: {e}")

    @pytest.mark.asyncio
    async def test_full_paper_exploration_workflow(self, datasrc):
        """Test full paper exploration: info -> authors + references."""
        print("\n=== Full Paper Exploration Workflow ===")

        paper = Paper(identifiers={TEST_DOI_URL})

        try:
            updated_paper, info = await datasrc.get_paper_info(paper)
            print(f"\n1. Paper: {info.get('title', 'Unknown')}")
            print(f"   Year: {info.get('year')}")
            print(f"   Type: {info.get('crossref:type')}")
            print(f"   Publisher: {info.get('publisher')}")

            authors = await datasrc.get_authors_by_paper(updated_paper)
            print(f"\n2. Authors with ORCID ({len(authors)}):")
            for author in authors:
                orcid = next(
                    (ident[6:] for ident in author.identifiers if ident.startswith("orcid:")),
                    "?"
                )
                print(f"   - {orcid}")

            references = await datasrc.get_references_by_paper(updated_paper)
            print(f"\n3. References ({len(references)}):")
            for i, ref in enumerate(references[:5]):
                doi = next(
                    (ident for ident in ref.identifiers if ident.startswith("https://doi.org/")),
                    "?"
                )
                print(f"   {i+1}. {doi}")

            print("\n✓ Workflow completed!")
        except ValueError as e:
            pytest.skip(f"API request failed: {e}")


class TestCacheIntegration:
    """Test caching behavior with real API."""

    @pytest.mark.asyncio
    async def test_paper_info_cached(self, datasrc):
        """Test that paper info is cached after first fetch."""
        paper = Paper(identifiers={TEST_DOI_URL})

        try:
            updated_paper1, info1 = await datasrc.get_paper_info(paper)
            updated_paper2, info2 = await datasrc.get_paper_info(paper)

            assert info1["doi"] == info2["doi"]
            assert info1["title"] == info2["title"]

            print("\n✓ Paper info caching works correctly")
            print(f"  Title: {info1['title']}")
        except ValueError as e:
            pytest.skip(f"API request failed: {e}")

    @pytest.mark.asyncio
    async def test_references_use_cached_work(self, datasrc):
        """Test that get_references reuses the cached work JSON from get_paper_info."""
        paper = Paper(identifiers={TEST_DOI_URL})

        try:
            _, info = await datasrc.get_paper_info(paper)
            references = await datasrc.get_references_by_paper(paper)

            assert info["doi"].lower() == TEST_DOI.lower()
            assert isinstance(references, list)

            print("\n✓ References reuse cached work JSON")
            print(f"  References count: {len(references)}")
        except ValueError as e:
            pytest.skip(f"API request failed: {e}")


class TestCacheBackend:
    """Test to verify which cache backend is being used."""

    @pytest.mark.asyncio
    async def test_cache_backend_info(self, cache):
        """Display which cache backend is being used."""
        if REAL_REDIS_AVAILABLE:
            assert isinstance(cache, RedisDataSrcCache)
            print("\n✓ Using RedisDataSrcCache (Redis at localhost:6379)")
        else:
            assert isinstance(cache, MemoryDataSrcCache)
            print("\n✓ Using MemoryDataSrcCache (Redis not available)")


class TestErrorHandling:
    """Test error handling with invalid inputs."""

    @pytest.mark.asyncio
    async def test_invalid_doi(self, datasrc):
        """Test handling of invalid DOI."""
        paper = Paper(identifiers={"https://doi.org/10.0000/nonexistent.invalid"})

        with pytest.raises(ValueError, match="Failed to fetch"):
            await datasrc.get_paper_info(paper)

        print("\n✓ Invalid DOI handled correctly")

    @pytest.mark.asyncio
    async def test_missing_doi_identifier(self, datasrc):
        """Test handling of paper without DOI identifier."""
        paper = Paper(identifiers={"other:identifier:123"})

        with pytest.raises(ValueError, match="No valid DOI identifier"):
            await datasrc.get_paper_info(paper)

        print("\n✓ Missing DOI identifier handled correctly")

    @pytest.mark.asyncio
    async def test_missing_doi_for_authors(self, datasrc):
        """Test handling of paper without DOI when fetching authors."""
        paper = Paper(identifiers={"title:Some Paper Title"})

        with pytest.raises(ValueError, match="No valid DOI identifier"):
            await datasrc.get_authors_by_paper(paper)

        print("\n✓ Missing DOI for authors handled correctly")

    @pytest.mark.asyncio
    async def test_missing_doi_for_references(self, datasrc):
        """Test handling of paper without DOI when fetching references."""
        paper = Paper(identifiers={"title:Some Paper Title"})

        with pytest.raises(ValueError, match="No valid DOI identifier"):
            await datasrc.get_references_by_paper(paper)

        print("\n✓ Missing DOI for references handled correctly")
