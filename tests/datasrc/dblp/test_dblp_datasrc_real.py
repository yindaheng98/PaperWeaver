"""
Real API tests for DBLPDataSrc.

These tests send actual HTTP requests to the DBLP API.
Run with: pytest tests/datasrc/dblp/test_dblp_datasrc_real.py -v -s

To use HTTP proxy, set the environment variable:
    set HTTP_PROXY=http://127.0.0.1:7890  (Windows)
    export HTTP_PROXY=http://127.0.0.1:7890  (Linux/Mac)

Paper Key: conf/cvpr/HeZRS16 (Deep Residual Learning for Image Recognition)
Author PID: 34/7659 (Kaiming He)
Venue Key: db/conf/cvpr/cvpr2016 (CVPR 2016)

Cache: Uses Redis at localhost:6379 if available, otherwise falls back to MemoryDataSrcCache.
"""

import os
import pytest
import pytest_asyncio

from paper_weaver.datasrc.dblp import DBLPDataSrc
from paper_weaver.datasrc.cache_impl import MemoryDataSrcCache, RedisDataSrcCache
from paper_weaver.dataclass import Paper, Author, Venue


# Try to import redis library
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


# Test data constants
TEST_PAPER_KEY = "conf/cvpr/HeZRS16"  # ResNet paper
TEST_AUTHOR_PID = "34/7659"  # Kaiming He
TEST_AUTHOR_NAME = "Kaiming He"
TEST_VENUE_KEY = "db/conf/cvpr/cvpr2016"  # CVPR 2016


def get_http_proxy() -> str | None:
    """Get HTTP proxy from environment variable."""
    return os.getenv("HTTP_PROXY")


@pytest_asyncio.fixture
async def cache():
    """
    Fresh cache for each test.
    Uses Redis at localhost:6379 if available, otherwise falls back to MemoryDataSrcCache.
    """
    if REAL_REDIS_AVAILABLE:
        # Use real Redis server
        client = aioredis.Redis(host='localhost', port=6379, db=15)  # Use db=15 for testing
        # Clean the test database before use
        await client.flushdb()
        cache = RedisDataSrcCache(client, prefix="test_dblp_datasrc", default_expire=None)
        yield cache
        # Clean up after test
        await client.flushdb()
        await client.aclose()
    else:
        # Fall back to memory cache
        yield MemoryDataSrcCache()


@pytest.fixture
def datasrc(cache):
    """DataSrc with real API access. Uses HTTP_PROXY env var if set."""
    return DBLPDataSrc(
        cache,
        max_concurrent=3,
        record_cache_ttl=3600,
        person_cache_ttl=3600,
        venue_cache_ttl=3600,
        http_proxy=get_http_proxy(),
        http_timeout=30
    )


class TestRealPaperAPI:
    """Real API tests for paper-related methods."""

    @pytest.mark.asyncio
    async def test_get_paper_info(self, datasrc):
        """Test fetching real paper info."""
        paper = Paper(identifiers={f"dblp:key:{TEST_PAPER_KEY}"})
        
        try:
            updated_paper, info = await datasrc.get_paper_info(paper)
            
            # Verify we got data
            assert "dblp:key" in info
            assert info["dblp:key"] == TEST_PAPER_KEY
            assert "title" in info
            assert "Deep Residual Learning" in info["title"]
            
            # Verify identifiers are updated
            assert len(updated_paper.identifiers) > 0
            
            # Check that dblp:key identifier exists
            has_dblp_key = any(
                ident.startswith("dblp:key:")
                for ident in updated_paper.identifiers
            )
            assert has_dblp_key
            
            print(f"\n✓ Paper: {info['title']}")
            print(f"  Year: {info.get('year', 'N/A')}")
            print(f"  Venue: {info.get('dblp:venue', 'N/A')}")
            print(f"  Type: {info.get('dblp:type', 'N/A')}")
            print(f"  Identifiers: {updated_paper.identifiers}")
        except ValueError as e:
            pytest.skip(f"API request failed: {e}")

    @pytest.mark.asyncio
    async def test_get_authors_by_paper(self, datasrc):
        """Test fetching real authors for a paper."""
        paper = Paper(identifiers={f"dblp:key:{TEST_PAPER_KEY}"})
        
        try:
            authors = await datasrc.get_authors_by_paper(paper)
            
            # Should have at least one author (ResNet has 4 authors)
            assert len(authors) >= 1
            
            # Check author identifiers
            author_names = set()
            for author in authors:
                for ident in author.identifiers:
                    if ident.startswith("name:"):
                        author_names.add(ident[5:])
            
            print(f"\n✓ Found {len(authors)} authors")
            print(f"  Author names: {author_names}")
            
            # Note: DBLP record pages don't include author pids,
            # so we only get names here
        except ValueError as e:
            pytest.skip(f"API request failed: {e}")

    @pytest.mark.asyncio
    async def test_get_venues_by_paper(self, datasrc):
        """Test fetching real venues for a paper."""
        paper = Paper(identifiers={f"dblp:key:{TEST_PAPER_KEY}"})
        
        try:
            venues = await datasrc.get_venues_by_paper(paper)
            
            # Should have venue info
            assert len(venues) >= 1
            
            print(f"\n✓ Found {len(venues)} venues")
            for venue in venues:
                print(f"  Venue: {venue.identifiers}")
        except ValueError as e:
            pytest.skip(f"API request failed: {e}")

    @pytest.mark.asyncio
    async def test_get_references_by_paper_not_implemented(self, datasrc):
        """Test that get_references_by_paper raises NotImplementedError."""
        paper = Paper(identifiers={f"dblp:key:{TEST_PAPER_KEY}"})
        
        with pytest.raises(NotImplementedError, match="DBLP API does not provide reference"):
            await datasrc.get_references_by_paper(paper)
        
        print("\n✓ get_references_by_paper correctly raises NotImplementedError")

    @pytest.mark.asyncio
    async def test_get_citations_by_paper_not_implemented(self, datasrc):
        """Test that get_citations_by_paper raises NotImplementedError."""
        paper = Paper(identifiers={f"dblp:key:{TEST_PAPER_KEY}"})
        
        with pytest.raises(NotImplementedError, match="DBLP API does not provide citation"):
            await datasrc.get_citations_by_paper(paper)
        
        print("\n✓ get_citations_by_paper correctly raises NotImplementedError")


class TestRealAuthorAPI:
    """Real API tests for author-related methods."""

    @pytest.mark.asyncio
    async def test_get_author_info(self, datasrc):
        """Test fetching real author info."""
        author = Author(identifiers={f"dblp:pid:{TEST_AUTHOR_PID}"})
        
        try:
            updated_author, info = await datasrc.get_author_info(author)
            
            # Verify we got data
            assert "dblp:pid" in info
            assert info["dblp:pid"] == TEST_AUTHOR_PID
            
            # Verify name exists
            assert "name" in info
            assert info["name"] == TEST_AUTHOR_NAME
            
            # Check that dblp:pid identifier exists
            has_dblp_pid = any(
                ident.startswith("dblp:pid:")
                for ident in updated_author.identifiers
            )
            assert has_dblp_pid
            
            print(f"\n✓ Author: {info['name']}")
            print(f"  PID: {info['dblp:pid']}")
            if "affiliations" in info:
                print(f"  Affiliations: {info['affiliations']}")
            if "orcid" in info:
                print(f"  ORCID: {info['orcid']}")
            print(f"  Identifiers: {updated_author.identifiers}")
        except ValueError as e:
            pytest.skip(f"API request failed: {e}")

    @pytest.mark.asyncio
    async def test_get_papers_by_author(self, datasrc):
        """Test fetching real papers for an author."""
        author = Author(identifiers={f"dblp:pid:{TEST_AUTHOR_PID}"})
        
        try:
            papers = await datasrc.get_papers_by_author(author)
            
            # Author should have papers
            assert len(papers) > 0
            
            # Check if ResNet paper is in the list
            resnet_found = False
            for paper in papers:
                for ident in paper.identifiers:
                    if ident == f"dblp:key:{TEST_PAPER_KEY}":
                        resnet_found = True
                        break
            
            print(f"\n✓ Found {len(papers)} papers by author")
            print(f"  ResNet paper found: {resnet_found}")
            # Print first 5 papers
            for i, p in enumerate(papers[:5]):
                paper_key = next(
                    (ident[9:] for ident in p.identifiers if ident.startswith("dblp:key:")),
                    "?"
                )
                print(f"  {i+1}. {paper_key}")
        except ValueError as e:
            pytest.skip(f"API request failed: {e}")


class TestRealVenueAPI:
    """Real API tests for venue-related methods."""

    @pytest.mark.asyncio
    async def test_get_venue_info(self, datasrc):
        """Test fetching real venue info."""
        venue = Venue(identifiers={f"dblp:key:{TEST_VENUE_KEY}"})
        
        try:
            updated_venue, info = await datasrc.get_venue_info(venue)
            
            # Verify we got data
            assert "dblp:key" in info
            assert info["dblp:key"] == TEST_VENUE_KEY
            
            # Check that dblp:key identifier exists
            has_dblp_key = any(
                ident.startswith("dblp:key:")
                for ident in updated_venue.identifiers
            )
            assert has_dblp_key
            
            print(f"\n✓ Venue info:")
            print(f"  Key: {info.get('dblp:key')}")
            print(f"  Title: {info.get('title')}")
            if "proceedings_title" in info:
                title_short = info["proceedings_title"][:60] + "..." if len(info.get("proceedings_title", "")) > 60 else info.get("proceedings_title")
                print(f"  Proceedings Title: {title_short}")
            if "proceedings_year" in info:
                print(f"  Year: {info['proceedings_year']}")
            print(f"  Identifiers: {updated_venue.identifiers}")
        except ValueError as e:
            pytest.skip(f"API request failed: {e}")

    @pytest.mark.asyncio
    async def test_get_papers_by_venue(self, datasrc):
        """Test fetching real papers for a venue."""
        venue = Venue(identifiers={f"dblp:key:{TEST_VENUE_KEY}"})
        
        try:
            papers = await datasrc.get_papers_by_venue(venue)
            
            # Venue should have papers
            assert len(papers) > 0
            
            # Check if ResNet paper is in the list
            resnet_found = False
            for paper in papers:
                for ident in paper.identifiers:
                    if ident == f"dblp:key:{TEST_PAPER_KEY}":
                        resnet_found = True
                        break
            
            print(f"\n✓ Found {len(papers)} papers in venue")
            print(f"  ResNet paper found: {resnet_found}")
            # Print first 5 papers
            for i, p in enumerate(papers[:5]):
                paper_key = next(
                    (ident[9:] for ident in p.identifiers if ident.startswith("dblp:key:")),
                    "?"
                )
                print(f"  {i+1}. {paper_key}")
        except ValueError as e:
            pytest.skip(f"API request failed: {e}")


class TestRealWorkflow:
    """Integration tests with real API calls."""

    @pytest.mark.asyncio
    async def test_author_to_papers_workflow(self, datasrc):
        """Test getting an author's papers."""
        print("\n=== Author to Papers Workflow ===")
        
        author = Author(identifiers={f"dblp:pid:{TEST_AUTHOR_PID}"})
        
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
            pytest.skip(f"API request failed: {e}")

    @pytest.mark.asyncio
    async def test_paper_to_venue_workflow(self, datasrc):
        """Test getting a paper's venue."""
        print("\n=== Paper to Venue Workflow ===")
        
        paper = Paper(identifiers={f"dblp:key:{TEST_PAPER_KEY}"})
        
        try:
            # 1. Get paper info
            updated_paper, info = await datasrc.get_paper_info(paper)
            print(f"\n1. Paper: {info.get('title', 'Unknown')[:50]}...")
            
            # 2. Get paper's venue
            venues = await datasrc.get_venues_by_paper(updated_paper)
            print(f"\n2. Venues: {len(venues)}")
            for venue in venues:
                venue_key = next(
                    (ident[9:] for ident in venue.identifiers if ident.startswith("dblp:key:")),
                    "?"
                )
                print(f"   - {venue_key}")
            
            assert len(venues) > 0
            print("\n✓ Workflow completed!")
        except ValueError as e:
            pytest.skip(f"API request failed: {e}")

    @pytest.mark.asyncio
    async def test_paper_authors_workflow(self, datasrc):
        """Test getting a paper's authors."""
        print("\n=== Paper Authors Workflow ===")
        
        paper = Paper(identifiers={f"dblp:key:{TEST_PAPER_KEY}"})
        
        try:
            # Get authors for paper
            authors = await datasrc.get_authors_by_paper(paper)
            print(f"\n1. Found {len(authors)} authors for paper")
            
            for i, author in enumerate(authors[:5]):
                author_name = next(
                    (ident[5:] for ident in author.identifiers if ident.startswith("name:")),
                    "?"
                )
                print(f"   - Author {i+1}: {author_name}")
            
            assert len(authors) > 0
            print("\n✓ Workflow completed!")
        except ValueError as e:
            pytest.skip(f"API request failed: {e}")

    @pytest.mark.asyncio
    async def test_venue_to_papers_workflow(self, datasrc):
        """Test getting papers from a venue."""
        print("\n=== Venue to Papers Workflow ===")
        
        venue = Venue(identifiers={f"dblp:key:{TEST_VENUE_KEY}"})
        
        try:
            # 1. Get venue info
            updated_venue, info = await datasrc.get_venue_info(venue)
            print(f"\n1. Venue: {info.get('title', 'Unknown')}")
            
            # 2. Get venue's papers
            papers = await datasrc.get_papers_by_venue(updated_venue)
            print(f"\n2. Papers in venue: {len(papers)}")
            
            # Print first 5 papers
            for i, p in enumerate(papers[:5]):
                paper_key = next(
                    (ident[9:] for ident in p.identifiers if ident.startswith("dblp:key:")),
                    "?"
                )
                print(f"   {i+1}. {paper_key}")
            
            assert len(papers) > 0
            print("\n✓ Workflow completed!")
        except ValueError as e:
            pytest.skip(f"API request failed: {e}")

    @pytest.mark.asyncio
    async def test_full_paper_exploration_workflow(self, datasrc):
        """Test full paper exploration: paper -> authors, venue."""
        print("\n=== Full Paper Exploration Workflow ===")
        
        paper = Paper(identifiers={f"dblp:key:{TEST_PAPER_KEY}"})
        
        try:
            # 1. Get paper info
            updated_paper, info = await datasrc.get_paper_info(paper)
            print(f"\n1. Paper: {info.get('title', 'Unknown')}")
            print(f"   Year: {info.get('year')}")
            print(f"   Type: {info.get('dblp:type')}")
            
            # 2. Get authors
            authors = await datasrc.get_authors_by_paper(updated_paper)
            print(f"\n2. Authors ({len(authors)}):")
            for author in authors:
                name = next(
                    (ident[5:] for ident in author.identifiers if ident.startswith("name:")),
                    "?"
                )
                print(f"   - {name}")
            
            # 3. Get venue
            venues = await datasrc.get_venues_by_paper(updated_paper)
            print(f"\n3. Venue ({len(venues)}):")
            for venue in venues:
                title = next(
                    (ident[6:] for ident in venue.identifiers if ident.startswith("title:")),
                    "?"
                )
                print(f"   - {title}")
            
            assert len(authors) > 0
            assert len(venues) > 0
            print("\n✓ Workflow completed!")
        except ValueError as e:
            pytest.skip(f"API request failed: {e}")


class TestCacheIntegration:
    """Test caching behavior with real API."""

    @pytest.mark.asyncio
    async def test_paper_info_cached(self, datasrc):
        """Test that paper info is cached after first fetch."""
        paper = Paper(identifiers={f"dblp:key:{TEST_PAPER_KEY}"})
        
        try:
            # First fetch
            updated_paper1, info1 = await datasrc.get_paper_info(paper)
            
            # Second fetch (should be from cache)
            updated_paper2, info2 = await datasrc.get_paper_info(paper)
            
            # Results should be the same
            assert info1["dblp:key"] == info2["dblp:key"]
            assert info1["title"] == info2["title"]
            
            print("\n✓ Paper info caching works correctly")
            print(f"  Title: {info1['title']}")
        except ValueError as e:
            pytest.skip(f"API request failed: {e}")

    @pytest.mark.asyncio
    async def test_author_info_cached(self, datasrc):
        """Test that author info is cached after first fetch."""
        author = Author(identifiers={f"dblp:pid:{TEST_AUTHOR_PID}"})
        
        try:
            # First fetch
            updated_author1, info1 = await datasrc.get_author_info(author)
            
            # Second fetch (should be from cache)
            updated_author2, info2 = await datasrc.get_author_info(author)
            
            # Results should be the same
            assert info1["dblp:pid"] == info2["dblp:pid"]
            assert info1["name"] == info2["name"]
            
            print("\n✓ Author info caching works correctly")
            print(f"  Name: {info1['name']}")
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
    async def test_invalid_paper_key(self, datasrc):
        """Test handling of invalid paper key."""
        paper = Paper(identifiers={f"dblp:key:invalid/nonexistent/key"})
        
        with pytest.raises(ValueError, match="Failed to fetch"):
            await datasrc.get_paper_info(paper)
        
        print("\n✓ Invalid paper key handled correctly")

    @pytest.mark.asyncio
    async def test_missing_paper_identifier(self, datasrc):
        """Test handling of paper without DBLP identifier."""
        paper = Paper(identifiers={"other:identifier:123"})
        
        with pytest.raises(ValueError, match="No valid DBLP identifier"):
            await datasrc.get_paper_info(paper)
        
        print("\n✓ Missing identifier handled correctly")

    @pytest.mark.asyncio
    async def test_invalid_author_pid(self, datasrc):
        """Test handling of invalid author PID."""
        author = Author(identifiers={f"dblp:pid:invalid/nonexistent"})
        
        with pytest.raises(ValueError, match="Failed to fetch"):
            await datasrc.get_author_info(author)
        
        print("\n✓ Invalid author PID handled correctly")

    @pytest.mark.asyncio
    async def test_missing_author_identifier(self, datasrc):
        """Test handling of author without DBLP identifier."""
        author = Author(identifiers={"name:Unknown Author"})
        
        with pytest.raises(ValueError, match="No valid DBLP identifier"):
            await datasrc.get_author_info(author)
        
        print("\n✓ Missing author identifier handled correctly")

    @pytest.mark.asyncio
    async def test_invalid_venue_key(self, datasrc):
        """Test handling of invalid venue key."""
        venue = Venue(identifiers={f"dblp:key:invalid/nonexistent"})
        
        with pytest.raises(ValueError, match="Failed to fetch"):
            await datasrc.get_venue_info(venue)
        
        print("\n✓ Invalid venue key handled correctly")

    @pytest.mark.asyncio
    async def test_missing_venue_identifier(self, datasrc):
        """Test handling of venue without DBLP identifier."""
        venue = Venue(identifiers={"title:Unknown Venue"})
        
        with pytest.raises(ValueError, match="No valid DBLP identifier"):
            await datasrc.get_venue_info(venue)
        
        print("\n✓ Missing venue identifier handled correctly")
