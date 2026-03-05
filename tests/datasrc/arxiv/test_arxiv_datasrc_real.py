"""
Real API tests for ArxivDataSrc.

These tests send actual HTTP requests to the arXiv XML API.
Run with: pytest tests/datasrc/arxiv/test_arxiv_datasrc_real.py -v -s
"""

import datetime

import pytest
import pytest_asyncio

from paper_weaver.datasrc.arxiv import ArxivDataSrc
from paper_weaver.datasrc.cache_impl import MemoryDataSrcCache, RedisDataSrcCache
from paper_weaver.dataclass import Paper, Author, Venue

try:
    import redis.asyncio as aioredis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False


def _check_real_redis_connection():
    if not REDIS_AVAILABLE:
        return False
    try:
        import redis
        client = redis.Redis(host="localhost", port=6379)
        client.ping()
        client.close()
        return True
    except Exception:
        return False


REAL_REDIS_AVAILABLE = _check_real_redis_connection()

TEST_ARXIV_ID = "2508.14891v2"
TEST_ARXIV_URL = f"https://arxiv.org/abs/{TEST_ARXIV_ID}"
TEST_ARXIV_URL_STRIPPED = "https://arxiv.org/abs/2508.14891"
TEST_DOI_URL = "https://doi.org/10.48550/arXiv.2508.14891"
TEST_TITLE_SUBSTR = "GaussianArt"


@pytest_asyncio.fixture
async def cache():
    if REAL_REDIS_AVAILABLE:
        client = aioredis.Redis(host="localhost", port=6379, db=15)
        await client.flushdb()
        cache = RedisDataSrcCache(client, prefix="test_arxiv_datasrc", default_expire=None)
        yield cache
        await client.flushdb()
        await client.aclose()
    else:
        yield MemoryDataSrcCache()


@pytest.fixture
def datasrc(cache):
    return ArxivDataSrc(
        cache=cache,
        max_concurrent=3,
        cache_ttl=3600,
    )


class TestRealPaperAPI:
    @pytest.mark.asyncio
    async def test_get_paper_info(self, datasrc):
        paper = Paper(identifiers={TEST_ARXIV_URL})

        try:
            updated_paper, info = await datasrc.get_paper_info(paper)

            assert "title" in info
            assert TEST_TITLE_SUBSTR in info["title"]
            assert "abstract" in info
            assert "arxiv:published" in info
            assert isinstance(info["arxiv:published"], datetime.datetime)
            assert "arxiv:updated" in info
            assert isinstance(info["arxiv:updated"], datetime.datetime)
            assert "year" in info
            assert isinstance(info["year"], int)
            assert "arxiv:links" in info
            assert isinstance(info["arxiv:links"], list)

            assert TEST_ARXIV_URL in updated_paper.identifiers
            assert TEST_ARXIV_URL_STRIPPED in updated_paper.identifiers
            assert TEST_DOI_URL in updated_paper.identifiers

            print(f"\n✓ Paper: {info['title']}")
            print(f"  Year: {info['year']}")
            print(f"  Identifiers count: {len(updated_paper.identifiers)}")
        except ValueError as e:
            pytest.skip(f"API request failed: {e}")

    @pytest.mark.asyncio
    async def test_get_paper_info_preserves_original_identifiers(self, datasrc):
        custom_ident = "custom:my-tag-123"
        paper = Paper(identifiers={TEST_ARXIV_URL, custom_ident})

        try:
            updated_paper, _ = await datasrc.get_paper_info(paper)
            assert TEST_ARXIV_URL in updated_paper.identifiers
            assert custom_ident in updated_paper.identifiers
        except ValueError as e:
            pytest.skip(f"API request failed: {e}")

    @pytest.mark.asyncio
    async def test_get_venues_by_paper_returns_empty(self, datasrc):
        paper = Paper(identifiers={TEST_ARXIV_URL})
        venues = await datasrc.get_venues_by_paper(paper)
        assert venues == []


class TestUnsupportedMethods:
    @pytest.mark.asyncio
    async def test_get_authors_by_paper(self, datasrc):
        with pytest.raises(NotImplementedError, match="author"):
            await datasrc.get_authors_by_paper(Paper(identifiers={TEST_ARXIV_URL}))

    @pytest.mark.asyncio
    async def test_get_references_by_paper(self, datasrc):
        with pytest.raises(NotImplementedError, match="references"):
            await datasrc.get_references_by_paper(Paper(identifiers={TEST_ARXIV_URL}))

    @pytest.mark.asyncio
    async def test_get_citations_by_paper(self, datasrc):
        with pytest.raises(NotImplementedError, match="citations"):
            await datasrc.get_citations_by_paper(Paper(identifiers={TEST_ARXIV_URL}))

    @pytest.mark.asyncio
    async def test_get_author_info(self, datasrc):
        with pytest.raises(NotImplementedError, match="author"):
            await datasrc.get_author_info(Author(identifiers={"orcid:0000-0000-0000-0000"}))

    @pytest.mark.asyncio
    async def test_get_papers_by_author(self, datasrc):
        with pytest.raises(NotImplementedError, match="author"):
            await datasrc.get_papers_by_author(Author(identifiers={"orcid:0000-0000-0000-0000"}))

    @pytest.mark.asyncio
    async def test_get_venue_info(self, datasrc):
        with pytest.raises(NotImplementedError, match="venue"):
            await datasrc.get_venue_info(Venue(identifiers={"issn:0000-0000"}))

    @pytest.mark.asyncio
    async def test_get_papers_by_venue(self, datasrc):
        with pytest.raises(NotImplementedError, match="venue"):
            await datasrc.get_papers_by_venue(Venue(identifiers={"issn:0000-0000"}))


class TestCacheIntegration:
    @pytest.mark.asyncio
    async def test_url_cache_key(self, datasrc, cache):
        paper = Paper(identifiers={TEST_ARXIV_URL})

        try:
            await datasrc.get_paper_info(paper)
            expected_key = f"https://export.arxiv.org/api/query?id_list={TEST_ARXIV_ID}"
            cached = await cache.get(expected_key)
            assert cached is not None
        except ValueError as e:
            pytest.skip(f"API request failed: {e}")


class TestErrorHandling:
    @pytest.mark.asyncio
    async def test_missing_arxiv_identifier(self, datasrc):
        with pytest.raises(ValueError, match="No valid arXiv identifier"):
            await datasrc.get_paper_info(Paper(identifiers={"other:identifier:123"}))
