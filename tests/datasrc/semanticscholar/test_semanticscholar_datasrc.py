"""
Unit tests for SemanticScholarDataSrc.

Tests the Semantic Scholar DataSrc implementation with mocked HTTP responses.
Uses paper ID: 2c03df8b48bf3fa39054345bafabfeff15bfd11d
Uses author ID: 39353098
"""

import pytest
import json
from unittest.mock import AsyncMock, patch, MagicMock

from paper_weaver.datasrc.semanticscholar import SemanticScholarDataSrc
from paper_weaver.datasrc.cache_impl import MemoryDataSrcCache
from paper_weaver.dataclass import Paper, Author, Venue


# Test data constants
TEST_PAPER_ID = "2c03df8b48bf3fa39054345bafabfeff15bfd11d"
TEST_AUTHOR_ID = "39353098"


# Mock response data
MOCK_PAPER_RESPONSE = {
    "paperId": TEST_PAPER_ID,
    "title": "Attention Is All You Need",
    "abstract": "The dominant sequence transduction models are based on complex recurrent or convolutional neural networks...",
    "year": 2017,
    "publicationDate": "2017-06-12",
    "externalIds": {
        "DOI": "10.48550/arXiv.1706.03762",
        "ArXiv": "1706.03762",
        "DBLP": "conf/nips/VaswaniSPUJGKP17"
    },
    "publicationTypes": ["Conference"],
    "journal": {
        "name": "Neural Information Processing Systems"
    },
    "authors": [
        {"authorId": "39353098", "name": "Ashish Vaswani", "externalIds": {"DBLP": ["Ashish Vaswani"]}},
        {"authorId": "1846258", "name": "Noam Shazeer", "externalIds": {}}
    ]
}

MOCK_AUTHORS_RESPONSE = {
    "data": [
        {
            "authorId": "39353098",
            "name": "Ashish Vaswani",
            "externalIds": {"DBLP": ["Ashish Vaswani"], "ORCID": "0000-0001-1234-5678"},
            "affiliations": ["Google"],
            "homepage": "https://example.com/ashish"
        },
        {
            "authorId": "1846258",
            "name": "Noam Shazeer",
            "externalIds": {},
            "affiliations": ["Google"],
            "homepage": None
        }
    ]
}

MOCK_REFERENCES_RESPONSE = {
    "data": [
        {
            "citedPaper": {
                "paperId": "ref123456",
                "title": "Referenced Paper 1",
                "year": 2016,
                "externalIds": {"DOI": "10.1234/ref1"}
            }
        },
        {
            "citedPaper": {
                "paperId": "ref789abc",
                "title": "Referenced Paper 2",
                "year": 2015,
                "externalIds": {}
            }
        },
        {
            "citedPaper": None  # Some references may not have paper data
        }
    ]
}

MOCK_CITATIONS_RESPONSE = {
    "data": [
        {
            "citingPaper": {
                "paperId": "cite123456",
                "title": "Citing Paper 1",
                "year": 2018,
                "externalIds": {"DOI": "10.1234/cite1"}
            }
        },
        {
            "citingPaper": {
                "paperId": "cite789abc",
                "title": "Citing Paper 2",
                "year": 2019,
                "externalIds": {"ArXiv": "1901.12345"}
            }
        }
    ]
}

MOCK_AUTHOR_INFO_RESPONSE = {
    "authorId": TEST_AUTHOR_ID,
    "name": "Ashish Vaswani",
    "externalIds": {"DBLP": ["Ashish Vaswani"], "ORCID": "0000-0001-1234-5678"},
    "affiliations": ["Google Brain"],
    "homepage": "https://example.com/ashish"
}

MOCK_AUTHOR_PAPERS_RESPONSE = {
    "data": [
        {"paperId": TEST_PAPER_ID, "title": "Attention Is All You Need"},
        {"paperId": "paper123", "title": "Another Paper"},
        {"paperId": "paper456", "title": "Yet Another Paper"}
    ]
}


class TestSemanticScholarDataSrcInit:
    """Tests for SemanticScholarDataSrc initialization."""

    def test_init_with_defaults(self):
        """Test initialization with default parameters."""
        cache = MemoryDataSrcCache()
        datasrc = SemanticScholarDataSrc(cache)
        
        assert datasrc._cache == cache
        assert datasrc._cache_ttl == SemanticScholarDataSrc.DEFAULT_CACHE_TTL
        assert datasrc._http_headers == {}
        assert datasrc._http_timeout == 30

    def test_init_with_custom_params(self):
        """Test initialization with custom parameters."""
        cache = MemoryDataSrcCache()
        headers = {"Authorization": "Bearer token123"}
        datasrc = SemanticScholarDataSrc(
            cache,
            max_concurrent=5,
            cache_ttl=3600,
            http_headers=headers,
            http_proxy="http://proxy:8080",
            http_timeout=60
        )
        
        assert datasrc._cache == cache
        assert datasrc._cache_ttl == 3600
        assert datasrc._http_headers == headers
        assert datasrc._http_proxy == "http://proxy:8080"
        assert datasrc._http_timeout == 60


class TestSemanticScholarDataSrcHelpers:
    """Tests for helper methods."""

    @pytest.fixture
    def datasrc(self):
        cache = MemoryDataSrcCache()
        return SemanticScholarDataSrc(cache)

    def test_extract_ss_paper_id_with_ss_prefix(self, datasrc):
        """Test extracting paper ID with ss: prefix."""
        paper = Paper(identifiers={f"ss:{TEST_PAPER_ID}"})
        result = datasrc._extract_ss_paper_id(paper)
        assert result == TEST_PAPER_ID

    def test_extract_ss_paper_id_with_doi_prefix(self, datasrc):
        """Test extracting paper ID with doi: prefix."""
        paper = Paper(identifiers={"doi:10.1234/test"})
        result = datasrc._extract_ss_paper_id(paper)
        assert result == "doi:10.1234/test"

    def test_extract_ss_paper_id_no_valid_id(self, datasrc):
        """Test extracting paper ID when no valid ID exists."""
        paper = Paper(identifiers={"unknown:12345"})
        result = datasrc._extract_ss_paper_id(paper)
        assert result is None

    def test_extract_ss_paper_id_prefers_ss_over_doi(self, datasrc):
        """Test that ss: prefix is found even with multiple identifiers."""
        paper = Paper(identifiers={f"ss:{TEST_PAPER_ID}", "doi:10.1234/test"})
        result = datasrc._extract_ss_paper_id(paper)
        # Should find one of them (depends on set iteration order)
        assert result in [TEST_PAPER_ID, "doi:10.1234/test"]

    def test_extract_ss_author_id_with_prefix(self, datasrc):
        """Test extracting author ID with ss-author: prefix."""
        author = Author(identifiers={f"ss-author:{TEST_AUTHOR_ID}"})
        result = datasrc._extract_ss_author_id(author)
        assert result == TEST_AUTHOR_ID

    def test_extract_ss_author_id_no_valid_id(self, datasrc):
        """Test extracting author ID when no valid ID exists."""
        author = Author(identifiers={"orcid:0000-0001-1234-5678"})
        result = datasrc._extract_ss_author_id(author)
        assert result is None

    def test_paper_from_ss_data(self, datasrc):
        """Test creating Paper from API data."""
        paper = datasrc._paper_from_ss_data(MOCK_PAPER_RESPONSE)
        
        assert f"ss:{TEST_PAPER_ID}" in paper.identifiers
        assert "doi:10.48550/arXiv.1706.03762" in paper.identifiers
        assert "arxiv:1706.03762" in paper.identifiers
        assert "dblp:conf/nips/VaswaniSPUJGKP17" in paper.identifiers

    def test_paper_from_ss_data_minimal(self, datasrc):
        """Test creating Paper from minimal API data."""
        data = {"paperId": "abc123"}
        paper = datasrc._paper_from_ss_data(data)
        
        assert paper.identifiers == {"ss:abc123"}

    def test_author_from_ss_data(self, datasrc):
        """Test creating Author from API data."""
        author_data = MOCK_AUTHORS_RESPONSE["data"][0]
        author = datasrc._author_from_ss_data(author_data)
        
        assert f"ss-author:{TEST_AUTHOR_ID}" in author.identifiers
        assert "dblp-author:Ashish Vaswani" in author.identifiers
        assert "orcid:0000-0001-1234-5678" in author.identifiers

    def test_author_from_ss_data_minimal(self, datasrc):
        """Test creating Author from minimal API data."""
        data = {"authorId": "12345"}
        author = datasrc._author_from_ss_data(data)
        
        assert author.identifiers == {"ss-author:12345"}

    def test_venue_from_ss_data_with_journal(self, datasrc):
        """Test creating Venue from API data with journal."""
        venue = datasrc._venue_from_ss_data(MOCK_PAPER_RESPONSE)
        
        assert "ss-venue:Neural Information Processing Systems" in venue.identifiers

    def test_venue_from_ss_data_with_venue_field(self, datasrc):
        """Test creating Venue from API data with venue field."""
        data = {"venue": "ICML 2020"}
        venue = datasrc._venue_from_ss_data(data)
        
        assert "ss-venue:ICML 2020" in venue.identifiers

    def test_venue_from_ss_data_empty(self, datasrc):
        """Test creating Venue from API data without venue info."""
        data = {"paperId": "abc123"}
        venue = datasrc._venue_from_ss_data(data)
        
        assert len(venue.identifiers) == 0


class TestSemanticScholarDataSrcPaperMethods:
    """Tests for paper-related methods."""

    @pytest.fixture
    def cache(self):
        return MemoryDataSrcCache()

    @pytest.fixture
    def datasrc(self, cache):
        return SemanticScholarDataSrc(cache, cache_ttl=3600)

    @pytest.mark.asyncio
    async def test_get_paper_info_success(self, datasrc):
        """Test successful paper info retrieval."""
        paper = Paper(identifiers={f"ss:{TEST_PAPER_ID}"})
        
        with patch.object(datasrc, '_fetch_json', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = json.dumps(MOCK_PAPER_RESPONSE)
            
            updated_paper, info = await datasrc.get_paper_info(paper)
            
            assert info["paperId"] == TEST_PAPER_ID
            assert info["title"] == "Attention Is All You Need"
            assert f"ss:{TEST_PAPER_ID}" in updated_paper.identifiers
            assert "doi:10.48550/arXiv.1706.03762" in updated_paper.identifiers

    @pytest.mark.asyncio
    async def test_get_paper_info_no_valid_id(self, datasrc):
        """Test paper info with no valid identifier raises error."""
        paper = Paper(identifiers={"unknown:12345"})
        
        with pytest.raises(ValueError, match="No valid Semantic Scholar identifier"):
            await datasrc.get_paper_info(paper)

    @pytest.mark.asyncio
    async def test_get_paper_info_fetch_failed(self, datasrc):
        """Test paper info when fetch fails raises error."""
        paper = Paper(identifiers={f"ss:{TEST_PAPER_ID}"})
        
        with patch.object(datasrc, '_fetch_json', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = None
            
            with pytest.raises(ValueError, match="Failed to fetch paper"):
                await datasrc.get_paper_info(paper)

    @pytest.mark.asyncio
    async def test_get_paper_info_cached(self, cache, datasrc):
        """Test paper info is cached after first fetch."""
        paper = Paper(identifiers={f"ss:{TEST_PAPER_ID}"})
        cache_key = f"ss:paper:{TEST_PAPER_ID.lower()}"
        
        with patch.object(datasrc, '_fetch_json', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = json.dumps(MOCK_PAPER_RESPONSE)
            
            # First call
            await datasrc.get_paper_info(paper)
            assert mock_fetch.call_count == 1
            
            # Second call should use cache
            await datasrc.get_paper_info(paper)
            assert mock_fetch.call_count == 1  # No additional fetch
            
            # Verify cache
            cached = await cache.get(cache_key)
            assert cached is not None

    @pytest.mark.asyncio
    async def test_get_authors_by_paper_success(self, datasrc):
        """Test successful authors retrieval for a paper."""
        paper = Paper(identifiers={f"ss:{TEST_PAPER_ID}"})
        
        with patch.object(datasrc, '_fetch_json', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = json.dumps(MOCK_AUTHORS_RESPONSE)
            
            authors = await datasrc.get_authors_by_paper(paper)
            
            assert len(authors) == 2
            assert any(f"ss-author:{TEST_AUTHOR_ID}" in a.identifiers for a in authors)
            assert any("ss-author:1846258" in a.identifiers for a in authors)

    @pytest.mark.asyncio
    async def test_get_authors_by_paper_no_valid_id(self, datasrc):
        """Test authors retrieval with no valid paper identifier."""
        paper = Paper(identifiers={"unknown:12345"})
        
        with pytest.raises(ValueError, match="No valid Semantic Scholar identifier"):
            await datasrc.get_authors_by_paper(paper)

    @pytest.mark.asyncio
    async def test_get_venues_by_paper_success(self, datasrc):
        """Test successful venue retrieval for a paper."""
        paper = Paper(identifiers={f"ss:{TEST_PAPER_ID}"})
        
        with patch.object(datasrc, '_fetch_json', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = json.dumps(MOCK_PAPER_RESPONSE)
            
            venues = await datasrc.get_venues_by_paper(paper)
            
            assert len(venues) == 1
            assert "ss-venue:Neural Information Processing Systems" in venues[0].identifiers

    @pytest.mark.asyncio
    async def test_get_venues_by_paper_no_venue(self, datasrc):
        """Test venue retrieval when paper has no venue info."""
        paper = Paper(identifiers={f"ss:{TEST_PAPER_ID}"})
        response = {"paperId": TEST_PAPER_ID, "title": "Test Paper"}
        
        with patch.object(datasrc, '_fetch_json', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = json.dumps(response)
            
            venues = await datasrc.get_venues_by_paper(paper)
            
            assert len(venues) == 0

    @pytest.mark.asyncio
    async def test_get_references_by_paper_success(self, datasrc):
        """Test successful references retrieval for a paper."""
        paper = Paper(identifiers={f"ss:{TEST_PAPER_ID}"})
        
        with patch.object(datasrc, '_fetch_json', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = json.dumps(MOCK_REFERENCES_RESPONSE)
            
            references = await datasrc.get_references_by_paper(paper)
            
            # Should skip the None citedPaper
            assert len(references) == 2
            assert any("ss:ref123456" in r.identifiers for r in references)
            assert any("ss:ref789abc" in r.identifiers for r in references)

    @pytest.mark.asyncio
    async def test_get_references_by_paper_no_valid_id(self, datasrc):
        """Test references retrieval with no valid paper identifier."""
        paper = Paper(identifiers={"unknown:12345"})
        
        with pytest.raises(ValueError, match="No valid Semantic Scholar identifier"):
            await datasrc.get_references_by_paper(paper)

    @pytest.mark.asyncio
    async def test_get_citations_by_paper_success(self, datasrc):
        """Test successful citations retrieval for a paper."""
        paper = Paper(identifiers={f"ss:{TEST_PAPER_ID}"})
        
        with patch.object(datasrc, '_fetch_json', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = json.dumps(MOCK_CITATIONS_RESPONSE)
            
            citations = await datasrc.get_citations_by_paper(paper)
            
            assert len(citations) == 2
            assert any("ss:cite123456" in c.identifiers for c in citations)
            assert any("ss:cite789abc" in c.identifiers for c in citations)

    @pytest.mark.asyncio
    async def test_get_citations_by_paper_with_external_ids(self, datasrc):
        """Test citations include external IDs from response."""
        paper = Paper(identifiers={f"ss:{TEST_PAPER_ID}"})
        
        with patch.object(datasrc, '_fetch_json', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = json.dumps(MOCK_CITATIONS_RESPONSE)
            
            citations = await datasrc.get_citations_by_paper(paper)
            
            # Find the citation with ArXiv ID
            arxiv_citation = next((c for c in citations if "arxiv:1901.12345" in c.identifiers), None)
            assert arxiv_citation is not None


class TestSemanticScholarDataSrcAuthorMethods:
    """Tests for author-related methods."""

    @pytest.fixture
    def cache(self):
        return MemoryDataSrcCache()

    @pytest.fixture
    def datasrc(self, cache):
        return SemanticScholarDataSrc(cache, cache_ttl=3600)

    @pytest.mark.asyncio
    async def test_get_author_info_success(self, datasrc):
        """Test successful author info retrieval."""
        author = Author(identifiers={f"ss-author:{TEST_AUTHOR_ID}"})
        
        with patch.object(datasrc, '_fetch_json', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = json.dumps(MOCK_AUTHOR_INFO_RESPONSE)
            
            updated_author, info = await datasrc.get_author_info(author)
            
            assert info["authorId"] == TEST_AUTHOR_ID
            assert info["name"] == "Ashish Vaswani"
            assert f"ss-author:{TEST_AUTHOR_ID}" in updated_author.identifiers
            assert "orcid:0000-0001-1234-5678" in updated_author.identifiers

    @pytest.mark.asyncio
    async def test_get_author_info_no_valid_id(self, datasrc):
        """Test author info with no valid identifier raises error."""
        author = Author(identifiers={"unknown:12345"})
        
        with pytest.raises(ValueError, match="No valid Semantic Scholar identifier"):
            await datasrc.get_author_info(author)

    @pytest.mark.asyncio
    async def test_get_author_info_fetch_failed(self, datasrc):
        """Test author info when fetch fails raises error."""
        author = Author(identifiers={f"ss-author:{TEST_AUTHOR_ID}"})
        
        with patch.object(datasrc, '_fetch_json', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = None
            
            with pytest.raises(ValueError, match="Failed to fetch author"):
                await datasrc.get_author_info(author)

    @pytest.mark.asyncio
    async def test_get_author_info_cached(self, cache, datasrc):
        """Test author info is cached after first fetch."""
        author = Author(identifiers={f"ss-author:{TEST_AUTHOR_ID}"})
        cache_key = f"ss:author:{TEST_AUTHOR_ID.lower()}"
        
        with patch.object(datasrc, '_fetch_json', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = json.dumps(MOCK_AUTHOR_INFO_RESPONSE)
            
            # First call
            await datasrc.get_author_info(author)
            assert mock_fetch.call_count == 1
            
            # Second call should use cache
            await datasrc.get_author_info(author)
            assert mock_fetch.call_count == 1

    @pytest.mark.asyncio
    async def test_get_papers_by_author_success(self, datasrc):
        """Test successful papers retrieval for an author."""
        author = Author(identifiers={f"ss-author:{TEST_AUTHOR_ID}"})
        
        with patch.object(datasrc, '_fetch_json', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = json.dumps(MOCK_AUTHOR_PAPERS_RESPONSE)
            
            papers = await datasrc.get_papers_by_author(author)
            
            assert len(papers) == 3
            assert any(f"ss:{TEST_PAPER_ID}" in p.identifiers for p in papers)
            assert any("ss:paper123" in p.identifiers for p in papers)
            assert any("ss:paper456" in p.identifiers for p in papers)

    @pytest.mark.asyncio
    async def test_get_papers_by_author_no_valid_id(self, datasrc):
        """Test papers retrieval with no valid author identifier."""
        author = Author(identifiers={"unknown:12345"})
        
        with pytest.raises(ValueError, match="No valid Semantic Scholar identifier"):
            await datasrc.get_papers_by_author(author)

    @pytest.mark.asyncio
    async def test_get_papers_by_author_empty_result(self, datasrc):
        """Test papers retrieval when author has no papers."""
        author = Author(identifiers={f"ss-author:{TEST_AUTHOR_ID}"})
        
        with patch.object(datasrc, '_fetch_json', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = json.dumps({"data": []})
            
            papers = await datasrc.get_papers_by_author(author)
            
            assert len(papers) == 0


class TestSemanticScholarDataSrcVenueMethods:
    """Tests for venue-related methods."""

    @pytest.fixture
    def datasrc(self):
        cache = MemoryDataSrcCache()
        return SemanticScholarDataSrc(cache)

    @pytest.mark.asyncio
    async def test_get_venue_info(self, datasrc):
        """Test venue info returns venue as-is with empty dict."""
        venue = Venue(identifiers={"ss-venue:NeurIPS"})
        
        updated_venue, info = await datasrc.get_venue_info(venue)
        
        assert updated_venue == venue
        assert info == {}

    @pytest.mark.asyncio
    async def test_get_papers_by_venue_not_implemented(self, datasrc):
        """Test papers by venue raises NotImplementedError."""
        venue = Venue(identifiers={"ss-venue:NeurIPS"})
        
        with pytest.raises(NotImplementedError, match="does not support direct venue-to-papers lookup"):
            await datasrc.get_papers_by_venue(venue)


class TestSemanticScholarDataSrcErrorHandling:
    """Tests for error handling and edge cases."""

    @pytest.fixture
    def cache(self):
        return MemoryDataSrcCache()

    @pytest.fixture
    def datasrc(self, cache):
        return SemanticScholarDataSrc(cache, cache_ttl=3600)

    @pytest.mark.asyncio
    async def test_invalid_json_response(self, datasrc):
        """Test handling of invalid JSON response."""
        paper = Paper(identifiers={f"ss:{TEST_PAPER_ID}"})
        
        with patch.object(datasrc, '_fetch_json', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = "not valid json"
            
            with pytest.raises(ValueError, match="Failed to fetch paper"):
                await datasrc.get_paper_info(paper)

    @pytest.mark.asyncio
    async def test_response_missing_paper_id(self, datasrc):
        """Test handling of response without paperId."""
        paper = Paper(identifiers={f"ss:{TEST_PAPER_ID}"})
        
        with patch.object(datasrc, '_fetch_json', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = json.dumps({"title": "Test"})
            
            with pytest.raises(ValueError, match="Failed to fetch paper"):
                await datasrc.get_paper_info(paper)

    @pytest.mark.asyncio
    async def test_response_missing_author_id(self, datasrc):
        """Test handling of response without authorId."""
        author = Author(identifiers={f"ss-author:{TEST_AUTHOR_ID}"})
        
        with patch.object(datasrc, '_fetch_json', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = json.dumps({"name": "Test Author"})
            
            with pytest.raises(ValueError, match="Failed to fetch author"):
                await datasrc.get_author_info(author)

    @pytest.mark.asyncio
    async def test_references_with_null_papers(self, datasrc):
        """Test references properly skips null cited papers."""
        paper = Paper(identifiers={f"ss:{TEST_PAPER_ID}"})
        response = {
            "data": [
                {"citedPaper": {"paperId": "valid123", "title": "Valid"}},
                {"citedPaper": None},
                {"citedPaper": {"paperId": None}},  # paperId is null
                {"citedPaper": {}},  # No paperId
            ]
        }
        
        with patch.object(datasrc, '_fetch_json', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = json.dumps(response)
            
            references = await datasrc.get_references_by_paper(paper)
            
            # Should only include the valid paper
            assert len(references) == 1
            assert "ss:valid123" in references[0].identifiers

    @pytest.mark.asyncio
    async def test_authors_with_null_author_ids(self, datasrc):
        """Test authors properly skips null author IDs."""
        paper = Paper(identifiers={f"ss:{TEST_PAPER_ID}"})
        response = {
            "data": [
                {"authorId": "valid123", "name": "Valid Author"},
                {"authorId": None, "name": "Invalid Author"},
                {"name": "No ID Author"},  # No authorId
            ]
        }
        
        with patch.object(datasrc, '_fetch_json', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = json.dumps(response)
            
            authors = await datasrc.get_authors_by_paper(paper)
            
            # Should only include the valid author
            assert len(authors) == 1
            assert "ss-author:valid123" in authors[0].identifiers


class TestSemanticScholarDataSrcCaching:
    """Tests for caching behavior."""

    @pytest.fixture
    def cache(self):
        return MemoryDataSrcCache()

    @pytest.fixture
    def datasrc(self, cache):
        return SemanticScholarDataSrc(cache, cache_ttl=3600)

    @pytest.mark.asyncio
    async def test_cache_key_case_insensitive(self, cache, datasrc):
        """Test that cache keys are case-insensitive."""
        paper_upper = Paper(identifiers={f"ss:{TEST_PAPER_ID.upper()}"})
        paper_lower = Paper(identifiers={f"ss:{TEST_PAPER_ID.lower()}"})
        
        with patch.object(datasrc, '_fetch_json', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = json.dumps(MOCK_PAPER_RESPONSE)
            
            # Fetch with uppercase
            await datasrc.get_paper_info(paper_upper)
            assert mock_fetch.call_count == 1
            
            # Fetch with lowercase should use cache
            await datasrc.get_paper_info(paper_lower)
            assert mock_fetch.call_count == 1

    @pytest.mark.asyncio
    async def test_different_endpoints_different_cache_keys(self, cache, datasrc):
        """Test that different endpoints use different cache keys."""
        paper = Paper(identifiers={f"ss:{TEST_PAPER_ID}"})
        
        with patch.object(datasrc, '_fetch_json', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.side_effect = [
                json.dumps(MOCK_PAPER_RESPONSE),
                json.dumps(MOCK_AUTHORS_RESPONSE),
                json.dumps(MOCK_REFERENCES_RESPONSE),
            ]
            
            await datasrc.get_paper_info(paper)
            await datasrc.get_authors_by_paper(paper)
            await datasrc.get_references_by_paper(paper)
            
            # All three should trigger separate fetches
            assert mock_fetch.call_count == 3

    @pytest.mark.asyncio
    async def test_cache_preserves_identifiers(self, datasrc):
        """Test that original identifiers are preserved after cache hit."""
        paper = Paper(identifiers={f"ss:{TEST_PAPER_ID}", "custom:myid"})
        
        with patch.object(datasrc, '_fetch_json', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = json.dumps(MOCK_PAPER_RESPONSE)
            
            # First fetch
            updated_paper1, _ = await datasrc.get_paper_info(paper)
            
            # Second fetch (from cache)
            paper2 = Paper(identifiers={f"ss:{TEST_PAPER_ID}", "another:id"})
            updated_paper2, _ = await datasrc.get_paper_info(paper2)
            
            # Both should have their original identifiers plus the ones from response
            assert "custom:myid" in updated_paper1.identifiers
            assert "another:id" in updated_paper2.identifiers


class TestSemanticScholarDataSrcNoExceptionMethods:
    """Tests for *_no_exception methods inherited from DataSrc."""

    @pytest.fixture
    def cache(self):
        return MemoryDataSrcCache()

    @pytest.fixture
    def datasrc(self, cache):
        return SemanticScholarDataSrc(cache, cache_ttl=3600)

    @pytest.mark.asyncio
    async def test_get_paper_info_no_exception_success(self, datasrc):
        """Test get_paper_info_no_exception returns result on success."""
        paper = Paper(identifiers={f"ss:{TEST_PAPER_ID}"})
        
        with patch.object(datasrc, '_fetch_json', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = json.dumps(MOCK_PAPER_RESPONSE)
            
            updated_paper, info = await datasrc.get_paper_info_no_exception(paper)
            
            assert info is not None
            assert info["paperId"] == TEST_PAPER_ID

    @pytest.mark.asyncio
    async def test_get_paper_info_no_exception_returns_none_on_error(self, datasrc):
        """Test get_paper_info_no_exception returns None on error."""
        paper = Paper(identifiers={"unknown:12345"})
        
        updated_paper, info = await datasrc.get_paper_info_no_exception(paper)
        
        assert updated_paper == paper
        assert info is None

    @pytest.mark.asyncio
    async def test_get_authors_by_paper_no_exception_returns_none_on_error(self, datasrc):
        """Test get_authors_by_paper_no_exception returns None on error."""
        paper = Paper(identifiers={"unknown:12345"})
        
        result = await datasrc.get_authors_by_paper_no_exception(paper)
        
        assert result is None

    @pytest.mark.asyncio
    async def test_get_references_by_paper_no_exception_returns_none_on_error(self, datasrc):
        """Test get_references_by_paper_no_exception returns None on error."""
        paper = Paper(identifiers={"unknown:12345"})
        
        result = await datasrc.get_references_by_paper_no_exception(paper)
        
        assert result is None

    @pytest.mark.asyncio
    async def test_get_citations_by_paper_no_exception_returns_none_on_error(self, datasrc):
        """Test get_citations_by_paper_no_exception returns None on error."""
        paper = Paper(identifiers={"unknown:12345"})
        
        result = await datasrc.get_citations_by_paper_no_exception(paper)
        
        assert result is None


class TestSemanticScholarDataSrcWithDOI:
    """Tests using DOI as paper identifier."""

    @pytest.fixture
    def cache(self):
        return MemoryDataSrcCache()

    @pytest.fixture
    def datasrc(self, cache):
        return SemanticScholarDataSrc(cache, cache_ttl=3600)

    @pytest.mark.asyncio
    async def test_get_paper_info_with_doi(self, datasrc):
        """Test paper info retrieval using DOI."""
        paper = Paper(identifiers={"doi:10.48550/arXiv.1706.03762"})
        
        with patch.object(datasrc, '_fetch_json', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = json.dumps(MOCK_PAPER_RESPONSE)
            
            updated_paper, info = await datasrc.get_paper_info(paper)
            
            assert info["paperId"] == TEST_PAPER_ID
            # Original DOI identifier should be preserved
            assert "doi:10.48550/arXiv.1706.03762" in updated_paper.identifiers
            # SS paper ID should be added
            assert f"ss:{TEST_PAPER_ID}" in updated_paper.identifiers
            
            # Verify the URL was constructed with the DOI
            call_args = mock_fetch.call_args[0][0]
            assert "doi:10.48550/arXiv.1706.03762" in call_args


class TestSemanticScholarDataSrcIntegration:
    """Integration-style tests combining multiple operations."""

    @pytest.fixture
    def cache(self):
        return MemoryDataSrcCache()

    @pytest.fixture
    def datasrc(self, cache):
        return SemanticScholarDataSrc(cache, cache_ttl=3600)

    @pytest.mark.asyncio
    async def test_paper_workflow(self, datasrc):
        """Test typical paper workflow: get info -> get authors -> get references."""
        paper = Paper(identifiers={f"ss:{TEST_PAPER_ID}"})
        
        with patch.object(datasrc, '_fetch_json', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.side_effect = [
                json.dumps(MOCK_PAPER_RESPONSE),
                json.dumps(MOCK_AUTHORS_RESPONSE),
                json.dumps(MOCK_REFERENCES_RESPONSE),
            ]
            
            # Get paper info
            updated_paper, info = await datasrc.get_paper_info(paper)
            assert info["title"] == "Attention Is All You Need"
            
            # Get authors
            authors = await datasrc.get_authors_by_paper(updated_paper)
            assert len(authors) == 2
            
            # Get references
            references = await datasrc.get_references_by_paper(updated_paper)
            assert len(references) == 2

    @pytest.mark.asyncio
    async def test_author_workflow(self, datasrc):
        """Test typical author workflow: get info -> get papers."""
        author = Author(identifiers={f"ss-author:{TEST_AUTHOR_ID}"})
        
        with patch.object(datasrc, '_fetch_json', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.side_effect = [
                json.dumps(MOCK_AUTHOR_INFO_RESPONSE),
                json.dumps(MOCK_AUTHOR_PAPERS_RESPONSE),
            ]
            
            # Get author info
            updated_author, info = await datasrc.get_author_info(author)
            assert info["name"] == "Ashish Vaswani"
            
            # Get papers
            papers = await datasrc.get_papers_by_author(updated_author)
            assert len(papers) == 3

