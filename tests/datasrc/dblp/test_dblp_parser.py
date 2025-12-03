"""
Unit tests for DBLP XML parsers.

Tests use real XML data fetched from DBLP API:
- Record page: https://dblp.org/rec/conf/cvpr/HeZRS16.xml (ResNet paper)
- Person page: https://dblp.org/pid/34/7659.xml (Kaiming He)
- Venue page: https://dblp.org/db/conf/cvpr/cvpr2016.xml (CVPR 2016)

Run with: pytest tests/datasrc/dblp/test_dblp_parser.py -v -s
"""

import pytest
import pytest_asyncio

from paper_weaver.datasrc.dblp import fetch_xml
from paper_weaver.datasrc.dblp.parser import (
    RecordParser,
    RecordPageParser,
    RecordAuthor,
    PersonPageParser,
    VenuePageParser,
)


# Test URLs
RECORD_PAGE_URL = "https://dblp.org/rec/conf/cvpr/HeZRS16.xml"  # ResNet paper
PERSON_PAGE_URL = "https://dblp.org/pid/34/7659.xml"  # Kaiming He
VENUE_PAGE_URL = "https://dblp.org/db/conf/cvpr/cvpr2016.xml"  # CVPR 2016

# Expected data
RECORD_KEY = "conf/cvpr/HeZRS16"
PERSON_PID = "34/7659"
PERSON_NAME = "Kaiming He"


@pytest_asyncio.fixture
async def record_page_xml():
    """Fetch record page XML."""
    xml = await fetch_xml(RECORD_PAGE_URL)
    if xml is None:
        pytest.skip("Failed to fetch record page XML from DBLP")
    return xml


@pytest_asyncio.fixture
async def person_page_xml():
    """Fetch person page XML."""
    xml = await fetch_xml(PERSON_PAGE_URL)
    if xml is None:
        pytest.skip("Failed to fetch person page XML from DBLP")
    return xml


@pytest_asyncio.fixture
async def venue_page_xml():
    """Fetch venue page XML."""
    xml = await fetch_xml(VENUE_PAGE_URL)
    if xml is None:
        pytest.skip("Failed to fetch venue page XML from DBLP")
    return xml


class TestRecordPageParser:
    """Tests for record page parsing."""
    
    @pytest.mark.asyncio
    async def test_parse_record_page(self, record_page_xml):
        """Test parsing a record page."""
        result = RecordPageParser(record_page_xml)
        
        # Basic assertions
        assert isinstance(result, RecordPageParser)
        assert isinstance(result, RecordParser)  # Inheritance check
        assert result.key == RECORD_KEY
        assert result.type == "inproceedings"
        
        # Title should contain "Deep Residual Learning"
        assert result.title is not None
        assert "Deep Residual Learning" in result.title
        
        # Year should be 2016
        assert result.year == 2016
        
        # Venue should be CVPR (from booktitle for inproceedings)
        assert result.venue is not None
        assert "CVPR" in result.venue
        
        # Check venue_type
        assert result.venue_type == "proceedings"
        
        print(f"\n✓ Parsed record page:")
        print(f"  Key: {result.key}")
        print(f"  Title: {result.title}")
        print(f"  Year: {result.year}")
        print(f"  Venue: {result.venue}")
        print(f"  Type: {result.type}")
        print(f"  Venue Type: {result.venue_type}")
    
    @pytest.mark.asyncio
    async def test_record_page_parser_authors(self, record_page_xml):
        """Test author extraction from record page."""
        result = RecordPageParser(record_page_xml)
        
        # Convert iterator to list
        authors = list(result.authors)
        
        # Should have 4 authors (He, Zhang, Ren, Sun)
        assert len(authors) == 4
        
        # Authors should be RecordAuthor instances
        for author in authors:
            assert isinstance(author, RecordAuthor)
            assert author.name is not None
        
        # Author names should be extractable
        names = [a.name for a in result.authors]
        assert len(names) == 4
        
        print(f"\n✓ Authors ({len(authors)}):")
        for author in authors:
            print(f"  - {author.name} (pid={author.pid})")
    
    @pytest.mark.asyncio
    async def test_record_page_parser_ees(self, record_page_xml):
        """Test ee (electronic edition) URLs extraction."""
        result = RecordPageParser(record_page_xml)
        
        ees = list(result.ees)
        
        # ResNet paper should have ee URLs
        assert len(ees) > 0
        
        print(f"\n✓ EE URLs ({len(ees)}):")
        for ee in ees:
            print(f"  - {ee}")
    
    @pytest.mark.asyncio
    async def test_record_page_parser_url(self, record_page_xml):
        """Test DBLP URL extraction."""
        result = RecordPageParser(record_page_xml)
        
        # Should have a DBLP URL
        assert result.url is not None
        assert "dblp.org" in result.url or result.url.startswith("db/")
        
        print(f"\n✓ DBLP URL: {result.url}")
    
    def test_invalid_record_page(self):
        """Test parsing invalid XML."""
        with pytest.raises(ValueError, match="Invalid"):
            RecordPageParser("<invalid>xml</notvalid>")
        
        with pytest.raises(ValueError, match="Expected <dblp>"):
            RecordPageParser("<other><node/></other>")
        
        with pytest.raises(ValueError, match="no publication"):
            RecordPageParser("<dblp></dblp>")
        
        print("\n✓ Invalid XML handling works correctly")


class TestRecordParser:
    """Tests for RecordParser with ElementTree.Element."""
    
    def test_record_parser_from_element(self):
        """Test RecordParser from XML element."""
        import xml.etree.ElementTree as ET
        elem = ET.fromstring('''
            <inproceedings key="conf/test/Test23" mdate="2023-01-01">
                <author>Test Author</author>
                <title>Test Title</title>
                <year>2023</year>
                <booktitle>Test Conference</booktitle>
                <ee>https://doi.org/10.1000/test</ee>
                <url>db/conf/test/test2023.html#Test23</url>
            </inproceedings>
        ''')
        
        parser = RecordParser(elem)
        
        assert parser.key == "conf/test/Test23"
        assert parser.type == "inproceedings"
        assert parser.title == "Test Title"
        assert parser.year == 2023
        assert parser.venue == "Test Conference"
        assert parser.venue_type == "proceedings"
        assert parser.mdate == "2023-01-01"
        
        # Check ees
        ees = list(parser.ees)
        assert len(ees) == 1
        assert "doi.org" in ees[0]
        
        # Check url
        assert parser.url is not None
        
        authors = list(parser.authors)
        assert len(authors) == 1
        assert authors[0].name == "Test Author"
        
        print(f"\n✓ RecordParser from element: key={parser.key}, venue={parser.venue}")
    
    def test_record_parser_article(self):
        """Test RecordParser for article type."""
        import xml.etree.ElementTree as ET
        elem = ET.fromstring('''
            <article key="journals/test/Test23" mdate="2023-01-01">
                <author>Test Author</author>
                <title>Test Article</title>
                <year>2023</year>
                <journal>Test Journal</journal>
                <volume>10</volume>
                <number>1</number>
                <pages>1-10</pages>
            </article>
        ''')
        
        parser = RecordParser(elem)
        
        assert parser.type == "article"
        assert parser.venue == "Test Journal"
        assert parser.venue_type == "journal"
        assert parser.journal == "Test Journal"
        assert parser.volume == "10"
        assert parser.number == "1"
        assert parser.pages == "1-10"
        
        print(f"\n✓ Article parser: venue={parser.venue}, volume={parser.volume}")


class TestPersonPageParser:
    """Tests for person page parsing."""
    
    @pytest.mark.asyncio
    async def test_parse_person_page(self, person_page_xml):
        """Test parsing a person page."""
        result = PersonPageParser(person_page_xml)
        
        # Basic assertions
        assert isinstance(result, PersonPageParser)
        assert result.pid == PERSON_PID
        assert result.name == PERSON_NAME
        
        print(f"\n✓ Parsed person page:")
        print(f"  PID: {result.pid}")
        print(f"  Name: {result.name}")
        affiliations = list(result.affiliations)
        if affiliations:
            print(f"  Affiliations: {affiliations}")
        urls = list(result.urls)
        if urls:
            print(f"  URLs: {urls[:3]}...")
        if result.orcid:
            print(f"  ORCID: {result.orcid}")
    
    @pytest.mark.asyncio
    async def test_person_page_parser_publications(self, person_page_xml):
        """Test publication list from person page."""
        result = PersonPageParser(person_page_xml)
        
        # Convert iterator to list
        publications = list(result.publications)
        
        # Kaiming He should have many papers
        assert len(publications) > 0
        
        # Each publication should be a RecordParser
        for pub in publications:
            assert isinstance(pub, RecordParser)
            assert pub.key is not None
        
        # Should contain the ResNet paper
        resnet_found = False
        for pub in publications:
            if pub.key == RECORD_KEY:
                resnet_found = True
                # From person page, authors should have pids
                authors = list(pub.authors)
                assert len(authors) > 0
                break
        
        assert resnet_found, f"ResNet paper ({RECORD_KEY}) not found in publications"
        
        print(f"\n✓ Publications: {len(publications)}")
        print(f"  First 5:")
        for pub in publications[:5]:
            title_short = (pub.title[:50] + "...") if pub.title and len(pub.title) > 50 else pub.title
            print(f"    - {pub.key}: {title_short}")
    
    @pytest.mark.asyncio
    async def test_person_page_parser_authors_have_pids(self, person_page_xml):
        """Test that authors in person page publications have pids."""
        result = PersonPageParser(person_page_xml)
        
        # Find a publication with multiple authors
        multi_author_pub = None
        for pub in result.publications:
            authors = list(pub.authors)
            if len(authors) > 1:
                multi_author_pub = pub
                break
        
        assert multi_author_pub is not None
        
        # At least some authors should have pids
        authors = list(multi_author_pub.authors)
        authors_with_pids = [a for a in authors if a.pid]
        assert len(authors_with_pids) > 0
        
        print(f"\n✓ Authors with PIDs in '{multi_author_pub.key}':")
        for author in authors[:5]:
            print(f"    - {author.name} (pid={author.pid})")
    
    def test_invalid_person_page(self):
        """Test parsing invalid XML."""
        with pytest.raises(ValueError, match="Invalid"):
            PersonPageParser("<invalid>xml</notvalid>")
        
        with pytest.raises(ValueError, match="Expected <dblpperson>"):
            PersonPageParser("<other><node/></other>")
        
        print("\n✓ Invalid XML handling works correctly")


class TestVenuePageParser:
    """Tests for venue page parsing."""
    
    @pytest.mark.asyncio
    async def test_parse_venue_page(self, venue_page_xml):
        """Test parsing a venue page."""
        result = VenuePageParser(venue_page_xml)
        
        # Basic assertions
        assert isinstance(result, VenuePageParser)
        assert result.title is not None
        
        print(f"\n✓ Parsed venue page:")
        print(f"  Key: {result.key}")
        print(f"  Title: {result.title}")
        if result.h2:
            print(f"  H2: {result.h2}")
        if result.h3:
            print(f"  H3: {result.h3}")
    
    @pytest.mark.asyncio
    async def test_venue_page_parser_publications(self, venue_page_xml):
        """Test publication list from venue page."""
        result = VenuePageParser(venue_page_xml)
        
        # Convert iterator to list
        publications = list(result.publications)
        
        # CVPR 2016 should have papers
        assert len(publications) > 0
        
        # Each publication should be a RecordParser
        for pub in publications:
            assert isinstance(pub, RecordParser)
        
        # Should contain the ResNet paper
        resnet_found = False
        for pub in publications:
            if pub.key == RECORD_KEY:
                resnet_found = True
                break
        
        assert resnet_found, f"ResNet paper ({RECORD_KEY}) not found in venue publications"
        
        print(f"\n✓ Publications: {len(publications)}")
        print(f"  First 5:")
        for pub in publications[:5]:
            title_short = (pub.title[:50] + "...") if pub.title and len(pub.title) > 50 else pub.title
            print(f"    - {pub.key}: {title_short}")
    
    @pytest.mark.asyncio
    async def test_venue_page_parser_proceedings(self, venue_page_xml):
        """Test proceedings information from venue page."""
        result = VenuePageParser(venue_page_xml)
        
        print(f"\n✓ Proceedings info:")
        print(f"    Title: {result.proceedings_title}")
        print(f"    Booktitle: {result.proceedings_booktitle}")
        print(f"    Publisher: {result.proceedings_publisher}")
        print(f"    ISBN: {result.proceedings_isbn}")
        print(f"    Year: {result.proceedings_year}")
        print(f"    URL: {result.proceedings_url}")
        
        ees = list(result.proceedings_ees)
        if ees:
            print(f"    EEs: {ees[:3]}")
    
    @pytest.mark.asyncio
    async def test_venue_page_parser_refs(self, venue_page_xml):
        """Test references from venue page."""
        result = VenuePageParser(venue_page_xml)
        
        # Check href and ref properties
        href = result.href
        ref = result.ref
        
        # May or may not have refs depending on page structure
        print(f"\n✓ Venue refs:")
        print(f"    href: {href}")
        print(f"    ref: {ref}")
    
    def test_invalid_venue_page(self):
        """Test parsing invalid XML."""
        with pytest.raises(ValueError, match="Invalid"):
            VenuePageParser("<invalid>xml</notvalid>")
        
        with pytest.raises(ValueError, match="Expected <bht>"):
            VenuePageParser("<other><node/></other>")
        
        print("\n✓ Invalid XML handling works correctly")


class TestRecordAuthor:
    """Tests for RecordAuthor class."""
    
    def test_record_author_from_element(self):
        """Test RecordAuthor from XML element."""
        import xml.etree.ElementTree as ET
        elem = ET.fromstring('<author pid="34/7659">Kaiming He</author>')
        
        author = RecordAuthor(elem)
        
        assert author.name == "Kaiming He"
        assert author.pid == "34/7659"
        
        print(f"\n✓ RecordAuthor: {author}")
    
    def test_record_author_with_orcid(self):
        """Test RecordAuthor with ORCID."""
        import xml.etree.ElementTree as ET
        elem = ET.fromstring('<author pid="34/7659" orcid="0000-0001-2345-6789">Test Author</author>')
        
        author = RecordAuthor(elem)
        
        assert author.name == "Test Author"
        assert author.pid == "34/7659"
        assert author.orcid == "0000-0001-2345-6789"
        
        d = author.__dict__()
        assert d["name"] == "Test Author"
        assert d["pid"] == "34/7659"
        assert d["orcid"] == "0000-0001-2345-6789"
        
        print(f"\n✓ RecordAuthor with ORCID: {author}")
    
    def test_record_author_to_dict(self):
        """Test RecordAuthor.__dict__ method."""
        import xml.etree.ElementTree as ET
        elem = ET.fromstring('<author pid="34/7659">Kaiming He</author>')
        
        author = RecordAuthor(elem)
        d = author.__dict__()
        
        assert d["name"] == "Kaiming He"
        assert d["pid"] == "34/7659"
        
        # Without optional fields
        elem2 = ET.fromstring('<author>Unknown Author</author>')
        author2 = RecordAuthor(elem2)
        d2 = author2.__dict__()
        assert d2["name"] == "Unknown Author"
        assert "pid" not in d2
        
        print(f"\n✓ __dict__(): {d}")


class TestCrossParserIntegration:
    """Integration tests across parsers."""
    
    @pytest.mark.asyncio
    async def test_person_page_record_consistency(self, person_page_xml, record_page_xml):
        """Test that RecordParser from person page matches record page."""
        person_result = PersonPageParser(person_page_xml)
        record_result = RecordPageParser(record_page_xml)
        
        # Find ResNet paper in person's publications
        resnet_from_person = None
        for pub in person_result.publications:
            if pub.key == RECORD_KEY:
                resnet_from_person = pub
                break
        
        assert resnet_from_person is not None
        
        # Compare key fields
        assert resnet_from_person.key == record_result.key
        assert resnet_from_person.title == record_result.title
        assert resnet_from_person.year == record_result.year
        
        print(f"\n✓ Record consistency verified:")
        print(f"  Key: {record_result.key}")
        print(f"  Title match: {resnet_from_person.title == record_result.title}")
        print(f"  Year match: {resnet_from_person.year == record_result.year}")
        
        # Person page should have author pids, record page may not
        person_authors = list(resnet_from_person.authors)
        record_authors = list(record_result.authors)
        person_authors_with_pids = [a for a in person_authors if a.pid]
        record_authors_with_pids = [a for a in record_authors if a.pid]
        
        print(f"  Authors from person page with pids: {len(person_authors_with_pids)}")
        print(f"  Authors from record page with pids: {len(record_authors_with_pids)}")
