"""
CrossRef Record/Paper utilities.

Provides functions to convert CrossRef API work data to Paper, Author,
and info dict, and extract identifiers from Paper/Author objects.

CrossRef API response structure (for a single work):
{
    "status": "ok",
    "message-type": "work",
    "message": { ... work fields ... }
}

Key work fields used:
- DOI, title, author, reference, type, publisher
- container-title, volume, issue, page, article-number
- published-print, published-online, created, deposited, indexed
- abstract, ISSN, ISBN, subject, link, license, funder
"""

import datetime

from ...dataclass import Paper, Author
from ..title_hash import title_hash


CROSSREF_DOI_URL_PREFIX = "https://doi.org/"


def paper_to_doi(paper: Paper) -> str | None:
    """
    Extract DOI from Paper identifiers.

    Looks for identifiers in the format "https://doi.org/..." and returns
    the DOI portion (after the prefix).
    """
    for ident in paper.identifiers:
        if ident.startswith("https://doi.org/"):
            return ident[len("https://doi.org/"):]
    return None


def _extract_year(work: dict) -> int | None:
    """Extract publication year as int from work data, trying multiple date fields."""
    for field in ("published-print", "published-online", "published", "issued", "created"):
        date_obj = work.get(field)
        if date_obj and "date-parts" in date_obj:
            parts = date_obj["date-parts"]
            if parts and parts[0] and parts[0][0]:
                return int(parts[0][0])
    return None


def work_json_to_paper(work: dict) -> Paper:
    """
    Convert CrossRef work data to Paper with identifiers.

    Identifiers extracted:
    - https://doi.org/{doi} - DOI URL
    - title:{title} - Paper title
    - title_hash:{hash} year:{year} - Title hash for cross-source matching
    """
    identifiers = set()

    doi = work.get("DOI")
    if doi:
        identifiers.add(f"https://doi.org/{doi}")

    titles = [work.get("title")] if isinstance(work.get("title"), str) else work.get("title")
    if titles:
        for title in titles:
            identifiers.add(f"title:{title}")
            year = _extract_year(work)
            for method, h in title_hash(title).items():
                identifiers.add(f"title_hash:{h} year:{year or 'unknown'}")

    return Paper(identifiers=identifiers)


def _parse_date_obj(date_obj: dict | None) -> datetime.datetime | datetime.date | int | None:
    """
    Convert a CrossRef date object to the most precise temporal type available.

    Priority: date-time (ISO 8601) > timestamp (ms) > date-parts.
    For date-parts: 3 parts -> date, 2 parts -> date (1st of month), 1 part -> int (year).
    """
    if not date_obj:
        return None

    dt_str = date_obj.get("date-time")
    if dt_str:
        return datetime.datetime.fromisoformat(dt_str.replace("Z", "+00:00"))

    ts = date_obj.get("timestamp")
    if ts is not None:
        return datetime.datetime.fromtimestamp(ts / 1000, tz=datetime.timezone.utc)

    if "date-parts" not in date_obj:
        return None
    parts = date_obj["date-parts"]
    if not parts or not parts[0]:
        return None
    p = parts[0]
    if len(p) >= 3 and p[0] and p[1] and p[2]:
        return datetime.date(p[0], p[1], p[2])
    elif len(p) >= 2 and p[0] and p[1]:
        return datetime.date(p[0], p[1], 1)
    elif len(p) >= 1 and p[0]:
        return int(p[0])
    return None


def work_json_to_info(work: dict) -> dict:
    """
    Convert CrossRef work data to info dict.

    Keys with "crossref:" prefix (CrossRef-specific):
    - crossref:type, crossref:publisher, crossref:member, crossref:prefix
    - crossref:created, crossref:deposited, crossref:indexed
    - crossref:published-print, crossref:published-online, crossref:published
    - crossref:issued, crossref:posted, crossref:accepted
    - crossref:link, crossref:license, crossref:funder
    - crossref:reference-count, crossref:is-referenced-by-count
    - crossref:subject, crossref:alternative-id, crossref:article-number
    - crossref:update-policy, crossref:content-domain
    - crossref:short-container-title

    Common keys:
    - title, abstract, year, pages, volume, issue, number
    - container_title (journal/proceedings name), issn, isbn
    - resource, links (from link field)
    """
    info = {}

    # Common keys
    titles = work.get("title")
    if titles:
        info["crossref:title"] = titles

    abstract = work.get("abstract")
    if abstract:
        info["abstract"] = abstract

    year = _extract_year(work)
    if year:
        info["year"] = year

    page = work.get("page")
    if page:
        info["page"] = page

    volume = work.get("volume")
    if volume:
        info["volume"] = volume

    issue = work.get("issue")
    if issue:
        info["issue"] = issue

    number = work.get("number")
    if number:
        info["number"] = number

    container_title = work.get("container-title")
    if container_title:
        info["crossref:container-title"] = container_title

    publisher = work.get("publisher")
    if publisher:
        info["publisher"] = publisher

    issn = work.get("ISSN")
    if issn:
        info["issn"] = issn

    isbn = work.get("ISBN")
    if isbn:
        info["isbn"] = isbn

    doi = work.get("DOI")
    if doi:
        info["doi"] = doi

    resource = work.get("resource")
    if resource and isinstance(resource, dict):
        primary = resource.get("primary")
        if primary and isinstance(primary, dict) and primary.get("URL"):
            info["crossref:resource"] = primary["URL"]

    links = work.get("link")
    if links:
        for link in links:
            if link.get("URL"):
                if "crossref:links" not in info:
                    info["crossref:links"] = []
                info["crossref:links"].append(link["URL"])

    url = work.get("URL")
    if url:
        info["crossref:url"] = url

    # CrossRef-specific keys
    cr_type = work.get("type")
    if cr_type:
        info["crossref:type"] = cr_type

    for date_field in ("created", "deposited", "indexed", "published-print", "published-online", "published", "issued", "posted", "accepted"):
        date_obj = work.get(date_field)
        if date_obj:
            parsed = _parse_date_obj(date_obj)
            if parsed is not None:
                info[f"crossref:{date_field}"] = parsed

    short_ct = work.get("short-container-title")
    if short_ct:
        info["crossref:short-container-title"] = short_ct

    event = work.get("event")
    if event and event.get("name"):
        info["crossref:event"] = event.get("name")

    return info


def work_json_to_authors(work: dict) -> list[Author]:
    """
    Extract authors from CrossRef work data.

    Author identifiers:
    - orcid:{orcid} - ORCID (extracted from ORCID URL like https://orcid.org/...)

    Authors without ORCID are skipped.
    """
    authors = []
    for author_data in work.get("author", []):
        orcid_url = author_data.get("ORCID")
        if not orcid_url:
            continue
        orcid = orcid_url[len("https://orcid.org/"):] if orcid_url.startswith("https://orcid.org/") else orcid_url
        authors.append(Author(identifiers={f"orcid:{orcid}"}))
    return authors


def work_json_to_references(work: dict) -> list[Paper]:
    """
    Extract references from CrossRef work data.

    Each reference may have a DOI. References without DOIs are skipped
    since they cannot be uniquely identified.
    """
    papers = []
    for ref in work.get("reference", []):
        doi = ref.get("DOI")
        if not doi:
            continue
        papers.append(Paper(identifiers={f"https://doi.org/{doi}"}))
    return papers
