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


def _extract_year(work: dict) -> str | None:
    """Extract publication year from work data, trying multiple date fields."""
    for field in ("published-print", "published-online", "published", "issued", "created"):
        date_obj = work.get(field)
        if date_obj and "date-parts" in date_obj:
            parts = date_obj["date-parts"]
            if parts and parts[0] and parts[0][0]:
                return str(parts[0][0])
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


def _date_parts_to_str(date_obj: dict | None) -> str | None:
    """Convert CrossRef date-parts structure to a string like '2025-08-13'."""
    if not date_obj or "date-parts" not in date_obj:
        return None
    parts = date_obj["date-parts"]
    if not parts or not parts[0]:
        return None
    p = parts[0]
    if len(p) >= 3:
        return f"{p[0]}-{p[1]:02d}-{p[2]:02d}"
    elif len(p) >= 2:
        return f"{p[0]}-{p[1]:02d}"
    elif len(p) >= 1:
        return str(p[0])
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
    - urls (from link field)
    """
    info = {}

    # Common keys
    titles = [work.get("title")] if isinstance(work.get("title"), str) else work.get("title")
    if titles:
        info["title"] = titles[0]
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
        info["container-title"] = container_title

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

    urls = []
    if doi:
        urls.append(f"https://doi.org/{doi}")
    resource = work.get("resource")
    if resource and isinstance(resource, dict):
        primary = resource.get("primary")
        if primary and isinstance(primary, dict) and primary.get("URL"):
            urls.append(primary["URL"])
    links = work.get("link")
    for link in links:
        if link.get("URL"):
            urls.append(link["URL"])
    if urls:
        info["urls"] = urls

    # CrossRef-specific keys
    cr_type = work.get("type")
    if cr_type:
        info["crossref:type"] = cr_type

    member = work.get("member")
    if member:
        info["crossref:member"] = member

    prefix = work.get("prefix")
    if prefix:
        info["crossref:prefix"] = prefix

    for date_field in ("created", "deposited", "indexed", "published-print", "published-online", "published", "issued", "posted", "accepted"):
        date_obj = work.get(date_field)
        if date_obj:
            date_str = _date_parts_to_str(date_obj)
            if date_str:
                info[f"crossref:{date_field}"] = date_str

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
    author_list = work.get("author", [])
    if not author_list:
        return authors

    for author_data in author_list:
        orcid_url = author_data.get("ORCID")
        if not orcid_url:
            continue

        if orcid_url.startswith("https://orcid.org/"):
            orcid = orcid_url[len("https://orcid.org/"):]
        else:
            orcid = orcid_url

        authors.append(Author(identifiers={f"orcid:{orcid}"}))

    return authors


def work_json_to_references(work: dict) -> list[Paper]:
    """
    Extract references from CrossRef work data.

    Each reference may have a DOI. References without DOIs are skipped
    since they cannot be uniquely identified.
    """
    papers = []
    ref_list = work.get("reference", [])
    if not ref_list:
        return papers

    for ref in ref_list:
        doi = ref.get("DOI")
        if doi:
            identifiers = {f"{CROSSREF_DOI_URL_PREFIX}{doi}"}
            papers.append(Paper(identifiers=identifiers))

    return papers
