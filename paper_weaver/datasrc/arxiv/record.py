"""
arXiv entry/paper conversion utilities.
"""

import datetime

from ...dataclass import Paper


ARXIV_ABS_PREFIX = "https://arxiv.org/abs/"
ARXIV_PDF_PREFIX = "https://arxiv.org/pdf/"
DOI_URL_PREFIX = "https://doi.org/"
ARXIV_DOI_PREFIX = "10.48550/arXiv."


def strip_arxiv_version(arxiv_id: str) -> str:
    if "v" in arxiv_id:
        base, ver = arxiv_id.rsplit("v", 1)
        if ver.isdigit() and base:
            return base
    return arxiv_id


def arxiv_to_doi(arxiv_url: str) -> str:
    if not arxiv_url.startswith(ARXIV_ABS_PREFIX):
        raise ValueError(f"Not an arXiv abs URL: {arxiv_url}")
    arxiv_id = strip_arxiv_version(arxiv_url[len(ARXIV_ABS_PREFIX):])
    return f"{DOI_URL_PREFIX}{ARXIV_DOI_PREFIX}{arxiv_id}"


def doi_to_arxiv(doi_url: str) -> str:
    if not doi_url.startswith(DOI_URL_PREFIX):
        raise ValueError(f"Not a DOI URL: {doi_url}")
    doi = doi_url[len(DOI_URL_PREFIX):]
    if not doi.startswith(ARXIV_DOI_PREFIX):
        raise ValueError(f"Not an arXiv DOI URL: {doi_url}")
    arxiv_id = doi[len(ARXIV_DOI_PREFIX):]
    return f"{ARXIV_ABS_PREFIX}{arxiv_id}"


def paper_to_arxiv_id(paper: Paper) -> str | None:
    """
    Extract arXiv ID from paper identifiers.
    """
    for ident in paper.identifiers:
        if ident.startswith(ARXIV_ABS_PREFIX):
            return ident[len(ARXIV_ABS_PREFIX):]
        if ident.startswith(f"{DOI_URL_PREFIX}{ARXIV_DOI_PREFIX}"):
            return ident[len(f"{DOI_URL_PREFIX}{ARXIV_DOI_PREFIX}"):]
    return None


def entry_to_paper(entry: dict) -> Paper:
    identifiers: set[str] = set()
    entry_id = entry["id"]
    links = [link["href"] for link in entry["links"] if link.get("href")]

    for ident in [entry_id, *links]:
        identifiers.add(ident)
        if ident.startswith(ARXIV_ABS_PREFIX):
            identifiers.add(arxiv_to_doi(ident))
            identifiers.add(f"{ARXIV_ABS_PREFIX}{strip_arxiv_version(ident[len(ARXIV_ABS_PREFIX):])}")
        if ident.startswith(ARXIV_PDF_PREFIX):
            pdf_id = ident[len(ARXIV_PDF_PREFIX):]
            if pdf_id.endswith(".pdf"):
                pdf_id = pdf_id[:-4]
            pdf_id = strip_arxiv_version(pdf_id)
            identifiers.add(f"{ARXIV_PDF_PREFIX}{pdf_id}.pdf")
            identifiers.add(f"{ARXIV_PDF_PREFIX}{pdf_id}")

    return Paper(identifiers=identifiers)


def entry_to_info(entry: dict) -> dict:
    info = {
        "title": entry["title"],
        "abstract": entry["summary"],
        "arxiv:published": datetime.datetime.fromisoformat(entry["published"].replace("Z", "+00:00")),
        "arxiv:updated": datetime.datetime.fromisoformat(entry["updated"].replace("Z", "+00:00")),
        "arxiv:links": [link["href"] for link in entry["links"] if link.get("href")],
    }
    info["year"] = info["arxiv:published"].year
    if entry.get("arxiv_comment"):
        info["arxiv:comment"] = entry["arxiv_comment"]
    if entry.get("arxiv_journal_ref"):
        info["arxiv:journal_ref"] = entry["arxiv_journal_ref"]
    if isinstance(entry.get("arxiv_primary_category"), dict) and entry["arxiv_primary_category"].get("term"):
        info["arxiv:primary_category"] = entry["arxiv_primary_category"]["term"]
    elif isinstance(entry.get("arxiv_primary_category"), str) and entry["arxiv_primary_category"]:
        info["arxiv:primary_category"] = entry["arxiv_primary_category"]
    categories = [tag["term"] for tag in entry.get("tags", []) if isinstance(tag, dict) and tag.get("term")]
    if categories:
        info["arxiv:categories"] = categories
    return info
