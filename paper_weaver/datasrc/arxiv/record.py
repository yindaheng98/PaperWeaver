"""
arXiv entry/paper conversion utilities.
"""

import datetime

from ...dataclass import Paper


ARXIV_ABS_PREFIX = "https://arxiv.org/abs/"
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
