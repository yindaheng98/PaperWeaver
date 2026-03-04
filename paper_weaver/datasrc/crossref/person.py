"""
CrossRef author/person utilities.

Provides functions to convert CrossRef author objects to Author entities,
including ORCID normalization.
"""

from ...dataclass import Author


def normalize_orcid(value: str) -> str:
    """
    Normalize ORCID input to bare ORCID id.

    Examples:
    - https://orcid.org/0000-0002-8922-9260 -> 0000-0002-8922-9260
    - 0000-0002-8922-9260 -> 0000-0002-8922-9260
    """
    lower = value.strip().lower()
    prefix = "https://orcid.org/"
    if lower.startswith(prefix):
        return lower[len(prefix):]
    return lower


def author_from_crossref_author(author_data: dict) -> Author | None:
    """
    Convert a CrossRef author object to Author.

    CrossRef-specific requirement:
    - Author identifiers are ORCID only.

    Returns None if no ORCID is available.
    """
    if "ORCID" not in author_data:
        return None
    orcid = normalize_orcid(author_data["ORCID"])
    return Author(identifiers={f"orcid:{orcid}"})
