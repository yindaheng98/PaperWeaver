"""
CrossRef DataSrc module.

Provides data source implementation for CrossRef REST API.
"""

from .datasrc import CrossRefDataSrc
from .utils import fetch_json
from .record import (
    paper_to_doi,
    work_json_to_paper,
    work_json_to_info,
    work_json_to_authors,
    work_json_to_references,
)

__all__ = [
    "CrossRefDataSrc",
    "fetch_json",
    "paper_to_doi",
    "work_json_to_paper",
    "work_json_to_info",
    "work_json_to_authors",
    "work_json_to_references",
]
