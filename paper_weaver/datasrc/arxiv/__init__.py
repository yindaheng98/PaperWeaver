
"""
arXiv DataSrc module.
"""

from .datasrc import ArxivDataSrc
from .utils import fetch_xml
from .record import paper_to_arxiv_id, entry_to_paper, entry_to_info

__all__ = [
    "ArxivDataSrc",
    "fetch_xml",
    "paper_to_arxiv_id",
    "entry_to_paper",
    "entry_to_info",
]