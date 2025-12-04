"""
Common utilities for DBLP data fetching.
"""

import logging
import os
import aiohttp

logger = logging.getLogger(__name__)


async def fetch_xml(url: str, proxy: str | None = None, timeout: int = 30) -> str | None:
    """
    Fetch XML data from URL.

    Args:
        url: URL to fetch
        proxy: Optional HTTP proxy URL
        timeout: Request timeout in seconds

    Returns:
        XML text or None if fetch fails
    """
    proxy = proxy or os.getenv("HTTP_PROXY")
    logger.info(f"[DBLP] Fetching: {url}")
    try:
        connector = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.get(
                url,
                proxy=proxy,
                timeout=aiohttp.ClientTimeout(total=timeout)
            ) as response:
                if response.status == 200:
                    return await response.text()
                logger.warning(f"[DBLP] Failed ({response.status}): {url}")
    except Exception as e:
        logger.warning(f"[DBLP] Error: {url} - {e}")
    return None
