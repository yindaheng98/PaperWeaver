"""
Common utilities for DBLP data fetching.
"""

import logging
import aiohttp

logger = logging.getLogger(__name__)


async def fetch_xml(url: str) -> str | None:
    """
    Fetch XML data from URL.

    Proxy is automatically read from environment variables (HTTP_PROXY, HTTPS_PROXY)
    via aiohttp's trust_env=True setting.

    Args:
        url: URL to fetch

    Returns:
        XML text or None if fetch fails
    """
    logger.info(f"[DBLP] Fetching: {url}")
    try:
        async with aiohttp.ClientSession(trust_env=True) as session:
            async with session.get(
                url,
                timeout=aiohttp.ClientTimeout(total=30)
            ) as response:
                if response.status == 200:
                    return await response.text()
                logger.warning(f"[DBLP] Failed ({response.status}): {url}")
    except Exception as e:
        logger.warning(f"[DBLP] Error: {url} - {e}")
    return None
