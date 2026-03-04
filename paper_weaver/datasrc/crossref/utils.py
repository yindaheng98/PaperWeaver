"""
Common utilities for CrossRef data fetching.
"""

import json
import logging

import aiohttp

logger = logging.getLogger(__name__)


async def fetch_json(url: str, mailto: str | None = None) -> str | None:
    """
    Fetch CrossRef work JSON text from URL.

    Proxy is automatically read from environment variables (HTTP_PROXY, HTTPS_PROXY)
    via aiohttp's trust_env=True setting.

    Args:
        url: URL to fetch
        mailto: Contact email for CrossRef polite pool

    Returns:
        JSON text or None if fetch/validation fails
    """
    logger.info(f"[CrossRef] Fetching: {url}")
    try:
        headers = {
            "Accept": "application/json",
            "User-Agent": "PaperWeaver/1.0 (https://github.com/PaperWeaver; mailto:{})".format(
                mailto or "anonymous@example.com"
            ),
        }
        params = {"mailto": mailto} if mailto else None

        async with aiohttp.ClientSession(
            headers=headers,
            trust_env=True
        ) as session:
            async with session.get(
                url,
                params=params,
                timeout=aiohttp.ClientTimeout(total=30)
            ) as response:
                if response.status == 200:
                    text = await response.text()
                    try:
                        # Validate response before allowing it to be cached.
                        data = json.loads(text)
                        message = data.get("message")
                        if data.get("status") == "ok" and isinstance(message, dict) and message.get("DOI"):
                            return text
                        logger.warning(f"[CrossRef] Invalid work payload: {url}")
                    except json.JSONDecodeError:
                        logger.warning(f"[CrossRef] Invalid JSON payload: {url}")
                    return None
                logger.warning(f"[CrossRef] Failed ({response.status}): {url}")
    except Exception as e:
        logger.warning(f"[CrossRef] Error: {url} - {e}")
    return None
