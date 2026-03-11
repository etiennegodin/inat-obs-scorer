import logging

import aiohttp
from aiohttp.client_exceptions import ClientResponseError
from aiolimiter import AsyncLimiter

logger = logging.getLogger(__name__)


class RateLimiterFetcher:
    def __init__(self, limit: int):
        # Convert limiter value to AsyncLimiter object
        self.limiter = AsyncLimiter(limit, 60)

    async def fetch(self, session: aiohttp.ClientSession, url: str, params: dict):
        """Fetch data for multiple IDs in
        a single request using comma-separated ID string"""
        async with self.limiter:
            try:
                async with session.get(url, params=params, timeout=10) as resp:
                    resp.raise_for_status()
                    return await resp.json()
            except ClientResponseError as e:
                logger.debug(resp.url)
                logger.error(f"API request FAILED for IDs {params}:\n{e}")
                if e.code == 500:
                    logger.debug(resp.url)
                    return
            except Exception as e:
                logger.error(f"API request FAILED for IDs {params}:\n{e}")
                raise


class MockFetcher:
    def __init__(self, fixture: dict):
        self.fixture = fixture

    async def fetch(self, session: aiohttp.ClientSession, url: str, params: dict):
        return self.fixture
