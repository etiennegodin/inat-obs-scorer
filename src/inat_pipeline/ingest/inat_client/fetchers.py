import logging
import time

import aiohttp
from aiolimiter import AsyncLimiter

# item_key: str, chunk_idx: int = 0

logger = logging.getLogger(__name__)


class RateLimiterFetcher:
    def __init__(self, limit: int):
        # Convert limiter value to AsyncLimiter object
        self.limiter = AsyncLimiter(limit, 60)

    async def fetch(self, session: aiohttp.ClientSession, url: str, params: dict):
        """Fetch data for multiple IDs in
        a single request using comma-separated ID string"""

        async with params.limiter:
            try:
                start = time.monotonic()
                async with session.get(url, params=self.params, timeout=10) as r:
                    logger.debug(r.url)
                    r.raise_for_status()
                    data = await r.json()
                    response_time = int((time.monotonic() - start) * 1000)
                    if data and "results" in data:
                        results = data["results"]

                        async def _put_in_queue(result_to_add):
                            item_key = result_to_add["uuid"]
                            try:
                                await self.queue.put(
                                    (
                                        params["chunk_idx"],
                                        item_key,
                                        result_to_add,
                                        response_time,
                                        r.status,
                                    )
                                )
                            except Exception as e:
                                logger.error(f"FAILED to queue result: {e}")

                        if isinstance(results, list):
                            for result in data["results"]:  # iterate list of dicts
                                await _put_in_queue(result)
                    else:
                        logger.warning(f"No results found for IDs {item_key}")
            except Exception as e:
                logger.error(r.url)
                logger.error(f"API request FAILED for IDs {item_key}: {e}")
