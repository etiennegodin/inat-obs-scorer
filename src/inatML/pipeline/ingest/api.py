import asyncio
import json
import logging
from asyncio import Queue
from concurrent.futures import ThreadPoolExecutor

import aiohttp
import duckdb
from aiolimiter import AsyncLimiter

logger = logging.getLogger(__name__)


class inatApiClient:
    def __init__(
        self,
        name: str,
        explicit_params: dict = None,
        fields: dict = None,
        limiter: int = 60,
        per_page: int = 200,
    ):
        # Init api
        self.base_url = "https://api.inaturalist.org/v2/observations/"

        if fields is not None:
            self.fields = f"({fields_to_string(fields)})"
        else:
            self.fields = None

        self.explicit_params = explicit_params

        # Init fetch behaviour
        self.per_page = per_page
        self.limiter = AsyncLimiter(limiter, 60)

        # Init writer
        self.queue = Queue()
        self.table_name = f"raw.{name}"

    async def execute(self, items_chunks: list[str], con: duckdb.DuckDBPyConnection):
        # Store count of observers (for logger)
        async with aiohttp.ClientSession() as session:
            writer_task = asyncio.create_task(self._write_data(con))

            # Create fetchers with batched IDs
            fetchers = []
            for chunk_idx, chunk in enumerate(items_chunks):
                # Create comma-separated ID string
                items_key = ",".join(str(key) for key in chunk)
                fetchers.append(
                    asyncio.create_task(self._fetch_data(session, items_key, chunk_idx))
                )

            await asyncio.gather(*fetchers)
            await self.queue.join()
            await self.queue.put(None)
            await writer_task

        return self.table_name

    async def _write_data(self, con):
        logger.info("Init writer task")
        loop = asyncio.get_event_loop()
        executor = ThreadPoolExecutor(max_workers=1)
        processed_count = 0
        batch_size = self.per_page  # Insert multiple rows in a batch

        while True:
            item = await self.queue.get()
            if item is None:
                logger.info(f"Writer done, processed {processed_count} items")
                self.queue.task_done()
                break

            # Collect a batch of items
            batch = [item]
            while not self.queue.empty() and len(batch) < batch_size:
                try:
                    batch.append(self.queue.get_nowait())
                except Exception:
                    break

            try:
                # Run blocking database batch insert in thread pool
                def batch_insert():
                    for chunk_idx, item_key, data in batch:
                        logger.debug(f"Saved item {item_key}")
                        con.execute(
                            f"INSERT INTO {self.table_name} VALUES (?, ?, ?)",
                            (chunk_idx, item_key, json.dumps(data)),
                        )
                    con.commit()  # Commit batch

                await loop.run_in_executor(executor, batch_insert)
                processed_count += len(batch)
                logger.debug(f"Inserted batch of {len(batch)} items")

            except Exception as e:
                logger.error(f"FAILED to insert batch: {e}")

            # Mark all items in batch as done
            for _ in batch:
                self.queue.task_done()

        executor.shutdown(wait=True)

    async def _fetch_data(
        self, session: aiohttp.ClientSession, item_key: str, chunk_idx: int = 0
    ):
        """Fetch data for multiple IDs in
        a single request using comma-separated ID string"""

        # Set params
        if self.explicit_params is not None:
            params = self.explicit_params
        else:
            params = {}

        # Set fields
        if self.fields is not None:
            params["fields"] = self.fields

        # Per-page param
        params["per_page"] = self.per_page

        # Append observation ids to url
        url = self.base_url + (item_key)

        async with self.limiter:
            try:
                async with session.get(url, params=params, timeout=10) as r:
                    logger.debug(r.url)
                    r.raise_for_status()
                    data = await r.json()
                    if data and "results" in data:
                        results = data["results"]

                        async def _put_in_queue(result_to_add):
                            item_key = result_to_add["uuid"]
                            try:
                                await self.queue.put(
                                    (chunk_idx, item_key, result_to_add)
                                )
                            except Exception as e:
                                logger.error(f"FAILED to queue result: {e}")

                        if isinstance(results, list):
                            for result in data["results"]:  # iterate list of dicts
                                await _put_in_queue(result)

                        logger.info(f"Fetched {len(data['results'])} observations")
                    else:
                        logger.warning(f"No results found for IDs {item_key}")
            except Exception as e:
                logger.error(r.url)
                logger.error(f"API request FAILED for IDs {item_key}: {e}")


# Convert to the special syntax
def fields_to_string(fields_dict, level=0):
    parts = []
    for key, value in fields_dict.items():
        if isinstance(value, dict):
            nested = fields_to_string(value, level + 1)
            parts.append(f"{key}:({nested})")
        elif value is True:
            parts.append(f"{key}:!t")
    return ",".join(parts)
