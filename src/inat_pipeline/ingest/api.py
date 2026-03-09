import asyncio
import json
import logging
import time
from asyncio import Queue
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime

import aiohttp
import duckdb
from aiolimiter import AsyncLimiter
from tqdm.asyncio import tqdm_asyncio

from ..utils.git import get_git_hash

logger = logging.getLogger(__name__)


def fields_to_string(fields_dict, level=0):
    parts = []
    for key, value in fields_dict.items():
        if isinstance(value, dict):
            nested = fields_to_string(value, level + 1)
            parts.append(f"{key}:({nested})")
        elif value is True:
            parts.append(f"{key}:!t")
    return ",".join(parts)


@dataclass
class inatApiConfig:
    fields: dict = field(default_factory=dict)
    per_page: int = 200
    limiter: int = 10


class inatApiClient:
    def __init__(self, table_name: str, config: inatApiConfig):
        # Init api
        self.base_url = "https://api.inaturalist.org/v2/observations/"
        self.per_page = config.per_page
        self.table_name = table_name

        # Convert fields dict to request format
        self.fields = f"({fields_to_string(config.fields)})"

        # Init params
        self.params = {}

        # Add default params
        self.params["per_page"] = self.per_page
        if self.fields is not None:
            self.params["fields"] = self.fields

        # Set limiter
        self.limiter = AsyncLimiter(config.limiter, 60)

        # Init writer
        self.queue = Queue()

        # Get git has for version
        self.version = get_git_hash(short=True)

    async def execute(self, items: list[str], con: duckdb.DuckDBPyConnection):
        # Chunk items per page
        items_chunks = self._chunk_items(items)

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

            await tqdm_asyncio.gather(*fetchers)
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
                    for chunk_idx, item_key, data, response_time, status in batch:
                        logger.debug(f"Saved item {item_key}")
                        con.execute(
                            f"""INSERT INTO {self.table_name} VALUES (
                            ?, ?, ?, ?, ?, ?, ?, ?, ?
                            )""",
                            (
                                item_key,
                                json.dumps(data),
                                str(datetime.now()),
                                chunk_idx,
                                self.per_page,
                                json.dumps(self.params),
                                response_time,
                                status,
                                self.version,
                            ),
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

        # Append observation ids to url
        url = self.base_url + (item_key)

        async with self.limiter:
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
                                        chunk_idx,
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

    def _chunk_items(self, items: list[str]) -> list[str]:
        items_count = len(items)

        # Chunk items for batch processing
        items_chunks = [
            items[i : i + self.per_page] for i in range(0, len(items), self.per_page)
        ]

        logger.info(
            f"Processing {items_count} items in "
            f"{len(items_chunks)} chunks of {self.per_page}"
        )
        return items_chunks
