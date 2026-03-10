import asyncio
import datetime
import json
import logging
from concurrent.futures import ThreadPoolExecutor

import duckdb

logger = logging.getLogger(__name__)


class JsonWriter:
    async def write(self, results: list[dict]):
        raise NotImplementedError
        logger.info("Init json writer task")


class DuckDbWriter:
    def __init__(self, con: duckdb.DuckDBPyConnection, table_name: str, version: str):
        self.con = con
        self.table_name = table_name
        self.version = version
        self._executor = ThreadPoolExecutor(max_workers=1)  # DuckDB isn't thread-safe

    async def write(self, results: list[dict]):
        """Receive a ready batch, offload blocking insert to thread pool."""
        logger.info("Init writer task")
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(self._executor, self._insert_batch, results)

    def _insert_batch(self, results: list[dict]) -> None:
        try:
            for item in results:
                self.con.execute(
                    f"INSERT INTO {self.table_name} VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        item["id"],
                        json.dumps(item["data"]),
                        str(datetime.now()),
                        item["response_time"],
                        item["status"],
                        self.version,
                    ),
                )
            self.con.commit()
            logger.debug("Inserted batch of %d items", len(results))
        except Exception as e:
            logger.error("Failed to insert batch: %s", e)
            raise

    def close(self):
        self._executor.shutdown(wait=True)


class NullWriter:  # for dry runs / debugging
    def write(self, results: list[dict]) -> None:
        pass
