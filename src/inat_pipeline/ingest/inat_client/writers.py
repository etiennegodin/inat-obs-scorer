import asyncio
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from pprint import pprint
from typing import Union

import duckdb

logger = logging.getLogger(__name__)


class NullWriter:
    def __init__(self, limit: Union[int, None] = None):
        self.limit = limit

    async def write(self, results: list[dict]):
        logger.info("Init null writer task")
        if self.limit is not None:
            results = results[: self.limit]
        for r in results:
            pprint(r)


class JsonWriter:
    async def write(self, results: list[dict]):
        raise NotImplementedError
        logger.info("Init json writer task")


class DuckDbWriter:
    def __init__(
        self, con: duckdb.DuckDBPyConnection, table_name: str, version: str = "na"
    ):
        self.con = con
        self.table_name = table_name
        self.version = version
        self._executor = ThreadPoolExecutor(max_workers=1)  # DuckDB isn't thread-safe

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self._executor.shutdown(wait=True)  # drain inflight inserts

    async def write(self, results: list[dict]):
        """Receive a ready batch, offload blocking insert to thread pool."""
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(self._executor, self._insert_batch, results)

    def _insert_batch(self, results: list[dict]) -> None:
        try:
            for item in results:
                source_id = item["_source_id"]  # resolved in base
                is_empty = item.get("_empty", False)
                self.con.execute(
                    f"INSERT INTO {self.table_name} VALUES (?, ?, ?, ?)",
                    (
                        source_id,
                        None
                        if is_empty
                        else json.dumps(
                            {k: v for k, v in item.items() if k != "_source_id"}
                        ),  # since results is tagged
                        str(datetime.now()),
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


class LocalBinaryWriter:
    """
    Writes raw bytes to the local filesystem, organized by subfolders.
    """

    def __init__(self, parent_folder: Union[str, Path]):
        self.parent_folder = Path(parent_folder)
        self.parent_folder.mkdir(parents=True, exist_ok=True)
        self._executor = ThreadPoolExecutor(max_workers=4)

    async def write(self, data: bytes, subfolder: str, filename: str):
        """Offload blocking file write to thread pool."""
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            self._executor, self._write_sync, data, subfolder, filename
        )

    def _write_sync(self, data: bytes, subfolder: str, filename: str):
        target_dir = self.parent_folder / str(subfolder)
        target_dir.mkdir(parents=True, exist_ok=True)
        target_path = target_dir / filename
        with open(target_path, "wb") as f:
            f.write(data)

    def close(self):
        self._executor.shutdown(wait=True)
