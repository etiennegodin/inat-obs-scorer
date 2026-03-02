import asyncio
import logging
from pathlib import Path

from duckdb import CatalogException

from ..pipeline.ingest.api import inatApiClient
from ..utils.db import _open_connection

logger = logging.getLogger(__name__)


def execute(db_path: Path):
    TABLE_NAME = "ina_api"
    CHUNK_SIZE = 5
    last_id = None

    con = _open_connection(db_path)

    fields = {"id": True}
    items = []
    # print(items)
    quit()

    items_count = len(items)

    # Create table
    try:
        con.execute(
            f"""CREATE TABLE IF NOT EXISTS {TABLE_NAME}
            ( chunk_idx INT, item_key VARCHAR, json JSON)"""
        )
        logger.info(f"Created table {TABLE_NAME}")
    except CatalogException:
        pass

    # Get last_id from table
    try:
        max_id = con.execute(f"SELECT MAX(item_key) FROM {TABLE_NAME}").fetchone()[0]
        min_id = con.execute(f"SELECT MIN(item_key) FROM {TABLE_NAME}").fetchone()[0]
        print(min_id)
        print(max_id)
        if max_id is not None and min_id is not None:
            max_id = int(max_id)
            min_id = int(min_id)
            if max_id > min_id:
                last_id = max_id
            else:
                last_id = min_id
    except Exception as e:
        logger.error(e)

    # Filter items based on last processed ID (idempotent resume)
    if last_id is not None:
        logger.info(f"Resuming from last processed ID: {last_id}")
        # Filter items: keep only those > last_id (since ordered ASC)
        items = [item for item in items if item >= last_id]
        if not items:
            logger.info("All items already processed")

    # Chunk items for batch processing
    items_chunks = [items[i : i + CHUNK_SIZE] for i in range(0, len(items), CHUNK_SIZE)]

    logger.info(
        f"Processing {items_count} items in {len(items_chunks)} chunks of {CHUNK_SIZE}"
    )

    # Set up api
    api = inatApiClient(TABLE_NAME, fields=fields, limiter=30, per_page=CHUNK_SIZE)

    asyncio.run(api.execute(items_chunks, con))
