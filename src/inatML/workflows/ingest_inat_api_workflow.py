import asyncio
import logging

from duckdb import CatalogException

from ..app.container import Dependencies
from ..pipeline.ingest.api import inatApiClient
from ..utils.db import _open_connection

logger = logging.getLogger(__name__)


def execute(deps: Dependencies):
    TABLE_NAME = "ina_api"
    CHUNK_SIZE = 5
    last_id = None
    con = _open_connection(deps.RAW_DB_PATH)

    fields = {"id": True}

    # get list

    items = [
        "886f032e-7fa8-44ef-bd9c-826597a6ce3c",
        "7a61472f-9828-436e-a6b0-4b93e79fc66d",
        "0f0e9fab-0477-480d-ad45-72db63806d48",
        "43ab2dd1-12ab-4722-a1a7-1b44780ead90",
        "ab04b53d-4656-4653-a763-11981ddcb312",
        "22e7add9-d886-4e00-8cc9-2b341ae81aaf",
        "60a6b4f1-db82-4f7d-8de0-07a8d8c5419e",
        "6ee8ec42-f924-4707-a7a9-eedb2f30973e",
        "b10af847-a156-4a01-842c-6fae691f6cad",
        "2f1f05eb-642a-44d3-9663-55f2a6ce9867",
    ]

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
