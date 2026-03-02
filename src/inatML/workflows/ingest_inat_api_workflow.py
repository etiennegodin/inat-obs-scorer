import asyncio
import logging
from typing import Union

from duckdb import CatalogException

from ..app.container import Dependencies
from ..pipeline.ingest.api import inatApiClient
from ..utils.db import _open_connection

logger = logging.getLogger(__name__)


def execute(deps: Dependencies, limit: Union[None, int] = 200):
    TABLE_NAME = "ina_api"
    CHUNK_SIZE = 100
    last_id = None

    con = _open_connection(deps.RAW_DB_PATH)

    # Create table
    try:
        con.execute(
            f"""CREATE TABLE IF NOT EXISTS {TABLE_NAME}
            ( chunk_idx INT, item_key VARCHAR, json JSON)"""
        )
        logger.info(f"Created table {TABLE_NAME}")
    except CatalogException:
        pass

    df = con.execute(
        f"SELECT * FROM obs_sample {f'LIMIT {limit}' if limit is not None else ''}"
    ).df()
    items_df = df.set_index("uuid")
    print(items_df)

    # Get last_id from table
    try:
        last_id = con.execute(f"SELECT MIN(item_key) FROM {TABLE_NAME}").fetchone()[0]
    except Exception as e:
        logger.error(e)

    # Filter items based on last processed ID (idempotent resume)
    if last_id is not None:
        logger.info(f"Resuming from last processed ID: {last_id}")
        # Filter items: keep only those > last_id (since ordered ASC)
        new_items_df = items_df.loc[last_id:]
        items = new_items_df.index.to_list()
        if not items:
            logger.info("All items already processed")
    else:
        items = items_df.index.to_list()

    if not items:
        return

    items_count = len(items)
    print(items_count)

    # Chunk items for batch processing
    items_chunks = [items[i : i + CHUNK_SIZE] for i in range(0, len(items), CHUNK_SIZE)]

    logger.info(
        f"Processing {items_count} items in {len(items_chunks)} chunks of {CHUNK_SIZE}"
    )

    fields = {"id": True}

    # Set up api
    api = inatApiClient(TABLE_NAME, fields=fields, limiter=30, per_page=CHUNK_SIZE)

    asyncio.run(api.execute(items_chunks, con))
