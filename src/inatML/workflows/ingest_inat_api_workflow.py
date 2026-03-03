import asyncio
import logging
from typing import Union

from duckdb import CatalogException

from ..app.container import Dependencies
from ..pipeline.ingest.api import inatApiClient
from ..utils.db import SQL_Engine, _open_connection

logger = logging.getLogger(__name__)

# params = {'place_id': place_id['id'] }

fields = {
    "id": True,
    "reviewed_by": True,
    "owners_identification_from_vision": True,
    "identifications_count": True,
    "user": {"id": True, "created_at": True, "orcid": True},
    "description": True,
    "tags": True,
    "observation_photos": True,
    "comments_count": True,
    "faves_count": True,
    "outlinks": True,
    "community_taxon_id": True,
    "taxon_geoprivacy": True,
    "place_ids": True,
    "identifications": {
        "uuid": True,
        "created_at": True,
        "user": {
            "id": True,
            "login": True,
            "observations_count": True,
            "identifications_count": True,
            "species_count": True,
        },
        "body": True,
        "category": True,
        "current": True,
        "own_observation": True,
        "vision": True,
        "disagreement": True,
        "previous_observation_taxon_id": True,
        "taxon_id": True,
    },
}


def execute(deps: Dependencies, limit: Union[None, int] = 200) -> None:
    TABLE_NAME = "raw.inat_api"
    CHUNK_SIZE = 200
    last_id = None

    con = _open_connection(deps.DB_PATH)

    # Create table
    try:
        con.execute(
            f"""CREATE TABLE IF NOT EXISTS {TABLE_NAME}
            ( chunk_idx INT, item_key VARCHAR, json JSON, time VARCHAR)"""
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
        last_id = con.execute(f"SELECT MAX(item_key) FROM {TABLE_NAME}").fetchone()[0]
    except Exception as e:
        logger.error(e)

    # Filter items based on last processed ID (idempotent resume)
    if last_id is not None:
        logger.info(f"Resuming from last processed ID: {last_id}")
        # Filter items: keep only those > last_id (since ordered ASC)
        new_items_df = items_df.loc[last_id:]
        items = new_items_df.index.to_list()

    else:
        items = items_df.index.to_list()

    if items:
        items_count = len(items)

        # Chunk items for batch processing
        items_chunks = [
            items[i : i + CHUNK_SIZE] for i in range(0, len(items), CHUNK_SIZE)
        ]

        logger.info(
            f"Processing {items_count} items in"
            f"{len(items_chunks)} chunks of {CHUNK_SIZE}"
        )

        # Set up api
        api = inatApiClient(TABLE_NAME, fields=fields, limiter=30, per_page=CHUNK_SIZE)

        asyncio.run(api.execute(items_chunks, con))
    else:
        logger.info("All items already processed")

    sql = SQL_Engine(con, deps.RAW_QUERY_FOLDER)
    sql.execute("unpack_observations")
    sql.execute("unpack_relative")
    con.execute()
