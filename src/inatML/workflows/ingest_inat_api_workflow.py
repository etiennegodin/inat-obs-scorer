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
    SOURCE_TABLE_NAME = "raw.obs_sample"
    TARGET_TABLE_NAME = "raw.inat_api"
    CHUNK_SIZE = 200

    con = _open_connection(deps.DB_PATH)

    # Create table to receive api data
    try:
        con.execute(
            f"""CREATE TABLE IF NOT EXISTS {TARGET_TABLE_NAME}
            ( chunk_idx INT, item_key VARCHAR, json JSON, time VARCHAR)"""
        )
        logger.info(f"Created table {TARGET_TABLE_NAME}")
    except CatalogException:
        pass

    # Get observations from sample table that are not already collected
    try:
        df_samples = con.execute(
            f"""
            SELECT s.uuid
            FROM {SOURCE_TABLE_NAME} s
            LEFT JOIN {TARGET_TABLE_NAME} t ON s.uuid  = t.item_key
            WHERE t.item_key IS NULL
            {f'LIMIT {limit}' if limit is not None else ''}"""
        ).df()
    except CatalogException:
        raise

    # Convert to list
    items = df_samples["uuid"].to_list()

    if items:
        # Set up api
        api = inatApiClient(
            TARGET_TABLE_NAME, fields=fields, limiter=30, per_page=CHUNK_SIZE
        )

        # Run api
        asyncio.run(api.execute(items, con))
    else:
        logger.info("All items already processed")

    sql = SQL_Engine(con, deps.RAW_QUERY_FOLDER)
    sql.execute("unpack_observations")
    sql.execute("unpack_relative")
    sql.execute("unpack_taxa")
