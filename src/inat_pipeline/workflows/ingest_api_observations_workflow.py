import asyncio
import logging
from typing import Union

from ..app.container import Dependencies
from ..ingest.inat_client import fields
from ..ingest.inat_client.base import inatApiClient
from ..utils.db import (
    SQL_Engine,
    _open_connection,
    create_api_raw_table,
    get_remaining_items,
)

logger = logging.getLogger(__name__)


def execute(deps: Dependencies, limit: Union[None, int]) -> None:
    SOURCE_TABLE_NAME = "raw.obs_sample"
    TARGET_TABLE_NAME = "raw.inat_api"
    SOURCE_KEY = "uuid"
    CHUNK_SIZE = 200

    con = _open_connection(deps.DB_PATH)

    # 1 Create table to receive api data
    create_api_raw_table(con, TARGET_TABLE_NAME)

    # 2 Get missing items not collected
    items = get_remaining_items(
        con, SOURCE_TABLE_NAME, TARGET_TABLE_NAME, SOURCE_KEY, limit
    )

    if items:
        # Read api fields to query
        api_fields = fields.load(deps.API_FIELDS_PATH / "observations.yaml")

        # Set up api configs
        config = inatApiConfig(
            endpoint="observations", fields=api_fields, limiter=10, per_page=CHUNK_SIZE
        )

        # Run api queries
        api = inatApiClient(TARGET_TABLE_NAME, config=config)
        asyncio.run(api.execute(items, con))
    else:
        logger.info("All items already processed")

    # 3 Stage collected data in db
    sql_ingest = SQL_Engine(con, deps.SQL_STAGE_PATH)
    # sql_features.execute("clean_inat_api")
    sql_ingest.execute("stage_obs_observations")
    sql_ingest.execute("stage_obs_identifications")
    sql_ingest.execute("stage_obs_photos")
    sql_ingest.execute("stage_obs_users")
    sql_ingest.execute("stage_obs_taxa")

    # Extract species list to feed in taxa api module
    sql_ingest.execute("extract_species_list")
