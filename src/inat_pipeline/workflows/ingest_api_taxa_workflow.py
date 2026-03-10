import asyncio
import logging
from typing import Union

from ..app.container import Dependencies
from ..ingest import fields
from ..ingest.api import inatApiClient, inatApiConfig
from ..utils.db import (
    _open_connection,
    create_api_raw_table,
    get_remaining_items,
)

logger = logging.getLogger(__name__)


def execute(deps: Dependencies, limit: Union[None, int]) -> None:
    SOURCE_TABLE_NAME = "staged.species_list"
    TARGET_TABLE_NAME = "raw.api_taxa"
    SOURCE_KEY = "taxon_id"
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
        api_fields = fields.load(deps.API_FIELDS_PATH / "taxa.yaml")

        # Set up api configs
        config = inatApiConfig(fields=api_fields, limiter=10, per_page=CHUNK_SIZE)

        # Run api queries
        api = inatApiClient(TARGET_TABLE_NAME, config=config)
        asyncio.run(api.execute(items, con))
    else:
        logger.info("All items already processed")

    # 3 Stage collected data in db
    # sql_ingest = SQL_Engine(con, deps.SQL_STAGE_PATH)
    # sql_ingest.execute()
