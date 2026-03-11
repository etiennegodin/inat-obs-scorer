import asyncio
import logging

from ..app.container import Dependencies
from ..ingest.inat_client import (
    DuckDbWriter,
    EndpointConfig,
    RateLimiterFetcher,
    make_client,
)
from ..utils.db import (
    SQL_Engine,
    _open_connection,
    create_api_raw_table,
    get_remaining_items,
)
from ..utils.git import get_git_hash

logger = logging.getLogger(__name__)


def execute(deps: Dependencies, rate: int, ignore_not_found: bool) -> None:
    SOURCE_TABLE_NAME = "staged.species_list"
    TARGET_TABLE_NAME = "raw.api_similar_species"
    SOURCE_KEY = "taxon_id"

    con = _open_connection(deps.DB_PATH)

    # 1 Create table to receive api data
    create_api_raw_table(con, TARGET_TABLE_NAME)

    # 2 Get missing items not collected
    items = get_remaining_items(con, SOURCE_TABLE_NAME, TARGET_TABLE_NAME, SOURCE_KEY)

    if items:
        # Read api fields to query
        config = EndpointConfig(
            "identifications/similar_species",
            id_param="taxon_id",
            write_empty_rows=ignore_not_found,
        )
        fetcher = RateLimiterFetcher(rate=rate, ignore_not_found=ignore_not_found)

        with DuckDbWriter(con, TARGET_TABLE_NAME, get_git_hash(short=True)) as writer:
            client = make_client(config, fetcher, writer)
            asyncio.run(client.execute(items))
    else:
        logger.info("All items already processed")

    # 3 Stage collected data in db
    sql_ingest = SQL_Engine(con, deps.SQL_STAGE_PATH)
    sql_ingest.execute("stage_similar_species")
