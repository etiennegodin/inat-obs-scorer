import asyncio
import logging

from ..app.container import Dependencies
from ..ingest.inat_client import (
    DuckDbWriter,
    EndpointConfig,
    RateLimiterFetcher,
    make_client,
)
from ..ingest.inat_client.registery import OBSERVATIONS_FIELDS
from ..utils.db import (
    SQL_Engine,
    create_api_raw_table,
    duckdb_con,
    get_remaining_items,
)
from ..utils.git import get_git_hash

logger = logging.getLogger(__name__)


def execute(deps: Dependencies, rate: int, ignore_not_found: bool) -> None:
    SOURCE_TABLE_NAME = "raw.obs_sample"
    TARGET_TABLE_NAME = "raw.inat_api"
    SOURCE_KEY = "uuid"
    CHUNK_SIZE = 200

    with duckdb_con(deps.DB_PATH) as con:
        # 1 Create table to receive api data
        create_api_raw_table(con, TARGET_TABLE_NAME)

        # 2 Get missing items not collected
        items = get_remaining_items(
            con, SOURCE_TABLE_NAME, TARGET_TABLE_NAME, SOURCE_KEY
        )

        if items:
            # Set up configs
            config = EndpointConfig(
                "identifications/similar_species",
                id_param="taxon_id",
                write_empty_rows=ignore_not_found,
                fields=OBSERVATIONS_FIELDS,
                chunk_size=CHUNK_SIZE,
                per_page=200,
            )

            fetcher = RateLimiterFetcher(rate=rate, ignore_not_found=ignore_not_found)

            with DuckDbWriter(
                con, TARGET_TABLE_NAME, get_git_hash(short=True)
            ) as writer:
                client = make_client(config, fetcher, writer)
                asyncio.run(client.execute(items))
        else:
            logger.info("All items already requested")

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
