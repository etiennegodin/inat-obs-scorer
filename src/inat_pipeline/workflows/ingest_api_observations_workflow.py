import asyncio
import logging

from ..app.container import Dependencies
from ..db import DuckDBConnection, DuckDbSQL
from ..inat_client import (
    DuckDbWriter,
    EndpointConfig,
    RateLimiterFetcher,
    make_client,
)
from ..inat_client.registery import OBSERVATIONS_FIELDS

logger = logging.getLogger(__name__)


def execute(deps: Dependencies, rate: int, ignore_not_found: bool) -> None:
    SOURCE_TABLE_NAME = "raw.obs_sample"
    TARGET_TABLE_NAME = "raw.inat_api"
    SOURCE_KEY = "uuid"
    CHUNK_SIZE = 200

    with DuckDBConnection(deps.DB_PATH) as con:
        # 1 Create table to receive api data

        sql_api = DuckDbSQL(con, deps.SQL_API_PATH)
        sql_api.execute("create_api_raw_table", params=(), table_name=TARGET_TABLE_NAME)

        df = sql_api.fetch_df(
            "fetch_missing_items",
            source_key=SOURCE_KEY,
            source_table_name=SOURCE_TABLE_NAME,
            target_table_name=TARGET_TABLE_NAME,
        )
        items = df[SOURCE_KEY].to_list()

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

            with DuckDbWriter(con, TARGET_TABLE_NAME, version=deps.version) as writer:
                client = make_client(config, fetcher, writer)
                asyncio.run(client.execute(items))
        else:
            logger.info("All items already requested")

        # 3 Stage collected data in db
        sql_stage = DuckDbSQL(con, deps.SQL_STAGE_PATH)

        sql_stage.execute_many(
            "stage_obs_observations",
            "stage_obs_identifications",
            "stage_obs_photos",
            "stage_obs_users",
        )
