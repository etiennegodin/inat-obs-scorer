import asyncio
import logging

from ..app.container import Dependencies
from ..db import DuckDBAdapter, DuckDbSQL
from ..ingest.inat_client import (
    DuckDbWriter,
    EndpointConfig,
    RateLimiterFetcher,
    make_client,
)

logger = logging.getLogger(__name__)


def execute(deps: Dependencies, rate: int, ignore_not_found: bool) -> None:
    SOURCE_TABLE_NAME = "staged.species_list"
    TARGET_TABLE_NAME = "raw.obs_histogram_na_observed"
    SOURCE_KEY = "taxon_id"

    with DuckDBAdapter(
        deps.RAW_DB_PATH,
    ) as con:
        # Extract species list to feed in taxa api module
        sql_api = DuckDbSQL(con, deps.SQL_API_PATH)
        logger.info(
            "Listing species from observations to request observations histogram data"
        )
        sql_api.execute("extract_species_list")

        # 1 Create table to receive api data
        sql_api.execute("create_api_raw_table", table_name=TARGET_TABLE_NAME)

        # 2 Get missing items not collected
        df = sql_api.fetch_df(
            "fetch_missing_items",
            source_key=SOURCE_KEY,
            source_table_name=SOURCE_TABLE_NAME,
            target_table_name=TARGET_TABLE_NAME,
        )
        items = df[SOURCE_KEY].to_list()

        # Set years
        years = [y for y in range(2010, 2026)]

        # Custom params
        params = {
            "place_id": 97394,  # North america
            "interval": "week_of_year",
            "date_field": "observed",
            "years": years,
        }

        if items:
            # Set up configs
            config = EndpointConfig(
                "observations/histogram",
                id_param="taxon_id",
                params=params,
                write_empty_rows=ignore_not_found,
                per_page=53,
            )
            fetcher = RateLimiterFetcher(rate=rate, ignore_not_found=ignore_not_found)

            with DuckDbWriter(con, TARGET_TABLE_NAME, version=deps.version) as writer:
                client = make_client(config, fetcher, writer)
                asyncio.run(client.execute(items))
        else:
            logger.info("All items already requested")

        # 3 Stage collected data in db
        sql_stage = DuckDbSQL(con, deps.SQL_STAGE_PATH)
        sql_stage.execute("stage_histogram_observed")
