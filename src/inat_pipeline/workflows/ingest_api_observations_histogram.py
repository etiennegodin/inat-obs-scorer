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
    TARGET_TABLE_NAME = "raw.species_histogram_quebec"
    SOURCE_KEY = "taxon_id"

    with DuckDBAdapter(deps.DB_PATH) as con:
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

        items = items[:2]
        years = [y for y in range(2010, 2026)]

        params = {
            "place_id": 13336,
            "interval": "week",
            "date_field": "observed",
            "years": years,
        }

        # fields = {"date_field=obser}

        if items:
            # Set up configs
            config = EndpointConfig(
                "observations/histogram",
                id_param="taxon_id",
                params=params,
                write_empty_rows=ignore_not_found,
                per_page=1000,
                chunk_size=1,
            )
            fetcher = RateLimiterFetcher(rate=rate, ignore_not_found=ignore_not_found)

            with DuckDbWriter(con, TARGET_TABLE_NAME, version=deps.version) as writer:
                client = make_client(config, fetcher, writer)
                asyncio.run(client.execute(items))
        else:
            logger.info("All items already requested")

        # 3 Stage collected data in db
        # sql_stage = DuckDbSQL(con, deps.SQL_STAGE_PATH)
        # sql_stage.execute("stage_similar_species")
