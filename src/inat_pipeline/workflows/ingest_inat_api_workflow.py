import asyncio
import logging
from typing import Union

from duckdb import CatalogException

from ..app.container import Dependencies
from ..pipeline.ingest.api import inatApiClient, inatApiConfig
from ..utils.config import read_config
from ..utils.db import _open_connection

logger = logging.getLogger(__name__)


def execute(deps: Dependencies, limit: Union[None, int]) -> None:
    SOURCE_TABLE_NAME = "raw.obs_sample"
    TARGET_TABLE_NAME = "raw.inat_api"
    CHUNK_SIZE = 200

    con = _open_connection(deps.DB_PATH)

    # Create table to receive api data
    try:
        con.execute(
            f"""CREATE TABLE IF NOT EXISTS {TARGET_TABLE_NAME}
            (
            raw_id VARCHAR,
            raw_json JSON,

            scraped_at VARCHAR,
            api_page INT,
            api_per_page INT,
            request_params JSON,
            response_time_ms INT,
            http_status_code INT,
            scrapper_version VARCHAR,

            )"""
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
            LEFT JOIN {TARGET_TABLE_NAME} t ON s.uuid  = t.raw_id
            WHERE t.raw_id IS NULL
            {f'LIMIT {limit}' if limit is not None else ''}"""
        ).df()
    except CatalogException:
        raise

    # Convert to list
    items = df_samples["uuid"].to_list()

    # Read api fields to query
    api_fields = read_config(deps.API_FIELDS_PATH)

    # Set up api configs
    config = inatApiConfig(fields=api_fields, limiter=10, per_page=CHUNK_SIZE)

    # Run api
    if items:
        api = inatApiClient(TARGET_TABLE_NAME, config=config)
        asyncio.run(api.execute(items, con))
    else:
        logger.info("All items already processed")
