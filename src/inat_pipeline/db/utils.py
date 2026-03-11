import logging
from typing import Any

import duckdb
from duckdb import CatalogException

logger = logging.getLogger(__name__)


def create_api_raw_table(con: duckdb.DuckDBPyConnection, TARGET_TABLE_NAME: str):
    try:
        con.execute(
            f"""CREATE TABLE IF NOT EXISTS {TARGET_TABLE_NAME}
            (
            raw_id VARCHAR,
            raw_json JSON,
            scraped_at VARCHAR,
            scrapper_version VARCHAR,
            )"""
        )
        logger.info(f"Created table {TARGET_TABLE_NAME}")
    except CatalogException:
        pass


def get_remaining_items(
    con: duckdb.DuckDBPyConnection,
    SOURCE_TABLE_NAME: str,
    TARGET_TABLE_NAME: str,
    SOURCE_KEY: str,
) -> list[Any]:
    # Get rows from source table that are not already collected
    try:
        df_samples = con.execute(
            f"""
            SELECT s.{SOURCE_KEY}
            FROM {SOURCE_TABLE_NAME} s
            LEFT JOIN {TARGET_TABLE_NAME} t ON s.{SOURCE_KEY}  = t.raw_id
            WHERE t.raw_id IS NULL
            """
        ).df()
    except CatalogException:
        raise

    try:
        items = df_samples[SOURCE_KEY].to_list()
    except Exception as e:
        logger.error(e)
        raise e

    if items:
        return items
    else:
        return []
