import logging
from pathlib import Path

import duckdb

logger = logging.getLogger(__name__)


def ingest_downloads(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    downloads_path: Path,
    ignore_error: bool = True,
) -> list[Path]:
    create_query = f"""CREATE OR REPLACE TABLE raw.{table_name} AS
            SELECT *
            FROM read_csv_auto('{downloads_path}/*.csv',
            ignore_errors={ignore_error})"""
    files = [file for file in downloads_path.rglob("*.csv")]
    try:
        con.execute(create_query)
        return files
    except Exception as e:
        raise e
