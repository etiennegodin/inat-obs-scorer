import logging
from pathlib import Path

import duckdb

logger = logging.getLogger(__name__)


def _open_connection(db_path: str) -> duckdb.DuckDBPyConnection:
    # always create a fresh connection; use context manager where possible
    try:
        con = duckdb.connect(database=db_path)
        _load_spatial_extension(con)
        return con

    except Exception as e:
        logger.error(f"Error connection to duckdb {db_path} : \n ", e)
        raise IOError(f"Error connecting : {e}")


def _load_spatial_extension(con: duckdb.DuckDBPyConnection) -> None:
    try:
        con.execute("INSTALL spatial;")
        con.execute("LOAD spatial;")
    except Exception as e:
        logger.error(f"Error loading spatial extension : {e}")


def execute_sql(file: Path, con: duckdb.DuckDBPyConnection):
    with open(file, "r") as f:
        sql_query = f.read()
    try:
        con.execute(sql_query)
    except Exception as e:
        logger.error(e)
        raise e
