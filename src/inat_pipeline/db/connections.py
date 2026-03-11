import logging

import duckdb

logger = logging.getLogger(__name__)


class DuckDBConnection:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._con: duckdb.DuckDBPyConnection | None = None

    def __enter__(self):
        logger.debug("Opening DuckDB connection: %s", self.db_path)
        self._con = duckdb.connect(self.db_path)

        self._load_spatial_extension()  # or whatever extensions you need
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._con:
            self._con.close()
            logger.debug("DuckDB connection closed")
        return False

    def execute(self, query: str, params: tuple = ()):
        return self._con.execute(query, params)

    def executemany(self, query: str, params: list[tuple]):
        return self._con.executemany(query, params)

    def _load_spatial_extension(self) -> None:
        try:
            self._con.execute("INSTALL spatial;")
            self._con.execute("LOAD spatial;")
        except Exception as e:
            logger.error(f"Error loading spatial extension : {e}")
            raise e
