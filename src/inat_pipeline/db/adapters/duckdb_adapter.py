import logging
from typing import Any

import duckdb

from ...exceptions import DBConnectionError, DBError

logger = logging.getLogger(__name__)


class DuckDBAdapter:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._con: duckdb.DuckDBPyConnection | None = None

    def __enter__(self):
        logger.debug("Opening DuckDB connection: %s", self.db_path)
        try:
            self._con = duckdb.connect(
                self.db_path, config={"allow_unsigned_extensions": "true"}
            )
        except duckdb.IOException as e:
            raise DBConnectionError(str(e), file=self.db_path)

        # Spatial extension
        self._con.execute("INSTALL spatial;")
        self._con.execute("LOAD spatial;")

        # duckpgq extension
        # self._load_duckpgq_extension()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._con:
            self._con.close()
            logger.debug("DuckDB connection closed")
        return False

    def execute(self, query: str, params: Any, script: str | None = None):
        try:
            return self._con.execute(query, params)
        except duckdb.CatalogException as e:
            raise DBError(str(e), script=script) from e
        except duckdb.BinderException as e:
            raise DBError(str(e), script=script) from e
        except duckdb.Error as e:
            raise DBError(str(e), script=script) from e

    def executemany(self, query: str, params: list[tuple]):
        return self._con.executemany(query, params)

    def _load_duckpgq_extension2(self):
        self._con.execute(
            "SET custom_extension_repository = 'http://duckpgq.s3.eu-north-1.amazonaws.com';"
        )
        self._con.execute("FORCE INSTALL 'duckpgq';")
        self._con.execute("LOAD 'duckpgq';")

    def _load_duckpgq_extension(self):
        self._con.execute("INSTALL duckpgq FROM community;;")
        self._con.execute("LOAD 'duckpgq';")
