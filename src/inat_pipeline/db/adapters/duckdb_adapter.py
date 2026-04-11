import logging
from pathlib import Path
from typing import Any

import duckdb

from ...exceptions import DBConnectionError, DBError

logger = logging.getLogger(__name__)


class DuckDBAdapter:
    def __init__(
        self,
        db_path: str,
        attach_path: str | None = None,
        attach_alias: str | None = None,
        read_only: bool = False,
        schema_path: Path | None = None,
    ):
        self.db_path = db_path
        self.attach_path = attach_path
        self.attach_alias = attach_alias
        self.read_only = read_only
        self.schema_path = schema_path
        self._con: duckdb.DuckDBPyConnection | None = None

    def __enter__(self):
        # We connect to the feature database (writable)
        # and attach the raw database as 'raw_db' (read-only)
        logger.debug(
            "Opening DuckDB connection: %s (read_only=%s)", self.db_path, self.read_only
        )
        try:
            # Main connection is the writable database
            self._con = duckdb.connect(
                str(self.db_path),
                read_only=False,  # Must be writable to create schemas/views
                config={"allow_unsigned_extensions": "true"},
            )

            if self.attach_path and self.attach_alias:
                # db_path is RAW_DB_PATH and attach_path is FEATURES_DB_PATH
                # So we connect to FEATURES_DB_PATH and attach RAW_DB_PATH
                self._con.close()
                self._con = duckdb.connect(
                    str(self.attach_path), config={"allow_unsigned_extensions": "true"}
                )
                logger.debug(
                    "Attached raw database %s as %s", self.db_path, self.attach_alias
                )
                self._con.execute(
                    f"ATTACH '{self.db_path}' AS {self.attach_alias} (READ_ONLY TRUE);"
                )

                # To make 'staged.table' work, we proxy schemas from the raw database
                # into the features database using views.
                schemas = self._con.execute(
                    f"""SELECT schema_name
                    FROM information_schema.schemata
                    WHERE catalog_name = '{self.attach_alias}'"""
                ).fetchall()
                for (schema,) in schemas:
                    if schema in ("main", "information_schema", "pg_catalog"):
                        continue

                    self._con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
                    tables = self._con.execute(
                        f"""SELECT table_name
                        FROM information_schema.tables
                        WHERE table_catalog = '{self.attach_alias}'
                            AND table_schema = '{schema}'"""
                    ).fetchall()
                    for (table,) in tables:
                        # Create a view that points to the raw database
                        # This allows 'staged.table' to work locally.
                        self._con.execute(
                            f"""CREATE OR REPLACE VIEW {schema}.{table} AS
                              SELECT * FROM {self.attach_alias}.{schema}.{table};"""
                        )

                # Set search path to prioritize local schemas
                self._con.execute("SET search_path = 'main';")
            else:
                # Standard single-db connection
                self._con.close()
                self._con = duckdb.connect(
                    str(self.db_path),
                    read_only=self.read_only,
                    config={"allow_unsigned_extensions": "true"},
                )
        except duckdb.IOException as e:
            raise DBConnectionError(str(e), file=self.db_path)

        # Spatial extension
        self._con.execute("INSTALL spatial;")
        self._con.execute("LOAD spatial;")

        # httpfs extension
        self._con.execute("INSTALL httpfs;")
        self._con.execute("LOAD httpfs;")
        self._con.execute("SET s3_region='us-east-1';")

        # duckpgq extension
        self._con.install_extension("duckpgq", repository="community")
        self._con.load_extension("duckpgq")

        # Run schema initialization if path provided
        if self.schema_path and self.schema_path.exists():
            self._init_schema()

        return self

    def _init_schema(self):
        """Run all .sql files in the schema directory."""
        logger.debug("Initializing schemas from %s", self.schema_path)
        for sql_file in sorted(self.schema_path.glob("*.sql")):
            logger.debug("Running schema file: %s", sql_file.name)
            try:
                self._con.execute(sql_file.read_text())
            except duckdb.Error as e:
                logger.error("Error initializing schema from %s: %s", sql_file, e)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._con:
            self._con.close()
            logger.debug("DuckDB connection closed")
        return False

    def execute(self, query: str, params: Any | None = None, script: str | None = None):
        # logger.debug(query)
        # logger.debug(params)
        try:
            return self._con.execute(query, params)
        except duckdb.ParserException as e:
            raise DBError(str(e), script=script, details={"params": params}) from e
        except duckdb.InvalidInputException as e:
            raise DBError(str(e), script=script, details={"params": params}) from e
        except duckdb.CatalogException as e:
            raise DBError(str(e), script=script) from e
        except duckdb.BinderException as e:
            raise DBError(str(e), script=script) from e
        except duckdb.Error as e:
            raise DBError(str(e), script=script) from e

    def executemany(self, query: str, params: list[tuple]):
        return self._con.executemany(query, params)

    def commit(self):
        return self._con.commit()
