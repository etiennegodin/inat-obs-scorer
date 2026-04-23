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
        schema_path: Path | None = None,
        read_only: bool = False,
    ):
        self.db_path = db_path
        self.schema_path = schema_path
        self.read_only = read_only
        self._con: duckdb.DuckDBPyConnection | None = None
        self._attached_aliases: set[str] = set()

    def __enter__(self):
        logger.debug(
            "Connecting to database: %s (read_only=%s)",
            self.db_path,
            self.read_only,
        )
        try:
            self._con = duckdb.connect(
                str(self.db_path),
                read_only=self.read_only,
                config={"allow_unsigned_extensions": "true"},
            )
            self._setup_connection()
            self._init_resources()
        except duckdb.Error as e:
            logger.error("DuckDB connection failed: %s", e)
            raise DBConnectionError(str(e), file=self.db_path) from e
        except Exception:
            if self._con:
                self._con.close()
            raise

        return self

    def attach_readonly_database(self, db_path: str | Path, alias: str):
        """Lazily attach another database in read-only mode."""
        if alias in self._attached_aliases:
            return

        if not self._con:
            raise DBConnectionError(
                "Database connection not established. Use within context manager."
            )

        logger.info("Attaching database %s as %s (READ_ONLY)", db_path, alias)
        try:
            self._con.execute(f"ATTACH '{db_path}' AS {alias} (READ_ONLY)")
            self._attached_aliases.add(alias)
        except duckdb.Error as e:
            logger.error("Failed to attach database %s: %s", db_path, e)
            raise DBError(f"Failed to attach {alias}: {e}") from e

    def create_proxy_schemas(
        self, attached_alias: str, ignore_schemas: list = ["meta", "tests"]
    ):
        # Creating a view in database that points to attached db
        schemas = self._con.execute(
            f"""SELECT schema_name
                    FROM information_schema.schemata
                    WHERE catalog_name = '{attached_alias}'"""
        ).fetchall()

        for (schema,) in schemas:
            if schema in ("main", "information_schema", "pg_catalog"):
                continue
            if schema in ignore_schemas:
                continue

            self._con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
            tables = self._con.execute(
                f"""SELECT table_name
                FROM information_schema.tables
                WHERE table_catalog = '{attached_alias}'
                    AND table_schema = '{schema}'"""
            ).fetchall()
            for (table,) in tables:
                # Creating a view in features database that points to raw db
                self._con.execute(
                    f"""CREATE OR REPLACE VIEW {schema}.{table} AS
                    SELECT *
                    FROM {attached_alias}.{schema}.{table};"""
                )
        # Set search path to prioritize local schemas
        self._con.execute("SET search_path = 'main';")

    def _setup_connection(self):
        """Install and load required extensions."""
        self._con.execute("INSTALL spatial; LOAD spatial;")
        self._con.execute("INSTALL httpfs; LOAD httpfs;")
        self._con.execute("SET s3_region='us-east-1';")
        self._con.install_extension("duckpgq", repository="community")
        self._con.load_extension("duckpgq")

    def _init_resources(self):
        """Initialize schemas and macros if paths are provided."""
        logger.debug("Initialize schemas and macros if paths are provided.")
        if self.schema_path and self.schema_path.exists():
            self._init_schema()

    def _init_schema(self):
        """Run all .sql files in the schema directory."""
        logger.debug(self.schema_path)
        for sql_file in sorted(self.schema_path.glob("*.sql")):
            try:
                self._con.execute(sql_file.read_text())
                logger.debug(f"Ran {sql_file} schema")
            except duckdb.Error as e:
                logger.error("Error initializing schema from %s: %s", sql_file, e)

    def load_macros(self, macro_path: Path):
        """Run all .sql files in the macros directory."""
        for sql_file in sorted(macro_path.glob("*.sql")):
            try:
                self._con.execute(sql_file.read_text())
            except duckdb.Error as e:
                logger.error("Error loading macro from %s: %s", sql_file, e)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._con:
            self._con.close()
            logger.debug("DuckDB connection closed")
        return False

    def execute(self, query: str, params: Any | None = None, script: str | None = None):
        try:
            return self._con.execute(query, params)
        except duckdb.Error as e:
            raise DBError(str(e), script=script, details={"params": params}) from e

    def executemany(self, query: str, params: list[tuple]):
        return self._con.executemany(query, params)

    def commit(self):
        return self._con.commit()
