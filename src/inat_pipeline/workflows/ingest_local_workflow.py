import logging

from ..app.container import Dependencies
from ..db import DuckDBConnection, SQLEngine

logger = logging.getLogger(__name__)


def execute(deps: Dependencies):
    with DuckDBConnection(deps.DB_PATH) as con:
        data_dir = deps._RAW_DATA_FOLDER

        con.execute("CREATE SCHEMA IF NOT EXISTS raw")

        sql = SQLEngine(con, deps.SQL_STAGE_PATH)

        # Ingest observations csv files
        source = "downloads"
        sql.execute("ingest_csv", [True], table_name=source, source=data_dir / source)

        # Ingest taxa
        source = "taxa"
        sql.execute("ingest_csv", [True], table_name=source, source=data_dir / source)
        sql.execute("stage_taxa")

        # Ingest places
        source = "places"
        sql.execute("ingest_csv", [True], table_name=source, source=data_dir / source)
