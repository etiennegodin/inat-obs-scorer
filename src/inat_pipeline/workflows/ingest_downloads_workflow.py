import logging

from ..app.container import Dependencies
from ..ingest.downloads import ingest_downloads
from ..utils.db import SQL_Engine, _open_connection

logger = logging.getLogger(__name__)


def execute(deps: Dependencies):
    con = _open_connection(deps.DB_PATH)
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")

    # Ingest csv files to db
    ingested = ingest_downloads(con, "downloads", deps.DOWNLOADS_FOLDER)
    logger.info(f"Ingested {len(ingested)} files ")

    # Ingest taxa
    ingested = ingest_downloads(con, "taxa", deps._RAW_DATA_FOLDER / "taxa")
    logger.info(f"Ingested {len(ingested)} files ")

    # Select observations to sample
    sql = SQL_Engine(con, deps.SQL_INGEST_PATH)
    sql.execute("api_samples")
