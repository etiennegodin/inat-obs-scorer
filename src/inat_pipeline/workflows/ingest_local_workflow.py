import logging

from ..app.container import Dependencies
from ..ingest.downloads import ingest_downloads
from ..utils.db import _open_connection

logger = logging.getLogger(__name__)


def execute(deps: Dependencies):
    con = _open_connection(deps.DB_PATH)
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")

    # Ingest observations csv files
    source = "downloads"
    ingested = ingest_downloads(con, "downloads", deps.DOWNLOADS_FOLDER)
    logger.info(f"Ingested {len(ingested)} files from raw/{source}")

    # Ingest taxa
    source = "taxa"
    ingested = ingest_downloads(con, source, deps._RAW_DATA_FOLDER / source)
    logger.info(f"Ingested {len(ingested)} files raw/{source} ")

    # Ingest places
    source = "places"
    ingested = ingest_downloads(con, source, deps._RAW_DATA_FOLDER / source)
    logger.info(f"Ingested {len(ingested)} files raw/{source}")
