import logging

from ..app.container import Dependencies
from ..db.utils import DuckDBConnection
from ..ingest.downloads import ingest_downloads

logger = logging.getLogger(__name__)


def execute(deps: Dependencies):
    with DuckDBConnection(deps.DB_PATH) as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS raw")

        # Ingest observations csv files
        source = "downloads"

        ingested = ingest_downloads(con, "downloads", deps.DOWNLOADS_FOLDER)
        logger.info(f"Ingested {len(ingested)} files from raw/{source}")

        # Ingest taxa
        source = "taxa"
        ingested = ingest_downloads(con, source, deps._RAW_DATA_FOLDER / source)
        logger.info(f"Ingested {len(ingested)} files from raw/{source} ")

        # Ingest places
        source = "places"
        ingested = ingest_downloads(
            con, source, deps._RAW_DATA_FOLDER / source, ignore_error=False
        )
        logger.info(f"Ingested {len(ingested)} files from raw/{source}")
