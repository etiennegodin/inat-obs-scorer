import logging

from ..app.container import Dependencies
from ..pipeline.ingest.downloads import ingest_downloads, select_sample_observations
from ..utils.db import _open_connection

logger = logging.getLogger(__name__)


def execute(deps: Dependencies):
    con = _open_connection(deps.RAW_DB_PATH)

    # Ingest csv files t db
    try:
        ingested = ingest_downloads(con, deps.DOWNLOADS_FOLDER)
        logger.info(f"Ingested {len(ingested)} files ")
        ###
        # clean
    except Exception as e:
        logger.exception(e)

    # Select observations to sample
    try:
        select_sample_observations(con)
    except Exception as e:
        raise e
