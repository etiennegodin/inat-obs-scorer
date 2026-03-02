from ..app.container import Dependencies
from ..pipeline.ingest.downloads import ingest_downloads


def execute(deps: Dependencies):
    logger = deps.logger
    try:
        ingested = ingest_downloads(deps.RAW_DB_PATH, deps.DOWNLOADS_FOLDER)
        logger.info(f"Ingested {len(ingested)} files ")
        ###
        # clean
    except Exception as e:
        logger.exception(e)
