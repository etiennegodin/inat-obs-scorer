import logging

from ..app.container import Dependencies
from ..utils.db import SQL_Engine, _open_connection
from . import (
    ingest_api_similar_species_workflow,
)

logger = logging.getLogger(__name__)


def execute(deps: Dependencies, limiter: int):
    logger.info("Running sample strategy to select observations to query")
    con = _open_connection(deps.DB_PATH)
    sql = SQL_Engine(con, deps.SQL_API_PATH)
    sql.execute("sample")

    logger.info("Starting api observations workflow")
    # ingest_api_observations_workflow.execute(deps, limiter=limiter)

    logger.info("Starting api taxa workflow")
    ingest_api_similar_species_workflow.execute(deps, limiter=limiter)
