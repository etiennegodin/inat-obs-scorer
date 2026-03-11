import logging

from ..app.container import Dependencies
from ..utils.db import SQL_Engine, duckdb_con
from . import (
    ingest_api_similar_species_workflow,
)

logger = logging.getLogger(__name__)


def execute(deps: Dependencies, rate: int, ignore_not_found: bool):
    logger.info("Running sample strategy to select observations to query")
    con = duckdb_con(deps.DB_PATH)
    sql = SQL_Engine(con, deps.SQL_API_PATH)
    sql.execute("sample")

    logger.info("Starting api observations workflow")
    # ingest_api_observations_workflow.execute(deps,
    #  rate=rate, ignore_not_found = ignore_not_found)

    logger.info("Starting api taxa workflow")
    ingest_api_similar_species_workflow.execute(
        deps, rate=rate, ignore_not_found=ignore_not_found
    )
