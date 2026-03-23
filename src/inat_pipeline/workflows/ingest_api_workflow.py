import logging

from ..app.container import Dependencies
from . import (
    ingest_api_observations_histogram,
    ingest_api_observations_workflow,
    ingest_api_similar_species_workflow,
)

logger = logging.getLogger(__name__)


def execute(deps: Dependencies, rate: int, ignore_not_found: bool):
    logger.info("Starting api observations workflow")
    ingest_api_observations_workflow.execute(
        deps, rate=rate, ignore_not_found=ignore_not_found
    )

    logger.info("Starting api taxa workflow")
    ingest_api_similar_species_workflow.execute(
        deps, rate=rate, ignore_not_found=ignore_not_found
    )

    logger.info("Starting api observations_histogram workflow")
    ingest_api_observations_histogram.execute(
        deps, rate=rate, ignore_not_found=ignore_not_found
    )
