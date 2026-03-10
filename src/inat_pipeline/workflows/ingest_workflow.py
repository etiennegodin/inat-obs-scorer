import logging
from typing import Union

from ..app.container import Dependencies
from . import (
    ingest_api_observations_workflow,
    ingest_api_taxa_workflow,
    ingest_downloads_workflow,
)

logger = logging.getLogger(__name__)


def execute(deps: Dependencies, limit: Union[int, None]):
    logger.info("Starting downloads workflow")
    ingest_downloads_workflow.execute(deps)

    logger.info("Starting api observations workflow")
    ingest_api_observations_workflow.execute(deps, limit=limit)

    logger.info("Starting api taxa workflow")
    ingest_api_taxa_workflow.execute(deps, limit=limit)
