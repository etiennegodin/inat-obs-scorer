import logging
from typing import Union

from ..app.container import Dependencies
from . import (
    ingest_api_observations_workflow,
    ingest_api_taxa_workflow,
)

logger = logging.getLogger(__name__)


def execute(deps: Dependencies, api_limit: Union[None, int]):
    logger.info("Starting api observations workflow")
    ingest_api_observations_workflow.execute(deps, limit=api_limit)

    logger.info("Starting api taxa workflow")
    ingest_api_taxa_workflow.execute(deps, limit=api_limit)
