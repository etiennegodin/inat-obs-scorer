import logging

from ..app.container import Dependencies

logger = logging.getLogger(__name__)


def execute(deps: Dependencies):
    # con = _open_connection(deps.DB_PATH)

    logger.info("Model workflow")
