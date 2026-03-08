"""
Application service - orchestrates workflows with dependency injection.

This is the entry point for all use cases. It handles:
- Dependency injection
- Error handling and translation
- Logging
- Transaction management
"""

import logging

from ..pipeline.exceptions import InatPipelineError, WorkflowError
from ..workflows import (
    ingest_downloads_workflow,
    ingest_inat_api_workflow,
    process_features_workflow,
)
from .container import Dependencies

logger = logging.getLogger(__name__)


class ApplicationService:
    """
    Application service that orchestrates all workflows.

    This is the single entry point for CLI and GUI. It handles:
    - Creating/injecting dependencies
    - Error handling
    - Logging
    - Result formatting
    """

    def __init__(self, deps: Dependencies):
        """
        Initialize application service.

        Args:
            deps: Application dependencies
        """
        self.deps = deps

    def ingest(self, api_limit: int):
        logger.info("Starting ingest downloads workflow")
        try:
            ingest_downloads_workflow.execute(self.deps)

        except InatPipelineError as e:
            logger.error(f"Ingest downloads failed {e}")
            raise WorkflowError(f"Ingest downloads failed failed {e}") from e
        except Exception as e:
            logger.exception(e)

        logger.info("Starting ingest api workflow")
        try:
            ingest_inat_api_workflow.execute(self.deps, limit=api_limit)
        except InatPipelineError as e:
            logger.error(f"Ingest api failed {e}")
            raise WorkflowError(f"Ingest api failed failed {e}") from e
        except Exception as e:
            logger.exception(e)

    def process(self):
        logger.info("Starting process_features workflow")
        try:
            process_features_workflow.execute(self.deps)
        except InatPipelineError as e:
            logger.error(f"Process_features failed {e}")
            raise WorkflowError(f"Process_features failed {e}") from e
        except Exception as e:
            logger.exception("Unexpected error during install")
            raise WorkflowError(f"Install failed: {e}") from e
