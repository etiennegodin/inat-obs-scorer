"""
Application service - orchestrates workflows with dependency injection.

This is the entry point for all use cases. It handles:
- Dependency injection
- Error handling and translation
- Logging
- Transaction management
"""

import logging

from ..workflows import ingest_downloads_workflow, ingest_inat_api_workflow
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
        self.logger = deps.logger

    def ingest_download_data(self):
        self.logger.info("Starting ingest workflow")

        try:
            ingest_downloads_workflow.execute(self.deps)
        except Exception as e:
            self.logger.exception(e)

    def ingest_inat_api_data(self):
        self.logger.info("Starting ingest workflow")

        try:
            ingest_inat_api_workflow.execute(self.deps)
        except Exception as e:
            self.logger.exception(e)
