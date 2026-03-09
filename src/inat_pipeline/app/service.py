"""
Application service - orchestrates workflows with dependency injection.

This is the entry point for all use cases. It handles:
- Dependency injection
- Error handling and translation
- Logging
- Transaction management
"""

import logging
from typing import Union

from ..pipeline.exceptions import InatPipelineError, WorkflowError
from ..workflows import (
    features_workflow,
    ingest_downloads_workflow,
    ingest_inat_api_workflow,
    ingest_unpack_workflow,
    train_workflow,
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

    def ingest(self, api_limit: Union[int, None]):
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

        logger.info("Starting ingest unpack workflow")
        try:
            ingest_unpack_workflow.execute(self.deps, limit=api_limit)
        except InatPipelineError as e:
            logger.error(f"Ingest api failed {e}")
            raise WorkflowError(f"Ingest api failed failed {e}") from e
        except Exception as e:
            logger.exception(e)

    def features(self, limit: Union[int, None]):
        logger.info("Starting features workflow")
        try:
            features_workflow.execute(self.deps, limit=limit)
        except InatPipelineError as e:
            logger.error(f"Process_features failed {e}")
            raise WorkflowError(f"Process_features failed {e}") from e
        except Exception as e:
            logger.exception("Unexpected error during install")
            raise WorkflowError(f"Install failed: {e}") from e

    def train(self, args):
        logger.info("Starting training workflow")

        # Catch if test mode
        if args.test:
            logger.info("Test mode, limited to 10 trials")
            n_trials = 10
        else:
            n_trials = args.n_trials

        try:
            return train_workflow.execute(
                self.deps.DB_PATH,
                classifier=args.classifier,
                reducer=args.reducer,
                scaler=args.scaler,
                encoder=args.encoder,
                imputer=args.imputer,
                n_trials=n_trials,
            )
        except InatPipelineError as e:
            logger.error(f"Process_features failed {e}")
            raise WorkflowError(f"Process_features failed {e}") from e
        except Exception as e:
            logger.exception("Unexpected error during install")
            raise WorkflowError(f"Install failed: {e}") from e
