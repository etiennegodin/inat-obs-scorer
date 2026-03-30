"""
Application service - orchestrates workflows with dependency injection.

This is the entry point for all use cases. It handles:
- Dependency injection
- Error handling and translation
- Logging
- Transaction management
"""

import logging

from ..exceptions import DBConnectionError, DBError, InatPipelineError, WorkflowError
from ..workflows import (
    features_workflow,
    ingest_api_workflow,
    ingest_local_workflow,
    test_workflow,
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

    def ingest_local(self, args):
        logger.info("Starting local ingest workflow")
        try:
            ingest_local_workflow.execute(self.deps)
        except InatPipelineError as e:
            logger.error(f"Ingest downloads failed {e}")
            raise WorkflowError(f"Ingest downloads failed failed {e}") from e
        except Exception as e:
            logger.exception(e)

    def ingest_api(self, args):
        logger.info("Starting api ingest workflow")
        try:
            ingest_api_workflow.execute(
                self.deps, rate=args.rate, ignore_not_found=args.ignore_not_found
            )
        except InatPipelineError as e:
            logger.error(f"Ingest downloads failed {e}")
            raise WorkflowError(f"Ingest downloads failed failed {e}") from e
        except Exception as e:
            logger.exception(e)

    def features(self):
        logger.info("Starting features workflow")
        try:
            features_workflow.execute(self.deps)
        except DBError as e:
            # Specific handling
            logger.error("SQL failure in script '%s': %s", e.script, e)
            raise  # re-raise to propagate up, or handle + continue
        except DBConnectionError as e:
            # Specific handling
            logger.warning("Error connecting to '%s': \n%s", e.file, e)
            raise  # re-raise to propagate up, or handle + continue
        except InatPipelineError as e:
            logger.error("Features workflow failed: %s", e)
            raise
        except Exception as e:
            logger.exception("Unexpected error during features workflow")
            raise WorkflowError("Features workflow failed") from e

    def train(self, args):
        logger.info("Starting training workflow")
        try:
            return train_workflow.execute(
                self.deps,
                classifier=args.classifier,
                reducer=args.reducer,
                scaler=args.scaler,
                encoder=args.encoder,
                imputer=args.imputer,
                n_trials=args.n_trials,
                cv_folds=args.cv_folds,
                random_seed=args.seed,
                use_gpu=args.gpu,
                n_jobs=args.n_jobs,
                stopping_rounds=args.stopping_rounds,
            )
        except InatPipelineError as e:
            logger.error("Train workflow failed: %s", e)
            raise
        except Exception as e:
            logger.exception("Unexpected error during train workflow")
            raise WorkflowError("Train workflow failed") from e

    def test(self, args):
        logger.info("Starting testing workflow")
        try:
            return test_workflow.execute(self.deps)

        except InatPipelineError as e:
            logger.error("Test workflow failed: %s", e)
            raise
        except Exception as e:
            logger.exception("Unexpected error during test workflow")
            raise WorkflowError("Test workflow failed") from e
