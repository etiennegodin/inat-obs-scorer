"""
Application service - orchestrates workflows with dependency injection.

This is the entry point for all use cases. It handles:
- Dependency injection
- Error handling and translation
- Logging
- Transaction management
"""

import logging
from datetime import date

from ..exceptions import DBConnectionError, DBError, InatPipelineError, WorkflowError
from ..queries.params import TrainingSplitParams
from ..workflows import (
    features_workflow,
    ingest_api_workflow,
    ingest_local_workflow,
    ingest_s3_workflow,
    stage_workflow,
    test_s3_workflow,
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

    def ingest_s3(self, args):
        logger.info("Starting S3 ingest workflow")
        try:
            ingest_s3_workflow.execute(self.deps)
        except InatPipelineError as e:
            logger.error(f"Ingest S3 failed {e}")
            raise WorkflowError(f"Ingest S3 failed {e}") from e
        except Exception as e:
            logger.exception(e)

    def test_s3(self, args):
        logger.info("Starting S3 test workflow")
        try:
            test_s3_workflow.execute(self.deps)
        except Exception as e:
            logger.exception(e)
            raise WorkflowError(f"S3 test failed {e}") from e

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

    def ingest(self, args):
        """Run all ingest sub-routines."""
        logger.info("Starting full ingest sequence")
        self.ingest_local(args)
        self.ingest_s3(args)
        self.ingest_api(args)

    def stage(self, args):
        logger.info("Starting stage workflow")
        try:
            stage_workflow.execute(self.deps)
        except InatPipelineError as e:
            logger.error(f"Ingest downloads failed {e}")
            raise WorkflowError(f"Ingest downloads failed failed {e}") from e
        except Exception as e:
            logger.exception(e)

    def features(self):
        logger.info("Starting features workflow")
        try:
            # 1- Train/Val set (2023-01-01 cutoff)
            train_val_params = TrainingSplitParams(
                label_window_days=365,
                scraped_at=date(2026, 3, 1),
                score_window_days=7,
                cutoff_date=date(2023, 1, 1),
                max_val_size=30000,
                val_window_days=410,
                max_test_size=100000,
                gap_days=30,
            )
            features_workflow.execute(
                self.deps, params=train_val_params, feature_set_name="train_val"
            )

            # 2- Full set (2024-01-01 cutoff)
            full_params = TrainingSplitParams(
                label_window_days=365,
                scraped_at=date(2026, 3, 1),
                score_window_days=7,
                cutoff_date=date(2024, 1, 1),
                max_val_size=30000,
                val_window_days=410,
                max_test_size=100000,
                gap_days=30,
            )
            features_workflow.execute(
                self.deps, params=full_params, feature_set_name="full"
            )

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
                version=args.version,
                early_stopping_rounds=args.early_stop,
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

    def run(self, args):
        """Run the full pipeline sequence: ingest -> stage -> features -> train."""
        logger.info("Starting full pipeline run")
        self.ingest(args)
        self.stage(args)
        self.features()
        self.train(args)
