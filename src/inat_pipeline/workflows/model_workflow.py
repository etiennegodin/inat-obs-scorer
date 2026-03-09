import logging
import warnings
from pathlib import Path

import mlflow
import mlflow.sklearn
import optuna

from ..pipeline.model import config, core

logger = logging.getLogger(__name__)

# Suppress noisy warnings during hyperparameter search
warnings.filterwarnings("ignore", category=UserWarning)
optuna.logging.set_verbosity(optuna.logging.WARNING)


def execute(
    db_path: Path,
    classifier: str,
    reducer: str,
    scaler: str,
    encoder: str,
    imputer: str,
    n_trials: int,
):
    # Data Loader
    X_train, y_train, X_val, y_val, X_test, y_test, split_seed = core.load_and_split(
        db_path
    )

    # Initialise pipeline configs
    pipe_conf = config.PipelineConfig(
        classifier=classifier,
        numeric_imputer=imputer,
        reducer=reducer,
        scaler=scaler,
        encoder=encoder,
        n_trials=n_trials,
    )

    mlflow.set_experiment(pipe_conf.experiment_name)

    # Store features from dataframe
    pipe_conf.set_features(X_test)

    # Override features type
    pipe_conf.change_feature_type("oauth_application_id")

    # Build pipeline from config
    pipe = core.build_pipeline(pipe_conf)

    logger.info(f"Fitting {pipe_conf.experiment_name}")

    pipe.fit(X_train, y_train)

    logger.info("Model workflow")
