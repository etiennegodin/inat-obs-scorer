import logging
import warnings
from pathlib import Path

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
    # Initialise pipeline configs
    pipe_conf = config.PipelineConfig(
        classifier=classifier,
        numeric_imputer=imputer,
        reducer=reducer,
        scaler=scaler,
        encoder=encoder,
        n_trials=n_trials,
    )

    # Data Loader
    X_train, y_train, X_val, y_val, X_test, y_test, data_stats = core.load_and_split(
        db_path, pipe_conf
    )

    print(pipe_conf.categorical_features)

    # mlflow.set_experiment(pipe_conf.experiment_name)
    """
    with mlflow.start_run(run_name=f"{config.classifier}_optuna") as parent_run:
        parent_run_id = parent_run.info.run_id
        print(f"\n{'=' * 60}")
        print(f"MLflow Run ID: {parent_run_id}")
        print(f"Classifier:    {config.classifier}")
        print(f"Trials:        {config.n_trials}")
        print(f"CV Folds:      {config.cv_folds}")
        print(f"{'=' * 60}\n")

        # Log full config so this run is 100% reproducible
        mlflow.log_params(config.to_dict())

        # Log data statistics
        mlflow.log_metrics(data_stats)
    # Build pipeline from config
    pipe = core.build_pipeline(pipe_conf)

    logger.info(f"Fitting {pipe_conf.experiment_name}")

    pipe.fit(X_train, y_train)

    logger.info("Model workflow")
    """
