import pandas as pd

from .config import PipelineConfig
from .core import build_pipeline


def train_final_model(
    config: PipelineConfig,
    best_params: dict,
    X_train: pd.DataFrame,
    y_train: pd.Series,
) -> object:
    """
    After Optuna finds the best hyperparameters, we retrain on the FULL training set.

    WHY RETRAIN ON FULL DATA?
      During cross-validation, each fold only trains on K-1/K of the data.
      The final model should see all available training data to maximize performance.
      We use the winning hyperparameters, but now fit on everything.
    """
    # Strip "classifier__" prefix from Optuna param names
    classifier_params = {
        k.replace("classifier__", ""): v for k, v in best_params.items()
    }

    # Inject random state
    classifier_params["random_state"] = config.random_seed

    final_pipeline = build_pipeline(config, classifier_params=classifier_params)
    final_pipeline.fit(X_train, y_train)

    return final_pipeline
