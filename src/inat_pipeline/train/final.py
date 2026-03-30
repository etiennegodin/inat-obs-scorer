import logging

import lightgbm as lgb
import pandas as pd

from .config import PipelineConfig
from .core import build_pipeline
from .registery import LIGHTGBM_GPU_PARAMS

logger = logging.getLogger(__name__)


def train_final_model(
    config: PipelineConfig,
    best_params: dict,
    X_train: pd.DataFrame,
    y_train: pd.Series,
    X_val: pd.DataFrame,
    y_val: pd.Series,
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

    # Inject classifier params from config
    classifier_params["random_state"] = config.random_seed
    classifier_params["n_jobs"] = config.n_jobs

    # Inject gpu params if flagged
    if config.use_gpu:
        classifier_params.update(LIGHTGBM_GPU_PARAMS)

    final_pipeline = build_pipeline(config, classifier_params=classifier_params)
    X_train_transformed = final_pipeline[:-1].fit_transform(X_train, y_train)
    X_val_transformed = final_pipeline[:-1].transform(X_val)

    model = final_pipeline.named_steps["classifier"]

    model.fit(
        X_train_transformed,
        y_train,
        eval_set=[(X_val_transformed, y_val)],
        eval_metric="average_precision",
        callbacks=[
            # LightGBM native early stopping callback
            # 'stopping_rounds' is now a callback in newer versions
            lgb.early_stopping(stopping_rounds=config.stopping_rounds, verbose=False),
            lgb.log_evaluation(period=0),  # keep logs clean
        ],
    )
    logger.debug(f"Final model number of trees: {model.booster_.num_trees()}")

    return final_pipeline
