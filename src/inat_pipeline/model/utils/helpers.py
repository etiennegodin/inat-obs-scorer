import importlib
from typing import Optional

import pandas as pd
from sklearn.pipeline import Pipeline

from ..config import PipelineConfig


def get_features_stats(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    config: PipelineConfig,
) -> dict:
    # ── Log basic data stats ──────
    return {
        "data/n_rows_total": len(X_train),
        "data/n_features_numeric": len(config.numeric_features),
        "data/n_features_cat": len(config.categorical_features),
        "data/target_positive_rate": float(y_train.mean()),
        "data/scale_pos_weight": (y_train == 0).sum() / (y_train == 1).sum(),
    }


def describe_pipeline(pipeline: Pipeline) -> dict:
    """
    Returns a human-readable description of all pipeline steps and their params.
    Useful for logging to MLflow as a run artifact (saved as pipeline_description.json).
    """
    description = {}
    for name, step in pipeline.steps:
        if hasattr(step, "steps"):  # nested Pipeline (num/cat sub-pipelines)
            description[name] = {
                sub_name: {
                    "class": type(sub_step).__name__,
                    "params": sub_step.get_params(),
                }
                for sub_name, sub_step in step.steps
            }
        elif hasattr(step, "transformers"):  # ColumnTransformer
            description[name] = {
                t_name: {
                    "class": (
                        type(t_step).__name__
                        if not hasattr(t_step, "steps")
                        else "Pipeline"
                    ),
                    "columns": cols,
                }
                for t_name, t_step, cols in step.transformers
            }
        else:
            description[name] = {
                "class": type(step).__name__,
                "params": step.get_params(),
            }
    return description


# ── HELPER: instantiate a class from the registry ──────────────────────────────


def _instantiate(registry: dict, key: str, override_params: Optional[dict] = None):
    """
    Looks up `key` in `registry`, imports the class, and returns an instance.

    WHY DYNAMIC IMPORT?
      We don't want to import xgboost, lightgbm, umap, etc. at module load time —
      users may not have all of them installed. Import only what's actually needed.

    Args:
        registry:        One of the *_REGISTRY dicts from config.py
        key:             The string key (e.g. "median", "random_forest")
        override_params: Extra params that Optuna may inject at trial time

    Returns:
        An instantiated sklearn-compatible transformer or estimator.
        Returns None if the registry entry is None (i.e. step is disabled).
    """
    entry = registry.get(key)
    if entry is None:
        return None  # step is disabled (e.g. reducer = "none")

    module_path, class_name, default_params = entry
    params = {**default_params, **(override_params or {})}

    module = importlib.import_module(module_path)
    cls = getattr(module, class_name)
    return cls(**params)
