import logging
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.model_selection import BaseCrossValidator
from sklearn.pipeline import Pipeline

from .config import PipelineConfig
from .registery import (
    CATEGORICAL_IMPUTER_REGISTRY,
    CLASSIFIER_REGISTRY,
    ENCODER_REGISTRY,
    REDUCER_REGISTRY,
    SCALER_REGISTRY,
)
from .utils import _instantiate

logger = logging.getLogger(__name__)


class ExpandingWindowCvSplit(BaseCrossValidator):
    def __init__(self, n_folds=3, gap_days: int = 30, date_col="created_at"):
        self.n_folds = n_folds
        self.gap_days = gap_days
        self.date_col = date_col

    def split(self, X, y=None, groups=None):
        indices = np.arange(len(X))
        dates = pd.to_datetime(X[self.date_col].values)
        gap = pd.Timedelta(days=self.gap_days)

        chunks = np.array_split(indices, self.n_folds + 1)
        logger.debug(f"Using a {self.gap_days} days gap")
        for i in range(self.n_folds):
            train_idx = np.concatenate(chunks[: i + 1])
            start_train_idx = train_idx
            val_idx = chunks[i + 1]

            if self.gap_days > 0:
                # Boundary: first date in the raw val chunk (before any trimming)
                val_boundary = dates[val_idx[0]]
                logger.debug(f"val start boundary : {val_boundary}")
                logger.debug(f"val end boundary : {dates[val_idx[-1]]}")

                train_cutoff = val_boundary - gap
                train_idx = train_idx[dates[train_idx] <= train_cutoff]

            if len(train_idx) == 0 or len(val_idx) == 0:
                # Degenerate fold — gap consumed the entire train or val set.
                # Log a warning rather than silently skipping.
                logger.warning(
                    f"Fold {i + 1}: gap_days={self.gap_days} produced an empty "
                    f"{'train' if len(train_idx) == 0 else 'val'} set. "
                    "Consider reducing gap_days or n_folds."
                )
                continue

            logger.debug(f"{len(train_idx)} train observations")
            logger.debug(f"{len(val_idx)} val observations")

            logger.debug(f"Removed {len(start_train_idx) - len(train_idx)}")
            yield train_idx, val_idx

    def get_n_splits(self, X=None, y=None, groups=None):
        return self.n_folds


def load_and_split(
    features_path: Path,
) -> tuple[
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    dict,
]:
    df = pd.read_parquet(features_path)

    # Fix timezone
    df["created_at"] = df["created_at"].dt.tz_convert("UTC").dt.tz_localize(None)

    # Set index
    df.set_index("observation_id", inplace=True)

    # Double check ordered
    df.sort_index()

    # Drop taxon_id
    df.drop(columns=["taxon_id"], inplace=True)

    # Splits
    train = df[df["split"] == "train"]
    val = df[df["split"] == "val"]
    test = df[df["split"] == "test"]

    train.pop("split")
    val.pop("split")
    test.pop("split")

    y_train = train["label"]
    train.pop("label")

    y_val = val["label"]
    val.pop("label")

    y_test = test["label"]
    test.pop("label")

    return train, y_train, val, y_val, test, y_test


def _build_categorical_transformer(config: PipelineConfig) -> Pipeline:
    steps = []

    cat_imputer = _instantiate(CATEGORICAL_IMPUTER_REGISTRY, config.categorical_imputer)
    steps.append(("imputer", cat_imputer))

    encoder = _instantiate(ENCODER_REGISTRY, config.encoder)
    steps.append(("encoder", encoder))

    return Pipeline(steps)


def _build_numeric_transformer(config: PipelineConfig) -> Pipeline:
    steps = []

    # imputer = _instantiate(IMPUTER_REGISTRY, config.numeric_imputer)
    # steps.append(("imputer", imputer))

    scaler = _instantiate(SCALER_REGISTRY, config.scaler)
    if scaler is not None:  # scaler = "none" skips this step
        steps.append(("scaler", scaler))

    return Pipeline(steps)


def _build_null_transformer(config: PipelineConfig) -> Pipeline:
    steps = []

    scaler = _instantiate(SCALER_REGISTRY, "robust")  # force scaler for Nans
    if scaler is not None:  # scaler = "none" skips this step
        steps.append(("scaler", scaler))
        # If no scaler is provided, we return "passthrough"
        # as a string instead of an empty Pipeline
        if not steps:
            return "passthrough"

    return Pipeline(steps)


def build_preprocessor(config: PipelineConfig) -> ColumnTransformer:
    numeric_transformer = _build_numeric_transformer(config)
    categorical_transformer = _build_categorical_transformer(config)
    null_transformer = _build_null_transformer(config)

    preprocessor = ColumnTransformer(
        transformers=[
            ("num", numeric_transformer, config.numeric_features),
            ("cat", categorical_transformer, config.categorical_features),
            ("pass", null_transformer, config.passthrough_features),  # Add this line
        ],
        remainder="drop",  # drop any unlisted columns
        verbose_feature_names_out=config.ct_verbose_feature_names_out,
    )
    return preprocessor


def build_pipeline(
    config: PipelineConfig, classifier_params: Optional[dict] = None
) -> Pipeline:
    steps = []
    # Step 1: preprocessing (always present)
    preprocessor = build_preprocessor(config)
    steps.append(("preprocessor", preprocessor))

    # Step 2: dimensionality reduction (optional)
    reducer = _instantiate(REDUCER_REGISTRY, config.reducer)
    if reducer is not None:
        steps.append(("reducer", reducer))

    # Step 3: classifier
    classifier = _instantiate(CLASSIFIER_REGISTRY, config.classifier, classifier_params)
    steps.append(("classifier", classifier))

    pipeline = Pipeline(steps, verbose=False)
    return pipeline
