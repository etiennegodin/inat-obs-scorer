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
    IMPUTER_REGISTRY,
    REDUCER_REGISTRY,
    SCALER_REGISTRY,
)
from .utils import _instantiate

logger = logging.getLogger(__name__)


class CustomCvSplit(BaseCrossValidator):
    def __init__(self, n_splits=3):
        self.n_splits = n_splits

    def split(self, X, y=None, groups=None):
        n_samples = len(X)
        indices = np.arange(n_samples)
        chunks = np.array_split(indices, self.n_splits)
        for i in range(self.n_splits - 1):
            # In a basic time-series CV,
            # you train on the past and test on the next block
            # For 3 equal sets:
            # Iter 1: Train on Set 1, Test on Set 2
            # Iter 2: Train on Set 1+2, Test on Set 3
            train_idx = np.concatenate(chunks[: i + 1])
            test_idx = chunks[i + 1]
            yield train_idx, test_idx

    def get_n_splits(self, X=None, y=None, groups=None):
        return 1


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
    df.set_index("observation_id", inplace=True)

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

    imputer = _instantiate(IMPUTER_REGISTRY, config.numeric_imputer)
    steps.append(("imputer", imputer))

    scaler = _instantiate(SCALER_REGISTRY, config.scaler)
    if scaler is not None:  # scaler = "none" skips this step
        steps.append(("scaler", scaler))

    return Pipeline(steps)


def build_preprocessor(config: PipelineConfig) -> ColumnTransformer:
    numeric_transformer = _build_numeric_transformer(config)
    categorical_transformer = _build_categorical_transformer(config)

    preprocessor = ColumnTransformer(
        transformers=[
            ("num", numeric_transformer, config.numeric_features),
            ("cat", categorical_transformer, config.categorical_features),
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
