from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline

from ...utils.db import _open_connection
from ...utils.git import get_git_hash
from .config import (
    CATEGORICAL_IMPUTER_REGISTRY,
    ENCODER_REGISTRY,
    IMPUTER_REGISTRY,
    SCALER_REGISTRY,
    _instantiate,
)


def load(db_path: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, str]:
    con = _open_connection(db_path)

    df = con.execute("SELECT * FROM features.training").df()
    train = df[df["split"] == "train"]
    val = df[df["split"] == "val"]
    test = df[df["split"] == "test"]

    train.pop("split")
    val.pop("split")
    test.pop("split")

    split_seed = get_git_hash(short=True)

    return train, val, test, split_seed


def _build_categorical_transformer(config: dict):
    steps = []

    cat_imputer = _instantiate(CATEGORICAL_IMPUTER_REGISTRY, config.categorical_imputer)
    steps.append(("imputer", cat_imputer))

    encoder = _instantiate(ENCODER_REGISTRY, config.encoder)
    steps.append(("encoder", encoder))

    return Pipeline(steps)


def _build_numeric_transformer(config: dict):
    steps = []

    imputer = _instantiate(IMPUTER_REGISTRY, config.numeric_imputer)
    steps.append(("imputer", imputer))

    scaler = _instantiate(SCALER_REGISTRY, config.scaler)
    if scaler is not None:  # scaler = "none" skips this step
        steps.append(("scaler", scaler))

    return Pipeline(steps)


def build_preprocessor(config: dict):
    numeric_transformer = _build_numeric_transformer(config)
    categorical_transformer = _build_categorical_transformer(config)

    preprocessor = ColumnTransformer(
        transformers=[
            ("num", numeric_transformer, config.numeric_features),
            ("cat", categorical_transformer, config.categorical_features),
        ],
        remainder="drop",  # drop any unlisted columns
        verbose_feature_names_out=True,  # keeps feature names for inspection
    )

    return preprocessor


def scale():
    pass


def reduce(df: pd.DataFrame):
    corr_cols = _find_correlated(df)

    if corr_cols:
        pass


def _find_correlated(self, df: pd.DataFrame) -> list:
    df = df.drop(columns=[self.table_id])
    corr = df.corr(numeric_only=True)
    upper_tri = corr.where(np.triu(np.ones(corr.shape), k=1).astype(bool))
    return [
        column for column in upper_tri.columns if any(upper_tri[column].abs() > 0.8)
    ]
