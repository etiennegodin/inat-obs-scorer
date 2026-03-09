import importlib
import logging
from dataclasses import asdict, dataclass, field
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)


IMPUTER_REGISTRY = {
    "median": ("sklearn.impute", "SimpleImputer", {"strategy": "median"}),
    "mean": ("sklearn.impute", "SimpleImputer", {"strategy": "mean"}),
    "knn": ("sklearn.impute", "KNNImputer", {"n_neighbors": 5}),
    "constant": (
        "sklearn.impute",
        "SimpleImputer",
        {"strategy": "constant", "fill_value": 0},
    ),
}

CATEGORICAL_IMPUTER_REGISTRY = {
    "most_frequent": ("sklearn.impute", "SimpleImputer", {"strategy": "most_frequent"}),
    "constant": (
        "sklearn.impute",
        "SimpleImputer",
        {"strategy": "constant", "fill_value": "missing"},
    ),
}

ENCODER_REGISTRY = {
    "onehot": (
        "sklearn.preprocessing",
        "OneHotEncoder",
        {"handle_unknown": "ignore", "sparse_output": False},
    ),
    "ordinal": (
        "sklearn.preprocessing",
        "OrdinalEncoder",
        {"handle_unknown": "use_encoded_value", "unknown_value": -1},
    ),
    "target": (
        "category_encoders",
        "TargetEncoder",
        {},
    ),  # pip install category_encoders
}

SCALER_REGISTRY = {
    "standard": ("sklearn.preprocessing", "StandardScaler", {}),
    "minmax": ("sklearn.preprocessing", "MinMaxScaler", {}),
    "robust": ("sklearn.preprocessing", "RobustScaler", {}),  # good when outliers exist
    "none": None,  # skip scaling entirely
}

REDUCER_REGISTRY = {
    "pca": (
        "sklearn.decomposition",
        "PCA",
        {"n_components": 0.95},
    ),  # keep 95% variance
    "svd": ("sklearn.decomposition", "TruncatedSVD", {"n_components": 50}),
    "umap": ("umap", "UMAP", {"n_components": 2}),  # pip install umap-learn
    "none": None,  # skip reduction entirely
}

CLASSIFIER_REGISTRY = {
    "random_forest": (
        "sklearn.ensemble",
        "RandomForestClassifier",
        {"random_state": 42},
    ),
    "gradient_boost": (
        "sklearn.ensemble",
        "GradientBoostingClassifier",
        {"random_state": 42},
    ),
    "xgboost": (
        "xgboost",
        "XGBClassifier",
        {"random_state": 42, "eval_metric": "logloss"},
    ),
    "logistic": (
        "sklearn.linear_model",
        "LogisticRegression",
        {"max_iter": 500, "random_state": 42},
    ),
    "lightgbm": ("lightgbm", "LGBMClassifier", {"random_state": 42, "verbose": -1}),
}


# ── HYPERPARAMETER SEARCH SPACES (used by Optuna) ──────────────────────────────
# Each entry maps to a trial.suggest_* call in experiment.py.
# "type" tells Optuna what kind of suggestion to make.
# All other keys are passed as kwargs to the suggest function.

SEARCH_SPACES = {
    "random_forest": {
        "classifier__n_estimators": {"type": "int", "low": 50, "high": 500, "step": 50},
        "classifier__max_depth": {"type": "int", "low": 3, "high": 20},
        "classifier__min_samples_leaf": {"type": "int", "low": 1, "high": 20},
        "classifier__max_features": {
            "type": "categorical",
            "choices": ["sqrt", "log2", None],
        },
    },
    "gradient_boost": {
        "classifier__n_estimators": {"type": "int", "low": 50, "high": 500, "step": 50},
        "classifier__learning_rate": {
            "type": "float",
            "low": 1e-3,
            "high": 0.3,
            "log": True,
        },
        "classifier__max_depth": {"type": "int", "low": 2, "high": 10},
        "classifier__subsample": {"type": "float", "low": 0.5, "high": 1.0},
    },
    "xgboost": {
        "classifier__n_estimators": {"type": "int", "low": 50, "high": 500, "step": 50},
        "classifier__learning_rate": {
            "type": "float",
            "low": 1e-3,
            "high": 0.3,
            "log": True,
        },
        "classifier__max_depth": {"type": "int", "low": 2, "high": 10},
        "classifier__subsample": {"type": "float", "low": 0.5, "high": 1.0},
        "classifier__colsample_bytree": {"type": "float", "low": 0.5, "high": 1.0},
        "classifier__reg_alpha": {
            "type": "float",
            "low": 1e-8,
            "high": 1.0,
            "log": True,
        },
    },
    "logistic": {
        "classifier__C": {"type": "float", "low": 1e-4, "high": 100, "log": True},
        "classifier__solver": {"type": "categorical", "choices": ["lbfgs", "saga"]},
    },
    "lightgbm": {
        "classifier__n_estimators": {"type": "int", "low": 50, "high": 500, "step": 50},
        "classifier__learning_rate": {
            "type": "float",
            "low": 1e-3,
            "high": 0.3,
            "log": True,
        },
        "classifier__num_leaves": {"type": "int", "low": 20, "high": 300},
        "classifier__min_child_samples": {"type": "int", "low": 5, "high": 100},
        "classifier__reg_alpha": {
            "type": "float",
            "low": 1e-8,
            "high": 10.0,
            "log": True,
        },
    },
}


@dataclass
class PipelineConfig:
    # ── Data ──────────────────────────────────────────────────────────────────
    # data_path:          str = "data/observations.csv"
    target_column: str = "label"  # what we're predicting
    test_size: float = 0.2
    random_seed: int = 42  # controls ALL randomness

    # ── Column groups ─────────────────────────────────────────────────────────
    # List columns explicitly so the pipeline is transparent about what it uses.
    numeric_features: list = field(default_factory=list)
    categorical_features: list = field(default_factory=list)
    features_types: list = field(default_factory=lambda: ["numeric", "categorical"])

    # ── Module selection (keys into the registries above) ─────────────────────
    numeric_imputer: str = "median"
    categorical_imputer: str = "most_frequent"
    encoder: str = "onehot"
    scaler: str = "standard"
    reducer: str = "pca"  # set to "pca" to enable
    classifier: str = "logistic"

    # ── Experiment tracking ───────────────────────────────────────────────────
    experiment_name: str = "inat_obs_scorer_v0_1"
    n_trials: int = 50  # Optuna: number of hyperparameter combos to try
    cv_folds: int = 5  # cross-validation folds per trial
    scoring_metric: str = "roc_auc"  # what Optuna optimizes for

    def set_features(self, df: pd.DataFrame) -> None:
        try:
            df.pop(self.target_column)
            logger.debug(f"Removed {self.target_column}")
        except Exception as e:
            logger.debug(e)

        self.features = df.columns.to_list()
        self.categorical_features = df.select_dtypes(include="object").columns.to_list()
        self.numeric_features = df.select_dtypes(include="number").columns.to_list()

    def change_feature_type(self, feature_name: str) -> None:
        if feature_name in self.features:
            try:
                self.categorical_features.append(feature_name)
                self.numeric_features.remove(feature_name)
                logger.info(f"Moved {feature_name} to categorical features")
            except ValueError:
                self.numeric_features.append(feature_name)
                self.categorical_features.remove(feature_name)
                logger.info(f"Moved {feature_name} to numeric features")
        else:
            logger.warning(f"{feature_name} not found in features list")

    def to_dict(self) -> dict:
        """Serialize config for logging to MLflow."""
        return asdict(self)


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
