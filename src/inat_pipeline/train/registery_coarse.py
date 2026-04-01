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
    )
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
        {"n_components": 0.99},
    ),  # keep 95% variance
    "svd": ("sklearn.decomposition", "TruncatedSVD", {"n_components": 40}),
    "none": None,  # skip reduction entirely
}

CLASSIFIER_REGISTRY = {
    "lightgbm": (
        "lightgbm",
        "LGBMClassifier",
        {
            "verbose": -1,
            "n_estimators": 50000,
            "learning_rate": 0.05,
            "scale_pos_weight": 1,
            "subsample_freq": 1,
        },
    ),
}

LIGHTGBM_GPU_PARAMS = {
    "device": "gpu",
    "gpu_platform_id": 0,  # usually 0 for single-GPU machines
    "gpu_device_id": 0,
}


# ── HYPERPARAMETER SEARCH SPACES (used by Optuna) ──────────────────────────────
# Each entry maps to a trial.suggest_* call in experiment.py.
# "type" tells Optuna what kind of suggestion to make.
# All other keys are passed as kwargs to the suggest function.

SEARCH_SPACES = {
    "lightgbm": {
        # Capacity
        "classifier__num_leaves": {
            "type": "int",
            "low": 10,
            "high": 31,
            "log": True,
        },
        "classifier__min_child_samples": {
            "type": "int",
            "low": 5,
            "high": 50,
            "log": True,
        },
        # Regularisation
        "classifier__reg_alpha": {
            "type": "float",
            "low": 1e-4,
            "high": 10.0,
            "log": True,
        },
        "classifier__reg_lambda": {
            "type": "float",
            "low": 1e-4,
            "high": 10.0,
            "log": True,
        },
        # Subsampling
        "classifier__subsample": {
            "type": "float",
            "low": 0.4,
            "high": 1.0,
        },
        "classifier__colsample_bytree": {
            "type": "float",
            "low": 0.4,
            "high": 1.0,
        },
    },
}
