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
            "learning_rate": 0.03,
            "scale_pos_weight": 1,
            "subsample_freq": 1,
            "reg_alpha": 0.5987133447610614,
            "reg_lambda": 0.007829252467134224,
            "subsample": 0.6000493288544528,
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
            "low": 5,
            "high": 15,
            "log": True,
        },
        "classifier__min_child_samples": {
            "type": "int",
            "low": 5,
            "high": 75,
            "log": True,
        },
        "classifier__colsample_bytree": {
            "type": "float",
            "low": 0.75,
            "high": 0.85,
        },
    },
}
