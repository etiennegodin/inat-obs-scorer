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
            "boosting_type": "gbdt",
            "colsample_bytree": 0.8558083612168199,
            "importance_type": "split",
            "learning_rate": 0.01,
            "max_depth": -1,
            "min_child_samples": 32,
            "min_child_weight": 0.001,
            "min_split_gain": 0,
            "n_estimators": 2001,
            "n_jobs": 4,
            "num_leaves": 7,
            "random_state": 42,
            "reg_alpha": 0.0014718547064252957,
            "reg_lambda": 1.1734247901028145,
            "subsample": 0.593727005942368,
            "subsample_for_bin": 200000,
            "subsample_freq": 1,
            "verbose": -1,
            "scale_pos_weight": 1,
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
    "lightgbm": {},
}
