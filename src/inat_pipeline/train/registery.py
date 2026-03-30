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
    "random_forest": (
        "sklearn.ensemble",
        "RandomForestClassifier",
        {},
    ),
    "gradient_boost": (
        "sklearn.ensemble",
        "GradientBoostingClassifier",
        {},
    ),
    "logistic": (
        "sklearn.linear_model",
        "LogisticRegression",
        {"max_iter": 500},
    ),
    "lightgbm": (
        "lightgbm",
        "LGBMClassifier",
        {
            "verbose": -1,
            "n_estimators": 1000,
            "scale_pos_weight": 1,
            "bagging_freq": 1,
            "force_col_wise": True,
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
    "random_forest": {
        "classifier__n_estimators": {"type": "int", "low": 10, "high": 100, "step": 10},
        "classifier__max_depth": {"type": "int", "low": 3, "high": 15},
        "classifier__min_samples_leaf": {"type": "int", "low": 1, "high": 10},
        "classifier__max_features": {
            "type": "categorical",
            "choices": ["sqrt", "log2", None],
        },
    },
    "gradient_boost": {
        "classifier__n_estimators": {"type": "int", "low": 10, "high": 100, "step": 10},
        "classifier__learning_rate": {
            "type": "float",
            "low": 1e-3,
            "high": 0.3,
            "log": True,
        },
        "classifier__max_depth": {"type": "int", "low": 2, "high": 10},
        "classifier__subsample": {"type": "float", "low": 0.5, "high": 1.0},
    },
    "logistic": {
        "classifier__C": {"type": "float", "low": 1e-4, "high": 100, "log": True},
        "classifier__solver": {"type": "categorical", "choices": ["lbfgs", "saga"]},
    },
    "lightgbm": {
        # Capacity
        "classifier__num_leaves": {
            "type": "int",
            "low": 15,
            "high": 127,
            "log": True,
            # rule of thumb: never exceed 2^(max_depth)
            # for depth=7 that's 128 — 200 is already generous
        },
        "classifier__min_child_samples": {
            "type": "int",
            "low": 5,
            "high": 300,
            "log": True,
        },
        # Learning rate
        "classifier__learning_rate": {
            "type": "float",
            "low": 0.05,
            "high": 0.1,
            "log": True,
            # log=True means Optuna samples 0.01, 0.012, 0.015...
            # rather than 0.01, 0.11, 0.21 — much smarter for rates
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
