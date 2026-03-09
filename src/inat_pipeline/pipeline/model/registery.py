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
